from datetime import datetime
import os
import discord_logging
import sys
import zstandard
import json
from enum import Enum
from sortedcontainers import SortedList
from collections import defaultdict

log = discord_logging.get_logger()

import utils
import merge

NEWLINE_ENCODED = "\n".encode('utf-8')


class ApiRequest:
	def __init__(self, ids, is_submission, ingest_name, estimated_datetime=None, missing_expected=False):
		self.ids = ids
		self.is_submission = is_submission
		self.ingest_name = ingest_name
		self.estimated_datetime = estimated_datetime
		self.missing_expected = missing_expected
		self.results = None
		self.complete = False
		self.tries = 0
		self.prev_lengths = []

	def should_retry(self):
		if self.complete:
			return False  # the request is complete, no need to retry
		if len(self.prev_lengths) <= 1:
			return True  # we've only made one attempt and it didn't work, do retry
		if self.prev_lengths[-1] == 0:
			if len(self.prev_lengths) < (10 if self.missing_expected else 100):
				return True  # the most recent result was 0 objects, retry up to 100 times
			else:
				log.info(f"Force finished request with retries: {self}")
				self.complete = True
				return False
		if self.prev_lengths[-1] == self.prev_lengths[-2]:
			if self.missing_expected:
				self.complete = True
				return False  # the latest two requests were the same and we're expecting missing objects, mark as complete
			elif len(self.prev_lengths) >= 4 and \
					self.prev_lengths[-1] == self.prev_lengths[-3] and \
					self.prev_lengths[-1] == self.prev_lengths[-4]:
				log.info(f"Force finished request with retries: {self}")
				self.complete = True
				return False  # the latest four requests were the same, go ahead and mark as complete
		return True  # recent requests didn't match, and weren't 0, go ahead and retry

	def get_body_key(self):
		return "self_text" if self.is_submission else "body"

	def get_string_type(self):
		return "submission" if self.is_submission else "comment"

	def get_prefix(self):
		return "t3_" if self.is_submission else "t1_"

	def set_results(self, results):
		self.prev_lengths.append(len(results))
		self.results = []
		current_timestamp = int(datetime.utcnow().timestamp())
		for result in results:
			obj = result['data']
			if 'body_html' in obj:
				del obj['body_html']
			if 'selftext_html' in obj:
				del obj['selftext_html']
			obj['retrieved_on'] = current_timestamp
			self.results.append(obj)
		log.debug(f"Set result: {self}")

	def id_string(self):
		return f"{self.get_prefix()}{(f',{self.get_prefix()}'.join(self.ids))}"

	def __str__(self):
		return \
			f"{self.ingest_name}: {self.ids[0]}-{self.ids[-1]} {self.get_string_type()}: " \
			f"{len(self.results) if self.results else self.results} : {self.tries} : " \
			f"{self.complete} : {','.join([str(val) for val in self.prev_lengths])}"

	def __gt__(self, other):
		if isinstance(other, ApiRequest):
			return False
		return True

	def __lt__(self, other):
		if isinstance(other, ApiRequest):
			return True
		return False

	def __eq__(self, other):
		if isinstance(other, ApiRequest):
			return True
		return False


class Queue:
	def __init__(self, max_size):
		self.list = []
		self.max_size = max_size

	def put(self, item):
		if len(self.list) >= self.max_size:
			self.list.pop(0)
		self.list.append(item)

	def peek(self):
		return self.list[0] if len(self.list) > 0 else None


class OutputHandle:
	def __init__(self, is_submission, dump_folder):
		self.handle = None
		self.current_path = None
		self.current_minute = None
		self.is_submission = is_submission
		self.dump_folder = dump_folder

		if not os.path.exists(dump_folder):
			os.makedirs(dump_folder)

	def matched_minute(self, new_date_time):
		return self.current_minute is not None and new_date_time.minute == self.current_minute

	def get_path(self, date_folder, export_filename, increment=None):
		folder = f"{self.dump_folder}{os.path.sep}{date_folder}"
		if not os.path.exists(folder):
			os.makedirs(folder)

		bldr = [folder]
		bldr.append(os.path.sep)
		if self.is_submission:
			bldr.append("RS_")
		else:
			bldr.append("RC_")
		bldr.append(export_filename)
		if increment is not None:
			bldr.append("_")
			bldr.append(str(increment))
		bldr.append(".zst")

		return ''.join(bldr)

	def rollover_to_minute(self, date_time):
		if self.handle is not None:
			self.handle.close()
			os.rename(self.current_path + ".tmp", self.current_path)
		date_folder = date_time.strftime('%y-%m-%d')
		export_filename = date_time.strftime('%y-%m-%d_%H-%M')
		export_path = self.get_path(date_folder, export_filename)
		if os.path.exists(export_path + ".tmp"):
			os.rename(export_path + ".tmp", export_path)
		i = 0
		while os.path.exists(export_path):
			log.info(f"Dump exists, incrementing: {export_path}")
			i += 1
			export_path = self.get_path(date_folder, export_filename, i)
			if i > 100:
				log.warning(f"Something went wrong, more than 100 dumps for minute, aborting")
				sys.exit(3)
		self.current_path = export_path
		self.handle = zstandard.ZstdCompressor().stream_writer(open(export_path + ".tmp", 'wb'))
		self.current_minute = date_time.minute

	def write_object(self, obj):
		self.handle.write(json.dumps(obj, sort_keys=True).encode('utf-8'))
		self.handle.write(NEWLINE_ENCODED)

	def flush(self):
		self.handle.flush()

	def close(self):
		if self.handle is not None:
			self.handle.close()


class IngestType(Enum):
	INGEST = 1
	RESCAN = 2
	DOWNLOAD = 3
	PUSHSHIFT = 4
	BACKFILL = 5
	MISSING = 6


class ObjectDict:
	def __init__(self, min_datetime, max_datetime, obj_type):
		self.min_datetime = min_datetime
		self.max_datetime = max_datetime
		self.obj_type = obj_type

		self.counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
		self.min_id = None
		self.max_id = None

		self.by_id = {}
		self.by_minute = defaultdict(ObjectMinuteList)

	def contains_id(self, str_id):
		return str_id in self.by_id

	def delete_object_id(self, str_id):
		del self.by_id[str_id]

	def delete_objects_below_minute(self, delete_below_minute):
		for minute, minute_list in self.by_minute.items():
			if minute < delete_below_minute:
				for obj in minute_list.obj_list:
					self.delete_object_id(obj['id'])

	def rebuild_minute_dict(self):
		self.by_minute = defaultdict(ObjectMinuteList)
		for obj in self.by_id.values():
			created_minute = datetime.utcfromtimestamp(obj["created_utc"]).replace(second=0, microsecond=0)
			self.by_minute[created_minute].add(obj)

	def count_minutes(self):
		return len(self.by_minute)

	@staticmethod
	def get_counts_string_from_dict(counts_dict, ingest_types):
		bldr = []
		for ingest_type in ingest_types:
			if ingest_type in counts_dict:
				bldr.append(f"{counts_dict[ingest_type][True]}({counts_dict[ingest_type][False]})")
			else:
				bldr.append("0(0)")
		return "|".join(bldr)

	def get_counts_string_by_minute(self, minute, ingest_types):
		count_string = ObjectDict.get_counts_string_from_dict(self.counts[minute], ingest_types)
		minute_dict = self.by_minute.get(minute)
		if minute_dict is None:
			range_string = ""
		else:
			range_string = f" - {len(minute_dict.obj_list)} ({minute_dict.max_id - minute_dict.min_id}) ({utils.base36encode(minute_dict.min_id)}-{utils.base36encode(minute_dict.max_id)})"
		return count_string + range_string

	def get_counts_string(self):
		sum_dict = defaultdict(lambda: defaultdict(int))
		for counts_dict in self.counts.values():
			for ingest_type in IngestType:
				if ingest_type in counts_dict:
					sum_dict[ingest_type][True] += counts_dict[ingest_type][True]
					sum_dict[ingest_type][False] += counts_dict[ingest_type][False]

		return ObjectDict.get_counts_string_from_dict(sum_dict, IngestType)

	def get_missing_ids_by_minutes(self, start_minute, end_minute):
		start_id = self.by_minute[start_minute].min_id
		end_id = self.by_minute[end_minute].max_id
		missing_ids = []
		for int_id in range(start_id, end_id + 1):
			string_id = utils.base36encode(int_id)
			if not self.contains_id(string_id):
				missing_ids.append(string_id)
		return missing_ids, start_id, end_id

	def add_object(self, obj, ingest_type):
		created_utc = datetime.utcfromtimestamp(obj["created_utc"])
		created_minute = created_utc.replace(second=0, microsecond=0)
		if obj['id'] in self.by_id:
			existing_obj = self.by_id[obj['id']]
			unmatched_field = merge.merge_fields(existing_obj, obj, self.obj_type)
			self.counts[created_minute][ingest_type][False] += 1
			return unmatched_field
		if created_utc < self.min_datetime or created_utc > self.max_datetime:
			return False
		unmatched_field = merge.parse_fields(obj, self.obj_type)
		self.by_id[obj['id']] = obj
		self.by_minute[created_minute].add(obj)
		self.counts[created_minute][ingest_type][True] += 1
		self.min_id, self.max_id = utils.merge_lowest_highest_id(obj['id'], self.min_id, self.max_id)
		return unmatched_field

	def add_missing_object(self, obj_id):
		if obj_id in self.by_id:
			return
		int_id = utils.base36decode(obj_id)
		for minute, minute_dict in self.by_minute.items():
			if minute_dict.min_id is None:
				continue
			if minute_dict.min_id < int_id < minute_dict.max_id:
				self.counts[minute][IngestType.MISSING][True] += 1
				return


class ObjectMinuteList:
	def __init__(self):
		self.obj_list = SortedList(key=lambda x: f"{x['created_utc']}:{x['id']}")
		self.min_id = None
		self.max_id = None

	def add(self, obj):
		self.min_id, self.max_id = utils.merge_lowest_highest_id(obj['id'], self.min_id, self.max_id)
		self.obj_list.add(obj)
