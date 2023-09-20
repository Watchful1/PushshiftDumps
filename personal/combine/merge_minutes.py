import sys
import requests
import time
import discord_logging
import argparse
import os
import re
import zstandard
from datetime import datetime, timedelta
import json
import praw
from praw import endpoints
import prawcore
import logging.handlers

sys.path.append('personal')

log = discord_logging.init_logging(debug=False)

import utils
import classes
from classes import IngestType
from merge import ObjectType


NEWLINE_ENCODED = "\n".encode('utf-8')
reg = re.compile(r"\d\d-\d\d-\d\d_\d\d-\d\d")


def end_of_day(input_minute):
	return input_minute.replace(hour=0, minute=0, second=0) + timedelta(days=1)


def build_day(day_to_process, input_folders, output_folder, object_type):
	file_type = "comments" if object_type == ObjectType.COMMENT else "submissions"

	file_minutes = {}
	minute_iterator = day_to_process - timedelta(minutes=2)
	end_time = end_of_day(day_to_process) + timedelta(minutes=2)
	while minute_iterator <= end_time:
		file_minutes[minute_iterator] = []
		minute_iterator += timedelta(minutes=1)

	for merge_folder, ingest_type in input_folders:
		merge_date_folder = os.path.join(merge_folder, file_type, day_to_process.strftime('%y-%m-%d'))
		if os.path.exists(merge_date_folder):
			for file in os.listdir(merge_date_folder):
				match = reg.search(file)
				if not match:
					log.info(f"File doesn't match regex: {file}")
					continue
				file_date = datetime.strptime(match.group(), '%y-%m-%d_%H-%M')
				if file_date in file_minutes:
					file_minutes[file_date].append((os.path.join(merge_date_folder, file), ingest_type))

	objects = classes.ObjectDict(day_to_process, day_to_process + timedelta(days=1) - timedelta(seconds=1), object_type)
	unmatched_field = False
	minute_iterator = day_to_process - timedelta(minutes=2)
	working_lowest_minute = day_to_process
	last_minute_of_day = end_of_day(day_to_process) - timedelta(minutes=1)
	while minute_iterator <= end_time:
		for ingest_file, ingest_type in file_minutes[minute_iterator]:
			for obj in utils.read_obj_zst(ingest_file):
				if objects.add_object(obj, ingest_type):
					unmatched_field = True
		log.info(f"Loaded {minute_iterator.strftime('%y-%m-%d_%H-%M')} : {objects.get_counts_string_by_minute(minute_iterator, [IngestType.INGEST, IngestType.DOWNLOAD])}")

		if minute_iterator >= end_time or objects.count_minutes() >= 11:
			if minute_iterator > last_minute_of_day:
				working_highest_minute = last_minute_of_day
			else:
				working_highest_minute = minute_iterator - timedelta(minutes=1)

			objects.delete_objects_below_minute(working_lowest_minute)
			while working_lowest_minute <= working_highest_minute:
				folder = os.path.join(output_folder, file_type, working_lowest_minute.strftime('%y-%m-%d'))
				if not os.path.exists(folder):
					os.makedirs(folder)
				output_path = os.path.join(folder, f"{('RS' if object_type == ObjectType.COMMENT else 'RC')}_{working_lowest_minute.strftime('%y-%m-%d_%H-%M')}.zst")
				output_handle = zstandard.ZstdCompressor().stream_writer(open(output_path, 'wb'))

				for obj in objects.by_minute[working_lowest_minute].obj_list:
					output_handle.write(json.dumps(obj, sort_keys=True).encode('utf-8'))
					output_handle.write(NEWLINE_ENCODED)
					objects.delete_object_id(obj['id'])
				log.info(f"Wrote up to {working_lowest_minute.strftime('%y-%m-%d_%H-%M')}")
				output_handle.close()
				working_lowest_minute += timedelta(minutes=1)

			objects.rebuild_minute_dict()

		discord_logging.flush_discord()
		if unmatched_field:
			log.info(f"Unmatched field, aborting")
			sys.exit(1)

		minute_iterator += timedelta(minutes=1)

	log.info(f"Finished day {day_to_process.strftime('%y-%m-%d')}: {objects.get_counts_string()}")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Combine two ingest files")
	parser.add_argument("--type", help="The object type, either comments or submissions", required=True)
	parser.add_argument("--start_date", help="The start of the date range to process, format YY-MM-DD_HH-MM", required=True)
	parser.add_argument("--end_date", help="The end of the date range to process, format YY-MM-DD. If not provided, the script processes to the end of the day")
	parser.add_argument('--input', help='Input folder', required=True)
	parser.add_argument('--output', help='Output folder', required=True)
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
	args = parser.parse_args()

	if args.debug:
		discord_logging.set_level(logging.DEBUG)

	if args.start_date is None:
		log.error(f"No start date provided")
		sys.exit(2)
	start_date = datetime.strptime(args.start_date, '%y-%m-%d_%H-%M')
	end_date = end_of_day(start_date)
	if args.end_date is not None:
		end_date = datetime.strptime(args.end_date, '%y-%m-%d')

	input_folders = [
		(os.path.join(args.input, "combined"), IngestType.INGEST),
		(os.path.join(args.input, "download"), IngestType.DOWNLOAD),
	]

	for input_folder, ingest_type in input_folders:
		log.info(f"Input folder: {input_folder}")
	log.info(f"Output folder: {args.output}")

	object_type = None
	if args.type == "comments":
		object_type = ObjectType.COMMENT
	elif args.type == "submissions":
		object_type = ObjectType.SUBMISSION
	else:
		log.error(f"Invalid type: {args.type}")
		sys.exit(2)

	while start_date <= end_date:
		build_day(start_date, input_folders, args.output, object_type)
		start_date = end_of_day(start_date)
