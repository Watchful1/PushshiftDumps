import sys
import requests
import subprocess
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

log = discord_logging.get_logger(init=True)

import utils
import classes
from classes import IngestType
from merge import ObjectType


NEWLINE_ENCODED = "\n".encode('utf-8')
reg = re.compile(r"\d\d-\d\d-\d\d_\d\d-\d\d")


TOKEN_SSH_TIMEOUT_SECONDS = 60
TOKEN_WAIT_SECONDS = 30
TOKEN_WAIT_LIMIT_SECONDS = 15 * 60


def fetch_pushshift_token(token_host, token_path):
	"""Read the token file the ingest maintains on token_host over ssh. Never logs the value."""
	try:
		result = subprocess.run(
			["ssh", "-n", "-o", "BatchMode=yes", "-o", "ConnectTimeout=20", token_host, "cat", token_path],
			capture_output=True, text=True, timeout=TOKEN_SSH_TIMEOUT_SECONDS)
	except subprocess.TimeoutExpired:
		raise RuntimeError(f"Timed out fetching the pushshift token from {token_host}")
	except OSError as err:
		raise RuntimeError(f"Could not run ssh to fetch the pushshift token: {err}")
	if result.returncode != 0:
		raise RuntimeError(
			f"Fetching the pushshift token from {token_host}:{token_path} failed with code {result.returncode}: {result.stderr.strip()}")
	token = result.stdout.strip()
	if not token:
		raise RuntimeError(f"Pushshift token file {token_path} on {token_host} is empty")
	return token


class PushshiftTokenFetcher:
	"""Read-only view of the token the ingest refreshes on token_host.

	The ingest is the only process that may call auth.pushshift.io/refresh, so on a 403 this
	just re-fetches the file and waits for the ingest to change it. A transient ssh failure is
	treated the same as an unchanged token: retried every TOKEN_WAIT_SECONDS until the deadline.
	Pass a lock when several workers share one so they do not all hit ssh at once; it is only
	held around the ssh call itself, never across a sleep.
	"""
	def __init__(self, token_host, token_path, lock=None):
		self.token_host = token_host
		self.token_path = token_path
		self.lock = lock
		self.token = None

	def _fetch(self):
		if self.lock is not None:
			self.lock.acquire()
		try:
			return fetch_pushshift_token(self.token_host, self.token_path)
		finally:
			if self.lock is not None:
				self.lock.release()

	def _wait_for_token(self, failed_token, waiting_for):
		deadline = time.monotonic() + TOKEN_WAIT_LIMIT_SECONDS
		first_wait = True
		while True:
			try:
				token = self._fetch()
			except RuntimeError as err:
				log.info(f"Pushshift token fetch failed, will retry: {err}")
				token = failed_token
			if token != failed_token:
				return token
			if time.monotonic() >= deadline:
				raise RuntimeError(
					f"Could not get {waiting_for} from {self.token_host} in {TOKEN_WAIT_LIMIT_SECONDS // 60} minutes, is the ingest refreshing it?")
			message = f"Pushshift token not available yet ({waiting_for}), waiting {TOKEN_WAIT_SECONDS}s for the ingest"
			if first_wait:
				log.warning(message)
				first_wait = False
			else:
				log.info(message)
			time.sleep(TOKEN_WAIT_SECONDS)

	def __call__(self, failed_token):
		if failed_token is None:
			if self.token is None:
				self.token = self._wait_for_token(None, "an initial token")
				log.info(f"Fetched pushshift token from {self.token_host}")
			return self.token

		if self.token is not None and self.token != failed_token:
			return self.token

		self.token = self._wait_for_token(failed_token, "a changed token")
		log.info(f"Pushshift token on {self.token_host} changed, using the new one")
		return self.token


def query_pushshift(ids, bearer, object_type, pushshift_token_function):
	object_name = "comment" if object_type == ObjectType.COMMENT else "submission"
	url = f"https://api.pushshift.io/reddit/{object_name}/search?limit=1000&ids={','.join(ids)}"
	log.debug(f"pushshift query: {url}")
	response = None
	total_attempts = 100
	current_attempt = 0
	sleep_per_attempt = 10
	for current_attempt in range(total_attempts):
		try:
			response = requests.get(url, headers={
				'User-Agent': "In script by /u/Watchful1",
				'Authorization': f"Bearer {bearer}"}, timeout=20)
		except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as err:
			log.info(f"Pushshift failed, sleeping {current_attempt * sleep_per_attempt} : {err}")
			time.sleep(current_attempt * sleep_per_attempt)
			continue
		if response is None:
			log.info(f"Pushshift failed, sleeping {current_attempt * sleep_per_attempt} : no response")
			time.sleep(current_attempt * sleep_per_attempt)
			continue
		if response.status_code == 200:
			break
		if response.status_code == 403:
			log.warning(f"Pushshift 403, fetching the current token: {response.text[:200]}")
			log.warning(url)
			bearer = pushshift_token_function(bearer)
		log.info(f"Pushshift failed, sleeping {current_attempt * sleep_per_attempt} : status {response.status_code}")
		time.sleep(current_attempt * sleep_per_attempt)
	if response is None:
		log.warning(f"{current_attempt + 1} requests failed with no response")
		log.warning(url)
		discord_logging.flush_discord()
		raise RuntimeError(f"Pushshift gave no response after {current_attempt + 1} attempts")
	if response.status_code != 200:
		log.warning(f"{current_attempt + 1} requests failed with status code {response.status_code}")
		log.warning(url)
		discord_logging.flush_discord()
		raise RuntimeError(f"Pushshift failed with status {response.status_code} after {current_attempt + 1} attempts")
	if current_attempt > 0:
		log.info(f"Pushshift call succeeded after {current_attempt + 1} retries")
	return response.json()['data'], bearer


def query_reddit(ids, reddit, object_type):
	id_prefix = 't1_' if object_type == ObjectType.COMMENT else 't3_'
	id_string = f"{id_prefix}{(f',{id_prefix}'.join(ids))}"
	response = None
	for i in range(20):
		try:
			response = reddit.request(method="GET", path=endpoints.API_PATH["info"], params={"id": id_string})
			break
		except (prawcore.exceptions.ServerError, prawcore.exceptions.RequestException, prawcore.exceptions.TooManyRequests) as err:
			log.info(f"No response from reddit api for {object_type}, sleeping {i * 5} seconds: {err} : {id_string}")
			time.sleep(i * 5)
	if response is None:
		log.warning(f"Reddit api failed after retries, skipping chunk: {id_string}")
		return []
	return response['data']['children']


def end_of_day(input_minute):
	return input_minute.replace(hour=0, minute=0, second=0) + timedelta(days=1)


def build_day(day_to_process, input_folders, output_folder, object_type, reddit, ignore_ids, pushshift_token_function):
	file_type = "comments" if object_type == ObjectType.COMMENT else "submissions"

	pushshift_token = pushshift_token_function(None)

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
					log.info(f"{file_type}: File doesn't match regex: {file}")
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
		log.info(f"{file_type}: Loaded {minute_iterator.strftime('%y-%m-%d_%H-%M')} : {objects.get_counts_string_by_minute(minute_iterator, [IngestType.INGEST, IngestType.RESCAN, IngestType.DOWNLOAD])}")

		if minute_iterator >= end_time or objects.count_minutes() >= 11:
			if minute_iterator > last_minute_of_day:
				working_highest_minute = last_minute_of_day
			else:
				working_highest_minute = minute_iterator - timedelta(minutes=1)
			missing_ids, start_id, end_id = objects.get_missing_ids_by_minutes(working_lowest_minute, working_highest_minute, ignore_ids)
			if start_id is None or end_id is None:
				log.warning(f"Unable to get start or end id for minute {minute_iterator} : {working_lowest_minute} : {working_highest_minute}")
				minute_iterator += timedelta(minutes=1)
				continue
			log.debug(
				f"{file_type}: Backfilling from: {working_lowest_minute.strftime('%y-%m-%d_%H-%M')} ({utils.base36encode(start_id)}|{start_id}) to "
				f"{working_highest_minute.strftime('%y-%m-%d_%H-%M')} ({utils.base36encode(end_id)}|{end_id}) with {len(missing_ids)} ({end_id - start_id}) ids")

			for chunk in utils.chunk_list(missing_ids, 50):
				pushshift_objects, pushshift_token = query_pushshift(chunk, pushshift_token, object_type, pushshift_token_function)
				for pushshift_object in pushshift_objects:
					if objects.add_object(pushshift_object, IngestType.PUSHSHIFT):
						unmatched_field = True

			for chunk in utils.chunk_list(missing_ids, 100):
				reddit_objects = query_reddit(chunk, reddit, object_type)
				for reddit_object in reddit_objects:
					if objects.add_object(reddit_object['data'], IngestType.BACKFILL):
						unmatched_field = True

			for missing_id in missing_ids:
				if missing_id not in objects.by_id:
					objects.add_missing_object(missing_id)

			objects.delete_objects_below_minute(working_lowest_minute)
			while working_lowest_minute <= working_highest_minute:
				folder = os.path.join(output_folder, file_type, working_lowest_minute.strftime('%y-%m-%d'))
				if not os.path.exists(folder):
					os.makedirs(folder)
				output_path = os.path.join(folder, f"{('RC' if object_type == ObjectType.COMMENT else 'RS')}_{working_lowest_minute.strftime('%y-%m-%d_%H-%M')}.zst")
				output_handle = zstandard.ZstdCompressor().stream_writer(open(output_path, 'wb'))

				for obj in objects.by_minute[working_lowest_minute].obj_list:
					output_handle.write(json.dumps(obj, sort_keys=True).encode('utf-8'))
					output_handle.write(NEWLINE_ENCODED)
					objects.delete_object_id(obj['id'])
				log.info(
					f"{file_type}: Wrote up to {working_lowest_minute.strftime('%y-%m-%d_%H-%M')} : "
					f"{objects.get_counts_string_by_minute(working_lowest_minute, [IngestType.PUSHSHIFT, IngestType.BACKFILL, IngestType.MISSING])}")
				output_handle.close()
				working_lowest_minute += timedelta(minutes=1)

			objects.rebuild_minute_dict()

		discord_logging.flush_discord()
		if unmatched_field:
			log.warning(f"{file_type}: Unmatched field, aborting")
			discord_logging.flush_discord()
			raise RuntimeError(f"{file_type}: unmatched field in {day_to_process.strftime('%y-%m-%d')}")

		minute_iterator += timedelta(minutes=1)

	log.info(f"{file_type}: Finished day {day_to_process.strftime('%y-%m-%d')}: {objects.get_counts_string()}")


def merge_and_backfill(start_date, end_date, input_folders, output_folder, object_type, ignore_ids, reddit_username, pushshift_token_function):
	reddit = praw.Reddit(reddit_username)
	while start_date <= end_date:
		build_day(start_date, input_folders, output_folder, object_type, reddit, ignore_ids, pushshift_token_function)
		start_date = end_of_day(start_date)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Combine the ingest and rescan files, clean and do pushshift lookups as needed")
	parser.add_argument("--type", help="The object type, either comments or submissions", required=True)
	parser.add_argument("--start_date", help="The start of the date range to process, format YY-MM-DD_HH-MM", required=True)
	parser.add_argument("--end_date", help="The end of the date range to process, format YY-MM-DD. If not provided, the script processes to the end of the day")
	parser.add_argument('--input', help='Input folder', required=True)
	parser.add_argument('--output', help='Output folder', required=True)
	parser.add_argument('--token_host', help='ssh host that runs PushshiftIngest; the token is read from there and never refreshed here', required=True)
	parser.add_argument('--token_path', help='Path of the token file on token_host', default="PushshiftIngest/pushshift_token.txt")
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
	parser.add_argument("--ignore_ids", help="Ignore ids between the id ranges listed", default=None)
	args = parser.parse_args()

	if args.debug:
		discord_logging.set_level(logging.DEBUG)

	input_folders = [
		(os.path.join(args.input, "ingest"), IngestType.INGEST),
		(os.path.join(args.input, "rescan"), IngestType.RESCAN),
		(os.path.join(args.input, "download"), IngestType.DOWNLOAD),
	]

	if args.start_date is None:
		log.error(f"No start date provided")
		sys.exit(2)
	start_date = datetime.strptime(args.start_date, '%y-%m-%d_%H-%M')
	end_date = end_of_day(start_date)
	if args.end_date is not None:
		end_date = datetime.strptime(args.end_date, '%y-%m-%d')

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

	ignore_ids = []
	if args.ignore_ids is not None:
		for id_range in args.ignore_ids.split(","):
			start_id, end_id = id_range.split("-")
			ignore_ids.append((utils.base36decode(start_id), utils.base36decode(end_id)))

	discord_logging.init_discord_logging(
		section_name="Watchful12",
		log_level=logging.WARNING
	)

	merge_and_backfill(
		start_date,
		end_date,
		input_folders,
		args.output,
		object_type,
		ignore_ids,
		"Watchful12",
		PushshiftTokenFetcher(args.token_host, args.token_path)
	)
