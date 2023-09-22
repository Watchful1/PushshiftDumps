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


def save_pushshift_token(token):
	with open("pushshift.txt", 'w') as file:
		file.write(token)


def load_pushshift_token():
	with open("pushshift.txt", 'r') as file:
		token = file.read().strip()
	return token


def re_auth_pushshift(old_token):
	url = f"https://auth.pushshift.io/refresh?access_token={old_token}"
	log.warning(f"Reauth request: {url}")
	response = requests.post(url)
	result = response.json()
	log.warning(f"Reauth response: {str(result)}")
	discord_logging.flush_discord()
	if 'access_token' in result:
		new_token = result['access_token']
		log.warning(f"New pushshift token: {new_token}")
		save_pushshift_token(new_token)
		discord_logging.flush_discord()
		return new_token
	elif 'detail' in result:
		if result['detail'] == 'Access token is still active and can not be refreshed.':
			log.warning(f"Access token still active, trying request again")
			time.sleep(5)
			return old_token

		log.warning(f"Reauth failed: {result['detail']}")
		discord_logging.flush_discord()
		sys.exit(1)
	else:
		log.warning(f"Something went wrong re-authing")
		discord_logging.flush_discord()
		sys.exit(1)


def query_pushshift(ids, bearer, object_type):
	object_name = "comment" if object_type == ObjectType.COMMENT else "submission"
	url = f"https://api.pushshift.io/reddit/{object_name}/search?limit=1000&ids={','.join(ids)}"
	log.debug(f"pushshift query: {url}")
	response = None
	for i in range(4):
		try:
			response = requests.get(url, headers={
				'User-Agent': "In script by /u/Watchful1",
				'Authorization': f"Bearer {bearer}"}, timeout=15)
		except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout):
			time.sleep(2)
			continue
		if response is None:
			continue
		if response.status_code == 200:
			break
		if response.status_code == 403:
			log.warning(f"Pushshift 403, trying reauth: {response.json()}")
			log.warning(url)
			log.warning(f"'Authorization': Bearer {bearer}")
			bearer = re_auth_pushshift(bearer)
		time.sleep(2)
	if response.status_code != 200:
		log.warning(f"4 requests failed with status code {response.status_code}")
		sys.exit(1)
	return response.json()['data'], bearer


def query_reddit(ids, reddit, object_type):
	id_prefix = 't1_' if object_type == ObjectType.COMMENT else 't3_'
	id_string = f"{id_prefix}{(f',{id_prefix}'.join(ids))}"
	response = None
	for i in range(4):
		try:
			response = reddit.request(method="GET", path=endpoints.API_PATH["info"], params={"id": id_string})
			break
		except (prawcore.exceptions.ServerError, prawcore.exceptions.RequestException):
			time.sleep(2)
	return response['data']['children']


def end_of_day(input_minute):
	return input_minute.replace(hour=0, minute=0, second=0) + timedelta(days=1)


def build_day(day_to_process, input_folders, output_folder, object_type, reddit):
	pushshift_token = load_pushshift_token()
	log.info(f"Using pushshift token: {pushshift_token}")

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
		log.info(f"Loaded {minute_iterator.strftime('%y-%m-%d_%H-%M')} : {objects.get_counts_string_by_minute(minute_iterator, [IngestType.INGEST, IngestType.RESCAN, IngestType.DOWNLOAD])}")

		if minute_iterator >= end_time or objects.count_minutes() >= 11:
			if minute_iterator > last_minute_of_day:
				working_highest_minute = last_minute_of_day
			else:
				working_highest_minute = minute_iterator - timedelta(minutes=1)
			missing_ids, start_id, end_id = objects.get_missing_ids_by_minutes(working_lowest_minute, working_highest_minute)
			log.debug(
				f"Backfilling from: {working_lowest_minute.strftime('%y-%m-%d_%H-%M')} ({utils.base36encode(start_id)}|{start_id}) to "
				f"{working_highest_minute.strftime('%y-%m-%d_%H-%M')} ({utils.base36encode(end_id)}|{end_id}) with {len(missing_ids)} ({end_id - start_id}) ids")

			for chunk in utils.chunk_list(missing_ids, 50):
				pushshift_objects, pushshift_token = query_pushshift(chunk, pushshift_token, object_type)
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
					f"Wrote up to {working_lowest_minute.strftime('%y-%m-%d_%H-%M')} : "
					f"{objects.get_counts_string_by_minute(working_lowest_minute, [IngestType.PUSHSHIFT, IngestType.BACKFILL, IngestType.MISSING])}")
				output_handle.close()
				working_lowest_minute += timedelta(minutes=1)

			objects.rebuild_minute_dict()

		discord_logging.flush_discord()
		if unmatched_field:
			log.warning(f"Unmatched field, aborting")
			discord_logging.flush_discord()
			sys.exit(1)

		minute_iterator += timedelta(minutes=1)

	log.info(f"Finished day {day_to_process.strftime('%y-%m-%d')}: {objects.get_counts_string()}")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Combine the ingest and rescan files, clean and do pushshift lookups as needed")
	parser.add_argument("--type", help="The object type, either comments or submissions", required=True)
	parser.add_argument("--start_date", help="The start of the date range to process, format YY-MM-DD_HH-MM", required=True)
	parser.add_argument("--end_date", help="The end of the date range to process, format YY-MM-DD. If not provided, the script processes to the end of the day")
	parser.add_argument('--input', help='Input folder', required=True)
	parser.add_argument('--output', help='Output folder', required=True)
	parser.add_argument('--pushshift', help='The pushshift token')
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
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

	user_name = "Watchful12"
	reddit = praw.Reddit(user_name)

	discord_logging.init_discord_logging(
		section_name=None,
		log_level=logging.WARNING,
		logging_webhook=reddit.config.custom["logging_webhook"]
	)

	if args.pushshift is not None:
		log.warning(f"Saving pushshift token: {args.pushshift}")
		save_pushshift_token(args.pushshift)

	while start_date <= end_date:
		build_day(start_date, input_folders, args.output, object_type, reddit)
		start_date = end_of_day(start_date)
