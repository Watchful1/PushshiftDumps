import sys
sys.path.append('personal')
sys.path.append('combine')
sys.path.append('personal/combine')

import os
import argparse
import json
import time
import logging.handlers
import requests
import praw
from datetime import datetime, timedelta
import multiprocessing_logging

import discord_logging
import multiprocessing

log = discord_logging.init_logging()
multiprocessing_logging.install_mp_handler("bot")

import utils
from transform import split_blocks_by_minutes
from combine.merge_and_backfill import build_day, IngestType, ObjectType
from combine import build_month


def get_pushshift_token(old_token):
	global pushshift_lock
	pushshift_lock.acquire()
	saved_token = load_pushshift_token()
	if saved_token is None or saved_token == "" or old_token == saved_token:
		if old_token is None:
			log.warning("No saved or passed in token")
			save_pushshift_token("")
			raise ValueError("No saved or passed in token")

		log.info(f"Requesting new token")
		result_token = re_auth_pushshift(old_token)
		save_pushshift_token(result_token)
	else:
		result_token = saved_token

	pushshift_lock.release()
	return result_token


def save_pushshift_token(token):
	with open("pushshift.txt", 'w') as file:
		file.write(token)


def load_pushshift_token():
	if not os.path.exists("pushshift.txt"):
		return None
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
		return old_token
	else:
		log.warning(f"Something went wrong re-authing")
		discord_logging.flush_discord()
		return old_token


def init(p_lock):
	global pushshift_lock
	pushshift_lock = p_lock


def save_status(status_json, stages, month):
	log.debug(f"Saving status: {stages}")
	output_dict = {
		"stages": stages,
		"month": month,
	}
	json_string = json.dumps(output_dict, indent=4, default=str)
	with open(status_json, 'w') as status_json_file:
		status_json_file.write(json_string)


def load_status(status_json):
	if os.path.exists(status_json):
		with open(status_json, 'r') as status_json_file:
			output_dict = json.load(status_json_file)
			for stage_type, stage in output_dict["stages"].items():
				if stage["merge"] is not None:
					stage["merge"] = datetime.strptime(stage["merge"], "%Y-%m-%d %H:%M:%S")
			return output_dict["stages"], output_dict["month"]
	else:
		stages = {
			"comment": {
				"split": False,
				"merge": None,  # 24-02-01
				"build": False,
			},
			"submission": {
				"split": False,
				"merge": None,  # 24-02-01
				"build": False,
			}
		}
		return stages, None


def end_of_day(input_minute):
	return input_minute.replace(hour=0, minute=0, second=0) + timedelta(days=1)


def process(queue, base_folder, month, file_type, type_stages, reddit_username, compression_level, ignore_ids):
	try:
		# for stage, status in type_stages.items():
		# 	log.info(f"{file_type} {stage}: {status}")
		file_prefix = "RC" if file_type == "comment" else "RS"
		if not type_stages["split"]:
			original_split_file = os.path.join(base_folder, "reddit", "blocks", f"{file_prefix}_20{month}.zst")
			split_file = os.path.join(base_folder, "reddit", "blocks", f"{file_prefix}B_20{month}.zst")
			if os.path.exists(original_split_file):
				os.rename(original_split_file, split_file)

			if not os.path.exists(split_file):
				log.info(f"{file_type}: File {split_file} doesn't exist, checking for blocks")
				split_file = os.path.join(base_folder, "reddit", "blocks", f"{file_prefix}_20{month}.zst_blocks")
				if not os.path.exists(split_file):
					log.error(f"{file_type}: File {split_file} doesn't exist, aborting")
					return False

			split_folder = os.path.join(base_folder, "ingest", "download")

			log.info(f"{file_type}: Starting {file_type} split")
			log.info(f"{file_type}: Reading from: {split_file}")
			log.info(f"{file_type}: Writing to: {split_folder}")
			split_blocks_by_minutes.split_by_minutes(split_file, split_folder)

			log.warning(f"{file_type}: {file_type} split complete")
			discord_logging.flush_discord()
			queue.put((file_type, "split", True))

		start_date = datetime.strptime(month, "%y-%m")
		if start_date.month == 12:
			end_date = start_date.replace(year=start_date.year + 1, month=1)
		else:
			end_date = start_date.replace(month=start_date.month + 1)
		if type_stages["merge"] is None or type_stages["merge"] < end_date:
			if type_stages["merge"] is not None:
				start_date = type_stages["merge"]

			log.info(f"{file_type}: Starting {file_type} merge from {start_date}")

			reddit = praw.Reddit(reddit_username)

			input_folders = [
				(os.path.join(base_folder, "ingest", "ingest"), IngestType.INGEST),
				(os.path.join(base_folder, "ingest", "rescan"), IngestType.RESCAN),
				(os.path.join(base_folder, "ingest", "download"), IngestType.DOWNLOAD),
			]
			for input_folder in input_folders:
				log.info(f"{file_type}: Reading from: {input_folder[0]} : {input_folder[1]}")
			combined_folder = os.path.join(base_folder, "ingest", "combined")
			log.info(f"{file_type}: Writing to: {combined_folder}")
			while start_date < end_date:
				build_day(
					start_date,
					input_folders,
					combined_folder,
					ObjectType.COMMENT if file_type == "comment" else ObjectType.SUBMISSION,
					reddit,
					ignore_ids,
					get_pushshift_token
				)
				start_date = end_of_day(start_date)
				queue.put((file_type, "merge", start_date))
			log.warning(f"{file_type}: {file_type} merge complete")
			discord_logging.flush_discord()

		if not type_stages["build"]:
			log.info(f"{file_type}: Starting {file_type} build")
			start_date = datetime.strptime(month, "%y-%m")

			input_folder = os.path.join(base_folder, "ingest", "combined")
			output_folder = os.path.join(base_folder, "reddit")
			log.info(f"{file_type}: Reading from: {input_folder}")
			log.info(f"{file_type}: Writing to: {output_folder}")
			build_month.build_month(
				start_date,
				input_folder,
				output_folder,
				file_type+"s",
				compression_level
			)
			queue.put((file_type, "build", True))
			log.warning(f"{file_type}: {file_type} build complete")
			discord_logging.flush_discord()

		log.warning(f"{file_type}: {file_type} all steps complete")

		log.info(f'torrenttools create -a "https://academictorrents.com/announce.php" -c "Reddit comments and submissions from {month}" --include ".*(comments|submissions).*R._{month}.zst$" -o reddit_{month}.torrent reddit')

		discord_logging.flush_discord()

		# for stage, status in type_stages.items():
		# 	log.info(f"{file_type} {stage}: {status}")
	except Exception as err:
		queue.put((file_type, "error", str(err)))
		discord_logging.flush_discord()
		# for stage, status in type_stages.items():
		# 	log.info(f"{file_type} {stage}: {status}")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="")
	parser.add_argument('month', help='Month to process')
	parser.add_argument('folder', help='Folder under which all the files are stored')
	parser.add_argument("--ignore_ids", help="Ignore ids between the id ranges listed", default=None)
	parser.add_argument("--level", help="The compression ratio to output at", default="22")
	args = parser.parse_args()

	ignore_ids = []
	if args.ignore_ids is not None:
		for id_range in args.ignore_ids.split(","):
			start_id, end_id = id_range.split("-")
			ignore_ids.append((utils.base36decode(start_id), utils.base36decode(end_id)))

	discord_logging.init_discord_logging(
		section_name="Watchful12",
		log_level=logging.WARNING,
	)
	log.warning("test")
	discord_logging.flush_discord()

	status_file = "process.json"
	stages, month = load_status(status_file)

	if month is not None and args.month != month:
		log.error(f"Month does not match saved month, aborting: {month} : {args.month}")
		sys.exit(0)
	month = args.month
	log.info(f"Processing {month}")
	level = int(args.level)
	log.info(f"Compression level: {level}")

	multiprocessing.set_start_method('spawn')
	queue = multiprocessing.Manager().Queue()
	p_lock = multiprocessing.Lock()
	with multiprocessing.Pool(processes=2, initializer=init, initargs=(p_lock,)) as pool:
		arguments = []
		for file_type, type_stages in stages.items():
			arguments.append((queue, args.folder, month, file_type, type_stages, "Watchful12", level, ignore_ids))
		workers = pool.starmap_async(process, arguments, chunksize=1, error_callback=log.info)
		while not workers.ready() or not queue.empty():
			file_type, stage, status = queue.get()
			if stage == "error":
				log.error(f"Error in {file_type}: {status}")
			stages[file_type][stage] = status
			save_status(status_file, stages, month)
			discord_logging.flush_discord()
			#log.info(f"workers {workers.ready()} : queue {queue.empty()}")
