import sys
sys.path.append('personal')
sys.path.append('combine')
sys.path.append('personal/combine')

import os
import shutil
import argparse
import json
import logging.handlers
import praw
import traceback
from datetime import datetime, timedelta
import multiprocessing_logging

import discord_logging
import multiprocessing

log = discord_logging.init_logging()
discord_logging.init_discord_logging(
	section_name="Watchful12",
	log_level=logging.WARNING,
)
# multiprocessing_logging only installs under the fork start method. Python 3.14 defaults to
# forkserver on Linux (and the Pool below uses spawn anyway), so fall back to each process
# logging through its own handlers, which is what the spawned workers already did.
try:
	multiprocessing_logging.install_mp_handler(log)
except AssertionError:
	log.info("Not installing the multiprocessing log handler: start method is not fork")

import utils
from transform import split_blocks_by_minutes
from combine.merge_and_backfill import build_day, fetch_pushshift_token, PushshiftTokenFetcher, IngestType, ObjectType
from combine import build_month


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
				"verify": False,
				"cleanup": False,
			},
			"submission": {
				"split": False,
				"merge": None,  # 24-02-01
				"build": False,
				"verify": False,
				"cleanup": False,
			}
		}
		return stages, None


def end_of_day(input_minute):
	return input_minute.replace(hour=0, minute=0, second=0) + timedelta(days=1)


def delete_minute_files(base_folder, month_start, file_type):
	"""Remove this month's per-minute download and combined day folders for one type."""
	count_folders = 0
	for day in build_month.month_days(month_start):
		for stage_folder in ("download", "combined"):
			day_folder = os.path.join(base_folder, "ingest", stage_folder, file_type + "s", day.strftime('%y-%m-%d'))
			if os.path.exists(day_folder):
				shutil.rmtree(day_folder)
				count_folders += 1
	return count_folders


def process(queue, base_folder, month, file_type, type_stages, reddit_username, compression_level, ignore_ids, token_host, token_path, threads, keep_minute_files):
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
			# the ingest on token_host is the only thing that refreshes the token; this only reads it
			token_fetcher = PushshiftTokenFetcher(token_host, token_path, pushshift_lock)

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
					token_fetcher
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
				compression_level,
				threads
			)
			queue.put((file_type, "build", True))
			log.warning(f"{file_type}: {file_type} build complete")
			discord_logging.flush_discord()

		# Keep the minute files until the month file is proven to hold exactly what they hold.
		# verify and cleanup are separate stages so a restart after a partial delete does not
		# try to re-verify against minute files that are already gone.
		if keep_minute_files:
			log.info(f"{file_type}: Keeping minute files, skipping verify and cleanup")
		else:
			start_date = datetime.strptime(month, "%y-%m")
			if not type_stages.get("verify", False):
				log.info(f"{file_type}: Starting {file_type} verify")
				ok, message = build_month.verify_month(
					start_date,
					os.path.join(base_folder, "ingest", "combined"),
					os.path.join(base_folder, "reddit"),
					file_type+"s"
				)
				if not ok:
					raise RuntimeError(f"Verify failed, keeping minute files: {message}")
				queue.put((file_type, "verify", True))
				log.warning(f"{file_type}: {file_type} verify complete: {message}")
				discord_logging.flush_discord()

			if not type_stages.get("cleanup", False):
				count_folders = delete_minute_files(base_folder, start_date, file_type)
				queue.put((file_type, "cleanup", True))
				log.warning(f"{file_type}: {file_type} cleanup complete: deleted {count_folders} day folders of download and combined minute files")
				discord_logging.flush_discord()

		log.warning(f"{file_type}: {file_type} all steps complete")

		log.info(f'torrenttools create -a "https://academictorrents.com/announce.php" -c "Reddit comments and submissions from 20{month}" --include ".*(comments|submissions).*R._20{month}.zst$" -o reddit_20{month}.torrent reddit')

		discord_logging.flush_discord()

		# for stage, status in type_stages.items():
		# 	log.info(f"{file_type} {stage}: {status}")
	except Exception as err:
		error_message = f"{type(err).__module__}.{type(err).__name__}: {err}"
		log.warning(f"Error in {file_type}: {error_message}")
		log.warning(traceback.format_exc())
		queue.put((file_type, "error", error_message))
		discord_logging.flush_discord()
		# for stage, status in type_stages.items():
		# 	log.info(f"{file_type} {stage}: {status}")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="")
	parser.add_argument('month', help='Month to process', nargs='?')
	parser.add_argument('folder', help='Folder under which all the files are stored', nargs='?')
	parser.add_argument("--ignore_ids", help="Ignore ids between the id ranges listed", default=None)
	parser.add_argument("--level", help="The compression ratio to output at", default="22")
	parser.add_argument("--threads", help="zstd worker threads for the build, -1 for one per core, 0 for single threaded", default=-1, type=int)
	parser.add_argument("--keep_minute_files", help="Do not verify the month files or delete the download and combined minute files afterwards", action='store_const', const=True, default=False)
	parser.add_argument("--token_host", help="ssh host that runs PushshiftIngest; the pushshift token is read from there and never refreshed here", required=True)
	parser.add_argument("--token_path", help="Path of the token file on token_host", default="PushshiftIngest/pushshift_token.txt")
	parser.add_argument("--check_token", help="Only fetch the token once over ssh, report ok or the error, and exit", action='store_const', const=True, default=False)
	args = parser.parse_args()

	if args.check_token:
		try:
			fetch_pushshift_token(args.token_host, args.token_path)
		except RuntimeError as err:
			print(f"token check failed: {err}")
			sys.exit(1)
		print(f"ok: fetched a token from {args.token_host}:{args.token_path}")
		sys.exit(0)
	if args.month is None or args.folder is None:
		parser.error("month and folder are required unless --check_token is given")

	ignore_ids = []
	if args.ignore_ids is not None:
		for id_range in args.ignore_ids.split(","):
			start_id, end_id = id_range.split("-")
			ignore_ids.append((utils.base36decode(start_id), utils.base36decode(end_id)))

	log.warning(f"Processing {args.month}")
	discord_logging.flush_discord()

	status_file = "process.json"
	stages, month = load_status(status_file)

	if month is not None and args.month != month:
		log.error(f"Month does not match saved month, aborting: {month} : {args.month}")
		sys.exit(0)
	month = args.month
	log.info(f"Processing {month}")
	level = int(args.level)
	log.info(f"Compression level: {level}, threads: {args.threads}, keep minute files: {args.keep_minute_files}")

	multiprocessing.set_start_method('spawn', force=True)
	queue = multiprocessing.Manager().Queue()
	p_lock = multiprocessing.Lock()
	with multiprocessing.Pool(processes=2, initializer=init, initargs=(p_lock,)) as pool:
		arguments = []
		for file_type, type_stages in stages.items():
			arguments.append((queue, args.folder, month, file_type, type_stages, "Watchful12", level, ignore_ids, args.token_host, args.token_path, args.threads, args.keep_minute_files))
		workers = pool.starmap_async(process, arguments, chunksize=1, error_callback=log.info)
		while not workers.ready() or not queue.empty():
			file_type, stage, status = queue.get()
			if stage == "error":
				log.error(f"Error in {file_type}: {status}")
			stages[file_type][stage] = status
			save_status(status_file, stages, month)
			discord_logging.flush_discord()
			#log.info(f"workers {workers.ready()} : queue {queue.empty()}")
	discord_logging.flush_discord()
