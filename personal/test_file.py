import utils
import discord_logging
import os
import sys
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	input_path = r"\\MYCLOUDPR4100\Public\reddit\subreddits\NoStupidQuestions_comments.zst"

	input_file_paths = []
	if os.path.isdir(input_path):
		for subdir, dirs, files in os.walk(input_path):
			files.sort()
			for file_name in files:
				if file_name.endswith(".zst"):
					input_file_paths.append(os.path.join(subdir, file_name))
	else:
		input_file_paths.append(input_path)

	files_processed = 0
	for file_path in input_file_paths:
		file_name = os.path.basename(file_path)
		file_size = os.stat(file_path).st_size
		file_lines = 0
		file_bytes_processed = 0
		created = None
		previous_timestamp = None
		inserts = []
		for obj, line, file_bytes_processed in utils.read_obj_zst_meta(file_path):
			new_timestamp = int(obj['created_utc'])
			created = datetime.utcfromtimestamp(new_timestamp)
			if previous_timestamp is not None and previous_timestamp - (60 * 60 * 4) > new_timestamp:
				log.warning(f"Out of order timestamps {datetime.utcfromtimestamp(previous_timestamp).strftime('%Y-%m-%d %H:%M:%S')} - 4 hours > {created.strftime('%Y-%m-%d %H:%M:%S')}")
			previous_timestamp = new_timestamp
			file_lines += 1
			if file_lines % 10000 == 0:
				log.info(f"{files_processed}/{len(input_file_paths)}: {file_name} : {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")

		log.info(f"{files_processed}/{len(input_file_paths)}: {file_name} : {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : 100%")
