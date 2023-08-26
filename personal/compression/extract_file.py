import utils
import discord_logging
import os
import sys
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	input_file_path = r"\\MYCLOUDPR4100\Public\reddit_final\curiousdrive_submissions.zst"
	output_file_path = r"\\MYCLOUDPR4100\Public\reddit_final\curiousdrive_submissions.txt"
	file_size = os.stat(input_file_path).st_size

	file_lines = 0
	file_bytes_processed = 0
	created = None
	inserts = []
	output_file = open(output_file_path, 'w')
	for obj, line, file_bytes_processed in utils.read_obj_zst_meta(input_file_path):
		created = datetime.utcfromtimestamp(int(obj['created_utc']))
		file_lines += 1
		output_file.write(line)
		output_file.write("\n")
		if file_lines % 100000 == 0:
			log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")

	log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : 100%")
	output_file.close()

