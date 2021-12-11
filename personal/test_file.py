import utils
import discord_logging
import os
import sys
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	file_path = r"\\MYCLOUDPR4100\Public\reddit\submissions\RS_2011-01.zst"
	file_size = os.stat(file_path).st_size

	file_lines = 0
	file_bytes_processed = 0
	created = None
	inserts = []
	for obj, line, file_bytes_processed in utils.read_obj_zst_meta(file_path):
		created = datetime.utcfromtimestamp(int(obj['created_utc']))
		file_lines += 1
		if file_lines % 100000 == 0:
			log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")

	log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : 100%")

