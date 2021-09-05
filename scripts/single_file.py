import zstandard
import os
import json
import sys
from datetime import datetime
import logging.handlers


log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = reader.read(2**27).decode()
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line, file_handle.tell()

			buffer = lines[-1]
		reader.close()


if __name__ == "__main__":
	file_path = r"\\MYCLOUDPR4100\Public\reddit\submissions\RS_2013-03.zst"
	file_size = os.stat(file_path).st_size
	file_lines = 0
	file_bytes_processed = 0
	created = None
	field = "subreddit"
	value = "wallstreetbets"
	bad_lines = 0
	try:
		for line, file_bytes_processed in read_lines_zst(file_path):
			try:
				obj = json.loads(line)
				created = datetime.utcfromtimestamp(int(obj['created_utc']))
				temp = obj[field] == value
			except (KeyError, json.JSONDecodeError) as err:
				bad_lines += 1
			file_lines += 1
			if file_lines % 100000 == 0:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")
	except Exception as err:
		log.info(err)

	log.info(f"Complete : {file_lines:,} : {bad_lines:,}")

