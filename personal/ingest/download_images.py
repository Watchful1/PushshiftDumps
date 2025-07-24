import zstandard
import os
import json
import sys
import csv
from datetime import datetime
import logging.handlers
import traceback
import praw
from praw import endpoints
import prawcore
import time

# put the path to the input file
input_file = r"\\MYCLOUDPR4100\Public\wallstreetbets_gainloss_rehydrate.zst"
# put the name or path to the output file. The file extension from below will be added automatically. If the input file is a folder, the output will be treated as a folder as well
output_folder = r"\\MYCLOUDPR4100\Public\wallstreetbets_gainloss_images"


# sets up logging to the console as well as a file
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_str_handler = logging.StreamHandler()
log_str_handler.setFormatter(log_formatter)
log.addHandler(log_str_handler)
if not os.path.exists("../combine/logs"):
	os.makedirs("../combine/logs")
log_file_handler = logging.handlers.RotatingFileHandler(os.path.join(
	"../combine/logs", "bot.log"), maxBytes=1024 * 1024 * 16, backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)


def query_reddit(ids, reddit, is_submission):
	id_prefix = 't3_' if is_submission else 't1_'
	id_string = f"{id_prefix}{(f',{id_prefix}'.join(ids))}"
	response = None
	for i in range(20):
		try:
			response = reddit.request(method="GET", path=endpoints.API_PATH["info"], params={"id": id_string})
			break
		except (prawcore.exceptions.ServerError, prawcore.exceptions.RequestException) as err:
			log.info(f"No response from reddit api for {is_submission}, sleeping {i * 5} seconds: {err} : {id_string}")
			time.sleep(i * 5)
	if response is None:
		log.warning(f"Reddit api failed, aborting")
		return []
	return response['data']['children']


def write_line_zst(handle, line):
	handle.write(line.encode('utf-8'))
	handle.write("\n".encode('utf-8'))


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)

			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				yield line.strip(), file_handle.tell()

			buffer = lines[-1]

		reader.close()


if __name__ == "__main__":
	log.info(f"Input: {input_file} : Output: {output_folder}")

	reddit = praw.Reddit("Watchful1BotTest")

	file_size = os.stat(input_file).st_size
	created = None
	found_lines = 0
	total_lines = 0
	for line, file_bytes_processed in read_lines_zst(input_file):
		total_lines += 1
		if total_lines < 100000:
			continue

		obj = json.loads(line)
		created = datetime.utcfromtimestamp(int(obj['created_utc']))

		url = obj["url"]
		if "i.redd.it" in url:
			log.info(url)
		elif "reddit.com/gallery" in url and "media_metadata" in obj and obj["media_metadata"] is not None:
			for media in obj["media_metadata"]:
				log.info(obj["media_metadata"][media]["s"]["u"])


		if total_lines > 100100:
			break

		# if total_lines % 10000 == 0:
		# 	log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {total_lines:,} : {found_lines:,} : {missing_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")


	# log.info(f"Complete : {total_lines:,} : {found_lines:,} : {missing_lines:,}")
