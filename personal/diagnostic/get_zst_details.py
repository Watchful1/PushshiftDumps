import zstandard
import os
import json
import sys
import time
import argparse
import re
from collections import defaultdict
import logging.handlers
import multiprocessing
import utils


# sets up logging to the console as well as a file
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')

log_str_handler = logging.StreamHandler()
log_str_handler.setFormatter(log_formatter)
log.addHandler(log_str_handler)
if not os.path.exists("logs"):
	os.makedirs("logs")
log_file_handler = logging.handlers.RotatingFileHandler(
	os.path.join("logs", "bot.log"), maxBytes=1024*1024*16, backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += len(chunk)
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode(), bytes_read
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def count_lines_bytes(file_name):
	count_lines = 0
	uncompressed_bytes = 0
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)

		while True:
			chunk, chunk_bytes = read_and_decode(reader, 2**27, (2**29) * 2)
			uncompressed_bytes += chunk_bytes
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")
			count_lines += len(lines) - 1

			buffer = lines[-1]
		reader.close()
	return count_lines, uncompressed_bytes


if __name__ == '__main__':
	input_path = r"\\MYCLOUDPR4100\Public\reddit\comments\RC_2008-03.zst"
	compressed_size = os.stat(input_path).st_size
	count_lines, uncompressed_bytes = count_lines_bytes(input_path)
	log.info(f"Compressed size: {compressed_size:,} : {(compressed_size / (2**30)):.2f} gb")
	log.info(f"Uncompressed size: {uncompressed_bytes:,} : {(uncompressed_bytes / (2**30)):.2f} gb")
	log.info(f"Ratio: {(uncompressed_bytes / compressed_size):.2f}")
	log.info(f"Lines: {count_lines:,}")
