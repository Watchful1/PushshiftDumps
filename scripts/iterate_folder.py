# this is an example of iterating over all zst files in a single folder,
# decompressing them and reading the created_utc field to make sure the files
# are intact. It has no output other than the number of lines

import zstandard
import os
import json
import sys
from datetime import datetime
import logging.handlers


log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


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


input_folder = sys.argv[1]
input_files = []
total_size = 0
for subdir, dirs, files in os.walk(input_folder):
	for filename in files:
		input_path = os.path.join(subdir, filename)
		if input_path.endswith(".zst"):
			file_size = os.stat(input_path).st_size
			total_size += file_size
			input_files.append([input_path, file_size])

log.info(f"Processing {len(input_files)} files of {(total_size / (2**30)):.2f} gigabytes")

total_lines = 0
total_bytes_processed = 0
for input_file in input_files:
	file_lines = 0
	file_bytes_processed = 0
	created = None
	for line, file_bytes_processed in read_lines_zst(input_file[0]):
		obj = json.loads(line)
		created = datetime.utcfromtimestamp(int(obj['created_utc']))
		file_lines += 1
		if file_lines == 1:
			log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines + total_lines:,} : 0% : {(total_bytes_processed / total_size) * 100:.0f}%")
		if file_lines % 100000 == 0:
			log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines + total_lines:,} : {(file_bytes_processed / input_file[1]) * 100:.0f}% : {(total_bytes_processed / total_size) * 100:.0f}%")
	total_lines += file_lines
	total_bytes_processed += input_file[1]
	log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {total_lines:,} : 100% : {(total_bytes_processed / total_size) * 100:.0f}%")

log.info(f"Total: {total_lines}")
