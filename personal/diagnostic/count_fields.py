import zstandard
import os
import json
import sys
from datetime import datetime
import logging.handlers
from collections import defaultdict


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
				yield json.loads(line)
			buffer = lines[-1]
		reader.close()


if __name__ == "__main__":
	#input_folder = r"\\MYCLOUDPR4100\Public\ingest\ingest\comments\23-06-23"
	input_folder = r"\\MYCLOUDPR4100\Public\reddit\comments"
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
	fields = defaultdict(lambda: defaultdict(int))
	for input_file in input_files:
		file_lines = 0
		created = None
		for obj in read_lines_zst(input_file[0]):
			for key, value in obj.items():
				value = str(value)[:20]
				fields[key][value] += 1

			created = datetime.utcfromtimestamp(int(obj['created_utc']))
			file_lines += 1
			if file_lines % 100000 == 0:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines + total_lines:,}")
			if file_lines >= 1000:
				break
		total_lines += file_lines
		log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines + total_lines:,}")

	sorted_fields = []
	for key, values in fields.items():
		total_occurrences = 0
		unique_values = 0
		examples = []
		for value_name, count in values.items():
			unique_values += 1
			total_occurrences += count
			if len(examples) < 3:
				examples.append(value_name)
		sorted_fields.append((total_occurrences, f"{key}: {(total_occurrences / total_lines) * 100:.2f} : {unique_values:,} : {','.join(examples)}"))
	sorted_fields.sort(key=lambda x:x[0], reverse=True)
	for count, string in sorted_fields:
		log.info(string)
