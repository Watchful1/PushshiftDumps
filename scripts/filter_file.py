# this is an example

import zstandard
import os
import json
import sys
import csv
from datetime import datetime
import logging.handlers

# put the path to the input file
input_file = r"\\MYCLOUDPR4100\Public\reddit\subreddits\redditdev_comments.zst"
# put the name or path to the output file. The file extension from below will be added automatically
output_file = r"\\MYCLOUDPR4100\Public\output"
# the format to output in, pick from the following options
#   zst: same as the input, a zstandard compressed ndjson file. Can be read by the other scripts in the repo
#   txt: an ndjson file, which is a text file with a separate json object on each line. Can be opened by any text editor
#   csv: a comma separated value file. Can be opened by a text editor or excel
# WARNING READ THIS: if you use txt or csv output on a large input file without filtering out most of the rows, the resulting file will be extremely large. Usually about 7 times as large as the compressed input file
output_format = "csv"
# the fields in the file are different depending on whether it has comments or submissions. If we're writing a csv, we need to know which fields to write.
# The filename from the torrent has which type it is, but you'll need to change this if you removed that from the filename
is_submission = "submission" in input_file

# only output items between these two dates
from_date = datetime.strptime("2005-01-01", "%Y-%m-%d")
to_date = datetime.strptime("2025-01-01", "%Y-%m-%d")

# the field to filter on, the values to filter with and whether it should be an exact match
# some examples:
#
# return only objects where the author is u/watchful1 or u/spez
# field = "author"
# values = ["watchful1","spez"]
# exact_match = True
#
# return only objects where the title contains either "stonk" or "moon"
# field = "title"
# values = ["stonk","moon"]
# exact_match = False
#
# return only objects where the body contains either "stonk" or "moon". For submissions the body is in the "selftext" field, for comments it's in the "body" field
# field = "selftext"
# values = ["stonk","moon"]
# exact_match = False

field = "author"
values = ["watchful1","spez"]
exact_match = True


# sets up logging to the console as well as a file
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_str_handler = logging.StreamHandler()
log_str_handler.setFormatter(log_formatter)
log.addHandler(log_str_handler)
if not os.path.exists("logs"):
	os.makedirs("logs")
log_file_handler = logging.handlers.RotatingFileHandler(os.path.join("logs", "bot.log"), maxBytes=1024*1024*16, backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)


def write_line_zst(handle, line):
	handle.write(line.encode('utf-8'))
	handle.write("\n".encode('utf-8'))


def write_line_json(handle, obj):
	handle.write(json.dumps(obj))
	handle.write("\n")


def write_line_csv(writer, obj, is_submission):
	output_list = []
	output_list.append(str(obj['score']))
	output_list.append(datetime.fromtimestamp(obj['created_utc']).strftime("%Y-%m-%d"))
	if is_submission:
		output_list.append(obj['title'])
	output_list.append(f"u/{obj['author']}")
	output_list.append(f"https://www.reddit.com{obj['permalink']}")
	if is_submission:
		if obj['is_self']:
			if 'selftext' in obj:
				output_list.append(obj['selftext'])
			else:
				output_list.append("")
		else:
			output_list.append(obj['url'])
	else:
		output_list.append(obj['body'])
	writer.writerow(output_list)


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
	output_path = f"{output_file}.{output_format}"

	writer = None
	if output_format == "zst":
		handle = zstandard.ZstdCompressor().stream_writer(open(output_path, 'wb'))
	elif output_format == "txt":
		handle = open(output_path, 'w', encoding='UTF-8')
	elif output_format == "csv":
		handle = open(output_path, 'w', encoding='UTF-8', newline='')
		writer = csv.writer(handle)
	else:
		log.error(f"Unsupported output format {output_format}")
		sys.exit()

	values = [value.lower() for value in values]  # convert to lowercase

	file_size = os.stat(input_file).st_size
	file_bytes_processed = 0
	created = None
	matched_lines = 0
	bad_lines = 0
	total_lines = 0
	for line, file_bytes_processed in read_lines_zst(input_file):
		total_lines += 1
		if total_lines % 100000 == 0:
			log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {total_lines:,} : {matched_lines:,} : {bad_lines:,} : {file_bytes_processed:,}:{(file_bytes_processed / file_size) * 100:.0f}%")

		try:
			obj = json.loads(line)
			created = datetime.utcfromtimestamp(int(obj['created_utc']))

			if created < from_date:
				continue
			if created > to_date:
				continue

			field_value = obj[field].lower()
			matched = False
			for value in values:
				if exact_match:
					if value == field_value:
						matched = True
						break
				else:
					if value in field_value:
						matched = True
						break
			if not matched:
				continue

			matched_lines += 1
			if output_format == "zst":
				write_line_zst(handle, line)
			elif output_format == "csv":
				write_line_csv(writer, obj, is_submission)
			elif output_format == "txt":
				write_line_json(handle, obj)
		except (KeyError, json.JSONDecodeError) as err:
			bad_lines += 1

	handle.close()
	log.info(f"Complete : {total_lines:,} : {matched_lines:,} : {bad_lines:,}")

