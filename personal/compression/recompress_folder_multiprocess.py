# this script iterates through zst compressed ndjson files, like the pushshift reddit dumps, loads each line
# and if it matches the criteria in the command line arguments, it's written out into a separate file for
# that month. After all the ndjson files are processed, it iterates through the resulting files and combines
# them into a final file.

# this script assumes the files are named in chronological order and prefixed with RS_ or RC_, like the pushshift dumps

# features:
#  - multiple processes in parallel to maximize drive read and decompression
#  - saves state as it completes each file and picks up where it stopped
#  - detailed progress indicators

# examples:
#  - get all comments that have a subreddit field (subreddit is the default) of "wallstreetbets". This will create a single output file "wallstreetbets_comments.zst" in the folder the script is run in
#    python3 combine_folder_multiprocess.py reddit/comments --value wallstreetbets
#  - get all comments and submissions (assuming both types of dump files are under the reddit folder) that have an author field of Watchful1 or spez and output the results to a folder called pushshift.
#    This will result in four files, pushshift/Watchful1_comments, pushshift/Watchful1_submissions, pushshift/spez_comments, pushshift/spez_submissions
#    python3 combine_folder_multiprocess.py reddit --field author --value Watchful1,spez --output pushshift

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
from enum import Enum


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


# convenience object used to pass status information between processes
class FileConfig:
	def __init__(self, input_path, output_path, complete=False, uncompressed_size=None, new_compressed_size=None, total_lines=None):
		self.input_path = input_path
		self.output_path = output_path
		self.complete = complete
		self.error_message = None

		self.old_compressed_size = os.stat(input_path).st_size
		self.uncompressed_size = uncompressed_size
		self.new_compressed_size = new_compressed_size

		self.total_lines = total_lines

	def __str__(self):
		return f"{self.input_path} : {self.output_path} : {self.complete} : {self.old_compressed_size} - {self.uncompressed_size} - {self.new_compressed_size}"


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


class Queue:
	def __init__(self, max_size):
		self.list = []
		self.max_size = max_size

	def put(self, item):
		if len(self.list) >= self.max_size:
			self.list.pop(0)
		self.list.append(item)

	def peek(self):
		return self.list[0] if len(self.list) > 0 else None


# save file information and progress to a json file
# we don't want to save the whole FileConfig object, since some info resets if we restart
def save_file_list(input_files, working_folder, status_json, arg_string, script_type):
	if not os.path.exists(working_folder):
		os.makedirs(working_folder)
	simple_file_list = []
	for file in input_files:
		simple_file_list.append([file.input_path, file.output_path, file.complete, file.uncompressed_size, file.new_compressed_size, file.total_lines])
	with open(status_json, 'w') as status_json_file:
		output_dict = {
			"args": arg_string,
			"type": script_type,
			"files": simple_file_list,
		}
		status_json_file.write(json.dumps(output_dict, indent=4))


# load file information from the json file and recalculate file sizes
def load_file_list(status_json):
	if os.path.exists(status_json):
		with open(status_json, 'r') as status_json_file:
			output_dict = json.load(status_json_file)
			input_files = []
			for simple_file in output_dict["files"]:
				input_files.append(
					FileConfig(simple_file[0], simple_file[1], simple_file[2], simple_file[3], simple_file[4], simple_file[5])
				)
			return input_files, output_dict["args"], output_dict["type"]
	else:
		return None, None, None


# base of each separate process. Loads a file, iterates through lines and writes out
# the ones where the `field` of the object matches `value`. Also passes status
# information back to the parent via a queue
def process_file(file, queue, threads, level):
	queue.put(file)
	file.total_lines, file.uncompressed_size = count_lines_bytes(file.input_path)
	queue.put(file)

	decompressor = zstandard.ZstdDecompressor(max_window_size=2**31)
	compressor = zstandard.ZstdCompressor(level=level, write_content_size=True, write_checksum=True, threads=threads)
	with open(file.input_path, 'rb') as input_handle, open(file.output_path, "wb") as output_handle:
		compression_reader = decompressor.stream_reader(input_handle)
		read_count, file.new_compressed_size = compressor.copy_stream(compression_reader, output_handle, size=file.uncompressed_size)
	#log.info(f"{read_count:,} to {write_count:,} in {seconds:,.2f} with {threads} threads")
	file.complete = True
	queue.put(file)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Use multiple processes to recompress zst files in a folder")
	parser.add_argument("input", help="The input folder to read files from")
	parser.add_argument("output", help="Put the output files in this folder")
	parser.add_argument("--level", help="The compression ratio to output at. From 0 to 22", default=22, type=int)
	parser.add_argument("--working", help="The folder to store temporary files in", default="pushshift_working")
	parser.add_argument("--processes", help="Number of processes to use", default=4, type=int)
	parser.add_argument("--threads", help="Number of threads per process", default=1, type=int)
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
	script_type = "compress"

	args = parser.parse_args()
	arg_string = f"{args.input}:{args.output}:{args.level}"

	if args.debug:
		log.setLevel(logging.DEBUG)

	log.info(f"Loading files from: {args.input}")
	log.info(f"Writing output to: {args.output}")

	multiprocessing.set_start_method('spawn')
	queue = multiprocessing.Manager().Queue()
	status_json = os.path.join(args.working, "status.json")
	input_files, saved_arg_string, saved_type = load_file_list(status_json)
	if saved_arg_string and saved_arg_string != arg_string:
		log.warning(f"Args don't match args from json file. Delete working folder")
		sys.exit(0)

	if saved_type and saved_type != script_type:
		log.warning(f"Script type doesn't match type from json file. Delete working folder")
		sys.exit(0)

	# if the file list wasn't loaded from the json, this is the first run, find what files we need to process
	if input_files is None:
		input_files = []
		for file_name in os.listdir(args.input):
			input_path = os.path.join(args.input, file_name)
			if os.path.isfile(input_path) and file_name.endswith(".zst"):
				output_path = os.path.join(args.output, file_name)
				input_files.append(FileConfig(input_path, output_path=output_path))

		save_file_list(input_files, args.working, status_json, arg_string, script_type)
	else:
		log.info(f"Existing input file was read, if this is not correct you should delete the {args.working} folder and run this script again")

	files_processed, total_old_bytes, processed_old_bytes, processed_uncompressed_bytes, processed_new_bytes, processed_lines = 0, 0, 0, 0, 0, 0
	files_to_process = []
	# calculate the total file size for progress reports, build a list of incomplete files to process
	# do this largest to smallest by file size so that we aren't processing a few really big files with only a few threads at the end
	for file in sorted(input_files, key=lambda item: item.old_compressed_size, reverse=True):
		total_old_bytes += file.old_compressed_size
		if file.complete:
			files_processed += 1
			processed_old_bytes += file.old_compressed_size
			processed_uncompressed_bytes += file.uncompressed_size
			processed_new_bytes += file.new_compressed_size
			processed_lines += file.total_lines
		else:
			files_to_process.append(file)

	log.info(f"Processed {files_processed} of {len(input_files)} files with {(processed_old_bytes / (2**30)):.2f} of {(total_old_bytes / (2**30)):.2f} gigabytes")

	start_time = time.time()
	if len(files_to_process):
		progress_queue = Queue(40)
		progress_queue.put([start_time, processed_old_bytes])
		speed_queue = Queue(40)
		# start the workers
		with multiprocessing.Pool(processes=min(args.processes, len(files_to_process))) as pool:
			workers = pool.starmap_async(process_file, [(file, queue, args.threads, args.level) for file in files_to_process], chunksize=1, error_callback=log.info)
			while not workers.ready() or not queue.empty():
				# loop until the workers are all done, pulling in status messages as they are sent
				file_update = queue.get()
				if file_update.error_message is not None:
					log.warning(f"File failed {file_update.input_path}: {file_update.error_message}")

				# this is the workers telling us they are starting a new file, print the debug message but nothing else
				if not file_update.complete:
					if file_update.uncompressed_size is not None:
						log.debug(f"Calculated uncompressed size: {file_update.input_path} : {file_update.uncompressed_size:,}")
					else:
						log.debug(f"Starting file: {file_update.input_path} : {file_update.old_compressed_size:,}")
					continue

				# I'm going to assume that the list of files is short enough that it's no
				# big deal to just iterate each time since that saves a bunch of work
				files_processed, processed_old_bytes, processed_uncompressed_bytes, processed_new_bytes, processed_lines, files_errored, i = 0, 0, 0, 0, 0, 0, 0
				for file in input_files:
					if file.input_path == file_update.input_path:
						input_files[i] = file_update
						file = file_update
					processed_old_bytes += file.old_compressed_size
					processed_uncompressed_bytes += file.uncompressed_size
					processed_new_bytes += file.new_compressed_size
					processed_lines += file.total_lines
					files_processed += 1 if file.complete or file.error_message is not None else 0
					files_errored += 1 if file.error_message is not None else 0
					i += 1
				if file_update.complete or file_update.error_message is not None:
					save_file_list(input_files, args.working, status_json, arg_string, script_type)
					log.debug(f"Finished file: {file_update.input_path} : {file_update.file_size:,}")
				current_time = time.time()
				progress_queue.put([current_time, processed_old_bytes])

				first_time, first_bytes = progress_queue.peek()
				bytes_per_second = int((processed_old_bytes - first_bytes)/(current_time - first_time))
				speed_queue.put(bytes_per_second)
				seconds_left = int((total_old_bytes - processed_old_bytes) / int(sum(speed_queue.list) / len(speed_queue.list)))
				minutes_left = int(seconds_left / 60)
				hours_left = int(minutes_left / 60)
				days_left = int(hours_left / 24)

				log.info(
					f"{(processed_old_bytes / (2**30)):.2f} gb at {(bytes_per_second / (2**20)):,.0f} mb/s, {(processed_old_bytes / total_old_bytes) * 100:.0f}% : "
					f"{(processed_uncompressed_bytes / (2**30)):.2f} gb uncompressed to {(processed_new_bytes / (2**30)):.2f} gb : "
					f"{(processed_old_bytes / processed_uncompressed_bytes)} old ratio : {(processed_new_bytes / processed_uncompressed_bytes)} new ratio : {(processed_new_bytes / processed_old_bytes)} difference : "
					f"{files_processed}({files_errored})/{len(input_files)} files : "
					f"{(str(days_left) + 'd ' if days_left > 0 else '')}{hours_left - (days_left * 24)}:{minutes_left - (hours_left * 60):02}:{seconds_left - (minutes_left * 60):02} remaining")

	log.info(f"{(processed_old_bytes / (2**30)):.2f} gb, {(processed_old_bytes / total_old_bytes) * 100:.0f}% : {files_processed}/{len(input_files)}")
