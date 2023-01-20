import zstandard
import os
import json
import sys
import time
import argparse
import re
from collections import defaultdict
from datetime import datetime
import logging.handlers
import multiprocessing


# sets up logging to the console as well as a file
log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')

log_stderr_handler = logging.StreamHandler()
log_stderr_handler.setFormatter(log_formatter)
log.addHandler(log_stderr_handler)
if not os.path.exists("logs"):
	os.makedirs("logs")
log_file_handler = logging.handlers.RotatingFileHandler(
	os.path.join("logs", "bot.log"), maxBytes=1024*1024*16, backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)


# convenience object used to pass status information between processes
class FileConfig:
	def __init__(self, input_path, output_path=None, complete=False, lines_processed=0, error_lines=0):
		self.input_path = input_path
		self.output_path = output_path
		self.file_size = os.stat(input_path).st_size
		self.complete = complete
		self.bytes_processed = self.file_size if complete else 0
		self.lines_processed = lines_processed if complete else 0
		self.error_message = None
		self.error_lines = error_lines

	def __str__(self):
		return f"{self.input_path} : {self.output_path} : {self.file_size} : {self.complete} : {self.bytes_processed} : {self.lines_processed}"


# used for calculating running average of read speed
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
def save_file_list(input_files, working_folder, status_json, script_type):
	if not os.path.exists(working_folder):
		os.makedirs(working_folder)
	simple_file_list = []
	for file in input_files:
		simple_file_list.append([file.input_path, file.output_path, file.complete, file.lines_processed, file.error_lines])
	with open(status_json, 'w') as status_json_file:
		output_dict = {
			"files": simple_file_list,
			"type": script_type,
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
					FileConfig(simple_file[0], simple_file[1], simple_file[2], simple_file[3], simple_file[4])
				)
			return input_files, output_dict["type"]
	else:
		return None, None


# recursively decompress and decode a chunk of bytes. If there's a decode error then read another chunk and try with that, up to a limit of max_window_size bytes
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
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


# open a zst compressed ndjson file and yield lines one at a time
# also passes back file progress
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
				yield line, file_handle.tell()

			buffer = lines[-1]
		reader.close()


# base of each separate process. Loads a file, iterates through lines and writes out
# the ones where the `field` of the object matches `value`. Also passes status
# information back to the parent via a queue
def process_file(file, queue, field):
	output_file = None
	log.debug(f"Starting file: {file.input_path} : {file.file_size:,}")
	try:
		for line, file_bytes_processed in read_lines_zst(file.input_path):
			try:
				obj = json.loads(line)
				observed = obj[field].lower()
				if output_file is None:
					output_file = open(file.output_path, 'w', encoding="utf-8")
				output_file.write(observed)
				output_file.write("\n")
			except (KeyError, json.JSONDecodeError) as err:
				file.error_lines += 1
			file.lines_processed += 1
			if file.lines_processed % 1000000 == 0:
				file.bytes_processed = file_bytes_processed
				queue.put(file)

		if output_file is not None:
			output_file.close()

		file.complete = True
		file.bytes_processed = file.file_size
		log.debug(f"Finished file: {file.input_path} : {file.file_size:,}")
	except Exception as err:
		file.error_message = str(err)
	queue.put(file)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Use multiple processes to decompress and iterate over pushshift dump files")
	parser.add_argument("input", help="The input folder to recursively read files from")
	parser.add_argument("--output", help="Name of the output file", default="field_counts")
	parser.add_argument("--working", help="The folder to store temporary files in", default="pushshift_working")
	parser.add_argument("--field", help="Which field to count", default="subreddit")
	parser.add_argument("--min_count", help="Dont write any counts below this number", default=1000)
	parser.add_argument("--processes", help="Number of processes to use", default=10, type=int)
	parser.add_argument("--file_filter", help="Regex filenames have to match to be processed", default="^rc_|rs_")
	parser.add_argument(
		"--error_rate", help=
		"Percentage as an integer from 0 to 100 of the lines where the field can be missing. For the subreddit field especially, "
		"there are a number of posts that simply don't have a subreddit attached", default=1, type=int)
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
	script_type = "count"

	args = parser.parse_args()

	if args.debug:
		log.setLevel(logging.DEBUG)

	log.info(f"Loading files from: {args.input}")
	if args.output:
		log.info(f"Writing output to: {args.output}")
	else:
		log.info(f"Writing output to working folder")

	multiprocessing.set_start_method('spawn')
	queue = multiprocessing.Manager().Queue()
	status_json = os.path.join(args.working, "status.json")
	input_files, saved_type = load_file_list(status_json)

	if saved_type and saved_type != script_type:
		log.warning(f"Script type doesn't match type from json file. Delete working folder")
		sys.exit(0)

	# if the file list wasn't loaded from the json, this is the first run, find what files we need to process
	if input_files is None:
		input_files = []
		for subdir, dirs, files in os.walk(args.input):
			files.sort()
			for file_name in files:
				if file_name.endswith(".zst") and re.search(args.file_filter, file_name, re.IGNORECASE) is not None:
					input_path = os.path.join(subdir, file_name)
					output_path = os.path.join(args.working, file_name[:-4])
					input_files.append(FileConfig(input_path, output_path=output_path))

		save_file_list(input_files, args.working, status_json, script_type)
	else:
		log.info(f"Existing input file was read, if this is not correct you should delete the {args.working} folder and run this script again")

	files_processed = 0
	total_bytes = 0
	total_bytes_processed = 0
	total_lines_processed = 0
	total_lines_errored = 0
	files_to_process = []
	# calculate the total file size for progress reports, build a list of incomplete files to process
	# do this largest to smallest by file size so that we aren't processing a few really big files with only a few threads at the end
	for file in sorted(input_files, key=lambda item: item.file_size, reverse=True):
		total_bytes += file.file_size
		if file.complete:
			files_processed += 1
			total_lines_processed += file.lines_processed
			total_bytes_processed += file.file_size
			total_lines_errored += file.error_lines
		else:
			files_to_process.append(file)

	log.info(f"Processed {files_processed} of {len(input_files)} files with {(total_bytes_processed / (2**30)):.2f} of {(total_bytes / (2**30)):.2f} gigabytes")

	start_time = time.time()
	if len(files_to_process):
		progress_queue = Queue(40)
		progress_queue.put([start_time, total_lines_processed, total_bytes_processed])
		speed_queue = Queue(40)
		for file in files_to_process:
			log.info(f"Processing file: {file.input_path}")
		# start the workers
		with multiprocessing.Pool(processes=min(args.processes, len(files_to_process))) as pool:
			workers = pool.starmap_async(process_file, [(file, queue, args.field) for file in files_to_process], error_callback=log.info)
			while not workers.ready():
				# loop until the workers are all done, pulling in status messages as they are sent
				file_update = queue.get()
				if file_update.error_message is not None:
					log.warning(f"File failed {file_update.input_path}: {file_update.error_message}")
				# I'm going to assume that the list of files is short enough that it's no
				# big deal to just iterate each time since that saves a bunch of work
				total_lines_processed = 0
				total_bytes_processed = 0
				total_lines_errored = 0
				files_processed = 0
				files_errored = 0
				i = 0
				for file in input_files:
					if file.input_path == file_update.input_path:
						input_files[i] = file_update
						file = file_update
					total_lines_processed += file.lines_processed
					total_bytes_processed += file.bytes_processed
					total_lines_errored += file.error_lines
					files_processed += 1 if file.complete or file.error_message is not None else 0
					files_errored += 1 if file.error_message is not None else 0
					i += 1
				if file_update.complete or file_update.error_message is not None:
					save_file_list(input_files, args.working, status_json, script_type)
				current_time = time.time()
				progress_queue.put([current_time, total_lines_processed, total_bytes_processed])

				first_time, first_lines, first_bytes = progress_queue.peek()
				bytes_per_second = int((total_bytes_processed - first_bytes)/(current_time - first_time))
				speed_queue.put(bytes_per_second)
				seconds_left = int((total_bytes - total_bytes_processed) / int(sum(speed_queue.list) / len(speed_queue.list)))
				minutes_left = int(seconds_left / 60)
				hours_left = int(minutes_left / 60)
				days_left = int(hours_left / 24)

				log.info(
					f"{total_lines_processed:,} lines at {(total_lines_processed - first_lines)/(current_time - first_time):,.0f}/s, {total_lines_errored:,} errored : "
					f"{(total_bytes_processed / (2**30)):.2f} gb at {(bytes_per_second / (2**20)):,.0f} mb/s, {(total_bytes_processed / total_bytes) * 100:.0f}% : "
					f"{files_processed}({files_errored})/{len(input_files)} files : "
					f"{(str(days_left) + 'd ' if days_left > 0 else '')}{hours_left - (days_left * 24)}:{minutes_left - (hours_left * 60):02}:{seconds_left - (minutes_left * 60):02} remaining")

	log.info(f"{total_lines_processed:,}, {total_lines_errored} errored : {(total_bytes_processed / (2**30)):.2f} gb, {(total_bytes_processed / total_bytes) * 100:.0f}% : {files_processed}/{len(input_files)}")

	working_file_paths = []
	count_incomplete = 0
	# build a list of output files to combine
	for file in sorted(input_files, key=lambda item: os.path.split(item.output_path)[1]):
		if not file.complete:
			if file.error_message is not None:
				log.info(f"File {file.input_path} errored {file.error_message}")
			else:
				log.info(f"File {file.input_path} is not marked as complete")
			count_incomplete += 1
		else:
			if file.error_lines > file.lines_processed * (args.error_rate * 0.01):
				log.info(
					f"File {file.input_path} has {file.error_lines:,} errored lines out of {file.lines_processed:,}, "
					f"{(file.error_lines / file.lines_processed) * (args.error_rate * 0.01):.2f}% which is above the limit of {args.error_rate}%")
				count_incomplete += 1
			elif file.output_path is not None:
				if os.path.exists(file.output_path):
					working_file_paths.append(file.output_path)

	if count_incomplete > 0:
		log.info(f"{count_incomplete} files were not completed, errored or don't exist, something went wrong. Aborting")
		sys.exit()

	log.info(f"Processing complete, combining {len(working_file_paths)} result files")

	input_lines = 0
	files_combined = 0
	field_counts = defaultdict(int)
	for working_file_path in working_file_paths:
		files_combined += 1
		log.info(f"Reading {files_combined}/{len(working_file_paths)} : {input_lines:,} : {len(field_counts):,} : {os.path.split(working_file_path)[1]}")
		with open(working_file_path, 'r') as input_file:
			for line in input_file:
				input_lines += 1
				field_counts[line.strip()] += 1

	output_counts = 0
	with open(f"{args.output}.txt", 'w') as output_handle:
		for field, count in field_counts.items():
			if count >= args.min_count:
				output_counts += 1
				output_handle.write(f"{field}	{count}\n")

	log.info(f"Finished combining files, {input_lines:,} lines read, {output_counts:,} field counts written")
