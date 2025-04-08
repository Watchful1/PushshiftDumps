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


class FileType(Enum):
	COMMENT = 1
	SUBMISSION = 2

	@staticmethod
	def to_str(file_type):
		if file_type == FileType.COMMENT:
			return "comments"
		elif file_type == FileType.SUBMISSION:
			return "submissions"
		return "other"


# convenience object used to pass status information between processes
class FileConfig:
	def __init__(self, input_path, output_path=None, complete=False, lines_processed=0, error_lines=0, lines_matched=0):
		self.input_path = input_path
		self.output_path = output_path
		self.file_size = os.stat(input_path).st_size
		self.complete = complete
		self.bytes_processed = self.file_size if complete else 0
		self.lines_processed = lines_processed if complete else 0
		self.error_message = None
		self.error_lines = error_lines
		self.lines_matched = lines_matched
		file_name = os.path.split(input_path)[1]
		if file_name.startswith("RS"):
			self.file_type = FileType.SUBMISSION
		elif file_name.startswith("RC"):
			self.file_type = FileType.COMMENT
		else:
			raise ValueError(f"Unknown working file type: {file_name}")

	def __str__(self):
		return f"{self.input_path} : {self.output_path} : {self.file_size} : {self.complete} : {self.bytes_processed} : {self.lines_processed}"


# another convenience object to read and write from both zst files and ndjson files
class FileHandle:
	newline_encoded = "\n".encode('utf-8')
	ext_len = len(".zst")

	def __init__(self, path, is_split=False):
		self.path = path
		self.is_split = is_split
		self.handles = {}

	def get_paths(self, character_filter=None):
		if self.is_split:
			paths = []
			for file in os.listdir(self.path):
				if not file.endswith(".zst"):
					continue
				if character_filter is not None and character_filter != file[-FileHandle.ext_len - 1:-FileHandle.ext_len]:
					continue
				paths.append(os.path.join(self.path, file))
			return paths
		else:
			return [self.path]

	def get_count_files(self):
		return len(self.get_paths())

	# recursively decompress and decode a chunk of bytes. If there's a decode error then read another chunk and try with that, up to a limit of max_window_size bytes
	@staticmethod
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
			return FileHandle.read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)

	# open a zst compressed ndjson file, or a regular uncompressed ndjson file and yield lines one at a time
	# also passes back file progress
	def yield_lines(self, character_filter=None):
		if self.is_split:
			if character_filter is not None:
				path = os.path.join(self.path, f"{character_filter}.zst")
			else:
				raise ValueError(f"{self.path} is split but no filter passed")
		else:
			path = self.path
		if os.path.exists(path):
			with open(path, 'rb') as file_handle:
				buffer = ''
				reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
				while True:
					chunk = FileHandle.read_and_decode(reader, 2**27, (2**29) * 2)
					if not chunk:
						break
					lines = (buffer + chunk).split("\n")

					for line in lines[:-1]:
						yield line, file_handle.tell()

					buffer = lines[-1]
				reader.close()

	# get either the main write handle or the character filter one, opening a new handle as needed
	def get_write_handle(self, character_filter=None):
		if character_filter is None:
			character_filter = 1  # use 1 as the default name since ints hash quickly
		handle = self.handles.get(character_filter)
		if handle is None:
			if character_filter == 1:
				path = self.path
			else:
				if not os.path.exists(self.path):
					os.makedirs(self.path)
				path = os.path.join(self.path, f"{character_filter}.zst")
			handle = zstandard.ZstdCompressor().stream_writer(open(path, 'wb'))
			self.handles[character_filter] = handle
		return handle

	# write a line, opening the appropriate handle
	def write_line(self, line, value=None):
		if self.is_split:
			if value is None:
				raise ValueError(f"{self.path} is split but no value passed")
			character_filter = value[:1]
			handle = self.get_write_handle(character_filter)
		else:
			handle = self.get_write_handle()

		handle.write(line.encode('utf-8'))
		handle.write(FileHandle.newline_encoded)

	def close(self):
		for handle in self.handles.values():
			handle.close()


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
def save_file_list(input_files, working_folder, status_json, arg_string, script_type, completed_prefixes=None):
	if not os.path.exists(working_folder):
		os.makedirs(working_folder)
	simple_file_list = []
	for file in input_files:
		simple_file_list.append([file.input_path, file.output_path, file.complete, file.lines_processed, file.error_lines, file.lines_matched])
	if completed_prefixes is None:
		completed_prefixes = []
	else:
		completed_prefixes = sorted([prefix for prefix in completed_prefixes])
	with open(status_json, 'w') as status_json_file:
		output_dict = {
			"args": arg_string,
			"type": script_type,
			"completed_prefixes": completed_prefixes,
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
			completed_prefixes = set()
			for prefix in output_dict["completed_prefixes"]:
				completed_prefixes.add(prefix)
			return input_files, output_dict["args"], output_dict["type"], completed_prefixes
	else:
		return None, None, None, set()


# base of each separate process. Loads a file, iterates through lines and writes out
# the ones where the `field` of the object matches `value`. Also passes status
# information back to the parent via a queue
def process_file(file, queue, field, values, partial, regex, split_intermediate):
	queue.put(file)
	input_handle = FileHandle(file.input_path)
	output_handle = FileHandle(file.output_path, is_split=split_intermediate)

	value = None
	if len(values) == 1:
		value = min(values)

	try:
		for line, file_bytes_processed in input_handle.yield_lines():
			try:
				obj = json.loads(line)
				matched = False
				observed = obj[field].lower()
				if regex:
					for reg in values:
						if reg.search(observed):
							matched = True
							break
				elif partial:
					for val in values:
						if val in observed:
							matched = True
							break
				else:
					if value is not None:
						if observed == value:
							matched = True
					elif observed in values:
						matched = True

				if matched:
					output_handle.write_line(line, observed)
					file.lines_matched += 1
			except (KeyError, json.JSONDecodeError, AttributeError) as err:
				file.error_lines += 1
			file.lines_processed += 1
			if file.lines_processed % 1000000 == 0:
				file.bytes_processed = file_bytes_processed
				queue.put(file)

		output_handle.close()
		file.complete = True
		file.bytes_processed = file.file_size
	except Exception as err:
		file.error_message = str(err)
	queue.put(file)


if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Use multiple processes to decompress and iterate over pushshift dump files")
	parser.add_argument("input", help="The input folder to recursively read files from")
	parser.add_argument("--output", help="Put the output files in this folder", default="")
	parser.add_argument("--working", help="The folder to store temporary files in", default="pushshift_working")
	parser.add_argument("--field", help="When deciding what lines to keep, use this field for comparisons", default="subreddit")
	parser.add_argument("--value", help="When deciding what lines to keep, compare the field to this value. Supports a comma separated list. This is case sensitive", default="pushshift")
	parser.add_argument("--value_list", help="A file of newline separated values to use. Overrides the value param if it is set", default=None)
	parser.add_argument("--processes", help="Number of processes to use", default=10, type=int)
	parser.add_argument("--file_filter", help="Regex filenames have to match to be processed", default="^RC_|^RS_")
	parser.add_argument(
		"--split_intermediate",
		help="Split the intermediate files by the first letter of the matched field, use if the filter will result in a large number of separate files",
		action="store_true")
	parser.add_argument(
		"--single_output",
		help="Output a single combined file instead of splitting by the search term",
		action="store_true")
	parser.add_argument(
		"--error_rate", help=
		"Percentage as an integer from 0 to 100 of the lines where the field can be missing. For the subreddit field especially, "
		"there are a number of posts that simply don't have a subreddit attached", default=1, type=int)
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
	parser.add_argument(
		"--partial", help="The values only have to be contained in the field, not match exactly. If this is set, "
		"the output files are not split by value. WARNING: This can severely slow down the script, especially if searching the "
		"body.", action='store_const', const=True, default=False)
	parser.add_argument(
		"--regex", help="The values are treated as regular expressions. If this is set, "
		"the output files are not split by value. WARNING: This can severely slow down the script, especially if searching the "
		"body. If set, ignores the --partial flag", action='store_const', const=True, default=False)
	script_type = "split"

	args = parser.parse_args()
	arg_string = f"{args.field}:{(args.value if args.value else args.value_list)}"

	if args.debug:
		log.setLevel(logging.DEBUG)

	log.info(f"Loading files from: {args.input}")
	if args.output:
		log.info(f"Writing output to: {args.output}")
	else:
		log.info(f"Writing output to working folder")

	if (args.partial or args.regex or args.single_output) and args.split_intermediate:
		log.info("The partial, regex and single_output flags are not compatible with the split_intermediate flag")
		sys.exit(1)

	values = set()
	if args.value_list:
		log.info(f"Reading {args.value_list} for values to compare")
		with open(args.value_list, 'r') as value_list_handle:
			for line in value_list_handle:
				values.add(line)

	else:
		values = set(args.value.split(","))

	if args.regex:
		regexes = []
		for reg in values:
			regexes.append(re.compile(reg))
		values = regexes
		if len(values) > 1:
			log.info(f"Checking field {args.field} against {len(values)} regexes")
		else:
			log.info(f"Checking field {args.field} against regex {values[0]}")
	else:
		lower_values = set()
		for value_inner in values:
			lower_values.add(value_inner.strip().lower())
		values = lower_values
		if len(values) > 5:
			val_string = f"any of {len(values)} values"
		elif len(values) == 1:
			val_string = f"the value {(','.join(values))}"
		else:
			val_string = f"any of the values {(','.join(values))}"
		if args.partial:
			log.info(f"Checking if any of {val_string} are contained in field {args.field}")
		else:
			log.info(f"Checking if any of {val_string} exactly match field {args.field}")

	if args.partial or args.regex or args.single_output:
		log.info(f"Outputing to a single combined file")

	multiprocessing.set_start_method('spawn')
	queue = multiprocessing.Manager().Queue()
	status_json = os.path.join(args.working, "status.json")
	input_files, saved_arg_string, saved_type, completed_prefixes = load_file_list(status_json)
	if saved_arg_string and saved_arg_string != arg_string:
		log.warning(f"Args don't match args from json file. Delete working folder")
		sys.exit(0)

	if saved_type and saved_type != script_type:
		log.warning(f"Script type doesn't match type from json file. Delete working folder")
		sys.exit(0)

	# if the file list wasn't loaded from the json, this is the first run, find what files we need to process
	if input_files is None:
		input_files = []
		for subdir, dirs, files in os.walk(args.input):
			files.sort()
			for file_name in files:
				if file_name.endswith(".zst") and re.search(args.file_filter, file_name) is not None:
					input_path = os.path.join(subdir, file_name)
					if args.split_intermediate:
						output_extension = ""
					else:
						output_extension = ".zst"
					output_path = os.path.join(args.working, f"{file_name[:-4]}{output_extension}")
					input_files.append(FileConfig(input_path, output_path=output_path))

		save_file_list(input_files, args.working, status_json, arg_string, script_type)
	else:
		log.info(f"Existing input file was read, if this is not correct you should delete the {args.working} folder and run this script again")

	files_processed, total_bytes, total_bytes_processed, total_lines_processed, total_lines_matched, total_lines_errored = 0, 0, 0, 0, 0, 0
	files_to_process = []
	# calculate the total file size for progress reports, build a list of incomplete files to process
	# do this largest to smallest by file size so that we aren't processing a few really big files with only a few threads at the end
	for file in sorted(input_files, key=lambda item: item.file_size, reverse=True):
		total_bytes += file.file_size
		if file.complete:
			files_processed += 1
			total_lines_processed += file.lines_processed
			total_lines_matched += file.lines_matched
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
			workers = pool.starmap_async(process_file, [(file, queue, args.field, values, args.partial, args.regex, args.split_intermediate) for file in files_to_process], chunksize=1, error_callback=log.info)
			while not workers.ready() or not queue.empty():
				# loop until the workers are all done, pulling in status messages as they are sent
				file_update = queue.get()
				if file_update.error_message is not None:
					log.warning(f"File failed {file_update.input_path}: {file_update.error_message}")

				# this is the workers telling us they are starting a new file, print the debug message but nothing else
				if file_update.lines_processed == 0:
					log.debug(f"Starting file: {file_update.input_path} : {file_update.file_size:,}")
					continue

				# I'm going to assume that the list of files is short enough that it's no
				# big deal to just iterate each time since that saves a bunch of work
				total_lines_processed, total_lines_matched, total_bytes_processed, total_lines_errored, files_processed, files_errored, i = 0, 0, 0, 0, 0, 0, 0
				for file in input_files:
					if file.input_path == file_update.input_path:
						input_files[i] = file_update
						file = file_update
					total_lines_processed += file.lines_processed
					total_lines_matched += file.lines_matched
					total_bytes_processed += file.bytes_processed
					total_lines_errored += file.error_lines
					files_processed += 1 if file.complete or file.error_message is not None else 0
					files_errored += 1 if file.error_message is not None else 0
					i += 1
				if file_update.complete or file_update.error_message is not None:
					save_file_list(input_files, args.working, status_json, arg_string, script_type)
					log.debug(f"Finished file: {file_update.input_path} : {file_update.file_size:,}")
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
					f"{total_lines_processed:,} lines at {(total_lines_processed - first_lines)/(current_time - first_time):,.0f}/s, {total_lines_errored:,} errored, {total_lines_matched:,} matched : "
					f"{(total_bytes_processed / (2**30)):.2f} gb at {(bytes_per_second / (2**20)):,.0f} mb/s, {(total_bytes_processed / total_bytes) * 100:.0f}% : "
					f"{files_processed}({files_errored})/{len(input_files)} files : "
					f"{(str(days_left) + 'd ' if days_left > 0 else '')}{hours_left - (days_left * 24)}:{minutes_left - (hours_left * 60):02}:{seconds_left - (minutes_left * 60):02} remaining")

	log.info(f"{total_lines_processed:,}, {total_lines_errored} errored : {(total_bytes_processed / (2**30)):.2f} gb, {(total_bytes_processed / total_bytes) * 100:.0f}% : {files_processed}/{len(input_files)}")

	type_handles = defaultdict(list)
	prefixes = set()
	count_incomplete = 0
	count_intermediate_files = 0
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
			elif file.output_path is not None and os.path.exists(file.output_path):
				input_handle = FileHandle(file.output_path, is_split=args.split_intermediate)
				for path in input_handle.get_paths():
					prefixes.add(path[-FileHandle.ext_len - 1:-FileHandle.ext_len])
					count_intermediate_files += 1
				type_handles[file.file_type].append(input_handle)

	if count_incomplete > 0:
		log.info(f"{count_incomplete} files were not completed, errored or don't exist, something went wrong. Aborting")
		sys.exit()

	log.info(f"Processing complete, combining {count_intermediate_files} result files")

	for completed_prefix in completed_prefixes:
		if completed_prefix in prefixes:
			prefixes.remove(completed_prefix)

	output_lines = 0
	output_handles = {}
	files_combined = 0
	if values:
		split = True
	else:
		split = False
	if args.split_intermediate:
		for prefix in sorted(prefixes):
			log.info(f"From {files_combined}/{count_intermediate_files} files to {len(output_handles):,} output handles : {output_lines:,}/{total_lines_matched:,} lines")
			for file_type, input_handles in type_handles.items():
				for input_handle in input_handles:
					has_lines = False
					for line, file_bytes_processed in input_handle.yield_lines(character_filter=prefix):
						if not has_lines:
							has_lines = True
							files_combined += 1
						output_lines += 1
						obj = json.loads(line)
						observed_case = obj[args.field]
						observed = observed_case.lower()
						if observed not in output_handles:
							if args.output:
								if not os.path.exists(args.output):
									os.makedirs(args.output)
								output_file_path = os.path.join(args.output, f"{observed_case}_{FileType.to_str(file_type)}.zst")
							else:
								output_file_path = f"{observed_case}_{FileType.to_str(file_type)}.zst"
							log.debug(f"Writing to file {output_file_path}")
							output_handle = FileHandle(output_file_path)
							output_handles[observed] = output_handle
						else:
							output_handle = output_handles[observed]

						output_handle.write_line(line)
						if output_lines % 1000000 == 0:
							log.info(f"From {files_combined}/{count_intermediate_files} files to {len(output_handles):,} output handles : {output_lines:,}/{total_lines_matched:,} lines : {input_handle.path} / {prefix}")
				for handle in output_handles.values():
					handle.close()
				output_handles = {}
			completed_prefixes.add(prefix)
			save_file_list(input_files, args.working, status_json, arg_string, script_type, completed_prefixes)

	else:
		log.info(f"From {files_combined}/{count_intermediate_files} files to {len(output_handles):,} output handles : {output_lines:,}/{total_lines_matched:,} lines")
		for file_type, input_handles in type_handles.items():
			for input_handle in input_handles:
				files_combined += 1
				for line, file_bytes_processed in input_handle.yield_lines():
					output_lines += 1
					obj = json.loads(line)
					if args.partial or args.regex or args.single_output:
						observed_case = "output"
					else:
						observed_case = obj[args.field]
					observed = observed_case.lower()
					if observed not in output_handles:
						if args.output:
							if not os.path.exists(args.output):
								os.makedirs(args.output)
							output_file_path = os.path.join(args.output, f"{observed_case}_{FileType.to_str(file_type)}.zst")
						else:
							output_file_path = f"{observed_case}_{FileType.to_str(file_type)}.zst"
						log.debug(f"Writing to file {output_file_path}")
						output_handle = FileHandle(output_file_path)
						output_handles[observed] = output_handle
					else:
						output_handle = output_handles[observed]

					output_handle.write_line(line)
					if output_lines % 1000000 == 0:
						log.info(f"From {files_combined}/{count_intermediate_files} files to {len(output_handles):,} output handles : {output_lines:,}/{total_lines_matched:,} lines : {input_handle.path}")
			for handle in output_handles.values():
				handle.close()
			output_handles = {}

	log.info(f"From {files_combined}/{count_intermediate_files} files to {len(output_handles):,} output handles : {output_lines:,}/{total_lines_matched:,} lines")
