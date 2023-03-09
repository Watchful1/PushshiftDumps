# this is an example of loading and iterating over a single file, doing some processing along the way to export a resulting csv

import zstandard
import os
import json
from collections import defaultdict
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
				yield line, file_handle.tell()

			buffer = lines[-1]

		reader.close()


if __name__ == "__main__":
	# the path to the input comment file
	input_path = r"\\MYCLOUDPR4100\Public\reddit\requests\wallstreetbets_comments.zst"
	# the path to the output csv file of word counts
	output_path = r"\\MYCLOUDPR4100\Public\reddit\wallstreetbets_counts.csv"
	# skip everything before this date. The subreddit was created in 2012, so there's a lot of dates before it gets to the good stuff if you want to skip them
	start_date = datetime.strptime("2020-01-01", '%Y-%m-%d')
	# list of word phrases to search for. Make sure these are all lowercase
	phrases = [
		"diamond hands",
		"sell",
	]

	# bunch of initialization stuff
	word_counts = defaultdict(int)
	file_lines = 0
	file_bytes_processed = 0
	created = None
	bad_lines = 0
	current_day = None
	output_file = open(output_path, 'w')
	output_file.write(f"Date,{(','.join(phrases))}\n")
	input_size = os.stat(input_path).st_size
	try:
		# this is the main loop where we iterate over every single line in the zst file
		for line, file_bytes_processed in read_lines_zst(input_path):
			try:
				# load the line into a json object
				obj = json.loads(line)
				# turn the created timestamp into a date object
				created = datetime.utcfromtimestamp(int(obj['created_utc']))
				# skip if we're before the start date defined above
				if created >= start_date:
					# if this is a different day than the previous line we looked at, save the word counts to the csv
					if current_day != created.replace(hour=0, minute=0, second=0, microsecond=0):
						# don't save the dates if this is the very first day, we're just starting
						if current_day is not None:
							# write out the date at the beginning of the line
							output_file.write(f"{current_day.strftime('%Y-%m-%d')}")
							# for each phrase in the list, look up the count associated with it and write it out
							for phrase in phrases:
								output_file.write(",")
								output_file.write(str(word_counts[phrase]))
							output_file.write("\n")
							# reset the dictionary so we can start counting up for the new day
							word_counts = defaultdict(int)
						# update the variable to the new day, so we can then tell when we get to the next day
						current_day = created.replace(hour=0, minute=0, second=0, microsecond=0)

					# get the lowercase of the object text
					body_lower = obj['body'].lower()
					# for each of the phrases in the list
					for phrase in phrases:
						# check if it's the text
						if phrase in body_lower:
							word_counts[phrase] += 1

			# just in case there's corruption somewhere in the file
			except (KeyError, json.JSONDecodeError) as err:
				bad_lines += 1
			file_lines += 1
			if file_lines % 100000 == 0:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {bad_lines:,} : {(file_bytes_processed / input_size) * 100:.0f}%")
	except Exception as err:
		log.info(err)

	# write out the last day
	output_file.write(f"{current_day.strftime('%Y-%m-%d')}")
	for phrase in phrases:
		output_file.write(",")
		output_file.write(str(word_counts[phrase]))
	output_file.write("\n")

	output_file.close()
	log.info(f"Complete : {file_lines:,} : {bad_lines:,}")
