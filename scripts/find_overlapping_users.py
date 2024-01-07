from collections import defaultdict
from datetime import datetime, timedelta
import time
import os
import logging.handlers
import zstandard
import json

input_files = [
	r"\\MYCLOUDPR4100\Public\reddit\subreddits\srilanka_comments.zst",
	r"\\MYCLOUDPR4100\Public\reddit\subreddits\Warthunder_comments.zst",
]
ignored_users = ['[deleted]', 'automoderator']
min_comments_per_sub = 1
file_name = "users.txt"
require_first_subreddit = False  # if true, print users that occur in the first subreddit and any one of the following ones. Otherwise just find the most overlap between all subs


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
	commenterSubreddits = defaultdict(int)
	is_first = True
	total_lines = 0
	for subreddit_file in input_files:
		file_lines = 0
		created = None
		file_size = os.stat(subreddit_file).st_size
		commenters = defaultdict(int)
		for line, file_bytes_processed in read_lines_zst(subreddit_file):
			total_lines += 1
			file_lines += 1
			if total_lines % 100000 == 0:
				log.info(f"{total_lines:,}: {subreddit_file}: {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")

			try:
				obj = json.loads(line)
				created = datetime.utcfromtimestamp(int(obj['created_utc']))

				if obj['author'].lower() not in ignored_users:
					commenters[obj['author']] += 1
			except (KeyError, json.JSONDecodeError) as err:
				pass
		log.info(f"{total_lines:,}: {subreddit_file}: {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : 100%")

		for commenter in commenters:
			if require_first_subreddit and not is_first and commenter not in commenterSubreddits:
				continue
			if commenters[commenter] >= min_comments_per_sub:
				commenterSubreddits[commenter] += 1
		is_first = False

	if require_first_subreddit:
		count_found = 0
		with open(file_name, 'w') as txt:
			txt.write(f"Commenters in r/{input_files[0]} and at least one of r/{(', '.join(input_files))}\n")
			for commenter, countSubreddits in commenterSubreddits.items():
				if countSubreddits >= 2:
					count_found += 1
					txt.write(f"{commenter}\n")
		log.info(f"{count_found} commenters in r/{input_files[0]} and at least one of r/{(', '.join(input_files))}")

	else:
		sharedCommenters = defaultdict(list)
		for commenter, countSubreddits in commenterSubreddits.items():
			if countSubreddits >= len(input_files) - 2:
				sharedCommenters[countSubreddits].append(commenter)

		commentersAll = len(sharedCommenters[len(input_files)])
		commentersMinusOne = len(sharedCommenters[len(input_files) - 1])
		commentersMinusTwo = len(sharedCommenters[len(input_files) - 2])

		log.info(f"{commentersAll} commenters in all subreddits, {commentersMinusOne} in all but one, {commentersMinusTwo} in all but 2. Writing output to {file_name}")

		with open(file_name, 'w') as txt:
			if commentersAll == 0:
				txt.write(f"No commenters in all subreddits\n")
			else:
				txt.write(f"{commentersAll} commenters in all subreddits\n")
				for user in sorted(sharedCommenters[len(input_files)], key=str.lower):
					txt.write(f"{user}\n")
			txt.write("\n")

			if commentersAll < 10 and len(input_files) > 2:
				if commentersMinusOne == 0:
					txt.write(f"No commenters in all but one subreddits\n")
				else:
					txt.write(f"{commentersMinusOne} commenters in all but one subreddits\n")
					for user in sorted(sharedCommenters[len(input_files) - 1], key=str.lower):
						txt.write(f"{user}\n")
				txt.write("\n")

				if commentersMinusOne < 10:
					if commentersMinusTwo == 0:
						txt.write(f"No commenters in all but two subreddits\n")
					else:
						txt.write(f"{commentersMinusTwo} commenters in all but two subreddits\n")
						for user in sorted(sharedCommenters[len(input_files) - 2], key=str.lower):
							txt.write(f"{user}\n")
					txt.write("\n")
