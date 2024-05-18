import sys
from collections import defaultdict
from datetime import datetime, timedelta
import time
import os
import logging.handlers
import zstandard
import json

# IMPORTANT SETUP INSTRUCTIONS
# change the folder line to the folder where the files are stored
# change the subreddits to the list of subreddits, one per line. The case must exactly match, ie, for r/AskReddit, put "AskReddit"
# the files in the folder must match the format from the torrent, subreddit_type.zst, like AskReddit_comments.zst
# the script will look for both comments and submissions files for each subreddit
folder = r"\\MYCLOUDPR4100\Public\reddit\subreddits23"
subreddits = [
	"aquarium",
	"opiates",
	"axolotls",
	"piercing",
	"titanfolk",
	"AskOuija",
	"piercing",
	"DPH",
	"dxm",
]
ignored_users = {'[deleted]', 'automoderator'}
# this is a list of users to ignore when doing the comparison. Most popular bots post in many subreddits and aren't the person you're looking for
# here's a good start, but add bots to your list as you encounter them https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/ignored.txt
ignored_users_file = "ignored.txt"
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


def get_commenters_from_file(subreddit_file, subreddit_commenters, total_lines):
	file_lines = 0
	created = None
	file_size = os.stat(subreddit_file).st_size
	for line, file_bytes_processed in read_lines_zst(subreddit_file):
		total_lines += 1
		file_lines += 1
		if total_lines % 100000 == 0:
			log.info(f"{total_lines:,}: {subreddit_file}: {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")

		try:
			obj = json.loads(line)
			created = datetime.utcfromtimestamp(int(obj['created_utc']))

			if obj['author'].lower() not in ignored_users:
				subreddit_commenters[obj['author']] += 1
		except (KeyError, json.JSONDecodeError) as err:
			pass
	log.info(f"{total_lines:,}: {subreddit_file}: {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : 100%")
	return total_lines


if __name__ == "__main__":
	log.info(f"Subreddit's folder: {folder}")
	if len(subreddits) <= 10:
		log.info(f"Finding overlapping users in {', '.join(subreddits)}")
	else:
		log.info(f"Finding overlapping users in {len(subreddits)} subreddits")
	if require_first_subreddit:
		log.info(f"Finding users from the first subreddit that are in any of the other subreddits")
	log.info(f"Minimum comments per subreddit set to {min_comments_per_sub}")
	log.info(f"Outputting to {file_name}")

	if os.path.exists(ignored_users_file):
		with open(ignored_users_file) as fh:
			for user in fh.readlines():
				ignored_users.add(user.strip().lower())
		log.info(f"Loaded {len(ignored_users)} ignored users from {ignored_users_file}")

	commenterSubreddits = defaultdict(int)
	is_first = True
	total_lines = 0
	for subreddit in subreddits:
		subreddit_exists = False
		commenters = defaultdict(int)
		for file_type in ["submissions", "comments"]:
			subreddit_file = os.path.join(folder, f"{subreddit}_{file_type}.zst")
			if not os.path.exists(subreddit_file):
				log.info(f"{file_type} for {subreddit} does not exist, skipping")
				continue
			subreddit_exists = True
			total_lines = get_commenters_from_file(subreddit_file, commenters, total_lines)
		if not subreddit_exists:
			log.error(f"Subreddit {subreddit} has no files, aborting")
			sys.exit(0)

		for commenter in commenters:
			if require_first_subreddit and not is_first and commenter not in commenterSubreddits:
				continue
			if commenters[commenter] >= min_comments_per_sub:
				commenterSubreddits[commenter] += 1
		is_first = False

	if require_first_subreddit:
		count_found = 0
		with open(file_name, 'w') as txt:
			txt.write(f"Commenters in r/{subreddits[0]} and at least one of {(', '.join(subreddits))}\n")
			for commenter, countSubreddits in commenterSubreddits.items():
				if countSubreddits >= 2:
					count_found += 1
					txt.write(f"{commenter}\n")
		log.info(f"{count_found} commenters in r/{subreddits[0]} and at least one of {(', '.join(subreddits))}")

	else:
		sharedCommenters = defaultdict(list)
		for commenter, countSubreddits in commenterSubreddits.items():
			if countSubreddits >= 2:
				sharedCommenters[countSubreddits].append(commenter)

		with open(file_name, 'w') as txt:
			log.info(f"Writing output to {file_name}")
			for i in range(len(subreddits)):
				commenters = len(sharedCommenters[len(subreddits) - i])
				inner_str = f"but {i} " if i != 0 else ""
				log.info(f"{commenters} commenters in all {inner_str}subreddits")
				if commenters == 0:
					txt.write(f"No commenters in all {inner_str}subreddits\n")
				else:
					txt.write(f"{commenters} commenters in all {inner_str}subreddits\n")
					for user in sorted(sharedCommenters[len(subreddits) - i], key=str.lower):
						txt.write(f"{user}\n")
				txt.write("\n")
				if commenters > 3:
					break
