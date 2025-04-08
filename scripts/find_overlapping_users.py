import sys
from collections import defaultdict
from datetime import datetime, timedelta
import time
import os
import logging.handlers
import zstandard
import json

# IMPORTANT SETUP INSTRUCTIONS
# get subreddit files from here https://www.reddit.com/r/pushshift/comments/1itme1k/separate_dump_files_for_the_top_40k_subreddits/
# change the folder line to the folder where the files are stored
# change the subreddits to the list of subreddits, one per line. The case must exactly match, ie, for r/AskReddit, put "AskReddit"
# the files in the folder must match the format from the torrent, subreddit_type.zst, like AskReddit_comments.zst
# the script will look for both comments and submissions files for each subreddit
folder = r"\\MYCLOUDPR4100\Public\reddit\subreddits24"
subreddits_string = """
	askcarsales
	Denton
	relationship_advice
	Dallas
	askdfw
	AskMen
	rolex
	lego
"""
ignored_users = {'[deleted]', 'automoderator'}
# this is a list of users to ignore when doing the comparison. Most popular bots post in many subreddits and aren't the person you're looking for
# here's a good start, but add bots to your list as you encounter them https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/ignored.txt
ignored_users_file = "ignored.txt"
min_comments_per_sub = 1
output_file_name = "users.txt"
require_first_subreddit = False  # if true, print users that occur in the first subreddit and any one of the following ones. Otherwise just find the most overlap between all subs
from_date = datetime.strptime("2005-01-01", "%Y-%m-%d")
to_date = datetime.strptime("2040-12-31", "%Y-%m-%d")


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


def get_commenters_from_file(subreddit, subreddit_file, subreddit_commenters, total_lines, files_status, from_date, to_date):
	file_lines = 0
	created = None
	file_size = os.stat(subreddit_file).st_size
	for line, file_bytes_processed in read_lines_zst(subreddit_file):
		total_lines += 1
		file_lines += 1
		if total_lines % 100000 == 0:
			log.info(f"{files_status}: {total_lines:,}: r/{subreddit}: {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : {(file_bytes_processed / file_size) * 100:.0f}%")

		try:
			obj = json.loads(line)
			created = datetime.utcfromtimestamp(int(obj['created_utc']))
			if created < from_date or created > to_date:
				continue

			if obj['author'].lower() not in ignored_users:
				subreddit_commenters[obj['author']] += 1
		except (KeyError, json.JSONDecodeError) as err:
			pass
	log.info(f"{total_lines:,}: {subreddit_file}: {created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines:,} : 100%")
	return total_lines


if __name__ == "__main__":
	log.info(f"Subreddit's folder: {folder}")
	if not os.path.exists(folder):
		log.error(f"Subreddit's folder either doesn't exist or the script doesn't have access to it: {folder}")
		sys.exit()
	subreddits = []
	for line in subreddits_string.split("\n"):
		subreddit = line.strip()
		if subreddit == "":
			continue
		subreddits.append(subreddit)

	if len(subreddits) <= 10:
		log.info(f"Finding overlapping users in {', '.join(subreddits)}")
	else:
		log.info(f"Finding overlapping users in {len(subreddits)} subreddits")
	if require_first_subreddit:
		log.info(f"Finding users from the first subreddit that are in any of the other subreddits")
	log.info(f"Minimum comments per subreddit set to {min_comments_per_sub}")
	log.info(f"Outputting to {output_file_name}")
	log.info(f"From date {from_date.strftime('%Y-%m-%d')} to date {to_date.strftime('%Y-%m-%d')}")

	if os.path.exists(ignored_users_file):
		with open(ignored_users_file) as fh:
			for user in fh.readlines():
				ignored_users.add(user.strip().lower())
		log.info(f"Loaded {len(ignored_users)} ignored users from {ignored_users_file}")

	log.info(f"Checking that subreddit files are present")

	folder_files = {}
	for file in os.listdir(folder):
		folder_files[file.lower()] = file

	subreddit_stats = []
	for subreddit in subreddits:
		subreddit_stat = {"files": 0, "bytes": 0, "name": subreddit}
		for file_type in ["submissions", "comments"]:
			file_ending = f"_{file_type}.zst"
			file_name = folder_files.get(f"{subreddit.lower()}{file_ending}")
			if file_name is None:
				continue
			subreddit_file = os.path.join(folder, file_name)

			subreddit_stat["name"] = file_name[0:-len(file_ending)]
			subreddit_stat[file_type] = subreddit_file
			subreddit_stat["files"] += 1
			subreddit_stat["bytes"] += os.stat(subreddit_file).st_size

		subreddit_stats.append(subreddit_stat)

	subreddit_stats.sort(key=lambda x: x["bytes"], reverse=True)
	abort = False
	for subreddit_stat in subreddit_stats:
		if subreddit_stat["files"] == 0:
			log.info(f"No files for {subreddit_stat['name']} exist")
			abort = True
		else:
			log.info(f"r/{subreddit_stat['name']} files total {(subreddit_stat['bytes'] / (2**30)):.2f} gb")

	if abort:
		log.error(f"The script can see {len(folder_files)} files in the folder, but not the ones requested: {folder}")
		sys.exit(0)

	commenterSubreddits = defaultdict(int)
	is_first = True
	total_lines = 0
	files_processed = 1
	for subreddit_stat in subreddit_stats:
		commenters = defaultdict(int)
		for file_type in ["submissions", "comments"]:
			total_lines = get_commenters_from_file(
				f"{subreddit_stat['name']}_{file_type}",
				subreddit_stat[file_type],
				commenters,
				total_lines,
				f"{files_processed}|{len(subreddit_stats)}",
				from_date,
				to_date
			)
		for commenter in commenters:
			if require_first_subreddit and not is_first and commenter not in commenterSubreddits:
				continue
			if commenters[commenter] >= min_comments_per_sub:
				commenterSubreddits[commenter] += 1
		is_first = False
		files_processed += 1

	if require_first_subreddit:
		count_found = 0
		with open(output_file_name, 'w') as txt:
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

		with open(output_file_name, 'w') as txt:
			log.info(f"Writing output to {output_file_name}")
			txt.write(f"Commenters in subreddits {(', '.join(subreddits))}\n")
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
