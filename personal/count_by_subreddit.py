import utils
import discord_logging
import os
from collections import defaultdict

log = discord_logging.init_logging()


if __name__ == "__main__":
	subreddits = defaultdict(int)
	input_file = r"\\MYCLOUDPR4100\Public\reddit\comments\RC_2021-06.zst"
	input_file_size = os.stat(input_file).st_size
	total_lines = 0
	for comment, line, file_bytes_processed in utils.read_obj_zst_meta(input_file):
		subreddits[comment['subreddit']] += 1
		total_lines += 1
		if total_lines % 100000 == 0:
			log.info(f"{total_lines:,} lines, {(file_bytes_processed / input_file_size) * 100:.0f}%")

	log.info(f"{total_lines:,} lines, 100%")

	for subreddit, count in sorted(subreddits.items(), key=lambda item: item[1] * -1):
		if count > 1000:
			log.info(f"r/{subreddit}: {count:,}")
