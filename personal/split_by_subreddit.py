import utils
import discord_logging
import os
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	subreddits = {}
	object_type = "submissions"
	folder = f"\\\\MYCLOUDPR4100\\Public\\reddit_final\\{object_type}"
	if not os.path.exists(folder):
		os.makedirs(folder)
	input_file = f"\\\\MYCLOUDPR4100\\Public\\reddit_final\\relationships_{object_type}.zst"
	input_file_size = os.stat(input_file).st_size
	total_lines = 0
	for comment, line, file_bytes_processed in utils.read_obj_zst_meta(input_file):
		if comment['subreddit'] not in subreddits:
			subreddits[comment['subreddit']] = {'writer': utils.OutputZst(os.path.join(folder, comment['subreddit'] + f"_{object_type}.zst")), 'lines': 0}
		subreddit = subreddits[comment['subreddit']]
		subreddit['writer'].write(line)
		subreddit['writer'].write("\n")
		subreddit['lines'] += 1
		total_lines += 1
		if total_lines % 100000 == 0:
			log.info(f"{total_lines:,} lines, {(file_bytes_processed / input_file_size) * 100:.0f}%")

	log.info(f"{total_lines:,} lines, 100%")

	for name, subreddit in subreddits.items():
		log.info(f"r/{name}: {subreddit['lines']:,} lines")
		subreddit['writer'].close()
