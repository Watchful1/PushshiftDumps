import os
import logging.handlers
from collections import defaultdict


log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

if __name__ == '__main__':
	input_folder = r"\\MYCLOUDPR4100\Public\pushshift_counts_summed"
	output_file = r"\\MYCLOUDPR4100\Public\subreddit_counts_total.txt"
	subreddits = defaultdict(int)

	for subdir, dirs, files in os.walk(input_folder):
		for filename in files:
			log.info(f"Processing file: {filename}")
			input_path = os.path.join(subdir, filename)
			with open(input_path, 'r') as input_handle:
				line_count = 0
				for line in input_handle:
					subreddit, count_string = line.strip().split("\t")
					count = int(count_string)
					subreddits[subreddit] += count
					line_count += 1

	log.info(f"Total subreddits: {len(subreddits):,}")

	count_written = 0
	with open(output_file, 'w') as output_handle:
		for subreddit, count in sorted(subreddits.items(), key=lambda item: item[1], reverse=True):
			output_handle.write(f"{subreddit}	{count}\n")
			count_written += 1
			if count_written % 1000000 == 0:
				log.info(f"Written: {count_written:,}/{len(subreddits):,}")

	log.info(f"Written: {count_written:,}/{len(subreddits):,}")
