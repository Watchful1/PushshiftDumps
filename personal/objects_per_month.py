import os
from collections import defaultdict


if __name__ == "__main__":
	input_folder = r"\\MYCLOUDPR4100\Public\pushshift_counts_summed"
	for subdir, dirs, files in os.walk(input_folder):
		for file_name in files:
			items = 0
			input_path = os.path.join(subdir, file_name)
			with open(input_path, 'r') as input_handle:
				for line in input_handle:
					subreddit, count = line.strip().split("\t")
					items += int(count)
			print(f"{file_name}	{items}")
