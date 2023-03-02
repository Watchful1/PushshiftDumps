import os
from collections import defaultdict


if __name__ == "__main__":
	input_folder = r"\\MYCLOUDPR4100\Public\pushshift_counts"
	output_folder = r"\\MYCLOUDPR4100\Public\pushshift_counts_summed"
	lines = 0
	for subdir, dirs, files in os.walk(input_folder):
		for file_name in files:
			subreddits = defaultdict(int)
			input_path = os.path.join(subdir, file_name)
			output_path = os.path.join(output_folder, f"{file_name}.txt")
			print(f"{lines} : {input_path}")
			with open(input_path, 'r') as input_handle:
				for line in input_handle:
					lines += 1
					subreddits[line.strip()] += 1
					if lines % 1000000 == 0:
						print(f"{lines} : {input_path}")

			with open(output_path, 'w') as output_handle:
				for subreddit, count in sorted(subreddits.items(), key=lambda item: item[1], reverse=True):
					output_handle.write(f"{subreddit}	{count}\n")
