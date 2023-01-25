

if __name__ == '__main__':
	input_file = r"\\MYCLOUDPR4100\Public\field_counts.txt"
	output_file = r"\\MYCLOUDPR4100\Public\field_counts_sorted.txt"
	subreddits = {}
	with open(input_file, 'r') as input_handle:
		for line in input_handle:
			subreddit, count_string = line.strip().split("\t")
			count = int(count_string)
			if count > 10000:
				subreddits[subreddit] = count

	print(f"{len(subreddits)}")

	with open(output_file, 'w') as output_handle:
		count_written = 0
		for subreddit, count in sorted(subreddits.items(), key=lambda item: item[1], reverse=True):
			output_handle.write(f"{subreddit}\n")
			count_written += 1
			if count_written >= 20000:
				break
