import shutil
import os
import logging.handlers
import re

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())

if __name__ == '__main__':
	input_folder = r"\\MYCLOUDPR4100\Public\pushshift_output"
	output_folder = r"\\MYCLOUDPR4100\Public\request"
	subs = ['PoliticalDiscussion', 'worldnews', 'science']
	overwrite = False

	lower_subs = set()
	for sub in subs:
		lower_subs.add(sub.lower())

	matched_subs = set()
	total_size = 0
	for file_name in os.listdir(input_folder):
		file_path = os.path.join(input_folder, file_name)
		if file_name.endswith(".zst") and os.path.isfile(file_path):
			match = re.match(r"(\w+)(?:_(?:comments|submissions).zst)", file_name)
			if match:
				sub_cased = match.group(1)
				if sub_cased.lower() in lower_subs:
					matched_subs.add(sub_cased)
					file_size = os.stat(file_path).st_size
					total_size += file_size
					log.info(f"Copying {file_name} : {(file_size / (2**20)):,.0f} mb : {(total_size / (2**20)):,.0f} mb")
					output_path = os.path.join(output_folder, file_name)
					if overwrite or not os.path.exists(output_path):
						shutil.copy(file_path, output_path)

	log.info(f"Copied {len(matched_subs)}/{len(subs)} subs of total size {(total_size / (2**20)):,.0f} mb")
	if len(matched_subs) != len(lower_subs):
		lower_matched_subs = [sub.lower() for sub in matched_subs]
		for sub in lower_subs:
			if sub not in lower_matched_subs:
				log.info(f"Missing r/{sub}")

	sorted_case_subs = sorted(matched_subs)
	bldr = ['torrenttools create -a "https://academictorrents.com/announce.php" -c "Comments and submissions from r/']
	bldr.append(', r/'.join(sorted_case_subs))
	bldr.append(' through the end of 2022"  --include ".*')
	bldr.append('.*zst" --include ".*'.join(sorted_case_subs))
	bldr.append('.*zst" -o username.torrent reddit')
	log.info(''.join(bldr))
