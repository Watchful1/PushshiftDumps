import os
import discord_logging
import re
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	parent_folder = r"\\MYCLOUDPR4100\Public\ingest\combined\comments"
	files = []
	for folder_name in os.listdir(parent_folder):
		folder = os.path.join(parent_folder, folder_name)
		for file in os.listdir(folder):
			file_path = os.path.join(parent_folder, folder, file)
			if file.endswith(".zst"):
				files.append((folder, file))
	log.info(f"{parent_folder}: {len(files):,}")

	count_moved = 0
	for folder, old_file in files:
		old_path = os.path.join(folder, old_file)
		new_file = old_file.replace("RS_", "RC_")
		new_path = os.path.join(folder, new_file)

		os.rename(old_path, new_path)
		count_moved += 1
		if count_moved % 100 == 0:
			log.info(f"{count_moved:,}/{len(files):,}: {folder}")
	log.info(f"{count_moved:,}/{len(files):,}")
