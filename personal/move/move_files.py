import os
import discord_logging
import re
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	parent_folder = r"\\MYCLOUDPR4100\Public\ingest"
	folders = [r"ingest\comments",r"ingest\submissions",r"rescan\comments",r"rescan\submissions"]
	reg = re.compile(r"\d\d-\d\d-\d\d_\d\d-\d\d")
	for folder in folders:
		files = []
		created_date_folders = set()
		folder_path = os.path.join(parent_folder, folder)
		for file in os.listdir(folder_path):
			file_path = os.path.join(folder_path, file)
			if file.endswith(".zst"):
				files.append(file)
		log.info(f"{folder}: {len(files):,}")

		count_moved = 0
		for file in files:
			match = reg.search(file)
			if not match:
				log.info(f"File doesn't match regex: {file}")
				continue
			file_date = datetime.strptime(match.group(), '%y-%m-%d_%H-%M')
			date_folder_name = file_date.strftime('%y-%m-%d')
			date_folder_path = os.path.join(folder_path, date_folder_name)
			if date_folder_name not in created_date_folders:
				log.info(f"Creating folder: {date_folder_path}")
				if not os.path.exists(date_folder_path):
					os.makedirs(date_folder_path)
				created_date_folders.add(date_folder_name)
			old_file_path = os.path.join(folder_path, file)
			new_file_path = os.path.join(date_folder_path, file)
			os.rename(old_file_path, new_file_path)
			count_moved += 1
			if count_moved % 100 == 0:
				log.info(f"{count_moved:,}/{len(files):,}: {folder}")
		log.info(f"{count_moved:,}/{len(files):,}: {folder}")
