import utils
import discord_logging
import os
import sys
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	file_one = open(r"\\MYCLOUDPR4100\Public\reddit_final\RelationshipsOver35_comments_dump.txt", 'r')
	file_two = open(r"\\MYCLOUDPR4100\Public\reddit_final\RelationshipsOver35_comments_mongo.txt", 'r')

	file_lines = 0
	while True:
		file_lines += 1
		line_one = file_one.readline().rstrip()
		line_two = file_two.readline().rstrip()
		if line_one != line_two:
			log.info(f"lines not matching: {file_lines}")
			log.info(line_one)
			log.info(line_two)
			#break

		if file_lines % 100000 == 0:
			log.info(f"{file_lines:,}")

		if not line_one:
			break

	log.info(f"{file_lines:,}")
	file_one.close()
	file_two.close()
