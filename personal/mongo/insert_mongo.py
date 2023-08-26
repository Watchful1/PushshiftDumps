import utils
import discord_logging
import os
import pymongo
import sys
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	mongo_address = sys.argv[1]  # 192.168.1.131
	client = pymongo.MongoClient(f"mongodb://{mongo_address}:27017", serverSelectionTimeoutMS=5000)

	log.info(f"Database connected at {mongo_address} on {client.admin.command('serverStatus')['host']}")

	object_type = sys.argv[2]
	input_folder = sys.argv[3]
	input_files = []
	total_size = 0
	for subdir, dirs, files in os.walk(input_folder + os.sep + object_type):
		files.sort()
		for filename in files:
			input_path = os.path.join(subdir, filename)
			if input_path.endswith(".zst"):
				file_size = os.stat(input_path).st_size
				total_size += file_size
				input_files.append([input_path, file_size])

	log.info(f"Processing {len(input_files)} files of {(total_size / (2 ** 30)):.2f} gigabytes")

	collection = client.reddit_database[object_type]

	log.info(f"Using collection {object_type} which has {collection.estimated_document_count()} objects already")

	total_lines = 0
	total_bytes_processed = 0
	for input_file in input_files:
		file_lines = 0
		file_bytes_processed = 0
		created = None
		inserts = []
		for obj, line, file_bytes_processed in utils.read_obj_zst_meta(input_file[0]):
			inserts.append(obj)
			if len(inserts) >= 10000:
				collection.insert_many(inserts)
				inserts = []

			created = datetime.utcfromtimestamp(int(obj['created_utc']))
			file_lines += 1
			if file_lines == 1:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines + total_lines:,} : 0% : {(total_bytes_processed / total_size) * 100:.0f}%")
			if file_lines % 100000 == 0:
				log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {file_lines + total_lines:,} : {(file_bytes_processed / input_file[1]) * 100:.0f}% : {(total_bytes_processed / total_size) * 100:.0f}%")

		if len(inserts) >= 0:
			collection.insert_many(inserts)
		total_lines += file_lines
		total_bytes_processed += input_file[1]
		log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {total_lines:,} : 100% : {(total_bytes_processed / total_size) * 100:.0f}%")

	log.info(f"Total: {total_lines}")
