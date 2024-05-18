import sys
sys.path.append('personal')

import discord_logging
import os
import zstandard
from datetime import datetime
import json
import argparse

log = discord_logging.get_logger(init=True)

import utils

NEWLINE_ENCODED = "\n".encode('utf-8')


def split_by_minutes(input_file, output_file):
	file_type = "comments" if "RC" in input_file else "submissions"

	log.info(f"{file_type}: Input file: {input_file}")
	log.info(f"{file_type}: Output folder: {output_file}")
	previous_minute, output_handle, created_utc = None, None, None
	count_objects, count_minute = 0, 0
	if input_file.endswith(".zst"):
		reader = utils.read_obj_zst(input_file)
	elif input_file.endswith(".zst_blocks"):
		reader = utils.read_obj_zst_blocks(input_file)
	else:
		log.error(f"{file_type}: Unsupported file type: {input_file}")
		return
	for obj in reader:
		created_utc = datetime.utcfromtimestamp(obj["created_utc"])
		current_minute = created_utc.replace(second=0)

		if previous_minute is None or current_minute > previous_minute:
			log.info(f"{file_type}: {created_utc.strftime('%y-%m-%d_%H-%M')}: {count_objects:,} : {count_minute: ,}")
			previous_minute = current_minute
			count_minute = 0
			if output_handle is not None:
				output_handle.close()

			output_path = os.path.join(output_file, file_type, created_utc.strftime('%y-%m-%d'))
			if not os.path.exists(output_path):
				os.makedirs(output_path)
			output_path = os.path.join(output_path, f"{('RC' if file_type == 'comments' else 'RS')}_{created_utc.strftime('%y-%m-%d_%H-%M')}.zst")
			output_handle = zstandard.ZstdCompressor().stream_writer(open(output_path, 'wb'))

		count_objects += 1
		count_minute += 1
		output_handle.write(json.dumps(obj, sort_keys=True).encode('utf-8'))
		output_handle.write(NEWLINE_ENCODED)

	if created_utc is None:
		log.error(f"{file_type}: {input_file} appears to be empty")
		sys.exit(1)
	log.info(f"{file_type}: {created_utc.strftime('%y-%m-%d_%H-%M')}: {count_objects:,} : {count_minute: ,}")
	if output_handle is not None:
		output_handle.close()


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Take a zst_blocks file and split it by minute chunks")
	parser.add_argument('--input', help='Input file', required=True)
	parser.add_argument('--output', help='Output folder', required=True)
	args = parser.parse_args()

	split_by_minutes(args.input, args.output)
