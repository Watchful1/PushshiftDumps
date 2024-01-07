import sys
import requests
import time
import discord_logging
import argparse
import os
import re
import zstandard
from datetime import datetime, timedelta
import json
import praw
from praw import endpoints
import prawcore
import logging.handlers

sys.path.append('personal')

log = discord_logging.init_logging(debug=False)

import utils
import classes
from classes import IngestType
from merge import ObjectType


NEWLINE_ENCODED = "\n".encode('utf-8')
reg = re.compile(r"\d\d-\d\d-\d\d_\d\d-\d\d")


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Combine the minute files into a single month")
	parser.add_argument("--type", help="The object type, either comments or submissions", required=True)
	parser.add_argument("--month", help="The month to process, format YY-MM", required=True)
	parser.add_argument('--input', help='Input folder', required=True)
	parser.add_argument('--output', help='Output folder', required=True)
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
	parser.add_argument("--level", help="The compression ratio to output at", default="3")
	args = parser.parse_args()

	if args.debug:
		discord_logging.set_level(logging.DEBUG)

	month = datetime.strptime(args.month, '%y-%m')
	level = int(args.level)

	log.info(f"Input folder: {args.input}")
	log.info(f"Output folder: {args.output}")
	log.info(f"Month: {args.month}")
	log.info(f"Compression level: {level}")

	prefix = None
	if args.type == "comments":
		prefix = "RC"
	elif args.type == "submissions":
		prefix = "RS"
	else:
		log.error(f"Invalid type: {args.type}")
		sys.exit(2)

	total_objects = 0
	total_bytes = 0
	minute_iterator = month
	if month.month == 12:
		end_time = month.replace(year=month.year + 1, month=1)
	else:
		end_time = month.replace(month=month.month + 1)
	while minute_iterator < end_time:
		minute_file_path = os.path.join(args.input, args.type, minute_iterator.strftime('%y-%m-%d'), f"{prefix}_{minute_iterator.strftime('%y-%m-%d_%H-%M')}.zst")
		for obj, line, _ in utils.read_obj_zst_meta(minute_file_path):
			total_bytes += len(line.encode('utf-8'))
			total_bytes += 1

			total_objects += 1
			if total_objects % 1000000 == 0:
				log.info(f"Counting: {minute_iterator.strftime('%y-%m-%d_%H-%M')} : {total_objects:,} : {total_bytes:,}")

		minute_iterator += timedelta(minutes=1)

	log.info(f"Counting: {minute_iterator.strftime('%y-%m-%d_%H-%M')} : {total_objects:,} : {total_bytes:,}")

	output_path = os.path.join(args.output, args.type, f"{prefix}_{month.strftime('%Y-%m')}.zst")
	output_handle = zstandard.ZstdCompressor(level=level, write_content_size=True, write_checksum=True, threads=-1).stream_writer(open(output_path, 'wb'), size=total_bytes)

	count_objects = 0
	count_bytes = 0
	minute_iterator = month
	end_time = month.replace(month=month.month + 1)
	while minute_iterator < end_time:
		minute_file_path = os.path.join(args.input, args.type, minute_iterator.strftime('%y-%m-%d'), f"{prefix}_{minute_iterator.strftime('%y-%m-%d_%H-%M')}.zst")
		for obj, line, _ in utils.read_obj_zst_meta(minute_file_path):
			line_encoded = line.encode('utf-8')
			count_bytes += len(line_encoded)
			count_bytes += 1
			output_handle.write(line_encoded)
			output_handle.write(NEWLINE_ENCODED)

			count_objects += 1
			if count_objects % 100000 == 0:
				log.info(f"Writing: {minute_iterator.strftime('%y-%m-%d_%H-%M')} : {count_objects:,}/{total_objects:,} : {count_bytes:,}/{total_bytes:,}")

		minute_iterator += timedelta(minutes=1)

	log.info(f"Writing: {minute_iterator.strftime('%y-%m-%d_%H-%M')} : {count_objects:,}/{total_objects:,} : {count_bytes:,}/{total_bytes:,}")
	output_handle.close()
