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

log = discord_logging.get_logger(init=True)

import utils
import classes
from classes import IngestType
from merge import ObjectType


NEWLINE_ENCODED = "\n".encode('utf-8')
reg = re.compile(r"\d\d-\d\d-\d\d_\d\d-\d\d")
# 2 GiB window, the same as `zstd --long=31`. Readers need max_window_size=2**31 (every reader
# in this repo already sets it) and the zstd command line needs `zstd -d --long=31`.
LONG_WINDOW_LOG = 31
READ_CHUNK = 2**27


def long_window_compressor(level, threads):
	"""The python-zstandard equivalent of `zstd -<level> --ultra --long=31`, plus a checksum."""
	params = zstandard.ZstdCompressionParameters(
		compression_level=level,
		window_log=LONG_WINDOW_LOG,
		enable_ldm=True,
		write_content_size=True,
		write_checksum=True,
		threads=threads)
	return zstandard.ZstdCompressor(compression_params=params)


def file_prefix(file_type):
	if file_type == "comments":
		return "RC"
	if file_type == "submissions":
		return "RS"
	raise ValueError(f"Invalid type: {file_type}")


def end_of_month(month):
	if month.month == 12:
		return month.replace(year=month.year + 1, month=1)
	return month.replace(month=month.month + 1)


def month_days(month):
	day = month
	end_time = end_of_month(month)
	while day < end_time:
		yield day
		day += timedelta(days=1)


def minute_file_paths(month, input_folder, file_type):
	prefix = file_prefix(file_type)
	minute_iterator = month
	end_time = end_of_month(month)
	while minute_iterator < end_time:
		yield minute_iterator, os.path.join(input_folder, file_type, minute_iterator.strftime('%y-%m-%d'), f"{prefix}_{minute_iterator.strftime('%y-%m-%d_%H-%M')}.zst")
		minute_iterator += timedelta(minutes=1)


def month_output_path(month, output_folder, file_type):
	return os.path.join(output_folder, file_type, f"{file_prefix(file_type)}_{month.strftime('%Y-%m')}.zst")


def count_lines_bytes(path):
	"""Decompress a whole zst file, which also checks its checksum, and count newlines and bytes."""
	lines = 0
	size = 0
	with open(path, 'rb') as file_handle:
		reader = zstandard.ZstdDecompressor(max_window_size=2**LONG_WINDOW_LOG).stream_reader(file_handle)
		while True:
			chunk = reader.read(READ_CHUNK)
			if not chunk:
				break
			lines += chunk.count(NEWLINE_ENCODED)
			size += len(chunk)
	return lines, size


def verify_month(month, input_folder, output_folder, file_type):
	"""Check the built month file holds exactly the lines and bytes of its minute files.

	Returns (ok, message). Decompressing the whole output also validates its zstd checksum, so a
	corrupt file raises zstandard.ZstdError from here instead of returning.
	"""
	expected_lines, expected_bytes = 0, 0
	for minute, minute_file_path in minute_file_paths(month, input_folder, file_type):
		lines, size = count_lines_bytes(minute_file_path)
		expected_lines += lines
		expected_bytes += size
		if minute.hour == 23 and minute.minute == 59:
			log.info(f"{file_type}: Verify counted minute files to {minute.strftime('%y-%m-%d')} : {expected_lines:,} : {expected_bytes:,}")

	output_path = month_output_path(month, output_folder, file_type)
	log.info(f"{file_type}: Verify decompressing {output_path}")
	actual_lines, actual_bytes = count_lines_bytes(output_path)
	message = (
		f"{output_path} has {actual_lines:,} lines and {actual_bytes:,} bytes, "
		f"the minute files have {expected_lines:,} lines and {expected_bytes:,} bytes")
	return actual_lines == expected_lines and actual_bytes == expected_bytes, message


def build_month(month, input_folder, output_folder, file_type, compression_level, threads=-1):
	total_objects = 0
	total_bytes = 0
	for minute, minute_file_path in minute_file_paths(month, input_folder, file_type):
		for obj, line, _ in utils.read_obj_zst_meta(minute_file_path):
			total_bytes += len(line.encode('utf-8'))
			total_bytes += 1

			total_objects += 1
			if total_objects % 1000000 == 0:
				log.info(f"{file_type}: Counting: {minute.strftime('%y-%m-%d_%H-%M')} : {total_objects:,} : {total_bytes:,}")

	log.info(f"{file_type}: Counting: {end_of_month(month).strftime('%y-%m-%d_%H-%M')} : {total_objects:,} : {total_bytes:,}")

	output_path = month_output_path(month, output_folder, file_type)
	output_handle = long_window_compressor(compression_level, threads).stream_writer(open(output_path, 'wb'), size=total_bytes)

	count_objects = 0
	count_bytes = 0
	for minute, minute_file_path in minute_file_paths(month, input_folder, file_type):
		for obj, line, _ in utils.read_obj_zst_meta(minute_file_path):
			line_encoded = line.encode('utf-8')
			count_bytes += len(line_encoded)
			count_bytes += 1
			output_handle.write(line_encoded)
			output_handle.write(NEWLINE_ENCODED)

			count_objects += 1
			if count_objects % 100000 == 0:
				log.info(f"{file_type}: Writing: {minute.strftime('%y-%m-%d_%H-%M')} : {count_objects:,}/{total_objects:,} : {count_bytes:,}/{total_bytes:,}")

	log.info(f"{file_type}: Writing: {end_of_month(month).strftime('%y-%m-%d_%H-%M')} : {count_objects:,}/{total_objects:,} : {count_bytes:,}/{total_bytes:,}")
	output_handle.close()


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Combine the minute files into a single month")
	parser.add_argument("--type", help="The object type, either comments or submissions", required=True)
	parser.add_argument("--month", help="The month to process, format YY-MM", required=True)
	parser.add_argument('--input', help='Input folder', required=True)
	parser.add_argument('--output', help='Output folder', required=True)
	parser.add_argument("--debug", help="Enable debug logging", action='store_const', const=True, default=False)
	parser.add_argument("--level", help="The compression ratio to output at", default="22")
	parser.add_argument("--threads", help="zstd worker threads, -1 for one per core, 0 for single threaded", default=-1, type=int)
	parser.add_argument("--verify", help="Only check an already built month against its minute files", action='store_const', const=True, default=False)
	args = parser.parse_args()

	if args.debug:
		discord_logging.set_level(logging.DEBUG)

	month = datetime.strptime(args.month, '%y-%m')
	level = int(args.level)

	log.info(f"Input folder: {args.input}")
	log.info(f"Output folder: {args.output}")
	log.info(f"Month: {args.month}")

	if args.verify:
		ok, message = verify_month(month, args.input, args.output, args.type)
		log.info(f"{'Verified' if ok else 'MISMATCH'}: {message}")
		sys.exit(0 if ok else 1)

	log.info(f"Compression level: {level}, threads: {args.threads}")
	build_month(
		month,
		args.input,
		args.output,
		args.type,
		level,
		args.threads
	)
