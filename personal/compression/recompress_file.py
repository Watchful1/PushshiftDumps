import argparse
import zstandard
import utils
import discord_logging
import time
import sys

sys.path.append('personal')

log = discord_logging.init_logging()

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Take all the zst files in the input folder, extract them and compress them again at the ratio specified")
	parser.add_argument("input", help="The input file")
	parser.add_argument("output", help="The output file")
	parser.add_argument("--level", help="The compression ratio to output at", default="3")
	args = parser.parse_args()

	log.info(f"Input file {args.input}")
	log.info(f"Output file {args.output}")

	total_objects = 0
	total_bytes = 0
	for obj, line, _ in utils.read_obj_zst_meta(args.input):
		total_bytes += len(line.encode('utf-8'))
		total_bytes += 1

		total_objects += 1
		if total_objects % 1000000 == 0:
			log.info(f"{total_objects:,} : {total_bytes:,}")

	log.info(f"{total_objects:,} : {total_bytes:,}")

	for threads in range(-1, 21):
		decompressor = zstandard.ZstdDecompressor(max_window_size=2**31)
		compressor = zstandard.ZstdCompressor(level=22, write_content_size=True, write_checksum=True, threads=threads)
		start_time = time.time()
		with open(args.input, 'rb') as input_handle, open(args.output, "wb") as output_handle:
			compression_reader = decompressor.stream_reader(input_handle)
			read_count, write_count = compressor.copy_stream(compression_reader, output_handle, size=total_bytes)
		seconds = time.time() - start_time

		log.info(f"{read_count:,} to {write_count:,} in {seconds:,.2f} with {threads} threads")

	# compressed_bytes_read += file_size
	# uncompressed_bytes_read += read_count
	# bytes_written += write_count
	# log.info(f"{files_read:,}/{len(files):,} : {(compressed_bytes_read / (2**30)):.2f} gb of {(total_size / (2**30)):.2f} gb compressed to {(bytes_written / (2**30)):.2f} gb : {bytes_written /compressed_bytes_read:.3f}")
