import argparse
import zstandard
import os
import logging.handlers

log = logging.getLogger("bot")
log.setLevel(logging.INFO)
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s')
log_str_handler = logging.StreamHandler()
log_str_handler.setFormatter(log_formatter)
log.addHandler(log_str_handler)
if not os.path.exists("logs"):
	os.makedirs("logs")
log_file_handler = logging.handlers.RotatingFileHandler(os.path.join("logs", "bot.log"), maxBytes=1024*1024*16, backupCount=5)
log_file_handler.setFormatter(log_formatter)
log.addHandler(log_file_handler)

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description="Take all the zst files in the input folder, extract them and compress them again at the ratio specified")
	parser.add_argument("input", help="The input folder to read files from")
	parser.add_argument("output", help="The output folder to write files to")
	parser.add_argument("--level", help="The compression ratio to output at", default="3")
	args = parser.parse_args()

	log.info(f"Reading all files from {args.input}")

	files = []
	total_size = 0
	for file_name in os.listdir(args.input):
		file_path = os.path.join(args.input, file_name)
		if file_name.endswith(".zst") and os.path.isfile(file_path):
			file_size = os.stat(file_path).st_size
			total_size += file_size
			files.append((file_name, file_size))
			if len(files) % 1000 == 0:
				log.info(f"Loaded {len(files)} files")
	log.info(f"Loaded {len(files)} files of total size {total_size:,}")

	level = int(args.level)
	log.info(f"Writing files out to {args.output} at ratio {level}")
	if not os.path.exists(args.output):
		os.makedirs(args.output)

	compressed_bytes_read = 0
	uncompressed_bytes_read = 0
	bytes_written = 0
	files_read = 0

	decompressor = zstandard.ZstdDecompressor(max_window_size=2**31)
	compressor = zstandard.ZstdCompressor(level=level, threads=-1)
	for file_name, file_size in files:
		input_path = os.path.join(args.input, file_name)
		output_path = os.path.join(args.output, file_name)
		with open(input_path, 'rb') as input_handle, open(output_path, "wb") as output_handle:
			compression_reader = decompressor.stream_reader(input_handle)
			read_count, write_count = compressor.copy_stream(compression_reader, output_handle)

		compressed_bytes_read += file_size
		uncompressed_bytes_read += read_count
		bytes_written += write_count
		files_read += 1
		log.info(f"{files_read:,}/{len(files):,} : {(compressed_bytes_read / (2**30)):.2f} gb of {(total_size / (2**30)):.2f} gb compressed to {(bytes_written / (2**30)):.2f} gb : {bytes_written /compressed_bytes_read:.3f}")
