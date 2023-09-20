import zstandard
import json


def read_obj_zst(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")
			for line in lines[:-1]:
				if line == "":
					continue
				yield json.loads(line.strip())

			buffer = lines[-1]
		reader.close()


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
	chunk = reader.read(chunk_size)
	bytes_read += chunk_size
	if previous_chunk is not None:
		chunk = previous_chunk + chunk
	try:
		return chunk.decode()
	except UnicodeDecodeError:
		if bytes_read > max_window_size:
			raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
		return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_obj_zst_meta(file_name):
	with open(file_name, 'rb') as file_handle:
		buffer = ''
		reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
		while True:
			chunk = read_and_decode(reader, 2**27, (2**29) * 2)
			if not chunk:
				break
			lines = (buffer + chunk).split("\n")

			for line in lines[:-1]:
				line = line.strip()
				try:
					json_object = json.loads(line)
				except (KeyError, json.JSONDecodeError) as err:
					continue
				yield json_object, line, file_handle.tell()

			buffer = lines[-1]
		reader.close()


class OutputZst:
	def __init__(self, file_name):
		output_file = open(file_name, 'wb')
		self.writer = zstandard.ZstdCompressor().stream_writer(output_file)

	def write(self, line):
		encoded_line = line.encode('utf-8')
		self.writer.write(encoded_line)

	def close(self):
		self.writer.close()

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_value, exc_traceback):
		self.close()
		return True


def base36encode(integer: int) -> str:
	chars = '0123456789abcdefghijklmnopqrstuvwxyz'
	sign = '-' if integer < 0 else ''
	integer = abs(integer)
	result = ''
	while integer > 0:
		integer, remainder = divmod(integer, 36)
		result = chars[remainder] + result
	return sign + result


def base36decode(base36: str) -> int:
	return int(base36, 36)


def merge_lowest_highest_id(str_id, lowest_id, highest_id):
	int_id = base36decode(str_id)
	if lowest_id is None or int_id < lowest_id:
		lowest_id = int_id
	if highest_id is None or int_id > highest_id:
		highest_id = int_id
	return lowest_id, highest_id


def chunk_list(items, chunk_size):
	for i in range(0, len(items), chunk_size):
		yield items[i:i + chunk_size]
