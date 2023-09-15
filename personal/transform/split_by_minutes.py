import discord_logging
import os
import zstandard
from datetime import datetime
import json

log = discord_logging.init_logging()

import utils

NEWLINE_ENCODED = "\n".encode('utf-8')


if __name__ == "__main__":
	input_file = r"\\MYCLOUDPR4100\Public\RC_2023-05.zst"
	output_folder = r"\\MYCLOUDPR4100\Public\ingest\download"
	file_type = "comments" if "RC" in input_file else "submissions"

	log.info(f"Input: {input_file} - Output: {output_folder}")
	previous_minute, output_handle, created_utc = None, None, None
	count_objects, count_minute = 0, 0
	for obj in utils.read_obj_zst(input_file):
		created_utc = datetime.utcfromtimestamp(obj["created_utc"])
		current_minute = created_utc.replace(second=0)

		if previous_minute is None or current_minute > previous_minute:
			log.info(f"{created_utc.strftime('%y-%m-%d_%H-%M')}: {count_objects:,} : {count_minute: ,}")
			previous_minute = current_minute
			count_minute = 0
			if output_handle is not None:
				output_handle.close()

			output_path = os.path.join(output_folder, file_type, created_utc.strftime('%y-%m-%d'))
			if not os.path.exists(output_path):
				os.makedirs(output_path)
			output_path = os.path.join(output_path, f"{('RC' if file_type == 'comments' else 'RS')}_{created_utc.strftime('%y-%m-%d_%H-%M')}.zst")
			output_handle = zstandard.ZstdCompressor().stream_writer(open(output_path, 'wb'))

		count_objects += 1
		count_minute += 1
		output_handle.write(json.dumps(obj, sort_keys=True).encode('utf-8'))
		output_handle.write(NEWLINE_ENCODED)

	log.info(f"{created_utc.strftime('%y-%m-%d_%H-%M')}: {count_objects:,} : {count_minute: ,}")
	if output_handle is not None:
		output_handle.close()
