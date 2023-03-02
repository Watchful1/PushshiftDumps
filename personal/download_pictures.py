import utils
import discord_logging
from datetime import datetime
from collections import defaultdict
from urllib.parse import urlparse

log = discord_logging.init_logging()

if __name__ == "__main__":
	domains = defaultdict(list)
	lines = 0
	for submission in utils.read_obj_zst(r"\\MYCLOUDPR4100\Public\guessmybf_submissions.zst"):
		if submission['is_self']:
			continue

		domain = urlparse(submission['url']).netloc
		domains[domain].append(submission['url'])
		lines += 1

	log.info(f"{lines}")
