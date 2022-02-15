import json

import utils
import discord_logging
import pymongo
import time
import sys
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	mongo_address = sys.argv[1]  # 192.168.1.131
	client = pymongo.MongoClient(f"mongodb://{mongo_address}:27017", serverSelectionTimeoutMS=5000)
	log.info(f"Database connected at {mongo_address} on {client.admin.command('serverStatus')['host']}")

	subreddits = [
		"PersonalFinanceCanada"
	]
	start_date = datetime(2020, 1, 1)
	end_date = datetime(2021, 1, 1)

	for subreddit in subreddits:
		count = 0
		start_time = time.time()
		cursor = client.reddit_database.comments.find(
			filter={"subreddit": subreddit, "created_utc": {"$gte": int(start_date.timestamp()), "$lt": int(end_date.timestamp())}},
			projection={'_id': False},
			sort=[('created_utc', pymongo.ASCENDING)]
		)
		log.info(f"Got cursor in {int(time.time() - start_time)} seconds")

		output_writer = utils.OutputZst(r"\\MYCLOUDPR4100\Public\reddit_final\{0}_comments.zst".format(subreddit))
		start_time = time.time()
		for comment in cursor:
			count += 1
			output_writer.write(json.dumps(comment, separators=(',', ':')))
			output_writer.write("\n")
			if count % 10000 == 0:
				log.info(f"{count:,} through {datetime.utcfromtimestamp(int(comment['created_utc'])).strftime('%Y-%m-%d %H:%M:%S')} in {int(time.time() - start_time)} seconds r/{subreddit}")

		output_writer.close()
		log.info(f"{count:,} in {int(time.time() - start_time)} seconds r/{subreddit}")


# db.comments.createIndex({subreddit:1}) // remove
# db.comments.createIndex({subreddit:1, created_utc:1})
# db.comments.createIndex({author:1, created_utc:1})
# db.comments.createIndex({id:1})
# db.submissions.createIndex({subreddit:1, created_utc:1})
# db.submissions.createIndex({author:1, created_utc:1})
# db.submissions.createIndex({id:1})
# db.submissions.createIndex({created_utc:1})
# db.comments.createIndex({created_utc:1})
