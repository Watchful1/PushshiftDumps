import json
from datetime import datetime
import utils
import discord_logging
import pymongo
import time
import sys

log = discord_logging.init_logging()


if __name__ == "__main__":
	mongo_address = sys.argv[1]  # 192.168.1.131
	client = pymongo.MongoClient(f"mongodb://{mongo_address}:27017", serverSelectionTimeoutMS=5000)
	log.info(f"Database connected at {mongo_address} on {client.admin.command('serverStatus')['host']}")

	count = 0
	start_time = time.time()
	start_date = int(datetime(2021, 6, 1).timestamp())
	cursor = client.reddit_database.submissions.aggregate(
		[
			{"$match": {"created_utc": {"$gt": start_date}}},
			{"$project": {"subreddit": 1, "over_18": {"$cond": ["$over_18", 1, 0]}}},
			{"$group": {"_id": "$subreddit", "countTotal": {"$count": {}}, "countNsfw": {"$sum": "$over_18"}}},
			{"$match": {"countTotal": {"$gt": 100}}},
		],
		allowDiskUse=True
	)
	log.info(f"Got cursor in {int(time.time() - start_time)} seconds")

	start_time = time.time()
	subreddits = []
	for subreddit in cursor:
		subreddit['percent'] = int((subreddit['countNsfw']/subreddit['countTotal'])*100)
		if subreddit['percent'] >= 10:
			subreddits.append(subreddit)
		count += 1
		if count % 100000 == 0:
			log.info(f"{count:,} in {int(time.time() - start_time)} seconds")

	log.info(f"{count:,} in {int(time.time() - start_time)} seconds")

	file_out = open(r"\\MYCLOUDPR4100\Public\reddit_final\subreddits.txt", 'w')
	for subreddit in sorted(subreddits, key=lambda item: (item['percent'], item['countTotal']), reverse=True):
		file_out.write(f"{subreddit['_id']: <22}{subreddit['countTotal']: <8}{subreddit['countNsfw']: <8}{subreddit['percent']}%\n")
	file_out.close()


# db.comments.createIndex({subreddit:1}) // remove
# db.comments.createIndex({subreddit:1, created_utc:1})
# db.comments.createIndex({author:1, created_utc:1})
# db.comments.createIndex({id:1})
# db.submissions.createIndex({subreddit:1, created_utc:1})
# db.submissions.createIndex({author:1, created_utc:1})
# db.submissions.createIndex({id:1})
# db.submissions.createIndex({created_utc:1})
# db.comments.createIndex({created_utc:1})
