import utils
import discord_logging
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	day = None
	day_comments = 0
	for comment in utils.read_obj_zst(r"C:\Users\greg\Desktop\Drive\pushshift\haley0530\chatbots_submissions.zst"):
		created_day = datetime.utcfromtimestamp(int(comment['created_utc'])).strftime("%y-%m-%d")
		if day is None:
			day = created_day
		if day != created_day:
			log.info(f"{day}	{day_comments}")
			day_comments = 0
			day = created_day
		day_comments += 1

	log.info(f"{day}	{day_comments}")
