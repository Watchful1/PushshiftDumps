import utils
import discord_logging
from datetime import datetime

log = discord_logging.init_logging()


if __name__ == "__main__":
	day = None
	day_comments, day_comments_with_score = 0, 0
	for comment in utils.read_obj_zst(r"\\MYCLOUDPR4100\Public\reddit\subreddits23\antiwork_comments.zst"):
		created_day = datetime.utcfromtimestamp(int(comment['created_utc'])).strftime("%y-%m-%d")
		if day is None:
			day = created_day
		if day != created_day:
			log.info(f"{day}	{day_comments}	{day_comments_with_score}	{int((day_comments_with_score / day_comments) * 100):.2}%")
			day_comments, day_comments_with_score = 0, 0
			day = created_day
		day_comments += 1
		if comment['score'] != 1:
			day_comments_with_score += 1

	log.info(f"{day}	{day_comments}	{day_comments_with_score}	{int((day_comments_with_score / day_comments) * 100):.2}%")
