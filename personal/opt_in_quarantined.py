import asyncpraw
import requests
import asyncio


async def opt_in(reddit, subreddit_name):
	subreddit = await reddit.subreddit(subreddit_name)
	await subreddit.quaran.opt_in()


async def main(subreddits):
	reddit = asyncpraw.Reddit("Watchful12")
	for subreddit_name in subreddits:
		print(f"r/{subreddit_name}")
		try:
			subreddit = await reddit.subreddit(subreddit_name)
			await subreddit.quaran.opt_in()
		except Exception as err:
			print(f"Error opting into r/{subreddit_name} : {err}")
	await reddit.close()


if __name__ == "__main__":
	subreddits = requests.get("https://pastebin.com/raw/WKi36t1w").text.split("\r\n")
	asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
	asyncio.run(main(subreddits))
