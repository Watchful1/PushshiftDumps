import re
import sys
from enum import Enum
import discord_logging
import zstandard
import json
from datetime import datetime
import requests
import time

import counters

log = discord_logging.get_logger(init=True)


def parse_ingest_string(ingest_string):
	ingest_ids = []
	for char in ingest_string:
		ingest_ids.append(char)
	return ingest_ids


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


def next_string_id(string_id):
	return base36encode(base36decode(string_id) + 1)


def get_next_hundred_ids(start_id):
	start_num = base36decode(start_id)
	ids = []
	id_num = -1
	for id_num in range(start_num, start_num + 100):
		ids.append(base36encode(id_num))
	return ids, base36encode(id_num)


class FieldAction(Enum):
	OVERWRITE = 1
	OVERWRITE_NOT_NONE = 2
	OVERWRITE_IF_NONE = 3
	DONT_OVERWRITE = 4
	DELETE = 5
	SPECIAL = 6
	SPECIAL_NO_OVERWRITE = 7
	ALLOW = 8
	ALLOW_EMPTY = 9


class ObjectType(Enum):
	COMMENT = 1
	SUBMISSION = 2


field_actions = {
	ObjectType.COMMENT: {
		"all_awardings": FieldAction.OVERWRITE_NOT_NONE,
		"approved": FieldAction.DELETE,
		"approved_at_utc": FieldAction.SPECIAL_NO_OVERWRITE,
		"approved_by": FieldAction.SPECIAL_NO_OVERWRITE,
		"archived": FieldAction.OVERWRITE,
		"associated_award": FieldAction.ALLOW_EMPTY,
		"author": FieldAction.OVERWRITE_IF_NONE,
		"author_cakeday": FieldAction.DONT_OVERWRITE,
		"author_flair_background_color": FieldAction.OVERWRITE_IF_NONE,
		"author_flair_css_class": FieldAction.OVERWRITE_IF_NONE,
		"author_flair_richtext": FieldAction.OVERWRITE_IF_NONE,
		"author_flair_template_id": FieldAction.OVERWRITE_IF_NONE,
		"author_flair_text": FieldAction.OVERWRITE_IF_NONE,
		"author_flair_text_color": FieldAction.OVERWRITE_IF_NONE,
		"author_flair_type": FieldAction.OVERWRITE_IF_NONE,
		"author_fullname": FieldAction.OVERWRITE_IF_NONE,
		"author_is_blocked": FieldAction.SPECIAL_NO_OVERWRITE,
		"author_patreon_flair": FieldAction.OVERWRITE,
		"author_premium": FieldAction.OVERWRITE,
		"awarders": FieldAction.OVERWRITE_IF_NONE,
		"ban_note": FieldAction.DELETE,
		"banned_at_utc": FieldAction.SPECIAL_NO_OVERWRITE,
		"banned_by": FieldAction.SPECIAL_NO_OVERWRITE,
		"body": FieldAction.SPECIAL,
		"body_html": FieldAction.DELETE,
		"body_sha1": FieldAction.OVERWRITE_NOT_NONE,
		"can_gild": FieldAction.OVERWRITE,
		"can_mod_post": FieldAction.SPECIAL_NO_OVERWRITE,
		"collapsed": FieldAction.OVERWRITE,
		"collapsed_because_crowd_control": FieldAction.ALLOW_EMPTY,
		"collapsed_reason": FieldAction.OVERWRITE,
		"collapsed_reason_code": FieldAction.OVERWRITE,
		"comment_type": FieldAction.ALLOW_EMPTY,
		"controversiality": FieldAction.OVERWRITE,
		"created": FieldAction.OVERWRITE_IF_NONE,
		"created_utc": FieldAction.ALLOW,
		"distinguished": FieldAction.OVERWRITE,
		"downs": FieldAction.OVERWRITE_IF_NONE,
		"editable": FieldAction.OVERWRITE,
		"edited": FieldAction.OVERWRITE_NOT_NONE,
		"gilded": FieldAction.OVERWRITE_NOT_NONE,
		"gildings": FieldAction.OVERWRITE_NOT_NONE,
		"id": FieldAction.ALLOW,
		"ignore_reports": FieldAction.DELETE,
		"is_submitter": FieldAction.DONT_OVERWRITE,
		"likes": FieldAction.ALLOW_EMPTY,
		"link_id": FieldAction.ALLOW,
		"locked": FieldAction.OVERWRITE,
		"media_metadata": FieldAction.OVERWRITE,
		"mod_note": FieldAction.ALLOW_EMPTY,
		"mod_reason_by": FieldAction.ALLOW_EMPTY,
		"mod_reason_title": FieldAction.ALLOW_EMPTY,
		"mod_reports": FieldAction.SPECIAL_NO_OVERWRITE,
		"mod_reports_dismissed": FieldAction.SPECIAL_NO_OVERWRITE,
		"name": FieldAction.OVERWRITE_IF_NONE,
		"nest_level": FieldAction.OVERWRITE_NOT_NONE,
		"no_follow": FieldAction.OVERWRITE,
		"num_reports": FieldAction.SPECIAL_NO_OVERWRITE,
		"parent_id": FieldAction.OVERWRITE_IF_NONE,
		"permalink": FieldAction.DONT_OVERWRITE,
		"removal_reason": FieldAction.SPECIAL,
		"removed": FieldAction.DELETE,
		"replies": FieldAction.OVERWRITE_IF_NONE,
		"report_reasons": FieldAction.SPECIAL_NO_OVERWRITE,
		"retrieved_on": FieldAction.SPECIAL,
		"retrieved_utc": FieldAction.SPECIAL,
		"saved": FieldAction.SPECIAL_NO_OVERWRITE,
		"score": FieldAction.OVERWRITE_NOT_NONE,
		"score_hidden": FieldAction.OVERWRITE,
		"send_replies": FieldAction.OVERWRITE,
		"spam": FieldAction.DELETE,
		"stickied": FieldAction.OVERWRITE,
		"subreddit": FieldAction.OVERWRITE_NOT_NONE,
		"subreddit_id": FieldAction.ALLOW,
		"subreddit_name_prefixed": FieldAction.OVERWRITE_NOT_NONE,
		"subreddit_type": FieldAction.DONT_OVERWRITE,
		"top_awarded_type": FieldAction.ALLOW_EMPTY,
		"total_awards_received": FieldAction.OVERWRITE_NOT_NONE,
		"treatment_tags": FieldAction.OVERWRITE_NOT_NONE,
		"unrepliable_reason": FieldAction.ALLOW_EMPTY,
		"ups": FieldAction.OVERWRITE_NOT_NONE,
		"user_reports": FieldAction.SPECIAL_NO_OVERWRITE,
		"user_reports_dismissed": FieldAction.SPECIAL_NO_OVERWRITE,
		"updated_on": FieldAction.SPECIAL,
		"updated_utc": FieldAction.SPECIAL,
		"utc_datetime_str": FieldAction.DELETE,
	},
	ObjectType.SUBMISSION: {
		"ad_promoted_user_posts": FieldAction.ALLOW_EMPTY,
		"ad_supplementary_text_md": FieldAction.ALLOW,
		"adserver_click_url": FieldAction.ALLOW_EMPTY,
		"adserver_imp_pixel": FieldAction.ALLOW_EMPTY,
		"all_awardings": FieldAction.OVERWRITE_NOT_NONE,
		"allow_live_comments": FieldAction.OVERWRITE,
		"app_store_data": FieldAction.ALLOW_EMPTY,
		"approved": FieldAction.DELETE,
		"approved_at_utc": FieldAction.SPECIAL_NO_OVERWRITE,
		"approved_by": FieldAction.SPECIAL_NO_OVERWRITE,
		"archived": FieldAction.ALLOW_EMPTY,
		"author": FieldAction.OVERWRITE_IF_NONE,
		"author_cakeday": FieldAction.DONT_OVERWRITE,
		"author_flair_background_color": FieldAction.OVERWRITE_NOT_NONE,
		"author_flair_css_class": FieldAction.OVERWRITE_NOT_NONE,
		"author_flair_richtext": FieldAction.OVERWRITE_NOT_NONE,
		"author_flair_template_id": FieldAction.OVERWRITE_NOT_NONE,
		"author_flair_text": FieldAction.OVERWRITE_NOT_NONE,
		"author_flair_text_color": FieldAction.OVERWRITE_NOT_NONE,
		"author_flair_type": FieldAction.OVERWRITE_NOT_NONE,
		"author_fullname": FieldAction.OVERWRITE_NOT_NONE,
		"author_id": FieldAction.OVERWRITE_NOT_NONE,
		"author_is_blocked": FieldAction.SPECIAL_NO_OVERWRITE,
		"author_patreon_flair": FieldAction.OVERWRITE,
		"author_premium": FieldAction.OVERWRITE,
		"awarders": FieldAction.ALLOW_EMPTY,
		"ban_note": FieldAction.DELETE,
		"banned_at_utc": FieldAction.SPECIAL_NO_OVERWRITE,
		"banned_by": FieldAction.SPECIAL_NO_OVERWRITE,
		"call_to_action": FieldAction.OVERWRITE,
		"campaign_id": FieldAction.ALLOW_EMPTY,
		"can_gild": FieldAction.OVERWRITE,
		"can_mod_post": FieldAction.SPECIAL_NO_OVERWRITE,
		"category": FieldAction.OVERWRITE_NOT_NONE,
		"clicked": FieldAction.SPECIAL_NO_OVERWRITE,
		"collections": FieldAction.OVERWRITE_NOT_NONE,
		"content_categories": FieldAction.ALLOW,
		"contest_mode": FieldAction.OVERWRITE,
		"created": FieldAction.OVERWRITE_IF_NONE,
		"created_utc": FieldAction.ALLOW,
		"crosspost_parent": FieldAction.ALLOW,
		"crosspost_parent_list": FieldAction.OVERWRITE_NOT_NONE,
		"discussion_type": FieldAction.ALLOW,
		"distinguished": FieldAction.OVERWRITE,
		"domain": FieldAction.OVERWRITE_NOT_NONE,
		"domain_override": FieldAction.OVERWRITE_NOT_NONE,
		"downs": FieldAction.SPECIAL_NO_OVERWRITE,
		"edited": FieldAction.OVERWRITE,
		"embed_type": FieldAction.ALLOW_EMPTY,
		"embed_url": FieldAction.ALLOW_EMPTY,
		"event_end": FieldAction.OVERWRITE_NOT_NONE,
		"event_is_live": FieldAction.OVERWRITE_NOT_NONE,
		"event_start": FieldAction.OVERWRITE_NOT_NONE,
		"events": FieldAction.ALLOW_EMPTY,
		"eventsOnRender": FieldAction.ALLOW_EMPTY,
		"gallery_data": FieldAction.OVERWRITE_NOT_NONE,
		"gilded": FieldAction.OVERWRITE_NOT_NONE,
		"gildings": FieldAction.OVERWRITE_NOT_NONE,
		"hidden": FieldAction.ALLOW_EMPTY,
		"hide_score": FieldAction.OVERWRITE,
		"href_url": FieldAction.DONT_OVERWRITE,
		"id": FieldAction.ALLOW,
		"ignore_reports": FieldAction.DELETE,
		"impression_id": FieldAction.ALLOW_EMPTY,
		"impression_id_str": FieldAction.ALLOW_EMPTY,
		"is_blank": FieldAction.ALLOW_EMPTY,
		"is_created_from_ads_ui": FieldAction.ALLOW,
		"is_crosspostable": FieldAction.OVERWRITE,
		"is_gallery": FieldAction.ALLOW,
		"is_meta": FieldAction.OVERWRITE,
		"is_original_content": FieldAction.OVERWRITE,
		"is_reddit_media_domain": FieldAction.OVERWRITE,
		"is_robot_indexable": FieldAction.OVERWRITE,
		"is_self": FieldAction.DONT_OVERWRITE,
		"is_survey_ad": FieldAction.ALLOW_EMPTY,
		"is_video": FieldAction.ALLOW,
		"likes": FieldAction.ALLOW_EMPTY,
		"link_flair_background_color": FieldAction.OVERWRITE_NOT_NONE,
		"link_flair_css_class": FieldAction.OVERWRITE_NOT_NONE,
		"link_flair_richtext": FieldAction.OVERWRITE_NOT_NONE,
		"link_flair_template_id": FieldAction.OVERWRITE_NOT_NONE,
		"link_flair_text": FieldAction.OVERWRITE_NOT_NONE,
		"link_flair_text_color": FieldAction.OVERWRITE_NOT_NONE,
		"link_flair_type": FieldAction.OVERWRITE_NOT_NONE,
		"locked": FieldAction.OVERWRITE,
		"media": FieldAction.OVERWRITE_NOT_NONE,
		"media_embed": FieldAction.OVERWRITE_NOT_NONE,
		"media_metadata": FieldAction.OVERWRITE_NOT_NONE,
		"media_only": FieldAction.OVERWRITE,
		"mobile_ad_url": FieldAction.ALLOW,
		"mod_note": FieldAction.ALLOW_EMPTY,
		"mod_reason_by": FieldAction.ALLOW_EMPTY,
		"mod_reason_title": FieldAction.ALLOW_EMPTY,
		"mod_reports": FieldAction.SPECIAL_NO_OVERWRITE,
		"name": FieldAction.OVERWRITE_IF_NONE,
		"no_follow": FieldAction.OVERWRITE,
		"num_comments": FieldAction.OVERWRITE_NOT_NONE,
		"num_crossposts": FieldAction.OVERWRITE,
		"num_reports": FieldAction.SPECIAL_NO_OVERWRITE,
		"original_link": FieldAction.ALLOW_EMPTY,
		"outbound_link": FieldAction.ALLOW_EMPTY,
		"over_18": FieldAction.OVERWRITE,
		"parent_whitelist_status": FieldAction.OVERWRITE,
		"permalink": FieldAction.DONT_OVERWRITE,
		"pinned": FieldAction.ALLOW_EMPTY,
		"poll_data": FieldAction.OVERWRITE_NOT_NONE,
		"post_hint": FieldAction.OVERWRITE,
		"preview": FieldAction.OVERWRITE_NOT_NONE,
		"priority_id": FieldAction.ALLOW_EMPTY,
		"product_ids": FieldAction.ALLOW_EMPTY,
		"promo_layout": FieldAction.OVERWRITE,
		"promoted": FieldAction.ALLOW_EMPTY,
		"promoted_by": FieldAction.ALLOW_EMPTY,
		"promoted_display_name": FieldAction.ALLOW_EMPTY,
		"promoted_url": FieldAction.ALLOW_EMPTY,
		"pwls": FieldAction.OVERWRITE,
		"quarantine": FieldAction.DONT_OVERWRITE,
		"removal_reason": FieldAction.SPECIAL,
		"removed": FieldAction.DELETE,
		"removed_by": FieldAction.SPECIAL_NO_OVERWRITE,
		"removed_by_category": FieldAction.OVERWRITE,
		"report_reasons": FieldAction.SPECIAL_NO_OVERWRITE,
		"retrieved_on": FieldAction.SPECIAL,
		"retrieved_utc": FieldAction.SPECIAL,
		"saved": FieldAction.SPECIAL_NO_OVERWRITE,
		"score": FieldAction.OVERWRITE_NOT_NONE,
		"secure_media": FieldAction.OVERWRITE_NOT_NONE,
		"secure_media_embed": FieldAction.OVERWRITE_NOT_NONE,
		"selftext": FieldAction.SPECIAL,
		"selftext_html": FieldAction.DELETE,
		"send_replies": FieldAction.OVERWRITE,
		"show_media": FieldAction.ALLOW,
		"sk_ad_network_data": FieldAction.ALLOW_EMPTY,
		"spam": FieldAction.DELETE,
		"spoiler": FieldAction.OVERWRITE,
		"stickied": FieldAction.OVERWRITE,
		"subcaption": FieldAction.OVERWRITE,
		"subreddit": FieldAction.ALLOW,
		"subreddit_id": FieldAction.ALLOW,
		"subreddit_name_prefixed": FieldAction.ALLOW,
		"subreddit_subscribers": FieldAction.OVERWRITE_IF_NONE,
		"subreddit_type": FieldAction.DONT_OVERWRITE,
		"suggested_sort": FieldAction.OVERWRITE,
		"third_party_trackers": FieldAction.ALLOW_EMPTY,
		"third_party_tracking": FieldAction.ALLOW_EMPTY,
		"third_party_tracking_2": FieldAction.ALLOW_EMPTY,
		"thumbnail": FieldAction.OVERWRITE_NOT_NONE,
		"thumbnail_height": FieldAction.OVERWRITE_NOT_NONE,
		"thumbnail_width": FieldAction.OVERWRITE_NOT_NONE,
		"title": FieldAction.DONT_OVERWRITE,
		"top_awarded_type": FieldAction.OVERWRITE,
		"total_awards_received": FieldAction.OVERWRITE_NOT_NONE,
		"treatment_tags": FieldAction.OVERWRITE_NOT_NONE,
		"updated_on": FieldAction.SPECIAL,
		"updated_utc": FieldAction.SPECIAL,
		"ups": FieldAction.OVERWRITE_NOT_NONE,
		"upvote_ratio": FieldAction.OVERWRITE,
		"url": FieldAction.OVERWRITE_NOT_NONE,
		"url_overridden_by_dest": FieldAction.OVERWRITE_NOT_NONE,
		"user_reports": FieldAction.SPECIAL_NO_OVERWRITE,
		"user_reports_dismissed": FieldAction.SPECIAL_NO_OVERWRITE,
		"utc_datetime_str": FieldAction.DELETE,
		"view_count": FieldAction.ALLOW_EMPTY,
		"visited": FieldAction.SPECIAL_NO_OVERWRITE,
		"whitelist_status": FieldAction.OVERWRITE,
		"wls": FieldAction.OVERWRITE,
	},
}


def is_empty(value):
	return value is None \
		or value == "" \
		or value == "[deleted]" \
		or value == "[removed]" \
		or value == [] \
		or value == {} \
		or value is False \
		or value == 0


def replace(match):
	if match.group(0) == "amp;": return ""
	if match.group(0) == "&lt;": return "<"
	if match.group(0) == "&gt;": return ">"
	log.warning(f"Unknown group: {match}")
	sys.exit(2)


unencode_regex = re.compile(r"amp;|&lt;|&gt;")


def merge_fields(existing_obj, new_obj, obj_type):
	unmatched_field = False
	type_actions = field_actions[obj_type]
	for key, new_value in new_obj.items():
		action = type_actions.get(key)

		original_value = existing_obj.get(key)
		if new_value != original_value:
			if isinstance(new_value, str) and unencode_regex.search(new_value):
				new_value_no_encode = unencode_regex.sub(replace, new_value)
				if new_value_no_encode == original_value:
					continue
			if action == FieldAction.OVERWRITE:
				existing_obj[key] = new_value
			elif action == FieldAction.OVERWRITE_NOT_NONE:
				if not is_empty(new_value):
					existing_obj[key] = new_value
			elif action == FieldAction.OVERWRITE_IF_NONE:
				if is_empty(original_value):
					existing_obj[key] = new_value
			elif action == FieldAction.SPECIAL:
				if key == "body":
					if not is_empty(new_value):
						if 'previous_body' in existing_obj:
							existing_obj['previous_body'] = original_value
						existing_obj['body'] = new_value
				elif key == "selftext":
					if not is_empty(new_value):
						if 'previous_selftext' not in existing_obj:
							existing_obj['previous_selftext'] = original_value
						existing_obj['selftext'] = new_value
				elif key == "removal_reason" and new_value in ["legal", None]:
					existing_obj[key] = new_value
				elif key in ["retrieved_on", "retrieved_utc"]:
					prev_retrieved_on = existing_obj["retrieved_on"]
					if new_value < prev_retrieved_on:
						existing_obj["retrieved_on"] = new_value
						existing_obj["updated_on"] = prev_retrieved_on
					if new_value > prev_retrieved_on:
						existing_obj["updated_on"] = new_value
				elif key in ["updated_on", "updated_utc"]:
					if new_value > existing_obj["updated_on"]:
						existing_obj["updated_on"] = new_value
				else:
					log.info(f"{new_obj['id']} unmatched special: {key}: {original_value} != {new_value}")
					unmatched_field = True
			elif action == FieldAction.DELETE or action == FieldAction.DONT_OVERWRITE or action == FieldAction.SPECIAL_NO_OVERWRITE:
				pass
			else:
				log.info(f"{new_obj['id']} unmatched no action: {key}|{action}: {original_value} != {new_value}")
				unmatched_field = True
		elif action is None:
			log.info(f"{new_obj['id']} matched no action: {key}: {new_value}")
			unmatched_field = True

	return unmatched_field


def parse_fields(new_obj, obj_type):
	keys_to_delete = []
	keys_to_add = []
	unmatched_field = False
	type_actions = field_actions[obj_type]
	for key, new_value in new_obj.items():
		action = type_actions.get(key)
		if action is not None:
			if action == FieldAction.DELETE:
				keys_to_delete.append(key)
			elif action == FieldAction.ALLOW_EMPTY:
				if not is_empty(new_value):
					log.info(f"{new_obj['id']} not empty: {key}: {new_value}")
					unmatched_field = True
					keys_to_delete.append(key)
			elif action == FieldAction.SPECIAL:
				if key in ["retrieved_on", "body", "selftext", "updated_on"]:
					pass
				elif key == "removal_reason" and new_value in ["legal", None]:
					pass
				elif key == "retrieved_utc":
					keys_to_add.append(("retrieved_on", new_value))
					keys_to_delete.append(key)
				elif key == "updated_utc":
					keys_to_add.append(("updated_on", new_value))
					keys_to_delete.append(key)
				else:
					log.info(f"{new_obj['id']} special no match: {key}: {new_value}")
					unmatched_field = True
					keys_to_delete.append(key)
			elif action == FieldAction.SPECIAL_NO_OVERWRITE:
				if key in ["can_mod_post", "saved", "clicked", "visited", "author_is_blocked"]:
					new_obj[key] = False
				elif key in ["banned_at_utc", "banned_by", "approved_at_utc", "approved_by", "user_reports_dismissed", "mod_reports_dismissed", "removed_by"]:
					new_obj[key] = None
				elif key in ["num_reports", "downs"]:
					new_obj[key] = 0
				elif key in ["report_reasons", "user_reports", "mod_reports"]:
					new_obj[key] = []
				else:
					log.info(f"{new_obj['id']} special no overwrite no match: {key}: {new_value}")
					unmatched_field = True
					keys_to_delete.append(key)
		else:
			log.info(f"{new_obj['id']} no action: {key}: {new_value}")
			unmatched_field = True

	for key in keys_to_delete:
		del new_obj[key]

	for key, value in keys_to_add:
		new_obj[key] = value

	if 'retrieved_on' not in new_obj:
		new_obj['retrieved_on'] = int(datetime.utcnow().timestamp())

	return unmatched_field


def merge_lowest_highest_id(str_id, lowest_id, highest_id):
	int_id = base36decode(str_id)
	if lowest_id is None or int_id < lowest_id:
		lowest_id = int_id
	if highest_id is None or int_id > highest_id:
		highest_id = int_id
	return lowest_id, highest_id


async def record_rate_limits(reddit, client):
	reddit_user = await reddit.user.me()
	remaining = int(reddit._core._rate_limiter.remaining)
	used = int(reddit._core._rate_limiter.used)
	counters.rate_requests_remaining.labels(username=reddit_user.name, client=client).set(remaining)
	counters.rate_requests_used.labels(username=reddit_user.name, client=client).set(used)

	reset_timestamp = reddit._core._rate_limiter.reset_timestamp
	seconds_to_reset = (datetime.utcfromtimestamp(reset_timestamp) - datetime.utcnow()).total_seconds()
	counters.rate_seconds_remaining.labels(username=reddit_user.name, client=client).set(int(seconds_to_reset))
	window_size = int(reddit._core._rate_limiter.window_size) if reddit._core._rate_limiter.window_size is not None else reddit._core._rate_limiter.window_size
	time_to_next_request = max((datetime.utcnow() - datetime.utcfromtimestamp(reddit._core._rate_limiter.next_request_timestamp)).total_seconds(), 0)
	#log.info(f"Rate: u/{reddit_user.name}: {window_size} : {remaining} : {used} : {seconds_to_reset:.2f} : {time_to_next_request:.3f} ")

	return


def chunk_list(items, chunk_size):
	for i in range(0, len(items), chunk_size):
		yield items[i:i + chunk_size]


def query_pushshift(ids, bearer, object_type):
	object_name = "comment" if object_type == ObjectType.COMMENT else "submission"
	url = f"https://api.pushshift.io/reddit/{object_name}/search?limit=1000&ids={','.join(ids)}"
	log.debug(f"pushshift query: {url}")
	response = None
	for i in range(4):
		response = requests.get(url, headers={
			'User-Agent': "In script by /u/Watchful1",
			'Authorization': f"Bearer {bearer}"})
		if response.status_code == 200:
			break
		if response.status_code == 403:
			log.warning(f"Pushshift unauthorized, aborting")
			sys.exit(2)
		time.sleep(2)
	if response.status_code != 200:
		log.warning(f"4 requests failed with status code {response.status_code}")
	return response.json()['data']
