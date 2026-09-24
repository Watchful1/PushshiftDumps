import zstandard
import os
import json
import shutil
import csv
from datetime import datetime
import logging.handlers
from pathlib import Path
from collections import defaultdict


input_single_folder = r"\\MYCLOUDPR4100\Public\wallstreetbets_gainloss_images\gallery-dl\reddit"
input_gallery_folder = r"\\MYCLOUDPR4100\Public\wallstreetbets_gainloss_images\gallery-dl\reddit\wallstreetbets"
csv_file_path = r"\\MYCLOUDPR4100\Public\wallstreetbets_gainloss.csv"
output_folder = r"\\MYCLOUDPR4100\Public\wallstreetbets_gainloss_images\sorted"

log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


if __name__ == "__main__":
	fields = ["author","title","score","created","link","text","url"]

	log.info(f"Loading single images")
	singles = {}
	for filename in os.listdir(input_single_folder):
		image_path = os.path.join(input_single_folder, filename)
		image_id = filename.split(".")[0]
		singles[image_id] = image_path
	log.info(f"Loaded {len(singles)} images")

	log.info(f"Loading gallery images")
	galleries = defaultdict(list)
	for filename in os.listdir(input_gallery_folder):
		post_id = filename.split(" ")[0]
		image_path = os.path.join(input_gallery_folder, filename)
		galleries[post_id].append(image_path)
	log.info(f"Loaded {len(galleries)} posts")

	single_found = 0
	single_not_found = 0
	gallery_found = 0
	gallery_not_found = 0
	total_posts = 76536
	current_posts = 0

	csv_handle = open(csv_file_path, 'r', encoding='utf-8')
	csv_reader = csv.reader(csv_handle)
	for line in csv_reader:
		current_posts += 1
		post_id = line[0]
		post_url = line[6]

		if "reddit.com/gallery" in post_url:
			if post_id in galleries:
				log.info(f"{current_posts}/{total_posts}: {post_id} found with {len(galleries[post_id])} images")
				image_num = 1
				for image_path in galleries[post_id]:
					target_path = os.path.join(output_folder, f"{post_id}_{image_num}.jpg")
					#log.info(f"copying {image_path} to {target_path}")
					shutil.copyfile(image_path, target_path)
					image_num += 1
				gallery_found += 1
			else:
				log.info(f"{current_posts}/{total_posts}: {post_id} not found with gallery")
				gallery_not_found += 1
		elif "i.redd.it" in post_url:
			image_id = post_url.split("/")[-1].split(".")[0]
			if image_id in singles:
				log.info(f"{current_posts}/{total_posts}: {post_id} single image found")
				target_path = os.path.join(output_folder, f"{post_id}.jpg")
				#log.info(f"copying {singles[image_id]} to {target_path}")
				shutil.copyfile(singles[image_id], target_path)
				single_found += 1
			else:
				log.info(f"{current_posts}/{total_posts}: {post_id} single image not found")
				single_not_found += 1
		else:
			log.info(f"Something wrong, url not matched: {post_url}")

	log.info(f"{single_found} | {single_not_found} | {gallery_found} | {gallery_not_found} | {single_found + gallery_found}/{single_found + gallery_found + single_not_found + gallery_not_found}")

