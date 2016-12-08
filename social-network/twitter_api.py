# -*- coding: utf-8 -*-
"""
__author__ = 'Shuangfei Fan'

"""

import ConfigParser
import codecs
import time
import tweepy
import csv
import sys
from sklearn import preprocessing
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()


def configuration():

	reload(sys)
	sys.setdefaultencoding('utf-8')

	config = ConfigParser.ConfigParser()
	config.read('conf/key.ini')
	consumer_key = config.get('DEFAULT', 'consumer_key').replace("'", "")
	consumer_secret = config.get('DEFAULT', 'consumer_secret').replace("'", "")
	access_key = config.get('DEFAULT', 'access_key').replace("'", "")
	access_secret = config.get('DEFAULT', 'access_secret').replace("'", "")

	global api
	# authorize twitter
	auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_key, access_secret)
	#api = tweepy.API(auth)
	api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True, compression=True)


def read_file_into_list_user(f_name):
	tmp_list = []
	with open(f_name) as f:
		reader = csv.DictReader(f, delimiter='\t')
		for row in reader:
			tmp_list.append(row['from_user_id'].replace(' ', ''))
	return tmp_list

def get_users_info(id_list, select_type):
	user_info = []


	for i in range(0, len(id_list), 100):
		id_sub_list = [item for item in id_list[i:i + 100]]

		user_list = api.lookup_users(user_ids=id_sub_list)

		for user in user_list:
			if select_type == 'ALL':
				user_info.append([user.id_str,
								   user.name,
                                   user.screen_name,
                                   user.created_at,
                                   user.description,
                                   user.geo_enabled,
                                   user.location,
                                   user.time_zone,
                                   user.url,
                                   user.verified,
                                   user.lang,
                                   user.following,
                                   user.followers_count,
                                   user.friends_count,
                                   user.statuses_count,
                                   user.favourites_count,
                                   user.listed_count])
			elif select_type == 'NAME':
				user_info.append([user.name])
		time.sleep(20)
	return user_info


def calculate_user_importance(user_info):
	im = []

	for user in user_info:
		x = user[4] * 0.25 + user[5] * 0.25 + user[6] * 0.1 + user[7] * 0.25 + user[8] * 0.15
		im.append((x))

	min_max_scaler = preprocessing.MinMaxScaler()
	imp = min_max_scaler.fit_transform(im)
	i = 0
	for user in user_info:
		user.append((imp[i]))
		i += 1

	return user_info


def write_list_into_file1(tmp_list, f_name):
	with codecs.open('data/' + f_name, 'w') as f:
		writer = csv.writer(f, lineterminator='\n')
		for row in tmp_list:
			writer.writerow([str(s).encode("utf-8") for s in row])


