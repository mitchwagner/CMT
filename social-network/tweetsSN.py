# coding=utf-8
"""
__author__ = 'Shuangfei Fan'

Input: tweets record in CSV
Output: Social network Nodes and Edges in CSV

"""

import csv
import tweepy
import time
import codecs
import numpy as np
import urltools
from sklearn import preprocessing
import twitter_api


# Create edges between users

def create_edge_user_user(tweet):
    try:
        rtfrom = []
        mentions = []
        res = {}
        rtflag=False
        ct = tweet.split()
        for i in range(len(ct)):
            if rtflag:
                rtflag=False
                continue
            if i<len(ct)-1 and ct[i] == 'RT':
                if (ct[i + 1][:1]!='@'): continue
                rtflag=True
                rtfrom.append(ct[i+1][1:-1])
            if ct[i][:1]=='@':
                mentions.append(ct[i][1:])
            res['Mentions'] = mentions
            res['RTfrom'] = rtfrom
        return res


    except TypeError as e:
        print(e)
        raise

# Create edges between tweets

def create_edge_tweet_tweet(tweet):
    try:
        rtfrom = []
        mentions = []
        res = {}
        rtflag = False
        ct = tweet.split()
        for i in range(len(ct)):
            if rtflag:
                rtflag = False
                continue
            if i < len(ct) - 1 and ct[i] == 'RT':
                if (ct[i + 1][:1] != '@'): continue
                rtflag = True
                rtfrom.append(ct[i + 1][1:-1])
            res['RTfrom'] = rtfrom
        return res

    except TypeError as e:
        print(e)
        raise


# Create Node List


tweets_record=[]

with open('collection-3-3.csv', 'rU') as csvfile:
    fieldnames = ['colection', 'id','user', 'from_user_id', 'mention', 'rt', 'url', 'tweet']
    reader = csv.DictReader(csvfile, fieldnames=fieldnames)
    for row in reader:
        tr={}
        tr['user_id']=row['from_user_id']
        tr['tweet_id']=row['id']
        tr['url'] = row['url']
        tr['tweet'] = row['tweet']
        tr['user'] = row['user']
        tweets_record.append(tr)

nodes = []
num_user = 0
num_url = 0
num_tweet = 0


user_id_list = []


for rcd in tweets_record:
    if rcd['url'] not in nodes and rcd['url'] and rcd['url'].strip():
        nodes.append(rcd['url'])
        num_url += 1

for rcd in tweets_record:
    if rcd['user'] not in nodes:
        nodes.append(rcd['user'])
        num_user += 1
        user_id_list.append(rcd['user_id'])

for rcd in tweets_record:
    if rcd['tweet_id'] not in nodes:
        nodes.append(rcd['tweet_id'])
        num_tweet += 1

nodesdic=[]

for i in range(len(nodes)):
    temp={}
    temp['node']=nodes[i]
    temp['id'] = i
    nodesdic.append(temp)


# Calculate the importance value for urls

ip = np.zeros(len(nodes))
for rcd in tweets_record:
    for node in nodesdic:
        if (urltools.compare(rcd['url'], node['node']) == True):
            ip[node['id']] += 1

url_ipr = ip[(num_user + num_tweet):]
min_max_scaler = preprocessing.MinMaxScaler()
url_ip = min_max_scaler.fit_transform(url_ipr)



# Calculate the importance value for users

twitter_api.configuration()


user_inf = twitter_api.get_users_info(user_id_list, 'ALL')

user_info = twitter_api.calculate_user_importance(user_inf)

twitter_api.write_list_into_file1(user_info, 'tweet_info.csv')

user_ip = np.zeros(num_user)

i = 0

for user in user_id_list:
    for row in user_info:
        if user == row[0]:
            user_ip[i] = row[-1]
    i += 1

# Calculate the importance value for tweets

ip = np.append(url_ip, user_ip)

tweet_ip = np.zeros(num_tweet)

i = 0

for rcd in tweets_record:
    for node in nodesdic:
        if rcd['user'] == node['node']:
            tweet_ip[i] += 0.7 * ip[node['id']]
        if rcd['url'] == node['node']:
            tweet_ip[i] += 0.3 * ip[node['id']]
    i += 1


ip = np.append(ip, tweet_ip)

nodesdic=[]

for i in range(len(nodes)):
    temp={}
    temp['node']=nodes[i]
    temp['id'] = i
    temp['if'] = ip[i]
    nodesdic.append(temp)


# Create user-user Edge List

u_u_edges={}

for rcd in tweets_record:

    user = rcd['user']
    tweet = rcd['tweet']

    toUser = create_edge_user_user(tweet)
    for row in nodesdic:
        if row['node'] == user:
            fromID = row['id']
            break
    for row in nodesdic:
        if row['node'] in toUser['RTfrom']:
            rtFromID = row['id']
            link=str(fromID)+'_'+str(rtFromID)
            if link not in u_u_edges:
                temp = []
                temp.append(fromID)
                temp.append(rtFromID)
                u_u_edges[link] = temp

        if row['node'] in toUser['Mentions']:
            mtToID = row['id']
            link = str(fromID)+'_'+str(mtToID)
            if link not in u_u_edges:
                temp = []
                temp.append(fromID)
                temp.append(mtToID)
                u_u_edges[link] = temp


# Create tweet-tweet Edge List

t_t_edges={}

for rcd in tweets_record:

    user = rcd['user']
    tweet = rcd['tweet']
    tweet_id = rcd['tweet_id']

    totweet = create_edge_tweet_tweet(tweet)
    for i in range(len(nodes)):
        if nodes[i] == tweet_id:
            fromID = i
            break
    for i in range(len(nodes)):
        if nodes[i] in totweet['RTfrom']:
            rtFromID = i
            link=str(fromID)+'_'+str(rtFromID)
            if link not in u_u_edges:
                temp = []
                temp.append(fromID)
                temp.append(rtFromID)
                t_t_edges[link] = temp


# Create user-tweet Edge List and tweet-url Edge List

u_t_edges = {}
t_url_edges = {}

for rcd in tweets_record:
    user = rcd['user']
    tweet = rcd['tweet']
    tweet_id = rcd['tweet_id']
    url = rcd['url']
    for row in nodesdic:
        if user == row['node']:
            link_u = row['id']
        if tweet_id == row['node']:
            link_t = row['id']
        if url == row['node']:
            link_url = row['id']

    link_u_t = str(link_u) + '_' + str(link_t)
    if link_u_t not in u_t_edges:
        temp = []
        temp.append(link_u)
        temp.append(link_t)
        u_t_edges[link_u_t] = temp

    if rcd['url'] and rcd['url'].strip():
        link_t_url = str(link_t) + '_' + str(link_url)
        if link_t_url not in t_url_edges:
            temp = []
            temp.append(link_t)
            temp.append(link_url)
            t_url_edges[link_t_url] = temp


edgesdic=[]

for row in u_u_edges:

    temp = {}
    temp['from']=row[0]
    temp['to'] = row[1]

    edgesdic.append(temp)

for row in t_t_edges:

    temp = {}
    temp['from']=row[0]
    temp['to'] = row[1]

    edgesdic.append(temp)


for row in u_t_edges.values():

    temp = {}
    temp['from']=row[0]
    temp['to'] = row[1]

    edgesdic.append(temp)

for row in t_url_edges.values():

    temp = {}
    temp['from']=row[0]
    temp['to'] = row[1]
    edgesdic.append(temp)


with open('nodescsvfile.csv', 'wb') as f:
    w = csv.DictWriter(f, nodesdic[0].keys())
    w.writeheader()
    w.writerows(nodesdic)

with open('edgecsvfile.csv', 'wb') as f:
    fieldnames = ['from', 'to']
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for row in edgesdic:
        w.writerow(row)



