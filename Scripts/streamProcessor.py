# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
from datetime import datetime
import json,time
from pprint import pprint
import codecs as cd
import sys

def processTweet(line):
    tweet = json.loads(line)
    if tweet['lang'] != 'en':
     	return ['', '', '', [], [], []]
    #pprint(tweet)
    #pprint('---------------------------------------------------------------')
    date = tweet['created_at']
    tweet_id = tweet['id']
    nFollowers = tweet['user']['followers_count']
    nFriends = tweet['user']['friends_count']
    if 'retweeted_status' in tweet:
        text = tweet['retweeted_status']['text']
    else:
        text = tweet['text']
    hashtags = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
    users = [user_mention['screen_name'] for user_mention in tweet['entities']['user_mentions']]
    urls = [url['expanded_url'] for url in tweet['entities']['urls']]
    
    media_urls = []
    if 'media' in tweet['entities']:
	    media_urls = [media['media_url'] for media in tweet['entities']['media']]	    

    return [date, tweet_id, text, hashtags, users, urls, media_urls, nFollowers, nFriends]
    

if __name__ == "__main__":
    print(sys.argv[1],sys.argv[2])
    with open(sys.argv[1],'r') as f:
        p = f.read()
        lines = p.splitlines()
        pprint(json.loads(lines[0]))
        l = len(lines)
        lines = [lines[i] for i in range(0,l) if i%2==0 ]
    
    f = cd.open(sys.argv[2],'w','utf-8')
    for line in lines:
        try:
            [tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends] = processTweet(line)
            pprint([tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends])
            try:
                c = time.strptime(tweet_gmttime.replace("+0000",''), '%a %b %d %H:%M:%S %Y')
            except:
                pass
            tweet_unixtime = int(time.mktime(c))
            f.write(str([tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends]) + "\n")
        except:
            pass
    f.close()