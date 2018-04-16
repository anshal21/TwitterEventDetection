#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 28 13:56:23 2017

@author: anshal
"""
import nltk
import re
import sys
import codecs as cd
from nltk.tokenize import TweetTokenizer 
import string
from pprint import pprint
import sklearn
from datetime import datetime
import numpy as np
import gensim
import matplotlib.pyplot as plt
import matplotlib as mpl
#import fastcluster
from collections import Counter
import scipy.cluster.hierarchy as sch 
import numpy as np
import collections
from nltk.stem.snowball import SnowballStemmer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from scipy.cluster.hierarchy import ward, dendrogram
from sklearn.cluster import KMeans
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

def getStopWords():
    stopWords = []
    with open('./filters/stopWords','r') as f:
        while True:
            line = f.readline()
            if line == "":
                break
            stopWords.append(line.strip())
    stopWords = set(stopWords)
    return stopWords
    
def getSLangs():
    slangs = {}
    with open('./filters/slangs','r') as f:
        while True:
            line_1 = f.readline()
            if line_1 == "":
                break
            line_1 = line_1.strip()
            line_2 = f.readline()
            line_2 = line_2.strip()
            slangs[line_1] = line_2
        return slangs
def processSlangs(text):
    tokens = text.split()
    tokens = [slangs[item] if item in slangs else item for item in tokens]
    tweet = ' '.join(tokens)
    #pprint(tweet+'00000000000')
    return tweet
    
def normalizeTweet(text):
    EMOTICONS = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
      |
      <3                         # heart
    )"""
    
    URLS = r"""			# Capture 1: entire matched URL
      (?:
      https?:				# URL protocol and colon
        (?:
          /{1,3}				# 1-3 slashes
          |					#   or
          [a-z0-9%]				# Single letter or digit or '%'
                                           # (Trying not to match e.g. "URI::Escape")
        )
        |					#   or
                                           # looks like domain name followed by a slash:
        [a-z0-9.\-]+[.]
        (?:[a-z]{2,13})
        /
      )
      (?:					# One or more:
        [^\s()<>{}\[\]]+			# Run of non-space, non-()<>{}[]
        |					#   or
        \([^\s()]*?\([^\s()]+\)[^\s()]*?\) # balanced parens, one level deep: (...(...)...)
        |
        \([^\s]+?\)				# balanced parens, non-recursive: (...)
      )+
      (?:					# End with:
        \([^\s()]*?\([^\s()]+\)[^\s()]*?\) # balanced parens, one level deep: (...(...)...)
        |
        \([^\s]+?\)				# balanced parens, non-recursive: (...)
        |					#   or
        [^\s`!()\[\]{};:'".,<>?«»“”‘’]	# not a space or one of these punct chars
      )
      |					# OR, the following to match naked domains:
      (?:
        (?<!@)			        # not preceded by a @, avoid matching foo@_gmail.com_
        [a-z0-9]+
        (?:[.\-][a-z0-9]+)*
        [.]
        (?:[a-z]{2,13})
        \b
        /?
        (?!@)			        # not succeeded by a @,
                                # avoid matching "foo.na" in "foo.na@example.com"
      )
    """
    WORDS= r"""
        (?:[^\W\d_](?:[^\W\d_]|['\-_])+[^\W\d_]) # Words with apostrophes or dashes.
        |
        (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
        |
        (?:[\w_]+)                     # Words without apostrophes or dashes.
        |
        (?:\.(?:\s*\.){1,})            # Ellipsis dots.
        |
        (?:\S)                         # Everything else that isn't whitespace.
        """
    HASH_TAG=r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    WORDS_RE = re.compile("[\w+]")
    HASH_TAG_RE = re.compile(HASH_TAG, re.VERBOSE | re.I | re.UNICODE)
    URL_RE = re.compile(URLS, re.VERBOSE | re.I | re.UNICODE)
    EMOTICONS_RE = re.compile(EMOTICONS, re.VERBOSE | re.I | re.UNICODE)
    HTML_TAG = r"""<[^>\s]+>"""
    HTML_TAG_RE = re.compile(HTML_TAG, re.VERBOSE | re.I | re.UNICODE)
    ARROWS = r"""[\-]+>|<[\-]+"""
    ARROWS_RE = re.compile(ARROWS, re.VERBOSE | re.I | re.UNICODE)
    TWITTER_USERNAME = r"""(?:@[\w_]+)"""
    TWITTER_USERNAME_RE = re.compile(TWITTER_USERNAME, re.VERBOSE | re.I | re.UNICODE)
    EMAIL = r"""[\w.+-]+@[\w-]+\.(?:[\w-]\.?)+[\w-]"""
    EMAIL_RE = re.compile(EMAIL, re.VERBOSE | re.I | re.UNICODE)
    ELIPSIS = r"""(?:\.(?:\s*\.){1,})"""
    ELIPSIS_RE = re.compile(ELIPSIS, re.VERBOSE | re.I | re.UNICODE)
    
   
    text = re.sub(URL_RE,'',text)
    text = re.sub(HASH_TAG_RE,'',text)
    text = re.sub(EMOTICONS_RE,'',text)
    text = re.sub(HTML_TAG_RE,'',text)
    text = re.sub(ARROWS_RE,'',text)
    text = re.sub(TWITTER_USERNAME_RE,'',text)
    text = re.sub(EMAIL_RE,'',text)
    text = re.sub(ELIPSIS_RE,'',text)
    text = re.sub('[\d]','', text)
    text = re.sub('[:;>?<=*+()/,\-#!$%\{˜|\}\[^_\\@\]1234567890’‘]',' ', text)
    text = text.replace(".", '')
    text = text.replace("'", ' ')
    text = text.replace("\"", ' ')
    text = text.replace("\x9d",' ').replace("\x8c",' ')
    text = text.replace("\xa0",' ')
    text = text.replace("\x9d\x92", ' ').replace("\x9a\xaa\xf0\x9f\x94\xb5", ' ').replace("\xf0\x9f\x91\x8d\x87\xba\xf0\x9f\x87\xb8", ' ').replace("\x9f",' ').replace("\x91\x8d",' ')
    text = text.replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8",' ').replace("\xf0",' ').replace('\xf0x9f','').replace("\x9f\x91\x8d",' ').replace("\x87\xba\x87\xb8",' ')	
    text = text.replace("\xe2\x80\x94",' ').replace("\x9d\xa4",' ').replace("\x96\x91",' ').replace("\xe1\x91\xac\xc9\x8c\xce\x90\xc8\xbb\xef\xbb\x89\xd4\xbc\xef\xbb\x89\xc5\xa0\xc5\xa0\xc2\xb8",' ')
    text = text.replace("\xe2\x80\x99s", " ").replace("\xe2\x80\x98", ' ').replace("\xe2\x80\x99", ' ').replace("\xe2\x80\x9c", " ").replace("\xe2\x80\x9d", " ")
    text = text.replace("\xe2\x82\xac", " ").replace("\xc2\xa3", " ").replace("\xc2\xa0", " ").replace("\xc2\xab", " ").replace("\xf0\x9f\x94\xb4", " ").replace("\xf0\x9f\x87\xba\xf0\x9f\x87\xb8\xf0\x9f", "")
    return text

def tokenizeTweets(tweet):
    tweetTokenizer = TweetTokenizer()
    features = tweetTokenizer.tokenize(tweet)
    features = [ item.lower() for item in features if item.lower() not in stopWords]
    punctuation=set(string.punctuation)
    #features = [item if item not in punctuation for item in features]
    features = [ item for item in features if len(item) > 1 ]   
    return features

def processTweet(tweet):
    tweet = normalizeTweet(tweet)
    tweet = processSlangs(tweet)
    features = tokenizeTweets(tweet)
    return features
    
def cutsom_tokenizer(text):
    x = re.compile(r",\s*")
    tokens = []
    tmp = x.split(text)
    for item in tmp:
        if '@' not in item:
            tokens.append(item.strip().lower())
    return tokens

if __name__ == "__main__":
    
    stopWords = getStopWords()
    slangs = getSLangs()
    nTweets = 0
    prevTime = -1
    timeWindow = 1000
    currentCorpus = []
    currentCorpusTokens = []
    tweetId_currentCorpus = []
    tid2url_currentCorpus = {}
    dfVocTimeWindows = {}
    tid2tweet_currentCorpus = {}
    windowCount = 0
    tweetFile = cd.open(sys.argv[1],'r','utf-8')
    doc = cd.open(sys.argv[2],'w','utf-8')
    
    #stemmer = SnowballStemmer("english")
    for line in tweetFile:
        [tweet_unixtime, tweet_gmttime, tweet_id, text, hashtags, users, urls, media_urls, nfollowers, nfriends] = eval(line)
        if prevTime == -1:
            prevTime = tweet_unixtime
        if (tweet_unixtime - prevTime) < timeWindow*60 :
            nTweets += 1
            features = processTweet(text)
            
            
            tweetBag = ""
            tweetBagTokens = []
            if len(users) < 3 and len(hashtags) < 3 and len(features) > 3:
                for user in set(users):
                    tweetBag += "@" + user.lower()+","
                for tag in set(hashtags):
                    if tag.lower() not in stopWords :
                        tweetBag += "#" + tag.lower()+","
                        tweetBagTokens.append(str("#"+tag.lower()))
                for feature in features:
                    tweetBag += feature + ","
                    tweetBagTokens.append(feature)
                doc.write(' '.join(tweetBagTokens)+"\n")
                    
 
    