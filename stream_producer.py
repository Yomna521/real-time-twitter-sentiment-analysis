# -*- coding: utf-8 -*-
"""
# Stream Producer
In this code we will fetch tweets from twitter using Tweepy, and initialize the kfka producer and ignest tweets to the topic
"""

from tweepy import OAuthHandler, StreamListener
from tweepy import Stream, API
from kafka import KafkaProducer
import json
from bson import json_util
from dateutil.parser import parse
import re


#Twitter Credentials 
access_token = '1438441025601671170-W8f9wR35o1GakmfsV91RK6qlRpiCao'
access_token_secret = 'gib7YGWqLVFbPcE8r89pxTIQzhkFznf8dWy9781MAapKB'
consumer_key = 'JPckg2OMgqqXZAPtUKGMKUXFJ'
consumer_secret = '5UmOrq0ChHTD4IjYPxTIoRv2UDMpPw0GL7O3rG341Ch5UDNJuH'

class KafkaConfig():
	def __init__(self):
		self.producer = KafkaProducer(bootstrap_servers='localhost:9092')
	def get_producer(self):
		return self.producer

class TwitterStreamListener(StreamListener):
	def on_data(self, data):
		kafka_producer = KafkaConfig().get_producer()   # initialize kafka producer
		kafka_topic = 'Movie'                 # Add new topic to producer
		tweet = json.loads(data)              # filter extra json from tweet
		tweet_text = ""

                #filter the tweets to include only english ones
		if all(x in tweet.keys() for x in ['lang', 'time']) and tweet['lang'] == 'en':
                        #Cover all tweet statuses 
			if 'retweeted_status' in tweet.keys():
				if 'quoted_status' in tweet['retweeted_status'].keys():
					if('extended_tweet' in tweet['retweeted_status']['quoted_status'].keys()):
						tweet_text = tweet['retweeted_status']['quoted_status']['extended_tweet']['full_text']
				elif 'extended_tweet' in tweet['retweeted_status'].keys():
					tweet_text = tweet['retweeted_status']['extended_tweet']['full_text']
			elif tweet['truncated'] == 'true':
				tweet_text = tweet['extended_tweet']['full_text']

			else:
				tweet_text = tweet['text']

		if(tweet_text):
			data = {
				'time': tweet['time'],
				 'text': tweet_text.replace(',','')
				 }
			kafka_producer.send(kafka_topic, value = json.dumps(data, default=json_util.default).encode('utf-8'))

	def on_error(self, status):
		if(status == 420): 
			return False #rate limit exceeded
		print(status)

if __name__ == "__main__":
	
	listener = TwitterStreamListener()

	#API Authentication
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	api = API(auth)

	stream = Stream(api.auth, listener)
        #the evaluation topic: the movie Cruella, and staring actress Emma Stone
	stream.filter(track=["Emma Stone", "Cruella"])

