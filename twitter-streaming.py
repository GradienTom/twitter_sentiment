from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import time

consumer_key= 'CvL60UbrszIbU7sPabRNsVis0'
consumer_secret= 'OOU3EOonHMqOfaPZ7SGRYucP2wxDpHv5l3L0TKcyV09Oxw0yu3'
access_token= '2374450932-EFnX9Rt07h4E1PWwj4lNyeunvF2KN3ahsqC5c3g'
access_token_secret= 'DQfcife9i7teD2DtWtJQOYlasK0yByqbkV7MR903AW2Zc'
limit = 5
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('TSA')

class listener (StreamListener):

    def __init__(self):
        StreamListener.__init__(self)
        self.count = 0

    def on_data(self, data):
        try:
            self.write(data)
        except ValueError, e:
            print("Failed on data", e)
            time.sleep(5)
        self.count += 1
        if self.count >= limit:
            exit()

    def on_error(self, status):
        print(status)

    def write(self, data):
        tweet = json.loads(data)
        date, text = tweet['created_at'], tweet['text']
        print(date, text)
        table.put_item(
            Item={
                'date': date,
                'text': text,
            }
        )
