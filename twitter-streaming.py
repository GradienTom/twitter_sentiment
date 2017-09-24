from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import datetime

class Listener (StreamListener):

    def __init__(self, table):
        StreamListener.__init__(self)
        self.table = table

    def on_data(self, data):
        try:
            self.table.insert_tweet(Tweet(data))
        except Exception, e:
            self.table.insert_log(Log(str(e)))

    def on_error(self, status):
        self.table.insert_log(Log(str(status)))


class DataTable:

    def __init__(self, tweets, logs):
        self.tweets = tweets
        self.logs = logs

    def insert_tweet(self, tweet):
        self.tweets.put_item(
            Item={
                'Date': tweet.time_stamp,
                'Text': tweet.text,
            }
        )

    def insert_log(self, log):
        self.logs.put_item(
            Item={
                'Date': log.time_stamp,
                'Message': log.message,
            }
        )


class Log:

    def __init__(self, message):
        self.message = message
        self.time_stamp = str(datetime.datetime.utcnow())


class Tweet:

    def __init__(self, json_data):
        tweet = json.loads(json_data)
        self.time_stamp, self.text = tweet['created_at'], tweet['text']
        print(self.text)


if __name__ == '__main__':
    KEY_WORD = 'Car'

    consumer_key= 'CvL60UbrszIbU7sPabRNsVis0'
    consumer_secret= 'OOU3EOonHMqOfaPZ7SGRYucP2wxDpHv5l3L0TKcyV09Oxw0yu3'
    access_token= '2374450932-EFnX9Rt07h4E1PWwj4lNyeunvF2KN3ahsqC5c3g'
    access_token_secret= 'DQfcife9i7teD2DtWtJQOYlasK0yByqbkV7MR903AW2Zc'

    dynamodb = boto3.resource('dynamodb')
    table = DataTable(dynamodb.Table('Tweet'), dynamodb.Table('Log'))

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitterStream = Stream(auth, Listener(table))
    twitterStream.filter(track=[KEY_WORD])
