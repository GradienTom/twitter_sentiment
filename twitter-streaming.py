from __future__ import print_function # Python 2/3 compatibility
import boto3
import json
import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import datetime
from bs4 import BeautifulSoup

class Listener (StreamListener):

    def __init__(self, table):
        StreamListener.__init__(self)
        self.table = table
        
    def on_data(self, data):
        try:
            decoded = json.loads(data)
            self.table.insert_tweet(Tweet(decoded))
        except KeyError, e:
            print("Key Error", e)
        except Exception, e:
            print("Error on data", e)

    def on_error(self, status):
        print(status)

class DataTable:

    def __init__(self, tweetTable, logTable):
        self.tweetTable = tweetTable
        self.logTable = logTable

    def insert_tweet(self, tweet):
        self.tweetTable.put_item(
            Item={
                'TimeStamp': tweet.time_stamp,
                'Text': tweet.text,
                'ID': tweet.id,
                'Language': tweet.lang,
                'Source': tweet.source,
                'entities': {
                    'Hashtags': tweet.entities.hashtags,
                    'Mentions': tweet.entities.mentions
                },
                'user': {
                    'ID': tweet.user.id,
                    'name': tweet.user.name,
                    'FavouritesCount': tweet.user.favourites_count,
                    'FollowersCount': tweet.user.followers_count,
                    'FriendsCount': tweet.user.friends_count,
                }
            }
        )

    def insert_log(self, log):
        self.logTable.put_item(
            Item={
                'TimeStamp': log.time_stamp,
                'Message': log.message,
            }
        )


class Log:
        
    def __init__(self, message):
        self.message = message
        self.time_stamp = str(datetime.datetime.utcnow())


class Tweet:

    def __init__(self, tweet):
        self.id = tweet['id']
        self.lang = tweet['lang']
        self.time_stamp, self.text = tweet['created_at'], tweet['text']
        self.source = BeautifulSoup(tweet['source'], 'html.parser').get_text()

        self.user = User(tweet['user'])
        self.entities = Entities(tweet['entities'])
        
        print(self.text)


class User:
    
    def __init__(self, user):
        self.name = user['screen_name']
        self.id = user['id']
        self.favourites_count = user['favourites_count']
        self.followers_count = user['followers_count']
        self.friends_count = user['friends_count']
        
class Entities:

    def __init__(self, entities):
        self.hashtags, self.mentions = [], []
        for hashtag in entities['hashtags']:
            self.hashtags.append(hashtag['text'])
        for mention in entities['user_mentions']:
            self.mentions.append(mention['screen_name'])
            

if __name__ == '__main__':
    consumer_key= 'CvL60UbrszIbU7sPabRNsVis0'
    consumer_secret= 'OOU3EOonHMqOfaPZ7SGRYucP2wxDpHv5l3L0TKcyV09Oxw0yu3'
    access_token= '2374450932-EFnX9Rt07h4E1PWwj4lNyeunvF2KN3ahsqC5c3g'
    access_token_secret= 'DQfcife9i7teD2DtWtJQOYlasK0yByqbkV7MR903AW2Zc'

    dynamodb = boto3.resource('dynamodb')
    table = DataTable(dynamodb.Table('Tweet'), dynamodb.Table('Log'))

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    twitterStream = Stream(auth, Listener(table))
    twitterStream.sample()
