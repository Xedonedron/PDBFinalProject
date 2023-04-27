import tweepy
import pandas as pd
from json import dumps
from kafka import KafkaProducer

# API token
CONSUMER_KEY = "5OBZDcObonT3Pdf9du705G5KP"
CONSUMER_SECRET = "24qgOlLKmywVLeID6FV0IHNscyPt1mTcWeXA5M8tDDV6bngvJt"
ACCESS_TOKEN = "1266772148875489282-AhXazdXEk0gMfT5V9tRtGSy6pLGIP5"
ACCESS_TOKEN_SECRET = "IziL7kVEUNF8KwgQVVAMWxhPLA4xNaZN1lbKDkVG3PE7W"

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda K:dumps(K).encode('utf-8'))

api = tweepy.API(auth) # API auth
tweets = tweepy.Cursor(api.search_tweets, q=["cupid"], tweet_mode='extended').items(100) # nyari tweets
result = []

# Untuk ambil tweets trus export ke txt
for tweet in tweets:
    print(tweet.full_text)
    result.append(tweet.full_text)
    producer.send('twitter', tweet.full_text)

df = pd.DataFrame({'tweets': result})
df.to_csv('tweets.txt', index=False, sep='\t', header=False)