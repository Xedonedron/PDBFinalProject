import tweepy
from kafka import KafkaProducer
import logging
import json

"""API ACCESS KEYS"""
consumerKey = 'QnupptSEsqShuukhaPGsk0Svf'
consumerSecret = "HuD4bnPtEZcE1IVFDcluzuYcngbSMMYxKyfYA8MTXPUYZvn4N0"
accessToken = '1390698546353999878-KExzyOWL2qm1zfmxZdrYmcnkaVbjue'
accessTokenSecret = 'dkEkoVa4QwW3Z8Rzvau6r37C7DhnUPU2zu91P6GZmm7Ec'
bearerToken = 'AAAAAAAAAAAAAAAAAAAAALP5cAEAAAAACkdJeE7i23YH3L4qeYWpvoeqZAA%3DoOefkiPYcftGrIzdm9YgPi7hnYrI6ymuojs6031Dd5K5U7CWul'

logging.basicConfig(level=logging.INFO)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'cupid'
topic_name = 'twitter'


def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(consumerKey, consumerSecret)
    # set the access token and the access token secret
    authenticate.set_access_token(accessToken, accessTokenSecret)
    authenticate.secure = True
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api


class TweetListener(tweepy.StreamingClient):

    def on_data(self, raw_data):
        logging.info(raw_data)

        tweet = json.loads(raw_data)

        if tweet['data']:
            data = {
                'message': tweet['data']['text'].replace(',', '')
            }
            producer.send(topic_name, value=json.dumps(data).encode('utf-8'))

        return True

    @staticmethod
    def on_error(status_code):
        if status_code == 420:
            # returning False in on_data disconnects the stream
            return False

    def start_streaming_tweets(self, search_term):
        self.add_rules(tweepy.StreamRule(search_term))
        self.filter()


if __name__ == '__main__':
    twitter_stream = TweetListener(bearerToken)
    twitter_stream.start_streaming_tweets(search_term)
