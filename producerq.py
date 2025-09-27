import os
import tweepy
from kafka import KafkaProducer
import logging
import json
from dotenv import load_dotenv
load_dotenv()

consumerKey = os.getenv('CONSUMERKEY')
consumerSecret = os.getenv('CONSUMERSECRET')
accessToken = os.getenv('ACCESSTOKEN')
accessTokenSecret = os.getenv('ACCESSTOKENSECRET')
bearerToken = os.getenv('BEARERTOKEN')

"""API ACCESS KEYS"""

logging.basicConfig(level=logging.INFO)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'ChatGPT'
topic_name = 'twitter'


# def twitterAuth():
#     # create the authentication object
#     authenticate = tweepy.OAuth1UserHandler(consumerKey, consumerSecret, accessToken, accessTokenSecret)
#     # set the access token and the access token secret
#     # authenticate.set_access_token(accessToken, accessTokenSecret)
#     # authenticate.secure = True
#     # create the API object
#     api = tweepy.API(authenticate, wait_on_rate_limit=True)
    
#     return api
def twitter_auth_v2():
    """
    Authenticates with the Twitter (X) API v2 using Bearer Token and returns the Tweepy Client object.
    """
    load_dotenv()  # Load environment variables from .env

    bearer_token = os.getenv("BEARERTOKEN")

    if not bearer_token:
        raise ValueError("Missing Twitter Bearer Token in environment variables.")

    # Create and return the v2 Client
    client = tweepy.Client(bearer_token=bearer_token, consumer_key=consumerKey, consumer_secret=consumerSecret, access_token=accessToken, access_token_secret=accessTokenSecret, wait_on_rate_limit=True)
    return client


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
