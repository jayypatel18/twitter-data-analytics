import os
import tweepy
from kafka import KafkaProducer
import logging
import json
import time
from dotenv import load_dotenv

load_dotenv()

consumerKey = os.getenv('CONSUMERKEY')
consumerSecret = os.getenv('CONSUMERSECRET')
accessToken = os.getenv('ACCESSTOKEN')
accessTokenSecret = os.getenv('ACCESSTOKENSECRET')
bearerToken = os.getenv('BEARERTOKEN')

logging.basicConfig(level=logging.INFO)
producer = KafkaProducer(bootstrap_servers='localhost:9092')
search_term = 'ChatGPT'
topic_name = 'twitter'

def twitter_auth_v2():
    """
    Authenticates with the Twitter (X) API v2 using Bearer Token and returns the Tweepy Client object.
    """
    if not bearerToken:
        raise ValueError("Missing Twitter Bearer Token in environment variables.")

    # Create and return the v2 Client
    client = tweepy.Client(bearer_token=bearerToken, 
                          consumer_key=consumerKey, 
                          consumer_secret=consumerSecret, 
                          access_token=accessToken, 
                          access_token_secret=accessTokenSecret, 
                          wait_on_rate_limit=True)
    return client

def search_and_send_tweets():
    """
    Search for tweets and send them to Kafka (works with Basic access)
    """
    client = twitter_auth_v2()
    
    try:
        # Search for recent tweets
        tweets = client.search_recent_tweets(query=search_term, 
                                           max_results=2,
                                           tweet_fields=['created_at', 'author_id', 'public_metrics'])
        
        if tweets.data is None:
            print("No tweets found")
            return
            
        for tweet in tweets.data:
            data = {
                'id': tweet.id,
                'text': tweet.text,
                'created_at': str(tweet.created_at),
                'author_id': tweet.author_id
            }
            
            print(f"Sending tweet: {tweet.text[:50]}...")
            producer.send(topic_name, value=json.dumps(data).encode('utf-8'))
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == '__main__':
    while True:
        search_and_send_tweets()
        print("Waiting 1200 seconds before next search...")
        time.sleep(1200)  # Wait 20 minutes between searches
