from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
from os import environ

access_token = environ.get('twiiter_access_token')
access_token_secret = environ.get('twitter_access_token_secret')
consumer_key = environ.get('twitter_consumer_key')
consumer_secret = environ.get('twitter_consumer_secret')

class StdOutListener(StreamListener):
	def on_data(self, data):
		producer.send("covid-data", value=data.encode('utf-8'))
		print(data)
		return True

	def on_error(self, status):
		print(status)

if __name__ == '__main__':
	producer = KafkaProducer(bootstrap_servers=["localhost:9092"])
	out = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, out)
	stream.filter(track="trump")