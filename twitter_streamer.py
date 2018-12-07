 #Import the necessary package to process data in JSON format

try:
    import json
except ImportError:
    import simplejson as json
import  csv
# Import the necessary methods from "twitter" library
#from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
from tweepy import API
from tweepy import Cursor
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from twitter import Twitter, OAuth, TwitterHTTPError, TwitterStream
#variables that contain the user credentials to access twitter api
ACCESS_TOKEN = "1674128370-iOeAEsMsmJvCjUWVBQnvpQKGJPgiscw6XHdIV7Q"
ACCESS_SECRET = "hB7xgbCbjk5LllWncAYvhkWh586il6poiblb2URzrfioH"
CONSUMER_KEY = "QB8iSkB6bB3v1GgE0ZRggBtnW"
CONSUMER_SECRET = "spYkunAZHMX6kW9NYEGqQygHJRBSla2znChqq7647V00dxrVtV"

def read_credentials():
    file_name = "credentials.json"
    try:
        with open(file_name) as data_file:
            return json.load(data_file)
    except:
        print ("Cannot load credentials.json")
        return None

def read_tweets(access_token, access_secret, consumer_key, consumer_secret):

    oauth = OAuth(access_token, access_secret, consumer_key, consumer_secret)
    #oauth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    #oauth.set_access_token(ACCESS_TOKEN, ACCESS_SECRET)

    # Initiate the connection to Twitter Streaming API
    twitter_stream = TwitterStream(auth=oauth)

    # Get a sample of the public data following through Twitter
    iterator = twitter_stream.statuses.sample()

    # Print each tweet in the stream to the screen
    # Here we set it to stop after getting 1000 tweets.
    # You don't have to set it to stop, but can continue running
    # the Twitter API to collect data for days or even longer.
    tweet_count = 10
    with open('tweet_file.csv', mode='w') as employee_file:
        tweet_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        tweet_writer.writerow(['Created at', 'User Screen name', 'Text', 'Favorite Count'])
    for tweet in iterator:
        tweet_count -= 1
        # Twitter Python Tool wraps the data returned by Twitter
        # as a TwitterDictResponse object.
        try:
            screeName =  tweet['user']['screen_name']
            text = tweet['text']
            createdAt = tweet['created_at']
            favoritecount = tweet['favourites_count']
            # print screen_name and name
            print "TWEET: ", tweet['user']['screen_name'], "\n"
            # The command below will do pretty printing for JSON data, try it out
            print "TWEET text: ", tweet['text'], "\n"
            # This next command, prints the tweet as a string
            print "CREATED_AT:", tweet['created_at'], "\n"
            print "Favorite count: ", tweet("favorite_count"), "\n"
            with open('tweet_file.csv', mode='w') as employee_file:
                tweet_writer = csv.writer(employee_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                tweet_writer.writerow([createdAt,screeName, text, favoritecount])
        except:
            pass

        if tweet_count <= 0:
            print("Done")
            break

if __name__ == "__main__":

    print("Starting to read tweets")
    read_tweets(ACCESS_TOKEN, ACCESS_SECRET, CONSUMER_KEY, CONSUMER_SECRET)