import tweepy                                                                                                                                                                                         import tweepy
import pandas as pd
import json
from datetime import datetime
from azure.storage.blob import BlobServiceClient


def run_twitter_etl():

    access_key = ""
    access_secret = ""
    consumer_key = ""
    consumer_secret = ""


    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # # # Creating an API object
    api = tweepy.API(auth)
    print("----------API--------------")
    print(api)
    print("----------------------------")
    tweets = api.user_timeline(screen_name='@elonmusk',
                            # 200 is the maximum allowed count
                            count=200,
                            include_rts = False,
                            # Necessary to keep full_text
                            # otherwise only the first 140 words are extracted
                            tweet_mode = 'extended'
                            )
    print("----------Tweets--------------")

    #print(tweets)
    print("----------------------------")
    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                            'text' : text,
                            'favorite_count' : tweet.favorite_count,
                            'retweet_count' : tweet.retweet_count,
                            'created_at' : tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)
    
   

    df.to_csv('<replace_file_name.csv>')
    
run_twitter_etl()

from azure.storage.blob import BlobServiceClient

#Go and check details on Access Key section of storage account under Security+networking

storage_account_key = ""
storage_account_name = ""
connection_string = ""
container_name = ""

def uploadtoblobstorage(file_path,file_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container=container_name,blob=file_name)

    with open(file_path,"rb") as data:
        blob_client.upload_blob(data)
    print(f" Uploaded {file_name} .")

uploadtoblobstorage('<path_to_that_file>/<replace_file_name.csv>','<replace_file_name.csv>')




