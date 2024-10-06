from dotenv import load_dotenv
import os
from etls.reddit_etl import *

def reddit_pipeline(file_name: str,subreddit: str, time_filter='day', limit=20, **context):

    load_dotenv()
    SECRET_KEY = os.getenv('secret_key')
    CLIENT_ID = os.getenv('client_id')
    USER_AGENT = os.getenv('user_agent')
    reddit_instance = connect_to_reddit(CLIENT_ID,SECRET_KEY,USER_AGENT)

    posts = extract_post(reddit_instance, subreddit, time_filter, limit)

    transform_df = transform_post(posts)

    load_data_to_csv(transform_df,file_name)