from dotenv import load_dotenv
import os
import praw
from praw import Reddit
import pandas as pd
import datetime
import numpy as np

def connect_to_reddit(client_id: str, secret_key: str, user_agent: str)-> Reddit:
    try:
        reddit_instance = Reddit(client_id=client_id,client_secret=secret_key, user_agent=user_agent)
        return reddit_instance
    except Exception as e:
        print(e)
        return None
    
def extract_post(instance: Reddit, subreddit: str, time_filter: str, limit: int):
    
    posts = instance.subreddit(subreddit).top(time_filter=time_filter, limit=limit)
    keys = ('id', 'title', 'selftext', 'author','num_comments','upvote_ratio', 'score','created_utc', 'over_18', 'url')
    extracted_post = []

    for post in posts:
        post = vars(post)
        post_dict = {key:post[key] for key in keys}
        print(post_dict)
        extracted_post.append(post_dict)
    
    print(extract_post)
    return extracted_post

def transform_post(extracted_post: list) -> pd.DataFrame:
    df = pd.DataFrame(extracted_post)
    df['created_utc'] = df['created_utc'].apply(lambda x: str(datetime.datetime.fromtimestamp(x)))
    df['author'] = df['author'].astype('str')
    df['over_18'] = np.where((df['over_18']),True,False)
    return df

def load_data_to_csv(df: pd.DataFrame, file_name: str):
    df.to_csv(f'./{file_name}',index=False)

def reddit_pipeline(file_name: str,subreddit: str, time_filter='day', limit=20, **context):
    
    load_dotenv()
    SECRET_KEY = os.getenv('secret_key')
    CLIENT_ID = os.getenv('client_id')
    USER_AGENT = os.getenv('user_agent')
    print(CLIENT_ID,SECRET_KEY,USER_AGENT)

    reddit_instance = connect_to_reddit(CLIENT_ID,SECRET_KEY,USER_AGENT)

    posts = extract_post(reddit_instance, subreddit, time_filter, limit)

    transform_df = transform_post(posts)

    load_data_to_csv(transform_df,file_name)



reddit_pipeline('test','dataengineering','day')