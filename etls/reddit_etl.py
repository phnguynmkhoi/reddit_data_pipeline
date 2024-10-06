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
        extracted_post.append(post_dict)
    
    return extracted_post

def transform_post(extracted_post: list) -> pd.DataFrame:
    df = pd.DataFrame(extracted_post)
    df['created_utc'] = df['created_utc'].apply(lambda x: str(datetime.datetime.fromtimestamp(x)))
    df['author'] = df['author'].astype('str')
    df['over_18'] = np.where((df['over_18']),True,False)
    return df

def load_data_to_csv(df: pd.DataFrame, file_name: str):
    df.to_csv(f'/opt/airflow/data/{file_name}',index=False)