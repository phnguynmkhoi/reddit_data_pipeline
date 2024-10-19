import praw
from praw import Reddit
import pandas as pd
import os


def connect_to_reddit(client_id: str, secret_key: str, user_agent: str)-> Reddit:
    try:
        reddit_instance = Reddit(client_id=client_id,client_secret=secret_key, user_agent=user_agent)
        return reddit_instance
    except Exception as e:
        print(e)
        return None
    
def extract_post(file_name: str, client_id:str, secret_key:str, user_agent:str ,subreddit: str, time_filter='day', limit=10, **kwargs):
    
    instance = connect_to_reddit(client_id,secret_key,user_agent)

    posts = instance.subreddit(subreddit).top(time_filter=time_filter, limit=limit)
    keys = ('id', 'title', 'selftext', 'author','num_comments','upvote_ratio', 'score','created_utc', 'over_18', 'url')
    extracted_post = []

    for post in posts:
        post = vars(post)
        post_dict = {key:post[key] for key in keys}
        extracted_post.append(post_dict)
    
    df = pd.DataFrame(extracted_post)
    df.set_index('id',inplace=True)
    if os.path.exists('/opt/airflow/data/raw') == False:
        os.mkdir('/opt/airflow/data/raw')
    df.to_csv(f'/opt/airflow/data/raw/{file_name}.csv')
