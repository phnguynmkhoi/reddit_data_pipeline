from dotenv import load_dotenv
import os
from etls.reddit_etl import *

def reddit_pipeline(file_name: str,subreddit: str, time_filter='day', limit=20, **context):

    load_dotenv()
    SECRET_KEY = os.getenv('secret_key')
    CLIENT_ID = os.getenv('client_id')
    USER_AGENT = os.getenv('user_agent')
    MINIO_ACCESS_KEY = os.getenv('minio_access_key')
    MINIO_SECRET_KEY = os.getenv('minio_secret_key')

    reddit_instance = connect_to_reddit(CLIENT_ID,SECRET_KEY,USER_AGENT)

    posts = extract_post(reddit_instance, subreddit, time_filter, limit)

    transform_df = transform_post(posts)

    load_data_to_csv(transform_df,file_name)

    file_path = f'/opt/airflow/data/{file_name}.csv'
    load_data_to_minio(bucket_name=subreddit,file_path=file_path,MINIO_ACCESS_KEY=MINIO_ACCESS_KEY,MINIO_SECRET_KEY=MINIO_SECRET_KEY)
    