import praw
from praw import Reddit
import pandas as pd
import datetime
import numpy as np
from minio import Minio
from minio.error import S3Error

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
    df.to_csv(f'/opt/airflow/data/{file_name}.csv',index=False)

def load_data_to_minio(bucket_name: str,file_path:str, MINIO_ACCESS_KEY: str, MINIO_SECRET_KEY: str):

    # Initialize the Minio client
    client = Minio(
        "minio:9000",  # MinIO server address
        access_key=MINIO_ACCESS_KEY,  # Replace with your MinIO access key
        secret_key=MINIO_SECRET_KEY,  # Replace with your MinIO secret key
        secure=False  # Set to True if you're using HTTPS
    )

    # Define bucket name and file paths
    object_name = file_path.split('/')[-1]  # The object name you want to save in the bucket

    # Create the bucket if it doesn't exist
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Upload the file
    try:
        client.fput_object(bucket_name, object_name, file_path)
        print(f"Successfully uploaded {file_path} to {bucket_name}/{object_name}.")
    except S3Error as e:
        print(f"Error uploading file: {e}")

    # Download the file back
    # download_path = f"./tests/{object_name}"
    # try:
    #     client.fget_object(bucket_name, object_name, download_path)
    #     print(f"Successfully downloaded {object_name} to {download_path}.")
    # except S3Error as e:
    #     print(f"Error downloading file: {e}")
