
import pandas as pd
from minio import Minio
from minio.error import S3Error
from dotenv import load_dotenv
import os

load_dotenv()
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
def load_data_to_minio(bucket_name: str,file_path:str, MINIO_ACCESS_KEY: str, MINIO_SECRET_KEY: str):

    # Initialize the Minio client
    client = Minio(
        "localhost:9000",  # MinIO server address
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
    download_path = f"./tests/{object_name}-1"
    try:
        client.fget_object(bucket_name, object_name, download_path)
        print(f"Successfully downloaded {object_name} to {download_path}.")
    except S3Error as e:
        print(f"Error downloading file: {e}")

MINIO_ACCESS_KEY = os.getenv('minio_access_key')
MINIO_SECRET_KEY = os.getenv('minio_secret_key')

load_data_to_minio('testing','./extracted_data/reddit_dataengineering_20241006',MINIO_ACCESS_KEY,MINIO_SECRET_KEY)

df = pd.read_csv('./tests/reddit_dataengineering_20241006')
print(df)