from minio import Minio
from minio.error import S3Error
from minio.commonconfig import REPLACE, CopySource

from dotenv import load_dotenv
import os
import datetime

load_dotenv()

minio_endpoint = 'localhost:10000'  
access_key = os.getenv('minio_access_key')
secret_key = os.getenv('minio_secret_key')

client = Minio(
    minio_endpoint,  
    access_key=access_key,  
    secret_key=secret_key,  
    secure=False  
)

today = datetime.datetime.now()
today = today.strftime('%Y%m%d')
bucket = "reddit"
prefix = f"raw/{today}/"

# print(source_path)
objects = client.list_objects(bucket, prefix, recursive=True)
for obj in objects:
    old_file_path = obj.object_name
    new_file_path = obj.object_name.replace('raw','processed')
    client.copy_object(
        bucket,
        new_file_path,
        CopySource(bucket, obj.object_name)
    )

    client.remove_object(bucket, old_file_path)