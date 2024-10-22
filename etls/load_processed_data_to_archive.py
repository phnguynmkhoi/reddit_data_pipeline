import boto3
from botocore.client import Config
import os
import datetime

def archive_data(MINIO_ACCESS_KEY: str, MINIO_SECRET_KEY: str):
# Replace these with your MinIO configuration
    minio_endpoint = 'http://minio:9000'  # Your MinIO endpoint

    # Create a session with MinIO
    s3 = boto3.client('s3',
                    endpoint_url=minio_endpoint,
                    aws_access_key_id=MINIO_ACCESS_KEY,
                    aws_secret_access_key=MINIO_ACCESS_KEY,
                    config=Config(signature_version='s3v4'))

    today = datetime.datetime.now()
    today = today.strftime('%Y%m%d')
    bucket = "reddit"
    source_path = f"raw/{today}/"
    des_path = f"processed/{today}/"

    
    object_list = s3.list_objects(Bucket=bucket, Prefix = source_path)

    for x in object_list['Contents']:
        file_name = x['Key'].split('/')[-1]
        key_path = f'{des_path}{file_name}'
        # print(des_path)
        s3.copy_object(
            Bucket=bucket,
            CopySource={'Bucket':bucket, 'Key': f"{x['Key']}"},
            Key=key_path
        )
         
        s3.delete_object(Bucket=bucket, Key=x['Key'])