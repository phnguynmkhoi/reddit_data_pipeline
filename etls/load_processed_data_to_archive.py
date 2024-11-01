from minio import Minio
from minio.error import S3Error
from minio.commonconfig import REPLACE, CopySource

import os
import datetime

def archive_data(file_name: str,MINIO_ACCESS_KEY: str, MINIO_SECRET_KEY: str):
# Replace these with your MinIO configuration
    minio_endpoint = 'minio:9000'  # Your MinIO endpoint

    # Create a session with MinIO
    client = Minio(
        minio_endpoint,  
        access_key=MINIO_ACCESS_KEY,  
        secret_key=MINIO_SECRET_KEY,  
        secure=False  
    )

    today = datetime.datetime.now()
    today = today.strftime('%Y%m%d')
    bucket = "reddit"
    source_path = f"raw/{today}/{file_name}.csv"
    des_path = f"archived/{today}/{file_name}.csv"
    try:
        client.copy_object(
            bucket,
            des_path,
            CopySource(bucket, source_path)
        )

        client.remove_object(bucket, source_path)
    except S3Error as e:
        print(f"Error moving file: {e}")