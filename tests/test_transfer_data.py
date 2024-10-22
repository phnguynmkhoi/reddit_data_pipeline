import boto3
from botocore.client import Config
from dotenv import load_dotenv
import os
import datetime
load_dotenv()
# Replace these with your MinIO configuration
minio_endpoint = 'http://localhost:10000'  # Your MinIO endpoint
access_key = os.getenv('minio_access_key')
secret_key = os.getenv('minio_secret_key')

# Create a session with MinIO
s3 = boto3.client('s3',
                  endpoint_url=minio_endpoint,
                  aws_access_key_id=access_key,
                  aws_secret_access_key=secret_key,
                  config=Config(signature_version='s3v4'))

today = datetime.datetime.now()
today = today.strftime('%Y%m%d')
bucket = "reddit"
source_path = f"raw/{today}/"
des_path = f"processed/{today}/"

print(source_path)
object_list = s3.list_objects(Bucket=bucket, Prefix = source_path)
print(object_list.keys())
print('end')
for x in object_list['Contents']:
    file_name = x['Key'].split('/')[-1]
    key_path = f'{des_path}{file_name}'
    print(key_path)
    s3.copy_object(
        Bucket=bucket,
        CopySource={'Bucket':bucket, 'Key': f"{x['Key']}"},
        Key=key_path
    )
        
#     s3.delete_object(Bucket=bucket, Key=x['Key'])