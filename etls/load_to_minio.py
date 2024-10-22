from minio import Minio
from minio.error import S3Error
import datetime

def load_data_to_minio(caterogy: str,file_name:str, MINIO_ACCESS_KEY: str, MINIO_SECRET_KEY: str, **kwargs):


    file_path = f'/opt/airflow/data/{file_name}.csv'

    client = Minio(
        "minio:9000", 
        access_key=MINIO_ACCESS_KEY, 
        secret_key=MINIO_SECRET_KEY, 
        secure=False  
    )
    today = datetime.datetime.now()
    today = today.strftime('%Y%m%d')
    object_name = f'raw/{today}/{file_name}.csv'

    if not client.bucket_exists('reddit'):
        client.make_bucket('reddit')

    try:
        client.fput_object('reddit', object_name, file_path)
        print(f"Successfully uploaded {file_path} to reddit/{object_name}.")
    except S3Error as e:
        print(f"Error uploading file: {e}")