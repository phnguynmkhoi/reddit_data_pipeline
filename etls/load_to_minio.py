from minio import Minio
from minio.error import S3Error

def load_data_to_minio(caterogy: str,file_name:str, MINIO_ACCESS_KEY: str, MINIO_SECRET_KEY: str, **kwargs):


    file_path = f'/opt/airflow/data/{file_name}.csv'
    # Initialize the Minio client
    client = Minio(
        "minio:9000",  # MinIO server address
        access_key=MINIO_ACCESS_KEY,  # Replace with your MinIO access key
        secret_key=MINIO_SECRET_KEY,  # Replace with your MinIO secret key
        secure=False  # Set to True if you're using HTTPS
    )

    # Define bucket name and file paths
    object_name = f'raw/{file_name}.csv' # The object name you want to save in the bucket

    # Create the bucket if it doesn't exist
    if not client.bucket_exists('reddit'):
        client.make_bucket('reddit')

    # Upload the file
    try:
        client.fput_object('reddit', object_name, file_path)
        print(f"Successfully uploaded {file_path} to reddit/{object_name}.")
        #Clean up the data file after loading it to Cloud Storage


    except S3Error as e:
        print(f"Error uploading file: {e}")