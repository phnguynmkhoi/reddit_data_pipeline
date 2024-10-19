import sys
import os

sys.path.append('/opt/airflow/')

from datetime import timedelta
import datetime
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
# from pipelines.reddit_pipeline import *
from etls.extract import *
from etls.transform import *
from etls.load_to_minio import *
from dotenv import load_dotenv

# from airflow.models.baseoperator import chain

load_dotenv()

default_args = {
    'owner': 'minhkhoi',
    'depends_on_past': False,
    'start_date': datetime.datetime(2024,10,5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}


SECRET_KEY = os.getenv('secret_key')
CLIENT_ID = os.getenv('client_id')
USER_AGENT = os.getenv('user_agent')
MINIO_ACCESS_KEY = os.getenv('minio_access_key')
MINIO_SECRET_KEY = os.getenv('minio_secret_key')

with DAG('reddit_pipeline', default_args=default_args, schedule_interval=timedelta(minutes=5), description='Reddit ETL pipeline', catchup=False) as dag:
    
    current_time = datetime.datetime.now().strftime("%Y%m%d")
    subreddit = 'dataanalyst'
    file_name = f'reddit_{subreddit}_{current_time}'

    
    extract = PythonOperator(task_id='extract_data', python_callable=extract_post,
                           op_kwargs={
                               'file_name': file_name,
                               'client_id': CLIENT_ID,
                               'secret_key':SECRET_KEY,
                               'user_agent':USER_AGENT,
                               'subreddit': subreddit,
                               'time_filter': 'day',
                               'limit': 10
                           })
    
    load = PythonOperator(task_id='load_data', python_callable=load_data_to_minio,
                          op_kwargs={
                              'caterogy': subreddit,
                              'file_name': file_name,
                              'MINIO_ACCESS_KEY': MINIO_ACCESS_KEY,
                              'MINIO_SECRET_KEY': MINIO_SECRET_KEY
                          })


    extract >> load