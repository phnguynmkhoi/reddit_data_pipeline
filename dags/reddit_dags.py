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
from pipelines.reddit_pipeline import *

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

SUBREDDIT = 'datascience'

with DAG('reddit_pipeline', default_args=default_args, schedule_interval=timedelta(minutes=5), description='Reddit ETL pipeline', catchup=False) as dag:
    task1 = PythonOperator(task_id='extract_data', python_callable=reddit_pipeline,
                           op_kwargs={
                               'file_name': f'reddit_{SUBREDDIT}_{datetime.datetime.now().strftime("%Y%m%d")}',
                               'subreddit': SUBREDDIT,
                               'time_filter': 'day',
                               'limit': 100
                           })
    
    
    task1 