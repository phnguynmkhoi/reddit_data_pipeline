import pandas as pd
import datetime
import numpy as np
import os

def transform_post(file_name:str, **kwargs):
    df = pd.read_csv(f'/opt/airflow/data/raw/{file_name}.csv')
    df['created_utc'] = df['created_utc'].apply(lambda x: str(datetime.datetime.fromtimestamp(x)))
    df['author'] = df['author'].astype('str')
    df['over_18'] = np.where((df['over_18']),True,False)
    if os.path.exists('/opt/airflow/data/transformed') == False:
        os.mkdir('/opt/airflow/data/transformed')
    df.to_csv(f'/opt/airflow/data/transformed/{file_name}.csv')