FROM apache/airflow:2.10.2-python3.10
ADD requirements.txt /opt/airflow
RUN pip install --no-cache-dir -r requirements.txt