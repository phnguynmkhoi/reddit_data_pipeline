FROM apache/airflow:2.10.2-python3.10

COPY requirements.txt /opt/airflow/

# Install Java and other necessary dependencies


USER root
RUN apt-get update && apt-get install -y gcc python3-dev

# RUN mkdir -p /opt/airflow/logs/scheduler && \
#     chown -R airflow:airflow /opt/airflow/logs

USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# Use a base image of your choice
