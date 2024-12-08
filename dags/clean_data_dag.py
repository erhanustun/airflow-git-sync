from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os, sys
from airflow import DAG
import boto3, logging, botocore
from botocore.config import Config
import pandas as pd
import io
from datetime import timedelta


start_date = datetime(2024, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('dirty_data_clean', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    t1 = SSHOperator(
    task_id="dirty_data_clean",
    command=f"""source /dataops/airflowenv/bin/activate && 
            python /dataops/dirty_data_clean.py""",
     execution_timeout=timedelta(minutes=20),  # Set the execution timeout to 10 minutes (600 seconds)
    retries=3,
    ssh_conn_id='spark_ssh_conn')

    


