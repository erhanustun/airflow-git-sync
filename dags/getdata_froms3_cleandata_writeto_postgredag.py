from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import os, sys
from airflow import DAG

start_date = datetime(2024, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('s3_to_postgres_dag', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    t1 = SSHOperator(
    task_id="dirty_data_clean",
    command=f"""source /dataops/airflowenv/bin/activate && 
    python /dataops/dirty_data_clean.py""",
    ssh_conn_id='spark_ssh_conn')

    t1
