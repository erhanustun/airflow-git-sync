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


start_date = datetime(2023, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

ssh_train_password = Variable.get("ssh_train_password")
SQLALCHEMY_DATABASE_URL = Variable.get("SQLALCHEMY_DATABASE_URL")

with DAG('dirty_data_clean', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    t1 = BashOperator(
    task_id="scp_python_scrips",
    bash_command=f"sshpass -v -p {ssh_train_password} scp -o StrictHostKeyChecking=no -r 
    /opt/airflow/dags/airflow-git-sync/dags ssh_train@spark_client:/home/ssh_train/")
    
    t2 = SSHOperator(task_id='run_python', 
                    command=f"python /home/ssh_train/python_scripts/clean_data_dag.py -t {tmdb_token} -c {SQLALCHEMY_DATABASE_URL}",
                    ssh_conn_id='spark_ssh_conn',
                    cmd_timeout=600)

    '''t2 = SSHOperator(
    task_id="dirty_data_clean",
    command=f"""source /dataops/airflowenv/bin/activate && 
            python /dataops/dirty_data_clean.py""",
     execution_timeout=timedelta(minutes=20),  # Set the execution timeout to 10 minutes (600 seconds)
    retries=3,
    ssh_conn_id='spark_ssh_conn')'''


    t1 >> t2
    


