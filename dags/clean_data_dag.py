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
from airflow.operators.bash import BashOperator

start_date = datetime(2024, 10, 11)

default_args = {
    'owner': 'airflow',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

ssh_train_password = Variable.get("ssh_train_password")

with DAG('dirty_data_clean', default_args=default_args, schedule_interval='@once', catchup=False) as dag:
    t1 = BashOperator(
        task_id="scp_python_scripts",
        bash_command=f"sshpass -v -p {ssh_train_password} scp -o StrictHostKeyChecking=no -r /opt/airflow/dags/airflow-git-sync ssh_train@spark_client:/home/ssh_train/python_scripts/airflow-git-sync"
    )
    
    t2 = SSHOperator(
        task_id='run_python', 
        command=f"python /home/ssh_train/python_scripts/airflow-git-sync/dirty_data_clean.py",
        ssh_conn_id='spark_ssh_conn',
        cmd_timeout=600
    )

    t1 >> t2

    


