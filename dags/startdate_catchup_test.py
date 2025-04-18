from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
from datetime import datetime, timedelta

def say_hello():
    print("Hello from a test")

default_args = {
    'owner' : 'airflow',
    'retries' : 1,
    'retry_delay' : timedelta(seconds=5),
}


with DAG(
    dag_id = 'startdate_catchup_test',
    default_args=default_args,
    description="start_date & catchup 실습",
    start_date=datetime(2025, 4, 5),
    schedule_interval='@daily',
    catchup=True,
    tags=['step4']
) as dag:
    t1 = PythonOperator(
        task_id = 'hello',
        python_callable=say_hello
    )