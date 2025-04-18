from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import random

# 실패 콜백 함수
def faile_callback(context):
    print("💥 Task Failed!")
    print("Task ID:", context['task_instance'].task_id)
    print("Dag ID:", context['dag'].dag_id)    

# 실패할 확률 함수
def unstable_fuc():
    if random.random() < 0.7:
        raise Exception("simulated failure!")
    print("success")

with DAG(
    dag_id = 'faile_task',
    start_date=datetime(2025, 4, 17),
    schedule_interval=None,
    catchup=False,
    default_args={
        'retries': 3,
        'retry_delay' : timedelta(seconds=10),
        'on_faile_callback': faile_callback
    }

) as dag:
    PythonOperator(
        task_id = 'task',
        python_callable=unstable_fuc
    )