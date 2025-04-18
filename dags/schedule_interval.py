from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def say_hello():
    print("Hello, this DAG runs every time")


with DAG(
    dag_id = "schedule_hello_DAG",
    start_date=datetime(2025,4, 18),
    schedule_interval='* * * * *',
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = "schedule_hello_dag",
        python_callable=say_hello
    )