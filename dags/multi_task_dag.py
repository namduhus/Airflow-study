from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def task_1():
    print("실행 중: Task1")


def task_2():
    print("실행 중: Task_2")


with DAG(
    dag_id = "multi_task_dag",
    start_date=datetime(2025, 4, 17),
    schedule_interval="@once",
    catchup=False,
    tags=["example"]
)as dag:
    t1 = PythonOperator(
        task_id = "task_1",
        python_callable=task_1
    )
    t2 = PythonOperator(
        task_id = "task_2",
        python_callable=task_2
    )

    # 실행 순서 지정
    t1 >> t2