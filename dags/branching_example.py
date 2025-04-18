from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
import random
from airflow.operators.empty import EmptyOperator


def choose_branch():
    rand_val = random.random()
    print(f"[Branch 결정] 생성된 Random 값: {rand_val}")
    return 'Branch_A' if rand_val< 0.5 else 'Branch_B'

def task_a():
    print(">> Branch_A 실행 중!")

def task_b():
    print(">> Branch_B 실행 중!")

with DAG(
    dag_id = "branching_example",
    start_date=datetime(2025, 4, 18),
    schedule_interval=None,
    catchup=False
) as dag:
    start = EmptyOperator(task_id='Start')
    
    branch = BranchPythonOperator(
        task_id = 'Branch_decision',
        python_callable=choose_branch
    )

    t1 = PythonOperator(
        task_id = "Branch_A",
        python_callable=task_a
    )
    t2 = PythonOperator(
        task_id = 'Branch_B',
        python_callable=task_b
    )

    end = EmptyOperator(task_id='End')
    start >> branch
    branch >> t1 >> end
    branch >> t2 >> end