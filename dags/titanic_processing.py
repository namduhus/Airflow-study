from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


def count_survived():
    df = pd.read_csv('/opt/airflow/dags/titanic.csv')
    survived = df[df['Survived'] == 1]
    survived.to_csv("Airflow-study\dags\titanic_survived.csv", index=False)
    print(f"생존자수 : {len(survived)}")


def count_death():
    df = pd.read_csv('/opt/airflow/dags/titanic.csv')
    died = df[df['Survived'] == 0]
    print(f"사망자 수 : {len(died)}")



with DAG(
    dag_id = 'titanic_processing',
    start_date=datetime(2025, 4, 18),
    schedule_interval=None,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'count_survived',
        python_callable=count_survived
    )
    t2 = PythonOperator(
        task_id = 'count_died',
        python_callable=count_death
    )

    t1 >> t2