from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime
import pandas as pd

def titanic_gender():
    df = pd.read_csv("/opt/airflow/dags/titanic.csv")
    male_count = len(df[df['Sex'] == 'male'])
    female_count = len(df[df['Sex'] == 'female'])
    print(f"남성: {male_count}, 여성: {female_count}")

    return 'Branch_Male' if male_count > female_count else 'Branch_Female'

def process_male():
    df = pd.read_csv('/opt/airflow/dags/titanic.csv')
    male_df = df[df['Sex'] == 'male']
    male_df.to_csv('/opt/airflow/dags/male_passengers.csv', index=False)

def process_female():
    df = pd.read_csv('/opt/airflow/dags/titanic.csv')
    female_df = df[df['Sex'] == 'female']
    female_df.to_csv('/opt/airflow/dags/female_passengers.csv', index=False)


with DAG(
    dag_id = 'Branch_gender',
    start_date=datetime(2025, 4, 18),
    schedule_interval=None,
    catchup=False
) as dag:
    
    branch_decision = BranchPythonOperator(
        task_id = 'Branch_decision',
        python_callable=titanic_gender
    )

    Branch_Male = PythonOperator(
        task_id = 'Branch_Male',
        python_callable=process_male
    )

    Branch_Female = PythonOperator(
        task_id = 'Branch_Female',
        python_callable=process_female
    )

    branch_decision >> [Branch_Male, Branch_Female]