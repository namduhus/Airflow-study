from airflow import DAG   # 워크플로우를 정의하는 핵심 클래스
from airflow.operators.python import PythonOperator   # Python 함수를 실행하는 Task를 만들기 위해 필요한 클래스
from datetime import datetime # DAG 시작 날짜를 지정하기 위해 사용

def say_hello():
    print("Hello from Airflow!")  

with DAG(
    dag_id="hello_airflow",               # DAG의 고유 이름
    start_date = datetime(2025, 4, 17),   # start_date: DAG 시작 날짜, 과거 날짜로 설명하면 해당 일자부터 스케줄이 생성 됨
    schedule_interval="@daily",           # 매일 실행을 의미
    catchup=False,                        # 과거 스케줄을 몰아서 실행할지 여부, False면 현재 시점부터 실행
    tags=["example"],                    # DAG 분류용 태그로 UI에서 필터링 할 수 있다.
) as dag:                                 # 구문은 DAG 컨텍스트를 지정해서 그 안에서 정의되니 Task들이 이 DAG에 속하도록 도와줌
    hello_task = PythonOperator(
        task_id = "say_hello",
        python_callable=say_hello,
    )