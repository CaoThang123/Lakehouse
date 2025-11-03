from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Hàm Python sẽ được Airflow gọi
def say_hello():
    print("👋 Hello from Airflow DAG!")

# Khai báo DAG
with DAG(
    dag_id="hello_airflow",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Không tự động chạy, chỉ chạy khi bạn click "Run"
    catchup=False,
    tags=["demo"],
) as dag:
    hello_task = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello,
    )
