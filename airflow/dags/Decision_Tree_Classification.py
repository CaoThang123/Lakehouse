from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='decision_tree_classification',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    ...
