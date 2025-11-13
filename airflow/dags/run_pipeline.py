# dags/ecommerce_pipeline_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from ingestion import import_data_to_minio
from load_purchase import process_purchase_file
from load_survey import process_survey_file
from load_field import process_fields_file
from gold import build_gold_layer

# ===============================
# Các hàm wrapper cho từng task
# ===============================
def task_import_data():
    """
    Task 1️⃣: Upload CSV mới lên MinIO
    Trả về danh sách file để các task tiếp theo dùng
    """
    files = import_data_to_minio()
    if not files:
        print("⏹ Không có file mới → Dừng pipeline.")
    return files

def task_process_purchase(**context):
    files = context['ti'].xcom_pull(task_ids='import_data')
    if not files:
        return
    for f in files:
        if "purchase" in f.lower():
            print(f"➡️ Xử lý PURCHASE: {f}")
            process_purchase_file(f)

def task_process_survey(**context):
    files = context['ti'].xcom_pull(task_ids='import_data')
    if not files:
        return
    for f in files:
        if "survey" in f.lower():
            print(f"➡️ Xử lý SURVEY: {f}")
            process_survey_file(f)

def task_process_field(**context):
    files = context['ti'].xcom_pull(task_ids='import_data')
    if not files:
        return
    for f in files:
        if "field" in f.lower():
            print(f"➡️ Xử lý FIELDS: {f}")
            process_fields_file(f)

def task_build_gold():
    print("\n🎯 Bắt đầu build Gold Layer...")
    build_gold_layer()
    print("🎉 Hoàn tất pipeline.")

# ===============================
# DAG definition
# ===============================
with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    import_data = PythonOperator(
        task_id='import_data',
        python_callable=task_import_data
    )

    process_purchase = PythonOperator(
        task_id='process_purchase',
        python_callable=task_process_purchase,
        provide_context=True
    )

    process_survey = PythonOperator(
        task_id='process_survey',
        python_callable=task_process_survey,
        provide_context=True
    )

    process_field = PythonOperator(
        task_id='process_field',
        python_callable=task_process_field,
        provide_context=True
    )

    build_gold = PythonOperator(
        task_id='build_gold',
        python_callable=task_build_gold
    )

# ===============================
# DAG structure
# ===============================
# import_data → purchase/survey/field → build_gold
import_data >> [process_purchase, process_survey, process_field] >> build_gold
