# dags/ecommerce_pipeline_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable
from datetime import datetime
from ingestion import import_data_to_minio


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
    else:
        Variable.set("imported_files", str(files))  # lưu danh sách file để Spark task dùng
    return files
with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2025, 11, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    common_jars = ",".join([
            "/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar",
            "/opt/airflow/jars/nessie-spark-extensions-3.5_2.12-0.105.7.jar",
            "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
            "/opt/airflow/jars/hadoop-common-3.3.4.jar",
            "/opt/airflow/jars/aws-java-sdk-bundle-1.12.300.jar"
            ])
    
    # 1️⃣ Python task: Tải file lên MinIO
    import_data = PythonOperator(
        task_id='import_data',
        python_callable=task_import_data
    )

    # 2️⃣ Spark task: xử lý purchase
    process_purchase = SparkSubmitOperator(
        task_id='process_purchase',
        application="/opt/airflow/dags/load_purchase.py",
        conn_id="spark_default",
        jars=common_jars,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True
    )

    # 3️⃣ Spark task: xử lý survey
    process_survey = SparkSubmitOperator(
        task_id='process_survey',
        application="/opt/airflow/dags/load_survey.py",
        conn_id="spark_default",
        jars=common_jars,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True
    )

    # 4️⃣ Spark task: xử lý field
    process_field = SparkSubmitOperator(
        task_id='process_field',
        application="/opt/airflow/dags/load_field.py",
        conn_id="spark_default",
        jars=common_jars,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True
    )

    # 5️⃣ Spark task: build GOLD
    build_gold = SparkSubmitOperator(
        task_id='build_gold',
        application="/opt/airflow/dags/gold.py",
        conn_id="spark_default",
        jars=common_jars,
        executor_memory="2g",
        driver_memory="1g",
        verbose=True
    )


import_data >> [process_purchase, process_survey, process_field] >> build_gold
