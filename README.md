# Cài đặt từng image nếu chạy docker-compose bị TLS handshake timeout 
docker pull bitnamilegacy/spark:3.5.1
docker pull jupyter/pyspark-notebook
docker pull minio/minio
docker pull dremio/dremio-oss:latest
docker pull ghcr.io/projectnessie/nessie:0.105.6
docker pull postgres:15
docker pull apache/superset:3.1.0
docker pull apache/airflow:2.8.0
docker pull ghcr.io/mlflow/mlflow:v2.14.1

# tải các file jars bỏ vào thư mục airflow/jars :

https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar
https://repo1.maven.org/maven2/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/0.105.7/nessie-spark-extensions-3.5_2.12-0.105.7.jar
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar
https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.300/aws-java-sdk-bundle-1.12.300.jar