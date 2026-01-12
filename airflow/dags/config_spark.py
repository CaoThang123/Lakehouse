# config_spark.py
from pyspark.sql import SparkSession
import pyspark

def get_spark_session(app_name="Lakehouse-Iceberg-ETL"):
    AWS_ACCESS_KEY = "minioadmin"
    AWS_SECRET_KEY = "minioadmin"
    AWS_S3_ENDPOINT = "http://minio:9000"
    WAREHOUSE = "s3a://silver/"
    NESSIE_URI = "http://nessie:19120/api/v1"

    jars_list = [
        "/opt/airflow/jars/iceberg-spark-runtime-3.5_2.12-1.10.0.jar",
        "/opt/airflow/jars/nessie-spark-extensions-3.5_2.12-0.105.7.jar",
        "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
        "/opt/airflow/jars/hadoop-common-3.3.4.jar",
        "/opt/airflow/jars/aws-java-sdk-bundle-1.12.300.jar"
    ]

    conf = (
        pyspark.SparkConf()
        .setAppName(app_name)
        .setMaster("spark://spark-master:7077")
        .set("spark.jars", ",".join(jars_list))
        .set("spark.driver.extraClassPath", ",".join(jars_list))
        .set("spark.executor.extraClassPath", ",".join(jars_list))
        # serializer để performance tốt hơn
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Nessie Catalog (Iceberg)
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        # Kết nối MinIO
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.sql.catalog.nessie.s3.endpoint", AWS_S3_ENDPOINT)
        .set("spark.sql.catalog.nessie.s3.access-key", AWS_ACCESS_KEY)
        .set("spark.sql.catalog.nessie.s3.secret-key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        # Iceberg extensions
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.nessie.cache-enabled", "false")
        
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    return spark


# Nếu chạy file trực tiếp, sẽ test SparkSession
if __name__ == "__main__":
    spark = get_spark_session()
    print("✅ Spark session created successfully!")
    print("Spark version:", spark.version)
