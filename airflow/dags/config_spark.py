# config_spark.py
from pyspark.sql import SparkSession
import pyspark

def get_spark_session(app_name="Lakehouse-Iceberg-ETL"):
    AWS_ACCESS_KEY = "minioadmin"
    AWS_SECRET_KEY = "minioadmin"
    AWS_S3_ENDPOINT = "http://minio:9000"
    WAREHOUSE = "s3a://silver/"
    NESSIE_URI = "http://nessie:19120/api/v1"

    conf = (
        pyspark.SparkConf()
        .setAppName(app_name)
        .set('spark.jars.packages',
             'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1,'
             'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.3_2.12:0.67.0,'
             'org.apache.hadoop:hadoop-aws:3.3.4,'
             'com.amazonaws:aws-java-sdk-bundle:1.12.300')
        # Nessie Catalog (Iceberg)
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        # Kết nối MinIO
        .set("spark.sql.catalog.nessie.s3.endpoint", AWS_S3_ENDPOINT)
        .set("spark.sql.catalog.nessie.s3.access-key", AWS_ACCESS_KEY)
        .set("spark.sql.catalog.nessie.s3.secret-key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    return spark


# Nếu chạy file trực tiếp, sẽ test SparkSession
if __name__ == "__main__":
    spark = get_spark_session()
    print("✅ Spark session created successfully!")
    print("Spark version:", spark.version)
