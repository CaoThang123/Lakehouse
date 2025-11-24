from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, date_format, year, month, dayofmonth, quarter, dayofweek, udf
from pyspark.sql.types import StringType
import pyspark

# -----------------------------
# Hàm tạo SparkSession
# -----------------------------
def get_spark_session1():
    AWS_ACCESS_KEY = "minioadmin"
    AWS_SECRET_KEY = "minioadmin"
    AWS_S3_ENDPOINT = "http://minio:9000"
    WAREHOUSE = "s3a://gold/"
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
        .setAppName("Lakehouse-Iceberg-GOLD")  
        .setMaster("spark://spark-master:7077")
        .set("spark.jars", ",".join(jars_list))
        .set("spark.driver.extraClassPath", ",".join(jars_list))
        .set("spark.executor.extraClassPath", ",".join(jars_list))
        # serializer để performance tốt hơn
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Iceberg catalog
        .set("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.nessie.uri", NESSIE_URI)
        .set("spark.sql.catalog.nessie.ref", "main")
        .set("spark.sql.catalog.nessie.authentication.type", "NONE")
        .set("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .set("spark.sql.catalog.nessie.warehouse", WAREHOUSE)
        .set("spark.sql.catalog.nessie.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        # S3 config
        .set("spark.sql.catalog.nessie.s3.endpoint", AWS_S3_ENDPOINT)
        .set("spark.sql.catalog.nessie.s3.access-key", AWS_ACCESS_KEY)
        .set("spark.sql.catalog.nessie.s3.secret-key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY)
        .set("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_KEY)
        .set("spark.hadoop.fs.s3a.endpoint", AWS_S3_ENDPOINT)
        .set("spark.hadoop.fs.s3a.path.style.access", "true")
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # Tối ưu upload/download cho S3/MinIO
        .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .set("spark.hadoop.fs.s3a.fast.upload", "true")
        .set("spark.hadoop.fs.s3a.multipart.size", "104857600")  # 100MB
        # Cấu hình để tránh lỗi catalog initialization
        .set("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .set("spark.sql.catalog.nessie.cache-enabled", "false")
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # đảm bảo path-style access và impl cho S3
    hconf = spark._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    hconf.set("fs.s3a.fast.upload", "true")
    hconf.set("fs.s3a.multipart.size", "104857600")

    return spark


# -----------------------------
# Hàm ghi Iceberg: tạo nếu chưa có, append nếu đã tồn tại
# -----------------------------
def save_iceberg_table(df, table_name, spark):
    existing_tables = [t.name for t in spark.catalog.listTables("nessie", "main")]
    if table_name in existing_tables:
        print(f"Bảng {table_name} đã tồn tại → append dữ liệu mới")
        df.writeTo(f"nessie.{table_name}").append()
    else:
        print(f"Bảng {table_name} chưa tồn tại → tạo mới")
        df.writeTo(f"nessie.{table_name}").create()

# -----------------------------
# Hàm tạo dim_customer
# -----------------------------
def build_dim_customer(df_survey):
    dim_customer = (
        df_survey.selectExpr(
            "`Survey ResponseID` as customer_id",
            "`Q-demos-age` as age_group",
            "`Q-demos-hispanic` as hispanic",
            "`Q-demos-race` as race",
            "`Q-demos-education` as education",
            "`Q-demos-income` as income",
            "`Q-demos-gender` as gender",
            "`Q-sexual-orientation` as sexual_orientation",
            "`Q-demos-state` as state",
            "`Q-amazon-use-howmany` as amazon_use_howmany",
            "`Q-amazon-use-hh-size` as amazon_use_hh_size",
            "`Q-amazon-use-how-oft` as amazon_use_how_oft",
            "`Q-substance-use-cigarettes` as substance_use_cigarettes",
            "`Q-substance-use-marijuana` as substance_use_marijuana",
            "`Q-substance-use-alcohol` as substance_use_alcohol",
            "`Q-personal-diabetes` as personal_diabetes",
            "`Q-personal-wheelchair` as personal_wheelchair",
            "`Q-life-changes` as life_changes",
            "`Q-sell-YOUR-data` as sell_your_data",
            "`Q-sell-consumer-data` as sell_consumer_data",
            "`Q-small-biz-use` as small_biz_use",
            "`Q-census-use` as census_use",
            "`Q-research-society` as research_society"
        )
        .dropDuplicates(["customer_id"])
    )
    return dim_customer

# -----------------------------
# Hàm tạo dim_product
# -----------------------------
def build_dim_product(df_amazon):
    dim_product = df_amazon.select(
        col("ASIN/ISBN (Product Code)").alias("product_id"),
        col("Title").alias("product_title"),
        col("Category").alias("product_category")
    ).dropDuplicates(["product_id"])
    return dim_product

# -----------------------------
# Hàm tạo dim_time
# -----------------------------
def build_dim_time(df_amazon):
    dim_time = df_amazon.select(col("Order Date").alias("order_date")).dropDuplicates()
    dim_time = dim_time.withColumn("year", year(col("order_date"))) \
                       .withColumn("month", month(col("order_date"))) \
                       .withColumn("day", dayofmonth(col("order_date"))) \
                       .withColumn("quarter", quarter(col("order_date"))) \
                       .withColumn("weekday", dayofweek(col("order_date"))) \
                       .withColumn("weekday_name", date_format(col("order_date"), "EEEE")) \
                       .withColumn("time_id", monotonically_increasing_id())
    return dim_time

# -----------------------------
# Hàm tạo dim_location
# -----------------------------
def build_dim_location(df_amazon):
    state_lookup = {
        "NJ": ("New Jersey", "Northeast"), "NY": ("New York", "Northeast"),
        "CA": ("California", "West"), "TX": ("Texas", "South"),
        "AZ": ("Arizona", "West"), "SC": ("South Carolina", "South"),
        "LA": ("Louisiana", "South"), "MN": ("Minnesota", "Midwest"),
        "DC": ("District of Columbia", "South"), "OR": ("Oregon", "West"),
        "VA": ("Virginia", "South"), "RI": ("Rhode Island", "Northeast"), "WY": ("Wyoming", "West"),
        "KY": ("Kentucky", "South"), "NH": ("New Hampshire", "Northeast"), "MI": ("Michigan", "Midwest"),
        "NV": ("Nevada", "West"), "WI": ("Wisconsin", "Midwest"), "ID": ("Idaho", "West"),
        "NE": ("Nebraska", "Midwest"), "CT": ("Connecticut", "Northeast"), "MT": ("Montana", "West")
    }

    def get_state_name(code): return state_lookup.get(code, ("Unknown", "Unknown"))[0]
    def get_region(code): return state_lookup.get(code, ("Unknown", "Unknown"))[1]

    udf_state_name = udf(get_state_name, StringType())
    udf_region = udf(get_region, StringType())

    dim_location = df_amazon.select(col("Shipping Address State").alias("state_code")).dropDuplicates()
    dim_location = dim_location.withColumn("location_id", monotonically_increasing_id()) \
                               .withColumn("state_name", udf_state_name(col("state_code"))) \
                               .withColumn("region", udf_region(col("state_code")))
    return dim_location

# -----------------------------
# Hàm tạo fact_order
# -----------------------------
def build_fact_order(df_amazon, dim_time, dim_product, dim_customer, dim_location):
    fact_order = df_amazon \
        .join(dim_time, df_amazon["Order Date"] == dim_time["order_date"], "left") \
        .join(dim_product, df_amazon["ASIN/ISBN (Product Code)"] == dim_product["product_id"], "left") \
        .join(dim_customer, df_amazon["Survey ResponseID"] == dim_customer["customer_id"], "left") \
        .join(dim_location, df_amazon["Shipping Address State"] == dim_location["state_code"], "left")

    fact_order = fact_order.select(
        col("time_id"),
        col("customer_id"),
        col("product_id"),
        col("location_id"),
        col("Purchase Price Per Unit").alias("purchase_price_per_unit"),
        col("Quantity").alias("quantity"),
        (col("Purchase Price Per Unit") * col("Quantity")).alias("total_price")
    )
    return fact_order

# -----------------------------
# Hàm main để build Gold Layer
# -----------------------------
def build_gold_layer():
    spark = get_spark_session1()
    
    # Đợi catalog khởi tạo xong
    import time
    time.sleep(2)
    
    # Sử dụng SQL query thay vì spark.table() để tránh lỗi catalog initialization
    try:
        df_amazon = spark.sql("SELECT * FROM nessie.amazon_purchase")
        df_survey = spark.sql("SELECT * FROM nessie.survey")
    except Exception as e:
        print(f"⚠️ Lỗi khi đọc bằng SQL, thử dùng spark.table(): {str(e)}")
        # Fallback về spark.table() nếu SQL không hoạt động
        df_amazon = spark.table("nessie.amazon_purchase")
        df_survey = spark.table("nessie.survey")

    dim_customer = build_dim_customer(df_survey)
    dim_product = build_dim_product(df_amazon)
    dim_time = build_dim_time(df_amazon)
    dim_location = build_dim_location(df_amazon)
    fact_order = build_fact_order(df_amazon, dim_time, dim_product, dim_customer, dim_location)

    save_iceberg_table(dim_customer, "dim_customer", spark)
    save_iceberg_table(dim_product, "dim_product", spark)
    save_iceberg_table(dim_time, "dim_time", spark)
    save_iceberg_table(dim_location, "dim_location", spark)
    save_iceberg_table(fact_order, "fact_order", spark)

    spark.stop()

# Nếu muốn chạy trực tiếp
if __name__ == "__main__":
    build_gold_layer()
