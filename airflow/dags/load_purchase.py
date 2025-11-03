from config_spark import get_spark_session
from pyspark.sql.functions import col, to_date
from pyspark.sql.utils import AnalysisException

def process_purchase_file(filename="amazon-purchases.csv"):
    """
    Đọc file purchase từ MinIO (bronze) → xử lý → ghi vào Iceberg (silver)
    """
    spark = get_spark_session("Purchase-Bronze-to-Silver")

    bronze_path = f"s3a://bronze/ecommerse/{filename}"
    print(f"Đang đọc file: {bronze_path}")

    # 1️⃣ Đọc file CSV từ MinIO (Bronze)
    df_amazon = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .csv(bronze_path)

    print(f"Số dòng ban đầu: {df_amazon.count()}")

    # 2️⃣ Xử lý dữ liệu
    # Chuyển kiểu dữ liệu cột ngày
    if "Order Date" in df_amazon.columns:
        df_amazon = df_amazon.withColumn("Order Date", to_date(col("Order Date"), "yyyy-MM-dd"))

    # Loại bỏ toàn bộ dòng có giá trị null
    df_amazon_clean = df_amazon.na.drop()
    print(f"Sau khi làm sạch: {df_amazon_clean.count()} dòng còn lại")

    # 3️⃣ Ghi vào bảng Iceberg (Silver)
    table_name = "nessie.amazon_purchase"

    try:
        # Kiểm tra xem bảng đã tồn tại chưa
        spark.table(table_name)
        table_exists = True
        print(f"Bảng {table_name} đã tồn tại → append dữ liệu mới.")
    except AnalysisException:
        table_exists = False
        print(f"Bảng {table_name} chưa tồn tại → sẽ tạo mới.")

    if table_exists:
        df_amazon_clean.writeTo(table_name).append()
    else:
        df_amazon_clean.writeTo(table_name).createOrReplace()

    print(f"Hoàn tất xử lý file: {filename}")
    spark.stop()

