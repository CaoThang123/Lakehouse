from config_spark import get_spark_session

def process_fields_file(filename="fields.csv"):
    """
    Đọc file fields từ MinIO (bronze) → xử lý → ghi vào Iceberg (silver)
    Nếu bảng chưa tồn tại, tạo mới; nếu đã có, append dữ liệu.
    """
    spark = get_spark_session("Fields-Bronze-to-Silver")

    bronze_path = f"s3a://bronze/ecommerse/{filename}"
    print(f"📥 Đang đọc file: {bronze_path}")

    # 1️⃣ Đọc file CSV từ MinIO (Bronze)
    df_fields = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(bronze_path)

    print(f"📊 Số dòng ban đầu: {df_fields.count()}")

    # 2️⃣ Làm sạch dữ liệu
    df_fields_clean = df_fields.na.drop()
    print(f"🧹 Sau khi loại bỏ null: {df_fields_clean.count()} dòng còn lại")

    # 3️⃣ Ghi vào bảng Iceberg (Silver)
    table_name = "nessie.fields"
    existing_tables = [t.name for t in spark.catalog.listTables("nessie")]

    if table_name.split(".")[-1] in existing_tables:
        print(f"💾 Bảng {table_name} đã tồn tại → append dữ liệu")
        df_fields_clean.writeTo(table_name).append()
    else:
        print(f"💾 Bảng {table_name} chưa tồn tại → tạo bảng mới")
        df_fields_clean.writeTo(table_name).create()

    print(f"✅ Hoàn tất xử lý file: {filename}")
    spark.stop()
