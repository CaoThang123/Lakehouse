# load_survey.py
from config_spark import get_spark_session
from pyspark.sql.functions import col, when, trim, lit
from pyspark.sql.utils import AnalysisException

def process_survey_file(filename="survey.csv"):
    """
    Đọc file survey từ MinIO (bronze) → xử lý → ghi vào Iceberg (silver)
    Nếu bảng chưa tồn tại, tạo mới; nếu đã có, append dữ liệu.
    """
    spark = get_spark_session("Survey-Bronze-to-Silver")

    bronze_path = f"s3a://bronze/ecommerse/{filename}"
    print(f"📥 Đang đọc file: {bronze_path}")

    # 1️⃣ Đọc file CSV từ MinIO (Bronze)
    df_survey = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(bronze_path)

    print(f"📊 Số dòng ban đầu: {df_survey.count()}")

    # 2️⃣ Làm sạch dữ liệu
    if "Q-life-changes" in df_survey.columns:
        df_survey_clean = df_survey.withColumn(
            "Q-life-changes",
            when(
                (col("Q-life-changes").isNull()) | (trim(col("Q-life-changes")) == ""),
                lit("No")
            ).otherwise(col("Q-life-changes"))
        )
    else:
        df_survey_clean = df_survey

    print(f"🧹 Sau khi làm sạch: {df_survey_clean.count()} dòng còn lại")

    # 3️⃣ Ghi vào bảng Iceberg (Silver)
    table_name = "nessie.survey"
    
    try:
        spark.table(table_name)
        table_exists = True
        print(f"Bảng {table_name} đã tồn tại → append dữ liệu mới.")
    except AnalysisException:
        table_exists = False
        print(f"Bảng {table_name} chưa tồn tại → sẽ tạo mới.")

    if table_exists:
        print(f"💾 Bảng {table_name} đã tồn tại → append dữ liệu")
        df_survey_clean.writeTo(table_name).append()
    else:
        print(f"💾 Bảng {table_name} chưa tồn tại → tạo bảng mới")
        df_survey_clean.writeTo(table_name).create()

    print(f"✅ Hoàn tất xử lý file: {filename}")
    spark.stop()

if __name__ == "__main__":
    process_survey_file("survey.csv")