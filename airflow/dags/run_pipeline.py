from ingestion import import_data_to_minio
from load_purchase import process_purchase_file
from load_survey import process_survey_file
from load_field import process_fields_file
from gold import build_gold_layer

if __name__ == "__main__":
    # 1️⃣ Upload CSV mới lên MinIO
    new_files = import_data_to_minio()

    if not new_files:
        print("⏹ Không có file mới → Dừng pipeline.")
    else:
        for f in new_files:
            fname_lower = f.lower()
            try:
                if "purchase" in fname_lower:
                    print(f"➡️ Xử lý PURCHASE: {f}")
                    process_purchase_file(f)
                elif "survey" in fname_lower:
                    print(f"➡️ Xử lý SURVEY: {f}")
                    process_survey_file(f)
                elif "field" in fname_lower:
                    print(f"➡️ Xử lý FIELDS: {f}")
                    process_fields_file(f)
                else:
                    print(f"❓ Không nhận dạng được loại file: {f}")
            except Exception as e:
                print(f"❌ Lỗi khi xử lý file {f}: {e}")

        # 2️⃣ Xây dựng Gold Layer
        print("\n🎯 Bắt đầu build Gold Layer...")
        build_gold_layer()
        print("🎉 Hoàn tất pipeline.")
