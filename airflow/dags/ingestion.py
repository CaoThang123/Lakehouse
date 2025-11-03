from minio import Minio
import os

def import_data_to_minio(folder_path="/home/jovyan/notebooks/dataverse_files"):
    """
    Upload dữ liệu thô (CSV) từ local lên MinIO bucket 'bronze/ecommerse'.
    - Nếu file đã tồn tại, bỏ qua.
    - Nếu bucket chưa có, tự tạo mới.
    - Trả về danh sách file mới được upload.
    """

    # 1️⃣ Kết nối tới MinIO
    client = Minio(
        "minio:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False
    )

    bucket_name = "bronze"
    prefix = "ecommerse/"

    # 2️⃣ Kiểm tra bucket
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"✅ Bucket '{bucket_name}' created.")
    else:
        print(f"📦 Bucket '{bucket_name}' already exists.")

    # 3️⃣ Lấy danh sách file đã tồn tại trong MinIO
    existing_files = {
        obj.object_name.split("/")[-1]
        for obj in client.list_objects(bucket_name, prefix=prefix, recursive=True)
    }

    # 4️⃣ Duyệt file CSV trong local folder
    uploaded_files = []
    skipped_files = []

    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            object_name = f"{prefix}{filename}"
            file_path = os.path.join(folder_path, filename)

            if filename in existing_files:
                print(f"⚠️  {filename} đã tồn tại → bỏ qua.")
                skipped_files.append(filename)
            else:
                client.fput_object(bucket_name, object_name, file_path)
                print(f"✅ Uploaded {filename} → {bucket_name}/{object_name}")
                uploaded_files.append(filename)

    # 5️⃣ Báo cáo kết quả
    print("\n📊 Kết quả:")
    print(f"  ✔ Uploaded mới: {uploaded_files if uploaded_files else 'Không có'}")
    print(f"  ⏭️  Bỏ qua: {skipped_files if skipped_files else 'Không có'}")

    print(f"\n🎯 Có file mới? {len(uploaded_files) > 0}")
    return uploaded_files  # 🟢 trả về danh sách file mới

# Nếu chạy độc lập
if __name__ == "__main__":
    new_files = import_data_to_minio()
    if new_files:
        for f in new_files:
            if "purchase" in f.lower():
                print(f"➡️ Xử lý file PURCHASE: {f}")
                # gọi hàm xử lý purchase tại đây
            elif "survey" in f.lower():
                print(f"➡️ Xử lý file SURVEY: {f}")
                # gọi hàm xử lý survey tại đây
            else:
                print(f"❓ Không nhận dạng được loại file: {f}")
    else:
        print("⏹ Không có file mới → Dừng pipeline.")
