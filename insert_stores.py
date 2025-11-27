from sqlalchemy import create_engine, text
from datetime import datetime

# Kết nối MySQL
import os
from dotenv import load_dotenv

load_dotenv()

# 🔗 Kết nối MySQL – tạo duy nhất 1 engine dùng xuyên suốt
# Lấy thông tin từ biến môi trường
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = os.getenv("DB_PORT", 3306)

# Kết nối MySQL
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

# Thêm pool_pre_ping=True và connect_args để tăng thời gian chờ
engine = create_engine(
    connection_string,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 30}  # tăng timeout từ mặc định (~10s) lên 20s
)
# Tạo engine kết nối đến MySQL
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}")

# Dữ liệu cần insert
store_data = {
    "217633": {
        "id": 217633,
        "code": "KHOSICHECK",
        "name": "KHOSICHECK",
        "mobile": "89898989898",
        "address": "Kho 40"
    },
    "217642": {
        "id": 217642,
        "code": "ECOM HN",
        "name": "ECOM HN",
        "mobile": "89898989899",
        "address": "Kho 41"
    }
}

# Hàm insert dữ liệu vào bảng stores
def insert_stores(data):
    try:
        with engine.connect() as conn:
            for store_id, info in data.items():
                query = text("""
                    INSERT INTO stores (store_id, name, address, phone, code_nhanh, created_at, updated_at)
                    VALUES (:store_id, :name, :address, :phone, :code_nhanh, :created_at, :updated_at)
                    ON DUPLICATE KEY UPDATE 
                        name = VALUES(name),
                        address = VALUES(address),
                        phone = VALUES(phone),
                        code_nhanh = VALUES(code_nhanh),
                        updated_at = VALUES(updated_at)
                """)
                conn.execute(query, {
                    "store_id": info["id"],
                    "name": info["name"],
                    "address": info["address"],
                    "phone": info["mobile"],
                    "code_nhanh": info["code"],
                    "created_at": datetime.now(),
                    "updated_at": datetime.now()
                })
            conn.commit()
            print("✅ Đã chèn dữ liệu thành công!")
    except Exception as e:
        print(f"❌ Lỗi khi chèn dữ liệu: {e}")

# Gọi hàm
insert_stores(store_data)