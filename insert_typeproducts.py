import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path

# ============================
# 1. Load biến môi trường
# ============================
load_dotenv()

host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = os.getenv("DB_PORT", 3306)

# Tạo engine MySQL
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)

# ============================
# 2. Đọc file Excel type_products
# ============================
file_path = Path(os.getenv('MA_SHONDO_DIR')) / 'type_products.xlsx'

# Đọc file Excel
df_type_products = pd.read_excel(file_path)

# Đảm bảo chỉ giữ 2 cột cần thiết
df_type_products = df_type_products[['default_code', 'type']]

# Chuẩn hóa dữ liệu (xóa khoảng trắng, uppercase code)
df_type_products['default_code'] = df_type_products['default_code'].astype(str).str.strip().str.upper()
df_type_products['type'] = df_type_products['type'].astype(str).str.strip()

# ============================
# 3. Tạo bảng nếu chưa có
# ============================
with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS type_products (
            default_code VARCHAR(50) PRIMARY KEY,
            type VARCHAR(100)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """))

    # ============================
    # 4. Xóa dữ liệu cũ trong bảng
    # ============================
    conn.execute(text("DELETE FROM type_products;"))
    conn.commit()

# ============================
# 5. Insert dữ liệu mới
# ============================
df_type_products.to_sql("type_products", con=engine, if_exists="append", index=False)
print("Đã cập nhật dữ liệu vào bảng type_products thành công!")