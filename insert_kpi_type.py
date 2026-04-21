import pandas as pd
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
from pathlib import Path
load_dotenv()

# 🔗 Kết nối MySQL – tạo duy nhất 1 engine dùng xuyên suốt
# Lấy thông tin từ biến môi trường
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = os.getenv("DB_PORT", 3306)

connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"
engine = create_engine(connection_string)

file_path = Path(os.getenv('ma_shondo_path')) /"table_adjust.xlsx"

# Đọc dữ liệu từ file Excel
df_kpi = pd.read_excel(file_path, sheet_name="kpi")
df_kpi['Date'] = pd.to_datetime(df_kpi['Date'], errors='coerce')

# Câu lệnh tạo bảng kpi với charset utf8mb4
create_kpi_table_sql = """
CREATE TABLE IF NOT EXISTS kpi (
    channel VARCHAR(20),
    store VARCHAR(100),
    kpi_revenue BIGINT,
    month INT,
    year INT,
    Date DATE
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci
"""
# Tạo bảng và chèn dữ liệu
with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS kpi"))
    conn.execute(text(create_kpi_table_sql))

# Insert dữ liệu
df_kpi.to_sql('kpi', con=engine, if_exists='append', index=False)
print("✅ Đã tạo bảng và insert dữ liệu thành công.")