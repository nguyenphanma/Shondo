import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd
from datetime import datetime
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

df_store_visits = pd.read_excel(Path(os.getenv('MA_SHONDO_DIR')) / 'store_visits.xlsx')
df_store_visits = df_store_visits[~df_store_visits['Cửa hàng'].isna()]
df_store_visits_filter = df_store_visits.drop(['Chỉ tiêu', 'Tổng'], axis=1)
df_long = df_store_visits_filter.melt(id_vars=['Cửa hàng'], var_name='period', value_name='visits')

# Lấy ngày hôm nay
today = datetime.today()

# Tính tháng trước
last_month = (today.replace(day=1) - timedelta(days=1)).month
current_year = today.year

# Bỏ cột period nếu có
if 'period' in df_long.columns:
    df_long.drop(columns=['period'], inplace=True)

# Đổi tên cột nếu cần
df_long.rename(columns={'Cửa hàng': 'store'}, inplace=True)

# Thêm cột month và year
df_long['month'] = last_month
df_long['year'] = current_year

# Gộp theo store, year, month và tính tổng visits
df_long['visits'] = pd.to_numeric(df_long['visits'])
df_grouped = df_long.groupby(['store', 'year', 'month']).agg({
    'visits':'sum'
}).reset_index()
# Sắp xếp lại kết quả
df_grouped = df_grouped.sort_values(['store', 'year', 'month']).reset_index(drop=True)

# Tạo engine MySQL
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)

query_store_sql = """
SELECT st.store_id,
       st.code_nhanh store
FROM stores st
"""
with engine.connect() as conn:
    df_store = pd.read_sql_query(text(query_store_sql), conn)

# Tạo bảng nếu chưa tồn tại
create_table_sql = """
CREATE TABLE IF NOT EXISTS store_visits (
    id INT AUTO_INCREMENT PRIMARY KEY,
    store VARCHAR(100),
    visits INT,
    year VARCHAR(4),
    month VARCHAR(2),
    store_id INT
) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
"""

with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    conn.commit()

# Đảm bảo kiểu dữ liệu phù hợp (tránh lỗi khi insert)
df_grouped = pd.merge(df_grouped, df_store[['store', 'store_id']], on='store', how='left')
df_grouped['visits'] = df_grouped['visits'].fillna(0).astype(int)

# Loại bỏ các dòng không có store_id
df_grouped = df_grouped[df_grouped['store_id'].notna()]

# Ghi dữ liệu vào bảng store_visits
df_grouped.to_sql("store_visits", con=engine, if_exists="append", index=False)
print("✅ Dữ liệu đã được insert vào bảng store_visits.")