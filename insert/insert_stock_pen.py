import pandas as pd
import os
from datetime import datetime
from sqlalchemy import text
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine

year = '2026'
month = '04'

engine = get_engine()

folder_path = rf"G:\My Drive\MA\TRẢ ĐƠN ĐẶT HÀNG\{year}\THANG {month}"

if not os.path.exists(folder_path):
    raise FileNotFoundError(f"Đường dẫn không tồn tại: {folder_path}")

files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx') and os.path.isfile(os.path.join(folder_path, f))]
max_file, max_value = None, -1
for file in files:
    try:
        num_value = int(file[:8])
        if num_value > max_value:
            max_value, max_file = num_value, file
    except ValueError:
        continue

if not max_file:
    raise FileNotFoundError("Không tìm thấy file phù hợp trong thư mục tồn kho!")

print(f"File trả nợ đơn được chọn: {max_file}")
df = pd.read_excel(os.path.join(folder_path, max_file), sheet_name="DATA  ALL")
df.columns = df.iloc[0]
df = df[1:].reset_index(drop=True)

cols = ['KÊNH BÁN', 'DANH MỤC', 'DANH MỤC CON', 'MÃ SP CHA', 'SIZE', 'Mã hàng',
        'ĐƠN ĐẶT HÀNG THÁNG', 'NĂM', 'SL ĐẶT', 'TỔNG TRẢ', 'SL CÒN NỢ\n(XƯỞNG)',
        'SL TRẢ\nNĂM 2023-2025'] + [f'SL TRẢ T{str(i).zfill(2)}' for i in range(1, 13)]
df = df[cols]

numeric_cols = ['SL ĐẶT', 'TỔNG TRẢ', 'SL CÒN NỢ\n(XƯỞNG)', 'SL TRẢ\nNĂM 2023-2025'] + \
               [f'SL TRẢ T{str(i).zfill(2)}' for i in range(1, 13)]
df[numeric_cols] = df[numeric_cols].fillna(0).astype(int)
df[['MÃ SP CHA']] = df[['MÃ SP CHA']].apply(lambda x: x.str.upper())
df['KÊNH BÁN'].replace({'CỬA HÀNG': 'KDC', 'BÁN SỈ': 'KDS'}, inplace=True)
df['ĐƠN ĐẶT HÀNG THÁNG'].replace({'Nợ 2024': '12'}, inplace=True)
df["SIZE"] = df["SIZE"].str.replace("Size ", "", regex=False)
df["ĐƠN ĐẶT HÀNG THÁNG"] = df["ĐƠN ĐẶT HÀNG THÁNG"].astype(str).str.extract(r"(\d{1,2})")[0].str.zfill(2)

df.rename(columns={
    'SL CÒN NỢ\n(XƯỞNG)': 'order_pen', 'TỔNG TRẢ': 'qty_delivered_by_manu',
    'MÃ SP CHA': 'default_code', 'KÊNH BÁN': 'channel', 'DANH MỤC': 'category',
    'DANH MỤC CON': 'subcategory', 'SIZE': 'size', 'Mã hàng': 'fdcode',
    'ĐƠN ĐẶT HÀNG THÁNG': 'month_ord', 'NĂM': 'year_ord', 'SL ĐẶT': 'qty_ord',
    'SL TRẢ\nNĂM 2023-2025': 'delivered_old_year',
    **{f'SL TRẢ T{str(i).zfill(2)}': f'delivered_{i}' for i in range(1, 13)}
}, inplace=True)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stock_pen (
            channel VARCHAR(50), category VARCHAR(100), subcategory VARCHAR(100),
            default_code VARCHAR(50), size VARCHAR(20), fdcode VARCHAR(100),
            month_ord INT, year_ord INT, qty_ord INT, qty_delivered_by_manu INT,
            order_pen INT, delivered_old_year INT,
            delivered_1 INT, delivered_2 INT, delivered_3 INT, delivered_4 INT,
            delivered_5 INT, delivered_6 INT, delivered_7 INT, delivered_8 INT,
            delivered_9 INT, delivered_10 INT, delivered_11 INT, delivered_12 INT
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """))
    conn.execute(text("DELETE FROM stock_pen;"))
    conn.commit()

df_insert = df.applymap(lambda x: str(x).encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)
df_insert.to_sql("stock_pen", con=engine, if_exists="append", index=False)
