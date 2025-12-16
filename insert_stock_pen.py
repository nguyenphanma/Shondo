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

year ='2025'
month ='12'

load_dotenv()

# 🔗 Kết nối MySQL – tạo duy nhất 1 engine dùng xuyên suốt
# Lấy thông tin từ biến môi trường
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = os.getenv("DB_PORT", 3306)

# Tạo engine MySQL
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)


current_drive = os.path.splitdrive(os.getcwd())[0].upper()
raw_path = rf"OneDrive\KDA_Trinh Võ\KDA data\THEO DÕI - ĐỀ XUẤT\TRẢ ĐƠN ĐẶT HÀNG\{year}\THANG {month}"
folder_path = os.path.normpath(f"{current_drive}/{raw_path}")
files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]

# Kiểm tra tồn tại thư mục
if not os.path.exists(folder_path):
    raise FileNotFoundError(f"Đường dẫn không tồn tại: {folder_path}")

# Lọc và lấy file có 6 số đầu lớn nhất
files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx') and os.path.isfile(os.path.join(folder_path, f))]

max_file = None
max_value = -1
for file in files:
    try:
        num_part = file[:8]
        num_value = int(num_part)
        if num_value > max_value:
            max_value = num_value
            max_file = file
    except ValueError:
        continue

# Đọc file tồn kho lớn nhất
if max_file:
    file_path = os.path.join(folder_path, max_file)
    print(f"File trả nợ đơn được chọn: {max_file}")
    df_pending_stock = pd.read_excel(file_path, sheet_name="DATA  ALL")
else:
    raise FileNotFoundError("Không tìm thấy file phù hợp trong thư mục tồn kho!")

# Lấy dòng thứ 3 làm tiêu đề mới
df_pending_stock.columns = df_pending_stock.iloc[0]

# Bỏ các dòng tiêu đề cũ
df_pending_stock_fix = df_pending_stock[1:].reset_index(drop=True)


df_pending_stock_fix = df_pending_stock_fix[['KÊNH BÁN', 'DANH MỤC', 'DANH MỤC CON', 'MÃ SP CHA', 'SIZE', 'Mã hàng', 'ĐƠN ĐẶT HÀNG THÁNG', 'NĂM','SL ĐẶT', 'TỔNG TRẢ','SL CÒN NỢ\n(XƯỞNG)',
                                             'SL TRẢ\nNĂM 2023-2024',
                                             'SL TRẢ T01',
                                             'SL TRẢ T02',
                                             'SL TRẢ T03',
                                             'SL TRẢ T04',
                                             'SL TRẢ T05',
                                             'SL TRẢ T06',
                                             'SL TRẢ T07',
                                             'SL TRẢ T08',
                                             'SL TRẢ T09',
                                             'SL TRẢ T10',
                                             'SL TRẢ T11',
                                             'SL TRẢ T12'
                                             ]]
cols_to_convert = ['SL ĐẶT', 'TỔNG TRẢ', 'SL CÒN NỢ\n(XƯỞNG)', 'SL TRẢ\nNĂM 2023-2024',
                                             'SL TRẢ T01',
                                             'SL TRẢ T02',
                                             'SL TRẢ T03',
                                             'SL TRẢ T04',
                                             'SL TRẢ T05',
                                             'SL TRẢ T06',
                                             'SL TRẢ T07',
                                             'SL TRẢ T08',
                                             'SL TRẢ T09',
                                             'SL TRẢ T10',
                                             'SL TRẢ T11',
                                             'SL TRẢ T12']


df_pending_stock_fix[cols_to_convert] = df_pending_stock_fix[cols_to_convert].fillna(0).astype(int)
df_pending_stock_fix[['MÃ SP CHA']] = df_pending_stock_fix[['MÃ SP CHA']].apply(lambda x: x.str.upper())
df_pending_stock_fix['KÊNH BÁN'].replace({'CỬA HÀNG': 'KDC', 'BÁN SỈ': 'KDS'}, inplace=True)
df_pending_stock_fix['ĐƠN ĐẶT HÀNG THÁNG'].replace({'T':""}, inplace=True)
df_pending_stock_fix['ĐƠN ĐẶT HÀNG THÁNG'].replace({'Nợ 2024':"12"}, inplace=True)
df_pending_stock_fix.rename(columns={'SL CÒN NỢ\n(XƯỞNG)': 'order_pen',
                                        'TỔNG TRẢ': 'qty_delivered_by_manu',
                                        'MÃ SP CHA': 'default_code', 
                                        'KÊNH BÁN': 'channel', 
                                        'DANH MỤC': 'category',
                                        'DANH MỤC CON':'subcategory',
                                        'SIZE':'size',
                                        'Mã hàng':'fdcode',
                                        'ĐƠN ĐẶT HÀNG THÁNG':'month_ord',
                                        'NĂM':'year_ord',
                                        'SL ĐẶT': 'qty_ord',
                                        'SL TRẢ\nNĂM 2023-2024':'delivered_old_year',
                                        'SL TRẢ T01':'delivered_1',
                                        'SL TRẢ T02':'delivered_2',
                                        'SL TRẢ T03':'delivered_3',
                                        'SL TRẢ T04':'delivered_4',
                                        'SL TRẢ T05':'delivered_5',
                                        'SL TRẢ T06':'delivered_6',
                                        'SL TRẢ T07':'delivered_7',
                                        'SL TRẢ T08':'delivered_8',
                                        'SL TRẢ T09':'delivered_9',
                                        'SL TRẢ T10':'delivered_10',
                                        'SL TRẢ T11':'delivered_11',
                                        'SL TRẢ T12':'delivered_12'
                                        }, inplace=True)
# Replace "Size " trong cột size
df_pending_stock_fix["size"] = df_pending_stock_fix["size"].str.replace("Size ", "", regex=False)

# Replace "T" trong cột month_ord
df_pending_stock_fix["month_ord"] = df_pending_stock_fix["month_ord"].astype(str).str.extract(r"(\d{1,2})")[0].str.zfill(2)

with engine.connect() as conn:
    # Tạo bảng nếu chưa tồn tại
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS stock_pen (
            channel VARCHAR(50),
            category VARCHAR(100),
            subcategory VARCHAR(100),
            default_code VARCHAR(50),
            size VARCHAR(20),
            fdcode VARCHAR(100),
            month_ord INT,
            year_ord INT,
            qty_ord INT,
            qty_delivered_by_manu INT,
            order_pen INT,
            delivered_old_year INT,
            delivered_1 INT,
            delivered_2 INT,
            delivered_3 INT,
            delivered_4 INT,
            delivered_5 INT,
            delivered_6 INT,
            delivered_7 INT,
            delivered_8 INT,
            delivered_9 INT,
            delivered_10 INT,
            delivered_11 INT,
            delivered_12 INT                          
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """))

    # Xóa toàn bộ dữ liệu cũ
    conn.execute(text("DELETE FROM stock_pen;"))

    conn.commit()

# ====================================
df_insert = df_pending_stock_fix.applymap(lambda x: str(x).encode('utf-8', 'ignore').decode('utf-8') if isinstance(x, str) else x)

df_insert.to_sql("stock_pen", con=engine, if_exists="append", index=False)