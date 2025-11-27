import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd
import os
from dotenv import load_dotenv

# GOOGLE SHEET
# Đường dẫn tới file JSON (đảm bảo tệp tồn tại)
gs = gspread.service_account(r'd:\OneDrive\KDA_Trinh Võ\KDA data\PYTHON_OPERATION\ma_shondo\mashondo.json')

# Mở Google Sheets bằng Google Sheets ID
sht = gs.open_by_key('1ULMcAbIDIh1VQZf66xotuvmTbrSmpXL3v5CqBe7s8ME')
SHEET1 = 'CATALOGUE'
worksheet_ctl = sht.worksheet(SHEET1)
data_ctl = worksheet_ctl.get_values('C8:C')
df_ctl = pd.DataFrame(data_ctl, columns=['default_code'])

# Thông tin kết nối MySQL
# Kết nối MySQL
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

CREATE_CATALOGUE_SQL = """
CREATE TABLE IF NOT EXISTS catalogue (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    default_code VARCHAR(64) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uq_default_code (default_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# Làm sạch dữ liệu từ Google Sheet
df_cat = (
    df_ctl.copy()
    .assign(default_code=lambda d: d['default_code'].astype(str).str.strip())
)
df_cat = df_cat[df_cat['default_code'].ne("") & df_cat['default_code'].ne("None")]
df_cat = df_cat.drop_duplicates(subset=['default_code']).reset_index(drop=True)

with engine.begin() as conn:
    # 1) Tạo bảng nếu chưa có
    conn.execute(text(CREATE_CATALOGUE_SQL))
    # 2) Xóa dữ liệu cũ
    conn.execute(text("TRUNCATE TABLE catalogue"))
    # 3) Insert dữ liệu mới
    # dùng executemany cho nhanh
    rows = df_cat.to_dict(orient="records")
    if rows:
        conn.execute(
            text("INSERT INTO catalogue (default_code) VALUES (:default_code)"),
            rows
        )

print(f"Catalogue refresh xong: {len(df_cat)} dòng.")