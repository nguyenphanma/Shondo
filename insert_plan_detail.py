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
sht = gs.open_by_key('1aFDuIMWZvW2dBIJsUpWgE4XUyIFfW4wFqq4Undhoyfw')
SHEET1 = 'UNPIVOT_TARGET'
worksheet_ctl = sht.worksheet(SHEET1)
data_plan = worksheet_ctl.get_all_values()
df_plan = pd.DataFrame(data_plan[1:], columns=data_plan[0])

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

# 1. Upper giá trị trong 2 cột
df_plan['sole material'] = df_plan['sole material'].astype(str).str.upper()
df_plan['type product'] = df_plan['type product'].astype(str).str.upper()

# 2. Đổi tên cột
df_plan = df_plan.rename(columns={
    'sole material': 'sole_material',
    'type product': 'type_products'
})

# 3. Chuẩn hóa metric
df_plan['metric'] = df_plan['metric'].replace({
    'SL kì vọng': 'qty_plan',
    'Dthu dự tính': 'rvn_plan'
})

# 5. (optional) thêm year để dễ join
df_plan['year'] = 2026

df_plan['date_plan'] = pd.to_datetime(
    df_plan['year'].astype(str) + '-' +
    df_plan['month'].astype(int).astype(str).str.zfill(2) + '-01'
)

df_plan_ft = df_plan[['channel', 'sole_material', 'type_products', 'category', 
                      'subcategory', 'metric', 'numbers', 'date_plan']]


df_plan_ft = df_plan_ft.copy()

# chuẩn hóa text
for c in ['channel', 'sole_material', 'type_products', 'category', 'subcategory', 'metric']:
    df_plan_ft[c] = df_plan_ft[c].astype(str).str.strip()

# numbers -> numeric (phòng trường hợp sheet có dấu phẩy, rỗng)
df_plan_ft['numbers'] = (
    df_plan_ft['numbers']
      .astype(str)
      .str.replace(',', '', regex=False)
      .replace({'': None, 'nan': None, 'None': None})
)
df_plan_ft['numbers'] = pd.to_numeric(df_plan_ft['numbers'], errors='coerce').fillna(0)

# date_plan đảm bảo dạng datetime (MySQL DATE/DATETIME)
df_plan_ft['date_plan'] = pd.to_datetime(df_plan_ft['date_plan']).dt.date


# =========================
# 1) TẠO BẢNG NẾU CHƯA CÓ
# =========================
table_name = "plan_target_unpivot"   # đổi tên nếu bạn muốn

create_table_sql = f"""
CREATE TABLE IF NOT EXISTS {table_name} (
    channel       VARCHAR(50)  NOT NULL,
    sole_material VARCHAR(80)  NULL,
    type_products VARCHAR(80)  NULL,
    category      VARCHAR(80)  NULL,
    subcategory   VARCHAR(80)  NULL,
    metric        VARCHAR(30)  NOT NULL,
    numbers       DECIMAL(18,2) NOT NULL DEFAULT 0,
    date_plan     DATE NOT NULL,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    KEY idx_date_plan (date_plan),
    KEY idx_main (channel, metric, date_plan),
    KEY idx_dim (category, subcategory, sole_material, type_products)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# =========================
# 2) XÓA CŨ THEO RANGE date_plan TRONG FILE (an toàn)
# =========================
date_min = df_plan_ft['date_plan'].min()
date_max = df_plan_ft['date_plan'].max()

delete_sql = text(f"""
DELETE FROM {table_name}
WHERE date_plan BETWEEN :date_min AND :date_max
""")

# =========================
# 3) INSERT df_plan_ft
# =========================
with engine.begin() as conn:  # transaction
    conn.execute(text(create_table_sql))
    conn.execute(delete_sql, {"date_min": date_min, "date_max": date_max})

# insert bằng to_sql (nhanh, ổn định)
df_plan_ft.to_sql(
    name=table_name,
    con=engine,
    if_exists='append',
    index=False,
    chunksize=20000,   # bạn hay dùng batch lớn, để 20k là hợp lý
    method='multi'
)

print(f"✅ Done: deleted [{date_min}..{date_max}] then inserted {len(df_plan_ft):,} rows into `{table_name}`")