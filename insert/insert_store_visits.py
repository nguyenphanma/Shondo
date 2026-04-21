import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine

engine = get_engine()

df = pd.read_excel(Path(os.getenv('MA_SHONDO_DIR')) / 'store_visits.xlsx')
df = df[~df['Cửa hàng'].isna()]
df = df.drop(['Chỉ tiêu', 'Tổng'], axis=1)
df_long = df.melt(id_vars=['Cửa hàng'], var_name='period', value_name='visits')
df_long.drop(columns=['period'], inplace=True)
df_long.rename(columns={'Cửa hàng': 'store'}, inplace=True)

today = datetime.today()
last_month = (today.replace(day=1) - timedelta(days=1)).month
df_long['month'] = last_month
df_long['year'] = today.year
df_long['visits'] = pd.to_numeric(df_long['visits'])

df_grouped = (
    df_long.groupby(['store', 'year', 'month'])
    .agg({'visits': 'sum'})
    .reset_index()
    .sort_values(['store', 'year', 'month'])
    .reset_index(drop=True)
)

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS store_visits (
            id INT AUTO_INCREMENT PRIMARY KEY,
            store VARCHAR(100), visits INT,
            year VARCHAR(4), month VARCHAR(2), store_id INT
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """))
    conn.commit()
    df_store = pd.read_sql_query(text("SELECT store_id, code_nhanh store FROM stores"), conn)

df_grouped = pd.merge(df_grouped, df_store[['store', 'store_id']], on='store', how='left')
df_grouped['visits'] = df_grouped['visits'].fillna(0).astype(int)
df_grouped = df_grouped[df_grouped['store_id'].notna()]
df_grouped.to_sql("store_visits", con=engine, if_exists="append", index=False)
print("✅ Dữ liệu đã được insert vào bảng store_visits.")
