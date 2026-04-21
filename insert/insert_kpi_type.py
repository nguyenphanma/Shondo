import pandas as pd
from sqlalchemy import text
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine

engine = get_engine()
file_path = Path(os.getenv('ma_shondo_path')) / "table_adjust.xlsx"

df_kpi = pd.read_excel(file_path, sheet_name="kpi")
df_kpi['Date'] = pd.to_datetime(df_kpi['Date'], errors='coerce')

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

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS kpi"))
    conn.execute(text(create_kpi_table_sql))

df_kpi.to_sql('kpi', con=engine, if_exists='append', index=False)
print("✅ Đã tạo bảng và insert dữ liệu thành công.")
