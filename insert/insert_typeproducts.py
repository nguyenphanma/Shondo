import pandas as pd
from sqlalchemy import text
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine

engine = get_engine()

file_path = Path(os.getenv('MA_SHONDO_DIR')) / 'type_products.xlsx'
df = pd.read_excel(file_path)[['default_code', 'type']]
df['default_code'] = df['default_code'].astype(str).str.strip().str.upper()
df['type'] = df['type'].astype(str).str.strip()

with engine.connect() as conn:
    conn.execute(text("""
        CREATE TABLE IF NOT EXISTS type_products (
            default_code VARCHAR(50) PRIMARY KEY,
            type VARCHAR(100)
        ) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    """))
    conn.execute(text("DELETE FROM type_products;"))
    conn.commit()

df.to_sql("type_products", con=engine, if_exists="append", index=False)
print("Đã cập nhật dữ liệu vào bảng type_products thành công!")
