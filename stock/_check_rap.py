import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from sqlalchemy import text
import pandas as pd

engine = get_engine()

rap_codes = ['36RAP23','39RAP22W','39RAP21W','38RAP20','40RAP19','42RAP18','37RAP17',
             '42RAP16','39RAP15','39RAP14','37RAP13','39RAP12W','40RAP11','37RAP11',
             '38RAP10','39RAP9W','38RAP8','43RAP7','37RAP6','40RAP5','39RAP4W',
             '40RAP3','38RAP2','43RAP1']
rap_upper = [c.upper() for c in rap_codes]

# ── 1. Check trong CSV PM ─────────────────────────────────────
csv_path = Path(__file__).parent / 'subcategory từ pm.csv'
df_csv = pd.read_csv(csv_path, encoding='utf-8-sig')
df_csv.columns = df_csv.columns.str.strip()
code_col = 'Mã sản phẩm'
cat_col  = 'Danh mục'
parent_col = 'Id sản phẩm cha'
df_csv['code_upper'] = df_csv[code_col].astype(str).str.upper()
found_csv = df_csv[df_csv['code_upper'].isin(rap_upper)]
print(f"=== CSV PM: tim thay {len(found_csv)} / {len(rap_codes)} ma RAP ===")
if not found_csv.empty:
    print(found_csv[[code_col, parent_col, cat_col]].to_string(index=False))

# ── 2. Check trong products DB ────────────────────────────────
with engine.connect() as conn:
    df_db = pd.read_sql(text("""
        SELECT ps.product_id, ps.code AS default_code,
               COALESCE(ps2.code, ps.code) AS fdcode,
               ps.category_id,
               c1.name AS cat_direct, c2.name AS cat_parent
        FROM products ps
        LEFT JOIN products ps2 ON ps2.parent_id = ps.external_product_id
        LEFT JOIN categories c1 ON ps.category_id = c1.external_category_id
        LEFT JOIN categories c2 ON c1.parent_id = c2.category_id
        WHERE ps.parent_id IN (-2,-1)
    """), conn)

df_db['fdcode_upper'] = df_db['fdcode'].astype(str).str.upper()
found_db = df_db[df_db['fdcode_upper'].isin(rap_upper)]
print(f"\n=== DB products: tim thay {len(found_db)} / {len(rap_codes)} ma RAP ===")
if not found_db.empty:
    print(found_db[['fdcode','default_code','cat_direct','cat_parent']].to_string(index=False))
else:
    # Tim parent product RAP
    with engine.connect() as conn:
        df_rap_parent = pd.read_sql(text("""
            SELECT product_id, code, category_id, parent_id
            FROM products
            WHERE code LIKE '%RAP%'
        """), conn)
    print(f"Parent products co 'RAP' trong code: {len(df_rap_parent)}")
    print(df_rap_parent.to_string(index=False))
