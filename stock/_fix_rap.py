import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from sqlalchemy import text
import pandas as pd

engine = get_engine()

# Lấy external_category_id của SANDALS
with engine.connect() as conn:
    df_sandals = pd.read_sql(text("""
        SELECT category_id, external_category_id, name, parent_id
        FROM categories
        WHERE UPPER(name) = 'SANDALS'
    """), conn)
print("SANDALS categories:")
print(df_sandals.to_string(index=False))

# Lấy product_ids của 24 mã RAP
rap_codes = ['36RAP23','39RAP22W','39RAP21W','38RAP20','40RAP19','42RAP18','37RAP17',
             '42RAP16','39RAP15','39RAP14','37RAP13','39RAP12W','40RAP11','37RAP11',
             '38RAP10','39RAP9W','38RAP8','43RAP7','37RAP6','40RAP5','39RAP4W',
             '40RAP3','38RAP2','43RAP1']
placeholders = ','.join([f"'{c}'" for c in rap_codes])

with engine.connect() as conn:
    df_rap = pd.read_sql(text(f"""
        SELECT product_id, code, category_id
        FROM products
        WHERE code IN ({placeholders})
        AND parent_id IN (-2, -1)
    """), conn)
print(f"\nRAP products: {len(df_rap)} rows")
print(df_rap.to_string(index=False))
