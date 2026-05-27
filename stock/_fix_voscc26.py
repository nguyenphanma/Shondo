import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from sqlalchemy import text
import pandas as pd

engine = get_engine()
with engine.connect() as conn:
    socks = pd.read_sql(text("SELECT external_category_id FROM categories WHERE UPPER(name)='SOCKS'"), conn)
    socks_ext_id = int(socks.iloc[0, 0])
    print(f"SOCKS external_category_id: {socks_ext_id}")

    prod = pd.read_sql(text("SELECT product_id, code, category_id FROM products WHERE code='VOSCC26' AND parent_id IN (-2,-1)"), conn)
    print(prod.to_string(index=False))

    r = conn.execute(text(f"UPDATE products SET category_id = {socks_ext_id} WHERE code = 'VOSCC26' AND parent_id IN (-2,-1)"))
    conn.commit()
    print(f"Updated: {r.rowcount} rows -> SOCKS ({socks_ext_id})")
