import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from sqlalchemy import text
import pandas as pd

engine = get_engine()

codes = [
    'BALBC12','BALBC24','BALSH13','VOSCC17','NONBC97','BALBC11','VOSCC19','BALBC09',
    'M7E4Z1010','NONBC02','NONBC03','NONBC00','VOSCC16','VOSCN00','TUIX13','TUIX70',
    'NONBC96','NONBC95','TUITE12','BALBC25','MXE4Z1010','VOSCC08','VOSCC18','QT2601',
    'TUIX14','VOSCL70','CHMFX00','VOSCC09','QTTUIBN10','TUITE60','VOSCC14','VOSCC12',
    'TUITE00','VOSCC06','CHMCI60','VOSCC15','VOSCC07','CHM2601','VOSCC20','VOSCC22',
    'VOSCL00','VOSCL07','HOPLIMAXNAM','HOPGIAYM55','HOPLIMAXNU','HOPGIAYL54','HOPGIAYL55',
    'NONBC12','NONSH21','TUIX60','HOPGIAYS54','M8E4Z1010','NONBC11','NONBC30','QHKHAC',
    'TUITE01','TUITE11','VOSCC03','VOSCL25'
]
codes_upper = [c.upper() for c in codes]
placeholders = ','.join([f"'{c}'" for c in codes_upper])

# Check trong DB
with engine.connect() as conn:
    df_db = pd.read_sql(text(f"""
        SELECT ps.product_id, ps.code AS default_code,
               COALESCE(ps2.code, ps.code) AS fdcode,
               ps.category_id,
               c1.name AS cat_direct,
               c2.name AS cat_parent,
               COALESCE(c2.name, c1.name) AS category,
               COALESCE(c1.name, c2.name) AS subcategory
        FROM products ps
        LEFT JOIN products ps2 ON ps2.parent_id = ps.external_product_id
        LEFT JOIN categories c1 ON ps.category_id = c1.external_category_id
        LEFT JOIN categories c2 ON c1.parent_id = c2.category_id
        WHERE ps.parent_id IN (-2,-1)
          AND ps.code IN ({placeholders})
    """), conn)

# Check trong CSV PM
csv_path = Path(__file__).parent / 'subcategory từ pm.csv'
df_csv = pd.read_csv(csv_path, encoding='utf-8-sig')
df_csv.columns = df_csv.columns.str.strip()
# Lấy parent rows (không có Id cha) → có Danh mục ở cấp cha
df_csv['code_upper'] = df_csv['Mã sản phẩm'].astype(str).str.upper()
df_csv['Danh mục'] = df_csv['Danh mục'].astype(str).str.strip()
df_csv_parents = df_csv[
    (df_csv['Id sản phẩm cha'].astype(str).str.strip().isin(['', 'nan'])) &
    (~df_csv['Danh mục'].isin(['', 'nan']))
][['Mã sản phẩm','Danh mục','code_upper']].drop_duplicates('code_upper')

# Merge DB result với CSV
df_db['code_upper'] = df_db['default_code'].str.upper()
merged = df_db.drop_duplicates('default_code').merge(
    df_csv_parents[['code_upper','Danh mục']].rename(columns={'Danh mục': 'subcat_pm'}),
    on='code_upper', how='left'
)

print(f"Tim thay trong DB: {len(merged)} / {len(codes)} ma")
print(f"Khong co trong DB: {set(codes_upper) - set(df_db['default_code'].str.upper())}")
print()
print(f"{'default_code':<16} {'category':<12} {'subcategory':<16} {'subcat_pm(CSV)'}")
print("-"*70)
for _, r in merged.sort_values('default_code').iterrows():
    cat   = str(r['category'])   if pd.notna(r['category'])   else 'NULL'
    subcat = str(r['subcategory']) if pd.notna(r['subcategory']) else 'NULL'
    pm    = str(r['subcat_pm'])  if pd.notna(r['subcat_pm'])  else '(khong co trong CSV)'
    flag  = ' <<< SAI' if pm != '(khong co trong CSV)' and subcat.strip().upper() != pm.strip().upper() else ''
    print(f"{r['default_code']:<16} {cat:<12} {subcat:<16} {pm}{flag}")
