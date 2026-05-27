"""
So sánh CSV từ PM với products DB, sinh SQL UPDATE category_id cho parent products.
"""
import pandas as pd
import sys, io
from pathlib import Path
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from sqlalchemy import text

CSV_PATH = r"G:\My Drive\MA\PYTHON_OPERATION\ma_shondo\stock\subcategory từ pm.csv"

engine = get_engine()

# ── 1. Đọc CSV ───────────────────────────────────────────────────
df_csv = pd.read_csv(CSV_PATH, encoding='utf-8-sig')
df_csv.columns = df_csv.columns.str.strip()

col_parent = 'Id sản phẩm cha'
col_code   = 'Mã sản phẩm'
col_cat    = 'Danh mục'

df_csv[col_parent] = df_csv[col_parent].astype(str).str.strip()
df_csv[col_code]   = df_csv[col_code].astype(str).str.strip()
df_csv[col_cat]    = df_csv[col_cat].astype(str).str.strip()

# Chỉ lấy dòng con (có parent ID) và có danh mục
df_children = df_csv[
    (df_csv[col_parent] != '') & (df_csv[col_parent] != 'nan') &
    (df_csv[col_cat]    != '') & (df_csv[col_cat]    != 'nan')
][[col_code, col_cat]].copy()
df_children.columns = ['fdcode', 'subcat_pm']
df_children['fdcode'] = df_children['fdcode'].str.upper()
df_children = df_children.drop_duplicates('fdcode')
print(f"CSV children co danh muc: {len(df_children)}")
print(f"Subcategories duy nhat trong PM: {df_children['subcat_pm'].nunique()}")
print(df_children['subcat_pm'].value_counts().to_string())

# ── 2. Đọc DB: categories + parent products ───────────────────
with engine.connect() as conn:
    df_cats = pd.read_sql(text("""
        SELECT category_id, external_category_id, parent_id, name
        FROM categories
    """), conn)

    # Lấy parent products và fdcodes của chúng
    # JOIN qua external_product_id (không phải product_id)
    df_prods = pd.read_sql(text("""
        SELECT
            ps.product_id          AS parent_product_id,
            ps.code                AS default_code,
            ps.category_id         AS current_cat_id,
            COALESCE(ps2.code, ps.code) AS fdcode
        FROM products ps
        LEFT JOIN products ps2 ON ps2.parent_id = ps.external_product_id
        WHERE ps.parent_id IN (-2, -1)
          AND ps.product_id IS NOT NULL
    """), conn)

df_prods['fdcode'] = df_prods['fdcode'].str.strip().str.upper()
print(f"\nProducts DB: {len(df_prods)} fdcodes ({df_prods['parent_product_id'].nunique()} parent products)")

# ── 3. Lookup: category name ↔ external_category_id ──────────
name_to_ext = df_cats.set_index('name')['external_category_id'].to_dict()
ext_to_name = df_cats.set_index('external_category_id')['name'].to_dict()

# Danh sách tất cả category names trong DB để debug
all_cat_names = sorted(df_cats['name'].dropna().unique())

# ── 4. Merge PM × DB ─────────────────────────────────────────
merged = df_children.merge(
    df_prods[['fdcode', 'parent_product_id', 'default_code', 'current_cat_id']],
    on='fdcode', how='left'
)

merged['target_ext_id']   = merged['subcat_pm'].map(name_to_ext)
merged['current_cat_name'] = merged['current_cat_id'].map(ext_to_name)

# Cast về int64 để tránh float/int mismatch khi so sánh
merged['target_ext_id'] = pd.to_numeric(merged['target_ext_id'], errors='coerce')
merged['current_cat_id'] = pd.to_numeric(merged['current_cat_id'], errors='coerce')

# ── 5. Phân loại ──────────────────────────────────────────────
not_in_db    = merged[merged['parent_product_id'].isna()]
no_cat_in_db = merged[merged['parent_product_id'].notna() & merged['target_ext_id'].isna()]
correct      = merged[
    merged['parent_product_id'].notna() &
    merged['target_ext_id'].notna() &
    (merged['current_cat_id'] == merged['target_ext_id'])
]
needs_fix    = merged[
    merged['parent_product_id'].notna() &
    merged['target_ext_id'].notna() &
    (merged['current_cat_id'] != merged['target_ext_id'])
].copy()

print(f"\n=== Ket qua so sanh ===")
print(f"  Khop dung (category da dung)              : {len(correct)}")
print(f"  Can sua category (sai trong DB)            : {len(needs_fix)} fdcodes / {needs_fix['parent_product_id'].nunique()} parents")
print(f"  Fdcode trong PM nhung KHONG co trong DB   : {len(not_in_db)}")
print(f"  Subcategory PM chua co trong categories DB: {len(no_cat_in_db)} fdcodes")

# ── 6. Chi tiết needs_fix ─────────────────────────────────────
if not needs_fix.empty:
    print(f"\n--- Chi tiet {len(needs_fix)} fdcodes can sua ---")
    show = needs_fix[['fdcode', 'default_code', 'subcat_pm', 'current_cat_name',
                       'target_ext_id', 'current_cat_id']].copy()
    show.columns = ['fdcode', 'default_code', 'subcat_pm(dung)', 'cat_hien_tai', 'target_ext_id', 'current_ext_id']
    print(show.to_string(index=False))

# ── 7. Chi tiết subcategory PM chưa có trong categories DB ───
if not no_cat_in_db.empty:
    print(f"\n--- Subcategory trong PM CHUA CO trong categories DB ---")
    missing_cats = no_cat_in_db[['subcat_pm']].drop_duplicates()
    print(missing_cats.to_string(index=False))
    print("\n(Tat ca category names hien co trong DB:)")
    print([n for n in all_cat_names])

# ── 8. Chi tiết fdcodes không có trong DB ────────────────────
if not not_in_db.empty:
    print(f"\n--- Fdcodes trong PM KHONG co trong products DB ---")
    print(not_in_db[['fdcode', 'subcat_pm']].to_string(index=False))

# ── 9. Generate SQL UPDATE ────────────────────────────────────
print("\n\n========== SQL UPDATE products.category_id ==========")
if needs_fix.empty:
    print("-- Khong co gi can sua.")
else:
    for (parent_id, target_ext), grp in needs_fix.groupby(
            ['parent_product_id', 'target_ext_id'], sort=True):
        default_codes = grp['default_code'].dropna().unique()
        subcat        = grp['subcat_pm'].iloc[0]
        cur_name      = grp['current_cat_name'].iloc[0]
        print(f"-- {'/'.join(str(c) for c in default_codes)}: {cur_name} -> {subcat} (ext_id {int(target_ext)})")
        print(f"UPDATE products SET category_id = {int(target_ext)} WHERE product_id = {int(parent_id)};")
        print()
