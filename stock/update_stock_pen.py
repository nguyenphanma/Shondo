import pandas as pd
from datetime import date
from sqlalchemy import text
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from core.queries import get_product_template

# ──────────────────────────────────────────────────────────────────
# CẤU HÌNH
#   FROM_DATE : tính lại từ ngày nào (inclusive)
#   TO_DATE   : đến ngày nào (mặc định hôm nay)
#
#   Script RESET delivered cho các tháng trong khoảng rồi tính lại
#   từ movements — idempotent, phiếu sai làm lại chạy lại là đúng.
#
#   FIFO: qty sản xuất trả đơn cũ nhất trước, dư mới trả đơn tiếp theo.
# ──────────────────────────────────────────────────────────────────
FROM_DATE = '2026-05-01'
TO_DATE   = str(date.today())
# ──────────────────────────────────────────────────────────────────

engine = get_engine()

MOVEMENTS_SQL = """
SELECT
    DATE(im.date)      AS success_date,
    imi.product_code   AS fdcode,
    imi.quantity       AS qty,
    CASE
        WHEN im.description LIKE '%KDC%'      THEN 'KDC'
        WHEN im.description LIKE '%KHO TỔNG%' THEN 'KDC'
        WHEN im.description LIKE '%KHO SỈ%'   THEN 'KDS'
        WHEN im.description LIKE '%ECOM%'     THEN 'ECOM'
        ELSE im.description
    END AS channel
FROM inventory_movement_items imi
LEFT JOIN inventory_movements im ON im.id = imi.movement_id
WHERE im.depot_id = 101011
  AND im.mode = 3
  AND im.date >= :from_date
  AND im.date <= :to_date
  AND (
        im.description LIKE '%KDC%'
        OR im.description LIKE '%KHO TỔNG%'
        OR im.description LIKE '%KHO SỈ%'
        OR im.description LIKE '%ECOM%'
      )
"""

print(f"Query movements: {FROM_DATE} → {TO_DATE}")

with engine.connect() as conn:
    df_mv = pd.read_sql(
        text(MOVEMENTS_SQL), conn,
        params={'from_date': FROM_DATE, 'to_date': TO_DATE}
    )
    df_stock_full = pd.read_sql(
        text("SELECT * FROM stock_pen WHERE channel != 'NGOÀI ĐƠN' ORDER BY channel, fdcode, year_ord, month_ord"),
        conn
    )

df_prod = get_product_template(engine)[['fdcode', 'default_code', 'category', 'subcategory', 'size']].drop_duplicates('fdcode')

if df_mv.empty:
    print("Không có movements trong khoảng thời gian này.")
    sys.exit(0)

df_mv['success_date'] = pd.to_datetime(df_mv['success_date'])
df_mv['month'] = df_mv['success_date'].dt.month

covered_months = sorted(df_mv['month'].unique().tolist())
print(f"Tháng cần reset & tính lại : {covered_months}")

# Tổng qty theo fdcode + channel + tháng giao
df_agg = df_mv.groupby(['fdcode', 'channel', 'month'], as_index=False)['qty'].sum()

# Phân loại: có nợ đơn vs ngoài đơn
stock_keys = set(zip(df_stock_full['fdcode'], df_stock_full['channel']))
mask = df_agg.apply(lambda r: (r['fdcode'], r['channel']) in stock_keys, axis=1)

df_matched = df_agg[mask].copy()
df_ngoai   = (
    df_agg[~mask]
    .groupby(['fdcode', 'month'], as_index=False)['qty']
    .sum()
    .assign(channel='NGOÀI ĐƠN')
)

print(f"Movements có nợ đơn : {len(df_matched)} nhóm")
print(f"Movements ngoài đơn : {len(df_ngoai)} nhóm")

# ── FIFO allocation ────────────────────────────────────────────────
# Tính "nợ còn lại trước khi reset" cho mỗi dòng:
# = qty_ord - delivered_old_year - tổng delivered các tháng KHÔNG bị reset
fixed_del_cols = ['delivered_old_year'] + [
    f'delivered_{m}' for m in range(1, 13) if m not in covered_months
]

df_stock_full['_debt_before_reset'] = (
    df_stock_full['qty_ord'] - df_stock_full[fixed_del_cols].sum(axis=1)
).clip(lower=0)

# allocations[(row_id, delivery_month)] = qty_to_add
allocations: dict = {}
remaining_debt_final: dict = {}  # row_id → nợ còn lại sau pass 1
channel_surplus: list = []       # dư sau khi fill đủ đơn cùng kênh

# ── Pass 1: FIFO cùng kênh ────────────────────────────────────────
for (fdc, ch), grp_mv in df_matched.groupby(['fdcode', 'channel']):
    orders = df_stock_full[
        (df_stock_full['fdcode'] == fdc) & (df_stock_full['channel'] == ch)
    ].copy().sort_values(['year_ord', 'month_ord'])

    if orders.empty:
        continue

    remaining_debt = orders['_debt_before_reset'].tolist()
    row_id_list    = orders['id'].tolist()

    for _, mv_row in grp_mv.iterrows():
        delivery_month = int(mv_row['month'])
        remaining_qty  = int(mv_row['qty'])

        for i, rid in enumerate(row_id_list):
            if remaining_qty <= 0:
                break
            debt = remaining_debt[i]
            if debt <= 0:
                continue
            to_alloc = min(remaining_qty, debt)
            key = (rid, delivery_month)
            allocations[key] = allocations.get(key, 0) + to_alloc
            remaining_debt[i] -= to_alloc
            remaining_qty     -= to_alloc

        if remaining_qty > 0:
            channel_surplus.append({'fdcode': fdc, 'month': delivery_month, 'qty': remaining_qty})

    for rid, debt in zip(row_id_list, remaining_debt):
        remaining_debt_final[rid] = debt

# Khởi tạo nợ còn lại cho các dòng chưa có movement (vd: ECOM chưa được xuất kho)
for _, row in df_stock_full.iterrows():
    if row['id'] not in remaining_debt_final:
        remaining_debt_final[int(row['id'])] = row['_debt_before_reset']

# ── Pass 2: Cross-channel — fill đơn kênh khác trước khi ghi NGOÀI ĐƠN ───
# Pool = dư từ pass 1 + movements không khớp kênh nào trong stock_pen
cross_pool: list = channel_surplus.copy()
for _, row in df_ngoai.iterrows():
    cross_pool.append({'fdcode': row['fdcode'], 'month': int(row['month']), 'qty': int(row['qty'])})

fdcode_rows = (
    df_stock_full.sort_values(['year_ord', 'month_ord'])
    .groupby('fdcode')['id'].apply(list).to_dict()
)

true_ngoai: list = []
if cross_pool:
    pool_df = (
        pd.DataFrame(cross_pool)
        .groupby(['fdcode', 'month'], as_index=False)['qty'].sum()
    )
    for _, s_row in pool_df.iterrows():
        fdc            = s_row['fdcode']
        delivery_month = int(s_row['month'])
        pool_qty       = int(s_row['qty'])

        for rid in fdcode_rows.get(fdc, []):
            if pool_qty <= 0:
                break
            debt = remaining_debt_final.get(rid, 0)
            if debt <= 0:
                continue
            to_alloc = min(pool_qty, debt)
            key = (rid, delivery_month)
            allocations[key] = allocations.get(key, 0) + to_alloc
            remaining_debt_final[rid] -= to_alloc
            pool_qty -= to_alloc

        if pool_qty > 0:
            true_ngoai.append({'fdcode': fdc, 'month': delivery_month, 'qty': pool_qty})

df_ngoai = (
    pd.DataFrame(true_ngoai, columns=['fdcode', 'month', 'qty'])
    if true_ngoai else pd.DataFrame(columns=['fdcode', 'month', 'qty'])
)

cross_allocated = sum(r['qty'] for r in cross_pool) - (df_ngoai['qty'].sum() if not df_ngoai.empty else 0)
print(f"Số phân bổ FIFO pass 1 : {len(allocations)} (row_id × delivery_month)")
print(f"Cross-channel allocated: {int(cross_allocated)} units")
print(f"Số dư → NGOÀI ĐƠN      : {int(df_ngoai['qty'].sum()) if not df_ngoai.empty else 0} units")

# ── Ghi vào DB ────────────────────────────────────────────────────
delivered_sum_sql = " + ".join(
    ["delivered_old_year"] + [f"delivered_{m}" for m in range(1, 13)]
)

with engine.connect() as conn:

    # BƯỚC 1: Reset delivered cho các tháng được cập nhật
    for m in covered_months:
        col = f"delivered_{m}"
        conn.execute(text(f"UPDATE stock_pen SET {col} = 0 WHERE channel != 'NGOÀI ĐƠN'"))
    conn.execute(text("DELETE FROM stock_pen WHERE channel = 'NGOÀI ĐƠN'"))
    conn.commit()
    print(f"Đã reset delivered_{covered_months} và xóa NGOÀI ĐƠN cũ")

    # BƯỚC 2: Áp dụng phân bổ FIFO — UPDATE theo id để tránh hit nhiều dòng cùng month_ord
    updated_rows = 0
    for (row_id, delivery_month), qty in allocations.items():
        col = f"delivered_{delivery_month}"
        result = conn.execute(
            text(f"UPDATE stock_pen SET {col} = {col} + :qty WHERE id = :id"),
            {'qty': qty, 'id': row_id}
        )
        updated_rows += result.rowcount

    # BƯỚC 3: Tạo lại dòng NGOÀI ĐƠN (merge category/subcategory/default_code/size từ product_template)
    prod_lookup = df_prod.set_index('fdcode').to_dict('index')
    ngoai_inserted = 0
    for fdc, grp in df_ngoai.groupby('fdcode'):
        all_cols = {f"delivered_{m}": 0 for m in range(1, 13)}
        for _, r in grp.iterrows():
            all_cols[f"delivered_{int(r['month'])}"] = int(r['qty'])

        meta = prod_lookup.get(fdc, {})
        category     = meta.get('category')     or None
        subcategory  = meta.get('subcategory')  or None
        default_code = meta.get('default_code') or None
        size         = meta.get('size')         or None

        cols_sql = ", ".join(all_cols.keys())
        vals_sql = ", ".join([f":{c}" for c in all_cols.keys()])
        conn.execute(
            text(f"""
                INSERT INTO stock_pen
                    (channel, fdcode, category, subcategory, default_code, size,
                     month_ord, year_ord, qty_ord,
                     qty_delivered_by_manu, order_pen, delivered_old_year,
                     {cols_sql})
                VALUES
                    ('NGOÀI ĐƠN', :fdcode, :category, :subcategory, :default_code, :size,
                     0, 2026, 0, 0, 0, 0, {vals_sql})
            """),
            {**all_cols, 'fdcode': fdc,
             'category': category, 'subcategory': subcategory,
             'default_code': default_code, 'size': size}
        )
        ngoai_inserted += 1

    # BƯỚC 4: Tính lại qty_delivered_by_manu và order_pen
    conn.execute(text(f"""
        UPDATE stock_pen
        SET
            qty_delivered_by_manu = ({delivered_sum_sql}),
            order_pen = GREATEST(0, qty_ord - ({delivered_sum_sql}))
        WHERE channel != 'NGOÀI ĐƠN'
    """))

    conn.commit()

print(f"\nKết quả:")
print(f"  Áp dụng FIFO (dòng DB updated) : {updated_rows}")
print(f"  Thêm mới NGOÀI ĐƠN             : {ngoai_inserted} fdcode")
print(f"  qty_delivered_by_manu & order_pen đã được tính lại")
print(f"\nHoàn thành. Chạy lại với cùng FROM_DATE → kết quả luôn đúng.")
