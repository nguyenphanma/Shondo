"""
Dự báo doanh thu theo channel × subcategory bằng Holt-Winters (ETS),
sau đó so sánh với mục tiêu (UNPIVOT_TARGET) và phân bổ xuống default_code.

Yêu cầu: pip install statsmodels
"""
import warnings
import pandas as pd
import numpy as np
from datetime import date
import gspread_dataframe as gd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from statsmodels.tsa.holtwinters import ExponentialSmoothing
from core.queries import get_product_template
from core.db import get_engine, get_ecom_engine
from core.sheets import get_client

gs  = get_client()
TARGET_SHEET_KEY = '1aFDuIMWZvW2dBIJsUpWgE4XUyIFfW4wFqq4Undhoyfw'
sht = gs.open_by_key(TARGET_SHEET_KEY)
SHEET_TARGET = 'UNPIVOT_TARGET'
SHEET_OUTPUT = 'FORECAST_REVENUE'

engine      = get_engine()
engine_ecom = get_ecom_engine()

today = date.today()

# ==========================================
# THAM SỐ
# ==========================================
N_MONTHS       = 3      # số tháng dự báo (tính từ tháng sau tháng hiện tại)
CATEGORY_SETUP = ['SANDALS', 'SNEAKERS', 'SLIDES', 'KID SANDALS', 'KID SNEAKERS']

# Tháng cuối có dữ liệu đầy đủ = tháng trước tháng hiện tại
_last_m = today.month - 1 if today.month > 1 else 12
_last_y = today.year     if today.month > 1 else today.year - 1
hist_end = pd.Timestamp(date(_last_y, _last_m, 1))

# Lịch sử 2 năm
hist_start = pd.Timestamp(date(today.year - 2, 1, 1))
all_months_idx = pd.date_range(hist_start, hist_end, freq='MS')

# Tháng dự báo
_fm, _fy = today.month + 1, today.year
if _fm > 12: _fm = 1; _fy += 1
fc_index = pd.date_range(pd.Timestamp(date(_fy, _fm, 1)), periods=N_MONTHS, freq='MS')
_forecast_months = [(ts.year, ts.month) for ts in fc_index]
print(f'History: {hist_start.date()} → {hist_end.date()}  |  Forecast: {_forecast_months}')


# ==========================================
# HÀM FIT HOLT-WINTERS
# ==========================================
def hw_forecast(series: pd.Series, n: int) -> tuple[list[float], str]:
    """
    Fit Holt-Winters trên chuỗi tháng và trả về (forecast_list, model_label).
    Tự động chọn cấu hình tuỳ theo số điểm dữ liệu:
      >= 24 tháng → trend + seasonal (full ETS)
      12-23 tháng → trend only
       < 12 tháng → level only (SES)
    Nếu fit thất bại → fallback bình quân 3 tháng gần nhất.
    """
    s = series.fillna(0).clip(lower=0)
    npts = len(s)
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        try:
            if npts >= 24:
                model = ExponentialSmoothing(s, trend='add', seasonal='add',
                                             seasonal_periods=12, initialization_method='estimated')
                label = 'ETS(A,A,A)'
            elif npts >= 12:
                model = ExponentialSmoothing(s, trend='add', seasonal=None,
                                             initialization_method='estimated')
                label = 'ETS(A,A,N)'
            else:
                model = ExponentialSmoothing(s, trend=None, seasonal=None,
                                             initialization_method='estimated')
                label = 'ETS(A,N,N)'
            fit  = model.fit(optimized=True)
            fc   = fit.forecast(n).clip(lower=0).tolist()
            return fc, label
        except Exception as e:
            avg = float(s.iloc[-3:].mean()) if npts >= 3 else float(s.mean())
            return [avg] * n, f'avg_fallback({e.__class__.__name__})'


# ==========================================
# 1. PRODUCT TEMPLATE
# ==========================================
df_pt = get_product_template(engine)
df_pt['fdcode'] = df_pt['fdcode'].str.upper()

# ==========================================
# 2. LỊCH SỬ DOANH THU
#    2024-2025 : query riêng (chính xác hơn, có filter đầy đủ)
#    2026 YTD  : query kiểu adjust_plan.py
# ==========================================

# ---------- 2A. Main DB: 2024-2025 ----------
query_hist_main_old = """
WITH pt AS (
    SELECT
        ps.external_product_id,
        ps.code AS default_code,
        c1.name AS subcategory,
        c2.name AS category
    FROM categories c1
    LEFT JOIN categories c2 ON c1.parent_id = c2.category_id
    LEFT JOIN products ps
        ON ps.category_id = c1.external_category_id AND ps.parent_id = -2
    WHERE c2.name IS NOT NULL AND ps.parent_id IS NOT NULL
),
filtered_orders AS (
    SELECT
        so.orderId, so.createdDateTime, so.channelName, so.saleChannel,
        so.relatedBillId, so.status, so.depotId,
        so.usedPointsMoney, so.privateDescription, so.shopOrderId
    FROM sale_order so
    WHERE so.status NOT IN ('Canceled','Returning','Failed','Returned',
                            'Aborted','CarrierCanceled','ConfirmReturned')
      AND so.type != 'Khách trả lại hàng'
      AND NOT (
            so.privateDescription LIKE '%MDX%'
            AND so.saleChannel IN (1,2,10,20,21,46)
            AND so.channelName != 'Kho Lẻ'
      )
      AND YEAR(so.createdDateTime) BETWEEN 2024 AND 2025
),
base AS (
    SELECT
        YEAR(fo.createdDateTime) AS year,
        MONTH(fo.createdDateTime) AS month,
        CASE
            WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
            WHEN fo.channelName = 'Kho Lẻ'  THEN 'KDC'
            WHEN st.code_nhanh = 'KHO SỈ'  THEN 'KDS'
            WHEN fo.saleChannel IN (1,2,10,20,21,41,42,43,45,46,47,48,49,50,51) THEN 'ECOM'
            ELSE 'DT KHÁC'
        END AS channel,
        UPPER(
            CASE
                WHEN sc.sale_channel_name = 'Admin' AND st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
                WHEN fo.channelName = 'KHO LẺ'  THEN st.code_nhanh
                WHEN fo.saleChannel IN (2,10)    THEN 'WEB'
                WHEN fo.saleChannel IN (1,20,21,46) THEN 'FB/INS/ZL/NB'
                WHEN fo.saleChannel = 41 THEN 'LAZADA'
                WHEN fo.saleChannel = 42 THEN 'SHOPEE'
                WHEN fo.saleChannel = 48 THEN 'TIKTOK'
                ELSE 'KHO LỖI'
            END
        ) AS store,
        COALESCE(pt.category,     'BAGS') AS category,
        COALESCE(pt.subcategory,  'BAGS') AS subcategory,
        COALESCE(pt.default_code, ps2.code) AS default_code,
        CASE
            WHEN fo.relatedBillId IS NOT NULL AND TRIM(fo.relatedBillId) != ''
                THEN -soi.quantity ELSE soi.quantity
        END AS qty,
        CASE
            WHEN fo.relatedBillId IS NOT NULL AND TRIM(fo.relatedBillId) != ''
                THEN -((soi.price * soi.quantity) - (soi.discount * soi.quantity))
            WHEN fo.channelName = 'Kho Lẻ'
                THEN (soi.price * soi.quantity) - soi.discount - fo.usedPointsMoney
            ELSE (soi.price * soi.quantity) - (soi.discount * soi.quantity)
        END AS rvn,
        fo.channelName
    FROM filtered_orders fo
    LEFT JOIN sale_order_items soi ON fo.orderId = soi.sale_order_id
    LEFT JOIN products ps2 ON ps2.external_product_id = soi.external_product_id
    LEFT JOIN pt           ON pt.external_product_id  = ps2.parent_id
    LEFT JOIN stores st    ON st.depot_id_nhanh = fo.depotId
    LEFT JOIN sale_channel sc ON sc.id = fo.saleChannel
)
SELECT year, month, channel, category, subcategory, default_code,
       SUM(qty) AS qty, SUM(rvn) AS rvn
FROM base
WHERE channel NOT IN ('DT KHÁC')
  AND store NOT IN ('TIKTOK', 'SHOPEE', 'ECOM SG')
  AND NOT (store = 'WEB' AND channelName <> 'Kho Lẻ')
GROUP BY year, month, channel, category, subcategory, default_code
"""
with engine.connect() as conn:
    df_hist_main_old = pd.read_sql_query(text(query_hist_main_old), conn)
print(f'History main 2024-2025: {len(df_hist_main_old)} rows')

# ---------- 2B. Ecom DB: 2024-2025 ----------
query_hist_ecom_old = """
SELECT
    YEAR(eo.order_date)          AS year,
    MONTH(eo.order_date)         AS month,
    'ECOM'                       AS channel,
    eoi.product_sku              AS fdcode,
    eoi.quantity                 AS qty,
    eoi.price * eoi.quantity     AS rvn
FROM ecommerce_orders eo
JOIN ecommerce_order_items eoi ON eoi.external_order_id = eo.external_order_id
JOIN order_source os           ON eo.order_source_id = os.id
WHERE YEAR(eo.order_date) BETWEEN 2024 AND 2025
  AND UPPER(os.name) NOT IN ('BOXME','RETAIL')
  AND eoi.product_sku != ''
  AND eoi.product_sku NOT LIKE '%HOP%'
  AND eoi.product_sku NOT LIKE '%TUIRUT%'
  AND eoi.product_sku <> 'LIMAXCARD'
  AND eo.status <> 'cancelled'
"""
with engine_ecom.connect() as conn:
    df_hist_ecom_old = pd.read_sql_query(text(query_hist_ecom_old), conn)
print(f'History ecom 2024-2025: {len(df_hist_ecom_old)} rows')

df_hist_ecom_old = df_hist_ecom_old.merge(
    df_pt[['fdcode', 'default_code', 'category', 'subcategory']], on='fdcode', how='left')
df_hist_ecom_old = df_hist_ecom_old.groupby(
    ['year', 'month', 'channel', 'category', 'subcategory', 'default_code'], as_index=False
).agg({'qty': 'sum', 'rvn': 'sum'})

# ---------- 2C. Main DB: 2026 YTD ----------
query_hist_main_2026 = """
WITH pt AS (
    SELECT ps.external_product_id, ps.code AS default_code,
           c1.name AS subcategory, c2.name AS category
    FROM categories c1
    LEFT JOIN categories c2 ON c1.parent_id = c2.category_id
    LEFT JOIN products ps ON ps.category_id = c1.external_category_id AND ps.parent_id = -2
    WHERE c2.name IS NOT NULL AND ps.parent_id IS NOT NULL
),
base AS (
    SELECT
        YEAR(so.createdDateTime)  AS year,
        MONTH(so.createdDateTime) AS month,
        CASE
            WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
            WHEN so.channelName = 'Kho Lẻ'  THEN 'KDC'
            WHEN st.code_nhanh = 'KHO SỈ'  THEN 'KDS'
            WHEN so.saleChannel IN (1,2,10,20,21,41,42,43,45,46,47,48,49,50,51) THEN 'ECOM'
            ELSE 'DT KHÁC'
        END AS channel,
        UPPER(
            CASE
                WHEN sc.sale_channel_name = 'Admin' AND st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
                WHEN so.channelName = 'KHO LẺ'  THEN st.code_nhanh
                WHEN so.saleChannel IN (2,10)    THEN 'WEB'
                WHEN so.saleChannel IN (1,20,21,46) THEN 'FB/INS/ZL/NB'
                WHEN so.saleChannel = 41 THEN 'LAZADA'
                WHEN so.saleChannel = 42 THEN 'SHOPEE'
                WHEN so.saleChannel = 48 THEN 'TIKTOK'
                ELSE 'KHO LỖI'
            END
        ) AS store,
        COALESCE(pt.category,     'BAGS') AS category,
        COALESCE(pt.subcategory,  'BAGS') AS subcategory,
        COALESCE(pt.default_code, ps2.code) AS default_code,
        CASE
            WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != ''
                THEN -soi.quantity ELSE soi.quantity
        END AS qty,
        CASE
            WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != ''
                THEN -((soi.price * soi.quantity) - (soi.discount * soi.quantity))
            WHEN so.channelName = 'Kho Lẻ'
                THEN (soi.price * soi.quantity) - soi.discount - so.usedPointsMoney
            ELSE (soi.price * soi.quantity) - (soi.discount * soi.quantity)
        END AS rvn,
        so.channelName
    FROM sale_order so
    LEFT JOIN sale_order_items soi ON so.orderId = soi.sale_order_id
    LEFT JOIN products ps2 ON ps2.external_product_id = soi.external_product_id
    LEFT JOIN pt           ON pt.external_product_id  = ps2.parent_id
    LEFT JOIN stores st    ON st.depot_id_nhanh = so.depotId
    LEFT JOIN sale_channel sc ON sc.id = so.channel
    WHERE so.status NOT IN ('Canceled','Returning','Failed','Returned',
                            'Aborted','CarrierCanceled','ConfirmReturned')
      AND so.type != 'Khách trả lại hàng'
      AND YEAR(so.createdDateTime) = YEAR(CURDATE())
)
SELECT year, month, channel, category, subcategory, default_code,
       SUM(qty) AS qty, SUM(rvn) AS rvn
FROM base
WHERE channel NOT IN ('DT KHÁC')
  AND store NOT IN ('TIKTOK','SHOPEE','ECOM SG')
  AND NOT (store = 'WEB' AND channelName <> 'Kho Lẻ')
GROUP BY year, month, channel, category, subcategory, default_code
"""
with engine.connect() as conn:
    df_hist_main_2026 = pd.read_sql_query(text(query_hist_main_2026), conn)
print(f'History main 2026: {len(df_hist_main_2026)} rows')

# ---------- 2D. Ecom DB: 2026 YTD ----------
query_hist_ecom_2026 = """
SELECT
    YEAR(eo.order_date)      AS year,
    MONTH(eo.order_date)     AS month,
    'ECOM'                   AS channel,
    eoi.product_sku          AS fdcode,
    eoi.quantity             AS qty,
    eoi.price * eoi.quantity AS rvn
FROM ecommerce_orders eo
JOIN ecommerce_order_items eoi ON eoi.external_order_id = eo.external_order_id
JOIN order_source os           ON eo.order_source_id = os.id
WHERE YEAR(eo.order_date) = YEAR(CURDATE())
  AND UPPER(os.name) NOT IN ('BOXME','RETAIL')
  AND eoi.product_sku != ''
  AND eoi.product_sku NOT LIKE '%HOP%'
  AND eoi.product_sku NOT LIKE '%TUIRUT%'
  AND eoi.product_sku <> 'LIMAXCARD'
  AND eo.status <> 'cancelled'
"""
with engine_ecom.connect() as conn:
    df_hist_ecom_2026 = pd.read_sql_query(text(query_hist_ecom_2026), conn)
print(f'History ecom 2026: {len(df_hist_ecom_2026)} rows')

df_hist_ecom_2026 = df_hist_ecom_2026.merge(
    df_pt[['fdcode', 'default_code', 'category', 'subcategory']], on='fdcode', how='left')
df_hist_ecom_2026 = df_hist_ecom_2026.groupby(
    ['year', 'month', 'channel', 'category', 'subcategory', 'default_code'], as_index=False
).agg({'qty': 'sum', 'rvn': 'sum'})

# ---------- Gộp 4 nguồn ----------
df_hist = pd.concat(
    [df_hist_main_old, df_hist_ecom_old, df_hist_main_2026, df_hist_ecom_2026],
    ignore_index=True
)
df_hist = df_hist[df_hist['category'].isin(CATEGORY_SETUP)]
df_hist = df_hist.groupby(
    ['year', 'month', 'channel', 'subcategory', 'default_code'], as_index=False
).agg({'qty': 'sum', 'rvn': 'sum'})
print(f'History tổng hợp: {len(df_hist)} rows')

# DEBUG: so sánh T8 vs T9 theo năm và channel
print('\n=== SO SÁNH T8 vs T9 THEO NĂM ===')
_t89 = (df_hist[df_hist['month'].isin([8, 9])]
        .groupby(['year', 'month'])['rvn'].sum()
        .reset_index().sort_values(['year', 'month']))
_t89['rvn_b'] = (_t89['rvn'] / 1e9).round(2)
print(_t89[['year', 'month', 'rvn_b']].rename(columns={'rvn_b': 'rvn(B)'}).to_string(index=False))

print('\n=== SO SÁNH T8 vs T9 THEO CHANNEL × NĂM ===')
_t89_ch = (df_hist[df_hist['month'].isin([8, 9])]
           .groupby(['channel', 'year', 'month'])['rvn'].sum()
           .reset_index().sort_values(['channel', 'year', 'month']))
_t89_ch['rvn_b'] = (_t89_ch['rvn'] / 1e9).round(2)
print(_t89_ch[['channel', 'year', 'month', 'rvn_b']].rename(columns={'rvn_b': 'rvn(B)'}).to_string(index=False))

# Tổng theo channel × subcategory × tháng (để fit model)
df_hist_sub = df_hist.groupby(['year', 'month', 'channel', 'subcategory'], as_index=False).agg(
    {'rvn': 'sum', 'qty': 'sum'})
df_hist_sub['date'] = pd.to_datetime(df_hist_sub[['year', 'month']].assign(day=1))

# ==========================================
# 3. ĐỌC UNPIVOT_TARGET (đọc trước để dùng cho SP mới)
# ==========================================
ws_target   = sht.worksheet(SHEET_TARGET)
data_target = ws_target.get_all_values()
df_target   = pd.DataFrame(data_target[1:], columns=data_target[0])
df_target['month']   = pd.to_numeric(df_target['month'], errors='coerce')
df_target['year']    = pd.to_numeric(df_target['year'].astype(str).str.replace(',', ''), errors='coerce')
df_target['numbers'] = pd.to_numeric(df_target['numbers'].astype(str).str.replace(',', ''), errors='coerce')

df_tgt_rvn = (df_target[df_target['metric'].str.strip() == 'Dthu dự tính']
              .groupby(['channel', 'subcategory', 'year', 'month'], as_index=False)['numbers'].sum()
              .rename(columns={'numbers': 'target_rvn'}))
df_tgt_qty = (df_target[df_target['metric'].str.strip() == 'SL kì vọng']
              .groupby(['channel', 'subcategory', 'year', 'month'], as_index=False)['numbers'].sum()
              .rename(columns={'numbers': 'target_qty'}))

# Lookup dict để truy cập nhanh trong vòng lặp
_tgt_rvn_lkp = {(r.channel, r.subcategory, int(r.year), int(r.month)): r.target_rvn
                for r in df_tgt_rvn.itertuples()}
_tgt_qty_lkp = {(r.channel, r.subcategory, int(r.year), int(r.month)): r.target_qty
                for r in df_tgt_qty.itertuples()}
print(f'Target rvn rows: {len(df_tgt_rvn)}, target qty rows: {len(df_tgt_qty)}')

# ==========================================
# 4. FORECAST CHO TỪNG channel × subcategory
#    SP cũ (≥ 18 tháng data thực) → Holt-Winters
#    SP mới (< 18 tháng)          → target nếu có, không thì avg 3 tháng gần nhất
# ==========================================
NEW_PRODUCT_THRESHOLD = 18  # tháng có rvn > 0

fc_records = []

for (ch, sub), grp in df_hist_sub.groupby(['channel', 'subcategory']):
    grp = grp.set_index('date').sort_index()

    rvn_series = grp['rvn'].reindex(all_months_idx, fill_value=0.0)
    qty_series = grp['qty'].reindex(all_months_idx, fill_value=0.0)

    n_nonzero = int((rvn_series > 0).sum())

    if n_nonzero >= NEW_PRODUCT_THRESHOLD:
        # ── SP cũ: Holt-Winters ──
        fc_rvn, lbl = hw_forecast(rvn_series, N_MONTHS)
        fc_qty, _   = hw_forecast(qty_series, N_MONTHS)

    else:
        # ── SP mới: target ưu tiên, fallback avg ──
        recent_rvn = float(rvn_series[rvn_series > 0].iloc[-3:].mean()) if n_nonzero > 0 else 0.0
        recent_qty = float(qty_series[qty_series > 0].iloc[-3:].mean()) if n_nonzero > 0 else 0.0

        fc_rvn, fc_qty, labels = [], [], []
        for (yr, mo) in _forecast_months:
            key = (ch, sub, yr, mo)
            t_rvn = _tgt_rvn_lkp.get(key, 0.0) or 0.0
            t_qty = _tgt_qty_lkp.get(key, 0.0) or 0.0

            if t_rvn > 0:
                fc_rvn.append(t_rvn)
                fc_qty.append(t_qty if t_qty > 0 else recent_qty)
                labels.append('new(target)')
            else:
                fc_rvn.append(recent_rvn)
                fc_qty.append(recent_qty)
                labels.append('new(avg)')

        # Gán label chung (target nếu có ít nhất 1 tháng target)
        lbl = 'new(target)' if any(l == 'new(target)' for l in labels) else 'new(avg)'

    for i, ts in enumerate(fc_index):
        fc_records.append({
            'channel':      ch,
            'subcategory':  sub,
            'year':         ts.year,
            'month':        ts.month,
            'forecast_rvn': round(max(fc_rvn[i], 0)),
            'forecast_qty': round(max(fc_qty[i], 0)),
            'model':        lbl,
            'n_nonzero':    n_nonzero,
        })

df_fc_subcat = pd.DataFrame(fc_records)

# Bổ sung subcategory có target nhưng chưa có data lịch sử (SP hoàn toàn mới)
_covered = set(zip(df_fc_subcat['channel'], df_fc_subcat['subcategory']))
_tgt_fc_months = df_tgt_rvn[
    df_tgt_rvn[['year', 'month']].apply(
        lambda r: (int(r['year']), int(r['month'])) in set(_forecast_months), axis=1)
]
_missing_rows = []
for _, row in _tgt_fc_months.iterrows():
    key = (row['channel'], row['subcategory'])
    if key not in _covered and row['target_rvn'] > 0:
        t_qty = _tgt_qty_lkp.get((row['channel'], row['subcategory'],
                                   int(row['year']), int(row['month'])), 0.0) or 0.0
        _missing_rows.append({
            'channel':      row['channel'],
            'subcategory':  row['subcategory'],
            'year':         int(row['year']),
            'month':        int(row['month']),
            'forecast_rvn': round(row['target_rvn']),
            'forecast_qty': round(t_qty),
            'model':        'new(target,no_hist)',
            'n_nonzero':    0,
        })

if _missing_rows:
    df_fc_subcat = pd.concat(
        [df_fc_subcat, pd.DataFrame(_missing_rows)], ignore_index=True)
    print(f'Bổ sung {len(set((r["channel"], r["subcategory"]) for r in _missing_rows))} subcategory mới từ target')

new_prod_count = df_fc_subcat[df_fc_subcat['model'].str.startswith('new')]['subcategory'].nunique()
print(f'Forecast subcategory rows: {len(df_fc_subcat)}  |  SP mới: {new_prod_count} subcategory')

# DEBUG: subcategory nào đang forecast T9 > T8
_t8 = df_fc_subcat[df_fc_subcat['month'] == 8][['channel','subcategory','forecast_rvn']].rename(columns={'forecast_rvn':'fc_t8'})
_t9 = df_fc_subcat[df_fc_subcat['month'] == 9][['channel','subcategory','forecast_rvn']].rename(columns={'forecast_rvn':'fc_t9'})
_cmp = _t8.merge(_t9, on=['channel','subcategory']).query('fc_t9 > fc_t8').sort_values('fc_t9', ascending=False)
_cmp['diff_B'] = ((_cmp['fc_t9'] - _cmp['fc_t8']) / 1e9).round(2)
print('\n=== SUBCATEGORY CÓ FORECAST T9 > T8 ===')
print(_cmp[['channel','subcategory','fc_t8','fc_t9','diff_B']].to_string(index=False))

# ==========================================
# 5. HIỆU CHỈNH SEASONAL T8/T9 CHO ETS MODELS
#    Nếu lịch sử T8 > T9 nhưng model dự báo T9 > T8 → redistribute theo tỉ lệ lịch sử
# ==========================================
_hist_89 = (df_hist_sub[df_hist_sub['month'].isin([8, 9])]
            .groupby(['channel', 'subcategory', 'month'])['rvn'].mean()
            .unstack('month').reset_index()
            .rename(columns={8: 'hist_t8', 9: 'hist_t9'})
            .fillna(0))
_hist_89['hist_t8_gt_t9'] = _hist_89['hist_t8'] > _hist_89['hist_t9']
_hist_89['hist_total_89'] = _hist_89['hist_t8'] + _hist_89['hist_t9']
_hist_89['hist_t8_share'] = np.where(
    _hist_89['hist_total_89'] > 0,
    _hist_89['hist_t8'] / _hist_89['hist_total_89'], 0.5)

# Lấy forecast T8 và T9 của các ETS model
_fc_t8 = df_fc_subcat[(df_fc_subcat['month'] == 8) & (df_fc_subcat['model'].str.startswith('ETS'))].copy()
_fc_t9 = df_fc_subcat[(df_fc_subcat['month'] == 9) & (df_fc_subcat['model'].str.startswith('ETS'))].copy()
_fc_89 = _fc_t8.merge(_fc_t9, on=['channel', 'subcategory'], suffixes=('_t8', '_t9'))
_fc_89 = _fc_89.merge(_hist_89[['channel', 'subcategory', 'hist_t8_gt_t9', 'hist_t8_share']],
                      on=['channel', 'subcategory'], how='left')

# Tìm các (channel, subcategory) cần hiệu chỉnh
_need_fix = _fc_89[
    _fc_89['hist_t8_gt_t9'].fillna(False) &
    (_fc_89['forecast_rvn_t9'] > _fc_89['forecast_rvn_t8'])
][['channel', 'subcategory', 'hist_t8_share']].copy()

if len(_need_fix) > 0:
    print(f'\nHiệu chỉnh T8/T9 cho {len(_need_fix)} subcategory: {_need_fix["subcategory"].tolist()}')
    for _, row in _need_fix.iterrows():
        mask_t8 = (df_fc_subcat['channel'] == row['channel']) & \
                  (df_fc_subcat['subcategory'] == row['subcategory']) & \
                  (df_fc_subcat['month'] == 8)
        mask_t9 = (df_fc_subcat['channel'] == row['channel']) & \
                  (df_fc_subcat['subcategory'] == row['subcategory']) & \
                  (df_fc_subcat['month'] == 9)
        total_rvn = (df_fc_subcat.loc[mask_t8, 'forecast_rvn'].values[0] +
                     df_fc_subcat.loc[mask_t9, 'forecast_rvn'].values[0])
        total_qty = (df_fc_subcat.loc[mask_t8, 'forecast_qty'].values[0] +
                     df_fc_subcat.loc[mask_t9, 'forecast_qty'].values[0])
        df_fc_subcat.loc[mask_t8, 'forecast_rvn'] = round(total_rvn * row['hist_t8_share'])
        df_fc_subcat.loc[mask_t9, 'forecast_rvn'] = round(total_rvn * (1 - row['hist_t8_share']))
        df_fc_subcat.loc[mask_t8, 'forecast_qty'] = round(total_qty * row['hist_t8_share'])
        df_fc_subcat.loc[mask_t9, 'forecast_qty'] = round(total_qty * (1 - row['hist_t8_share']))
        df_fc_subcat.loc[mask_t8 | mask_t9, 'model'] = \
            df_fc_subcat.loc[mask_t8 | mask_t9, 'model'] + '(adj_seasonal)'

# ==========================================
# 6. GHÉP FORECAST + TARGET → TÍNH GAP
# ==========================================
df_compare = (df_fc_subcat
              .merge(df_tgt_rvn, on=['channel', 'subcategory', 'year', 'month'], how='left')
              .merge(df_tgt_qty, on=['channel', 'subcategory', 'year', 'month'], how='left'))

df_compare['gap_rvn'] = df_compare.apply(
    lambda r: round(r['forecast_rvn'] - r['target_rvn'])
              if pd.notna(r['target_rvn']) else None, axis=1)

df_compare['gap_rvn_pct'] = df_compare.apply(
    lambda r: round((r['forecast_rvn'] - r['target_rvn']) / r['target_rvn'] * 100, 1)
              if pd.notna(r['target_rvn']) and r['target_rvn'] != 0 else None, axis=1)

df_compare['gap_qty'] = df_compare.apply(
    lambda r: round(r['forecast_qty'] - r['target_qty'])
              if pd.notna(r['target_qty']) else None, axis=1)

# Cờ cảnh báo: forecast thấp hơn target quá 20%
df_compare['alert'] = df_compare['gap_rvn_pct'].apply(
    lambda x: '⚠ Dưới target' if pd.notna(x) and x < -20
              else ('✓ Đạt target' if pd.notna(x) else ''))

# ==========================================
# 6. PHÂN BỔ XUỐNG default_code THEO RVN_SHARE
#    Dùng 3 tháng gần nhất
# ==========================================
_recent = set()
_rm, _ry = today.month - 1, today.year
for _ in range(3):
    if _rm < 1: _rm = 12; _ry -= 1
    _recent.add((_ry, _rm))
    _rm -= 1

df_recent = df_hist[df_hist[['year', 'month']].apply(
    lambda r: (int(r['year']), int(r['month'])) in _recent, axis=1)]

df_code_share = df_recent.groupby(['channel', 'subcategory', 'default_code'], as_index=False)['rvn'].sum()
df_sub_tot    = df_recent.groupby(['channel', 'subcategory'], as_index=False)['rvn'].sum().rename(columns={'rvn': 'sub_rvn'})
df_code_share = df_code_share.merge(df_sub_tot, on=['channel', 'subcategory'], how='left')
df_code_share['rvn_share'] = np.where(
    df_code_share['sub_rvn'] > 0, df_code_share['rvn'] / df_code_share['sub_rvn'], 0)

df_fc_code = df_compare.merge(
    df_code_share[['channel', 'subcategory', 'default_code', 'rvn_share']],
    on=['channel', 'subcategory'], how='left')

df_fc_code['forecast_rvn_code'] = (df_fc_code['forecast_rvn'] * df_fc_code['rvn_share'].fillna(0)).round(0).astype(int)
df_fc_code['forecast_qty_code'] = (df_fc_code['forecast_qty'] * df_fc_code['rvn_share'].fillna(0)).round(0).astype(int)

# ==========================================
# 7. OUTPUT
# ==========================================
# Sheet 1 – Tổng theo subcategory (để review nhanh)
cols_sub = ['channel', 'subcategory', 'year', 'month',
            'forecast_rvn', 'forecast_qty',
            'target_rvn', 'target_qty',
            'gap_rvn', 'gap_rvn_pct', 'gap_qty',
            'alert', 'model', 'n_nonzero']
df_out_sub = df_compare[cols_sub].sort_values(['channel', 'subcategory', 'year', 'month'])

# Sheet 2 – Chi tiết theo default_code
cols_code = ['channel', 'subcategory', 'default_code', 'year', 'month',
             'forecast_rvn_code', 'forecast_qty_code', 'rvn_share', 'model']
df_out_code = (df_fc_code[df_fc_code['forecast_rvn_code'] > 0][cols_code]
               .rename(columns={'forecast_rvn_code': 'forecast_rvn', 'forecast_qty_code': 'forecast_qty'})
               .sort_values(['channel', 'subcategory', 'year', 'month', 'forecast_rvn'], ascending=[True,True,True,True,False]))

print('\n=== FORECAST vs TARGET (subcategory) ===')
print(df_out_sub.to_string(index=False))

# Ghi ra sheet (subcategory summary trước, cách 2 dòng, rồi detail)
try:
    ws_out = sht.worksheet(SHEET_OUTPUT)
    ws_out.clear()
except Exception:
    ws_out = sht.add_worksheet(title=SHEET_OUTPUT, rows=3000, cols=20)

# Ghi subcategory summary
gd.set_with_dataframe(ws_out, df_out_sub)
# Ghi detail bên dưới (cách 2 dòng)
start_row = len(df_out_sub) + 3
gd.set_with_dataframe(ws_out, df_out_code, row=start_row, col=1, include_column_header=True)

print(f'\nDone. Written {len(df_out_sub)} subcategory rows + {len(df_out_code)} code rows to "{SHEET_OUTPUT}".')

print(df_hist[
    (df_hist['subcategory'] == 'SUKE') & 
    (df_hist['channel'] == 'ECOM') &
    (df_hist['month'].isin([7, 8, 9]))
].groupby(['year', 'month'])['rvn'].sum().reset_index().to_string(index=False))