import calendar
import pandas as pd
import numpy as np
from datetime import date
import gspread_dataframe as gd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from core.queries import get_product_template
from core.db import get_engine, get_ecom_engine
from core.sheets import get_client

gs  = get_client()
sht = gs.open_by_key('1U-dcqwnDrgkQjPndtvszZfZIBSEYx0kPKm0f-ud8KFg')
SHEET3  = 'LIST_ORDER'
SHEET4  = 'SEMI_TOP20'
SHEET7  = 'RAW_STOCK'
SHEET8  = 'RAW_PRODUCTS'
SHEET9  = 'STOCK_PEN'
SHEET11 = 'REPORT_ORDER'

TARGET_SHEET_KEY  = '1aFDuIMWZvW2dBIJsUpWgE4XUyIFfW4wFqq4Undhoyfw'
TARGET_SHEET_NAME = 'UNPIVOT_TARGET'

engine      = get_engine()
engine_ecom = get_ecom_engine()

# ==========================================
# THAM SỐ
# ==========================================
ORDER_MONTH    = 8           # tháng cần bán (đặt hàng trước 3 tháng)
ORDER_YEAR     = 2026
METRIC_FILTER  = 'SL kì vọng'  # giá trị cột metric trong UNPIVOT_TARGET ('SL kì vọng' hoặc 'Dthu dự tính')
SAFETY_FACTOR  = {'KDS': 2.0, 'KDC': 3.0, 'ECOM': 2.5}  # tồn đầu kỳ = target × hệ số theo kênh
MIN_ORDER_CODE = 1000         # tổng đặt tối thiểu mỗi mã (tất cả kênh)

category_setup = ['SANDALS', 'SNEAKERS', 'SLIDES', 'KID SANDALS', 'KID SNEAKERS']


def map_channel(code):
    if code in ('KHO ECOM', 'ECOM2', 'ECOM', 'ECOM SG', 'KHO BOXME'):
        return 'ECOM'
    if code in ('KHO SỈ', 'KDS'):
        return 'KDS'
    return 'KDC'


# ==========================================
# 1. PRODUCT TEMPLATE
# ==========================================
df_products_template = get_product_template(engine)
df_products_template['fdcode'] = df_products_template['fdcode'].str.upper()

# ==========================================
# 2. TỒN KHO
# ==========================================
query_stock = """
    WITH category_tree AS (
        SELECT
            c1.external_category_id,
            c1.name,
            c2.name AS parent_name
        FROM categories c1
        LEFT JOIN categories c2 ON c1.parent_id = c2.category_id
        WHERE c2.name IS NOT NULL
    ),
    max_change AS (
        SELECT product_id, depot_id, MAX(changed_at) AS max_changed_at
        FROM product_inventory_history
        WHERE depot_id NOT IN (142410, 217633, 125224, 111753, 111752, 101011, 222877, 448734, 92162)
        GROUP BY product_id, depot_id
    ),
    stock_today AS (
        SELECT
            st.code_nhanh store,
            pih.depot_id AS depot_id_nhanh,
            pih.product_id,
            ps.code AS fdcode,
            COALESCE(ct.name, 'BAGS') AS subcategory,
            COALESCE(ct.parent_name, 'BAGS') AS category,
            pih.available,
            pih.last_updated_at
        FROM product_inventories AS pih
        LEFT JOIN stores AS st ON st.depot_id_nhanh = pih.depot_id
        LEFT JOIN products AS ps ON ps.product_id = pih.product_id
        LEFT JOIN category_tree ct ON ct.external_category_id = ps.category_id
        WHERE pih.available >= 1
          AND pih.depot_id NOT IN (142410, 217633, 125224, 111753, 111752, 101011, 222877, 448734, 92162)
          AND DATE(pih.last_updated_at) >= CURRENT_DATE() - INTERVAL 1 DAY
    ),
    stock_last_change AS (
        SELECT
            st.code_nhanh store,
            pih.depot_id AS depot_id_nhanh,
            pih.product_id,
            ps.code AS fdcode,
            COALESCE(ct.name, 'BAGS') AS subcategory,
            COALESCE(ct.parent_name, 'BAGS') AS category,
            pih.available,
            pih.changed_at
        FROM product_inventory_history AS pih
        JOIN max_change mc
            ON pih.product_id = mc.product_id
            AND pih.depot_id = mc.depot_id
            AND pih.changed_at = mc.max_changed_at
        LEFT JOIN (
            SELECT DISTINCT product_id, depot_id_nhanh
            FROM stock_today
        ) AS st_today
            ON pih.product_id = st_today.product_id
            AND pih.depot_id  = st_today.depot_id_nhanh
        LEFT JOIN products AS ps ON ps.product_id = pih.product_id
        LEFT JOIN stores AS st ON st.depot_id_nhanh = pih.depot_id
        LEFT JOIN category_tree ct ON ct.external_category_id = ps.category_id
        WHERE st_today.product_id IS NULL
    )
    SELECT * FROM stock_today
    UNION ALL
    SELECT * FROM stock_last_change
"""

with engine.connect() as conn:
    df_stock = pd.read_sql_query(text(query_stock), conn)
print('Finished query stock')

df_stock['channel'] = df_stock['store'].apply(map_channel)
df_stock = pd.merge(df_stock, df_products_template[['fdcode', 'default_code']], on='fdcode', how='left')

df_stock_fdcode = (
    df_stock[df_stock['category'].isin(category_setup)]
    .groupby(['channel', 'fdcode', 'default_code', 'subcategory', 'category'], as_index=False)
    .agg({'available': 'sum'})
)

# ==========================================
# 3. ĐƠN HÀNG ĐANG VỀ (ORDER PEN)
# ==========================================
with engine.connect() as conn:
    df_order_tracking = pd.read_sql_query(text("SELECT * FROM stock_pen"), conn)
print('Finished query order tracking')

worksheet_order_tracking = sht.worksheet(SHEET9)
worksheet_order_tracking.clear()
gd.set_with_dataframe(worksheet_order_tracking, df_order_tracking)
print('Updated STOCK_PEN')

df_order_pen = (
    df_order_tracking.groupby(['channel', 'fdcode'], as_index=False)
    .agg({'order_pen': 'sum'})
)

# ==========================================
# 4. DOANH SỐ 90 NGÀY – KDC / KDS
# ==========================================
query_sales_90 = """
    WITH pt AS (
        SELECT
            ps.external_product_id,
            ps.product_id,
            ps.code AS default_code,
            c1.name AS subcategory,
            c2.name AS category
        FROM categories c1
        LEFT JOIN categories c2 ON c1.parent_id = c2.category_id
        LEFT JOIN products ps ON ps.category_id = c1.external_category_id AND ps.parent_id = -2
        WHERE c2.name IS NOT NULL AND ps.parent_id IS NOT NULL
    ),
    main_data AS (
        SELECT
            UPPER(CASE
                WHEN sc.sale_channel_name = 'Admin' AND st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                WHEN so.channelName = 'KHO LẺ' THEN st.code_nhanh
                WHEN so.saleChannel IN (1, 2, 10, 20, 21, 41, 42, 46, 48) THEN 'ECOM'
                ELSE 'KHO LỖI'
            END) store,
            so.channelName,
            COALESCE(pt.category, 'BAGS') AS category,
            COALESCE(pt.subcategory, 'BAGS') AS subcategory,
            COALESCE(pt.default_code, ps2.code) AS default_code,
            ps2.code fdcode,
            soi.quantity,
            soi.price,
            soi.discount,
            so.relatedBillId,
            ps2.launch_date
        FROM sale_order so
        LEFT JOIN sale_order_items soi ON so.orderId = soi.sale_order_id
        LEFT JOIN products ps2 ON ps2.external_product_id = soi.external_product_id
        LEFT JOIN pt ON pt.external_product_id = ps2.parent_id
        LEFT JOIN stores st ON st.depot_id_nhanh = so.depotId
        LEFT JOIN sale_channel sc ON sc.id = so.channel
        WHERE so.status = 'Success'
          AND DATE(so.createdDateTime) >= CURRENT_DATE() - INTERVAL 90 DAY
    )
    SELECT
        store,
        category,
        subcategory,
        fdcode,
        default_code,
        SUM(CASE
            WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN -((price * quantity) - (quantity * discount))
            WHEN channelName = 'Kho Lẻ' THEN (price * quantity) - discount
            ELSE (price * quantity) - (discount * quantity)
        END) rvn,
        SUM(CASE WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN -quantity ELSE quantity END) AS qty,
        ROUND(
            CASE
                WHEN DATEDIFF(CURRENT_DATE(), MIN(launch_date)) <= 90
                    THEN SUM(CASE WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN -quantity ELSE quantity END)
                         / DATEDIFF(CURRENT_DATE(), MIN(launch_date)) * 90 / 3
                ELSE SUM(CASE WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN -quantity ELSE quantity END) / 3
            END, 1
        ) AS avg_qty
    FROM main_data
    WHERE store NOT IN ('ECOM', 'ECOM SG')
    GROUP BY store, category, subcategory, default_code, fdcode
"""

with engine.connect() as conn:
    df_sales_kdc = pd.read_sql_query(text(query_sales_90), conn)
print('Finished query sales 90 day KDC/KDS')

df_sales_kdc_ft = df_sales_kdc[df_sales_kdc['category'].isin(category_setup)].copy()
df_sales_kdc_ft['channel'] = df_sales_kdc_ft['store'].apply(map_channel)

# ==========================================
# 5. DOANH SỐ 90 NGÀY – ECOM
# ==========================================
query_ecom = """
    SELECT
        'ECOM' AS store,
        eoi.product_sku fdcode,
        SUM(eoi.quantity) qty,
        SUM(eoi.price * eoi.quantity) AS rvn
    FROM ecommerce_orders eo
    JOIN ecommerce_order_items eoi ON eoi.external_order_id = eo.external_order_id
    JOIN order_source os ON eo.order_source_id = os.id
    WHERE DATE(eo.order_date) >= CURRENT_DATE() - INTERVAL 90 DAY
      AND eo.status NOT IN ('cancelled', 'returned')
      AND UPPER(os.name) NOT IN ('BOXME', 'RETAIL')
      AND eoi.product_sku <> ''
    GROUP BY fdcode
"""

with engine_ecom.connect() as conn:
    df_sales_ecom_raw = pd.read_sql_query(text(query_ecom), conn)
print('Finished query sales 90 day ECOM')

df_sales_ecom_raw = df_sales_ecom_raw[df_sales_ecom_raw['fdcode'] != ''].copy()
df_sales_ecom_raw['fdcode'] = df_sales_ecom_raw['fdcode'].str.upper()

df_sales_ecom = pd.merge(
    df_sales_ecom_raw,
    df_products_template[['fdcode', 'default_code', 'category', 'subcategory', 'launch_date']],
    on='fdcode', how='left'
)
df_sales_ecom = df_sales_ecom[df_sales_ecom['category'].isin(category_setup)].copy()
df_sales_ecom['channel'] = 'ECOM'

# Tính avg_qty ECOM theo launch_date
df_sales_ecom['launch_date'] = pd.to_datetime(df_sales_ecom['launch_date'], errors='coerce')
today_ts = pd.Timestamp.today().normalize()
grp_keys = ['channel', 'fdcode']
total_qty  = df_sales_ecom.groupby(grp_keys)['qty'].transform('sum')
min_launch = df_sales_ecom.groupby(grp_keys)['launch_date'].transform('min')
days_since = ((today_ts - min_launch).dt.days).clip(lower=1)
df_sales_ecom['avg_qty'] = np.round(
    np.where(days_since <= 90, total_qty / days_since * 90 / 3, total_qty / 3), 1
)

# ==========================================
# 6. TỔNG HỢP DOANH SỐ
# ==========================================
cols = ['channel', 'fdcode', 'default_code', 'category', 'subcategory', 'qty', 'rvn', 'avg_qty']
df_sales_all = pd.concat([df_sales_kdc_ft[cols], df_sales_ecom[cols]], ignore_index=True)

# Theo fdcode (cho phân bổ size và tính tồn)
df_sales_fdcode = (
    df_sales_all
    .groupby(['channel', 'fdcode', 'default_code', 'category', 'subcategory'], as_index=False)
    .agg({'qty': 'sum', 'rvn': 'sum', 'avg_qty': 'sum'})
)

# Theo default_code (cho phân bổ từ subcategory xuống mã hàng)
df_sales_code = (
    df_sales_fdcode
    .groupby(['channel', 'subcategory', 'default_code'], as_index=False)
    .agg({'qty': 'sum', 'rvn': 'sum'})
)

# Giá bán trung bình theo channel + default_code (dùng khi target là revenue)
df_avg_price = (
    df_sales_fdcode
    .groupby(['channel', 'default_code'])
    .agg(total_rvn=('rvn', 'sum'), total_qty=('qty', 'sum'))
    .reset_index()
)
df_avg_price['avg_price'] = np.where(
    df_avg_price['total_qty'] > 0,
    df_avg_price['total_rvn'] / df_avg_price['total_qty'],
    np.nan
)

# ==========================================
# 7. CATALOGUE – chỉ đặt hàng mã có trong catalogue
# ==========================================
catalogue_sht  = gs.open_by_key('1ULMcAbIDIh1VQZf66xotuvmTbrSmpXL3v5CqBe7s8ME')
ws_catalogue   = catalogue_sht.worksheet('CATALOGUE')
data_catalogue = ws_catalogue.get_all_values()
df_catalogue   = pd.DataFrame(data_catalogue[6:], columns=data_catalogue[5])
catalogue_codes = set(df_catalogue.iloc[:, 2].str.strip().str.upper().dropna().unique())
print(f'Loaded catalogue: {len(catalogue_codes)} mã SP CHA')

df_sales_fdcode = df_sales_fdcode[df_sales_fdcode['default_code'].str.upper().isin(catalogue_codes)].copy()
df_sales_code   = df_sales_code[df_sales_code['default_code'].str.upper().isin(catalogue_codes)].copy()
df_avg_price    = df_avg_price[df_avg_price['default_code'].str.upper().isin(catalogue_codes)].copy()
print(f'Sau filter catalogue: {df_sales_fdcode["default_code"].nunique()} mã còn lại trong sales')

# ==========================================
# 8. ĐỌC TARGET TỪ UNPIVOT_TARGET
# ==========================================
target_sht      = gs.open_by_key(TARGET_SHEET_KEY)
worksheet_target = target_sht.worksheet(TARGET_SHEET_NAME)
data_target     = worksheet_target.get_all_values()
df_target       = pd.DataFrame(data_target[1:], columns=data_target[0])

df_target['month']   = pd.to_numeric(df_target['month'], errors='coerce')
df_target['year']    = pd.to_numeric(
    df_target['year'].astype(str).str.replace(',', '', regex=False), errors='coerce'
)
df_target['numbers'] = pd.to_numeric(
    df_target['numbers'].astype(str).str.replace(',', '', regex=False), errors='coerce'
)

# DEBUG: xem giá trị thực tế trong sheet
print('[DEBUG] metric values in sheet:', df_target['metric'].str.strip().unique().tolist())
print('[DEBUG] year/month combos:', df_target[['year','month']].drop_duplicates().sort_values(['year','month']).to_string(index=False))

df_target_month = df_target[
    (df_target['month'] == ORDER_MONTH) &
    (df_target['year']  == ORDER_YEAR)  &
    (df_target['metric'].str.strip() == METRIC_FILTER.strip())
].copy()

# Target theo channel + subcategory
df_target_subcat = (
    df_target_month.groupby(['channel', 'subcategory'], as_index=False)['numbers']
    .sum()
    .rename(columns={'numbers': 'target_value'})
)
print(f'[DEBUG] target rows matched: {len(df_target_subcat)} (month={ORDER_MONTH}, year={ORDER_YEAR}, metric="{METRIC_FILTER}")')
if len(df_target_subcat) > 0:
    print(df_target_subcat.to_string(index=False))

# ==========================================
# 8. PHÂN BỔ TARGET XUỐNG TỪNG MẪU (default_code)
#    Tỉ trọng theo SỐ LƯỢNG bán trong subcategory
# ==========================================
df_subcat_total = (
    df_sales_code.groupby(['channel', 'subcategory'], as_index=False)['qty']
    .sum()
    .rename(columns={'qty': 'subcat_total_qty'})
)

df_code_plan = (
    df_sales_code
    .merge(df_subcat_total, on=['channel', 'subcategory'], how='left')
    .merge(df_target_subcat, on=['channel', 'subcategory'], how='left')
    .merge(df_avg_price[['channel', 'default_code', 'avg_price']], on=['channel', 'default_code'], how='left')
)
df_code_plan['target_value'] = df_code_plan['target_value'].fillna(0)

# Tỉ trọng số lượng bán của mã trong subcategory
df_code_plan['product_share'] = np.where(
    df_code_plan['subcat_total_qty'] > 0,
    df_code_plan['qty'] / df_code_plan['subcat_total_qty'],
    0
)
df_code_plan['qty_rank'] = (
    df_code_plan.groupby(['channel', 'subcategory'])['qty']
    .rank(method='first', ascending=False)
    .astype(int)
)
df_code_plan['product_target_value'] = df_code_plan['target_value'] * df_code_plan['product_share']

# Đổi doanh thu → số lượng nếu metric là revenue; nếu là SL thì dùng trực tiếp
if METRIC_FILTER == 'Dthu dự tính':
    df_code_plan['avg_price'] = df_code_plan.groupby('channel')['avg_price'].transform(
        lambda x: x.fillna(x.median())
    )
    df_code_plan['product_target_qty'] = np.where(
        df_code_plan['avg_price'] > 0,
        df_code_plan['product_target_value'] / df_code_plan['avg_price'],
        0
    )
else:
    # 'SL kì vọng' – đơn vị đã là số lượng
    df_code_plan['product_target_qty'] = df_code_plan['product_target_value']

# ==========================================
# 9. PHÂN BỔ XUỐNG TỪNG SIZE (fdcode)
#    Tỉ trọng theo SỐ LƯỢNG bán của size trong mã hàng
# ==========================================
df_size_total = (
    df_sales_fdcode.groupby(['channel', 'default_code'], as_index=False)['qty']
    .sum()
    .rename(columns={'qty': 'code_total_qty'})
)

df_fdcode_plan = (
    df_sales_fdcode[['channel', 'fdcode', 'default_code', 'subcategory', 'category', 'qty', 'avg_qty']]
    .merge(df_size_total, on=['channel', 'default_code'], how='left')
    .merge(df_code_plan[['channel', 'subcategory', 'default_code', 'product_target_qty', 'qty_rank']], on=['channel', 'subcategory', 'default_code'], how='left')
)

df_fdcode_plan['size_share'] = np.where(
    df_fdcode_plan['code_total_qty'] > 0,
    df_fdcode_plan['qty'] / df_fdcode_plan['code_total_qty'],
    0
)
df_fdcode_plan['fdcode_target_qty'] = df_fdcode_plan['product_target_qty'].fillna(0) * df_fdcode_plan['size_share']

print(f'[DEBUG] fdcode_plan rows: {len(df_fdcode_plan)}')
print(f'[DEBUG] fdcode_target_qty > 0: {(df_fdcode_plan["fdcode_target_qty"] > 0).sum()} rows, total={df_fdcode_plan["fdcode_target_qty"].sum():.0f}')

# ==========================================
# 10. TÍNH SỐ LƯỢNG CẦN ĐẶT
# ==========================================
today_date           = date.today()
start_of_order_month = date(ORDER_YEAR, ORDER_MONTH, 1)
days_until_start     = max((start_of_order_month - today_date).days, 0)

# ── Dự báo bán từ hôm nay đến đầu kỳ theo target subcategory ──────────────
# Tập hợp các tháng cần lấy target (tháng hiện tại → ORDER_MONTH - 1)
_fc_months: set = set()
_m, _y = today_date.month, today_date.year
while (_y, _m) < (ORDER_YEAR, ORDER_MONTH):
    _fc_months.add((_y, _m))
    _m += 1
    if _m > 12:
        _m = 1
        _y += 1

_days_in_cur  = calendar.monthrange(today_date.year, today_date.month)[1]
_rem_days_cur = _days_in_cur - today_date.day + 1   # số ngày còn lại trong tháng hiện tại

df_target_fc = df_target[
    df_target[['year', 'month']].apply(
        lambda r: (int(r['year']), int(r['month'])) in _fc_months, axis=1
    ) &
    (df_target['metric'].str.strip() == 'SL kì vọng')
].copy()

# Prorate tháng hiện tại (chỉ tính số ngày còn lại / tổng ngày trong tháng)
df_target_fc['prorate'] = df_target_fc.apply(
    lambda r: _rem_days_cur / _days_in_cur
              if (int(r['year']) == today_date.year and int(r['month']) == today_date.month)
              else 1.0,
    axis=1
)
df_target_fc['fc_numbers'] = df_target_fc['numbers'] * df_target_fc['prorate']

df_forecast_subcat = (
    df_target_fc.groupby(['channel', 'subcategory'], as_index=False)['fc_numbers']
    .sum()
    .rename(columns={'fc_numbers': 'forecast_subcat'})
)
print(f'[DEBUG] forecast months: {sorted(_fc_months)}, forecast_subcat rows: {len(df_forecast_subcat)}')
print('[DEBUG] forecast_subcat ECOM SUKE:')
print(df_forecast_subcat[
    (df_forecast_subcat['channel'] == 'ECOM') & (df_forecast_subcat['subcategory'].str.upper() == 'SUKE')
].to_string(index=False))
print('[DEBUG] avg_qty-based forecast for ECOM SUKE (old method):',
      round(df_sales_fdcode[(df_sales_fdcode['channel']=='ECOM') &
                             (df_sales_fdcode['subcategory'].str.upper()=='SUKE')]['avg_qty'].sum()
            / 30.0 * days_until_start, 1))

# Phân bổ forecast_subcat → default_code theo tỉ trọng DOANH THU trong subcategory
_rvn_subcat_total = (
    df_sales_code.groupby(['channel', 'subcategory'], as_index=False)['rvn']
    .sum()
    .rename(columns={'rvn': 'subcat_total_rvn'})
)
_df_code_fc = (
    df_sales_code[['channel', 'subcategory', 'default_code', 'rvn']]
    .merge(_rvn_subcat_total, on=['channel', 'subcategory'], how='left')
    .merge(df_forecast_subcat, on=['channel', 'subcategory'], how='left')
)
_df_code_fc['rvn_share'] = np.where(
    _df_code_fc['subcat_total_rvn'] > 0,
    _df_code_fc['rvn'] / _df_code_fc['subcat_total_rvn'],
    0
)
_df_code_fc['forecast_code_qty'] = _df_code_fc['forecast_subcat'].fillna(0) * _df_code_fc['rvn_share']

# Gắn tồn kho và đơn hàng đang về
df_fdcode_plan = (
    df_fdcode_plan
    .merge(df_stock_fdcode[['channel', 'fdcode', 'available']], on=['channel', 'fdcode'], how='left')
    .merge(df_order_pen[['channel', 'fdcode', 'order_pen']],    on=['channel', 'fdcode'], how='left')
)
df_fdcode_plan['available'] = df_fdcode_plan['available'].fillna(0)
df_fdcode_plan['order_pen'] = df_fdcode_plan['order_pen'].fillna(0)

# Dự báo bán từ hôm nay đến đầu kỳ: phân bổ forecast_code_qty xuống fdcode theo size_share
df_fdcode_plan = df_fdcode_plan.merge(
    _df_code_fc[['channel', 'subcategory', 'default_code', 'forecast_code_qty']],
    on=['channel', 'subcategory', 'default_code'], how='left'
)
df_fdcode_plan['forecast_until_start'] = (
    df_fdcode_plan['forecast_code_qty'].fillna(0) * df_fdcode_plan['size_share']
)
df_fdcode_plan.drop(columns=['forecast_code_qty'], inplace=True)

# Tồn dự kiến đầu kỳ (không âm)
df_fdcode_plan['stock_at_start'] = (
    df_fdcode_plan['available'] + df_fdcode_plan['order_pen'] - df_fdcode_plan['forecast_until_start']
).clip(lower=0)

# Tồn đầu kỳ cần có để đạt target (hệ số theo kênh)
df_fdcode_plan['safety'] = df_fdcode_plan['channel'].map(SAFETY_FACTOR).fillna(2.0)
df_fdcode_plan['required_stock'] = df_fdcode_plan['fdcode_target_qty'] * df_fdcode_plan['safety']

# ── BƯỚC 1: Xác định subcategory × channel DƯ sớm ────────────────────────────
# Dùng TOÀN BỘ tồn kho (kể cả fdcode không có bán 90 ngày) để tránh bỏ sót hàng tồn
_stock_sub = df_stock_fdcode.groupby(['channel', 'subcategory'], as_index=False)['available'].sum()
_pen_sub = (
    df_order_pen
    .merge(df_products_template[['fdcode', 'subcategory']].drop_duplicates('fdcode'), on='fdcode', how='left')
    .groupby(['channel', 'subcategory'], as_index=False)['order_pen'].sum()
)
_forecast_sub = df_fdcode_plan.groupby(['channel', 'subcategory'], as_index=False)['forecast_until_start'].sum()
_required_sub = df_fdcode_plan.groupby(['channel', 'subcategory'], as_index=False)['required_stock'].sum()

subcat_check = (
    _stock_sub
    .merge(_pen_sub,      on=['channel', 'subcategory'], how='outer')
    .merge(_forecast_sub, on=['channel', 'subcategory'], how='left')
    .merge(_required_sub, on=['channel', 'subcategory'], how='left')
)
subcat_check.fillna({'available': 0, 'order_pen': 0,
                     'forecast_until_start': 0, 'required_stock': 0}, inplace=True)
subcat_check['total_stock'] = (
    subcat_check['available'] + subcat_check['order_pen'] - subcat_check['forecast_until_start']
).clip(lower=0)

# ── Cross-channel netting: dư kênh này bù thiếu kênh kia trong cùng subcategory ──
subcat_check['raw_net']     = subcat_check['total_stock'] - subcat_check['required_stock']
subcat_check['own_surplus'] = subcat_check['raw_net'].clip(lower=0)
subcat_check['own_deficit'] = (-subcat_check['raw_net']).clip(lower=0)

_cross_surplus = subcat_check.groupby('subcategory')['own_surplus'].sum().rename('_cross_surplus')
_total_deficit = subcat_check.groupby('subcategory')['own_deficit'].sum().rename('_total_deficit')
subcat_check = (
    subcat_check
    .merge(_cross_surplus.reset_index(), on='subcategory', how='left')
    .merge(_total_deficit.reset_index(), on='subcategory', how='left')
)

# Deficit thực tế sau khi trừ phần dư cross-channel (chia pro-rata nếu nhiều kênh thiếu)
subcat_check['deficit_subcat'] = np.where(
    subcat_check['_total_deficit'] > 0,
    (subcat_check['own_deficit'] *
     (1 - (subcat_check['_cross_surplus'] / subcat_check['_total_deficit']).clip(upper=1))
    ).clip(lower=0),
    0
).round(0)
subcat_check['surplus'] = subcat_check['deficit_subcat'] <= 0
subcat_check.drop(columns=['raw_net', 'own_surplus', 'own_deficit', '_cross_surplus', '_total_deficit'], inplace=True)

print('[DEBUG] TRENDY 7 surplus check:')
print(subcat_check[subcat_check['subcategory'].str.upper() == 'TRENDY 7'][
    ['channel', 'subcategory', 'available', 'order_pen',
     'forecast_until_start', 'total_stock', 'required_stock', 'surplus']
].to_string(index=False))

# ── BƯỚC 2: Phân bổ deficit xuống fdcode chỉ với subcategory THIẾU ────────────
df_fdcode_plan = df_fdcode_plan.merge(
    subcat_check[['channel', 'subcategory', 'surplus', 'deficit_subcat', 'required_stock']]
    .rename(columns={'required_stock': '_req_subcat'}),
    on=['channel', 'subcategory'], how='left'
)
df_fdcode_plan['qty_need_adj'] = np.where(
    df_fdcode_plan['surplus'].fillna(True) | (df_fdcode_plan['_req_subcat'] <= 0),
    0,
    (df_fdcode_plan['deficit_subcat'] * df_fdcode_plan['required_stock']
     / df_fdcode_plan['_req_subcat']).round(-1)
)
df_fdcode_plan.drop(columns=['surplus', 'deficit_subcat', '_req_subcat'], inplace=True)
# Hệ số tồn hiện tại
df_fdcode_plan['hst'] = np.where(
    df_fdcode_plan['avg_qty'] > 0,
    np.round((df_fdcode_plan['available'] + df_fdcode_plan['order_pen']) / df_fdcode_plan['avg_qty'], 1),
    np.nan
)

# ==========================================
# 11. LỌC VÀ XUẤT KẾT QUẢ
# ==========================================
print(f'[DEBUG] days_until_start: {days_until_start}')
print(f'[DEBUG] qty_need_adj > 0: {(df_fdcode_plan["qty_need_adj"] > 0).sum()} rows trước filter MIN_ORDER_CODE')

# Tổng qty_need_adj mỗi mã (tất cả kênh)
code_total = df_fdcode_plan[df_fdcode_plan['qty_need_adj'] > 0].groupby('default_code')['qty_need_adj'].sum()
below_min  = set(code_total[code_total < MIN_ORDER_CODE].index)
above_min  = set(code_total[code_total >= MIN_ORDER_CODE].index)

# Đánh dấu rank-1 theo từng (channel, subcategory) cụ thể — không dùng tập flat
df_top1_map = (
    df_code_plan[df_code_plan['qty_rank'] == 1][['channel', 'subcategory', 'default_code']]
    .assign(is_top1=True)
)
df_plan_marked = df_fdcode_plan.merge(df_top1_map, on=['channel', 'subcategory', 'default_code'], how='left')
df_plan_marked['is_top1'] = df_plan_marked['is_top1'].fillna(False).astype(bool)

# Một hàng cần rollup khi: mã dưới ngưỡng VÀ không phải rank-1 trong (channel, subcategory) đó
df_plan_marked['should_rollup'] = (
    df_plan_marked['default_code'].isin(below_min) & ~df_plan_marked['is_top1']
)

n_rollup_rows = df_plan_marked['should_rollup'].sum()
print(f'[DEBUG] mã >= {MIN_ORDER_CODE}: {len(above_min)},  dồn & loại (per channel×subcat): {n_rollup_rows} fdcode rows')

# Gom qty cần rollup theo (channel, subcategory) → đích là rank-1 của CÙNG subcategory
df_rollup = (
    df_plan_marked[df_plan_marked['should_rollup'] & (df_plan_marked['qty_need_adj'] > 0)]
    .groupby(['channel', 'subcategory'], as_index=False)['qty_need_adj']
    .sum()
    .rename(columns={'qty_need_adj': 'rollup_qty'})
    .merge(df_top1_map[['channel', 'subcategory', 'default_code']], on=['channel', 'subcategory'], how='left')
)

# Giữ: qty_need_adj > 0 VÀ không thuộc nhóm rollup
df_result = df_plan_marked[
    (df_plan_marked['qty_need_adj'] > 0) & ~df_plan_marked['should_rollup']
].drop(columns=['is_top1', 'should_rollup']).copy()

# Cộng rollup_qty vào mã top1, phân bổ theo tỉ trọng size hiện tại
df_result = df_result.merge(
    df_rollup[['channel', 'subcategory', 'default_code', 'rollup_qty']],
    on=['channel', 'subcategory', 'default_code'], how='left'
)
df_result['rollup_qty'] = df_result['rollup_qty'].fillna(0)

code_qty_sum = df_result.groupby(['channel', 'default_code'])['qty_need_adj'].transform('sum')
df_result['qty_need_adj'] = (
    df_result['qty_need_adj']
    + df_result['rollup_qty'] * np.where(code_qty_sum > 0, df_result['qty_need_adj'] / code_qty_sum, 0)
).round(-1)
df_result.drop(columns=['rollup_qty'], inplace=True)

# Gắn size từ product template
df_result = df_result.merge(
    df_products_template[['fdcode', 'size']].drop_duplicates('fdcode'),
    on='fdcode', how='left'
)

df_output = df_result[[
    'channel', 'fdcode', 'size', 'default_code', 'subcategory', 'category',
    'available', 'avg_qty', 'order_pen', 'hst', 'qty_need_adj'
]]

# LIST_ORDER
worksheet_ord = sht.worksheet(SHEET3)
worksheet_ord.batch_clear(["A5:K"])
gd.set_with_dataframe(worksheet_ord, df_output, row=5, col=1, include_column_header=True)
print('Updated LIST_ORDER')

# REPORT_ORDER: tóm tắt theo channel + subcategory + default_code
df_report = (
    df_fdcode_plan
    .groupby(['channel', 'subcategory', 'default_code'], as_index=False)
    .agg(
        qty_rank        =('qty_rank',              'min'),
        target_qty      =('fdcode_target_qty',    'sum'),
        available       =('available',             'sum'),
        order_pen       =('order_pen',             'sum'),
        forecast_to_start=('forecast_until_start', 'sum'),
        stock_at_start  =('stock_at_start',        'sum'),
        required_stock  =('required_stock',        'sum'),
        avg_qty         =('avg_qty',               'sum'),
        qty_need_adj    =('qty_need_adj',          'sum'),
    )
    .sort_values(['channel', 'subcategory', 'qty_rank'])
)
worksheet_r = sht.worksheet(SHEET11)
worksheet_r.clear()
gd.set_with_dataframe(worksheet_r, df_report)
print('Updated REPORT_ORDER')

# RAW_STOCK
worksheet_stock = sht.worksheet(SHEET7)
worksheet_stock.clear()
gd.set_with_dataframe(worksheet_stock, df_stock_fdcode)
print('Updated RAW_STOCK')

# RAW_PRODUCTS
worksheet_products = sht.worksheet(SHEET8)
worksheet_products.clear()
gd.set_with_dataframe(worksheet_products, df_products_template)
print('Updated RAW_PRODUCTS')

# SEMI_TOP20: tóm tắt theo channel + default_code
df_top20 = (
    df_fdcode_plan
    .groupby(['channel', 'default_code', 'subcategory', 'category'], as_index=False)
    .agg(
        avg_qty        =('avg_qty',            'sum'),
        available      =('available',           'sum'),
        order_pen      =('order_pen',           'sum'),
        stock_at_start =('stock_at_start',      'sum'),
        target_qty     =('fdcode_target_qty',   'sum'),
        qty_need_adj   =('qty_need_adj',        'sum'),
    )
    .sort_values(['channel', 'qty_need_adj'], ascending=[True, False])
)
worksheet_top20 = sht.worksheet(SHEET4)
worksheet_top20.batch_clear(['A1:J'])
gd.set_with_dataframe(worksheet_top20, df_top20)
print('Updated SEMI_TOP20')
