import pandas as pd
import numpy as np
from datetime import datetime, date
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

gs = gspread.service_account(Path(os.getenv('ma_shondo_path')) / 'mashondo.json')
sht = gs.open_by_key('11S5h6iESAdd4LVCgdJDN5mPvlBfBvcr-SL4i76NyQ-w')
SHEET1 = 'RAW_SEMI'
SHEET2 = 'ORDER'
SHEET3 = 'LIST_ORDER'
SHEET4 = 'SEMI_TOP20'
SHEET5 = 'RAW_ORDER_NEW'
SHEET6 = 'RAW_SIZE'
SHEET7 = 'RAW_STOCK'
SHEET8 = 'RAW_PRODUCTS'
SHEET9 = 'STOCK_PEN'
SHEET10 ='TARGET_2025'
SHEET11 = 'REPORT_ORDER'


# 🔗 Kết nối MySQL – tạo duy nhất 1 engine dùng xuyên suốt
# Lấy thông tin từ biến môi trường
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = os.getenv("DB_PORT", 3306)

# Tạo engine MySQL
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)

def channel(code):
    if code in ('KHO ECOM', 'ECOM2', 'ECOM','ECOM SG', "KHO BOXME"):
        return 'ECOM'
    if code in ('KHO SỈ', 'KDS'):
        return 'KDS'
    else:
        return 'KDC'

query_product_template = """
    SELECT 
        ps.product_id AS parent_product_id,
        ps.code AS default_code,               -- Mã sản phẩm cha
        ps.category_id,
        -- Mã sản phẩm con (nếu có), nếu không thì dùng mã cha
        COALESCE(ps2.code, ps.code) AS fdcode,

        COALESCE(ps2.price, ps.price) AS price,

        -- Size nếu là giày dép
        CASE
            WHEN UPPER(COALESCE(c2.name, c1.name)) IN ('SANDALS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES', 'SNEAKERS') THEN
                CASE 
                    WHEN RIGHT(COALESCE(ps2.code, ps.code), 1) = 'W' THEN CONCAT(LEFT(COALESCE(ps2.code, ps.code), 2), 'W')
                    ELSE LEFT(COALESCE(ps2.code, ps.code), 2)
                END
            ELSE '#'
        END AS size,

        -- Danh mục con
        COALESCE(c1.name, c2.name) AS subcategory,

        -- Danh mục cha
        COALESCE(c2.name, c1.name) AS category,

        -- Ngày launch từ sản phẩm con nếu có, không thì lấy của sản phẩm cha
        COALESCE(ps2.launch_date, ps.launch_date) AS launch_date,

        -- Phân loại sản phẩm
        CASE
            WHEN COALESCE(ps2.launch_date, ps.launch_date) IS NULL 
                AND UPPER(COALESCE(c2.name, c1.name)) IN ('SANDALS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES', 'SNEAKERS') 
                THEN 'SP CHỜ BÁN'
            WHEN DATEDIFF(CURRENT_DATE(), COALESCE(ps2.launch_date, ps.launch_date)) <= 90 
                THEN 'SP MỚI'
            WHEN UPPER(COALESCE(c2.name, c1.name)) IN ('BAGS', 'ACCESSORIES', 'BRACELETS', 'HATS', 'T-SHIRTS') 
                THEN 'PHỤ KIỆN'
            ELSE 'SP CŨ'
        END AS type_products,
        ps.image
    FROM products ps
    LEFT JOIN products ps2 
        ON ps2.parent_id = ps.external_product_id   -- Ghép sản phẩm con
    LEFT JOIN categories c1 
        ON ps.category_id = c1.external_category_id
    LEFT JOIN categories c2 
        ON c1.parent_id = c2.category_id
    WHERE ps.parent_id IN (-2, -1)                  -- Chỉ lấy sản phẩm cha
    AND ps.product_id IS NOT NULL
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_products_template = pd.read_sql_query(text(query_product_template), conn)
print('Finished query product_template')

df_products_template['launch_date'] = pd.to_datetime(df_products_template['launch_date'])

category_setup = ['SANDALS', 'SNEAKERS', 'SLIDES', 'KID SANDALS', 'KID SNEAKERS']

# Truy vấn ngày tồn kho lớn nhất và dữ liệu tương ứng
query_data = """
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
        SELECT 
            product_id, 
            depot_id, 
            MAX(changed_at) AS max_changed_at
        FROM product_inventory_history
        WHERE depot_id NOT IN (142410, 217633, 125224, 111753, 111752, 101011, 222877)
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
        WHERE 
            pih.available >= 1
            AND pih.depot_id NOT IN (142410, 217633, 125224, 111753, 111752, 101011, 222877)
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
            AND pih.depot_id = st_today.depot_id_nhanh
        LEFT JOIN products AS ps ON ps.product_id = pih.product_id
        LEFT JOIN stores AS st ON st.depot_id_nhanh = pih.depot_id
        LEFT JOIN category_tree ct ON ct.external_category_id = ps.category_id
        WHERE st_today.product_id IS NULL
    )
    SELECT * FROM stock_today
    UNION ALL
    SELECT * FROM stock_last_change
"""
# Lấy dữ liệu tồn kho theo ngày lớn nhất
with engine.connect() as conn:
    df_stock = pd.read_sql_query(text(query_data), conn)
print('Finished query stock')

df_stock['channel'] = df_stock['store'].apply(channel)
df_stock = pd.merge(df_stock, df_products_template[['fdcode', 'default_code']], on='fdcode', how='left')

df_stock_filter = df_stock[df_stock['category'].isin(category_setup)]
df_stock_gr = df_stock_filter.groupby(['channel', 'category', 'default_code']).agg({
    'available':'sum'
}).reset_index()

query_order_tracking = """
    SELECT * FROM stock_pen
"""
# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_order_tracking = pd.read_sql_query(text(query_order_tracking), conn)
print('Finished query order tracking')
worksheet_order_tracking = sht.worksheet(SHEET9)
worksheet_order_tracking.clear()
gd.set_with_dataframe(worksheet_order_tracking, df_order_tracking)
print('Finished update order tracking')

df_order_gr = df_order_tracking.groupby(['channel', 'category', 'subcategory','default_code']).agg({
    'qty_ord':'sum',
    'qty_delivered_by_manu':'sum',
    'order_pen':'sum'
}).reset_index()

# SALE 3 THÁNG GẦN NHẤT
query_sales_90_days = """
    WITH pt AS (
        SELECT 
            ps.external_product_id,
            ps.product_id,
            ps.code AS default_code,
            c1.name AS subcategory,
            c2.name AS category
        FROM categories c1
        LEFT JOIN categories c2
            ON c1.parent_id = c2.category_id
        LEFT JOIN products ps 
            ON ps.category_id = c1.external_category_id AND ps.parent_id = -2
        WHERE c2.name IS NOT NULL
        AND ps.parent_id IS NOT NULL
    ),
    main_data AS (
        SELECT 
            UPPER(CASE 
                WHEN sc.sale_channel_name = 'Admin' AND st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                WHEN so.channelName = 'KHO LẺ' THEN st.code_nhanh
                WHEN so.saleChannel = 1 THEN 'ECOM'
                WHEN so.saleChannel IN (2, 10) THEN 'ECOM'
                WHEN so.saleChannel IN (20, 21, 46) THEN 'ECOM'
                WHEN so.saleChannel = 41 THEN 'ECOM'
                WHEN so.saleChannel = 42 THEN 'ECOM'
                WHEN so.saleChannel = 48 THEN 'ECOM'
            ELSE 'KHO LỖI' END) store,
            so.channelName,
            COALESCE(pt.category, 'BAGS') AS category,
            COALESCE(pt.subcategory, 'BAGS') AS subcategory,
            COALESCE(pt.default_code, ps2.code) AS default_code,
            ps2.code fdcode,
            soi.quantity,
            soi.price,
            soi.discount,
            so.relatedBillId,
            so.saleChannel,
            ps2.launch_date,
            ps2.price price_retail
        FROM sale_order so
        LEFT JOIN sale_order_items soi 
            ON so.orderId = soi.sale_order_id
        LEFT JOIN products ps2
            ON ps2.external_product_id = soi.external_product_id
        LEFT JOIN pt
            ON pt.external_product_id = ps2.parent_id
        LEFT JOIN stores st 
            ON st.depot_id_nhanh = so.depotId
        LEFT JOIN sale_channel sc
            ON sc.id = so.channel
        WHERE 
            so.status = 'Success'
            AND DATE(so.createdDateTime) >= CURRENT_DATE() - INTERVAL 90 DAY
    )

    SELECT 
        store,
        category,
        subcategory,
        fdcode,
        default_code,
        SUM(CASE
                WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN  -((price * quantity) - (quantity * discount)) 
                WHEN channelName ='Kho Lẻ' THEN (price * quantity) - discount 
        ELSE (price * quantity) - (discount * quantity) END) rvn,
        SUM(CASE WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN -quantity ELSE quantity END) AS qty,
        ROUND(
            CASE 
                WHEN DATEDIFF(CURRENT_DATE(), MIN(launch_date)) <= 90 THEN
                    SUM(CASE WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN -quantity ELSE quantity END) / 
                    DATEDIFF(CURRENT_DATE(), MIN(launch_date)) * 90 / 3
                ELSE
                    SUM(CASE WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN -quantity ELSE quantity END) / 3
            END, 1
        ) AS avg_qty
    FROM main_data
    WHERE store NOT IN('ECOM', 'ECOM SG')
    GROUP BY 
        store,
        category,
        subcategory,
        default_code,
        fdcode
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    combined_df = pd.read_sql_query(text(query_sales_90_days), conn)
print("query sale 90 day finished.")

# SALE ECOM 3 THÁNG GẦN NHẤT
# Lấy thông tin từ biến môi trường
host_ecom = os.getenv("DB_HOST_ECOM")
user_ecom = os.getenv("DB_USER_ECOM")
password_ecom = os.getenv("DB_PASSWORD_ECOM")
database_ecom = os.getenv("DB_NAME_ECOM")
port_ecom = os.getenv("DB_PORT_ECOM", 3306)

# Kết nối MySQL
connection_string_ecom = f"mysql+pymysql://{user_ecom}:{password_ecom}@{host_ecom}:{port_ecom}/{database_ecom}"

# Thêm pool_pre_ping=True và connect_args để tăng thời gian chờ
engine_ecom = create_engine(
    connection_string_ecom,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 30}  # tăng timeout từ mặc định (~10s) lên 20s
)


query_sales_90_days_ecom = """
    SELECT
        "ECOM" as store,
        eoi.product_sku fdcode,
        SUM(eoi.quantity) qty,
        SUM(eoi.price * eoi.quantity) as rvn
    FROM ecommerce_orders eo
    JOIN ecommerce_order_items eoi ON eoi.external_order_id = eo.external_order_id
    JOIN order_source os ON eo.order_source_id = os.id
    WHERE
        DATE(eo.order_date) >= CURRENT_DATE() - INTERVAL 90 DAY
        AND eo.status NOT IN ('cancelled', 'returned')
        AND UPPER(os.name) <> 'BOXME'
        AND eoi.product_sku <>''
    GROUP BY store,
             fdcode
"""

# Lấy dữ liệu bán hàng từ database
with engine_ecom.connect() as conn:
    combined_df_ecom = pd.read_sql_query(text(query_sales_90_days_ecom), conn)
print("query sale_ecom 90 day finished.")

combined_df_ecom_ft = combined_df_ecom[combined_df_ecom['fdcode'] != "" ]

combined_df_ecom_ft['fdcode'] = combined_df_ecom_ft['fdcode'].str.upper()
df_products_template['fdcode'] = df_products_template['fdcode'].str.upper()

combined_df_ecom_merge = pd.merge(
    combined_df_ecom_ft,
    df_products_template[['fdcode', 'default_code', 'category', 'subcategory', 'launch_date']],
    on='fdcode',
    how='left'
)

combined_df_ecom_merge_ft = combined_df_ecom_merge[combined_df_ecom_merge['category'].isin(category_setup)]

df = combined_df_ecom_merge_ft.copy()

# Đảm bảo launch_date là datetime (an toàn nếu cột đang là string)
df['launch_date'] = pd.to_datetime(df['launch_date'], errors='coerce')

# Chọn keys nhóm giống logic gộp đầu ra (giữ theo channel + fdcode; 
# nếu bạn muốn chi tiết hơn có thể thêm 'default_code','category','subcategory')
keys = ['store', 'fdcode']

# Tổng qty theo nhóm và launch_date nhỏ nhất theo nhóm
total_qty = df.groupby(keys)['qty'].transform('sum')
min_launch = df.groupby(keys)['launch_date'].transform('min')

# Số ngày kể từ launch đến hôm nay (tránh chia 0)
today = pd.Timestamp.today().normalize()
days_since_launch = (today - min_launch).dt.days.clip(lower=1)

# avg_qty theo công thức:
# nếu days_since_launch <= 90:
#   avg_qty = total_qty / days_since_launch * 90 / 3
# else:
#   avg_qty = total_qty / 3
avg_qty = np.where(
    days_since_launch <= 90,
    total_qty / days_since_launch * 90 / 3,
    total_qty / 3
)

df['avg_qty'] = np.round(avg_qty, 1)

# Gán ngược lại vào dataframe chính (hoặc dùng df ở dưới cho tiếp tục xử lý)
combined_df_ecom_merge_ft = df


# Target

worksheet_target = sht.worksheet(SHEET10)
data_target = worksheet_target.get_all_values()
df_target = pd.DataFrame(data_target[1:], columns=data_target[0])
df_target['month'] = df_target['month'].astype(int)

# =========================
# 0) THAM SỐ THÁNG ĐẶT HÀNG
# =========================
ORDER_MONTH = 4      # ví dụ: tháng 11
ORDER_YEAR  = 2026     # ví dụ: năm 2025

# % TARGET dành cho TOP30 mỗi kênh (bạn điều chỉnh nếu khác nhau theo kênh)
ALLOC_PERCENT = {
    'ECOM': 0.8,
    'KDC' : 0.6,
    'KDS' : 0.8
}

# =========================
# 1) TỔNG HỢP BÁN & TỒN CŨ
# =========================
combined_filter = combined_df[combined_df['category'].isin(category_setup)].copy()
combined_filter['channel'] = combined_filter['store'].apply(channel)
combined_df_ecom_merge_ft['channel'] = combined_df_ecom_merge_ft['store'].apply(channel)

combined_df_ecom_merge_fn = combined_df_ecom_merge_ft[['channel', 'fdcode', 'qty', 'rvn', 'default_code', 'category', 'subcategory', 'avg_qty']]
df_sale_total = pd.concat([combined_filter, combined_df_ecom_merge_ft], ignore_index=True)
combined_gr = (
    df_sale_total.groupby(['channel', 'category', 'subcategory', 'default_code'], as_index=False)
    .agg({'rvn': 'sum', 'qty': 'sum', 'avg_qty': 'sum'})
)

combined_gr = pd.merge(
    combined_gr,
    df_stock_gr[['channel', 'default_code', 'available']],
    on=['channel', 'default_code'],
    how='left'
)

combined_gr = pd.merge(
    combined_gr,
    df_order_gr[['channel', 'default_code', 'qty_ord', 'qty_delivered_by_manu', 'order_pen']],
    on=['channel', 'default_code'],
    how='left'
)

combined_gr.fillna(0, inplace=True)

# Lấy launch_date sớm nhất theo mã
df_template_fix = df_products_template.groupby('default_code', as_index=False).agg({'launch_date': 'min'})
combined_gr = pd.merge(combined_gr, df_template_fix[['default_code', 'launch_date']], on='default_code', how='left')

combined_gr['days_since_launch'] = (datetime.now() - combined_gr['launch_date']).dt.days
combined_gr['qty_cdeliver'] = np.round(combined_gr['order_pen'], 0)
combined_gr['stock_af_production'] = np.round(combined_gr['qty_cdeliver'] + combined_gr['available'], 0)

df_top20 = combined_gr[['channel', 'default_code',
                        'qty', 'rvn', 'launch_date',
                        'days_since_launch', 'avg_qty',
                        'available', 'category', 'qty_ord',
                        'order_pen', 'qty_cdeliver', 'stock_af_production']].copy()

worksheet_top20 = sht.worksheet(SHEET4)
worksheet_top20.batch_clear(['A1:M'])
gd.set_with_dataframe(worksheet_top20, df_top20)
print('Finished update data top20')


worksheet_product_template = sht.worksheet(SHEET8)
worksheet_product_template.clear()
gd.set_with_dataframe(worksheet_product_template, df_products_template)
print('Finished update data product_template')

df_stock_gr2 = (
    df_stock.groupby(['channel', 'fdcode', 'default_code', 'subcategory', 'category'], as_index=False)
    .agg({'available': 'sum'})
)

worksheet_stock = sht.worksheet(SHEET7)
worksheet_stock.clear()
gd.set_with_dataframe(worksheet_stock, df_stock_gr2)
print('Finished update data stock')

# =========================
# 2) ĐỌC ĐƠN ĐẶT & TARGET
# =========================
worksheet_rp = sht.worksheet(SHEET2)
data_order = worksheet_rp.get('E12:S200')
df_order = pd.DataFrame(data_order[1:], columns=data_order[0])


# Chuẩn hoá df_target
df_target['kpi_revenue'] = pd.to_numeric(
    df_target['kpi_revenue'].astype(str).str.replace(',', ''), errors='coerce'
)

df_target_gr = (
    df_target.groupby(['channel', 'month'], as_index=False)['kpi_revenue']
    .sum()
    .rename(columns={'kpi_revenue': 'target_revenue'})
)

# Lọc target của tháng đặt hàng
df_target_month = df_target_gr[df_target_gr['month'].eq(ORDER_MONTH)][['channel', 'target_revenue']].copy()
df_target_month['alloc_pct'] = df_target_month['channel'].map(ALLOC_PERCENT).fillna(0.70)
df_target_month['alloc_budget_top30'] = df_target_month['target_revenue'] * df_target_month['alloc_pct']

# =========================
# 3) DỮ LIỆU MẪU ĐƯỢC ĐẶT
# =========================
# Tỉ trọng đặt hàng mẫu mới: các cột: channel, density
worksheet_density = sht.worksheet(SHEET2)
data_density = worksheet_density.get('A14:B17')
df_density = pd.DataFrame(data_density[1:], columns=data_density[0])

# DS mẫu mới cần đặt: các cột: default_code, size_default, qty_need
worksheet_new_order = sht.worksheet(SHEET5)
data_new_order = worksheet_new_order.get_all_values()
df_new_order = pd.DataFrame(data_new_order[1:], columns=data_new_order[0])

# DS size tỉ trọng: các cột: default_code, size, density_size
worksheet_raw_size = sht.worksheet(SHEET6)
data_raw_size = worksheet_raw_size.get_all_values()
df_raw_size = pd.DataFrame(data_raw_size[1:], columns=data_raw_size[0])

# Lọc đơn hàng hợp lệ & chuẩn hoá doanh thu
df_order_filter = df_order[df_order['MẪU ĐƯỢC ĐẶT'].isin(['Được phép đặt', 'Mẫu mới'])].copy()
df_order_filter['Tổng Doanh Thu'] = pd.to_numeric(
    df_order_filter['Tổng Doanh Thu'].astype(str).str.replace(',', ''), errors='coerce'
)
df_order_filter_renamed = df_order_filter.rename(columns={'MSP': 'default_code', 'Kênh bán': 'channel'})


# =========================
# 4) KẾ HOẠCH TỒN & HST
# =========================
# Merge đúng mã + kênh
df_stock_ft = pd.merge(
    df_stock_gr2,
    df_order_filter_renamed[['default_code', 'channel']].drop_duplicates(),
    on=['default_code', 'channel'],
    how='inner'
)

# avg_qty theo kênh + fdcode
combined_gr2 = (
    df_sale_total.groupby(['channel', 'fdcode'], as_index=False)
    .agg({'avg_qty': 'sum'})
)
df_order_plan = pd.merge(
    df_stock_ft, combined_gr2[['channel', 'fdcode', 'avg_qty']],
    on=['channel', 'fdcode'], how='left'
)



# Hàng đang về
df_order_tracking_gr = (
    df_order_tracking.groupby(['channel', 'fdcode'], as_index=False)
    .agg({'order_pen': 'sum'})
)
df_order_plan = pd.merge(
    df_order_plan, df_order_tracking_gr[['channel', 'fdcode', 'order_pen']],
    on=['channel', 'fdcode'], how='left'
)
df_order_plan['order_pen'] = df_order_plan['order_pen'].fillna(0)

# TOP10 THEO DOANH THU (để chỉnh hệ số HST kênh)
top10_revenue = (
    df_order_filter_renamed.groupby(['channel', 'default_code'], as_index=False)['Tổng Doanh Thu']
    .sum()
)
top10_revenue['rank_in_channel'] = top10_revenue.groupby('channel')['Tổng Doanh Thu'] \
    .rank(method='first', ascending=False)
top10_revenue['is_top10'] = top10_revenue['rank_in_channel'] <= 10

df_order_plan = pd.merge(
    df_order_plan, top10_revenue[['channel', 'default_code', 'is_top10']],
    on=['channel', 'default_code'], how='left'
)
df_order_plan['is_top10'] = df_order_plan['is_top10'].fillna(False)

# Hệ số tồn theo kênh + ngoài TOP10 giảm 0.5
channel_coeff = {'ECOM': 2, 'KDC': 3, 'KDS': 2}
df_order_plan['channel_coeff'] = df_order_plan['channel'].map(channel_coeff).fillna(2.5)

# HST hiện tại
df_order_plan['avg_qty'] = df_order_plan['avg_qty'].fillna(0)
df_order_plan['available'] = df_order_plan['available'].fillna(0)
df_order_plan['hst'] = np.where(
    df_order_plan['avg_qty'] > 0,
    np.round((df_order_plan['available'] + df_order_plan['order_pen']) / df_order_plan['avg_qty'], 1),
    np.nan
)

# =========================================
# 5) PHÂN BỔ TARGET CHO TOP30 THEO DOANH THU
#    + QUY ĐỔI SANG SỐ LƯỢNG (AVG.Price)
# =========================================
# =============================
# 1) Tổng doanh thu theo mã/kênh + xếp hạng
# =============================
rev_by_code = (
    df_order_filter_renamed.groupby(['channel', 'default_code'], as_index=False)['Tổng Doanh Thu']
    .sum()
    .rename(columns={'Tổng Doanh Thu': 'rev'})
)

# Rank theo doanh thu trong từng kênh
rev_by_code['rank_in_channel'] = rev_by_code.groupby('channel')['rev'] \
    .rank(method='first', ascending=False)
rev_by_code['is_top30'] = rev_by_code['rank_in_channel'] <= 30

# =============================
# 2) Phân bổ ngân sách TOP30 theo tỷ trọng doanh thu
# =============================
top30_sum = (
    rev_by_code[rev_by_code['is_top30']]
    .groupby('channel', as_index=False)['rev'].sum()
    .rename(columns={'rev': 'top30_rev_sum'})
)

rev_top30 = (
    rev_by_code.loc[rev_by_code['is_top30'], ['channel', 'default_code', 'rev']]
    .merge(top30_sum, on='channel', how='left')
)
rev_top30['rev_share_in_top30'] = np.where(
    rev_top30['top30_rev_sum'] > 0,
    rev_top30['rev'] / rev_top30['top30_rev_sum'],
    0
)

# Gắn ngân sách TOP30 từ TARGET tháng
rev_top30 = rev_top30.merge(
    df_target_month[['channel', 'alloc_budget_top30']],
    on='channel', how='left'
)
rev_top30['allocated_revenue'] = rev_top30['rev_share_in_top30'] * rev_top30['alloc_budget_top30']

# =============================
# 3) Giá bán TB (AVG.Price) -> số lượng dự kiến từ ngân sách
# =============================
avg_price = (
    df_order[df_order['MẪU ĐƯỢC ĐẶT'].isin(['Được phép đặt', 'Mẫu mới'])]
    .rename(columns={'MSP': 'default_code', 'Kênh bán': 'channel'})
    [['channel', 'default_code', 'AVG.Price']]
    .copy()
)

avg_price['AVG.Price'] = (
    avg_price['AVG.Price']
    .astype(str)                # ép sang chuỗi để xử lý
    .str.replace(',', '', regex=False)  # bỏ dấu phẩy
)

avg_price['AVG.Price'] = pd.to_numeric(avg_price['AVG.Price'], errors='coerce')

avg_price = avg_price.groupby(['channel', 'default_code'], as_index=False)['AVG.Price'].mean()
avg_price = avg_price.rename(columns={'AVG.Price': 'avg_price'})

# Merge giá vào TOP30
rev_top30 = rev_top30.merge(avg_price, on=['channel', 'default_code'], how='left')
rev_top30['avg_price'] = rev_top30['avg_price'].replace([0, np.inf, -np.inf], np.nan)
if rev_top30['avg_price'].isna().any():
    rev_top30['avg_price'] = rev_top30['avg_price'].fillna(
        rev_top30.groupby('channel')['avg_price'].transform('median')
    )

# Số lượng dự kiến từ ngân sách (cấp channel, default_code)
rev_top30['forecast_qty_from_alloc'] = np.where(
    rev_top30['avg_price'] > 0,
    rev_top30['allocated_revenue'] / rev_top30['avg_price'],
    0
)

# Gắn về kế hoạch (mã ngoài TOP30 = 0)
df_order_plan = df_order_plan.merge(
    rev_top30[['channel', 'default_code', 'forecast_qty_from_alloc']],
    on=['channel', 'default_code'], how='left'
)
df_order_plan['forecast_qty_from_alloc'] = df_order_plan['forecast_qty_from_alloc'].fillna(0)

# =============================
# 4) Dự kiến bán đến TRƯỚC ngày 1 của tháng đặt (để làm tỉ trọng)
# =============================

days_remaining_pre_start = 30

# Bán TB/ngày = avg_qty/30
df_order_plan['daily_avg_qty'] = df_order_plan['avg_qty'].fillna(0) / 30.0
df_order_plan['forecast_qty_until_start'] = df_order_plan['daily_avg_qty'] * days_remaining_pre_start

# Phân rã forecast_qty_from_alloc xuống từng dòng theo tỉ trọng forecast_qty_until_start
tot_forecast = (
    df_order_plan.groupby(['channel','default_code'], as_index=False)['forecast_qty_until_start'].sum()
    .rename(columns={'forecast_qty_until_start':'total_forecast_until_start'})
)
alloc_per_code = (
    df_order_plan.groupby(['channel','default_code'], as_index=False)['forecast_qty_from_alloc'].first()
    .rename(columns={'forecast_qty_from_alloc':'alloc_qty_per_code'})
)
df_order_plan = df_order_plan.merge(tot_forecast, on=['channel','default_code'], how='left') \
                             .merge(alloc_per_code, on=['channel','default_code'], how='left')
df_order_plan['forecast_qty_from_alloc'] = np.where(
    df_order_plan['total_forecast_until_start'] > 0,
    df_order_plan['alloc_qty_per_code'] * (df_order_plan['forecast_qty_until_start'] /
                                           df_order_plan['total_forecast_until_start']),
    0
)
df_order_plan.drop(columns=['total_forecast_until_start','alloc_qty_per_code'], inplace=True)

# =============================
# 5) Hệ số tồn theo bậc TOP10/TOP20/TOP30 và theo kênh
# =============================
# Dùng rank đã tính sẵn trong rev_by_code để gắn vào df_order_plan
df_order_plan = df_order_plan.merge(
    rev_by_code[['channel','default_code','rank_in_channel']],
    on=['channel','default_code'], how='left'
)


df_order_plan['channel_coeff'] = df_order_plan['channel'].map(channel_coeff).fillna(2.5)

# Áp dụng bậc:
# - ECOM/KDC: Top1-10: base; 11-20: base-0.5; >=21 (kể cả ngoài Top30/NaN): base-1.0
# - KDS: ngoài Top10: base-0.5
df_order_plan['coeff_adj'] = df_order_plan['channel_coeff']
is_ec_kdc = df_order_plan['channel'].isin(['ECOM','KDC'])

df_order_plan.loc[
    is_ec_kdc & df_order_plan['rank_in_channel'].between(11,20, inclusive='both'),
    'coeff_adj'
] = df_order_plan['channel_coeff'] - 0.5

df_order_plan.loc[
    is_ec_kdc & ((df_order_plan['rank_in_channel'] >= 21) | (df_order_plan['rank_in_channel'].isna())),
    'coeff_adj'
] = df_order_plan['channel_coeff'] - 1.0

is_kds = df_order_plan['channel'].eq('KDS')
df_order_plan['is_top10'] = df_order_plan['rank_in_channel'] <= 10
df_order_plan.loc[is_kds & (~df_order_plan['is_top10'].fillna(False)), 'coeff_adj'] = \
    df_order_plan['channel_coeff'] - 0.5

# Tồn kho cần có để cover
df_order_plan['required_stock_for_cover'] = round(df_order_plan['coeff_adj'] * df_order_plan['forecast_qty_from_alloc'].fillna(0),0)

df_order_plan['planned_production'] = (
    df_order_plan['required_stock_for_cover'] - df_order_plan['available']
).clip(lower=0)

# Giữ lại order_pen gốc để kiểm tra
df_order_plan['order_pen_original'] = df_order_plan['order_pen']

# Ưu tiên kênh
priority_map = {'ECOM': 1, 'KDC': 2, 'KDS': 3}
df_order_plan['priority'] = df_order_plan['channel'].map(priority_map).fillna(99)

def allocate_cross_channel(group):
    group = group.copy()
    group = group.sort_values(['priority', 'planned_production'], ascending=[True, False])

    total_supply = group['order_pen'].sum()

    allocations = []
    for _, row in group.iterrows():
        alloc = min(row['planned_production'], total_supply)
        allocations.append(alloc)
        total_supply -= alloc

    group['final_production'] = allocations
    group['shortage'] = (group['planned_production'] - group['final_production']).clip(lower=0)
    group['production_status'] = np.where(group['shortage'] > 0, 'LACK', 'OK')
    group['unused_order_pen_sku'] = total_supply

    return group

df_order_plan = (
    df_order_plan
    .groupby('fdcode', group_keys=False)
    .apply(allocate_cross_channel)
    .reset_index(drop=True)
)


df_order_plan = df_order_plan[
    df_order_plan.groupby('default_code')['final_production'].transform('sum') >= 300
]

df_order_plan = pd.merge(df_order_plan, df_products_template[['fdcode', 'size']], on='fdcode', how='left')
df_order_plan_ft = df_order_plan[['channel', 'fdcode', 'size', 'default_code', 'subcategory', 'category', 'available', 'order_pen', 'final_production', 'shortage']]
worksheet_pro = sht.worksheet(SHEET3)
worksheet_pro.clear()
gd.set_with_dataframe(worksheet_pro, df_order_plan_ft)