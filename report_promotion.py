import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd
import os
from dotenv import load_dotenv

gs = gspread.service_account(r'd:\OneDrive\KDA_Trinh Võ\KDA data\PYTHON_OPERATION\ma_shondo\mashondo.json')
sht = gs.open_by_key('1WCyB2XBaOH4Kn1DAY5J0lZnu7ar8a-dEpezoRi8Qa0M')
SHEET1 = 'RAW_SALE'
SHEET2 = 'RAW_STOCK'
SHEET3 = 'REPORT'
print('Finished querying the google sheet')


worksheet_report = sht.worksheet(SHEET3)
date_start_raw = worksheet_report.get_values('B8')[0][0].strip()
date_end_raw = worksheet_report.get_values('B9')[0][0].strip()

# Chuyển về định dạng chuẩn 'YYYY-MM-DD'
date_start = datetime.strptime(date_start_raw, '%Y/%m/%d').strftime('%Y-%m-%d')
date_end = datetime.strptime(date_end_raw, '%Y/%m/%d').strftime('%Y-%m-%d')
 

# Kết nối MySQL
load_dotenv()

# 🔗 Kết nối MySQL – tạo duy nhất 1 engine dùng xuyên suốt
# Lấy thông tin từ biến môi trường
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = os.getenv("DB_PORT", 3306)

# Kết nối MySQL
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)


query_products_template = """
    SELECT 
        ps.product_id AS parent_product_id,
        ps.code AS default_code,               -- Mã sản phẩm cha
        CASE 
            WHEN ps2.code IS NULL THEN ps.code -- Nếu không có mã con thì lấy mã cha
            ELSE ps2.code                      -- Nếu có mã con thì lấy mã con
        END AS fdcode,
        ps.price,
        CASE
        WHEN UPPER(c2.name) IN ('SANDALS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES', 'SNEAKERS') THEN
            CASE 
            WHEN RIGHT(COALESCE(ps2.code, ps.code), 1) = 'W' THEN CONCAT(LEFT(COALESCE(ps2.code, ps.code), 2), 'W')
            ELSE LEFT(COALESCE(ps2.code, ps.code), 2)
            END
        ELSE '#'
        END AS size,
        CASE 
            WHEN c1.name IS NULL THEN c2.name 
            ELSE c1.name 
        END AS subcategory,
        
        CASE 
            WHEN c2.name IS NULL THEN c1.name
            ELSE c2.name 
        END AS category,
        ps2.launch_date,
        CASE
            WHEN ps2.launch_date IS NULL 
                AND UPPER(c2.name) IN ('SANDALS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES', 'SNEAKERS') THEN 'SP CHỜ BÁN'
            WHEN DATEDIFF(CURRENT_DATE(), ps2.launch_date) <= 90 THEN 'SP MỚI'
            WHEN UPPER(c2.name) IN ('BAGS', 'ACCESSORIES', 'BRACELETS', 'HATS', 'T-SHIRTS') THEN 'PHỤ KIỆN'
            ELSE 'SP CŨ'
        END AS type_products
    FROM categories c1
    LEFT JOIN categories c2
        ON c1.parent_id = c2.category_id
    LEFT JOIN products ps
        ON ps.category_id = c1.external_category_id 
    AND ps.parent_id IN (-2, -1)                   -- Chỉ lấy SP cha
    LEFT JOIN products ps2 
        ON ps2.parent_id = ps.external_product_id     -- Ghép với SP con
    WHERE ps.product_id IS NOT NULL
        AND ps.code IS NOT NULL;
"""
# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_template_fix = pd.read_sql_query(text(query_products_template), conn)
print('Finished querying the template')

query_data = """
        -- 1. CTE: Danh mục ngành hàng cha - con
        WITH category_tree AS (
            SELECT 
                c1.external_category_id,
                c1.name,
                c2.name AS parent_name
            FROM categories c1
            LEFT JOIN categories c2 ON c1.parent_id = c2.category_id
            WHERE c2.name IS NOT NULL
        ),

        -- 2. Lần thay đổi gần nhất cho mỗi mã sản phẩm theo kho
        max_change AS (
            SELECT 
                product_id, 
                depot_id, 
                MAX(changed_at) AS max_changed_at
            FROM product_inventory_history
            WHERE depot_id NOT IN (142410, 111752, 101011, 125224, 111753, 111754, 100906, 217633, 220636, 142408, 222877, 217642, 202374, 218091)
            GROUP BY product_id, depot_id
        ),

        -- 3. Tồn hôm qua
        stock_today AS (
            SELECT 
                st.code_nhanh,
                pih.depot_id AS depot_id_nhanh,
                pih.product_id,
                ps.code AS fdcode,
                COALESCE(ct.name, 'BAGS') AS subcategory,
                COALESCE(ct.parent_name, 'BAGS') AS category,
                pih.available
            FROM product_inventories AS pih
            LEFT JOIN stores AS st ON st.depot_id_nhanh = pih.depot_id
            LEFT JOIN products AS ps ON ps.product_id = pih.product_id
            LEFT JOIN category_tree ct ON ct.external_category_id = ps.category_id
            WHERE 
                pih.available >= 1
                AND pih.depot_id NOT IN (142410, 111752, 101011, 125224, 111753, 111754, 100906, 217633, 220636, 142408, 222877, 217642, 202374, 218091)
                AND DATE(pih.last_updated_at) = CURRENT_DATE() - INTERVAL 1 DAY
        ),

        -- 4. Tồn cập nhật gần nhất nhưng chưa có trong stock_today (dùng LEFT JOIN thay vì NOT IN)
        stock_last_change AS (
            SELECT 
                st.code_nhanh,
                pih.depot_id AS depot_id_nhanh,
                pih.product_id,
                ps.code AS fdcode,
                COALESCE(ct.name, 'BAGS') AS subcategory,
                COALESCE(ct.parent_name, 'BAGS') AS category,
                pih.available
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
            WHERE 
                pih.available >= 1
                AND st_today.product_id IS NULL
        )

        -- 5. Gộp kết quả cuối cùng
        SELECT * FROM stock_today
        UNION ALL
        SELECT * FROM stock_last_change;
"""
with engine.connect() as conn:
    df_stock = pd.read_sql_query(text(query_data), conn)
print('Finished querying the inventory')

df_stock = pd.merge(df_stock, df_template_fix[['fdcode', 'default_code']], on='fdcode', how='left')

def channel(code):
    if code == 'KHO SỈ':
        return 'KDS'
    if code in ('KHO ECOM', 'ECOM2', 'ECOM','ECOM SG', 'ECOM HN', 'KHO BOXME'):
        return 'ECOM'
    if code == 'KHO SẢN XUẤT':
        return 'KHO SẢN XUẤT'
    if code == 'KHO XUẤT':
        return 'KHO XUẤT'
    if code == 'KHO LỖI':
        return 'KHO LỖI'
    return 'KDC'
df_stock['channel'] = df_stock['code_nhanh'].apply(channel)

df_stock_fn = df_stock[['channel', 'code_nhanh', 'default_code', 'subcategory', 'category', 'available']]
df_stock_fn.rename(columns={
    'code_nhanh':'store'
},inplace=True)

df_stock_gr = df_stock_fn.groupby(['channel', 'store', 'default_code', 'subcategory', 'category']).agg({
    'available':'sum'
}).reset_index()

df_stock_gr['store'] = df_stock_gr['store'].str.replace('ECOM2', 'ECOM')

# Số lượng đã bán tháng hiện tại
query_sales_current = f"""
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
            )

            SELECT 
                so.orderId AS order_id,
                DATE(so.createdDateTime) AS date_order,

                -- Kênh tổng
                CASE 
                    WHEN so.channelName = 'Kho Lẻ' THEN 'KDC'
                    WHEN st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                    WHEN so.saleChannel IN (1, 2, 10, 20, 21, 41, 42, 43, 45, 46, 47, 48, 49, 50, 51) THEN 'ECOM'
                    ELSE 'DT KHÁC'
                END AS channel,

                -- Cửa hàng cụ thể
                UPPER(CASE 
                        WHEN sc.sale_channel_name = 'Admin' AND st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                        WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
                        WHEN so.channelName = 'KHO LẺ' THEN st.code_nhanh
                        WHEN so.saleChannel IN (2, 10) THEN 'WEB'
                        WHEN so.saleChannel IN (1, 20, 21, 46) THEN 'FB/INS/ZL/NB'
                        WHEN so.saleChannel = 41 THEN 'LAZADA'
                        WHEN so.saleChannel = 42 THEN 'SHOPEE'
                        WHEN so.saleChannel = 48 THEN 'TIKTOK'
                ELSE 'KHO LỖI' END) store,
                COALESCE(pt.category, 'BAGS') AS category,
                COALESCE(pt.subcategory, 'BAGS') AS subcategory,
                COALESCE(pt.default_code, ps2.code) AS default_code,

                -- Tổng số lượng
                SUM(CASE 
                    WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != '' THEN -soi.quantity
                    ELSE soi.quantity
                END) AS qty,

                -- Tổng doanh thu thực nhận
                SUM(
                    CASE
                        WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != '' THEN  -((soi.price * soi.quantity) - (soi.quantity * soi.discount)) 
                        WHEN so.channelName ='Kho Lẻ' THEN (soi.price * soi.quantity) - soi.discount 
                        ELSE (soi.price * soi.quantity) - (soi.discount * soi.quantity) END
                ) AS rvn,

                ps2.price,

                -- Trạng thái giá
                CASE
                    WHEN SUM(
                            CASE
                                WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != '' THEN  -((soi.price * soi.quantity) - (soi.quantity * soi.discount)) 
                                WHEN so.channelName ='Kho Lẻ' THEN (soi.price * soi.quantity) - soi.discount 
                            ELSE (soi.price * soi.quantity) - (soi.discount * soi.quantity) END
                        ) < SUM(ps2.price * soi.quantity)
                    THEN 'Giảm giá'
                    ELSE 'Nguyên giá'
                END AS price_status

            FROM sale_order so
            LEFT JOIN sale_order_items soi ON so.orderId = soi.sale_order_id
            LEFT JOIN products ps2 ON ps2.external_product_id = soi.external_product_id
            LEFT JOIN pt ON pt.external_product_id = ps2.parent_id
            LEFT JOIN stores st ON st.depot_id_nhanh = so.depotId
            LEFT JOIN customers cus ON cus.external_customer_id = so.customer_id
            LEFT JOIN sale_channel sc ON sc.id = so.channel
            WHERE 
                st.code_nhanh != 'KHO SỈ'
                AND so.status NOT IN ('Canceled', 'Returning', 'Failed', 'Returned', 'Aborted', 'CarrierCanceled', 'ConfirmReturned')
				AND (
					(DATE(so.createdDateTime) BETWEEN DATE('{date_start}') AND DATE('{date_end}'))
					OR
					(DATE(so.createdDateTime) BETWEEN DATE('{date_start}') - INTERVAL 365 DAY 
												  AND DATE('{date_end}') - INTERVAL 365 DAY)
				)

            GROUP BY 
                so.orderId,
                DATE(so.createdDateTime),
                CASE 
                    WHEN so.channelName = 'Kho Lẻ' THEN 'KDC'
                    WHEN st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                    WHEN so.saleChannel IN (1, 2, 10, 20, 21, 41, 42, 43, 45, 46, 47, 48, 49, 50, 51) THEN 'ECOM'
                    ELSE 'DT KHÁC'
                END,
                UPPER(CASE 
                    WHEN sc.sale_channel_name = 'Admin' AND st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                    WHEN so.channelName = 'KHO LẺ' THEN st.code_nhanh
                    WHEN so.saleChannel = 1 THEN 'ADMIN'
                    WHEN so.saleChannel IN (2, 10) THEN 'WEB'
                    WHEN so.saleChannel IN (20, 21, 46) THEN 'FB/INS/ZL/NB'
                    WHEN so.saleChannel = 41 THEN 'LAZADA'
                    WHEN so.saleChannel = 42 THEN 'SHOPEE'
                    WHEN so.saleChannel = 48 THEN 'TIKTOK'
                    ELSE 'KHO LỖI'
                END),
                COALESCE(pt.category, 'BAGS'),
                COALESCE(pt.subcategory, 'BAGS'),
                COALESCE(pt.default_code, ps2.code),
                ps2.price
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    current_df = pd.read_sql_query(text(query_sales_current), conn)
print('Finished query the sale')

# SALE ECOM
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


query_sales_ecom = f"""
SELECT
        DATE(eo.order_date) date_order,
        SUBSTRING_INDEX(eo.order_id, '_', -1) AS order_id,
        "ECOM" as channel,
        CASE 
            WHEN UPPER(os.name) = 'FACEBOOK' THEN 'FB/INS/ZL/NB'
            WHEN UPPER(os.name) = 'TIKTOKSHOP' THEN 'TIKTOK'
            ELSE UPPER(os.name) 
        END AS store,
        eoi.product_sku fdcode,
        SUM(eoi.quantity) qty,
        SUM(eoi.price * eoi.quantity) as rvn,
        CASE 
            WHEN eoi.price * eoi.quantity + eoi.seller_discount + eoi.voucher_seller_discount > SUM(eoi.price * eoi.quantity) THEN 'Giảm giá'
            ELSE 'Nguyên giá' 
        END price_status
    FROM ecommerce_orders eo
    JOIN ecommerce_order_items eoi ON eoi.external_order_id = eo.external_order_id
    JOIN order_source os ON eo.order_source_id = os.id
    WHERE
        DATE(eo.order_date) BETWEEN DATE('{date_start}') AND DATE('{date_end}')
        AND eo.status NOT IN ('cancelled', 'returned')
    GROUP BY channel,
             store,
             fdcode,
             date_order,
             order_id
"""

# Lấy dữ liệu bán hàng từ database
with engine_ecom.connect() as conn:
    combined_df_ecom = pd.read_sql_query(text(query_sales_ecom), conn)
print("query sale_ecom 90 day finished.")
combined_df_ecom = combined_df_ecom[combined_df_ecom['fdcode'] != ""]
combined_df_ecom_mer = pd.merge(combined_df_ecom, df_template_fix[['fdcode', 'default_code', 
                                                                           'category', 'subcategory']], on='fdcode', how='left')
combined_df_ecom_filter = combined_df_ecom_mer[['order_id', 'date_order', 'channel', 'store',
                                                'category', 'subcategory', 'default_code', 'qty', 'rvn', 'price_status']]
df_total = pd.concat([current_df, combined_df_ecom_filter], ignore_index=True)
#df_total = df_total[df_total['default_code'].isin(['SUK0060', 'SUK6000', 'GIM0001', 'F109595'])]
worksheet_ss = sht.worksheet(SHEET1)
worksheet_ss.batch_clear(['A1:K'])
gd.set_with_dataframe(worksheet_ss, df_total)
print("RAW_SALE sheet updated with data.")

worksheet_stock = sht.worksheet(SHEET2)
worksheet_stock.batch_clear(['A1:F'])
print("Cleared Stock sheet.")
gd.set_with_dataframe(worksheet_stock, df_stock_gr)
print("Stock sheet updated with data.")


current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
worksheet_report.update('B2', [[current_time]])
print(f"Report sheet updated with current time: {current_time}")