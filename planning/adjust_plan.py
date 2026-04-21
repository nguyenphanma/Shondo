import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import gspread_dataframe as gd
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from core.queries import get_product_template
from core.db import get_engine, get_ecom_engine
from core.sheets import get_client
engine = get_engine()

engine_ecom = get_ecom_engine()

# GOOGLE SHEET
# Đường dẫn tới file JSON (đảm bảo tệp tồn tại)
gs = get_client()
sht = gs.open_by_key('1aFDuIMWZvW2dBIJsUpWgE4XUyIFfW4wFqq4Undhoyfw')
SHEET1 = 'SEMI_DATA'
SHEET2 = 'ORDER_TRACKING'
SHEET3 = 'SALE_CURRENT'
SHEET4 = 'ORDER PLAN'
SHEET5 ='RAW_STOCK'

category = ['SANDALS', 'SNEAKERS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES']

df_products_template = get_product_template(engine)
query_order_tracking = """
    SELECT * FROM stock_pen
"""
# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_order_tracking = pd.read_sql_query(text(query_order_tracking), conn)

worksheet_order = sht.worksheet(SHEET2)
worksheet_order.batch_clear(['A1:K'])
gd.set_with_dataframe(worksheet_order, df_order_tracking)

order_gr = df_order_tracking.groupby(['channel', 'category', 'subcategory', 'default_code']).agg({
    'order_pen':'sum'
}).reset_index()

query_stock = """
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
    WHERE depot_id NOT IN (142410, 101011, 111753, 217633, 220636, 142408, 222877, 125224, 217642, 218091, 111752)
    GROUP BY product_id, depot_id
),

-- 3. Tồn hôm qua
stock_today AS (
    SELECT 
        st.code_nhanh store,
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
        AND pih.depot_id NOT IN (142410, 101011, 111753, 217633, 220636, 142408, 222877, 125224, 217642, 218091, 111752)
        AND DATE(pih.last_updated_at) = CURRENT_DATE() - INTERVAL 1 DAY
),

-- 4. Tồn cập nhật gần nhất nhưng chưa có trong stock_today (dùng LEFT JOIN thay vì NOT IN)
stock_last_change AS (
    SELECT 
        st.code_nhanh store,
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

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_stock = pd.read_sql_query(text(query_stock), conn)

df_stock = pd.merge(df_stock, df_products_template[['fdcode', 'default_code']], on='fdcode', how='left')
df_stock = df_stock[df_stock['category'].isin(category)]

def channel(code):
    if code in ('KHO ECOM', 'ECOM2', 'ECOM', 'KHO BOXME','ECOM SG'):
        return 'ECOM'
    if code in ('KHO SỈ', 'KDS'):
        return 'KDS'
    else:
        return 'KDC'

df_stock['channel'] = df_stock['store'].apply(channel)

df_stock_gr = df_stock.groupby(['channel', 'category', 'subcategory', 'default_code']).agg({
    'available':'sum'
}).reset_index()

df_stock_filter = df_stock_gr[df_stock_gr['category'].isin(category)]

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

combined_filter = combined_df[combined_df['category'].isin(category)]

combined_filter['channel'] = combined_filter['store'].apply(channel)
combined_gr = combined_filter.groupby(['channel', 'category', 'subcategory', 'default_code']).agg({
    'qty':'sum',
    'avg_qty':'sum'
}).reset_index()

# SALE THÁNG HIỆN TẠI
query_sales_current = """
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
filtered_orders AS (
    SELECT 
        so.orderId,
        so.createdDateTime,
        so.channelName,
        so.saleChannel,
        so.channel,
        so.relatedBillId,
        so.type,
        so.status,
        so.description,
        so.customer_id,
        so.shopOrderId,
        so.privateDescription,
        so.depotId,
        so.usedPointsMoney
    FROM sale_order so
    WHERE so.status NOT IN ('Canceled', 'Returning', 'Failed','Returned', 'Aborted', 'CarrierCanceled', 'ConfirmReturned')
      AND so.type != 'Khách trả lại hàng'
      AND NOT (
            so.privateDescription LIKE '%MDX%'
            AND so.saleChannel IN (1, 2, 10, 20, 21, 46)
            AND so.channelName != 'Kho Lẻ'
      )
      AND (
          DATE(so.createdDateTime) BETWEEN DATE_FORMAT(CURDATE(), '%Y-01-01')
                                  AND CURDATE() - INTERVAL 1 DAY
       OR DATE(so.createdDateTime) BETWEEN DATE_FORMAT(CURDATE() - INTERVAL 1 YEAR, '%Y-01-01')
                                  AND DATE_SUB(CURDATE() - INTERVAL 1 DAY, INTERVAL 1 YEAR)
      )
),
base AS (
    SELECT 
        YEAR(fo.createdDateTime)  AS year,
        MONTH(fo.createdDateTime) AS month,

        CASE 
            WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
            WHEN fo.channelName = 'Kho Lẻ' THEN 'KDC'
            WHEN st.code_nhanh = 'KHO SỈ' THEN 'KDS'
            WHEN fo.saleChannel IN (1,2,10,20,21,41,42,43,45,46,47,48,49,50,51) THEN 'ECOM'
            ELSE 'DT KHÁC'
        END AS channel,

        UPPER(
            CASE 
                WHEN sc.sale_channel_name = 'Admin' AND st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
                WHEN fo.channelName = 'KHO LẺ' THEN st.code_nhanh
                WHEN fo.saleChannel IN (2,10) THEN 'WEB'
                WHEN fo.saleChannel IN (1,20,21,46) THEN 'FB/INS/ZL/NB'
                WHEN fo.saleChannel = 41 THEN 'LAZADA'
                WHEN fo.saleChannel = 42 THEN 'SHOPEE'
                WHEN fo.saleChannel = 48 THEN 'TIKTOK'
                ELSE 'KHO LỖI'
            END
        ) AS store,

        CASE WHEN pt.category    IS NULL THEN 'BAGS' ELSE pt.category    END AS category,
        CASE WHEN pt.subcategory IS NULL THEN 'BAGS' ELSE pt.subcategory END AS subcategory,
        CASE WHEN pt.default_code IS NULL THEN ps2.code ELSE pt.default_code END AS default_code,

        CASE 
            WHEN fo.relatedBillId IS NOT NULL AND TRIM(fo.relatedBillId) != '' THEN -soi.quantity 
            ELSE soi.quantity
        END AS qty,

        CASE
            WHEN fo.relatedBillId IS NOT NULL AND TRIM(fo.relatedBillId) != '' 
                THEN -((soi.price * soi.quantity) + (soi.quantity * COALESCE(soi.vat,0)) - (soi.discount * soi.quantity)) 
            WHEN fo.channelName = 'Kho Lẻ' 
                THEN (soi.price * soi.quantity) + (soi.quantity * COALESCE(soi.vat,0)) - soi.discount - fo.usedPointsMoney
            ELSE (soi.price * soi.quantity) + (soi.quantity * COALESCE(soi.vat,0)) - (soi.discount * soi.quantity)
        END AS rvn,

        fo.channelName
    FROM filtered_orders fo
    LEFT JOIN sale_order_items soi ON fo.orderId = soi.sale_order_id
    LEFT JOIN products ps2 ON ps2.external_product_id = soi.external_product_id
    LEFT JOIN pt ON pt.external_product_id = ps2.parent_id
    LEFT JOIN stores st ON st.depot_id_nhanh = fo.depotId
    LEFT JOIN sale_channel sc ON sc.id = fo.channel
)
SELECT
    year,
    month,
    channel,
    category,
    subcategory,
    default_code,
    SUM(qty) AS qty,
    SUM(rvn) AS rvn
FROM base
WHERE
    store NOT IN ('TIKTOK', 'SHOPEE', 'ECOM SG')
    AND NOT (store = 'WEB' AND channelName <> 'Kho Lẻ')
GROUP BY
    year, month, channel, category, subcategory, default_code;

"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_sale_cr = pd.read_sql_query(text(query_sales_current), conn)
print("Query sale current finished.")

df_sale_cr = df_sale_cr[df_sale_cr['category'].isin(category)]

# SALE THÁNG HIỆN TẠI ECOM
query_sales_current_ecom = """
SELECT
	YEAR(eo.order_date) year,
    MONTH(eo.order_date) month,
	"ECOM" as channel,
    eoi.product_sku fdcode,
    eoi.quantity qty,
    eoi.price * eoi.quantity as rvn
FROM ecommerce_orders eo
JOIN ecommerce_order_items eoi ON eoi.external_order_id = eo.external_order_id
JOIN order_source os ON eo.order_source_id = os.id
WHERE
    (
        DATE(eo.order_date) BETWEEN DATE_FORMAT(CURDATE(), '%Y-01-01')
                               AND (CURDATE() - INTERVAL 1 DAY)
        OR
        DATE(eo.order_date) BETWEEN DATE_FORMAT(CURDATE() - INTERVAL 1 YEAR, '%Y-01-01')
                               AND DATE_SUB(CURDATE() - INTERVAL 1 DAY, INTERVAL 1 YEAR)
    )
    AND UPPER(os.name) <> 'BOXME'
    AND eoi.product_sku <>''
    AND eoi.product_sku NOT LIKE '%HOP%'
    AND eoi.product_sku NOT LIKE '%TUIRUT%'
    AND eoi.product_sku <> 'LIMAXCARD'
    AND eo.status <> 'cancelled';
"""

# Lấy dữ liệu bán hàng từ database
with engine_ecom.connect() as conn:
    df_sale_ecom = pd.read_sql_query(text(query_sales_current_ecom), conn)
print("Query sale current ecom finished.")

df_sale_ecom = pd.merge(df_sale_ecom, df_products_template[['fdcode', 'default_code', 'category', 'subcategory']], on='fdcode', how='left')
df_sale_ecom = df_sale_ecom[df_sale_ecom['category'].isin(category)]

df_sale_ecom_gr = df_sale_ecom.groupby(['year', 'month', 'channel', 'category', 'subcategory', 'default_code']).agg({
    'qty':'sum',
    'rvn':'sum'
}).reset_index()

df_sale_total = pd.concat([df_sale_cr, df_sale_ecom_gr], ignore_index=True)

worksheet_cr = sht.worksheet(SHEET3)
worksheet_cr.batch_clear(['A1:H'])
gd.set_with_dataframe(worksheet_cr, df_sale_total)
print("update sale current finished.")

worksheet_stock = sht.worksheet(SHEET5)
worksheet_stock.batch_clear(['A1:E'])
gd.set_with_dataframe(worksheet_stock, df_stock_filter)
print("update stock data finished.")