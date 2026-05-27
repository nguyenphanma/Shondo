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
sht = gs.open_by_key('1b6VbnBCZyIB24jiGtsHf-aCHqZzQyOeBNTTs0GP6A68')
SHEET1 = 'RAW_SALE'


category = ['SANDALS', 'SNEAKERS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES']

df_products_template = get_product_template(engine)

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
        so.privateDescription,
        so.depotId,
        so.usedPointsMoney
    FROM sale_order so
    WHERE so.status NOT IN ('Canceled', 'Returning', 'Failed','Returned', 'Aborted', 'CarrierCanceled', 'ConfirmReturned')
      AND so.type != 'Khách trả lại hàng'
      AND NOT (
            so.privateDescription LIKE '%%MDX%%'
            AND so.saleChannel IN (1, 2, 10, 20, 21, 46)
            AND so.channelName != 'Kho Lẻ'
      )
      AND (
          DATE(so.createdDateTime) BETWEEN DATE_FORMAT(CURDATE(), '%%Y-01-01')
                                  AND CURDATE() - INTERVAL 1 DAY
       OR DATE(so.createdDateTime) BETWEEN DATE_FORMAT(CURDATE() - INTERVAL 1 YEAR, '%%Y-01-01')
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
        ps2.code AS fdcode,

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
    fdcode,
    SUM(qty) AS qty,
    SUM(rvn) AS rvn
FROM base
WHERE
    store NOT IN ('TIKTOK', 'SHOPEE', 'ECOM SG')
    AND NOT (store = 'WEB' AND channelName <> 'Kho Lẻ')
GROUP BY
    year, month, channel, category, subcategory, default_code, fdcode;
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
    AND UPPER(os.name) NOT IN ('BOXME', 'RETAIL')
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

df_sale_ecom_gr = df_sale_ecom.groupby(['year', 'month', 'channel', 'category', 'subcategory', 'default_code', 'fdcode']).agg({
    'qty':'sum',
    'rvn':'sum'
}).reset_index()

df_sale_total = pd.concat([df_sale_cr, df_sale_ecom_gr], ignore_index=True)

worksheet_cr = sht.worksheet(SHEET1)
worksheet_cr.clear()
gd.set_with_dataframe(worksheet_cr, df_sale_total)
print("update sale current finished.")