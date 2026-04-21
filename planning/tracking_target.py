import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import gspread_dataframe as gd
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine, get_ecom_engine
from core.sheets import get_client

# GOOGLE SHEET
# Đường dẫn tới file JSON (đảm bảo tệp tồn tại)
gs = get_client()
sht = gs.open_by_key('1aFDuIMWZvW2dBIJsUpWgE4XUyIFfW4wFqq4Undhoyfw')
SHEET1 = 'data_sale'

print('Finished querying the google sheet')

engine = get_engine()

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

def channel(code):
    if code == 'KHO SỈ':
        return 'KDS'
    if code in ('KHO ECOM', 'ECOM2', 'ECOM','ECOM SG', 'KHO BOXME'):
        return 'ECOM'
    if code == 'KHO SẢN XUẤT':
        return 'KHO SẢN XUẤT'
    if code == 'KHO XUẤT':
        return 'KHO XUẤT'
    if code == 'KHO LỖI':
        return 'KHO LỖI'
    return 'KDC'

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
    WHERE so.status = 'Success'
      AND so.type != 'Khách trả lại hàng'
      AND NOT (
            so.privateDescription LIKE '%MDX%'
            AND so.saleChannel IN (1, 2, 10, 20, 21, 46)
            AND so.channelName != 'Kho Lẻ'
      )
      AND (
          YEAR(so.createdDateTime) >= '2026'
      )
),
base AS (
    SELECT 
        fo.orderId AS order_id,
        DATE(fo.createdDateTime) AS date_order,
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
        CASE WHEN pt.category IS NULL THEN 'BAGS' ELSE pt.category END AS category,
        CASE WHEN pt.subcategory IS NULL THEN 'BAGS' ELSE pt.subcategory END AS subcategory,
        ps2.code AS fdcode,
        CASE WHEN pt.default_code IS NULL THEN ps2.code ELSE pt.default_code END AS default_code,
        CASE 
            WHEN fo.relatedBillId IS NOT NULL AND TRIM(fo.relatedBillId) != '' THEN -soi.quantity 
            ELSE soi.quantity
        END AS qty,
        CASE
            WHEN fo.relatedBillId IS NOT NULL AND TRIM(fo.relatedBillId) != '' 
                THEN -((soi.price * soi.quantity) - (soi.discount * soi.quantity)) 
            WHEN fo.channelName = 'Kho Lẻ' 
                THEN (soi.price * soi.quantity) - soi.discount - fo.usedPointsMoney
            ELSE (soi.price * soi.quantity) - (soi.discount * soi.quantity)
        END AS rvn,
        fo.saleChannel,
        fo.channelName
    FROM filtered_orders fo
    LEFT JOIN sale_order_items soi ON fo.orderId = soi.sale_order_id
    LEFT JOIN products ps2 ON ps2.external_product_id = soi.external_product_id
    LEFT JOIN pt ON pt.external_product_id = ps2.parent_id
    LEFT JOIN stores st ON st.depot_id_nhanh = fo.depotId
    LEFT JOIN customers cus ON cus.external_customer_id = fo.customer_id
    LEFT JOIN sale_channel sc ON sc.id = fo.channel
)
SELECT *
FROM base
WHERE
    -- Loại TIKTOK/SHOPEE luôn
    store NOT IN ('TIKTOK', 'SHOPEE', 'ECOM SG')
    -- Loại WEB nhưng GIỮ KDC: chỉ loại WEB khi không phải "Kho Lẻ"
    AND NOT (store = 'WEB' AND channelName <> 'Kho Lẻ');     
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    current_df_2026 = pd.read_sql_query(text(query_sales_current), conn)
print('Finished query the sale')

# SALSE 2024
# Số lượng đã bán tháng hiện tại
query_sales_2024= f"""
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
        WHERE so.status = 'Success'
        AND so.type != 'Khách trả lại hàng'
        AND NOT (
                so.privateDescription LIKE '%MDX%'
                AND so.saleChannel IN (1, 2, 10, 20, 21, 46)
                AND so.channelName != 'Kho Lẻ'
        )
        AND (
                (
                    YEAR(so.createdDateTime) = 2024
                    AND DATE(so.createdDateTime) <= DATE_SUB(CURDATE(), INTERVAL (YEAR(CURDATE()) - 2024) YEAR)
                )
                OR
                (
                    YEAR(so.createdDateTime) = 2025
                    AND DATE(so.createdDateTime) <= DATE_SUB(CURDATE(), INTERVAL (YEAR(CURDATE()) - 2025) YEAR)
                )
        )
    ),
    base AS (
        SELECT 
            fo.orderId AS order_id,
            DATE(fo.createdDateTime) AS date_order,
            YEAR(fo.createdDateTime) AS order_year,
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
            CASE WHEN pt.category IS NULL THEN 'BAGS' ELSE pt.category END AS category,
            CASE WHEN pt.subcategory IS NULL THEN 'BAGS' ELSE pt.subcategory END AS subcategory,
            ps2.code AS fdcode,
            CASE WHEN pt.default_code IS NULL THEN ps2.code ELSE pt.default_code END AS default_code,
            CASE 
                WHEN fo.relatedBillId IS NOT NULL AND TRIM(fo.relatedBillId) != '' THEN -soi.quantity 
                ELSE soi.quantity
            END AS qty,
            CASE
                WHEN fo.relatedBillId IS NOT NULL AND TRIM(fo.relatedBillId) != '' 
                    THEN -((soi.price * soi.quantity) - (soi.discount * soi.quantity)) 
                WHEN fo.channelName = 'Kho Lẻ' 
                    THEN (soi.price * soi.quantity) - soi.discount - fo.usedPointsMoney
                ELSE (soi.price * soi.quantity) - (soi.discount * soi.quantity)
            END AS rvn,
            fo.saleChannel,
            fo.channelName
        FROM filtered_orders fo
        LEFT JOIN sale_order_items soi ON fo.orderId = soi.sale_order_id
        LEFT JOIN products ps2 ON ps2.external_product_id = soi.external_product_id
        LEFT JOIN pt ON pt.external_product_id = ps2.parent_id
        LEFT JOIN stores st ON st.depot_id_nhanh = fo.depotId
        LEFT JOIN customers cus ON cus.external_customer_id = fo.customer_id
        LEFT JOIN sale_channel sc ON sc.id = fo.channel
    )
    SELECT *
    FROM base
    WHERE (order_year = 2024 OR (order_year = 2025 AND store NOT IN ('TIKTOK', 'SHOPEE')))
    AND NOT (store = 'WEB' AND channelName <> 'Kho Lẻ');
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    current_df_2024 = pd.read_sql_query(text(query_sales_2024), conn)
print('Finished query the sale 2024')
current_df = pd.concat([current_df_2024, current_df_2026], ignore_index=True)
current_df['date_order'] = pd.to_datetime(current_df['date_order'])

current_df['month'] = current_df['date_order'].dt.month
current_df['year']  = current_df['date_order'].dt.year
current_df_gr = current_df.groupby(['channel', 'store', 'category', 
                                    'subcategory', 'default_code', 'month', 'year']).agg({
                                        'qty':'sum',
                                        'rvn':'sum'
                                    }).reset_index()

engine_ecom = get_ecom_engine()

# ECOM 2024
query_sales_ecom_2024 = f"""
SELECT
    DATE(eo.order_date) AS date_ord,
    SUBSTRING_INDEX(eo.order_id, '_', -1) AS order_id_clean,
    'ECOM' AS channel,
    CASE 
        WHEN UPPER(os.name) = 'FACEBOOK' THEN 'FB/INS/ZL/NB'
        WHEN UPPER(os.name) = 'TIKTOKSHOP' THEN 'TIKTOK'
        ELSE UPPER(os.name) 
    END AS store,
    eoi.product_sku AS fdcode,
    eoi.quantity AS qty,
    eoi.price * eoi.quantity AS rvn
FROM ecommerce_orders eo
JOIN ecommerce_order_items eoi 
    ON eoi.external_order_id = eo.external_order_id
JOIN order_source os 
    ON eo.order_source_id = os.id
WHERE (
        (eo.order_date >= '2024-01-01' AND eo.order_date < DATE_ADD('2024-01-01', INTERVAL DAYOFYEAR(CURDATE()) DAY))
        OR
        (eo.order_date >= '2025-01-01' AND eo.order_date < DATE_ADD('2025-01-01', INTERVAL DAYOFYEAR(CURDATE()) DAY))
      )
  AND eoi.product_sku NOT LIKE '%HOP%'
  AND eoi.product_sku NOT LIKE '%TUIRUT%'
  AND eoi.product_sku <> 'LIMAXCARD'
  AND eo.status NOT IN ('cancelled', 'returned');
"""

# Lấy dữ liệu bán hàng từ database
with engine_ecom.connect() as conn:
    combined_df_ecom_2024 = pd.read_sql_query(text(query_sales_ecom_2024), conn)
print("query sale_ecom 2024 day finished.")

# ECOM 2024
query_sales_ecom_2026 = f"""
    SELECT
        DATE(eo.order_date) AS date_ord,
        SUBSTRING_INDEX(eo.order_id, '_', -1) AS order_id_clean,
        'ECOM' AS channel,
        CASE 
            WHEN UPPER(os.name) = 'FACEBOOK' THEN 'FB/INS/ZL/NB'
            WHEN UPPER(os.name) = 'TIKTOKSHOP' THEN 'TIKTOK'
            ELSE UPPER(os.name)
        END AS store,
        eoi.product_sku AS fdcode,
        eoi.quantity AS qty,
        eoi.price * eoi.quantity AS rvn
    FROM ecommerce_orders eo
    JOIN ecommerce_order_items eoi 
        ON eoi.external_order_id = eo.external_order_id
    JOIN order_source os 
        ON eo.order_source_id = os.id
    WHERE eo.order_date >= '2026-01-01'
    AND eo.order_date < CURDATE()
    AND UPPER(os.name) <> 'BOXME'
    AND eoi.product_sku <>''
    AND eoi.product_sku NOT LIKE '%HOP%'
    AND eoi.product_sku NOT LIKE '%TUIRUT%'
    AND eoi.product_sku <> 'LIMAXCARD'
    AND eo.status NOT IN ('cancelled', 'returned');
"""

# Lấy dữ liệu bán hàng từ database
with engine_ecom.connect() as conn:
    combined_df_ecom_2026 = pd.read_sql_query(text(query_sales_ecom_2026), conn)
print("query sale_ecom 2024 day finished.")

combined_df_ecom = pd.concat([combined_df_ecom_2024, combined_df_ecom_2026], ignore_index=True)
combined_df_ecom = combined_df_ecom[combined_df_ecom['fdcode'] != ""]

combined_df_ecom_mer = pd.merge(combined_df_ecom, df_template_fix[['fdcode', 'default_code', 
                                                                           'category', 'subcategory']], on='fdcode', how='left')

combined_df_ecom_mer['date_ord'] = pd.to_datetime(combined_df_ecom_mer['date_ord'])
combined_df_ecom_mer['month'] = combined_df_ecom_mer['date_ord'].dt.month
combined_df_ecom_mer['year']  = combined_df_ecom_mer['date_ord'].dt.year

combined_df_ecom_gr = combined_df_ecom_mer.groupby(['channel', 'store', 'category', 
                                    'subcategory', 'default_code', 'month', 'year']).agg({
                                        'qty':'sum',
                                        'rvn':'sum'
                                    }).reset_index()

df_total = pd.concat([current_df_gr, combined_df_ecom_gr], ignore_index=True)

df_total_filter = df_total[df_total['category'].isin(['SANDALS', 'SNEAKERS', 
                                                      'SLIDES', 'KID SANDALS', 'KID SNEAKERS'])]

worksheet_sale = sht.worksheet(SHEET1)
worksheet_sale.batch_clear(['A1:I'])
gd.set_with_dataframe(worksheet_sale, df_total_filter)