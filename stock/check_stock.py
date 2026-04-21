import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import gspread_dataframe as gd
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from core.sheets import get_client
# GOOGLE SHEET
# Đường dẫn tới file JSON (đảm bảo tệp tồn tại)
gs = get_client()

# Mở Google Sheets bằng Google Sheets ID
sht = gs.open_by_key('1dLmi5h3VpB03NA524BAEvSUwKaxE3W-DhAEhLGgUaEY')

SHEET1 = 'RAW_STOCK'
SHEET2 = 'PRODUCTS'
engine = get_engine()

def channel(code):
    if code in ('KHO ECOM', 'ECOM2', 'ECOM','ECOM SG', 'KHO BOXME'):
        return 'ECOM'
    if code in ('KHO SỈ', 'KDS'):
        return 'KDS'
    else:
        return 'KDC'
category_setup = ['SANDALS', 'SNEAKERS', 'SLIDES', 'KID SANDALS', 'KID SNEAKERS', 'BAGS']
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

df_products_template['launch_date'] = pd.to_datetime(df_products_template['launch_date'])
df_products_template_f = df_products_template[['default_code', 'fdcode', 'size', 'subcategory', 'category']]
df_products_template_f = df_products_template_f[df_products_template_f['category'].isin(category_setup)]

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
        WHERE depot_id NOT IN (142410, 217633, 125224, 111753, 111752, 101011, 220636, 142408, 222877, 202374, 217642, 205232, 142411, 198368)
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
            AND pih.depot_id NOT IN (142410, 217633, 125224, 111753, 111752, 101011, 220636, 142408, 222877, 202374, 217642, 205232, 142411, 198368)
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
print("query stock finished.")

df_stock['channel'] = df_stock['store'].apply(channel)
df_stock = pd.merge(df_stock, df_products_template[['fdcode', 'default_code', 'size']], on='fdcode', how='left')

df_stock_filter = df_stock[df_stock['category'].isin(category_setup)]

worksheet_stock = sht.worksheet(SHEET1)
worksheet_stock.clear()
gd.set_with_dataframe(worksheet_stock, df_stock_filter)
print("update stock finished.")

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

worksheet_pr = sht.worksheet(SHEET2)
worksheet_pr.clear()
gd.set_with_dataframe(worksheet_pr, df_products_template)
print("update stock finished.")

# KDS
df_stock_kds = df_stock_filter[df_stock_filter['store'] != 'KHO SỈ']
# Mở Google Sheets bằng Google Sheets ID
sht_kds = gs.open_by_key('1tpalIrkQJ-WQCsVxhoPhGLvK2Rb3WzALN8H5vUTGgxE')
worksheet_stock_kds = sht_kds.worksheet(SHEET1)
worksheet_stock_kds.clear()
gd.set_with_dataframe(worksheet_stock_kds, df_stock_kds)