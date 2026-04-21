from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from core.db import get_engine
from core.sheets import get_client
import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import gspread_dataframe as gd
import os

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
        WHERE depot_id NOT IN (142410, 217633, 220636, 142408, 222877)
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
            AND pih.depot_id NOT IN (142410, 217633, 220636, 142408, 222877)
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
    SELECT * FROM stock_last_change;
"""
with engine.connect() as conn:
    df_stock = pd.read_sql_query(text(query_data), conn)
print('Finished querying the inventory')

df_stock = pd.merge(df_stock, df_template_fix[['fdcode', 'default_code']], on='fdcode', how='left')

def channel(code):
    if code == 'KHO SỈ':
        return 'KDS'
    if code in ('ECOM2', 'ECOM HN', 'ECOM SG', 'KHO BOXME'):
        return 'ECOM'
    if code == 'KHO SẢN XUẤT':
        return 'KHO SẢN XUẤT'
    if code == 'KHO XUẤT':
        return 'KHO XUẤT'
    if code == 'KHO LỖI':
        return 'KHO LỖI'
    if code == 'KHO TỔNG':
        return 'KHO TỔNG'
    return 'KDC'
df_stock['channel'] = df_stock['store'].apply(channel)

# Lấy ngày và tháng hiện tại
today = datetime.today()

# Ngày hôm qua
yesterday = today - timedelta(days=1)

# Định dạng ngày hôm qua theo 'yyyymmdd'
yesterday_str = yesterday.strftime('%Y%m%d')

# Tháng của ngày hôm qua
yesterday_month_str = yesterday.strftime('%m')

# Tên file
file_name = f"{yesterday_str}_stock.xlsx"

# Thư mục đầu ra có tháng linh hoạt
base_dir = r"D:\OneDrive\KDA_Trinh Võ\KDA data\BỐC TỒN KHO\DATA TỒN KHO\2025\TK TUẦN"
output_dir = os.path.join(base_dir, f"THÁNG {yesterday_month_str}")

# Tạo thư mục nếu chưa có
os.makedirs(output_dir, exist_ok=True)

# Đường dẫn đầy đủ đến file
full_path = os.path.join(output_dir, file_name)

# Xuất file Excel
df_stock.to_excel(full_path, index=False)

print(f"Đã lưu file tại: {full_path}")