import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import gspread_dataframe as gd
from datetime import datetime
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from core.sheets import get_client

def show_stock():
    load_dotenv()
    gs = get_client()

    # Mở Google Sheets bằng Google Sheets ID
    sht = gs.open_by_key('1cYjexualwXFh5SQvD9-95FwMNSck4SBAeovKjcYWjW8')
    SHEET1 = 'RAW_STOCK'
    SHEET2 = 'RAW_PRODUCTS'

    

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

    # TEMPLATE
    query_products_template = """ 
            SELECT 
                ps.product_id AS parent_product_id,
                ps.code AS default_code,               -- Mã sản phẩm cha
                CASE 
                    WHEN ps2.code IS NULL THEN ps.code -- Nếu không có mã con thì lấy mã cha
                    ELSE ps2.code                      -- Nếu có mã con thì lấy mã con
                END AS fdcode,
                CASE
                WHEN UPPER(c2.name) IN ('SANDALS', 'KID SANDALS', 'KID SNEAKERS', 'SLIDES', 'SNEAKERS') THEN
                    CASE 
                    WHEN RIGHT(COALESCE(ps2.code, ps.code), 1) = 'W' THEN CONCAT(LEFT(COALESCE(ps2.code, ps.code), 2), 'W')
                    ELSE LEFT(COALESCE(ps2.code, ps.code), 2)
                    END
                ELSE '#'
                END AS size,
                ps.price,
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
                AND ps.code IS NOT NULL
    """
    with engine.connect() as conn:
        df_products_template = pd.read_sql_query(text(query_products_template), conn)

    # STOCK
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
                        WHERE depot_id NOT IN (110819, 111154, 101011, 111753, 125224, 142410, 217633, 222877)
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
                            AND pih.depot_id NOT IN (110819, 111154, 101011, 111753, 125224, 142410, 217633, 222877)
                            AND DATE(pih.last_updated_at) >= CURRENT_DATE() - INTERVAL 1 DAY
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
    with engine.connect() as conn:
        df_stock = pd.read_sql_query(text(query_data), conn,)
    df_stock.replace({'ECOM2': 'ECOM',
                      'KHO BOXME': 'ECOM',
                      'KHO SỈ':'KDS'}, inplace=True)
    df_stock = pd.merge(df_stock, df_products_template[['fdcode', 'default_code']], on='fdcode', how='left')
    worksheet_stock = sht.worksheet(SHEET1)
    worksheet_stock.clear()
    gd.set_with_dataframe(worksheet_stock, df_stock)

    df_products_template = df_products_template[df_products_template['default_code'].isin(df_stock['default_code'])]
    worksheet_products_template = sht.worksheet(SHEET2)
    worksheet_products_template.clear()
    gd.set_with_dataframe(worksheet_products_template, df_products_template)

if __name__ =="__main__":
    show_stock()