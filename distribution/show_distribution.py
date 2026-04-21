import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import gspread_dataframe as gd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from core.queries import get_product_template
from core.db import get_engine
from core.sheets import get_client

def show_stock():
    gs = get_client()
    engine = get_engine()

    # Mở Google Sheets bằng Google Sheets ID
    sht = gs.open_by_key('1cYjexualwXFh5SQvD9-95FwMNSck4SBAeovKjcYWjW8')
    SHEET1 = 'RAW_STOCK'
    SHEET2 = 'RAW_PRODUCTS'

    # TEMPLATE
    df_products_template = get_product_template(engine)
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