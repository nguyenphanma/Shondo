import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
import gspread_dataframe as gd
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from core.db import get_engine
from core.sheets import get_client

def main():

    gs = get_client()

    # Mở Google Sheets bằng Google Sheets ID
    sht = gs.open_by_key('1jIZl6HWBAZF4okz60J9JU-aXLjzFjjN-tzFDLK1JUYE')
    SHEET1 = 'DATA_SEMI'
    SHEET2 = 'RAW_STOCK'
    SHEET3 = 'RAW_PRODUCTS'
    SHEET4 = 'TOP BÁN CHẠY'
    SHEET5 = 'RAW_STOCK_PEN'
    SHEET6 = 'RAW_SALE'
    SHEET7 = 'BOXME'

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
            WHERE depot_id NOT IN (142410, 217633, 125224, 111753, 111752, 101011, 220636, 142408, 222877, 202374, 217642)
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
                AND pih.depot_id NOT IN (142410, 217633, 125224, 111753, 111752, 101011, 220636, 142408, 222877, 202374, 217642)
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

    df_stock['channel'] = df_stock['store'].apply(channel)
    df_stock = pd.merge(df_stock, df_products_template[['fdcode', 'default_code']], on='fdcode', how='left')

    df_stock_filter = df_stock[df_stock['category'].isin(category_setup)]
    df_stock_f = df_stock_filter.groupby(['channel', 'category', 'subcategory','default_code', 'fdcode']).agg({
        'available':'sum'
    }).reset_index()
    
    df_stock_gr = df_stock_filter.groupby(['channel', 'category', 'default_code']).agg({
        'available':'sum'
    }).reset_index()

    query_order_tracking = """
        SELECT * FROM stock_pen
    """
    # Lấy dữ liệu bán hàng từ database
    with engine.connect() as conn:
        df_order_tracking = pd.read_sql_query(text(query_order_tracking), conn)

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
                soi.vat,
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
                    WHEN relatedBillId IS NOT NULL AND TRIM(relatedBillId) != '' THEN  -((price * quantity) + (quantity * COALESCE(vat,0)) - (quantity * discount)) 
                    WHEN channelName ='Kho Lẻ' THEN (price * quantity) + quantity * COALESCE(vat,0) - discount 
            ELSE (price * quantity) + quantity * COALESCE(vat,0) - (discount * quantity) END) rvn,
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

    combined_filter = combined_df[combined_df['category'].isin(category_setup)]
    
    combined_filter['channel'] = combined_filter['store'].apply(channel)
    combined_filter_sale = combined_filter

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
            AND eo.status NOT IN ('cancelled', 'returned', 'Hủy bởi khách hàng')
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
    combined_df_ecom_merge_ft['channel'] = combined_df_ecom_merge_ft['store'].apply(channel)
    combined_df_ecom_merge_fn = combined_df_ecom_merge_ft[['channel', 'fdcode', 'qty', 'rvn', 'default_code', 'category', 'subcategory', 'avg_qty']]
    df_sale_total = pd.concat([combined_filter, combined_df_ecom_merge_fn], ignore_index=True)
    combined_gr = df_sale_total.groupby(['channel', 'category', 'subcategory', 'default_code']).agg({
        'rvn':'sum',
        'qty':'sum',
        'avg_qty':'sum'
    }).reset_index()

    combined_gr = pd.merge(combined_gr, df_stock_gr[['channel', 'default_code', 'available']], on=['channel', 'default_code'], how='left')
    combined_gr = pd.merge(combined_gr, df_order_gr[['channel', 'default_code', 'qty_ord', 'qty_delivered_by_manu', 'order_pen']], on=['channel', 'default_code'], how='left')

    combined_gr.fillna(0, inplace=True)

    df_template_fix = df_products_template.groupby('default_code').agg({
        'launch_date':'min'
    }).reset_index()
    combined_gr = pd.merge(combined_gr, df_template_fix[['default_code', 'launch_date']], on='default_code')

    combined_gr['days_since_launch'] = (datetime.now() - combined_gr['launch_date']).dt.days
    combined_gr['qty_cdeliver'] = round(combined_gr['order_pen'],0)
    combined_gr['stock_af_production'] = round(combined_gr['qty_cdeliver'] + combined_gr['available'],0)
    combined_gr_fix = combined_gr[['channel', 'default_code', 
                                'qty', 'rvn', 'launch_date', 
                                'days_since_launch', 'avg_qty', 
                                'available', 'category', 'qty_ord',
                                'order_pen', 'qty_cdeliver', 'stock_af_production']]
    

    # Tranfer boxme
    query_tranfer_boxme = """
        SELECT 
            "ECOM" channel,
            DATE(im.created_date_time) created_date,
            DATE(im.date) success_date,
            im.external_id,
            imi.product_code fdcode,
            imi.quantity - imi.quantity_lost qty,
            im.status status
        FROM inventory_movement_items imi
        LEFT JOIN inventory_movements im ON im.id = imi.movement_id
        WHERE im.depot_id = 442102
        AND im.status NOT IN ('Cancel', 'Completed', 'Huỷ bởi khách hàng')
    """

    # Lấy dữ liệu bán hàng từ database
    with engine.connect() as conn:
        tranfer_df = pd.read_sql_query(text(query_tranfer_boxme), conn)
    print("query tranfer boxme.")

    tranfer_df_mer = pd.merge(tranfer_df, df_products_template[['fdcode', 'default_code']], on='fdcode', how='left')
    
    worksheet_boxme = sht.worksheet(SHEET7)
    worksheet_boxme.clear()
    print("Cleared BOXME sheet.")
    gd.set_with_dataframe(worksheet_boxme, tranfer_df_mer)
    print("BOXME sheet updated with data.")

    # SEMI
    print("Starting to process SEMI sheet...")
    worksheet_semi = sht.worksheet(SHEET1)
    worksheet_semi.batch_clear(['A1:M'])
    print("Cleared SEMI sheet.")
    gd.set_with_dataframe(worksheet_semi, combined_gr_fix)
    print("SEMI sheet updated with data.")
    
    worksheet_stock = sht.worksheet(SHEET2)
    worksheet_stock.clear()
    gd.set_with_dataframe(worksheet_stock, df_stock_f)

    worksheet_template = sht.worksheet(SHEET3)
    worksheet_template.clear()
    gd.set_with_dataframe(worksheet_template, df_products_template_f)

    worksheet_order = sht.worksheet(SHEET5)
    worksheet_order.clear()
    gd.set_with_dataframe(worksheet_order, df_order_tracking)

    worksheet_sale = sht.worksheet(SHEET6)
    worksheet_sale.clear()
    gd.set_with_dataframe(worksheet_sale, df_sale_total)

    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    # TOP10
    print("Starting to process TOP10 sheet...")
    worksheet_top20 = sht.worksheet(SHEET4)
    worksheet_top20.update('B2', [[current_time]])
    print(f"TOP20 sheet updated with current time: {current_time}")

if __name__=="__main__":
    main()