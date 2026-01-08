import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from collections import defaultdict
from dotenv import load_dotenv


load_dotenv()

# 🔗 Kết nối MySQL – tạo duy nhất 1 engine dùng xuyên suốt
# Lấy thông tin từ biến môi trường
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = os.getenv("DB_PORT", 3306)

connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(
    connection_string,
    pool_size=5,
    max_overflow=0,
    pool_recycle=1800,
    pool_pre_ping=True
)

# Tham số mặc định
MOH = 2.5
df_merge = pd.DataFrame()
df_warehouse = pd.DataFrame()
df_process_warehouse = pd.DataFrame()

query_products_template = """
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
    df_template = pd.read_sql_query(text(query_products_template), conn)

# Thiết lập cột và lọc dữ liệu
set_products = set(df_template['fdcode'])

df_template_process_1 = df_template[df_template['subcategory'].isin(['BẢO HÀNH SỬA CHỮA', 'QUÀ TẶNG', 'T-SHIRTS', 'CCDC', 'RASTACLAT', 'KEY RING'])]
df_template_process_2 = df_template[df_template['default_code'].isin(['FXHOPQUANHO', 'QHKHAC', 'HOPBATM01', 'HOPBAD1', 'TUISI52'])]

df_template_process = pd.concat([df_template_process_1, df_template_process_2]).drop_duplicates()
set_products_process = set(df_template_process['fdcode'])
# 🧩 Hàm khởi tạo dữ liệu tồn kho và sức bán
def initialize_data():
    global df_warehouse, df_merge, df_store, combined_df, df_process_warehouse, df_warehouse_ecom

    # Truy vấn dữ liệu tồn kho theo ngày lớn nhất
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
                    WHERE depot_id NOT IN (110819, 111154, 101011, 111753, 125224, 142410, 217633, 217642, 110826, 111155, 222877, 218091)
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
                        AND pih.depot_id NOT IN (110819, 111154, 101011, 111753, 125224, 142410, 217633, 217642, 110826, 111155, 222877, 218091)
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

    with engine.connect() as conn:
        df_stock = pd.read_sql_query(text(query_data), conn)
    df_stock.replace({
                    'KHO BOXME': 'ECOM',
                    'ECOM SG': 'ECOM_SG',
                    'KHO SỈ':'KDS'}, inplace=True)


    # Tách tồn kho theo kho
    df_store = df_stock[~df_stock['store'].isin(['KHO TỔNG', 'ECOM_SG'])]  # ✅ ECOM_SG không phải store nhận hàng
    df_warehouse = df_stock[df_stock['store'] == 'KHO TỔNG']
    df_warehouse_ecom = df_stock[df_stock['store'] == 'ECOM_SG']          # ✅ nguồn cấp cho ECOM
    df_process_warehouse = df_stock[df_stock['store'] == 'KHO GIA CÔNG']
    df_process_warehouse = df_process_warehouse[~df_process_warehouse['fdcode'].isin(set_products_process)]

    # Truy vấn dữ liệu bán hàng 90 ngày gần nhất
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
                    ON ps2.product_id = soi.product_id
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
            WHERE store NOT IN('KHO XUẤT', 'ECOM')
            GROUP BY 
                store,
                category,
                subcategory,
                default_code,
                fdcode
    """

    with engine.connect() as conn:
        combined_df = pd.read_sql_query(text(query_sales_90_days), conn)

    # SALE ECOM 90 DAYS
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
            SUM(eoi.quantity) qty
        FROM ecommerce_orders eo
        JOIN ecommerce_order_items eoi ON eoi.external_order_id = eo.external_order_id
        JOIN order_source os ON eo.order_source_id = os.id
        WHERE
            DATE(eo.order_date) >= CURRENT_DATE() - INTERVAL 90 DAY
            AND eo.status NOT IN ('cancelled', 'returned')
        GROUP BY store,
                fdcode
    """

    # Lấy dữ liệu bán hàng từ database
    with engine_ecom.connect() as conn:
        combined_df_ecom = pd.read_sql_query(text(query_sales_90_days_ecom), conn)
    print("query sale_ecom 90 day finished.")

    combined_df_ecom_ft = combined_df_ecom[combined_df_ecom['fdcode'] != "" ]

    combined_df_ecom_ft['fdcode'] = combined_df_ecom_ft['fdcode'].str.upper()
    df_template['fdcode'] = df_template['fdcode'].str.upper()

    combined_df_ecom_merge = pd.merge(
        combined_df_ecom_ft,
        df_template[['fdcode', 'default_code', 'category', 'subcategory', 'launch_date']],
        on='fdcode',
        how='left'
    ) 

    df = combined_df_ecom_merge.copy()

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
    combined_df_ecom_merge = df
    df_sale_total = pd.concat([combined_df, combined_df_ecom_merge], ignore_index=True)
    # 6. Tính toán sức bán trung bình
    # KHAI BÁO SẢN PHẨM KHÔNG LUÂN CHUYỂN
    subcategory_none = ['KEY RING', 'UPPER', 'RASTACLAT']
    default_code_non = [
        # =========================
        # LOẠI TRỪ CHUNG
        # =========================
        'AOMUA2',
        'HOPF7SHONDO1',
        'FXKHAYDUNGQUA',
        'HOPGIAYF7M2',
        'HOPGIAYF7D1',
        'HOPGIAYF7D2',
        'HOPGIAYF7M1',
        'HOPGIAYS53',
        'HOPKID52',
        'HOPSINHNHAT62',
        'HOPQUAIF8',
        'HOPAOT',
        'HOPGIAYM54',
        'COMBO FX',
        'COMBOFX',
        'BANGKEO1',
        'PLA2595',
        'PLA0001',
        'PLA1113',
        'F7R2022',
        'TRE2995',
        'SND0002',
        'SND2525',
        'S2C0060',
        'F6S1071',
        'F6S1011',
        'F6S1031',
        'F6S1043',
        'F6S2580',
        'SC29',
        'SC39',
        'SC49',
        'TUIRAS1',
        'BANGKEO1',

        # =========================
        # NGỪNG BÁN
        # =========================
        'AOTH05',
        'CHN0001',
        'F6S0045',
        'F6S3540',
        'F7N1010',
        'F7N7272',
        'F7R1012',
        'F7R1212',
        'F7R3235',
        'F7R7272',
        'F8M0010',
        'F8M0323',
        'F8M1158',
        'FLR1111',
        'FLR2525',
        'FLR7777',
        'LIT0101',
        'LIT2211',
        'LIT2310',
        'LIT7070',
        'LIT7090',
        'PLA1010',
        'PLA9525',
        'S2C0141',
        'S2M2323',
        'S2M3330',
        'SND0013',
        'SND0070',
        'SND0104',
        'SND0110',
        'SND1212',
        'SND2525',
        'TAN2510',
        'TRE003',
        'TRE1115',
        'TRE2022',
        'TRE2529',
        'TRE2544',
        'TRE2596',
        'TRE2995',
        'TRE3030',
        'TRE9001',
        'TRE9525',
    ]

    df_sale_total = df_sale_total[~df_sale_total['subcategory'].isin(subcategory_none)]
    df_sale = df_sale_total[['store', 'fdcode', 'default_code', 'qty', 'avg_qty']].fillna(0)
    df_sale = df_sale[df_sale['qty'] > 0]
    df_sale = df_sale.groupby(['store', 'fdcode','default_code']).agg({
        'qty': 'sum',
        'avg_qty':'sum'
        }).reset_index()
    df_sale['plan_qty'] = round(df_sale['avg_qty'] * MOH, 0)
    # 7. Kết hợp dữ liệu tồn kho và sức bán
    df_store_fn = df_store[df_store['store'] != 'KHO GIA CÔNG']
    df_store_fn = df_store_fn[~df_store_fn['subcategory'].isin(subcategory_none)]

    # Merge dữ liệu
    df_merge = pd.merge(df_sale, df_store_fn, on=['store', 'fdcode'], how='outer')
    df_merge = df_merge[~df_merge['default_code'].isin(default_code_non)]
    df_merge['available'].fillna(0, inplace=True)
    # Công thức gốc
    df_merge['need_qty'] = df_merge['available'] - df_merge['plan_qty']

    # Điều kiện avg_qty > 0 và available < 3
    mask_condition = (df_merge['avg_qty'] > 0) & (df_merge['available'] < 3)

    # Điều kiện store = ECOM hoặc KDS
    mask_store_special = df_merge['store'].isin(['ECOM', 'KDS'])

    # Áp dụng công thức vectorized
    df_merge.loc[mask_condition & mask_store_special, 'need_qty'] = -10 + df_merge['available']
    df_merge.loc[mask_condition & ~mask_store_special, 'need_qty'] = -3 + df_merge['available']

    #df_merge = df_merge[df_merge['need_qty'] != 0]

    print("Dữ liệu tồn kho và sức bán đã được khởi tạo thành công!")

    # 8. ✅ QUAN TRỌNG: FILTER CÁC KHO
    # ============================================================
    # KHO TỔNG
    df_warehouse = df_warehouse[~df_warehouse['subcategory'].isin(subcategory_none)]
    df_warehouse = df_warehouse[~df_warehouse['fdcode'].isin(
        df_template[df_template['default_code'].isin(default_code_non)]['fdcode'].unique()
    )]
    
    # KHO ECOM_SG
    df_warehouse_ecom = df_warehouse_ecom[~df_warehouse_ecom['subcategory'].isin(subcategory_none)]
    df_warehouse_ecom = df_warehouse_ecom[~df_warehouse_ecom['fdcode'].isin(
        df_template[df_template['default_code'].isin(default_code_non)]['fdcode'].unique()
    )]
    
    # KHO GIA CÔNG (đã có logic riêng)
    df_process_warehouse = df_process_warehouse[~df_process_warehouse['fdcode'].isin(set_products_process)]
    df_process_warehouse = df_process_warehouse[~df_process_warehouse['subcategory'].isin(subcategory_none)]
    df_process_warehouse = df_process_warehouse[~df_process_warehouse['fdcode'].isin(
        df_template[df_template['default_code'].isin(default_code_non)]['fdcode'].unique()
    )]

def stock_for_new_store(filtered_df, df_warehouse):
    result_list = []

    # Tạo danh sách MSP bán chạy (ưu tiên gom hàng trước)
    popular_msp = filtered_df.sort_values(by='avg_qty', ascending=False)['fdcode'].unique()

    # Lọc các cửa hàng mới cần gom hàng
    df_need = filtered_df[(filtered_df['need_qty'] < 0) & (filtered_df['Is_New_Store'] == 1)].copy()

    for msp in popular_msp:
        for _, row_need in df_need[df_need['fdcode'] == msp].iterrows():
            need_qty = abs(row_need['need_qty'])  # Số lượng cần gom của MSP này

            # Bước 1: Lấy hàng từ kho tổng
            warehouse_qty = df_warehouse[df_warehouse['need_qty'] == msp]['available'].sum()
            if warehouse_qty > 0:  # Lấy hết hàng từ kho tổng nếu còn
                transfer_qty = min(warehouse_qty, need_qty)
                result_list.append({
                    'from_store': 'KHO TỔNG',
                    'to_store': row_need['store'],
                    'fdcode': msp,
                    'transfer_qty': transfer_qty
                })
                # Cập nhật tồn kho kho tổng
                df_warehouse.loc[df_warehouse['fdcode'] == msp, 'available'] -= transfer_qty
                need_qty -= transfer_qty

            # Bước 2: Nếu kho tổng chưa đủ, lấy từ các cửa hàng dư
            if need_qty > 0:
                df_surplus = filtered_df[(filtered_df['fdcode'] == msp) & (filtered_df['available'] > 1)]
                df_surplus = df_surplus.sort_values(by='available', ascending=False)

                for _, row_surplus in df_surplus.iterrows():
                    surplus_qty = row_surplus['available']
                    if surplus_qty > 1:  # Chừa lại tối thiểu 1 sản phẩm
                        transfer_qty = min(surplus_qty - 1, need_qty)
                        result_list.append({
                            'from_store': row_surplus['store'],
                            'to_store': row_need['store'],
                            'fdcode': msp,
                            'transfer_qty': transfer_qty
                        })
                        # Cập nhật tồn kho cửa hàng dư và NEED_QTY của cửa hàng mới
                        filtered_df.loc[row_surplus.name, 'available'] -= transfer_qty
                        filtered_df.loc[row_need.name, 'need_qty'] += transfer_qty
                        need_qty -= transfer_qty

                    if need_qty == 0:  # Nếu đã đủ hàng thì dừng lại
                        break

    return pd.DataFrame(result_list), df_warehouse


# Hàm luân chuyển giữa cửa hàng
def transfer_between_stores(filtered_df, df_warehouse, max_stock_normal_store=4):
    result_list = []

    # Đánh dấu cửa hàng mới
    filtered_df['Is_New_Store'] = filtered_df['store'].apply(lambda x: 1 if 'new' in x.lower() else 0)

    # ✅ Bổ sung nhu cầu giả định nếu không có avg_qty nhưng đang trắng mã (tồn = 0 hoặc NaN)
    filtered_df['adjusted_need_qty'] = filtered_df.apply(
        lambda row: -3 if pd.isna(row['avg_qty']) and (pd.isna(row['available']) or row['available'] == 0)
        else row['need_qty'],
        axis=1
    )

    # Cửa hàng cần hàng và cửa hàng dư hàng
    df_need = filtered_df[(filtered_df['adjusted_need_qty'] < 0) & (filtered_df['Is_New_Store'] != 1)].copy()
    df_surplus = filtered_df[(filtered_df['need_qty'] > 0) & (filtered_df['store'] != 'KHO TỔNG')].copy()

    need_msp_list = df_need['fdcode'].unique()

    # Ưu tiên theo mức độ mới và sức bán
    df_need = df_need.sort_values(by=['Is_New_Store', 'avg_qty'], ascending=[False, False])

    # Giai đoạn 1: Luân chuyển với mã có nhu cầu rõ ràng hoặc giả định
    for _, row_need in df_need.iterrows():
        msp = row_need['fdcode']
        to_store = row_need['store']
        need_qty = abs(row_need['adjusted_need_qty'])
        current_stock = row_need['available'] if pd.notna(row_need['available']) else 0

        if need_qty <= 0:
            continue

        df_surplus_filtered = df_surplus[df_surplus['fdcode'] == msp].sort_values(by='avg_qty', ascending=False)

        for _, row_surplus in df_surplus_filtered.iterrows():
            if need_qty == 0:
                break

            from_store = row_surplus['store']
            qty_stock = df_surplus.at[row_surplus.name, 'available']

            if pd.isna(qty_stock) or qty_stock <= 0:
                continue

            # Tính tồn tối thiểu phải giữ lại
            if "KDS" in from_store:
                if qty_stock < 50:
                    min_stock = 10
                elif qty_stock <= 100:
                    min_stock = int(qty_stock * 0.5)
                else:
                    min_stock = int(qty_stock * (2 / 3))
            else:
                min_stock = max(3, row_surplus['avg_qty'])

            surplus_qty = qty_stock - min_stock
            if surplus_qty <= 0:
                continue

            if ("KDS" not in to_store) and ("ECOM" not in to_store):
                max_transfer_allow = max_stock_normal_store - current_stock
                if max_transfer_allow <= 0:
                    continue
                transfer_qty = min(surplus_qty, need_qty, max_transfer_allow)
            else:
                transfer_qty = min(surplus_qty, need_qty)

            transfer_qty = int(round(transfer_qty))

            if transfer_qty > 0 and (qty_stock - transfer_qty >= min_stock):
                result_list.append({
                    'from_store': from_store,
                    'to_store': to_store,
                    'fdcode': msp,
                    'transfer_qty': transfer_qty
                })

                df_surplus.at[row_surplus.name, 'available'] -= transfer_qty
                filtered_df.at[row_need.name, 'adjusted_need_qty'] += transfer_qty
                current_stock += transfer_qty
                need_qty -= transfer_qty

    # Giai đoạn 2: Mã chỉ có dư, không có cửa hàng cần rõ ràng
    surplus_msps = df_surplus['fdcode'].unique()
    msps_only_in_surplus = set(surplus_msps) - set(need_msp_list)

    for msp in msps_only_in_surplus:
        df_surplus_msp = df_surplus[df_surplus['fdcode'] == msp].sort_values(by='available', ascending=False)

        candidate_stores = filtered_df[
            (filtered_df['fdcode'] == msp) &
            ((filtered_df['available'].isna()) | (filtered_df['available'] == 0))
        ].sort_values(by='avg_qty', ascending=False)

        for _, row_target in candidate_stores.iterrows():
            estimated_need = 3
            to_store = row_target['store']
            current_stock = row_target['available'] if pd.notna(row_target['available']) else 0

            for _, row_src in df_surplus_msp.iterrows():
                from_store = row_src['store']
                qty_stock = df_surplus.at[row_src.name, 'available']

                if pd.isna(qty_stock) or qty_stock <= 0:
                    continue

                if "KDS" in from_store:
                    if qty_stock < 50:
                        min_stock = 10
                    elif qty_stock <= 100:
                        min_stock = int(qty_stock * 0.5)
                    else:
                        min_stock = int(qty_stock * (2 / 3))
                else:
                    min_stock = max(3, row_src['avg_qty'])

                surplus_qty = qty_stock - min_stock
                if surplus_qty <= 0:
                    continue

                if ("KDS" not in to_store) and ("ECOM" not in to_store):
                    max_transfer_allow = max_stock_normal_store - current_stock
                    if max_transfer_allow <= 0:
                        continue
                    transfer_qty = min(surplus_qty, estimated_need, max_transfer_allow)
                else:
                    transfer_qty = min(surplus_qty, estimated_need)

                transfer_qty = int(round(transfer_qty))

                if transfer_qty > 0 and (qty_stock - transfer_qty >= min_stock):
                    result_list.append({
                        'from_store': from_store,
                        'to_store': to_store,
                        'fdcode': msp,
                        'transfer_qty': transfer_qty
                    })

                    df_surplus.at[row_src.name, 'available'] -= transfer_qty
                    break  # Mỗi cửa hàng chỉ nhận 1 lần mã này

    return pd.DataFrame(result_list)


def _assign_store_caps(store_list, max_stock_normal_store: int, store_need_dict=None, low_priority_stores=None):
    """
    Chia cap dựa trên nhu cầu: cửa hàng thiếu nhiều → cap cao
    Low priority stores luôn nhận cap = 1
    """
    caps = {}
    if max_stock_normal_store is None or max_stock_normal_store <= 0:
        max_stock_normal_store = 1

    M = max_stock_normal_store
    
    # ✅ Tách low priority stores ra khỏi danh sách phân cap chính
    if low_priority_stores:
        store_list_normal = [s for s in store_list if s not in low_priority_stores]
    else:
        store_list_normal = store_list
    
    # ✅ Sort các cửa hàng bình thường theo nhu cầu
    if store_need_dict:
        store_list_sorted = sorted(
            store_list_normal, 
            key=lambda s: store_need_dict.get(s, 0), 
            reverse=True  # Thiếu nhiều nhất → cap cao nhất
        )
    else:
        store_list_sorted = sorted(store_list_normal)

    # Phân cap cho các cửa hàng bình thường
    for idx, store in enumerate(store_list_sorted):
        group_idx = idx % M
        cap = M - group_idx
        # ✅ Đảm bảo cap tối thiểu là 2 (trừ low priority stores)
        caps[store] = max(cap, 2)
    
    # ✅ Gán cap = 1 cho low priority stores
    if low_priority_stores:
        for store in low_priority_stores:
            if store in store_list:
                caps[store] = 1

    return caps


def stock_from_warehouse(
    filtered_df,
    df_warehouse,
    df_process_warehouse,
    max_stock_normal_store=3,
    df_warehouse_ecom=None,
    ecom_min_stock =10,
    ecom_max_stock =200,
    allow_ecom_fallback_to_general=False,
    debug=True
):
    LOW_PRIORITY_STORES = {"101AEONHAIPHONG", "201AEONHUE", "304GIGAMALL"}
    ECOM_STORE = "ECOM"
    ECOM_SOURCE = "ECOM_SG"
    total_transfer_limit = 10000
    total_transferred = 0

    # NORMALIZE
    filtered_df = filtered_df.copy()
    filtered_df["store"] = filtered_df["store"].astype(str).str.strip()
    filtered_df["fdcode"] = filtered_df["fdcode"].astype(str).str.strip().str.upper()

    if df_warehouse is not None and not df_warehouse.empty:
        df_warehouse["fdcode"] = df_warehouse["fdcode"].astype(str).str.strip().str.upper()

    if df_warehouse_ecom is not None and not df_warehouse_ecom.empty:
        df_warehouse_ecom["fdcode"] = df_warehouse_ecom["fdcode"].astype(str).str.strip().str.upper()

    if df_process_warehouse is not None and not df_process_warehouse.empty:
        df_process_warehouse["fdcode"] = df_process_warehouse["fdcode"].astype(str).str.strip().str.upper()

    def is_ecom_store(s):
        return str(s).strip().upper() == ECOM_STORE

    def get_store_priority(store):
        """Return 1 for low priority stores, 0 for normal stores"""
        return 1 if str(store).strip() in LOW_PRIORITY_STORES else 0

    # PREP NEED
    filtered_df["Is_New_Store"] = filtered_df["store"].apply(
        lambda x: 1 if "new" in str(x).lower() else 0
    )
    df_need = filtered_df[(filtered_df["need_qty"] < 0) & (filtered_df["Is_New_Store"] != 1)].copy()
    
    # Tính tổng nhu cầu của mỗi cửa hàng
    store_total_need = df_need.groupby("store")["need_qty"].sum().abs().to_dict()
    df_need["store_total_need"] = df_need["store"].map(store_total_need)

    # Thêm priority cho cửa hàng
    df_need["priority"] = df_need["store"].apply(get_store_priority)
    
    # ✅ Sắp xếp theo priority, nhu cầu cửa hàng, và avg_qty
    df_need = df_need.sort_values(
        by=["priority", "store_total_need", "avg_qty"], 
        ascending=[True, False, False]
    )

    store_list = filtered_df["store"].dropna().unique().tolist()
    store_list = [s for s in store_list if str(s).strip().upper() != ECOM_SOURCE]

    # ✅ Truyền thêm LOW_PRIORITY_STORES vào _assign_store_caps
    store_caps = _assign_store_caps(
        store_list, 
        max_stock_normal_store, 
        store_total_need,
        LOW_PRIORITY_STORES
    )
    
    # ✅ Override cap cho ECOM
    if ECOM_STORE in store_caps:
        store_caps[ECOM_STORE] = ecom_min_stock

    # Tồn hiện tại tại store
    tmp = filtered_df[["store", "fdcode", "available"]].copy()
    tmp["available"] = tmp["available"].fillna(0)

    store_stock = defaultdict(int)
    for _, r in tmp.iterrows():
        store_stock[(r["store"], r["fdcode"])] = int(r["available"])

    # Need remaining
    need_remaining = defaultdict(int)
    for _, r in df_need[["store", "fdcode", "need_qty"]].iterrows():
        need_remaining[(r["store"], r["fdcode"])] = int(abs(r["need_qty"]))

    transfers = defaultdict(int)

    def add_transfer(from_store, to_store, msp, qty):
        nonlocal total_transferred
        if qty <= 0:
            return
        transfers[(from_store, to_store, msp)] += qty
        total_transferred += qty
        store_stock[(to_store, msp)] += qty
        if (to_store, msp) in need_remaining:
            need_remaining[(to_store, msp)] = max(0, need_remaining[(to_store, msp)] - qty)

    def wh_decrease(df_wh, msp, qty):
        if df_wh is None or df_wh.empty or qty <= 0:
            return
        idx = df_wh["fdcode"] == msp
        if idx.any():
            df_wh.loc[idx, "available"] = df_wh.loc[idx, "available"].fillna(0) - qty
            df_wh.loc[idx, "available"] = df_wh.loc[idx, "available"].clip(lower=0)

    # LIMITS
    wh_total_limit = int(df_warehouse["available"].fillna(0).sum()) if df_warehouse is not None else 0
    wh_ecom_limit = int(df_warehouse_ecom["available"].fillna(0).sum()) if (df_warehouse_ecom is not None and not df_warehouse_ecom.empty) else 0

    # ✅ FIX: Kiểm tra ECOM có trong store_list không
    has_ecom_store = any(is_ecom_store(s) for s in store_list)

    # STEP 0) FORCE ECOM_SG -> ECOM BY NEED
    if df_warehouse_ecom is not None and not df_warehouse_ecom.empty and wh_ecom_limit > 0 and has_ecom_store:
        ecom_need_rows = df_need[df_need["store"].apply(is_ecom_store)]

        for _, row in ecom_need_rows.iterrows():
            if total_transferred >= total_transfer_limit or wh_ecom_limit <= 0:
                break

            msp = row["fdcode"]
            to_store = ECOM_STORE

            need_qty = need_remaining[(to_store, msp)]
            if need_qty <= 0:
                continue

            current = store_stock[(to_store, msp)]
            cap = store_caps.get(to_store, max_stock_normal_store)
            room = ecom_max_stock - current
            
            # ✅ Kiểm tra tối đa cho ECOM
            if is_ecom_store(to_store):
                if current >= ecom_max_stock:
                    if debug:
                        print(f"      ⚠️  ECOM đã đạt max ({current} >= {ecom_max_stock}), bỏ qua")
                    continue
                room = min(room, ecom_max_stock - current)
            if room <= 0:
                continue

            src_qty = int(
                df_warehouse_ecom.loc[df_warehouse_ecom["fdcode"] == msp, "available"]
                .fillna(0).sum()
            )
            if src_qty <= 0:
                continue

            give = min(
                src_qty,
                need_qty,
                room,
                wh_ecom_limit,
                total_transfer_limit - total_transferred,
            )
            if give > 0:
                add_transfer(ECOM_SOURCE, ECOM_STORE, msp, give)
                wh_decrease(df_warehouse_ecom, msp, give)
                wh_ecom_limit -= give

    # STEP 1) PULL BY NEED FROM GENERAL WAREHOUSE
    for _, row_need in df_need.iterrows():
        if total_transferred >= total_transfer_limit or wh_total_limit <= 0:
            break

        msp = row_need["fdcode"]
        to_store = row_need["store"]

        if is_ecom_store(to_store) and not allow_ecom_fallback_to_general:
            continue

        need_qty = need_remaining[(to_store, msp)]
        if need_qty <= 0:
            continue

        # ✅ Lấy tồn thực tế từ filtered_df
        actual_stock = filtered_df[
            (filtered_df['store'] == to_store) & 
            (filtered_df['fdcode'] == msp)
        ]['available'].sum()
        
        # ✅ GIỚI HẠN TRẦN: Tồn + chia <= 6
        max_allowed = 6 - actual_stock
        
        # ✅ Với low priority stores: Tồn + chia <= 1
        if get_store_priority(to_store) == 1:
            max_allowed = max(0, 1 - actual_stock)
            if max_allowed <= 0:
                continue
        
        # ✅ Nếu tồn = 0, nhận tối thiểu 2
        if actual_stock == 0 and get_store_priority(to_store) == 0:
            min_qty = 2
        else:
            min_qty = 0
        
        if max_allowed <= 0:
            continue

        current = store_stock[(to_store, msp)]
        cap = store_caps.get(to_store, max_stock_normal_store)
        room = min(cap - current, max_allowed)
        
        # ✅ Kiểm tra tối đa cho ECOM
        if is_ecom_store(to_store):
            if current >= ecom_max_stock:
                continue
            room = min(room, ecom_max_stock - current)
        
        if room <= 0:
            continue

        wh_qty = int(df_warehouse.loc[df_warehouse["fdcode"] == msp, "available"].fillna(0).sum())
        if wh_qty <= 0:
            continue

        give = min(
            wh_qty,
            need_qty,
            room,
            wh_total_limit,
            total_transfer_limit - total_transferred,
        )
        
        # ✅ Đảm bảo tối thiểu khi tồn = 0
        if actual_stock == 0 and give > 0 and give < min_qty and wh_qty >= min_qty:
            give = min(min_qty, wh_qty, need_qty, room, wh_total_limit, total_transfer_limit - total_transferred)
        
        if give > 0:
            add_transfer("KHO TỔNG", to_store, msp, give)
            wh_decrease(df_warehouse, msp, give)
            wh_total_limit -= give

    # STEP 2) EVEN DISTRIBUTION FROM GENERAL WAREHOUSE LEFTOVER
    if df_warehouse is not None and not df_warehouse.empty:
        wh_left = df_warehouse[df_warehouse["available"].fillna(0) > 0].copy()

        for msp, group in wh_left.groupby("fdcode"):
            if total_transferred >= total_transfer_limit or wh_total_limit <= 0:
                break

            total_qty = int(group["available"].sum())
            give_all = min(total_qty, wh_total_limit, total_transfer_limit - total_transferred)
            if give_all <= 0:
                continue

            store_candidates = []
            for store in store_list:
                if is_ecom_store(store) and not allow_ecom_fallback_to_general:
                    continue

                # ✅ Lấy tồn thực tế
                actual_stock = filtered_df[
                    (filtered_df['store'] == store) & 
                    (filtered_df['fdcode'] == msp)
                ]['available'].sum()
                
                # ✅ GIỚI HẠN TRẦN: Tồn + chia <= 6
                max_allowed = 6 - actual_stock
                
                # ✅ Với low priority stores: Tồn + chia <= 1
                if get_store_priority(store) == 1:
                    max_allowed = max(0, 1 - actual_stock)
                
                if max_allowed <= 0:
                    continue

                current = store_stock[(store, msp)]
                cap = store_caps.get(store, max_stock_normal_store)
                room = min(cap - current, max_allowed)
                
                if room > 0:
                    priority = get_store_priority(store)
                    store_candidates.append((store, current, cap, priority, actual_stock, max_allowed))

            if not store_candidates:
                continue

            # ✅ FIXED: Sort by priority first, then current stock
            store_candidates.sort(key=lambda x: (x[3], x[1]))

            qty_per_store = give_all // len(store_candidates)
            remainder = give_all % len(store_candidates)

            distributed = 0
            for i, (store, current, cap, priority, actual_stock, max_allowed) in enumerate(store_candidates):
                if total_transferred >= total_transfer_limit or wh_total_limit <= 0:
                    break

                room = min(cap - current, max_allowed)
                if room <= 0:
                    continue

                intended = qty_per_store + (1 if i < remainder else 0)
                give = min(intended, room, give_all - distributed, wh_total_limit, total_transfer_limit - total_transferred)

                # ✅ Đảm bảo tối thiểu 2 khi tồn = 0 (cho cửa hàng bình thường)
                if actual_stock == 0 and give > 0 and give < 2 and priority == 0:
                    if give_all - distributed >= 2 and room >= 2:
                        give = min(2, room, give_all - distributed, wh_total_limit, total_transfer_limit - total_transferred)

                if give > 0:
                    add_transfer("KHO TỔNG", store, msp, give)
                    distributed += give
                    wh_total_limit -= give

            if distributed > 0:
                wh_decrease(df_warehouse, msp, distributed)
    
    
    # ✅ STEP 2.5) EVEN DISTRIBUTION FROM ECOM_SG LEFTOVER -> only ECOM
    if df_warehouse_ecom is not None and not df_warehouse_ecom.empty and wh_ecom_limit > 0 and has_ecom_store:
        wh_left_ecom = df_warehouse_ecom[df_warehouse_ecom["available"].fillna(0) > 0].copy()

        for msp, group in wh_left_ecom.groupby("fdcode"):
            if total_transferred >= total_transfer_limit or wh_ecom_limit <= 0:
                break

            total_qty = int(group["available"].sum())
            give_all = min(total_qty, wh_ecom_limit, total_transfer_limit - total_transferred)
            if give_all <= 0:
                continue

            current = store_stock[(ECOM_STORE, msp)]
            cap = store_caps.get(ECOM_STORE, max_stock_normal_store)
            room = cap - current
            
            # ✅ Kiểm tra tối đa cho ECOM
            if current >= ecom_max_stock:
                if debug:
                    print(f"      ⚠️  ECOM đã đạt max ({current} >= {ecom_max_stock}), bỏ qua {msp}")
                continue
            room = min(room, ecom_max_stock - current)
            if room <= 0:
                continue

            give = min(give_all, room, wh_ecom_limit, total_transfer_limit - total_transferred)
            if give > 0:
                add_transfer(ECOM_SOURCE, ECOM_STORE, msp, give)
                wh_decrease(df_warehouse_ecom, msp, give)
                wh_ecom_limit -= give

# STEP 3) PROCESS WAREHOUSE (TOP-UP THEO TỒN) - UPDATED RULES

    def cap_process_after_transfer(store, fdcode):
        fd = str(fdcode).upper().strip()
        st = str(store).upper().strip()
        is_ecom = (st == "ECOM")

        # ❌ ECOM không bốc mã HF*
        if is_ecom and fd.startswith("HF"):
            return 0

        # ✅ TUIRUT* : như TUIGIAY nhưng CHỈ cho ECOM
        if fd.startswith("TUIRUT"):
            return 2000 if is_ecom else 0

        # ✅ CHM* tối đa 10 (mọi store)
        if fd.startswith("CHM"):
            return 30 if is_ecom else 5

        # ✅ HOPKID / HOPSUKID tối đa 50 (mọi store)
        if fd.startswith(("HOPKID", "HOPSUKEKID")):
            return 500 if is_ecom else 50

        # HOPGIAY theo size
        if fd.startswith(("HOPGIAYS")):
            return 0 if is_ecom else 30
        
                # HOPGIAY theo size
        if fd.startswith(("HOPGIAYL")):
            return 3000 if is_ecom else 50

        if fd.startswith("HOPGIAYM"):
            return 3000 if is_ecom else 50
        
        if fd.startswith("HOPLIMAXNAM"):
            return 0 if is_ecom else 50
        
        if fd.startswith("HOPLIMAXNU"):
            return 0 if is_ecom else 50

        # TUIGIAY
        if fd.startswith("TUIGIAY"):
            return 0 if is_ecom else 200
        
        # TUINHUAM01
        if fd.startswith(('TUINHUA')):
            return 0 if is_ecom else 3
        
        # GIẤY NẾN
        if fd.startswith("GIAYLIMAX"):
            return 0 if is_ecom else 3

        # Còn lại
        return 3


    def adjust_qty_special(fdcode, qty):
        """
        Rule số lượng đặc biệt:
        - VOSCC*: bốc theo bội số 5, tối thiểu 5
        """
        fd = str(fdcode).upper().strip()

        if fd.startswith("VOSC"):
            if qty < 5:
                return 0
            return (qty // 5) * 5

        return qty


    def add_transfer_process(to_store, msp, qty):
        # Add transfer từ KHO GIA CÔNG nhưng KHÔNG cộng total_transferred
        if qty <= 0:
            return
        transfers[("KHO GIA CÔNG", to_store, msp)] += qty
        store_stock[(to_store, msp)] += qty


    store_list_proc = [s for s in store_list if str(s).upper().strip() != "ECOM_SG"]

    if df_process_warehouse is not None and not df_process_warehouse.empty:

        df_process_warehouse["fdcode"] = df_process_warehouse["fdcode"].astype(str).str.upper().str.strip()

        proc_sum = (
            df_process_warehouse.groupby("fdcode", as_index=False)["available"]
            .sum()
            .sort_values("available", ascending=False)
        )

        for _, row in proc_sum.iterrows():
            msp = row["fdcode"]
            proc_qty_left = int(row["available"])
            if proc_qty_left <= 0:
                continue

            candidates = []
            for st in store_list_proc:
                current = int(store_stock[(st, msp)])
                cap_total = cap_process_after_transfer(st, msp)
                room = cap_total - current
                if room > 0:
                    candidates.append((st, current, room))

            if not candidates:
                continue

            candidates.sort(key=lambda x: x[1])  # tồn thấp trước

            distributed = 0
            for st, current, room in candidates:
                if proc_qty_left <= 0:
                    break

                raw_give = min(proc_qty_left, room)
                give = adjust_qty_special(msp, raw_give)

                if give <= 0:
                    continue

                add_transfer_process(st, msp, give)
                proc_qty_left -= give
                distributed += give

            if distributed > 0:
                wh_decrease(df_process_warehouse, msp, distributed)


    # OUTPUT
    if not transfers:
        return pd.DataFrame(columns=["from_store", "to_store", "fdcode", "transfer_qty"])

    rows = [
        {"from_store": f, "to_store": t, "fdcode": m, "transfer_qty": int(q)}
        for (f, t, m), q in transfers.items()
        if q > 0
    ]
    return pd.DataFrame(rows, columns=["from_store", "to_store", "fdcode", "transfer_qty"])

# LẤY HÀNG TỪ FILE IMPORT
def allocate_import_to_stores(imported_df, df_merge):
    """
    Phân bổ số lượng từ danh sách import trực tiếp cho các cửa hàng.

    Quy tắc:
    1. Xác định danh sách store sẽ phân bổ cho từng fdcode:
       - Nếu fdcode đã có trong df_merge → chỉ phân cho các store đó.
       - Nếu chưa có → phân cho toàn bộ all_stores (sẽ tạo dòng mới).
    2. Nếu tổng qty >= số store:
       - Bước seed: đảm bảo mỗi store có ít nhất 1 đôi (theo thứ tự thiếu nhiều nhất).
       - Cập nhật cả available và need_qty.
    3. Phần còn lại:
       - Bước lấp thiếu: ưu tiên các store có need_qty < 0 (thiếu nhiều nhất trước).
       - Bước dư: nếu vẫn còn dư sau khi lấp hết thiếu → chia đều cho các store.
    """

    result_list = []
    df_merge = df_merge.copy()  # tránh thay đổi df gốc

    # Danh sách tất cả cửa hàng đang có trong hệ thống
    all_stores = df_merge['store'].unique()

    for _, row in imported_df.iterrows():
        msp = row['fdcode']
        total_qty = int(row['qty'])

        if total_qty <= 0:
            continue

        # --- 0. Xác định danh sách store sẽ phân bổ cho MSP này ---
        df_alloc = df_merge[df_merge['fdcode'] == msp]
        if not df_alloc.empty:
            base_stores = df_alloc['store'].unique()
        else:
            base_stores = all_stores

        base_stores = list(base_stores)
        num_stores = len(base_stores)
        if num_stores == 0:
            continue

        # Sắp xếp store theo mức độ thiếu (store thiếu nhiều đứng trước)
        store_need = (
            df_merge[df_merge['fdcode'] == msp][['store', 'need_qty']]
            .drop_duplicates('store')
            .set_index('store')
        )

        need_series = pd.Series(
            [store_need['need_qty'].get(s, 0) for s in base_stores],
            index=base_stores
        )
        # sort tăng dần: âm nhiều (thiếu nhiều) đứng đầu
        stores_sorted = need_series.sort_values().index.tolist()

        # --- 1. Bước seed: mỗi store ít nhất 1 đôi (nếu đủ tổng qty) ---
        if total_qty >= num_stores:
            for store in stores_sorted:
                allocated_qty = 3

                mask = (df_merge['store'] == store) & (df_merge['fdcode'] == msp)
                if mask.any():
                    idx = mask.idxmax()
                    df_merge.loc[idx, 'available'] += allocated_qty
                    # Cập nhật luôn need_qty để không bị phân thêm sai
                    df_merge.loc[idx, 'need_qty'] += allocated_qty
                else:
                    new_row = {
                        'store': store,
                        'fdcode': msp,
                        'available': allocated_qty,
                        'need_qty': 0  # hàng mới, chưa có nhu cầu âm
                    }
                    df_merge = pd.concat(
                        [df_merge, pd.DataFrame([new_row])],
                        ignore_index=True
                    )

                result_list.append({
                    'fdcode': msp,
                    'to_store': store,
                    'allocated_qty': allocated_qty
                })
                total_qty -= allocated_qty

        # Nếu total_qty < num_stores thì không seed được hết,
        # lúc này ta chỉ ưu tiên theo mức thiếu ở các bước sau.

        # --- 2. Bước lấp thiếu: ưu tiên need_qty < 0 (thiếu nhiều nhất) ---
        if total_qty > 0:
            df_need = df_merge[
                (df_merge['fdcode'] == msp) & (df_merge['need_qty'] < 0)
            ].copy()

            df_need = df_need.sort_values(by='need_qty')  # âm nhiều đứng trước

            for idx, need_row in df_need.iterrows():
                if total_qty == 0:
                    break

                need_qty = abs(need_row['need_qty'])
                if need_qty <= 0:
                    continue

                allocated_qty = min(total_qty, need_qty)

                result_list.append({
                    'fdcode': msp,
                    'to_store': need_row['store'],
                    'allocated_qty': allocated_qty
                })

                total_qty -= allocated_qty
                df_merge.loc[idx, 'available'] += allocated_qty
                df_merge.loc[idx, 'need_qty'] += allocated_qty  # tiến về 0

        # --- 3. Bước dư: nếu vẫn còn dư → chia đều cho tất cả store ---
        if total_qty > 0:
            qty_per_store, remainder = divmod(total_qty, num_stores)

            for store in stores_sorted:
                if total_qty == 0:
                    break

                allocated_qty = qty_per_store + (1 if remainder > 0 else 0)
                if allocated_qty <= 0:
                    continue

                if remainder > 0:
                    remainder -= 1

                mask = (df_merge['store'] == store) & (df_merge['fdcode'] == msp)
                if mask.any():
                    idx = mask.idxmax()
                    df_merge.loc[idx, 'available'] += allocated_qty
                else:
                    new_row = {
                        'store': store,
                        'fdcode': msp,
                        'available': allocated_qty,
                        'need_qty': 0
                    }
                    df_merge = pd.concat(
                        [df_merge, pd.DataFrame([new_row])],
                        ignore_index=True
                    )

                result_list.append({
                    'fdcode': msp,
                    'to_store': store,
                    'allocated_qty': allocated_qty
                })
                total_qty -= allocated_qty

    return pd.DataFrame(result_list), df_merge
if __name__ == "__main__":
    initialize_data()