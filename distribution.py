import pandas as pd
import os
import glob
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
    df_template = pd.read_sql_query(text(query_products_template), conn)

# Thiết lập cột và lọc dữ liệu
set_products = set(df_template['fdcode'])

df_template_process_1 = df_template[df_template['subcategory'].isin(['BẢO HÀNH SỬA CHỮA', 'QUÀ TẶNG', 'T-SHIRTS', 'CCDC', 'RASTACLAT', 'KEY RING'])]
df_template_process_2 = df_template[df_template['default_code'].isin(['FXHOPQUANHO', 'QHKHAC', 'HOPBATM01', 'HOPBAD1', 'TUISI52'])]

df_template_process = pd.concat([df_template_process_1, df_template_process_2]).drop_duplicates()
set_products_process = set(df_template_process['fdcode'])
# 🧩 Hàm khởi tạo dữ liệu tồn kho và sức bán
def initialize_data():
    global df_warehouse, df_merge, df_store, combined_df, df_process_warehouse

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
                    WHERE depot_id NOT IN (110819, 111154, 101011, 111753, 125224, 142410, 217633, 217642, 110826, 111155, 222877)
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
                        AND pih.depot_id NOT IN (110819, 111154, 101011, 111753, 125224, 142410, 217633, 217642, 110826, 111155, 222877)
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
    df_stock.replace({'ECOM2': 'ECOM', 
                      'ECOM HN': 'ECOM',
                      'ECOM SG': 'ECOM',
                      'KHO BOXME': 'ECOM',
                      'KHO SỈ':'KDS'}, inplace=True)


    # Tách tồn kho theo kho
    df_store = df_stock[df_stock['store'] != 'KHO TỔNG']

    df_warehouse = df_stock[df_stock['store'] == 'KHO TỔNG']

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
            WHERE store NOT IN('KHO XUẤT', 'KHO SỈ')
            GROUP BY 
                store,
                category,
                subcategory,
                default_code,
                fdcode
    """

    with engine.connect() as conn:
        combined_df = pd.read_sql_query(text(query_sales_90_days), conn)
    # 6. Tính toán sức bán trung bình
    # KHAI BÁO SẢN PHẨM KHÔNG LUÂN CHUYỂN
    subcategory_none = ['BAGS', 'CHARM', 'KEY RING', 'UPPER', 'RASTACLAT']
    default_code_non = ['F6S2030', 'F6S2011', 'F7R2435', 'AOMUA2', 'COMBO FX', 
                        'PLA2595', 'PLA0001', 'PLA1113', 'F7R2022', 'TRE2995', 
                        'SND0002', 'SND2525', 'S2C0060', 'F6S1071', 'F6S1011', 
                        'F6S1031', 'F6S1043', 'F7R2530', 'F6S2030', 'F6S2580', 
                        'F6S2011', 'F6S0013', 'F6S2023', 'F6S1013']
    combined_df = combined_df[~combined_df['subcategory'].isin(subcategory_none)]
    df_sale = combined_df[['store', 'fdcode', 'default_code', 'qty', 'avg_qty']].fillna(0)
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

# Bốc tồn - bản tránh duplicate
def stock_from_warehouse(filtered_df, df_warehouse, df_process_warehouse, max_stock_normal_store=3):
    # ====== THAM SỐ HỆ THỐNG ======
    total_transfer_limit = 10000
    total_transferred = 0

    # ====== TIỀN XỬ LÝ ======
    # Cửa hàng cần hàng (loại NEW)
    filtered_df = filtered_df.copy()
    filtered_df['Is_New_Store'] = filtered_df['store'].apply(lambda x: 1 if 'new' in str(x).lower() else 0)
    df_need = filtered_df[(filtered_df['need_qty'] < 0) & (filtered_df['Is_New_Store'] != 1)].copy()
    df_need = df_need.sort_values(by='avg_qty', ascending=False)

    # Danh sách cửa hàng
    store_list = filtered_df['store'].dropna().unique().tolist()

    # Tồn hiện tại tại cửa hàng theo (store, fdcode)
    tmp = filtered_df[['store','fdcode','available']].copy()
    tmp['available'] = tmp['available'].fillna(0)
    store_stock = defaultdict(int)
    for _, r in tmp.iterrows():
        store_stock[(r['store'], r['fdcode'])] = int(r['available'])

    # Nhu cầu còn lại (số dương) theo (store, fdcode)
    need_remaining = defaultdict(int)
    need_view = df_need[['store','fdcode','need_qty']].copy()
    for _, r in need_view.iterrows():
        need_remaining[(r['store'], r['fdcode'])] = int(abs(r['need_qty']))

    # Bộ gộp kết quả: (from_store, to_store, fdcode) -> qty
    transfers = defaultdict(int)

    def add_transfer(from_store, to_store, msp, qty):
        """Cộng dồn kết quả + cập nhật tồn/nhu cầu."""
        nonlocal total_transferred
        if qty <= 0:
            return
        transfers[(from_store, to_store, msp)] += qty
        total_transferred += qty
        store_stock[(to_store, msp)] += qty
        if (to_store, msp) in need_remaining:
            need_remaining[(to_store, msp)] = max(0, need_remaining[(to_store, msp)] - qty)

    # ====== 1) BỐC TỪ KHO TỔNG THEO NHU CẦU ======
    warehouse_total_qty = int(df_warehouse['available'].fillna(0).sum())
    warehouse_transfer_limit = warehouse_total_qty

    for _, row_need in df_need.iterrows():
        if total_transferred >= total_transfer_limit or warehouse_transfer_limit <= 0:
            break

        msp = row_need['fdcode']
        to_store = row_need['store']

        current_stock = store_stock[(to_store, msp)]
        max_transfer_allow = max_stock_normal_store - current_stock
        if max_transfer_allow <= 0:
            continue

        # tồn kho tổng cho MSP
        warehouse_qty = int(df_warehouse.loc[df_warehouse['fdcode'] == msp, 'available'].fillna(0).sum())
        if warehouse_qty <= 0:
            continue

        need_qty = need_remaining[(to_store, msp)]
        if need_qty <= 0:
            continue

        transfer_qty = min(warehouse_qty, need_qty, warehouse_transfer_limit,
                           total_transfer_limit - total_transferred, max_transfer_allow)

        if transfer_qty > 0:
            add_transfer('KHO TỔNG', to_store, msp, transfer_qty)
            # trừ tồn kho tổng cho MSP
            idx = df_warehouse['fdcode'] == msp
            df_warehouse.loc[idx, 'available'] = df_warehouse.loc[idx, 'available'].fillna(0) - transfer_qty
            warehouse_transfer_limit -= transfer_qty

    # ====== 2) CHIA ĐỀU TỪ KHO TỔNG (MÃ CÒN TỒN) ======
    # Lấy các MSP còn tồn sau bước 1
    wh_left = df_warehouse[df_warehouse['available'].fillna(0) > 0].copy()

    for msp, group in wh_left.groupby('fdcode'):
        if total_transferred >= total_transfer_limit or warehouse_transfer_limit <= 0:
            break

        total_qty = int(group['available'].sum())
        transfer_qty_all = min(total_qty, warehouse_transfer_limit, total_transfer_limit - total_transferred)
        if transfer_qty_all <= 0:
            continue

        qty_per_store = transfer_qty_all // max(1, len(store_list))
        remainder = transfer_qty_all % max(1, len(store_list))

        for idx_s, store in enumerate(store_list):
            if total_transferred >= total_transfer_limit or warehouse_transfer_limit <= 0:
                break

            current_stock = store_stock[(store, msp)]
            max_transfer_allow = max_stock_normal_store - current_stock
            if max_transfer_allow <= 0:
                continue

            intended_qty = qty_per_store + (1 if idx_s < remainder else 0)
            give = min(intended_qty, max_transfer_allow,
                       warehouse_transfer_limit, total_transfer_limit - total_transferred)
            if give > 0:
                add_transfer('KHO TỔNG', store, msp, give)
                warehouse_transfer_limit -= give

        # Xóa sạch tồn MSP đó tại kho tổng (đã phân bổ hết transfer_qty_all)
        idx = df_warehouse['fdcode'] == msp
        df_warehouse.loc[idx, 'available'] = df_warehouse.loc[idx, 'available'].fillna(0) - transfer_qty_all
        df_warehouse.loc[idx, 'available'] = df_warehouse.loc[idx, 'available'].clip(lower=0)

    # ====== 3) KHO GIA CÔNG THEO NHU CẦU (ƯU TIÊN TỒN ÍT) ======
    '''process_only_msp = df_process_warehouse[df_process_warehouse['available'].fillna(0) > 0].copy()
    process_only_msp = process_only_msp.sort_values(by='available')

    for _, row_need in df_need.iterrows():
        if total_transferred >= total_transfer_limit:
            break

        msp = row_need['fdcode']
        to_store = row_need['store']

        need_qty = need_remaining[(to_store, msp)]
        if need_qty <= 0:
            continue

        # tồn gia công còn lại
        proc_qty = int(df_process_warehouse.loc[df_process_warehouse['fdcode'] == msp, 'available'].fillna(0).sum())
        if proc_qty <= 0:
            continue

        current_stock = store_stock[(to_store, msp)]
        max_transfer_allow = max_stock_normal_store - current_stock
        if max_transfer_allow <= 0:
            continue

        give = min(proc_qty, need_qty, max_transfer_allow, total_transfer_limit - total_transferred)
        if give > 0:
            add_transfer('KHO GIA CÔNG', to_store, msp, give)
            idx = df_process_warehouse['fdcode'] == msp
            df_process_warehouse.loc[idx, 'available'] = df_process_warehouse.loc[idx, 'available'].fillna(0) - give

    # ====== 4) CHIA ĐỀU KHO GIA CÔNG (CÒN LẠI) ======
    proc_left = df_process_warehouse[df_process_warehouse['available'].fillna(0) > 0].copy()

    for _, row_proc in proc_left.iterrows():
        if total_transferred >= total_transfer_limit:
            break

        msp = row_proc['fdcode']
        qty_stock = int(row_proc['available'])
        give_all = min(qty_stock, total_transfer_limit - total_transferred)
        if give_all <= 0:
            continue

        qty_per_store = give_all // max(1, len(store_list))
        remainder = give_all % max(1, len(store_list))

        distributed = 0
        for idx_s, store in enumerate(store_list):
            if total_transferred >= total_transfer_limit:
                break

            current_stock = store_stock[(store, msp)]
            max_transfer_allow = max_stock_normal_store - current_stock
            if max_transfer_allow <= 0:
                continue

            intended = qty_per_store + (1 if idx_s < remainder else 0)
            give = min(intended, max_transfer_allow, give_all - distributed)
            if give > 0:
                add_transfer('KHO GIA CÔNG', store, msp, give)
                distributed += give

        # trừ kho gia công cho MSP hiện tại
        idx = df_process_warehouse['fdcode'] == msp
        df_process_warehouse.loc[idx, 'available'] = df_process_warehouse.loc[idx, 'available'].fillna(0) - distributed
        df_process_warehouse.loc[idx, 'available'] = df_process_warehouse.loc[idx, 'available'].clip(lower=0)'''

    # ====== TRẢ KẾT QUẢ (ĐÃ GỘP, KHÔNG DUPLICATE) ======
    if not transfers:
        return pd.DataFrame(columns=['from_store','to_store','fdcode','transfer_qty'])

    rows = []
    for (f, t, m), q in transfers.items():
        if q > 0:
            rows.append({'from_store': f, 'to_store': t, 'fdcode': m, 'transfer_qty': int(q)})

    return pd.DataFrame(rows, columns=['from_store','to_store','fdcode','transfer_qty'])

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