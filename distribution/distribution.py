from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from core.queries import get_product_template
from core.db import get_engine, get_ecom_engine
import pandas as pd
import os
import glob
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

engine = get_engine()

# Tham số mặc định
MOH = 2.5
df_merge = pd.DataFrame()
df_warehouse = pd.DataFrame()
df_process_warehouse = pd.DataFrame()

df_template = get_product_template(engine)
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
            WHERE store NOT IN('KHO XUẤT', 'ECOM', 'ECOM SG', 'AMAZON', '307LEVANVIET', '201AEONHUE')
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
    engine_ecom = get_ecom_engine()

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
            AND UPPER(os.name) <> 'BOXME'
            AND eoi.product_sku <>''
        GROUP BY store,
                fdcode
    """

    # Lấy dữ liệu bán hàng từ database
    with engine_ecom.connect() as conn:
        combined_df_ecom = pd.read_sql_query(text(query_sales_90_days_ecom), conn)
    engine_ecom.dispose()
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

        # =========================
        # NGỪNG BÁN
        # =========================
        'AOTH05',
        'CHN0001',
        'LIT3030',
        'LIT3434',
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
        'TAN2510',
        'TRE003',
        'TRE1115',
        'TRE2022',
        'TRE2529',
        'TRE2544',
        'TRE2596',
        'TRE3030',
        'TRE9001',
        'TRE9525',
        'TRE0020',
        'SND0022',
        'SND0077',
        'SND0010',
        'SND0035',
        'SND0040',
        'SND0100',
        'SND0101',
        'SND0113',
        'SND0200',
        'SND0203',
        'SND0300',
        'CHN0020',
        'CHN2995',
        'CHN0303',
        'CHN0404',
        'CHN2626',
        'TRE9595',
        'TRE2595',
        'TRE2558',
        'TRE0006',
        'TRE0003',
        'PLK3542',
        'TAN0001',
        'THANKYOUCARD2511',
        'F6S0007',
        'F6S3310',
        'F6S0006',
        'F6S0017',
        'F7N0014',
        'F7R0008',
        'F7R0004',
        'F7R1111',
        'F7R0007',
        'F7T0004',
        'F8B2660',
        'F8M3913',
        'F8M2660',
        'GIM7095',
        'PLA2323',
        'PLA1111',
        'PL52420',
        'PL57095',
        'TRE0002',
        'TR22525',
        'TR40000',
        'TR41111',
        'TR51158',
        'LIT4044',
        'PLK6535',
        'Li47575',
        'Li80040',
        'F6S3525',
        'F6S4225',
        'F6S2570',
        'LIT6016',
        'LIT3032',
        'CHN0113',
        'SND0303',
        'F7N7272-KID',
        'SND0012',
        'SND0202',
        'SND1000',
        'DTB9595',
        'LIT7272',
        'LIT9525',
        'BANGKEO1'
    
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
    # 1. Store
    df_store = df_sale[['store']].drop_duplicates()

    # 2. Template sạch
    df_template_sku = (
        df_template[['fdcode', 'default_code', 'subcategory', 'category']]
        .dropna(subset=['fdcode'])
        .drop_duplicates()
    )

    # 3. Tạo full matrix store x fdcode
    df_full = (
        df_store.assign(key=1)
        .merge(df_template_sku.assign(key=1), on='key')
        .drop(columns='key')
    )

    # 4. Chỉ lấy các cột số liệu từ df_sale để merge thêm vào
    sale_cols = [c for c in df_sale.columns if c not in ['default_code', 'subcategory', 'category']]

    df_sale_full = df_full.merge(
        df_sale[sale_cols],
        on=['store', 'fdcode'],
        how='left'
    )

    # 5. Fill các cột số liệu còn thiếu
    for col in ['qty', 'avg_qty', 'plan_qty']:
        if col in df_sale_full.columns:
            df_sale_full[col] = df_sale_full[col].fillna(0)

    # 6. Optional: đánh dấu dòng được bổ sung từ template
    df_sale_full['is_missing_added'] = (
        df_sale_full[['qty', 'avg_qty', 'plan_qty']]
        .eq(0)
        .all(axis=1)
    ).astype(int)

    df_sale_full = df_sale_full.sort_values(['store', 'default_code', 'fdcode']).reset_index(drop=True)
    valid_fdcode = df_stock[df_stock['available'] > 0]['fdcode'].unique()
    df_sale_full = df_sale_full[df_sale_full['fdcode'].isin(valid_fdcode)]
    df_sale_full_ft = df_sale_full[['store', 'fdcode', 'default_code', 'subcategory', 'category', 'qty', 'avg_qty', 'plan_qty']]
    df_merge = pd.merge(df_sale_full_ft, df_store_fn[['store', 'fdcode', 'available']], on=['store', 'fdcode'], how='left')
    df_merge['available'] = df_merge['available'].fillna(0)
    df_merge = df_merge[~df_merge['default_code'].isin(default_code_non)]
    # Công thức gốc
    df_merge['need_qty'] = df_merge['available'] - df_merge['plan_qty']

    df_merge['need_qty'] = np.where(
        (df_merge['plan_qty'] == 0) & (df_merge['available'] == 0),
        -2,
        df_merge['available'] - df_merge['plan_qty']
    )

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
            warehouse_qty = df_warehouse[df_warehouse['fdcode'] == msp]['available'].sum()
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
def transfer_between_stores(filtered_df, df_warehouse=None, max_stock_normal_store=10):
    """
    Rewrite stable version:
    - Normalize store/fdcode
    - Numeric coercion for available/avg_qty/need_qty
    - Use ledger (dict) for stock updates instead of copy + index issues
    - Stage A: fulfill explicit needs (adjusted_need_qty < 0) from KHO TỔNG -> others -> ECOM last
    - Stage B: codes only in surplus: seed to zero-stock stores with estimated_need=3, still KHO TỔNG first, ECOM last
    """

    df = filtered_df.copy()

    # ---------- 0) Normalize & type safety ----------
    df["store"] = df["store"].astype(str).str.strip()
    df["fdcode"] = df["fdcode"].astype(str).str.strip()

    # numeric
    df["available"] = pd.to_numeric(df.get("available"), errors="coerce").fillna(0)
    df["avg_qty"] = pd.to_numeric(df.get("avg_qty"), errors="coerce")
    df["need_qty"] = pd.to_numeric(df.get("need_qty"), errors="coerce").fillna(0)

    # mark new store (kept same logic)
    df["Is_New_Store"] = df["store"].str.contains("new", case=False, na=False).astype(int)

    # adjusted_need_qty (kept same rule)
    df["adjusted_need_qty"] = np.where(
        df["avg_qty"].isna() & ((df["available"].isna()) | (df["available"] == 0)),
        -3,
        df["need_qty"]
    ).astype(float)

    # ---------- 0.1) Bổ sung KHO TỔNG từ df_warehouse cho fdcode chưa có trong df ----------
    # df_merge được build từ left join dữ liệu bán hàng → KHO TỔNG không có dòng bán hàng
    # sẽ không xuất hiện trong filtered_df, dẫn đến A1 bị bỏ qua hoàn toàn.
    if df_warehouse is not None and not df_warehouse.empty:
        wh = df_warehouse.copy()
        wh["store"] = wh["store"].astype(str).str.strip()
        wh["fdcode"] = wh["fdcode"].astype(str).str.strip()
        wh["available"] = pd.to_numeric(wh["available"], errors="coerce").fillna(0)

        # Chỉ lấy fdcode KHO TỔNG có hàng mà chưa có trong df
        existing_wh_fdcodes = set(df[df["store"] == "KHO TỔNG"]["fdcode"].tolist())
        wh_rows = wh[
            (wh["store"] == "KHO TỔNG") &
            (~wh["fdcode"].isin(existing_wh_fdcodes)) &
            (wh["available"] > 0)
        ].copy()

        if not wh_rows.empty:
            # Điền các cột cần thiết để hoà hợp với df
            wh_rows["avg_qty"] = np.nan
            wh_rows["need_qty"] = wh_rows["available"]      # surplus
            wh_rows["adjusted_need_qty"] = wh_rows["available"]
            wh_rows["Is_New_Store"] = 0
            # Bổ sung các cột còn thiếu
            for col in df.columns:
                if col not in wh_rows.columns:
                    wh_rows[col] = np.nan
            df = pd.concat([df, wh_rows[[c for c in df.columns if c in wh_rows.columns]]], ignore_index=True)

    # ---------- 0.5) Filter to top 15 default_code per store (by avg_qty) ----------
    # KHO TỔNG không bị giới hạn (giữ nguyên để cung ứng đủ)
    if "default_code" in df.columns:
        df["default_code"] = df["default_code"].astype(str).str.strip()

        store_top15: dict = {}
        for store_name in df["store"].unique():
            if store_name.upper() == "KHO TỔNG":
                continue  # warehouse không lọc
            store_df = df[df["store"] == store_name]
            top15 = (
                store_df.groupby("default_code")["avg_qty"]
                .sum()
                .sort_values(ascending=False)
                .head(15)
                .index.tolist()
            )
            store_top15[store_name] = set(top15)

        if store_top15:
            stores_arr = df["store"].values
            dcs_arr = df["default_code"].values
            keep = [
                True if str(s).upper() == "KHO TỔNG"
                else (store_top15.get(s) is None or dc in store_top15.get(s, set()))
                for s, dc in zip(stores_arr, dcs_arr)
            ]
            df = df[keep].copy()

    # ---------- 1) Build stock ledger & avg_qty lookup ----------
    # Ledger holds CURRENT available that will be updated after each transfer
    # key: (store, fdcode)
    stock = {(r.store, r.fdcode): float(r.available) for r in df.itertuples(index=False)}
    avg_lookup = {
        (r.store, r.fdcode): (float(r.avg_qty) if pd.notna(r.avg_qty) else np.nan)
        for r in df.itertuples(index=False)
    }

    # helper functions
    def is_ecom(store_name: str) -> bool:
        return "ECOM" in store_name.upper()

    def is_kds(store_name: str) -> bool:
        return "KDS" in store_name.upper()

    def is_warehouse(store_name: str) -> bool:
        return store_name.upper() == "KHO TỔNG"

    def is_normal_store(store_name: str) -> bool:
        # normal store = not KDS, not ECOM (warehouse is handled separately)
        s = store_name.upper()
        return ("KDS" not in s) and ("ECOM" not in s) and (s != "KHO TỔNG")

    def cap_left_for_target(to_store: str, fdcode: str) -> float:
        """How many more units the target can receive (only for normal stores)."""
        if is_normal_store(to_store):
            cur = stock.get((to_store, fdcode), 0.0)
            return max(0.0, float(max_stock_normal_store) - cur)
        return float("inf")

    def min_stock_rule(from_store: str, fdcode: str, avg_qty_src: float, qty_stock: float) -> float:
        """Minimum stock to keep at a source store before it can be considered surplus."""
        if is_warehouse(from_store):
            # keep max(5, 20% of current)
            return max(5.0, float(int(qty_stock * 0.2)))
        if is_kds(from_store):
            if qty_stock < 50:
                return 10.0
            elif qty_stock <= 100:
                return float(int(qty_stock * 0.5))
            else:
                return float(int(qty_stock * (2 / 3)))
        if is_ecom(from_store):
            # keep more for online
            base = 0.0 if pd.isna(avg_qty_src) else float(avg_qty_src)
            return max(5.0, float(int(base * 1.5)))
        # normal source store
        base = 0.0 if pd.isna(avg_qty_src) else float(avg_qty_src)
        return max(3.0, base)

    def get_avg_qty(store_name: str, fdcode: str) -> float:
        val = avg_lookup.get((store_name, fdcode))
        return val if val is not None else np.nan

    def transferable_qty(from_store: str, to_store: str, fdcode: str, need_qty: float) -> int:
        """Compute how many units can be transferred from -> to for fdcode given need."""
        qty_stock = stock.get((from_store, fdcode), 0.0)
        if qty_stock <= 0 or need_qty <= 0:
            return 0

        avg_src = get_avg_qty(from_store, fdcode)
        min_stock = min_stock_rule(from_store, fdcode, avg_src, qty_stock)
        surplus = qty_stock - min_stock
        if surplus <= 0:
            return 0

        cap_left = cap_left_for_target(to_store, fdcode)
        allow = min(surplus, need_qty, cap_left)
        allow_int = int(round(allow))
        if allow_int <= 0:
            return 0

        # double-check min_stock preserved
        if (qty_stock - allow_int) < min_stock:
            return 0

        return allow_int

    def commit_transfer(from_store: str, to_store: str, fdcode: str, qty: int, results: list):
        if qty <= 0:
            return
        stock[(from_store, fdcode)] = stock.get((from_store, fdcode), 0.0) - qty
        stock[(to_store, fdcode)] = stock.get((to_store, fdcode), 0.0) + qty
        results.append({
            "from_store": from_store,
            "to_store": to_store,
            "fdcode": fdcode,
            "transfer_qty": int(qty)
        })

    # ---------- 2) Prepare need & surplus sets ----------
    # Need = adjusted_need_qty < 0 (exclude new store like your original)
    df_need = df[(df["adjusted_need_qty"] < 0) & (df["Is_New_Store"] != 1)].copy()

    # sort need by avg_qty desc (higher priority first). Is_New_Store already excluded but keep stable.
    df_need["avg_qty_sort"] = df_need["avg_qty"].fillna(-1)
    df_need = df_need.sort_values(by=["avg_qty_sort"], ascending=[False])

    need_msp_list = df_need["fdcode"].unique().tolist()

    # Surplus candidates = need_qty > 0 and not warehouse
    df_surplus = df[(df["need_qty"] > 0) & (df["store"] != "KHO TỔNG")].copy()

    results = []

    def _has_enough_surplus(src_store: str, msp: str, need_qty: float) -> bool:
        s = stock.get((src_store, msp), 0.0)
        if s <= 0:
            return False
        s_min = min_stock_rule(src_store, msp, get_avg_qty(src_store, msp), s)
        return (s - s_min) >= need_qty

    def src_priority(s):
        sU = str(s).upper()
        if "KDS" in sU:
            return 0
        if "ECOM" in sU:
            return 2
        return 1

    # ---------- 3) Stage A: fulfill explicit needs ----------
    for row in df_need.itertuples():
        msp = row.fdcode
        to_store = row.store

        # compute remaining need from adjusted_need_qty (use ledger current stock for cap)
        need_qty = float(abs(row.adjusted_need_qty))

        if need_qty <= 0:
            continue

        # A1) Try from KHO TỔNG first
        if (("KHO TỔNG", msp) in stock) and stock.get(("KHO TỔNG", msp), 0.0) > 0:
            qty_from_wh = transferable_qty("KHO TỔNG", to_store, msp, need_qty)
            if qty_from_wh > 0:
                commit_transfer("KHO TỔNG", to_store, msp, qty_from_wh, results)
                need_qty -= qty_from_wh

        if need_qty <= 0:
            continue

        # If KHO TỔNG or ECOM_SG still has enough surplus to cover remaining need, DO NOT take from ECOM.
        if _has_enough_surplus("KHO TỔNG", msp, need_qty) or _has_enough_surplus("ECOM_SG", msp, need_qty) or _has_enough_surplus("KDS", msp, need_qty):
            # Remaining could be served by KHO TỔNG or ECOM_SG; skip other sources to avoid draining ECOM.
            continue

        # A2) Try other stores (KDS first, then normal), ECOM last
        df_src = df_surplus[df_surplus["fdcode"] == msp].copy()
        if df_src.empty:
            continue

        df_src["priority"] = df_src["store"].apply(src_priority)
        df_src["avg_qty_sort"] = df_src["avg_qty"].fillna(-1)
        # priority asc (KDS->normal->ECOM), then avg_qty desc, then available desc
        df_src = df_src.sort_values(by=["priority", "avg_qty_sort", "available"], ascending=[True, False, False])

        for src in df_src.itertuples():
            if need_qty <= 0:
                break
            from_store = src.store
            qty = transferable_qty(from_store, to_store, msp, need_qty)
            if qty > 0:
                commit_transfer(from_store, to_store, msp, qty, results)
                need_qty -= qty

    # ---------- 4) Stage B: codes only in surplus (seed to zero-stock stores) ----------
    surplus_msps = df_surplus["fdcode"].unique().tolist()
    msps_only_in_surplus = set(surplus_msps) - set(need_msp_list)

    for msp in msps_only_in_surplus:
        # candidate stores: have this msp but available == 0 (or missing in ledger)
        candidates = df[(df["fdcode"] == msp) & (df["available"] <= 0)].copy()
        if candidates.empty:
            continue

        candidates["avg_qty_sort"] = candidates["avg_qty"].fillna(-1)
        candidates = candidates.sort_values(by=["avg_qty_sort"], ascending=[False])

        # Build ordered sources: KHO TỔNG first if exists, then KDS, then normal, then ECOM
        sources = []
        if ("KHO TỔNG", msp) in stock and stock.get(("KHO TỔNG", msp), 0.0) > 0:
            sources.append("KHO TỔNG")

        src_msp = df_surplus[df_surplus["fdcode"] == msp].copy()
        if not src_msp.empty:
            src_msp["priority"] = src_msp["store"].apply(lambda s: 0 if is_kds(str(s))
                                                         else (2 if is_ecom(str(s)) else 1))
            src_msp = src_msp.sort_values(by=["priority", "available"], ascending=[True, False])
            sources.extend(src_msp["store"].tolist())

        # remove dup but keep order
        seen = set()
        sources = [s for s in sources if not (s in seen or seen.add(s))]

        for tgt in candidates.itertuples():
            to_store = tgt.store
            estimated_need = 3.0

            # try each source once; each store gets at most 1 transfer for this code (like your original)
            moved = False
            for from_store in sources:
                qty = transferable_qty(from_store, to_store, msp, estimated_need)
                if qty > 0:
                    commit_transfer(from_store, to_store, msp, qty, results)
                    moved = True
                    break
            if not moved:
                continue

    return pd.DataFrame(results)

def stock_from_warehouse(
    filtered_df,
    df_warehouse,
    df_process_warehouse,
    max_stock_normal_store=3,
    df_warehouse_ecom=None,
    ecom_min_stock=10,
    ecom_max_stock=200,
    kds_max_stock=50,
    kds_focus_codes=None,
    allow_ecom_fallback_to_general=False,
    allow_store_fallback_to_ecom_sg=True,
    debug=True
):
    """
    Phân phối hàng từ kho về cửa hàng.

    ✅ FIX các lỗi hay gặp:
    1) need_remaining KHÔNG bị ghi đè khi df_need có nhiều dòng cùng (store, fdcode) → dùng groupby sum abs
    2) store_stock KHÔNG bị ghi đè khi filtered_df có nhiều dòng cùng (store, fdcode) → dùng groupby sum
    3) current được clamp >= 0 để tránh room bị phình nếu current âm
    4) Debug rõ ràng cho từng bước

    Logic:
    - STEP 0: ECOM_SG → ECOM (theo need)
    - STEP 1: KHO TỔNG → store vật lý (theo need, giới hạn room = max_cap - current)
    - STEP 2: Nếu còn dư kho: chia đều cho các store còn room
    """

    LOW_PRIORITY_STORES = {"201AEONHUE"}
    ECOM_STORE = "ECOM"
    ECOM_SOURCE = "ECOM_SG"
    KDS_STORE = "KDS"

    total_transfer_limit = 12000
    total_transferred = 0
    ecom_from_general_transferred = 0
    kds_from_ecom_sg_transferred = 0

    # Nếu truyền tay → build set ngay; nếu None → sẽ tự tính sau khi normalize data
    kds_focus_set = (
        {str(c).strip().upper() for c in kds_focus_codes}
        if kds_focus_codes is not None
        else None
    )

    def is_kds_allowed(msp):
        """Trả về True nếu mã được phép bốc cho KDS.
        Closure: nhìn thấy kds_focus_set sau khi được gán lại bên dưới."""
        if kds_focus_set is None:
            return True
        return str(msp).strip().upper() in kds_focus_set

    # =========================================================
    # NORMALIZE DATA
    # =========================================================
    filtered_df = filtered_df.copy()
    filtered_df["store"] = filtered_df["store"].astype(str).str.strip()
    filtered_df["fdcode"] = filtered_df["fdcode"].astype(str).str.strip().str.upper()

    for df_name, df_obj in [
        ("df_warehouse", df_warehouse),
        ("df_warehouse_ecom", df_warehouse_ecom),
        ("df_process_warehouse", df_process_warehouse),
    ]:
        if df_obj is not None and not df_obj.empty:
            df_obj = df_obj.copy()
            df_obj["fdcode"] = df_obj["fdcode"].astype(str).str.strip().str.upper()
            if df_name == "df_warehouse":
                df_warehouse = df_obj
            elif df_name == "df_warehouse_ecom":
                df_warehouse_ecom = df_obj
            else:
                df_process_warehouse = df_obj

    # =========================================================
    # AUTO-COMPUTE KDS FOCUS CODES (nếu không truyền tay)
    # =========================================================
    # Logic:
    #   1) Group theo default_code (mã cha) → tổng avg_qty tại KDS → top 15 default_code
    #   2) Lấy tất cả fdcode con thuộc 15 default_code đó → kds_focus_set
    # Thực hiện SAU khi filtered_df đã normalize để fdcode đã là uppercase
    if kds_focus_codes is None:
        kds_df = filtered_df[filtered_df["store"].str.upper() == KDS_STORE]
        if (
            not kds_df.empty
            and "avg_qty" in kds_df.columns
            and "default_code" in kds_df.columns
        ):
            # Bước 1: top 15 default_code theo tổng avg_qty
            top15_default = (
                kds_df.groupby("default_code")["avg_qty"]
                .sum()
                .sort_values(ascending=False)
                .head(15)
            )
            top15_default_set = set(top15_default.index.tolist())

            # Bước 2: lấy tất cả fdcode thuộc 15 default_code đó
            kds_focus_set = set(
                filtered_df.loc[
                    filtered_df["default_code"].isin(top15_default_set), "fdcode"
                ].unique().tolist()
            )

            if debug:
                print(f"\n📊 KDS top 15 default_code (auto, theo avg_qty):")
                for rank, (code, val) in enumerate(top15_default.items(), 1):
                    fdcodes = filtered_df.loc[
                        filtered_df["default_code"] == code, "fdcode"
                    ].unique().tolist()
                    print(f"   {rank:2d}. {code}  avg_qty={val:.1f}  → {len(fdcodes)} fdcode")
                print(f"   Tổng fdcode được bốc cho KDS: {len(kds_focus_set)}")
        # Nếu KDS không có dữ liệu → kds_focus_set giữ nguyên None (bốc tất cả)

    # =========================================================
    # HELPER FUNCTIONS
    # =========================================================
    def is_ecom_store(s):
        return str(s).strip().upper() == ECOM_STORE

    def is_kds_store(s):
        return str(s).strip().upper() == KDS_STORE

    def is_low_priority(store):
        return str(store).strip() in LOW_PRIORITY_STORES

    def get_max_stock(store):
        """Cap max (tồn hiện tại + nhận thêm) tại store"""
        if is_ecom_store(store):
            return ecom_max_stock
        if is_kds_store(store):
            return kds_max_stock
        if is_low_priority(store):
            return 1
        return max_stock_normal_store

    def get_wh_qty(df_wh, msp):
        """Tồn kho hiện tại của 1 mã tại 1 nguồn kho"""
        if df_wh is None or df_wh.empty:
            return 0
        return int(df_wh.loc[df_wh["fdcode"] == msp, "available"].fillna(0).sum())

    def wh_decrease(df_wh, msp, qty):
        if df_wh is None or df_wh.empty or qty <= 0:
            return
        idx = df_wh["fdcode"] == msp
        if idx.any():
            df_wh.loc[idx, "available"] = (df_wh.loc[idx, "available"].fillna(0) - qty).clip(lower=0)

    transfers = defaultdict(int)
    store_stock = defaultdict(int)
    need_remaining = defaultdict(int)

    def add_transfer(from_store, to_store, msp, qty):
        nonlocal total_transferred, ecom_from_general_transferred, kds_from_ecom_sg_transferred
        if qty <= 0:
            return

        transfers[(from_store, to_store, msp)] += qty
        total_transferred += qty

        if to_store == ECOM_STORE and from_store == "KHO TỔNG":
            ecom_from_general_transferred += qty
        if to_store == KDS_STORE and from_store == ECOM_SOURCE:
            kds_from_ecom_sg_transferred += qty

        # update stock & need
        store_stock[(to_store, msp)] += qty
        if (to_store, msp) in need_remaining:
            need_remaining[(to_store, msp)] = max(0, need_remaining[(to_store, msp)] - qty)

    # =========================================================
    # PREP NEED DATA
    # =========================================================
    filtered_df["Is_New_Store"] = filtered_df["store"].apply(lambda x: 1 if "new" in str(x).lower() else 0)

    df_need = filtered_df[
        (filtered_df["need_qty"] < 0) &
        (filtered_df["Is_New_Store"] != 1)
    ].copy()

    # Tổng nhu cầu theo store (để tham khảo)
    store_total_need = df_need.groupby("store")["need_qty"].sum().abs().to_dict()
    df_need["store_total_need"] = df_need["store"].map(store_total_need)
    df_need["is_low_priority"] = df_need["store"].apply(lambda s: 1 if is_low_priority(s) else 0)

    # Sort:
    # 1) store thường trước
    # 2) need_qty âm nhiều nhất trước
    # 3) avg_qty cao nhất trước
    df_need = df_need.sort_values(
        by=["is_low_priority", "need_qty", "avg_qty"],
        ascending=[True, True, False]
    )

    # Store list (loại ECOM_SG)
    store_list = filtered_df["store"].dropna().unique().tolist()
    store_list = [s for s in store_list if str(s).strip().upper() != ECOM_SOURCE]

    # =========================================================
    # INIT STATE TRACKING (✅ FIX: GROUPBY để không bị ghi đè)
    # =========================================================
    # store_stock: sum available theo (store, fdcode)
    stock_map = (
        filtered_df
        .groupby(["store", "fdcode"])["available"]
        .sum()
        .fillna(0)
        .astype(int)
        .to_dict()
    )
    for k, v in stock_map.items():
        store_stock[k] = int(v)

    # need_remaining: sum abs(need_qty) theo (store, fdcode)  ✅ FIX
    need_map = (
        df_need
        .groupby(["store", "fdcode"])["need_qty"]
        .sum()
        .abs()
        .fillna(0)
        .astype(int)
        .to_dict()
    )
    for k, v in need_map.items():
        need_remaining[k] = int(v)

    # Tổng tồn kho
    wh_total_limit = int(df_warehouse["available"].fillna(0).sum()) if df_warehouse is not None else 0
    wh_ecom_limit = int(df_warehouse_ecom["available"].fillna(0).sum()) if (df_warehouse_ecom is not None and not df_warehouse_ecom.empty) else 0
    has_ecom_store = any(is_ecom_store(s) for s in store_list)
    has_kds_store = any(is_kds_store(s) for s in store_list)
    has_ecom_source_target = has_ecom_store or has_kds_store

    if debug:
        print(f"\n{'='*80}")
        print("🔧 STOCK_FROM_WAREHOUSE CONFIG:")
        print(f"   max_stock_normal_store          = {max_stock_normal_store}")
        print(f"   ecom_max_stock                  = {ecom_max_stock}")
        print(f"   kds_max_stock                   = {kds_max_stock}")
        kds_focus_label = "auto top15 avg_qty" if kds_focus_codes is None else "truyền tay"
        print(f"   kds_focus_codes ({kds_focus_label}) = {sorted(kds_focus_set) if kds_focus_set else 'ALL'}")
        print(f"   allow_ecom_fallback_to_general  = {allow_ecom_fallback_to_general}")
        print(f"   allow_store_fallback_to_ecom_sg = {allow_store_fallback_to_ecom_sg}")
        print(f"   wh_total_limit                  = {wh_total_limit}")
        print(f"   wh_ecom_limit                   = {wh_ecom_limit}")
        print(f"   has_ecom_store                  = {has_ecom_store}")
        print(f"   has_kds_store                   = {has_kds_store}")
        print(f"{'='*80}\n")

    # =========================================================
    # STEP 0: ECOM_SG → ECOM ONLY (BY NEED)
    #   - KDS được xử lý SAU physical stores (STEP 1.7) để ưu tiên stores trước
    # =========================================================
    if df_warehouse_ecom is not None and not df_warehouse_ecom.empty and wh_ecom_limit > 0 and has_ecom_store:
        if debug:
            print("📦 STEP 0: ECOM_SG → ECOM (BY NEED)")

        ecom_need_items = [
            (store, msp, need)
            for (store, msp), need in need_remaining.items()
            if is_ecom_store(store) and need > 0
        ]

        for (to_store, msp, _) in ecom_need_items:
            if total_transferred >= total_transfer_limit or wh_ecom_limit <= 0:
                break

            need_qty = need_remaining[(to_store, msp)]
            if need_qty <= 0:
                continue

            current = max(0, store_stock[(to_store, msp)])  # ✅ clamp
            max_cap = get_max_stock(to_store)
            room = max_cap - current
            if room <= 0:
                continue

            src_qty = get_wh_qty(df_warehouse_ecom, msp)
            if src_qty <= 0:
                continue

            give = min(src_qty, need_qty, room, wh_ecom_limit, total_transfer_limit - total_transferred)
            if give > 0:
                add_transfer(ECOM_SOURCE, to_store, msp, give)
                wh_decrease(df_warehouse_ecom, msp, give)
                wh_ecom_limit -= give

                if debug:
                    print(f"   ✅ {msp}: ECOM_SG → {to_store} {give} (need={need_qty}, room={room}, current={current})")
    
    # =========================================================
    # STEP 1: KHO TỔNG → PHYSICAL STORES (BY NEED)
    #   - Duyệt theo df_need (ưu tiên thiếu nhiều)
    #   - give <= min(need_remaining, room, wh_qty)
    # =========================================================
    if debug:
        print(f"\n📦 STEP 1: KHO TỔNG → PHYSICAL STORES (BY NEED, max_stock_normal_store={max_stock_normal_store})")

    # Duyệt theo df_need nhưng khi cấp, lấy theo need_remaining đã groupby
    seen_pairs = set()  # tránh lặp quá nhiều nếu df_need có nhiều dòng cùng store+msp (vẫn OK, nhưng đỡ spam)
    for _, row_need in df_need.iterrows():
        if total_transferred >= total_transfer_limit or wh_total_limit <= 0:
            break

        msp = row_need["fdcode"]
        to_store = row_need["store"]

        if is_ecom_store(to_store):
            continue

        # Chỉ bốc cho KDS những mã nằm trong kds_focus_set
        if is_kds_store(to_store) and not is_kds_allowed(msp):
            continue

        key = (to_store, msp)
        if key in seen_pairs:
            continue
        seen_pairs.add(key)

        need_qty = need_remaining.get(key, 0)
        if need_qty <= 0:
            continue

        current = max(0, store_stock.get(key, 0))  # ✅ clamp
        max_cap = get_max_stock(to_store)
        room = max_cap - current
        if room <= 0:
            continue

        wh_qty = get_wh_qty(df_warehouse, msp)
        if wh_qty <= 0:
            continue

        give = min(wh_qty, need_qty, room, wh_total_limit, total_transfer_limit - total_transferred)
        if give > 0:
            add_transfer("KHO TỔNG", to_store, msp, give)
            wh_decrease(df_warehouse, msp, give)
            wh_total_limit -= give

            if debug:
                print(
                    f"   ✅ {msp}: KHO TỔNG → {to_store} {give} "
                    f"(need={need_qty}, current={current}, max={max_cap}, room={room}, wh={wh_qty})"
                )
        # =========================================================
    # STEP 1.5: KHO TỔNG → ECOM (FALLBACK BY NEED)
    #   - Chỉ chạy sau khi đã cấp cho physical stores
    # =========================================================
    if allow_ecom_fallback_to_general and has_ecom_store and wh_total_limit > 0:
        if debug:
            print("\n📦 STEP 1.5: KHO TỔNG → ECOM (FALLBACK AFTER PHYSICAL STORES)")

        ecom_need_items = sorted(
            [
                (store, msp, need)
                for (store, msp), need in need_remaining.items()
                if is_ecom_store(store) and need > 0
            ],
            key=lambda x: x[2],
            reverse=True
        )

        for (to_store, msp, _) in ecom_need_items:
            if total_transferred >= total_transfer_limit or wh_total_limit <= 0:
                break

            need_qty = need_remaining.get((to_store, msp), 0)
            if need_qty <= 0:
                continue

            current = max(0, store_stock.get((to_store, msp), 0))
            room = ecom_max_stock - current
            if room <= 0:
                continue

            wh_qty = get_wh_qty(df_warehouse, msp)
            if wh_qty <= 0:
                continue

            give = min(
                wh_qty,
                need_qty,
                room,
                wh_total_limit,
                total_transfer_limit - total_transferred
            )

            if give > 0:
                add_transfer("KHO TỔNG", ECOM_STORE, msp, give)
                wh_decrease(df_warehouse, msp, give)
                wh_total_limit -= give

                if debug:
                    print(
                        f"   ✅ {msp}: KHO TỔNG → ECOM {give} "
                        f"(need={need_qty}, current={current}, room={room}, wh={wh_qty})"
                    )
    # =========================================================
    # STEP 1.6: ECOM_SG → PHYSICAL STORES (FALLBACK KHI KHO TỔNG THIẾU)
    #   - Chỉ chạy nếu allow_store_fallback_to_ecom_sg=True
    #   - Bổ sung cho store vật lý (không phải ECOM, không phải KDS) còn need_remaining > 0
    # =========================================================
    if allow_store_fallback_to_ecom_sg and df_warehouse_ecom is not None and not df_warehouse_ecom.empty and wh_ecom_limit > 0:
        if debug:
            print("\n📦 STEP 1.6: ECOM_SG → PHYSICAL STORES (FALLBACK KHI KHO TỔNG THIẾU)")

        store_fallback_items = sorted(
            [
                (store, msp, need)
                for (store, msp), need in need_remaining.items()
                if not is_ecom_store(store) and not is_kds_store(store) and need > 0
            ],
            key=lambda x: x[2],
            reverse=True
        )

        for (to_store, msp, _) in store_fallback_items:
            if total_transferred >= total_transfer_limit or wh_ecom_limit <= 0:
                break

            need_qty = need_remaining.get((to_store, msp), 0)
            if need_qty <= 0:
                continue

            current = max(0, store_stock.get((to_store, msp), 0))
            max_cap = get_max_stock(to_store)
            room = max_cap - current
            if room <= 0:
                continue

            src_qty = get_wh_qty(df_warehouse_ecom, msp)
            if src_qty <= 0:
                continue

            give = min(src_qty, need_qty, room, wh_ecom_limit, total_transfer_limit - total_transferred)
            if give > 0:
                add_transfer(ECOM_SOURCE, to_store, msp, give)
                wh_decrease(df_warehouse_ecom, msp, give)
                wh_ecom_limit -= give

                if debug:
                    print(
                        f"   ✅ {msp}: ECOM_SG → {to_store} {give} "
                        f"(need={need_qty}, current={current}, max={max_cap}, room={room}, src={src_qty})"
                    )

    # =========================================================
    # STEP 1.7: ECOM_SG → KDS (BY NEED, SAU KHI ĐÃ ƯU TIÊN STORES)
    #   - Chạy sau STEP 1.6 để đảm bảo physical stores được bốc trước KDS
    # =========================================================
    if has_kds_store and df_warehouse_ecom is not None and not df_warehouse_ecom.empty and wh_ecom_limit > 0:
        if debug:
            print("\n📦 STEP 1.7: ECOM_SG → KDS (BY NEED, ƯU TIÊN SAU STORES)")

        kds_need_items = [
            (store, msp, need)
            for (store, msp), need in need_remaining.items()
            if is_kds_store(store) and need > 0
        ]

        for (to_store, msp, _) in kds_need_items:
            if total_transferred >= total_transfer_limit or wh_ecom_limit <= 0:
                break

            if not is_kds_allowed(msp):
                continue

            need_qty = need_remaining[(to_store, msp)]
            if need_qty <= 0:
                continue

            current = max(0, store_stock[(to_store, msp)])
            max_cap = get_max_stock(to_store)
            room = max_cap - current
            if room <= 0:
                continue

            src_qty = get_wh_qty(df_warehouse_ecom, msp)
            if src_qty <= 0:
                continue

            give = min(src_qty, need_qty, room, wh_ecom_limit, total_transfer_limit - total_transferred)
            if give > 0:
                add_transfer(ECOM_SOURCE, to_store, msp, give)
                wh_decrease(df_warehouse_ecom, msp, give)
                wh_ecom_limit -= give

                if debug:
                    print(f"   ✅ {msp}: ECOM_SG → {to_store} {give} (need={need_qty}, room={room}, current={current})")

    # =========================================================
    # STEP 2.1: KHO TỔNG → ECOM (TOP-UP TỐI THIỂU 30)
    #   - Chỉ top-up các fdcode ECOM thực sự đang kinh doanh (có trong filtered_df)
    #   - Không giới hạn bởi wh_total_limit vì wh_qty đã giới hạn per-product
    # =========================================================
    ECOM_MIN_TOPUP = 30

    # Tập hợp fdcode mà ECOM thực sự có trong filtered_df
    ecom_fdcodes = {fdcode for (store, fdcode) in stock_map.keys() if store == ECOM_STORE}

    if has_ecom_store and ecom_fdcodes and df_warehouse is not None and not df_warehouse.empty:
        if debug:
            print("\n📦 STEP 2.1: KHO TỔNG → ECOM (TOP-UP TỐI THIỂU 30)")

        wh_left_topup = df_warehouse[
            (df_warehouse["available"].fillna(0) > 0) &
            (df_warehouse["fdcode"].isin(ecom_fdcodes))
        ].copy()

        for msp, group in wh_left_topup.groupby("fdcode"):
            if total_transferred >= total_transfer_limit:
                break

            current_ecom = max(0, store_stock.get((ECOM_STORE, msp), 0))
            if current_ecom >= ECOM_MIN_TOPUP:
                continue

            need_topup = ECOM_MIN_TOPUP - current_ecom
            wh_qty = get_wh_qty(df_warehouse, msp)
            if wh_qty <= 0:
                continue

            # wh_qty đã giới hạn per-product, không cần wh_total_limit ở đây
            give = min(need_topup, wh_qty, total_transfer_limit - total_transferred)
            if give > 0:
                add_transfer("KHO TỔNG", ECOM_STORE, msp, give)
                wh_decrease(df_warehouse, msp, give)

                if debug:
                    print(
                        f"   ✅ {msp}: KHO TỔNG → ECOM +{give} "
                        f"(tồn_ecom={current_ecom}, cần_thêm={need_topup}, kho={wh_qty})"
                    )

    # Tính lại wh_total_limit từ tồn thực tế cho STEP 2
    wh_total_limit = int(df_warehouse["available"].fillna(0).sum()) if df_warehouse is not None else 0

    # =========================================================
    # STEP 2: KHO TỔNG → PHYSICAL STORES (EVEN DISTRIBUTION - LEFTOVER)
    #   - Chia đều hàng còn dư cho các store còn room
    # =========================================================
    if debug:
        print("\n📦 STEP 2: KHO TỔNG → PHYSICAL STORES (EVEN DISTRIBUTION - LEFTOVER)")

    if df_warehouse is not None and not df_warehouse.empty and wh_total_limit > 0 and total_transferred < total_transfer_limit:
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
                if is_ecom_store(store):
                    continue

                # KDS chỉ nhận leftover nếu mã nằm trong kds_focus_set
                if is_kds_store(store) and not is_kds_allowed(msp):
                    continue

                current = max(0, store_stock.get((store, msp), 0))  # ✅ clamp
                max_cap = get_max_stock(store)
                room = max_cap - current
                if room > 0:
                    priority = 1 if is_low_priority(store) else 0
                    store_candidates.append((store, current, room, priority))

            if not store_candidates:
                continue

            # store thường trước, rồi theo tồn ít nhất
            store_candidates.sort(key=lambda x: (x[3], x[1]))

            qty_per_store = give_all // len(store_candidates)
            remainder = give_all % len(store_candidates)

            distributed = 0
            for i, (store, current, room, priority) in enumerate(store_candidates):
                if total_transferred >= total_transfer_limit or wh_total_limit <= 0:
                    break
                if room <= 0:
                    continue

                intended = qty_per_store + (1 if i < remainder else 0)
                give = min(intended, room, give_all - distributed, wh_total_limit, total_transfer_limit - total_transferred)
                if give > 0:
                    add_transfer("KHO TỔNG", store, msp, give)
                    distributed += give
                    wh_total_limit -= give

            if distributed > 0:
                wh_decrease(df_warehouse, msp, distributed)

                if debug:
                    print(f"   🔁 {msp}: distributed leftover={distributed} to {len(store_candidates)} stores")

    # =========================================================
    # OUTPUT
    # =========================================================
    # Convert transfers dict to DataFrame
    rows = []
    for (from_store, to_store, msp), qty in transfers.items():
        rows.append({
            "from_store": from_store,
            "to_store": to_store,
            "fdcode": msp,
            "qty": int(qty)
        })

    df_transfers = pd.DataFrame(rows)
    if debug:
        print(f"\n✅ DONE. total_transferred={total_transferred}, ecom_from_general={ecom_from_general_transferred}")
        if not df_transfers.empty:
            print(df_transfers.head(30))
    
    # STEP 2.5) EVEN DISTRIBUTION FROM ECOM_SG LEFTOVER → ECOM & KDS
    if debug:
        print(f"\n📦 STEP 2.5: ECOM_SG → ECOM & KDS (EVEN DISTRIBUTION)")

    if df_warehouse_ecom is not None and not df_warehouse_ecom.empty and wh_ecom_limit > 0 and has_ecom_source_target:
        wh_left_ecom = df_warehouse_ecom[df_warehouse_ecom["available"].fillna(0) > 0].copy()

        for msp, group in wh_left_ecom.groupby("fdcode"):
            if total_transferred >= total_transfer_limit or wh_ecom_limit <= 0:
                break

            total_qty = int(group["available"].sum())
            give_all = min(total_qty, wh_ecom_limit, total_transfer_limit - total_transferred)
            if give_all <= 0:
                continue

            # Phục vụ từng target store (ECOM và KDS) từ lượng còn lại
            for target_store, max_cap in [
                (ECOM_STORE, ecom_max_stock) if has_ecom_store else (None, 0),
                (KDS_STORE, kds_max_stock) if has_kds_store else (None, 0),
            ]:
                if target_store is None or wh_ecom_limit <= 0 or give_all <= 0:
                    continue

                # KDS chỉ nhận leftover ECOM_SG nếu mã nằm trong kds_focus_set
                if target_store == KDS_STORE and not is_kds_allowed(msp):
                    continue

                current = store_stock[(target_store, msp)]
                if current >= max_cap:
                    if debug:
                        print(f"   ⚠️  {msp}: {target_store} đã đạt max ({current} >= {max_cap})")
                    continue

                room = max_cap - current
                if room <= 0:
                    continue

                give = min(give_all, room, wh_ecom_limit, total_transfer_limit - total_transferred)

                if debug and msp == "M5GIM0009":
                    print(f"   🔍 M5GIM0009 → {target_store}:")
                    print(f"      current = {current}, room = {room}, give_all = {give_all}, give = {give}")

                if give > 0:
                    add_transfer(ECOM_SOURCE, target_store, msp, give)
                    wh_decrease(df_warehouse_ecom, msp, give)
                    wh_ecom_limit -= give
                    give_all -= give

                    if debug and msp == "M5GIM0009":
                        print(f"      ✅ TRANSFERRED {give} from ECOM_SG → {target_store}")

    # FINAL SUMMARY
    if debug:
        print(f"\n{'='*80}")
        print(f"📊 FINAL SUMMARY:")
        print(f"   Total transferred (ALL) = {total_transferred}")
        print(f"   - To ECOM from KHO TỔNG   = {ecom_from_general_transferred}")
        print(f"   - To KDS from ECOM_SG      = {kds_from_ecom_sg_transferred}")
        print(f"   wh_total_limit remaining = {wh_total_limit}")
        print(f"   wh_ecom_limit remaining = {wh_ecom_limit}")
        print(f"{'='*80}\n")

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

# RÚT HÀNG
def withdraw_from_stores(withdraw_df, df_merge, df_warehouse=None, df_warehouse_ecom=None):
    """
    Rút hàng từ các cửa hàng/kho theo mã sản phẩm.
    
    Quy tắc:
    1. Ưu tiên rút theo thứ tự: KHO TỔNG → KDS → ECOM_SG → ECOM → các CH còn lại
    2. Rút theo mức độ dư thừa (available cao nhất) trong cùng nhóm ưu tiên
    3. Loại trừ các cửa hàng ngoại thành: 101AEONHAIPHONG, 201AEONHUE, 355MAUTHAN
    4. Trả về kết quả: từ kho/CH nào, mã SP nào, số lượng bao nhiêu
    
    Args:
        withdraw_df: DataFrame chứa danh sách cần rút (fdcode, qty)
        df_merge: DataFrame hiện trạng tồn kho stores (store, fdcode, available, need_qty)
        df_warehouse: DataFrame tồn kho KHO TỔNG (optional)
        df_warehouse_ecom: DataFrame tồn kho ECOM_SG (optional)
    
    Returns:
        result_df: DataFrame kết quả rút hàng (fdcode, from_store, withdraw_qty)
        updated_df_merge: DataFrame đã cập nhật sau khi rút
    """
    
    # ✅ DANH SÁCH CỬA HÀNG KHÔNG ĐƯỢC RÚT HÀNG (ngoại thành)
    EXCLUDED_STORES = {'101AEONHAIPHONG', '201AEONHUE', '355MAUTHAN'}
    
    result_list = []
    df_merge = df_merge.copy()
    
    # ✅ GỘP CÁC KHO VÀO DF_MERGE ĐỂ XỬ LÝ CHUNG
    df_all_stock = df_merge.copy()
    
    if df_warehouse is not None and not df_warehouse.empty:
        df_wh = df_warehouse[['store', 'fdcode', 'available']].copy()
        df_wh['need_qty'] = 0  # Kho không có need_qty
        df_all_stock = pd.concat([df_all_stock, df_wh], ignore_index=True)
    
    if df_warehouse_ecom is not None and not df_warehouse_ecom.empty:
        df_wh_ecom = df_warehouse_ecom[['store', 'fdcode', 'available']].copy()
        df_wh_ecom['need_qty'] = 0
        df_all_stock = pd.concat([df_all_stock, df_wh_ecom], ignore_index=True)
    
    # ✅ LỌC BỎ CÁC CỬA HÀNG KHÔNG ĐƯỢC RÚT
    df_all_stock = df_all_stock[~df_all_stock['store'].isin(EXCLUDED_STORES)].copy()
    
    # Chuẩn hóa tên store
    def normalize_store_name(name):
        """Chuẩn hóa tên store: loại bỏ khoảng trắng thừa, chuyển chữ hoa"""
        return str(name).strip().upper().replace(' ', '').replace('_', '')
    
    # Định nghĩa thứ tự ưu tiên
    priority_groups = {
        1: ['KHOTỔNG', 'KHOTONG', 'KDS', 'ECOMSG'],
        2: ['ECOM'],
        3: []
    }
    
    def get_priority(store_name):
        """Xác định mức ưu tiên của store"""
        store_normalized = normalize_store_name(store_name)
        
        for priority_store in priority_groups[1]:
            if priority_store in store_normalized or store_normalized in priority_store:
                return 1
        
        for priority_store in priority_groups[2]:
            if priority_store == store_normalized:
                return 2
        
        return 3
    
    # Debug: In ra danh sách store và priority
    print("\n" + "="*80)
    print("🔍 DANH SÁCH STORE VÀ PRIORITY (SAU KHI GỘP KHO & LỌC LOẠI TRỪ):")
    print("="*80)
    print(f"⛔ Các cửa hàng bị loại trừ: {', '.join(EXCLUDED_STORES)}")
    print("-"*80)
    unique_stores = df_all_stock['store'].unique()
    for store in sorted(unique_stores):
        priority = get_priority(store)
        available_count = len(df_all_stock[df_all_stock['store'] == store])
        total_qty = df_all_stock[df_all_stock['store'] == store]['available'].sum()
        print(f"  {store:20} (normalized: {normalize_store_name(store):15}) → Priority: {priority} | {available_count} mã | Tổng: {total_qty:,.0f}")
    print("="*80 + "\n")
    
    # Thống kê tổng quan
    total_stores = len(unique_stores)
    total_products = len(df_all_stock)
    total_qty_available = df_all_stock['available'].sum()
    
    print(f"📊 TỔNG QUAN:")
    print(f"  - Tổng số kho/CH tham gia: {total_stores}")
    print(f"  - Tổng số dòng sản phẩm: {total_products}")
    print(f"  - Tổng số lượng available: {total_qty_available:,.0f}")
    print("="*80 + "\n")
    
    # Thống kê rút hàng
    total_qty_requested = 0
    total_qty_withdrawn = 0
    products_processed = 0
    products_fully_withdrawn = 0
    products_partially_withdrawn = 0
    
    for _, row in withdraw_df.iterrows():
        msp = row['fdcode']
        total_qty_needed = int(row['qty'])
        total_qty_requested += total_qty_needed
        
        if total_qty_needed <= 0:
            continue
        
        products_processed += 1
        
        # Lấy danh sách store có sản phẩm này từ df_all_stock
        df_available = df_all_stock[
            (df_all_stock['fdcode'] == msp) & 
            (df_all_stock['available'] > 0)
        ].copy()
        
        if df_available.empty:
            print(f"⚠️  Cảnh báo: Không có hàng available cho {msp}")
            continue
        
        # Thêm cột priority và sắp xếp
        df_available['priority'] = df_available['store'].apply(get_priority)
        
        # Debug: In ra store có hàng cho mã này
        print(f"\n📦 Rút hàng cho {msp} (cần: {total_qty_needed})")
        print(f"Các store có hàng:")
        for idx, av_row in df_available.iterrows():
            print(f"  - {av_row['store']:20} (Priority {av_row['priority']}): available = {av_row['available']:,.0f}")
        
        # Sắp xếp theo priority, rồi available
        df_available = df_available.sort_values(
            by=['priority', 'available'], 
            ascending=[True, False]
        )
        
        # Debug: In ra thứ tự rút
        print(f"Thứ tự rút (sau khi sắp xếp):")
        for idx, av_row in df_available.iterrows():
            print(f"  {av_row['priority']}. {av_row['store']:20}: {av_row['available']:,.0f}")
        
        # --- Bắt đầu rút hàng ---
        remaining_qty = total_qty_needed
        qty_withdrawn_this_product = 0
        
        for idx, avail_row in df_available.iterrows():
            if remaining_qty <= 0:
                break
            
            store = avail_row['store']
            available_qty = avail_row['available']
            
            # Số lượng rút từ store này
            withdraw_qty = min(remaining_qty, available_qty)
            
            if withdraw_qty <= 0:
                continue
            
            # Ghi nhận kết quả
            result_list.append({
                'fdcode': msp,
                'from_store': store,
                'withdraw_qty': int(withdraw_qty)
            })
            
            print(f"  ✅ Rút {withdraw_qty} từ {store}")
            
            # Cập nhật df_all_stock
            df_all_stock.loc[idx, 'available'] -= withdraw_qty
            if 'need_qty' in df_all_stock.columns:
                df_all_stock.loc[idx, 'need_qty'] -= withdraw_qty
            
            remaining_qty -= withdraw_qty
            qty_withdrawn_this_product += withdraw_qty
        
        total_qty_withdrawn += qty_withdrawn_this_product
        
        # Kiểm tra nếu không rút đủ
        if remaining_qty > 0:
            print(f"⚠️  {msp} chỉ rút được {total_qty_needed - remaining_qty}/{total_qty_needed}")
            products_partially_withdrawn += 1
        else:
            print(f"✅ {msp} đã rút đủ {total_qty_needed}")
            products_fully_withdrawn += 1
    
    # ✅ CẬP NHẬT LẠI DF_MERGE, DF_WAREHOUSE, DF_WAREHOUSE_ECOM
    # Tách lại các phần đã update
    df_merge_updated = df_all_stock[
        ~df_all_stock['store'].isin(['KHO TỔNG', 'ECOM_SG'] + list(EXCLUDED_STORES))
    ].copy()
    
    df_warehouse_updated = df_all_stock[df_all_stock['store'] == 'KHO TỔNG'].copy() if df_warehouse is not None else None
    df_warehouse_ecom_updated = df_all_stock[df_all_stock['store'] == 'ECOM_SG'].copy() if df_warehouse_ecom is not None else None
    
    # Tạo DataFrame kết quả
    if result_list:
        result_df = pd.DataFrame(result_list, columns=['fdcode', 'from_store', 'withdraw_qty'])
    else:
        result_df = pd.DataFrame(columns=['fdcode', 'from_store', 'withdraw_qty'])
    
    # ✅ IN THỐNG KÊ CUỐI CÙNG
    print("\n" + "="*80)
    print("📊 THỐNG KÊ KẾT QUẢ RÚT HÀNG:")
    print("="*80)
    print(f"  📝 Tổng số mã yêu cầu: {len(withdraw_df)}")
    print(f"  ✅ Số mã đã xử lý: {products_processed}")
    print(f"  🎯 Số mã rút đủ: {products_fully_withdrawn}")
    print(f"  ⚠️  Số mã rút thiếu: {products_partially_withdrawn}")
    print(f"  ❌ Số mã không rút được: {len(withdraw_df) - products_processed}")
    print("-"*80)
    print(f"  📦 Tổng số lượng yêu cầu: {total_qty_requested:,.0f}")
    print(f"  ✅ Tổng số lượng đã rút: {total_qty_withdrawn:,.0f}")
    print(f"  📉 Tỷ lệ hoàn thành: {(total_qty_withdrawn/total_qty_requested*100) if total_qty_requested > 0 else 0:.1f}%")
    
    if not result_df.empty:
        print("-"*80)
        print("  🏪 Phân bổ theo kho/cửa hàng:")
        store_summary = result_df.groupby('from_store')['withdraw_qty'].sum().sort_values(ascending=False)
        for store, qty in store_summary.items():
            print(f"     - {store:20}: {qty:>8,.0f} ({qty/total_qty_withdrawn*100:>5.1f}%)")
    
    print("="*80 + "\n")
    
    return result_df, df_merge_updated, df_warehouse_updated, df_warehouse_ecom_updated

if __name__ == "__main__":
    initialize_data()