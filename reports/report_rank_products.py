from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from core.sheets import get_client
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import gspread_dataframe as gd
import os

gs = get_client()
sht = gs.open_by_key('1DaB_1F0c4ZPvWPXzd-DpmKYQIBhzaDTFSdZ2p_yXsZM')
SHEET1 = 'RAW_SEMI'

engine = get_engine()
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
    AND ps.code IS NOT NULL
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_template_fix = pd.read_sql_query(text(query_products_template), conn)
print('Finished querying the template')

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
        WHERE depot_id NOT IN (110819, 111154, 101011, 111753, 125224, 142410, 217633, 217642, 110826, 111155, 111752, 220636, 142408, 222877)
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
            AND pih.depot_id NOT IN (110819, 111154, 101011, 111753, 125224, 142410, 217633, 217642, 110826, 111155, 111752, 220636, 142408, 222877)
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
print('Finished querying the stock')

df_stock = pd.merge(df_stock, df_template_fix[['fdcode', 'default_code']], on='fdcode', how='left')
df_stock_filter = df_stock[df_stock['category'].isin(['SANDALS', 'SLIDES', 'SNEAKERS', 'KID SNEAKERS', 'KID SANDALS'])]
df_stock_gr = df_stock_filter.groupby(['default_code', 'subcategory', 'category']).agg({
    'available':'sum'
}).reset_index()

uery_sales_year_days = """
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
    )
    # sales order
    SELECT 
        so.orderId AS order_id,
        DATE(so.createdDateTime) AS date_order,
        CASE 
                    WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
            WHEN so.channelName ='Kho Lẻ' THEN 'KDC'
            WHEN  st.code_nhanh = 'KHO SỈ' THEN 'KDS'
            WHEN so.saleChannel IN(1, 2, 10, 20, 21, 41, 42, 43, 45, 46, 47, 48, 49, 50, 51) THEN 'ECOM' 
            ELSE 'DT KHÁC' END channel,
        UPPER(CASE 
                WHEN sc.sale_channel_name = 'Admin' AND st.code_nhanh = 'KHO SỈ' THEN 'KDS'
                            WHEN st.code_nhanh = 'KHO XUẤT' THEN 'DT KHÁC'
                WHEN so.channelName = 'KHO LẺ' THEN st.code_nhanh
                WHEN so.saleChannel IN (2, 10) THEN 'WEB'
                WHEN so.saleChannel IN (1, 20, 21, 46) THEN 'FB/INS/ZL/NB'
                WHEN so.saleChannel = 41 THEN 'LAZADA'
                WHEN so.saleChannel = 42 THEN 'SHOPEE'
                WHEN so.saleChannel = 48 THEN 'TIKTOK'
            ELSE 'KHO LỖI' END) store,
        CASE 
            WHEN pt.category IS NULL THEN 'BAGS' 
            ELSE pt.category END category,
        CASE 
            WHEN pt.subcategory IS NULL THEN 'BAGS' 
            ELSE pt.subcategory END subcategory,
        ps2.code fdcode,
        CASE 
            WHEN pt.default_code IS NULL THEN ps2.code 
            ELSE pt.default_code END default_code,
        CASE 
            WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != '' THEN -soi.quantity 
            ELSE soi.quantity END qty,
        CASE
            WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != '' THEN  -((soi.price * soi.quantity) - (soi.quantity * soi.discount)) 
            WHEN so.channelName ='Kho Lẻ' THEN (soi.price * soi.quantity) - soi.discount 
            ELSE (soi.price * soi.quantity) - (soi.discount * soi.quantity) 
        END rvn
    FROM sale_order so
    LEFT JOIN sale_order_items soi 
        ON so.orderId = soi.sale_order_id
    LEFT JOIN products ps2
        ON ps2.external_product_id = soi.external_product_id
    LEFT JOIN pt
        ON pt.external_product_id = ps2.parent_id
    LEFT JOIN stores st 
        ON st.depot_id_nhanh = so.depotId
    LEFT JOIN customers cus
        ON cus.external_customer_id = so.customer_id
    LEFT JOIN sale_channel sc
        ON sc.id = so.channel
    WHERE 
        so.status = 'Success'
        AND so.type != 'Khách trả lại hàng'
        AND NOT (
        so.privateDescription LIKE '%MDX%'
        AND (
            so.saleChannel IN (1, 2, 10, 20, 21, 46)
            AND so.channelName != 'Kho Lẻ'
            AND st.code_nhanh != 'KHO SỈ'
            )
        )
        AND YEAR(so.createdDateTime) = YEAR(CURRENT_DATE())
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    combined_df = pd.read_sql_query(text(uery_sales_year_days), conn)
print('Finished querying the sale')

combined_df = combined_df[combined_df['category'].isin(['SANDALS', 'SLIDES', 'SNEAKERS', 'KID SNEAKERS', 'KID SANDALS'])]

combined_df['date_order'] = pd.to_datetime(combined_df['date_order'], errors='coerce')
combined_df['rvn'] = pd.to_numeric(combined_df['rvn'])
combined_df['qty'] = pd.to_numeric(combined_df['qty'])
default_gr = combined_df.groupby(['default_code', 'category', 'subcategory']).agg({
    'qty': 'sum',
    'rvn': 'sum',
    'date_order': ['min', 'max']
}).reset_index()
# Đổi tên cột sau khi reset_index để đảm bảo tên dễ đọc
default_gr.columns = ['default_code', 'category', 'subcategory', 'qty', 'rvn', 'date_order_min', 'date_order_max']

df_template_price = df_template_fix.drop_duplicates(subset='default_code', keep='first')
df_template_price = df_template_price[['default_code', 'price']]

default_gr = pd.merge(default_gr, df_template_price[['default_code', 'price']], on='default_code', how='left')
default_gr['price_total'] = default_gr['price'] * default_gr['qty']
default_gr['cost_total'] = default_gr['price_total'] * 0.3
default_gr['profit'] = default_gr['rvn'] - default_gr['cost_total']
default_gr['profit_margin'] = round(default_gr['profit'] / default_gr['rvn'],2)

total_sales_value = default_gr["rvn"].sum()
default_gr["sales_value_ratio"] = (default_gr["rvn"] / total_sales_value)
default_gr['avg_price'] = round(default_gr['rvn']/default_gr['qty'],0)
default_gr['discount'] = 1-default_gr['avg_price']/default_gr['price']

default_gr['month_sale'] = round((default_gr['date_order_max'] - default_gr['date_order_min']).dt.days / 30, 1)
default_gr['avg_qty'] = round(default_gr['qty']/default_gr['month_sale'],0)
default_gr['avg_rvn'] = round(default_gr['rvn']/default_gr['month_sale'],0)
default_gr = pd.merge(default_gr, df_stock_gr[['default_code', 'available']], on='default_code', how='left')
default_gr['stock_to_avg_qty_ratio'] = default_gr['available']/default_gr['avg_qty']

df_template_fix['launch_date'] = pd.to_datetime(df_template_fix['launch_date'], errors='coerce')

# Lấy ngày launch đầu tiên của mỗi default_code
df_template_launch_date = df_template_fix.groupby('default_code', as_index=False)['launch_date'].min()

# Merge vào bảng chính
default_gr = pd.merge(default_gr, df_template_launch_date, on='default_code', how='left')

# Tính thời gian bán từ launch tới đơn hàng cuối cùng
default_gr['ftime_sale'] = round(
    (default_gr['date_order_max'] - default_gr['launch_date']).dt.days / 30, 1
)

###############################################################
# Bước 1: Chuyển date_order thành tháng
combined_df['month'] = combined_df['date_order'].dt.to_period('M')

# Lấy 90 ngày gần nhất
cutoff_date = datetime.today() - timedelta(days=90)
df_recent = combined_df[combined_df['date_order'] >= cutoff_date]

# Tính tổng qty 3 tháng theo category và default_code
cat_code_qty = df_recent.groupby(['category', 'default_code'])['qty'].sum().reset_index()

# Tính trung bình 1 mã trong category (tổng qty / số mã / 3)
benchmarks_dynamic = (
    cat_code_qty.groupby('category')['qty']
    .sum()  # tổng qty cả category
    .div(cat_code_qty.groupby('category')['default_code'].nunique())  # chia cho số mã
    .div(3)  # chia 3 tháng
    .to_dict()
)

fallback_benchmark = df_recent['qty'].sum() / max(1, df_recent['default_code'].nunique()) / 3
###############################################################

# Tính điểm tổng
def calculate_score_with_stock_updated(row):
    category = row["category"]
    ftime_sale = row["ftime_sale"]
    sales_value_ratio = row["sales_value_ratio"]
    discount = row["discount"]
    qty_stock = row["available"]
    avg_qty = row["avg_qty"]
    profit_margin = row["profit_margin"]

    # Nếu sản phẩm có tỷ trọng doanh thu cao (≥ 3%), giảm nhẹ ảnh hưởng của thời gian bán
    if sales_value_ratio >= 0.025:
        ftime_sale_penalty = 0.5
    else:
        ftime_sale_penalty = 1

    # Tính điểm `ftime_sale`
    if category == "SANDALS":
        ftime_sale_score = max(0, 100 - ftime_sale_penalty * (ftime_sale - 9) * 5) if ftime_sale <= 25 else 20
    elif category == "SLIDES":
        ftime_sale_score = max(0, 100 - ftime_sale_penalty * (ftime_sale - 6) * 5) if ftime_sale <= 21 else 20
    elif category == "SNEAKERS":
        ftime_sale_score = max(0, 100 - ftime_sale_penalty * (ftime_sale - 12) * 5) if ftime_sale <= 28 else 20
    elif category == "KID SANDALS":
        ftime_sale_score = max(0, 100 - ftime_sale_penalty * (ftime_sale - 6) * 5) if ftime_sale <= 21 else 20
    elif category == "KID SNEAKERS":
        ftime_sale_score = max(0, 100 - ftime_sale_penalty * (ftime_sale - 6) * 5) if ftime_sale <= 21 else 20
    else:
        ftime_sale_score = 0

    # Tính điểm `sales_value_ratio`
    if sales_value_ratio >= 0.05:
        sales_value_ratio_score = 100  # Tỷ trọng rất cao
    elif 0.03 <= sales_value_ratio < 0.05:
        sales_value_ratio_score = 90  # Tỷ trọng cao
    elif 0.015 <= sales_value_ratio < 0.03:
        sales_value_ratio_score = 80  # Tỷ trọng khá cao
    elif 0.008 <= sales_value_ratio < 0.015:
        sales_value_ratio_score = 70  # Tỷ trọng trung bình khá
    elif 0.005 <= sales_value_ratio < 0.008:
        sales_value_ratio_score = 50  # Tỷ trọng trung bình thấp
    else:
        sales_value_ratio_score = 20  # Tỷ trọng rất thấp

    # Tính điểm `discount`
    if discount <= 0.1:
        discount_score = 100
    elif 0.1 < discount <= 0.15:
        discount_score = 90
    elif 0.15 < discount <= 0.2:
        discount_score = 80
    elif 0.2 < discount <= 0.3:
        discount_score = 70
    else:
        discount_score = 60

    # Tính tỷ lệ tồn kho so với số lượng bán trung bình
    stock_to_avg_qty_ratio = qty_stock / avg_qty if avg_qty > 0 else float('inf')
    if 2.5 < stock_to_avg_qty_ratio <= 3.5:
        stock_score = 100
    elif 3.5 < stock_to_avg_qty_ratio <= 4.5:
        stock_score = 90
    elif 4.5 < stock_to_avg_qty_ratio <= 5.5:
        stock_score = 80
    else:
        stock_score = 60

    # Tính điểm `profit_margin`
    if profit_margin >= 0.65:
        profit_margin_score = 100
    elif 0.55 <= profit_margin < 0.65:
        profit_margin_score = 90
    elif 0.45 <= profit_margin < 0.55:
        profit_margin_score = 70
    elif 0.35 <= profit_margin < 0.45:
        profit_margin_score = 50
    elif 0.25 <= profit_margin < 0.35:
        profit_margin_score = 30
    else:
        profit_margin_score = 10

# Tiêu chí số lượng bán
    # Tính điểm `avg_qty_score` dựa trên benchmark 3 tháng gần nhất
    benchmark = benchmarks_dynamic.get(category, fallback_benchmark)
    ratio = avg_qty / benchmark if benchmark > 0 else 0

    if ratio >= 1:
        avg_qty_score = 100
    elif ratio >= 0.7:
        avg_qty_score = 80
    elif ratio >= 0.5:
        avg_qty_score = 60
    else:
        avg_qty_score = 30

    total_score = (0.175 * ftime_sale_score +
                   0.35 * sales_value_ratio_score +
                   0.125 * discount_score +
                   0.225 * profit_margin_score +
                   0.125 * avg_qty_score)

    return total_score

# Tính điểm và phân loại sản phẩm
default_gr["total_score"] = default_gr.apply(calculate_score_with_stock_updated, axis=1)

def classify_rank(score):
    if score >= 85:
        return "S"
    elif score >= 75:
        return "A"
    elif score >= 70:
        return "B"
    elif score >= 50:
        return "C"
    else:
        return "D"

default_gr["rank"] = default_gr["total_score"].apply(classify_rank)

# Thêm cột hành động và số lượng đặt hàng đủ bán cho 3 tháng
def action_and_order(row):
    rank = row["rank"]
    avg_qty = row["avg_qty"]
    qty_stock = row["available"]
    profit_margin = row.get("profit_margin", 0)
    ftime_sale = row['ftime_sale']

    if rank in ["S", "A"]:
        if profit_margin < 0.2:
            action = "Giữ hàng, biên lợi nhuận thấp"
            order_qty = 0
        else:
            action = "Nhập hàng thêm" if qty_stock < 2 * avg_qty else "Giữ lượng hàng"
            order_qty = max(0, 3 * avg_qty - qty_stock)
    elif rank == "B":
        if profit_margin >= 0.4:
            action = "Duy trì lượng hàng, lợi nhuận khá"
            order_qty = min(avg_qty, max(0, 2 * avg_qty - qty_stock))
        else:
            action = "Nhập hàng thêm nhẹ" if qty_stock < 1.5 * avg_qty else "Duy trì lượng hàng"
            order_qty = min(avg_qty, max(0, 3 * avg_qty - qty_stock))
    elif rank == "C":
        if profit_margin >= 0.5 and ftime_sale <= 6:
            action = "Cân nhắc giữ lại do lợi nhuận cao"
            order_qty = 0
        else:
            action = "Cân nhắc giảm giá để giảm tồn"
            order_qty = 0
    else:
        action = "Clear hàng tồn, giảm mạnh giá"
        order_qty = 0

    return pd.Series({"action": action, "order_qty": order_qty})

# Áp dụng hàm để thêm các cột
default_gr[["action", "order_qty"]] = default_gr.apply(action_and_order, axis=1)
default_gr = default_gr.replace([np.inf, -np.inf], 0).fillna(0)

print("Starting to process RAW_SEMI sheet...")
worksheet_default = sht.worksheet(SHEET1)
worksheet_default.clear()
print("Cleared RAW_SEMI sheet.")
gd.set_with_dataframe(worksheet_default, default_gr)
print("RAW_SEMI sheet updated with data.")