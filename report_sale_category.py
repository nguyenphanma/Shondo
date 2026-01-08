import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd
import os
from dotenv import load_dotenv

# GOOGLE SHEET
# Đường dẫn tới file JSON (đảm bảo tệp tồn tại)
gs = gspread.service_account(r'd:\OneDrive\KDA_Trinh Võ\KDA data\PYTHON_OPERATION\ma_shondo\mashondo.json')

# Mở Google Sheets bằng Google Sheets ID
sht = gs.open_by_key('146zOvMRYKve9PIGod_MSMQ38m4fThRNc3BE7Azww6aU')
SHEET1 = 'RAW_SALE'

# Thông tin kết nối MySQL
# Kết nối MySQL
load_dotenv()

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
    AND ps.code IS NOT NULL;
"""
# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_template_fix = pd.read_sql_query(text(query_products_template), conn)

# Truy vấn ngày tồn kho lớn nhất và dữ liệu tương ứng
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
            WHERE depot_id NOT IN (142410, 111752, 101011, 125224, 111753, 111754, 217633, 220636, 142408, 222877)
            GROUP BY product_id, depot_id
        ),

        -- 3. Tồn hôm qua
        stock_today AS (
            SELECT 
                st.code_nhanh as store,
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
                AND pih.depot_id NOT IN (142410, 111752, 101011, 125224, 111753, 111754, 217633, 220636, 142408, 222877)
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
# Lấy dữ liệu tồn kho theo ngày lớn nhất
with engine.connect() as conn:
    df_stock = pd.read_sql_query(text(query_data), conn,)

df_stock_filter = df_stock[['store', 'fdcode', 'available', 'subcategory', 'category']]

def channel(code):
    if code == 'KHO SỈ':
        return 'KDS'
    if code in ('KHO ECOM', 'ECOM2', 'ECOM','ECOM SG', 'KHO BOXME'):
        return 'ECOM'
    return 'KDC'
df_stock['channel'] = df_stock['store'].apply(channel)

# SALE 3 THÁNG GẦN NHẤT
query_sales_180_days = """
    SELECT 
        CASE 
            WHEN so.channelName = 'Kho Lẻ' THEN 'KDC'
            WHEN st.code_nhanh = 'KHO SỈ' THEN 'KDS'
            WHEN so.saleChannel IN (1, 2, 10, 20, 21, 41, 42, 43, 45, 46, 47, 48, 49, 50, 51) THEN 'ECOM' 
            ELSE 'DT KHÁC' 
        END AS channel,
        ps2.code fdcode,
        CASE 
            WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != '' THEN -soi.quantity
            ELSE soi.quantity
        END AS qty,
        CASE
                WHEN so.relatedBillId IS NOT NULL AND TRIM(so.relatedBillId) != '' THEN  -((soi.price * soi.quantity) - (soi.quantity * soi.discount)) 
                WHEN so.channelName ='Kho Lẻ' THEN (soi.price * soi.quantity) - soi.discount 
                ELSE (soi.price * soi.quantity) - (soi.discount * soi.quantity) END as rvn,
        soi.discount,
        ps2.launch_date,
        ps2.price price_retail
    FROM sale_order so
    LEFT JOIN sale_order_items soi 
        ON so.orderId = soi.sale_order_id
    LEFT JOIN products ps2
        ON ps2.external_product_id = soi.external_product_id
    LEFT JOIN stores st 
        ON st.depot_id_nhanh = so.depotId
    WHERE 
        so.status = 'Success'
        AND DATE(so.createdDateTime) >= CURRENT_DATE() - INTERVAL 180 DAY
    """

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    combined_df = pd.read_sql_query(text(query_sales_180_days), conn)

combined_df = combined_df[combined_df['channel'] != 'KDS']

combined_df['launch_date'] = pd.to_datetime(combined_df['launch_date'])
combined_df = pd.merge(combined_df, df_template_fix[['fdcode', 'default_code','category', 'subcategory']], on='fdcode', how='left')

combined_group = combined_df.groupby(['category', 'subcategory', 'default_code', 'price_retail']).agg({
    'qty': 'sum',
    'rvn': 'sum',
    'launch_date':'min'
}).reset_index()

combined_group['days_since_launch'] = (datetime.now() - combined_group['launch_date']).dt.days
combined_group['avg_qty'] = combined_group.apply(
    lambda row: round(
        ((row['qty'] / row['days_since_launch']) *(180 / 6))
        if row['days_since_launch'] <= 180
        else (row['qty'] / 6),
        1
    ),
    axis=1
)
combined_group['month_launch'] = round(combined_group['days_since_launch']/30,1)
combined_group.drop(columns=['launch_date', 'days_since_launch'], inplace=True)
combined_group = combined_group[combined_group['category'].isin(["SANDALS", "SLIDES", "KID SANDALS", "SNEAKERS", "KID SNEAKERS", "BAGS", "HATS"])]

df_stock = pd.merge(df_stock, df_template_fix[['fdcode', 'default_code']], on='fdcode', how='left')

df_stock_gr = df_stock.groupby(['channel', 'default_code', 'subcategory', 'category']).agg({
    'available':'sum'
}).reset_index()

df_stock_pivot = df_stock_gr.pivot_table(index=['category', 'subcategory', 'default_code'], 
                                   columns='channel', 
                                   values='available', 
                                   aggfunc='sum').reset_index()

df_stock_pivot = df_stock_pivot.fillna(0)

query_stock_pen = """
            SELECT *
            FROM stock_pen
"""
# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_stock_pen = pd.read_sql_query(text(query_stock_pen), conn)

df_stock_pen_gr =  df_stock_pen.groupby(['default_code']).agg({
    'order_pen': 'sum'
}).reset_index()

df_stock_pivot['total_stock'] = df_stock_pivot['ECOM'] + df_stock_pivot['KDC'] + df_stock_pivot['KDS']
df_stock_pivot = pd.merge(df_stock_pivot, df_stock_pen_gr[['default_code', 'order_pen']], on='default_code', how='left')
df_stock_pivot['order_pen'] = df_stock_pivot['order_pen'].fillna(0)

def type_kds(code):
    kds_codes = [] # Nhập mã sp độc quyền nếu có
    if code in kds_codes:
        return 'độc quyền KDS'
    return ""

df_stock_pivot['type_kds'] = df_stock_pivot['default_code'].apply(type_kds)

SHEET2 = 'CATALOGUE'
worksheet_ctl = sht.worksheet(SHEET2)
data_ctl = worksheet_ctl.get_values('A6:A')
df_ctl = pd.DataFrame(data_ctl, columns=['default_code'])

def type_product(code):
    if code in df_ctl['default_code'].values:
        return 'S'
    if code not in df_ctl['default_code'].values:
        return 'Q'
    if code not in combined_group['default_code'].values:
        return 'C'
    return 'Q'

df_stock_pivot['type_products'] = df_stock_pivot['default_code'].apply(type_product)

df_fn = pd.merge(combined_group, df_stock_pivot[['default_code', 'ECOM', 'KDC', 'KDS', 'total_stock', 'order_pen', 'type_kds', 'type_products']], on='default_code', how='outer').fillna(0)

df_fn['hst'] = df_fn.apply(lambda row: round(row['total_stock'] / row['avg_qty'], 1) 
                           if row['avg_qty'] != 0 else 0, axis=1)

df_filtered = df_fn[~((df_fn['total_stock'] == 0) & (df_fn['order_pen'] == 0))]

SHEET3 = 'RAW_MAX_SALE'
worksheet_max_sale = sht.worksheet(SHEET3)
data_sale = worksheet_max_sale.get_all_values()
df_max_dis = pd.DataFrame(data_sale[1:], columns=data_sale[0])

df_fn_mer = pd.merge(df_filtered, df_max_dis[['default_code', 'discount_max']], on='default_code', how='left')

# Lấy danh sách top 10 sản phẩm có giá trị đơn hàng cao nhất
top10_sku = (
    combined_df.groupby(['default_code'])['rvn']
    .sum()
    .reset_index()
    .sort_values(by='rvn', ascending=False)
    .head(10)['default_code']
    .tolist()
)

# Tìm mã sp cha có AVG_SLB_MONTH cao nhất trong từng subcategory
max_avg_slb_month = df_fn_mer.groupby(['subcategory', 'default_code'])['avg_qty'].max().reset_index()

# Lấy danh sách các mã sản phẩm cha có AVG_SLB_MONTH cao nhất trong từng subcategory
max_avg_slb_month = max_avg_slb_month.loc[max_avg_slb_month.groupby('subcategory')['avg_qty'].idxmax()]
top_avg_slb_month_sku = set(max_avg_slb_month['default_code'])  # Chuyển thành set để kiểm tra nhanh hơn

# Cập nhật hàm tính giảm giá
def calculate_discount(row):
    month_launch = row['month_launch']
    hst = row['hst']
    total_stock = row['total_stock']
    type_products = row['type_products']
    type_kds = row['type_kds']
    ma_sp_cha = row['default_code']  # Lấy giá trị Mã sp cha của sản phẩm
    stock_pen = row['order_pen']
    
    # Nếu sản phẩm thuộc danh sách top10, chỉ giảm 10%
    if ma_sp_cha in top10_sku:
        return 0.1

    # Kiểm tra cột type_kds có phải là chuỗi và không rỗng
    if isinstance(type_kds, str) and type_kds.strip():
        return 0.1

    # 🚨 Ưu tiên điều kiện 0.7 trước - Nếu thỏa mãn, return ngay
    if (month_launch > 12) and (hst >= 0) and (type_products == 'Q') and (total_stock <= 50):
        return 0.7  # Không cho phép bị ghi đè

    # Xác định mức giảm giá ban đầu
    discount = 0.0  # Mặc định không giảm giá

    # Thiết lập mức giảm giá dựa trên `month_launch`
    if 3 < month_launch <= 6:
        discount = 0.1
    elif 6 < month_launch <= 9:
        discount = 0.2
    elif 9 < month_launch <= 12:
        discount = 0.3
    elif month_launch > 12:
        if hst > 5:
            discount = 0.5
        else:
            discount = 0.4

    # Giới hạn mức giảm giá với type_products = 'S'
    if type_products == 'S':
        if stock_pen > 200:
            discount = min(discount, 0.2)  # Nếu còn nợ > 200, giảm giá tối đa 0.2
        else:
            discount = min(discount, 0.3)  # Các trường hợp khác, giảm giá tối đa 0.3

    # Điều kiện giới hạn giảm giá nếu `Mã sp cha` thuộc danh sách top AVG_SLB_MONTH
    if ma_sp_cha in top_avg_slb_month_sku and type_products != 'Q':
        discount = min(discount, 0.15)

    return discount

# Áp dụng tính toán giảm giá
df_fn_mer['Suggested Discount'] = df_fn_mer.apply(calculate_discount, axis=1)


df_fn_mer['price_sale'] = df_fn_mer['price_retail'] *(1 - df_fn_mer['Suggested Discount'])

# Danh sách thứ tự mong muốn cho 'Danh mục'
category_order = ["SANDALS", "SLIDES", "KID SANDALS", "SNEAKERS", "KID SNEAKERS", "BAGS", "HATS"]

# Chuyển 'Danh mục' thành dạng Categorical để sắp xếp theo thứ tự mong muốn
df_fn_mer['category'] = pd.Categorical(df_fn_mer['category'], categories=category_order, ordered=True)

# Sắp xếp theo các tiêu chí:
df_fn_mer = df_fn_mer.sort_values(
    by=['category', 'subcategory', 'avg_qty', 'default_code'],  
    ascending=[True, True, False, True]  # AVG_SLB_MONTH phải giảm dần (False)
)

df_fn_mer.rename(columns={'qty': 'SL bán trong 6M',
                            'avg_qty': 'AVG.SLB',
                           'month_launch': 'Số tháng bán',
                           'total_stock': 'Tổng tồn'}, inplace=True)

df_fn_mer = df_fn_mer[df_fn_mer['type_products'].notna() & (df_fn_mer['type_products'] != "")]

df_fn_mer['discount_max'] = df_fn_mer['discount_max'].fillna(0)
df_fn_mer = df_fn_mer[
    (df_fn_mer['category'] != "") &
    (df_fn_mer['category'].notna()) &
    (df_fn_mer['subcategory'] != "BAGS") & 
    (df_fn_mer['subcategory'].notna())
]

worksheet_sale = sht.worksheet(SHEET1)
worksheet_sale.batch_clear(['A1:S'])
gd.set_with_dataframe(worksheet_sale, df_fn_mer)