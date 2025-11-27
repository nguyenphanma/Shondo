import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd

gs = gspread.service_account(r'd:\OneDrive\KDA_Trinh Võ\KDA data\PYTHON_OPERATION\ma_shondo\mashondo.json')


# Thông tin kết nối MySQL
host = "210.211.110.220"
user = "nguyen.mer"
password = "Shondo2025"
database = "merchandise"
port = "3306"

# Tạo engine MySQL
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)
query_products_template = """
SELECT * FROM products_template;
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_template_fix = pd.read_sql_query(text(query_products_template), conn)


# Tạo engine MySQL
engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

# Truy vấn lấy 4 ngày Chủ Nhật gần nhất
query_last_sundays = """
    SELECT DISTINCT data_stock
    FROM inventory_data
    WHERE WEEKDAY(data_stock) = 6  -- Chỉ lấy ngày Chủ Nhật
    ORDER BY data_stock DESC
    LIMIT 4;  -- Lấy 4 ngày Chủ Nhật gần nhất
"""

# Lấy 4 ngày Chủ Nhật gần nhất
with engine.connect() as conn:
    result = conn.execute(text(query_last_sundays))
    last_sundays = [row[0] for row in result.fetchall()]

if not last_sundays:
    raise ValueError("Không tìm thấy dữ liệu tồn kho vào ngày Chủ Nhật trong database!")

# Chuyển danh sách ngày thành chuỗi để dùng trong truy vấn SQL
placeholders = ', '.join([':date' + str(i) for i in range(len(last_sundays))])
query_data = f"""
    SELECT id.fdcode,
           pt.size,
           pt.default_code,     
           id.data_stock, 
           id.Store, 
           id.Qty_stock, 
           pt.subcategory,
           pt.category,
           pt.launch_date
    FROM inventory_data as id
    LEFT JOIN products_template as pt on pt.fdcode = id.fdcode
    WHERE id.data_stock IN ({placeholders})
        AND pt.category NOT IN('BAO BÌ', 'BẢO HÀNH SỬA CHỮA', 'CCDC')
        AND id.Store NOT IN ('303DUONGBATRAC', 'KHO LỖI', 'KHO XUẤT', 'KHO SẢN XUẤT');
"""

# Gán tham số cho query
params = {f"date{i}": date for i, date in enumerate(last_sundays)}

# Lấy dữ liệu tồn kho theo 4 ngày Chủ Nhật gần nhất
with engine.connect() as conn:
    df_stock = pd.read_sql_query(text(query_data), conn, params=params)
df_stock.rename(columns={'data_stock':'date_stock', 'Store':'warehouse', 'Qty_stock': 'qty_stock'}, inplace=True)
df_stock['date_stock'] = pd.to_datetime(df_stock['date_stock'])
df_stock['launch_date'] = pd.to_datetime(df_stock['launch_date'])

# Tính số ngày chênh lệch giữa ngày tồn kho và ngày ra mắt sản phẩm
df_stock['time_stock'] = (df_stock['date_stock'] - df_stock['launch_date']).dt.days

# Gán NaN nếu launch_date trống
df_stock.loc[df_stock['launch_date'].isna(), 'time_stock'] = None

# Xác định các điều kiện để gán nhóm thời gian tồn kho
conditions = [
    (df_stock['time_stock'] <= 90) & df_stock['time_stock'].notna(),
    (df_stock['time_stock'] > 90) & (df_stock['time_stock'] <= 180) & df_stock['time_stock'].notna(),
    (df_stock['time_stock'] > 180) & (df_stock['time_stock'] <= 270) & df_stock['time_stock'].notna(),
    (df_stock['time_stock'] > 270) & (df_stock['time_stock'] <= 365) & df_stock['time_stock'].notna(),
    (df_stock['time_stock'] > 365) & df_stock['time_stock'].notna()
]

# Nhãn tương ứng với khoảng thời gian
values = [
    'Dưới 3M',
    'Trên 3M - Dưới 6M',
    'Trên 6M - Dưới 9M',
    'Trên 9M - Dưới 12M',
    'Trên 12M'
]
  # 6 bins
# Áp dụng điều kiện để tạo cột group_time_stock
df_stock['group_time_stock'] = pd.cut(
    df_stock['time_stock'],
    bins = [-float('inf'), 90, 180, 270, 365, float('inf')],
    labels=values, 
    right=False
)

# Nếu launch_date trống thì group_time_stock cũng trống
df_stock['group_time_stock'] = df_stock['group_time_stock'].astype('category')  # Đảm bảo cột là category
df_stock['group_time_stock'] = df_stock['group_time_stock'].cat.add_categories("Chưa ra mắt")

# Gán giá trị cho các dòng có launch_date là NaN
df_stock.loc[df_stock['launch_date'].isna(), 'group_time_stock'] = "Chưa ra mắt"


df_stock['week'] = df_stock['date_stock'].dt.isocalendar().week
df_stock['month'] = df_stock['date_stock'].dt.month
df_stock['year'] = df_stock['date_stock'].dt.year
# Thay thế giá trị 'ECOM' bằng 'KHO ECOM'
df_stock.replace({'ECOM': 'KHO ECOM'}, inplace=True)


def channel(code):
    if code =='KDS':
        return 'KDS'
    if code == 'KHO ECOM':
        return 'ECOM'
    if code == 'KHO TỔNG':
        return 'KHO TỔNG'
    if code == 'KHO GIA CÔNG':
        return 'KHO GIA CÔNG'
    return 'KDC'
df_stock['channel'] = df_stock['warehouse'].apply(channel)

def category(code):
    if code =="":
        return 'QUÀ TẶNG'
    return code
df_stock['category'] = df_stock['category'].apply(category)

def time_stock(code):
    if code < 0:
        return 0
    return code
df_stock['time_stock'] = df_stock['time_stock'].apply(time_stock)
df_stock = df_stock[df_stock['qty_stock'] >=1]
# SALE TỪ 2024 TRỞ ĐI
query_sales_year_days = """
SELECT
    sa.date_order,
    sa.order_id,
    sa.ord_status,
    sa.channel,
    sa.warehouse,
    sa.ord_source,
    sa.fdcode,
    pt.size,
    pt.default_code,
    sa.sales_qty,
    sa.sales_value,
    pt.category,
    pt.subcategory,
    sa.payment_method,
    pt.launch_date,
    pt.retail_price
FROM sales_order_fn AS sa
LEFT JOIN products_template AS pt ON pt.fdcode = sa.fdcode
WHERE 
    sa.date_order >= DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1 + 4 * 7) DAY)
    AND sa.date_order <= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY)
    AND sa.warehouse NOT IN ('KHO XUẤT', 'KHO LỖI');
"""

# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    combined_df = pd.read_sql_query(text(query_sales_year_days), conn)
combined_df['channel'].replace({'CỬA HÀNG': 'KDC', 'BÁN SỈ': 'KDS'}, inplace=True)
combined_df['warehouse'].replace({'KDS_NV-Nguyễn Thị Thùy Linh': 'KDS',
                            'KDS_TBP-Huỳnh Văn Quân': 'KDS',
                            'KDS_NV-Nguyễn Quỳnh Như': 'KDS',
                            'Ecom':'FB/INS/NỘI BỘ/ZALO'}, inplace=True)
combined_df['date_order'] = pd.to_datetime(combined_df['date_order'], errors='coerce')
combined_df['discount'] = 1 - combined_df['sales_value']/combined_df['sales_qty']/combined_df['retail_price']
combined_df['year'] = combined_df['date_order'].dt.year
combined_df['month'] = combined_df['date_order'].dt.month
combined_df['week'] = combined_df['date_order'].dt.isocalendar().week
combined_df['sales_value'] = pd.to_numeric(combined_df['sales_value'])
combined_df['sales_qty'] = pd.to_numeric(combined_df['sales_qty'])
combined_df.replace([-float('inf'), float('inf')], None, inplace=True)

combined_df['launch_date'] = pd.to_datetime(combined_df['launch_date'], errors='coerce')
combined_df['date_order'] = pd.to_datetime(combined_df['date_order'], errors='coerce')

# Tính số ngày chênh lệch giữa ngày ra mắt sản phẩm và ngày đặt hàng
combined_df['time_sale'] = (combined_df['date_order'] - combined_df['launch_date']).dt.days

# Gán NaN nếu launch_date trống
combined_df.loc[combined_df['launch_date'].isna(), 'time_sale'] = None

# Xác định khoảng thời gian (bins) và nhãn tương ứng (labels)
bins = [-float('inf'), 90, 180, 270, 365, float('inf')]  # 6 bins
labels = ['Dưới 3M', 'Trên 3M - Dưới 6M', 'Trên 6M - Dưới 9M', 'Trên 9M - Dưới 12M', 'Trên 12M']  # 5 labels

# Gán giá trị theo khoảng thời gian
combined_df['time_sale'] = pd.cut(
    combined_df['time_sale'],
    bins=bins,
    labels=labels, 
    right=False  # Để 90 thuộc nhóm "Dưới 3M", 91 thuộc "Trên 3M - Dưới 6M"
)

# Nếu launch_date trống thì time_sale_group cũng trống
combined_df.loc[combined_df['launch_date'].isna(), 'time_sale'] = None

def cus_source(code):
    if code =="":
        return "KDC_Khách vãng lai"
    return code
combined_df['ord_source'] = combined_df['ord_source'].fillna("").apply(cus_source)

# GOOGLE SHEET
sht_pfm = gs.open_by_key('1n5GEkTzSD6ijdwldwzKcZ4aqg24bqpJXSxA8ZWcdq0M')
SHEET2 ='RAW_SALE'
SHEET3 ='RAW_STOCK'
SHEET4 = 'TARGET_MONTH'
SHEET5 = 'PRODUCTS_TEMPLATE'
SHEET6 = 'TARGET_WEEK'

# SALE
worksheet_sale = sht_pfm.worksheet(SHEET2)
worksheet_sale.clear()
print('Cleared sheet RAW_SALE')

# Chia nhỏ dữ liệu thành từng batch 20.000 dòng
batch_size = 20000
num_batches = (len(combined_df) // batch_size) + 1  # Tính số batch

# 🟢 Ghi tiêu đề (header) trước, chỉ một lần
gd.set_with_dataframe(worksheet_sale, combined_df.head(0), row=1, col=1)
print("Uploaded headers successfully!")

# 🟢 Ghi từng batch nhưng **không lặp lại tiêu đề**
for i in range(num_batches):
    start_row = i * batch_size
    end_row = start_row + batch_size
    batch_df = combined_df.iloc[start_row:end_row]  # Lấy từng batch
    
    if not batch_df.empty:
        gd.set_with_dataframe(worksheet_sale, batch_df, row=start_row + 2, col=1, include_column_header=False)
        print(f"Uploaded batch {i+1}/{num_batches}, Rows: {start_row + 2} to {end_row + 1}")

# STOCK
worksheet_stock = sht_pfm.worksheet(SHEET3)

# Xóa dữ liệu cũ trong RAW_STOCK
worksheet_stock.batch_clear(['A:N'])  
print('Cleared sheet RAW_STOCK')

# Chia nhỏ dữ liệu stock thành từng batch
num_batches_stock = (len(df_stock) // batch_size) + 1  # Tính số batch

# Ghi tiêu đề (header) trước, chỉ một lần
gd.set_with_dataframe(worksheet_stock, df_stock.head(0), row=1, col=1)
print("Uploaded headers for STOCK successfully!")

# Ghi từng batch nhưng **không lặp lại tiêu đề**
for i in range(num_batches_stock):
    start_row = i * batch_size
    end_row = start_row + batch_size
    batch_df = df_stock.iloc[start_row:end_row]  # Lấy từng batch
    
    if not batch_df.empty:
        gd.set_with_dataframe(worksheet_stock, batch_df, row=start_row + 2, col=1, include_column_header=False)
        print(f"Uploaded batch {i+1}/{num_batches_stock} for STOCK, Rows: {start_row + 2} to {end_row + 1}")

print("Finished uploading all STOCK data!")

# TEMPLATE
worksheet_template = sht_pfm.worksheet(SHEET5)
worksheet_template.clear()
print('Cleared sheet PRODUCTS_TEMPLATE')
gd.set_with_dataframe(worksheet_template, df_template_fix)
print("Finished uploading all data to PRODUCTS_TEMPLATE!")