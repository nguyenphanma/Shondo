import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd
from dotenv import load_dotenv
import os

# Load biến môi trường từ file .env
load_dotenv()

# GOOGLE SHEET
gs = gspread.service_account(r'd:\OneDrive\KDA_Trinh Võ\KDA data\PYTHON_OPERATION\ma_shondo\mashondo.json')

# Mở Google Sheet theo ID
sht = gs.open_by_key('1w7PN9UXeDf38q7ZtnquCcbikv2mBuwTRrpl-bOzBMiI')
SHEET1 = 'customers'

# Lấy thông tin từ biến môi trường
host = os.getenv("DB_HOST")
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_NAME")
port = os.getenv("DB_PORT", 3306)


# Kết nối MySQL
connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(connection_string)

# Truy vấn khách hàng có sinh nhật tháng 8 và mua hàng tại 'Kho Lẻ'
query_customers_birthday = """
    SELECT 
        s.customer_id,
        c.name,
        c.mobile,
        c.birthday,
        st.code_nhanh AS last_purchase_store,
        MAX(DATE(s.createdDateTime)) AS last_purchase_date
    FROM sale_order s
    LEFT JOIN customers c ON c.external_customer_id = s.customer_id
    LEFT JOIN stores st ON st.depot_id_nhanh = s.depotId
    WHERE s.channelName = 'Kho Lẻ'
      AND s.customer_id IS NOT NULL
      AND s.customer_id NOT IN (108248129, 122951605)
      AND MONTH(c.birthday) = 10
    GROUP BY s.customer_id, c.name, c.mobile, c.birthday, st.code_nhanh
    ORDER BY last_purchase_date DESC;
"""

# Đọc dữ liệu từ database
with engine.connect() as conn:
    df_customers = pd.read_sql_query(text(query_customers_birthday), conn)

# (Tùy chọn) Đẩy lên Google Sheets
worksheet = sht.worksheet(SHEET1)
worksheet.clear()
gd.set_with_dataframe(worksheet, df_customers)