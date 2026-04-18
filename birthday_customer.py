import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
# GOOGLE SHEET
# Đường dẫn tới file JSON (đảm bảo tệp tồn tại)
gs = gspread.service_account(Path(os.getenv('ma_shondo_path')) / 'mashondo.json')

# Mở Google Sheets bằng Google Sheets ID
sht = gs.open_by_key('1w7PN9UXeDf38q7ZtnquCcbikv2mBuwTRrpl-bOzBMiI')
SHEET1 = 'customers'

# Thông tin kết nối MySQL
# Kết nối MySQL


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

query_customers = """
    SELECT 
        s.customer_id,
        c.name,
        c.mobile,
        c.birthday,
        st.code_nhanh as last_purchase_store,
        MAX(DATE(s.createdDateTime)) AS last_purchase_date
    FROM sale_order s
    LEFT JOIN customers c ON c.external_customer_id = s.customer_id
    LEFT JOIN stores st ON st.depot_id_nhanh = s.depotId
    WHERE s.channelName = 'Kho Lẻ'
    AND s.customer_id NOT IN (108248129, 122951605)
    AND s.customer_id IS NOT NULL
    AND MONTH(c.birthday) = 4
    GROUP BY s.customer_id, 
            c.name, 
            c.mobile, 
            st.code_nhanh,
            c.birthday
    ORDER BY last_purchase_date DESC;
"""
# Lấy dữ liệu bán hàng từ database
with engine.connect() as conn:
    df_customers = pd.read_sql_query(text(query_customers), conn)

worksheet_customers = sht.worksheet(SHEET1)
worksheet_customers.clear()
gd.set_with_dataframe(worksheet_customers, df_customers)
print('customers sheet updated with data')