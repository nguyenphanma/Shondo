import pandas as pd
from datetime import datetime
from sqlalchemy import text
import gspread_dataframe as gd
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from core.sheets import get_client

gs = get_client()
sht = gs.open_by_key('1w7PN9UXeDf38q7ZtnquCcbikv2mBuwTRrpl-bOzBMiI')
SHEET1 = 'customers'

engine = get_engine()

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

with engine.connect() as conn:
    df_customers = pd.read_sql_query(text(query_customers), conn)

worksheet_customers = sht.worksheet(SHEET1)
worksheet_customers.clear()
gd.set_with_dataframe(worksheet_customers, df_customers)
print('customers sheet updated with data')
