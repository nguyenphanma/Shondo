import pandas as pd
from sqlalchemy import text
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine

engine = get_engine()

file_path = Path(os.getenv('ma_shondo_path')) / "KT.THONG KE DU LIEU MKT NAM 2026.xlsx"
df_tax = pd.read_excel(file_path, sheet_name="Data DT", header=2)

df_tax = df_tax.rename(columns={
    "Ngày hạch toán": "accounting_date", "Ngày chứng từ": "document_date",
    "Số chứng từ": "document_number", "Mã khách hàng": "customer_code",
    "Tên khách hàng": "customer_name", "Diễn giải chung": "description",
    "Mã hàng": "fcode", "Tên hàng": "product_name", "Mã kho": "warehouse_code",
    "Tên kho": "warehouse_name", "TK giá vốn": "cogs_account",
    "TK kho": "inventory_account", "Mã đơn vị kinh doanh": "business_unit_code",
    "Mã nhóm khách hàng": "customer_group_code", "Mã thống kê": "statistic_code",
    "Thuế GTGT": "tax", "Doanh thu gồm VAT": "revenue_incl_vat",
    "Giảm giá gồm VAT": "discount_incl_vat", "Trả hàng gồm VAT": "return_incl_vat",
    "Tổng số lượng": "total_quantity",
})

def assign_channel(row):
    cgc = str(row["customer_group_code"]).strip()
    sc  = str(row["statistic_code"]).strip()
    wh  = str(row["warehouse_name"]).strip()
    if cgc == "DVVC" and sc == "OLWEB":   return "ECOM"
    if cgc == "DVVC" and sc == "OLFACE":  return "ECOM"
    if sc == "AMZ":                        return "AMZ"
    if cgc == "KDX":                       return "KDX"
    if cgc == "SHAT01" and sc == "SL" and wh == "Kho Sỉ": return "KDS"
    if cgc == "SHAT01" and sc == "SL":    return "DT KHÁC"
    if cgc == "KHL":                       return "KDC"
    if cgc == "KHS":                       return "KDS"
    if cgc == "Shopee":                    return "ECOM"
    if cgc == "Tiktok":                    return "ECOM"
    return None

df_tax["channel"] = df_tax.apply(assign_channel, axis=1)
df_tax_gr = df_tax.groupby(['accounting_date', 'document_date', 'channel']).agg(
    {"tax": "sum", "revenue_incl_vat": "sum"}
).reset_index()

with engine.connect() as conn:
    conn.execute(text("DROP TABLE IF EXISTS tax_revenue_grouped"))
    conn.commit()

df_tax_gr.to_sql(name="tax_revenue_grouped", con=engine, if_exists="replace", index=False)
print("✅ Xóa và insert lại thành công!")
