import pandas as pd
import os
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
import gspread
import gspread_dataframe as gd
import datetime

def main():
    year ='2025'
    month ='05'
    # GOOGLE SHEET
    # Đường dẫn tới file JSON (đảm bảo tệp tồn tại)
    gs = gspread.service_account(r'd:\OneDrive\KDA_Trinh Võ\KDA data\PYTHON_OPERATION\ma_shondo\mashondo.json')

    # Mở Google Sheets bằng Google Sheets ID
    sht = gs.open_by_key('1mLcY4lijE8SP3JB1IGCLxk_a8jfm7Cj7fpiifNsuZg4')
    SHEET1 = 'RAW_SALE'
    SHEET2 = 'RAW_STOCK'
    SHEET3 = 'RAW_STOCK_PEN'
    SHEET4 = 'REPORT'

    # Thông tin kết nối MySQL
    host = "210.211.109.23"
    user = "nguyen.mer"
    password = "Shondo2025"
    database = "merchandise"
    port = "3306"

    # Tạo engine MySQL
    connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)

    # Truy vấn ngày tồn kho lớn nhất và dữ liệu tương ứng
    query_max_date = """
        SELECT MAX(data_stock) AS max_date
        FROM inventory_data;
    """

    query_data = """
        SELECT pt.default_code, id.fdcode, id.data_stock, id.Store, id.Qty_stock
        FROM inventory_data AS id
        LEFT JOIN products_template AS pt ON pt.fdcode = id.fdcode
        WHERE data_stock = :max_date
            AND Store NOT IN('303DUONGBATRAC', 'KHO LỖI', 'KHO XUẤT', 'KHO SẢN XUẤT', 'KHO GIA CÔNG');
    """

    # Lấy ngày tồn kho lớn nhất
    with engine.connect() as conn:
        result = conn.execute(text(query_max_date))
        max_date = result.scalar()

    if not max_date:
        raise ValueError("Không tìm thấy dữ liệu tồn kho trong database!")

    # Lấy dữ liệu tồn kho theo ngày lớn nhất
    with engine.connect() as conn:
        df_stock = pd.read_sql_query(text(query_data), conn, params={"max_date": max_date})
    df_stock[['fdcode', 'default_code']] = df_stock[['fdcode','default_code']].apply(lambda x: x.str.upper())
    def channel(code):
        if code =='KDS':
            return 'KDS'
        if code == 'ECOM':
            return 'ECOM'
        if code =='KHO TỔNG':
            return 'KHO TỔNG'
        return 'KDC'
    df_stock['channel'] = df_stock['Store'].apply(channel)

    df_stock.rename(columns={'Store':'warehouse',
                            'Qty_stock':'stock'}, inplace=True)

    # SẢN PHẨM MỚI RA MẮT 90 NGÀY GẦN ĐÂY
    query_sales_90_days = """
    SELECT so.date_order,
        so.order_id,
        so.channel,
        so.warehouse,
        pt.default_code,
        so.fdcode, 
        so.sales_qty,
        CASE WHEN so.sales_value IS NULL THEN 0 ELSE so.sales_value END as sales_value,
        pt.category,
        pt.subcategory,
        pt.retail_price,
        pt.launch_date
    FROM sales_order_fn as so
    LEFT JOIN products_template AS pt ON pt.fdcode = so.fdcode
    WHERE pt.launch_date >= CURRENT_DATE() - INTERVAL 90 DAY
        AND so.warehouse NOT IN('KHO XUẤT', 'KHO LỖI')
        AND pt.category IN('SANDALS', 'KID SANDALS', 'SLIDES', 'SNEAKERS')
        AND so.ord_status = 'Thành công';
    """
    # Lấy dữ liệu bán hàng từ database
    with engine.connect() as conn:
        combined_df = pd.read_sql_query(text(query_sales_90_days), conn)
    combined_df[['fdcode', 'default_code']] = combined_df[['fdcode', 'default_code']].apply(lambda x: x.str.upper())
    combined_df['channel'].replace({'CỬA HÀNG': 'KDC', 'BÁN SỈ': 'KDS'}, inplace=True)
    combined_df['warehouse'].replace({'KDS_NV-Nguyễn Thị Thùy Linh': 'KDS',
                                'KDS_TBP-Huỳnh Văn Quân': 'KDS',
                                'KDS_NV-Nguyễn Quỳnh Như': 'KDS',
                                'TIKTOK':'ECOM',
                                'SHOPEE':'ECOM',
                                'WEB/API':'ECOM',
                                'FB/INS/NỘI BỘ/ZALO':'ECOM'}, inplace=True)


    current_drive = os.path.splitdrive(os.getcwd())[0].upper()
    raw_path = rf"OneDrive\KDA_Trinh Võ\KDA data\THEO DÕI - ĐỀ XUẤT\TRẢ ĐƠN ĐẶT HÀNG\{year}\THANG {month}"
    folder_path = os.path.normpath(f"{current_drive}/{raw_path}")
    files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx')]

    # Kiểm tra tồn tại thư mục
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Đường dẫn không tồn tại: {folder_path}")

    # Lọc và lấy file có 6 số đầu lớn nhất
    files = [f for f in os.listdir(folder_path) if f.endswith('.xlsx') and os.path.isfile(os.path.join(folder_path, f))]

    max_file = None
    max_value = -1
    for file in files:
        try:
            num_part = file[:8]
            num_value = int(num_part)
            if num_value > max_value:
                max_value = num_value
                max_file = file
        except ValueError:
            continue

    # Đọc file tồn kho lớn nhất
    if max_file:
        file_path = os.path.join(folder_path, max_file)
        print(f"File trả nợ đơn được chọn: {max_file}")
        df_pending_stock = pd.read_excel(file_path, sheet_name="DATA  ALL")
    else:
        raise FileNotFoundError("Không tìm thấy file phù hợp trong thư mục tồn kho!")

    # Lấy dòng thứ 3 làm tiêu đề mới
    df_pending_stock.columns = df_pending_stock.iloc[0]

    # Bỏ các dòng tiêu đề cũ
    df_pending_stock_fix = df_pending_stock[1:].reset_index(drop=True)

    df_pending_stock_fix = df_pending_stock_fix[['KÊNH BÁN', 'DANH MỤC', 'MÃ SP CHA', 'SIZE', 'Mã hàng', 'ĐƠN ĐẶT HÀNG THÁNG', 'NĂM','SL ĐẶT', 'SL CÒN NỢ\n(XƯỞNG)']]
    df_pending_stock_fix[['MÃ SP CHA']] = df_pending_stock_fix[['MÃ SP CHA']].apply(lambda x: x.str.upper())
    df_pending_stock_fix.rename(columns={
        'KÊNH BÁN':'channel',
        'DANH MỤC':'category',
        'MÃ SP CHA': 'default_code',
        'SIZE':'size',
        'Mã hàng':'fdcode',
        'ĐƠN ĐẶT HÀNG THÁNG':'month',
        'NĂM': 'year',
        'SL ĐẶT':'qty_order',
        'SL CÒN NỢ\n(XƯỞNG)':'stock_pen'
    },inplace=True)
    df_pending_stock_fix['channel'].replace({'CỬA HÀNG': 'KDC', 'BÁN SỈ': 'KDS'}, inplace=True)
    df_pending_stock_fix = df_pending_stock_fix[(df_pending_stock_fix['fdcode'].isin(combined_df['fdcode'])) & (df_pending_stock_fix['channel'] != 'NGOÀI ĐƠN')]
    df_stock = df_stock[df_stock['fdcode'].isin(combined_df['fdcode'])]

    # SALE
    print("Starting to process SALE sheet...")
    worksheet_sale = sht.worksheet(SHEET1)
    worksheet_sale.batch_clear(['A1:L'])
    print("Cleared SALE sheet.")
    gd.set_with_dataframe(worksheet_sale, combined_df)
    print("SALE sheet updated with data.")

    # RAW_STOCK
    print("Starting to process RAW_STOCK sheet...")
    worksheet_stock = sht.worksheet(SHEET2)
    worksheet_stock.clear()
    print("Cleared RAW_STOCK sheet.")
    gd.set_with_dataframe(worksheet_stock, df_stock)
    print("RAW_STOCK sheet updated with data.")

    # STOCK_PEN
    print("Starting to process STOCK_PEN sheet...")
    worksheet_pen = sht.worksheet(SHEET3)
    worksheet_pen.clear()
    print("Cleared STOCK_PEN sheet.")
    gd.set_with_dataframe(worksheet_pen, df_pending_stock_fix)
    print("STOCK_PEN sheet updated with data.")

    # REPORT
    print("Starting to process REPORT sheet...")
    worksheet_report = sht.worksheet(SHEET4)

    # Ghi thời gian hiện tại vào ô B2
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    worksheet_report.update('B2', [[current_time]])
    print(f"REPORT sheet updated with current time: {current_time}")
if __name__=="__main__":
    main()