import streamlit as st
import distribution as dt  # File chứa các hàm tính toán
import pandas as pd
import show_distribution
from ai_analyst import render_ai_analyst_tab, render_feedback_tab, save_proposals


def sanitize_for_streamlit(df: pd.DataFrame) -> pd.DataFrame:
    """Make df Arrow-safe for Streamlit rendering."""
    if df is None or df.empty:
        return df

    out = df.copy()

    # ✅ ép các cột code thường bị lẫn int/str về str
    for col in ["default_code", "fdcode", "store", "from_store", "to_store"]:
        if col in out.columns:
            out[col] = out[col].astype(str)

    for c in out.columns:
        s = out[c]

        # timezone datetime -> remove tz (bản mới tránh warning)
        if isinstance(s.dtype, pd.DatetimeTZDtype):
            out[c] = s.dt.tz_localize(None)
            continue

        # object columns: ép mixed types về string
        if s.dtype == "object":
            sample = s.dropna().head(200)
            if not sample.empty:
                types = {type(v) for v in sample.values}
                if len(types) > 1:
                    out[c] = s.astype(str)

    return out

def normalize_core_dtypes():
    """Ép các cột code về string ngay trong session_state để tránh Arrow lỗi ở mọi nơi."""
    if "df_merge" in st.session_state and not st.session_state.df_merge.empty:
        for col in ["default_code", "fdcode", "store"]:
            if col in st.session_state.df_merge.columns:
                st.session_state.df_merge[col] = st.session_state.df_merge[col].astype(str)

    for key in ["df_warehouse", "df_process_warehouse", "df_warehouse_ecom"]:
        df = st.session_state.get(key)
        if df is not None and not df.empty and "fdcode" in df.columns:
            df["fdcode"] = df["fdcode"].astype(str)
            st.session_state[key] = df

# Khởi tạo dữ liệu tồn kho và sức bán
def initialize_inventory(moh_value):
    dt.MOH = moh_value
    dt.initialize_data()
    show_distribution.show_stock()
    # ✅ THÊM df_warehouse_ecom vào return
    return (
        dt.df_merge.copy(), 
        dt.df_warehouse.copy(), 
        dt.df_process_warehouse.copy(),
        dt.df_warehouse_ecom.copy()  # ✅ THÊM DÒNG NÀY
    )

# Hàm cập nhật tồn kho sau mỗi tác vụ
def update_stock(transfer_df, df_merge, df_warehouse, df_process_warehouse, df_warehouse_ecom=None):
    """
    Cập nhật tồn kho sau khi transfer
    
    Returns:
        tuple: (df_merge, df_warehouse, df_process_warehouse, df_warehouse_ecom)
    """
    if transfer_df.empty:
        return df_merge, df_warehouse, df_process_warehouse, df_warehouse_ecom
    
    # Copy để không modify original
    df_merge = df_merge.copy()
    df_warehouse = df_warehouse.copy() if df_warehouse is not None else None
    df_process_warehouse = df_process_warehouse.copy() if df_process_warehouse is not None else None
    df_warehouse_ecom = df_warehouse_ecom.copy() if df_warehouse_ecom is not None else None
    
    # Cập nhật tồn kho tại điểm đến (stores)
    for _, row in transfer_df.iterrows():
        to_store = row['to_store']
        fdcode = row['fdcode']
        qty = row['transfer_qty']
        
        # Tìm và cập nhật trong df_merge
        mask = (df_merge['store'] == to_store) & (df_merge['fdcode'] == fdcode)
        if mask.any():
            df_merge.loc[mask, 'available'] = df_merge.loc[mask, 'available'].fillna(0) + qty
            df_merge.loc[mask, 'need_qty'] = df_merge.loc[mask, 'available'] - df_merge.loc[mask, 'plan_qty']
    
    # Cập nhật tồn kho tại nguồn (warehouses)
    for _, row in transfer_df.iterrows():
        from_store = row['from_store']
        fdcode = row['fdcode']
        qty = row['transfer_qty']
        
        # Trừ tồn từ kho nguồn
        if from_store == 'KHO TỔNG' and df_warehouse is not None:
            mask = df_warehouse['fdcode'] == fdcode
            if mask.any():
                df_warehouse.loc[mask, 'available'] = (
                    df_warehouse.loc[mask, 'available'].fillna(0) - qty
                ).clip(lower=0)
        
        elif from_store == 'ECOM_SG' and df_warehouse_ecom is not None:  # ✅ XỬ LÝ ECOM_SG
            mask = df_warehouse_ecom['fdcode'] == fdcode
            if mask.any():
                df_warehouse_ecom.loc[mask, 'available'] = (
                    df_warehouse_ecom.loc[mask, 'available'].fillna(0) - qty
                ).clip(lower=0)
        
        elif from_store == 'KHO GIA CÔNG' and df_process_warehouse is not None:
            mask = df_process_warehouse['fdcode'] == fdcode
            if mask.any():
                df_process_warehouse.loc[mask, 'available'] = (
                    df_process_warehouse.loc[mask, 'available'].fillna(0) - qty
                ).clip(lower=0)
    
    return df_merge, df_warehouse, df_process_warehouse, df_warehouse_ecom

# Hàm lọc loại bỏ cửa hàng và fdcode không cần luân chuyển
def filter_excluded_data(df, excluded_stores, excluded_fdcode):
    if excluded_stores:
        df = df[~df['store'].isin(excluded_stores)]
    if excluded_fdcode:
        df = df[~df['default_code'].isin(excluded_fdcode)]
    return df

# ✅ Khởi tạo session_state - THÊM df_warehouse_ecom
if "df_merge" not in st.session_state:
    st.session_state.df_merge = pd.DataFrame()

if "df_warehouse" not in st.session_state:
    st.session_state.df_warehouse = pd.DataFrame()

if "df_process_warehouse" not in st.session_state:
    st.session_state.df_process_warehouse = pd.DataFrame()

# ✅ THÊM DÒNG NÀY
if "df_warehouse_ecom" not in st.session_state:
    st.session_state.df_warehouse_ecom = pd.DataFrame()

if "task_results" not in st.session_state:
    st.session_state.task_results = []

if "df_merge_before" not in st.session_state:
    st.session_state.df_merge_before = pd.DataFrame()

if "show_add_store" not in st.session_state:
    st.session_state.show_add_store = False

# ✅ Config cho stock_from_warehouse
if "cfg_stock_from_wh" not in st.session_state:
    st.session_state.cfg_stock_from_wh = {
        "max_stock_normal_store": 3,
        "ecom_min_stock": 10,
        "ecom_max_stock": 200,
        "allow_ecom_fallback_to_general": False,
        "debug": False,
    }

# Giao diện Streamlit
st.sidebar.title("Chọn Chức Năng")
page = st.sidebar.selectbox(
    "Đi tới trang:",
    ["Distribution Task", "🤖 AI Analyst", "📊 Feedback Loop"]
)

if page == "Distribution Task":
    st.title("Distribution Task")

    # Tham số MOH
    moh_value = st.sidebar.number_input("Tham số MOH:", min_value=1.0, max_value=12.0, value=2.5, step=0.1)

    # Nút khởi tạo dữ liệu
    if st.sidebar.button("Khởi tạo dữ liệu"):
        # ✅ NHẬN 4 giá trị thay vì 3
        (st.session_state.df_merge, 
         st.session_state.df_warehouse, 
         st.session_state.df_process_warehouse,
         st.session_state.df_warehouse_ecom) = initialize_inventory(moh_value)  # ✅ SỬA DÒNG NÀ
        normalize_core_dtypes()

        st.session_state.df_merge["Is_New_store"] = 0
        st.session_state.df_merge_before = st.session_state.df_merge.copy()
        st.session_state.df_merge_initial = st.session_state.df_merge.copy()  # Lưu bản gốc
        st.session_state.df_warehouse_initial = st.session_state.df_warehouse.copy()  # Lưu bản gốc
        st.session_state.df_process_warehouse_initial = st.session_state.df_process_warehouse.copy()
        # ✅ THÊM DÒNG NÀY
        st.session_state.df_warehouse_ecom_initial = st.session_state.df_warehouse_ecom.copy()
        st.session_state.task_results = []
        st.success("Dữ liệu đã khởi tạo thành công!")

    # Nút "Làm Lại Từ Đầu" (Reset dữ liệu về trạng thái ban đầu)
    if st.sidebar.button("Làm Lại Từ Đầu"):
        if "df_merge_initial" in st.session_state and "df_warehouse_initial" in st.session_state:
            st.session_state.df_merge = st.session_state.df_merge_initial.copy()
            st.session_state.df_warehouse = st.session_state.df_warehouse_initial.copy()
            st.session_state.df_process_warehouse = st.session_state.df_process_warehouse_initial.copy()
            st.session_state.df_warehouse_ecom = st.session_state.df_warehouse_ecom_initial.copy()
            normalize_core_dtypes()
            st.session_state.task_results = []
            st.session_state.show_add_store = False
            st.success("Dữ liệu đã được khôi phục về trạng thái ban đầu!")
        else:
            st.error("Dữ liệu chưa được khởi tạo!")

    # Tùy chọn loại bỏ cửa hàng và fdcode
    st.sidebar.subheader("Tùy Chọn Loại Bỏ")
    excluded_stores = st.sidebar.multiselect("Chọn cửa hàng không luân chuyển:", 
                                             st.session_state.df_merge['store'].unique()
                                             if not st.session_state.df_merge.empty else [])
    excluded_fdcode = st.sidebar.multiselect("Chọn default_code không luân chuyển:", 
                                          st.session_state.df_merge['default_code'].unique() 
                                          if not st.session_state.df_merge.empty else [])
    # ✅ UI cấu hình bốc tồn
    st.sidebar.subheader("Cấu hình Bốc Tồn (Kho Tổng)")

    with st.sidebar.expander("Thiết lập nâng cao", expanded=False):
        cfg = st.session_state.cfg_stock_from_wh

        cfg["max_stock_normal_store"] = st.number_input(
            "max_stock_normal_store",
            min_value=1, max_value=20,
            value=int(cfg["max_stock_normal_store"]),
            step=1
        )

        cfg["ecom_min_stock"] = st.number_input(
            "ecom_min_stock (ưu tiên ECOM)",
            min_value=10, max_value=50,
            value=int(cfg["ecom_min_stock"]),
            step=10
        )

        cfg["ecom_max_stock"] = st.number_input(
            "ecom_max_stock (giới hạn cứng ECOM)",
            min_value=50, max_value=500,
            value=int(cfg["ecom_max_stock"]),
            step=10
        )

        cfg["allow_ecom_fallback_to_general"] = st.checkbox(
            "Bốc hàng ĐA KHO",
            value=bool(cfg["allow_ecom_fallback_to_general"])
        )

        cfg["debug"] = st.checkbox(
            "debug",
            value=bool(cfg["debug"])
        )

    # Gom hàng cho cửa hàng mới
    if st.sidebar.button("Gom Hàng - New store"):
        if not st.session_state.df_merge.empty and not st.session_state.df_warehouse.empty:
            filtered_df = filter_excluded_data(st.session_state.df_merge.copy(), excluded_stores, excluded_fdcode)
            transfer_df, st.session_state.df_warehouse = dt.stock_for_new_store(filtered_df, st.session_state.df_warehouse)
            st.session_state.df_merge, st.session_state.df_warehouse, st.session_state.df_process_warehouse = update_stock(
                transfer_df, st.session_state.df_merge, st.session_state.df_warehouse, st.session_state.df_process_warehouse
            )
            normalize_core_dtypes()
            st.session_state.task_results.append(("Gom Hàng Cho Cửa Hàng Mới", sanitize_for_streamlit(transfer_df)))
            st.success("Đã gom hàng cho cửa hàng mới!")

    # Luân chuyển hàng hóa giữa các cửa hàng
    if st.sidebar.button("Luân Chuyển"):
        if not st.session_state.df_merge.empty:
            filtered_df = filter_excluded_data(st.session_state.df_merge.copy(), excluded_stores, excluded_fdcode)
            transfer_df = dt.transfer_between_stores(filtered_df, st.session_state.df_warehouse)
            st.session_state.df_merge, st.session_state.df_warehouse, st.session_state.df_process_warehouse, st.session_state.df_warehouse_ecom = update_stock(
                transfer_df, st.session_state.df_merge, st.session_state.df_warehouse, st.session_state.df_process_warehouse, st.session_state.df_warehouse_ecom
            )
            normalize_core_dtypes()
            st.session_state.task_results.append(("Luân Chuyển Giữa Cửa Hàng", sanitize_for_streamlit(transfer_df)))
            st.success("Đã luân chuyển hàng hóa!")
            # THÊM 2 DÒNG SAU st.success(...):
            n_saved = save_proposals(transfer_df, session_id="luan_chuyen")
            st.caption(f"💾 Đã lưu {n_saved} đề xuất vào Feedback Loop")

    # Bốc tồn từ kho tổng
    if st.sidebar.button("Bốc Tồn Từ Kho Tổng"):
        if not st.session_state.df_merge.empty and not st.session_state.df_process_warehouse.empty:
            if 'fdcode' not in st.session_state.df_process_warehouse.columns or 'available' not in st.session_state.df_process_warehouse.columns:
                st.error("Dữ liệu 'KHO GIA CÔNG' không hợp lệ. Vui lòng kiểm tra lại.")
            else:
                filtered_df = filter_excluded_data(st.session_state.df_merge.copy(), excluded_stores, excluded_fdcode)
                
                # ✅ FIX: Truyền đầy đủ tham số, bao gồm df_warehouse_ecom
                cfg = st.session_state.cfg_stock_from_wh

                transfer_df = dt.stock_from_warehouse(
                    filtered_df=filtered_df,
                    df_warehouse=st.session_state.df_warehouse,
                    df_process_warehouse=st.session_state.df_process_warehouse,
                    df_warehouse_ecom=st.session_state.get("df_warehouse_ecom", None),
                    **cfg
                )
                
                # ✅ Cập nhật cả df_warehouse_ecom sau khi transfer
                (st.session_state.df_merge, 
                st.session_state.df_warehouse, 
                st.session_state.df_process_warehouse,
                st.session_state.df_warehouse_ecom) = update_stock(
                    transfer_df, 
                    st.session_state.df_merge, 
                    st.session_state.df_warehouse, 
                    st.session_state.df_process_warehouse,
                    st.session_state.df_warehouse_ecom  # ✅ THÊM THAM SỐ NÀY
                )
                normalize_core_dtypes()
                st.session_state.task_results.append(("Bốc Tồn Từ Kho Tổng", sanitize_for_streamlit(transfer_df)))
                st.success("Đã bốc tồn từ kho tổng!")
                n_saved = save_proposals(transfer_df, session_id="boc_ton")
                st.caption(f"💾 Đã lưu {n_saved} đề xuất vào Feedback Loop")
        else:
            st.error("Dữ liệu kho không hợp lệ. Vui lòng kiểm tra dữ liệu!")
    
    # LẤY HÀNG THEO DANH SÁCH PHÂN BỔ
    st.sidebar.title("Phân Bổ Từ Danh Sách Import")
    uploaded_file = st.sidebar.file_uploader("Tải lên file danh sách (Excel):", type=["xlsx", "xls"])
    if uploaded_file:
        try:
            imported_df = pd.read_excel(uploaded_file)
            if 'fdcode' not in imported_df.columns or 'qty' not in imported_df.columns:
                st.error("File Excel phải chứa cột 'fdcode' và 'qty.")
            else:
                st.write("Dữ liệu import:")
                st.dataframe(sanitize_for_streamlit(imported_df))

                # Kiểm tra nếu dữ liệu tồn kho đã khởi tạo
                if "df_merge" in st.session_state and not st.session_state.df_merge.empty:
                    # Lọc dữ liệu theo các bộ lọc loại bỏ
                    filtered_df_merge = filter_excluded_data(
                        st.session_state.df_merge.copy(),
                        excluded_stores,
                        excluded_fdcode
                    )

                    # Nút phân bổ
                    if st.sidebar.button("Phân Bổ Từ Danh Sách Import"):
                        transfer_df, st.session_state.df_merge = dt.allocate_import_to_stores(
                            imported_df, filtered_df_merge
                        )
                        st.session_state.task_results.append(("Phân Bổ Từ Danh Sách Import", sanitize_for_streamlit(transfer_df)))
                        st.success("Đã phân bổ số lượng từ danh sách import!")
                        st.write("Kết quả phân bổ:")
                        st.dataframe(sanitize_for_streamlit(transfer_df))
                else:
                    st.error("Dữ liệu tồn kho chưa được khởi tạo!")
        except Exception as e:
            st.error(f"Lỗi khi đọc file: {e}")       
###################################################################
    # RÚT HÀNG THEO DANH SÁCH
    st.sidebar.title("Rút Hàng Theo Danh Sách")
    uploaded_withdraw_file = st.sidebar.file_uploader(
        "Tải lên file danh sách rút hàng (Excel):", 
        type=["xlsx", "xls"],
        key="withdraw_file"
    )

    if uploaded_withdraw_file:
        try:
            withdraw_df = pd.read_excel(uploaded_withdraw_file)
            
            if 'fdcode' not in withdraw_df.columns or 'qty' not in withdraw_df.columns:
                st.error("File Excel phải chứa cột 'fdcode' và 'qty'.")
            else:
                st.write("### 📋 Dữ liệu yêu cầu rút hàng:")
                display_withdraw_input = withdraw_df.copy()
                display_withdraw_input = display_withdraw_input.rename(columns={
                    'fdcode': 'Mã sản phẩm',
                    'qty': 'Số lượng cần rút'
                })
                st.dataframe(sanitize_for_streamlit(display_withdraw_input), use_container_width=True)

                if "df_merge" in st.session_state and not st.session_state.df_merge.empty:
                    # Lọc dữ liệu theo các bộ lọc loại bỏ
                    filtered_df_merge = filter_excluded_data(
                        st.session_state.df_merge.copy(),
                        excluded_stores,
                        excluded_fdcode
                    )
                    
                    # ✅ LẤY THÊM DF_WAREHOUSE VÀ DF_WAREHOUSE_ECOM TỪ SESSION STATE
                    df_warehouse_current = st.session_state.get('df_warehouse', pd.DataFrame())
                    df_warehouse_ecom_current = st.session_state.get('df_warehouse_ecom', pd.DataFrame())
                    
                    # Hiển thị tổng quan
                    st.write("### 📊 Tổng quan yêu cầu rút:")
                    summary_withdraw = withdraw_df.groupby('fdcode')['qty'].sum().reset_index()
                    summary_withdraw.columns = ['fdcode', 'total_qty_needed']
                    
                    display_summary = summary_withdraw.copy()
                    display_summary = display_summary.rename(columns={
                        'fdcode': 'Mã sản phẩm',
                        'total_qty_needed': 'Tổng số lượng cần rút'
                    })
                    st.dataframe(sanitize_for_streamlit(display_summary), use_container_width=True)
                    
                    # Kiểm tra tồn kho (GỘP CẢ KHO)
                    st.write("### ✅ Kiểm tra tồn kho (bao gồm cả kho):")
                    check_results = []
                    for _, row in summary_withdraw.iterrows():
                        msp = row['fdcode']
                        qty_needed = row['total_qty_needed']
                        
                        # Tính tổng từ tất cả nguồn
                        store_available = filtered_df_merge[filtered_df_merge['fdcode'] == msp]['available'].sum()
                        warehouse_available = df_warehouse_current[df_warehouse_current['fdcode'] == msp]['available'].sum() if not df_warehouse_current.empty else 0
                        warehouse_ecom_available = df_warehouse_ecom_current[df_warehouse_ecom_current['fdcode'] == msp]['available'].sum() if not df_warehouse_ecom_current.empty else 0
                        
                        available_total = store_available + warehouse_available + warehouse_ecom_available
                        
                        status = "✅ Đủ hàng" if available_total >= qty_needed else "⚠️ Không đủ"
                        check_results.append({
                            'Mã sản phẩm': msp,
                            'Cần rút': int(qty_needed),
                            'Tồn stores': int(store_available),
                            'Tồn KHO TỔNG': int(warehouse_available),
                            'Tồn ECOM_SG': int(warehouse_ecom_available),
                            'Tổng tồn': int(available_total),
                            'Chênh lệch': int(available_total - qty_needed),
                            'Trạng thái': status
                        })
                    
                    check_df = pd.DataFrame(check_results)
                    st.dataframe(sanitize_for_streamlit(check_df), use_container_width=True)

                    # Nút rút hàng
                    if st.sidebar.button("🔽 Thực Hiện Rút Hàng", use_container_width=True):
                        with st.spinner('Đang xử lý rút hàng...'):
                            # ✅ TRUYỀN THÊM DF_WAREHOUSE VÀ DF_WAREHOUSE_ECOM
                            withdraw_result_df, updated_merge, updated_warehouse, updated_warehouse_ecom = dt.withdraw_from_stores(
                                withdraw_df, 
                                filtered_df_merge,
                                df_warehouse_current,
                                df_warehouse_ecom_current
                            )
                            
                            # ✅ CẬP NHẬT LẠI SESSION STATE
                            st.session_state.df_merge = updated_merge
                            if updated_warehouse is not None:
                                st.session_state.df_warehouse = updated_warehouse
                            if updated_warehouse_ecom is not None:
                                st.session_state.df_warehouse_ecom = updated_warehouse_ecom
                            
                            if not withdraw_result_df.empty:
                                st.session_state.task_results.append(
                                    ("Rút Hàng Theo Danh Sách", sanitize_for_streamlit(withdraw_result_df))
                                )
                                st.success("✅ Đã thực hiện rút hàng thành công!")
                                n_saved = save_proposals(transfer_df, session_id="boc_ton")
                                st.caption(f"💾 Đã lưu {n_saved} đề xuất vào Feedback Loop")
                                
                                # Hiển thị kết quả chi tiết
                                st.write("### 📦 Kết quả rút hàng chi tiết:")
                                display_result = withdraw_result_df.copy()
                                display_result = display_result.rename(columns={
                                    'fdcode': 'Mã sản phẩm',
                                    'from_store': 'Rút từ kho/CH',
                                    'withdraw_qty': 'Số lượng'
                                })
                                st.dataframe(sanitize_for_streamlit(display_result), use_container_width=True)
                                
                                # Tổng hợp theo fdcode
                                st.write("### 📈 Tổng hợp theo mã sản phẩm:")
                                summary_result = withdraw_result_df.groupby('fdcode')['withdraw_qty'].sum().reset_index()
                                summary_result.columns = ['fdcode', 'total_withdrawn']
                                
                                summary_comparison = summary_withdraw.merge(
                                    summary_result, 
                                    on='fdcode', 
                                    how='left'
                                )
                                summary_comparison['total_withdrawn'] = summary_comparison['total_withdrawn'].fillna(0).astype(int)
                                summary_comparison['shortage'] = (
                                    summary_comparison['total_qty_needed'] - 
                                    summary_comparison['total_withdrawn']
                                ).astype(int)
                                
                                display_comparison = summary_comparison.rename(columns={
                                    'fdcode': 'Mã sản phẩm',
                                    'total_qty_needed': 'Yêu cầu rút',
                                    'total_withdrawn': 'Đã rút',
                                    'shortage': 'Còn thiếu'
                                })
                                
                                st.dataframe(sanitize_for_streamlit(display_comparison), use_container_width=True)
                                
                                # Cảnh báo
                                shortage_items = display_comparison[display_comparison['Còn thiếu'] > 0]
                                if not shortage_items.empty:
                                    st.warning("⚠️ Một số sản phẩm không rút đủ:")
                                    st.dataframe(sanitize_for_streamlit(shortage_items), use_container_width=True)
                                
                                # Tổng hợp theo store
                                st.write("### 🏪 Tổng hợp theo kho/cửa hàng:")
                                summary_by_store = withdraw_result_df.groupby('from_store')['withdraw_qty'].sum().reset_index()
                                summary_by_store.columns = ['from_store', 'total_qty']
                                summary_by_store = summary_by_store.sort_values('total_qty', ascending=False)
                                
                                display_by_store = summary_by_store.rename(columns={
                                    'from_store': 'Kho/Cửa hàng',
                                    'total_qty': 'Tổng số lượng đã rút'
                                })
                                
                                st.dataframe(sanitize_for_streamlit(display_by_store), use_container_width=True)
                            else:
                                st.warning("⚠️ Không có hàng để rút!")
                                
                else:
                    st.error("❌ Dữ liệu tồn kho chưa được khởi tạo!")
                    
        except Exception as e:
            st.error(f"❌ Lỗi: {e}")
            import traceback
            st.code(traceback.format_exc())
####################################
    # Hiển thị kết quả từng thao tác
    if st.session_state.task_results:
        st.subheader("Kết Quả Từng Thao Tác")
        for idx, (title, result_df) in enumerate(st.session_state.task_results):
            with st.expander(f"{title}"):
                st.dataframe(sanitize_for_streamlit(result_df))

    # Hiển thị tồn kho hiện tại
    if "df_merge" in st.session_state and not st.session_state.df_merge.empty:
        st.subheader("Tồn Kho Hiện Tại")
        st.dataframe(sanitize_for_streamlit(st.session_state.df_merge))

    if "df_warehouse" in st.session_state and not st.session_state.df_warehouse.empty and \
    "df_process_warehouse" in st.session_state and not st.session_state.df_process_warehouse.empty:
        col1, col2 = st.columns(2)  # Chia giao diện thành 2 cột

        # Hiển thị Tồn Kho KHO TỔNG
        with col1:
            st.subheader("TỒN KHO TỔNG")
            st.dataframe(sanitize_for_streamlit(st.session_state.df_warehouse))

        # Hiển thị Tồn Kho KHO GIA CÔNG
        with col2:
            st.subheader("TỒN KHO GIA CÔNG")
            st.dataframe(sanitize_for_streamlit(st.session_state.df_process_warehouse))
    # So sánh tồn kho trước và sau
    if not st.session_state.df_merge_before.empty:
        st.subheader("So Sánh Tồn Kho Trước và Sau")
        df_before = st.session_state.df_merge_before.groupby("store")['available'].sum().rename("Trước")
        df_after = st.session_state.df_merge.groupby("store")['available'].sum().rename("Sau")
        df_comparison = pd.concat([df_before, df_after], axis=1).fillna(0)
        df_comparison['% Thay Đổi'] = ((df_comparison['Sau'] - df_comparison['Trước']) / df_comparison['Trước'].replace(0, 1)) * 100
        df_comparison['% Thay Đổi'] = df_comparison['% Thay Đổi'].round(1)
        df_comparison = df_comparison.sort_values(by='% Thay Đổi', ascending=False)
        st.dataframe(sanitize_for_streamlit(df_comparison))

    # Tạo cửa hàng mới
    if st.sidebar.button("Tạo Cửa Hàng Mới"):
        st.session_state.show_add_store = True

    if st.session_state.show_add_store:
        st.subheader("Tạo Cửa Hàng Mới")
        with st.form("add_new_store"):
            new_store_name = st.text_input("Tên cửa hàng mới:")
            new_store_qty = st.number_input("Tổng số lượng cần nhập:", value=0, step=1)
            submitted = st.form_submit_button("Thêm Cửa Hàng")
            if submitted:
                if not st.session_state.df_merge.empty:
                    existing_fdcode = st.session_state.df_merge['fdcode'].unique()
                    new_store_data = pd.DataFrame({
                        "store": [new_store_name] * len(existing_fdcode),
                        "fdcode": existing_fdcode,
                        "available": 0,
                        "need_qty": -new_store_qty // len(existing_fdcode),
                        "Is_New_store": 1
                    })
                    st.session_state.df_merge = pd.concat([st.session_state.df_merge, new_store_data], ignore_index=True)
                    st.success(f"Đã thêm cửa hàng mới: {new_store_name}")
                else:
                    st.error("Dữ liệu chưa được khởi tạo!")

import streamlit as st
import pandas as pd

# Đặt đoạn này NGAY SAU toàn bộ block `if page == "Distribution Task": ...`

if False:  # ← đây chỉ là placeholder để IDE không báo lỗi, xóa dòng này khi paste vào
    pass

# ── TRANG AI ANALYST ──────────────────────────────────────────────
# Paste đoạn này vào cuối streamlit_distribution.py:

elif page == "🤖 AI Analyst":
    render_ai_analyst_tab(
        df_merge        = st.session_state.get("df_merge", pd.DataFrame()),
        df_warehouse    = st.session_state.get("df_warehouse", pd.DataFrame()),
        df_warehouse_ecom = st.session_state.get("df_warehouse_ecom", pd.DataFrame()),
    )


# ── TRANG FEEDBACK LOOP ───────────────────────────────────────────
# Paste đoạn này tiếp theo:

elif page == "📊 Feedback Loop":
    render_feedback_tab()
