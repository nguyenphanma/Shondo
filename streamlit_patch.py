"""
streamlit_patch.py
──────────────────────────────────────────────────────────────────
Đây là HƯỚNG DẪN tích hợp ai_analyst.py vào streamlit_distribution.py.
Không chạy file này trực tiếp — chỉ copy các đoạn cần thiết vào đúng vị trí.

3 bước:
  1. Thêm import ở đầu file
  2. Thêm "AI Analyst" và "Feedback Loop" vào page selector
  3. Thêm 2 block xử lý trang mới
──────────────────────────────────────────────────────────────────
"""

# ══════════════════════════════════════════════════════════════════
# BƯỚC 1 — THÊM VÀO ĐẦU FILE (sau các import hiện có)
# ══════════════════════════════════════════════════════════════════

"""
# --- THÊM DÒNG NÀY sau `import show_distribution` ---
from ai_analyst import render_ai_analyst_tab, render_feedback_tab, save_proposals
"""


# ══════════════════════════════════════════════════════════════════
# BƯỚC 2 — SỬA DÒNG page selector (dòng ~165 trong file gốc)
# ══════════════════════════════════════════════════════════════════

"""
# TÌM DÒNG NÀY:
page = st.sidebar.selectbox("Đi tới trang:", ["Distribution Task"])

# THAY BẰNG:
page = st.sidebar.selectbox(
    "Đi tới trang:",
    ["Distribution Task", "🤖 AI Analyst", "📊 Feedback Loop"]
)
"""


# ══════════════════════════════════════════════════════════════════
# BƯỚC 3 — THÊM VÀO CUỐI FILE (sau block `if page == "Distribution Task":`)
# ══════════════════════════════════════════════════════════════════

import streamlit as st
import pandas as pd

# Đặt đoạn này NGAY SAU toàn bộ block `if page == "Distribution Task": ...`

if False:  # ← đây chỉ là placeholder để IDE không báo lỗi, xóa dòng này khi paste vào
    pass

# ── TRANG AI ANALYST ──────────────────────────────────────────────
# Paste đoạn này vào cuối streamlit_distribution.py:
"""
elif page == "🤖 AI Analyst":
    render_ai_analyst_tab(
        df_merge        = st.session_state.get("df_merge", pd.DataFrame()),
        df_warehouse    = st.session_state.get("df_warehouse", pd.DataFrame()),
        df_warehouse_ecom = st.session_state.get("df_warehouse_ecom", pd.DataFrame()),
    )
"""

# ── TRANG FEEDBACK LOOP ───────────────────────────────────────────
# Paste đoạn này tiếp theo:
"""
elif page == "📊 Feedback Loop":
    render_feedback_tab()
"""

# ══════════════════════════════════════════════════════════════════
# BƯỚC 4 (OPTIONAL) — LƯU ĐỀ XUẤT VÀO FEEDBACK KHI THỰC HIỆN TASK
# ══════════════════════════════════════════════════════════════════
# Sau mỗi lần thực hiện task (Luân Chuyển, Bốc Tồn, Rút Hàng),
# thêm 1 dòng để lưu đề xuất vào feedback DB.
# Ví dụ sau nút "Luân Chuyển":

"""
# TÌM ĐOẠN CODE NÀY (trong block Luân Chuyển):
    st.session_state.task_results.append(("Luân Chuyển Giữa Cửa Hàng", sanitize_for_streamlit(transfer_df)))
    st.success("Đã luân chuyển hàng hóa!")

# THÊM 2 DÒNG SAU st.success(...):
    n_saved = save_proposals(transfer_df, session_id="luan_chuyen")
    st.caption(f"💾 Đã lưu {n_saved} đề xuất vào Feedback Loop")
"""

# Tương tự cho "Bốc Tồn Từ Kho Tổng" và "Rút Hàng Theo Danh Sách":
"""
    n_saved = save_proposals(transfer_df, session_id="boc_ton")
    st.caption(f"💾 Đã lưu {n_saved} đề xuất vào Feedback Loop")
"""
