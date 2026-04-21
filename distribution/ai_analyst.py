"""
ai_analyst.py
─────────────────────────────────────────────────────────────────
Bước 1 — AI Analyst: Chat với tồn kho bằng tiếng Việt
Bước 3 — Feedback loop: Ghi nhận đề xuất → duyệt → kết quả thực tế

Cách dùng trong streamlit_distribution.py:
    from ai_analyst import render_ai_analyst_tab, render_feedback_tab
    # Thêm 2 tab mới vào sidebar page selector
"""

import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from sqlalchemy import text
from core.db import get_engine

import pandas as pd
import requests
import streamlit as st

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
GROQ_MODEL   = "llama-3.3-70b-versatile"
GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions"

# File SQLite lưu feedback (tạo tự động nếu chưa có)
FEEDBACK_DB_PATH = Path(__file__).parent / "feedback_loop.db"

# ══════════════════════════════════════════════
# PHẦN 1 — AI ANALYST
# ══════════════════════════════════════════════

def _build_stock_context(
    df_merge: pd.DataFrame,
    df_warehouse: pd.DataFrame,
    df_warehouse_ecom: pd.DataFrame,
) -> str:
    """
    Xây dựng context tồn kho chi tiết theo fdcode/store, bao gồm sức bán và tình trạng
    dư/thiếu để AI có thể đưa ra khuyến nghị phân phối cụ thể.
    """
    lines = []
    has_need = "need_qty" in df_merge.columns
    has_avg  = "avg_qty"  in df_merge.columns
    has_plan = "plan_qty" in df_merge.columns

    # ── 1. Tổng quan ──
    if not df_merge.empty:
        total_stores = df_merge["store"].nunique()
        total_skus   = df_merge["fdcode"].nunique()
        total_qty    = int(df_merge["available"].sum())
        lines.append(f"TỔNG QUAN: {total_stores} store, {total_skus} SKU, {total_qty:,} sp tồn")

        by_store = df_merge.groupby("store")["available"].sum().sort_values(ascending=False)
        lines.append("Tồn/store: " + " | ".join(f"{s}={int(q)}" for s, q in by_store.items()))

        by_cat = df_merge.groupby("subcategory")["available"].sum().sort_values(ascending=False)
        lines.append("Tồn/danh mục: " + " | ".join(f"{c}={int(q)}" for c, q in by_cat.items()))

    # ── 2. Chi tiết dư/thiếu theo store × fdcode ──
    if not df_merge.empty and has_need:
        df_m = df_merge.copy()
        df_m["available"] = pd.to_numeric(df_m["available"], errors="coerce").fillna(0)
        df_m["need_qty"]  = pd.to_numeric(df_m["need_qty"],  errors="coerce").fillna(0)
        if has_avg:
            df_m["avg_qty"] = pd.to_numeric(df_m["avg_qty"], errors="coerce").fillna(0)
        if has_plan:
            df_m["plan_qty"] = pd.to_numeric(df_m["plan_qty"], errors="coerce").fillna(0)

        def _fmt_row(r):
            avg  = f" avg={r['avg_qty']:.1f}/th"  if has_avg  else ""
            plan = f" plan={int(r['plan_qty'])}"   if has_plan else ""
            return (
                f"  {r['store']} | {r['default_code']} | {r['fdcode']} | {r['subcategory']}"
                f" | tồn={int(r['available'])}{plan}{avg} | need={int(r['need_qty'])}"
            )

        # Thiếu hàng (need_qty < 0): ưu tiên SKU có avg_qty cao → đang bán tốt mà thiếu hàng
        shortage = df_m[df_m["need_qty"] < 0].copy()
        if has_avg:
            shortage = shortage.sort_values(["avg_qty", "need_qty"], ascending=[False, True])
        else:
            shortage = shortage.sort_values("need_qty")
        shortage = shortage.head(25)
        if not shortage.empty:
            lines.append("\nTHIẾU HÀNG — cần bổ sung (need_qty<0, sắp xếp theo sức bán cao → thấp):")
            for _, r in shortage.iterrows():
                lines.append(_fmt_row(r))

        # Dư hàng (need_qty > 0): sắp xếp theo dư nhiều nhất, loại KHO TỔNG (không phải store bán lẻ)
        surplus = df_m[(df_m["need_qty"] > 0) & (~df_m["store"].str.upper().str.contains("KHO"))].copy()
        surplus = surplus.sort_values("need_qty", ascending=False).head(25)
        if not surplus.empty:
            lines.append("\nDƯ HÀNG — có thể luân chuyển đi (need_qty>0):")
            for _, r in surplus.iterrows():
                lines.append(_fmt_row(r))

    # ── 4. Kho nguồn: hiện tồn cho đúng các fdcode đang thiếu ──
    # Lấy danh sách fdcode đang thiếu để cross-check với kho
    shortage_fdcodes = set()
    if not df_merge.empty and has_need:
        shortage_fdcodes = set(
            df_merge[df_merge["need_qty"] < 0]["fdcode"].unique()
        )

    if df_warehouse is not None and not df_warehouse.empty:
        wh = df_warehouse.groupby("fdcode")["available"].sum()
        wh_total = int(wh.sum())
        # Ưu tiên fdcode đang thiếu có tồn kho, rồi top theo available
        wh_shortage = wh[wh.index.isin(shortage_fdcodes) & (wh > 0)].sort_values(ascending=False)
        wh_other    = wh[~wh.index.isin(shortage_fdcodes) & (wh > 0)].sort_values(ascending=False).head(10)
        lines.append(f"\nKHO TỔNG (nguồn cấp chính cho store lẻ): {wh_total:,} sp")
        if not wh_shortage.empty:
            lines.append("  Có hàng cho fdcode đang THIẾU: " +
                         " | ".join(f"{c}={int(q)}" for c, q in wh_shortage.items()))
        if not wh_other.empty:
            lines.append("  Fdcode khác (top 10): " +
                         " | ".join(f"{c}={int(q)}" for c, q in wh_other.items()))

    if df_warehouse_ecom is not None and not df_warehouse_ecom.empty:
        we = df_warehouse_ecom.groupby("fdcode")["available"].sum()
        we_total = int(we.sum())
        # ECOM_SG cấp cho ECOM — lọc fdcode ECOM đang thiếu
        ecom_shortage_fdcodes = set()
        if not df_merge.empty and has_need:
            ecom_shortage_fdcodes = set(
                df_merge[
                    (df_merge["need_qty"] < 0) &
                    (df_merge["store"].str.upper() == "ECOM")
                ]["fdcode"].unique()
            )
        we_shortage = we[we.index.isin(ecom_shortage_fdcodes) & (we > 0)].sort_values(ascending=False)
        we_other    = we[~we.index.isin(ecom_shortage_fdcodes) & (we > 0)].sort_values(ascending=False).head(10)
        lines.append(f"\nKHO ECOM_SG (nguồn cấp riêng cho kênh ECOM): {we_total:,} sp")
        if not we_shortage.empty:
            lines.append("  Có hàng cho fdcode ECOM đang THIẾU: " +
                         " | ".join(f"{c}={int(q)}" for c, q in we_shortage.items()))
        if not we_other.empty:
            lines.append("  Fdcode khác (top 10): " +
                         " | ".join(f"{c}={int(q)}" for c, q in we_other.items()))

    return "\n".join(lines)

def _call_gemini(system_prompt: str, messages: list) -> str:
    """Gọi Groq API (llama), trả về text response."""
    api_key = os.getenv("GROQ_API_KEY", "")
    if not api_key:
        return (
            "❌ Chưa có GROQ_API_KEY.\n\n"
            "Thêm dòng sau vào file `.env`:\n"
            "```\nGROQ_API_KEY=gsk_...\n```\n"
            "Lấy API key miễn phí tại: https://console.groq.com\n"
            "Sau đó restart lại Streamlit."
        )

    payload = {
        "model": GROQ_MODEL,
        "messages": [{"role": "system", "content": system_prompt}] + messages,
        "max_tokens": 1500,
        "temperature": 0.4,
    }

    try:
        resp = requests.post(
            GROQ_API_URL,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {api_key}",
            },
            json=payload,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"]
    except requests.exceptions.Timeout:
        return "⚠️ Timeout — thử lại nhé."
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code if e.response is not None else "?"
        detail = ""
        if e.response is not None:
            try:
                detail = e.response.json().get("error", {}).get("message", e.response.text)
            except Exception:
                detail = e.response.text
        if code == 401:
            return "❌ API key không hợp lệ. Kiểm tra lại GROQ_API_KEY."
        elif code == 429:
            return "⚠️ Rate limit — chờ vài giây rồi thử lại."
        return f"❌ Lỗi HTTP {code}: {detail}"
    except Exception as e:
        return f"❌ Lỗi không xác định: {e}"

def render_ai_analyst_tab(
    df_merge: pd.DataFrame,
    df_warehouse: pd.DataFrame,
    df_warehouse_ecom: pd.DataFrame,
) -> None:
    """
    Render tab AI Analyst trong Streamlit.
    Người dùng chat bằng tiếng Việt, Claude phân tích tồn kho và trả lời.
    """
    st.header("🤖 AI Analyst — Hỏi đáp tồn kho")
    st.caption(
        "Hỏi bất kỳ câu hỏi nào về tồn kho bằng tiếng Việt. "
        "AI sẽ phân tích dữ liệu thực tế và đưa ra gợi ý."
    )

    # Kiểm tra dữ liệu
    if df_merge is None or df_merge.empty:
        st.warning("⚠️ Chưa có dữ liệu tồn kho. Vui lòng **Khởi tạo dữ liệu** trước.")
        return

    # Khởi tạo session state cho chat
    if "ai_chat_history" not in st.session_state:
        st.session_state.ai_chat_history = []
    if "stock_context_cache" not in st.session_state:
        st.session_state.stock_context_cache = None

    # ── Nút làm mới context ──
    col1, col2 = st.columns([3, 1])
    with col2:
        if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
            st.session_state.stock_context_cache = None
            st.session_state.ai_chat_history = []
            st.success("Đã làm mới!")

    # Build context (cache để không rebuild mỗi lần chat)
    if st.session_state.stock_context_cache is None:
        with st.spinner("Đang chuẩn bị dữ liệu tồn kho..."):
            st.session_state.stock_context_cache = _build_stock_context(
                df_merge, df_warehouse, df_warehouse_ecom
            )

    stock_context = st.session_state.stock_context_cache

    # System prompt
    system_prompt = f"""Bạn là chuyên gia phân tích tồn kho và phân phối hàng hóa cho chuỗi bán lẻ giày dép Shondo.
Trả lời bằng tiếng Việt, ngắn gọn, đưa số liệu cụ thể theo fdcode/default_code/store.

=== QUY TẮC NGHIỆP VỤ ===
- default_code: mã sản phẩm cha (model). fdcode: mã SKU con (màu/size cụ thể).
- avg_qty: sức bán trung bình/tháng tại store đó. plan_qty = avg_qty × MOH (mặc định 2.5 tháng).
- need_qty = available - plan_qty:
    • need_qty > 0 → DƯ THỪA (tồn vượt kế hoạch, cân nhắc luân chuyển đi)
    • need_qty < 0 → THIẾU HÀNG (cần bổ sung)
    • need_qty = -2 → không có lịch sử bán + không có tồn (bỏ qua)
- Sản phẩm bán chính tại store = fdcode có avg_qty cao nhất tại store đó.
- Tồn chết = tồn nhiều (available cao) nhưng avg_qty thấp hoặc = 0.

=== NGUỒN CUNG CẤP (theo thứ tự ưu tiên) ===
1. KHO TỔNG → cấp hàng cho tất cả store lẻ (ưu tiên đầu tiên)
2. KDS (kho sỉ) → có thể rút sang store lẻ nếu KHO TỔNG hết
3. Store lẻ dư → luân chuyển sang store lẻ thiếu
4. ECOM → rút cuối cùng, chỉ khi không còn nguồn khác
- KHO ECOM_SG: kho riêng cấp hàng cho kênh ECOM, KHÔNG dùng để cấp store lẻ.
- KHO GIA CÔNG: kho gia công, không tính trong phân phối thông thường.

=== KHI ĐỀ XUẤT PHÂN PHỐI ===
- Nêu rõ: rút fdcode nào, từ store/kho nào, chuyển đến store nào, số lượng bao nhiêu
- Ưu tiên SKU có avg_qty cao bị thiếu hàng trước
- Không rút dưới mức tối thiểu: KHO TỔNG giữ lại max(5, 20% tồn); store ECOM giữ max(5, avg_qty×1.5)

=== DỮ LIỆU TỒN KHO HIỆN TẠI ===
{stock_context}

Ngày: {datetime.today().strftime('%d/%m/%Y')}"""

    # ── Câu hỏi gợi ý ──
    st.markdown("**Câu hỏi gợi ý:**")
    suggestion_cols = st.columns(2)
    suggestions = [
        "Store nào đang dư hàng SANDALS nhiều nhất?",
        "SP nào nên rút về kho gấp?",
        "So sánh tồn kho ECOM vs store hiện tại",
        "Mã nào đang tồn chết (tồn nhiều, sức bán thấp)?",
    ]
    for i, suggestion in enumerate(suggestions):
        with suggestion_cols[i % 2]:
            if st.button(suggestion, key=f"suggest_{i}", use_container_width=True):
                st.session_state.ai_pending_question = suggestion

    st.divider()

    # ── Hiển thị lịch sử chat ──
    chat_container = st.container()
    with chat_container:
        for msg in st.session_state.ai_chat_history:
            with st.chat_message(msg["role"]):
                st.markdown(msg["content"])

    # ── Input người dùng ──
    user_input = st.chat_input("Nhập câu hỏi về tồn kho...")

    # Xử lý câu hỏi từ nút gợi ý
    if "ai_pending_question" in st.session_state:
        user_input = st.session_state.pop("ai_pending_question")

    if user_input:
        # Hiện câu hỏi của user
        with chat_container:
            with st.chat_message("user"):
                st.markdown(user_input)

        st.session_state.ai_chat_history.append({"role": "user", "content": user_input})

        # Gọi Claude
        with chat_container:
            with st.chat_message("assistant"):
                with st.spinner("Đang phân tích..."):
                    # Giữ tối đa 10 lượt chat gần nhất để tránh vượt context
                    recent_history = st.session_state.ai_chat_history[-10:]
                    api_messages = [
                        {"role": m["role"], "content": m["content"]}
                        for m in recent_history
                    ]
                    answer = _call_gemini(system_prompt, api_messages)
                st.markdown(answer)

        st.session_state.ai_chat_history.append({"role": "assistant", "content": answer})

    # ── Nút xóa lịch sử ──
    if st.session_state.ai_chat_history:
        if st.button("🗑️ Xóa lịch sử chat"):
            st.session_state.ai_chat_history = []
            st.rerun()

# ══════════════════════════════════════════════
# PHẦN 2 — FEEDBACK LOOP
# ══════════════════════════════════════════════

def _init_feedback_db() -> sqlite3.Connection:
    """Tạo SQLite DB và bảng nếu chưa có."""
    conn = sqlite3.connect(FEEDBACK_DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS proposals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at    TEXT NOT NULL,
            session_id    TEXT,
            fdcode        TEXT NOT NULL,
            from_store    TEXT NOT NULL,
            to_store      TEXT,
            proposed_qty  INTEGER NOT NULL,
            reason        TEXT,
            confidence    TEXT,
            forecast_qty  INTEGER,
            available_qty INTEGER,
            status        TEXT DEFAULT 'pending',  -- pending/approved/rejected
            approved_qty  INTEGER,
            actual_qty    INTEGER,
            actual_date   TEXT,
            reviewed_by   TEXT,
            notes         TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS feedback_summary (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            updated_at    TEXT NOT NULL,
            fdcode        TEXT NOT NULL,
            store         TEXT NOT NULL,
            accuracy_rate REAL,
            avg_error     REAL,
            n_samples     INTEGER
        )
    """)
    conn.commit()
    return conn

def save_proposals(proposals_df: pd.DataFrame, session_id: str = None) -> int:
    """
    Lưu danh sách đề xuất phân phối vào DB trước khi người dùng duyệt.

    proposals_df phải có cột: fdcode, from_store, proposed_qty
    Có thể có thêm: to_store, reason, confidence, forecast_qty, available_qty
    """
    conn = _init_feedback_db()
    now = datetime.now().isoformat()
    session_id = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")

    rows = []
    for _, row in proposals_df.iterrows():
        rows.append((
            now,
            session_id,
            str(row.get("fdcode", "")),
            str(row.get("from_store", "")),
            str(row.get("to_store", "")) if "to_store" in row else None,
            int(row.get("proposed_qty", row.get("transfer_qty", row.get("withdraw_qty", 0)))),
            str(row.get("reason", "")) if "reason" in row else None,
            str(row.get("confidence", "")) if "confidence" in row else None,
            int(row.get("forecast_qty", 0)) if "forecast_qty" in row else None,
            int(row.get("available", 0)) if "available" in row else None,
        ))

    conn.executemany("""
        INSERT INTO proposals
            (created_at, session_id, fdcode, from_store, to_store,
             proposed_qty, reason, confidence, forecast_qty, available_qty)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, rows)
    conn.commit()
    conn.close()
    return len(rows)

def render_feedback_tab() -> None:
    """
    Render tab Feedback Loop — phiên bản tối ưu hiệu năng:
    - Phân trang (20 dòng/trang) thay vì render toàn bộ
    - Duyệt/từ chối hàng loạt bằng 1 lần commit
    - Không st.rerun() sau từng dòng, chỉ rerun sau batch
    - Đóng conn ngay sau mỗi query
    """
    st.header("📊 Feedback Loop — Học từ kết quả thực tế")
    st.caption("Duyệt đề xuất, nhập kết quả thực tế để AI tự cải thiện độ chính xác.")

    tab_review, tab_actual, tab_report = st.tabs([
        "✅ Duyệt đề xuất",
        "📝 Nhập kết quả thực tế",
        "📈 Báo cáo độ chính xác",
    ])

    PAGE_SIZE = 20  # số dòng mỗi trang

    # ────────────────────────────────────────
    # TAB 1 — DUYỆT ĐỀ XUẤT (phân trang + batch)
    # ────────────────────────────────────────
    with tab_review:
        st.subheader("Đề xuất đang chờ duyệt")

        # ── Đếm nhanh, không load toàn bộ ──
        with sqlite3.connect(FEEDBACK_DB_PATH) as conn:
            total_pending = conn.execute(
                "SELECT COUNT(*) FROM proposals WHERE status='pending'"
            ).fetchone()[0]

        if total_pending == 0:
            st.info("Không có đề xuất nào đang chờ duyệt.")
        else:
            total_pages = max(1, (total_pending + PAGE_SIZE - 1) // PAGE_SIZE)

            # ── Header: đếm + nút duyệt/từ chối tất cả ──
            h1, h2, h3 = st.columns([3, 1, 1])
            with h1:
                st.caption(f"Tổng **{total_pending}** đề xuất — trang {PAGE_SIZE} dòng/lần")
            with h2:
                if st.button("✅ Duyệt tất cả", use_container_width=True, type="primary"):
                    with sqlite3.connect(FEEDBACK_DB_PATH) as conn:
                        conn.execute(
                            """UPDATE proposals
                               SET status='approved', approved_qty=proposed_qty,
                                   reviewed_by='user'
                               WHERE status='pending'"""
                        )
                    st.success(f"Đã duyệt {total_pending} đề xuất!")
                    st.rerun()
            with h3:
                if st.button("❌ Từ chối tất cả", use_container_width=True):
                    with sqlite3.connect(FEEDBACK_DB_PATH) as conn:
                        conn.execute(
                            "UPDATE proposals SET status='rejected' WHERE status='pending'"
                        )
                    st.info(f"Đã từ chối {total_pending} đề xuất.")
                    st.rerun()

            # ── Chọn trang ──
            if "fb_page" not in st.session_state:
                st.session_state.fb_page = 1
            page = st.session_state.fb_page

            # ── Load CHỈ trang hiện tại ──
            offset = (page - 1) * PAGE_SIZE
            with sqlite3.connect(FEEDBACK_DB_PATH) as conn:
                df_page = pd.read_sql(
                    """SELECT id, fdcode, from_store, to_store,
                              proposed_qty, available_qty, forecast_qty,
                              confidence, reason
                       FROM proposals
                       WHERE status='pending'
                       ORDER BY created_at DESC
                       LIMIT ? OFFSET ?""",
                    conn, params=(PAGE_SIZE, offset)
                )

            # ── Form batch: tất cả nằm trong 1 form, 1 lần submit ──
            with st.form(key=f"review_form_p{page}"):
                qty_inputs   = {}   # id → số lượng duyệt
                action_radio = {}   # id → Duyệt/Từ chối

                # Header row
                hc = st.columns([3, 2, 2, 1])
                hc[0].markdown("**Sản phẩm**")
                hc[1].markdown("**Đề xuất / Tồn**")
                hc[2].markdown("**SL duyệt**")
                hc[3].markdown("**Quyết định**")
                st.divider()

                for _, row in df_page.iterrows():
                    rid = int(row["id"])
                    c1, c2, c3, c4 = st.columns([3, 2, 2, 1])
                    with c1:
                        to_store_txt = f" → `{row['to_store']}`" if row.get("to_store") else ""
                        st.markdown(f"**{row['fdcode']}**  \n`{row['from_store']}`{to_store_txt}")
                        if row.get("reason"):
                            st.caption(row["reason"])
                    with c2:
                        st.caption(
                            f"Đề xuất: **{int(row['proposed_qty'])}**  \n"
                            f"Tồn: {int(row['available_qty']) if row['available_qty'] else '—'}  |  "
                            f"Forecast: {int(row['forecast_qty']) if row['forecast_qty'] else '—'}"
                        )
                    with c3:
                        qty_inputs[rid] = st.number_input(
                            "sl", min_value=0,
                            value=int(row["proposed_qty"]),
                            key=f"qty_{rid}",
                            label_visibility="collapsed"
                        )
                    with c4:
                        action_radio[rid] = st.selectbox(
                            "act", ["✅", "❌"],
                            key=f"act_{rid}",
                            label_visibility="collapsed"
                        )

                st.divider()
                submitted = st.form_submit_button(
                    f"💾 Lưu trang {page}/{total_pages}",
                    use_container_width=True, type="primary"
                )

            if submitted:
                approved_ids  = [(qty_inputs[rid], rid)
                                 for rid, act in action_radio.items() if act == "✅"]
                rejected_ids  = [rid for rid, act in action_radio.items() if act == "❌"]
                with sqlite3.connect(FEEDBACK_DB_PATH) as conn:
                    if approved_ids:
                        conn.executemany(
                            """UPDATE proposals
                               SET status='approved', approved_qty=?, reviewed_by='user'
                               WHERE id=?""",
                            approved_ids
                        )
                    if rejected_ids:
                        conn.executemany(
                            "UPDATE proposals SET status='rejected' WHERE id=?",
                            [(rid,) for rid in rejected_ids]
                        )
                st.success(f"✅ Đã lưu: {len(approved_ids)} duyệt, {len(rejected_ids)} từ chối")
                # Về trang 1 nếu hết pending, giữ nguyên nếu còn
                if len(approved_ids) + len(rejected_ids) >= PAGE_SIZE:
                    st.session_state.fb_page = 1
                st.rerun()

            # ── Điều hướng trang ──
            if total_pages > 1:
                pg_cols = st.columns(3)
                with pg_cols[0]:
                    if page > 1 and st.button("← Trước"):
                        st.session_state.fb_page = page - 1
                        st.rerun()
                with pg_cols[1]:
                    st.caption(f"Trang {page} / {total_pages}")
                with pg_cols[2]:
                    if page < total_pages and st.button("Sau →"):
                        st.session_state.fb_page = page + 1
                        st.rerun()

    # ────────────────────────────────────────
    # TAB 2 — NHẬP KẾT QUẢ THỰC TẾ
    # ────────────────────────────────────────
    with tab_actual:
        st.subheader("Nhập số lượng thực tế đã thực hiện")
        st.caption("Nhập kết quả để AI học và cải thiện dự báo.")

        with sqlite3.connect(FEEDBACK_DB_PATH) as conn:
            df_approved = pd.read_sql(
                """SELECT id, fdcode, from_store, approved_qty
                   FROM proposals
                   WHERE status='approved' AND actual_qty IS NULL
                   ORDER BY created_at DESC
                   LIMIT 50""",
                conn
            )

        if df_approved.empty:
            st.info("Không có đề xuất đã duyệt nào cần nhập kết quả.")
        else:
            st.caption(f"Hiển thị tối đa 50 dòng cần nhập kết quả")

            with st.form("actual_results_form"):
                updates = {}
                # Header
                hc = st.columns([4, 1])
                hc[0].markdown("**Sản phẩm / Kho**")
                hc[1].markdown("**Thực tế**")
                st.divider()

                for _, row in df_approved.iterrows():
                    rid = int(row["id"])
                    ci, ca = st.columns([4, 1])
                    with ci:
                        st.caption(f"**{row['fdcode']}** từ `{row['from_store']}` — duyệt **{int(row['approved_qty'])}** sp")
                    with ca:
                        updates[rid] = st.number_input(
                            "qty", min_value=0,
                            value=int(row["approved_qty"]),
                            key=f"actual_{rid}",
                            label_visibility="collapsed"
                        )

                submitted = st.form_submit_button("💾 Lưu tất cả", use_container_width=True, type="primary")
                if submitted:
                    today = datetime.today().strftime("%Y-%m-%d")
                    rows_to_update = [(qty, today, rid) for rid, qty in updates.items()]
                    with sqlite3.connect(FEEDBACK_DB_PATH) as conn:
                        conn.executemany(
                            "UPDATE proposals SET actual_qty=?, actual_date=?, status='completed' WHERE id=?",
                            rows_to_update
                        )
                        _update_accuracy_summary(conn)
                    st.success(f"✅ Đã lưu {len(updates)} kết quả!")
                    st.rerun()

    # ────────────────────────────────────────
    # TAB 3 — BÁO CÁO ĐỘ CHÍNH XÁC
    # ────────────────────────────────────────
    with tab_report:
        st.subheader("Độ chính xác của AI theo thời gian")

        with sqlite3.connect(FEEDBACK_DB_PATH) as conn:
            df_completed = pd.read_sql(
                """SELECT fdcode, from_store AS store,
                          proposed_qty, approved_qty, actual_qty,
                          confidence, created_at, actual_date
                   FROM proposals
                   WHERE status='completed' AND actual_qty IS NOT NULL
                   ORDER BY actual_date DESC
                   LIMIT 500""",
                conn
            )

        if df_completed.empty:
            st.info("Chưa có dữ liệu. Nhập kết quả thực tế ở tab bên để xem báo cáo.")
        else:
            df_completed["error"] = abs(
                df_completed["actual_qty"] - df_completed["proposed_qty"]
            )
            df_completed["accuracy"] = (
                1 - df_completed["error"] / df_completed["proposed_qty"].clip(lower=1)
            ).clip(0, 1) * 100

            total     = len(df_completed)
            avg_acc   = df_completed["accuracy"].mean()
            avg_error = df_completed["error"].mean()
            high_acc  = (df_completed["accuracy"] >= 80).sum()

            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Đề xuất hoàn thành", total)
            m2.metric("Độ chính xác TB", f"{avg_acc:.1f}%")
            m3.metric("Sai lệch TB (sp)", f"{avg_error:.1f}")
            m4.metric("Chính xác ≥80%", f"{high_acc}/{total}")

            if st.button("🤖 Nhận xét AI"):
                summary_text = df_completed[
                    ["fdcode", "store", "proposed_qty", "actual_qty", "accuracy", "confidence"]
                ].head(30).to_csv(index=False)
                prompt = (
                    "Phân tích kết quả dự báo tồn kho sau, trả lời tiếng Việt gạch đầu dòng:\n"
                    "1. Mã/store dự báo tốt nhất?\n"
                    "2. Mã/store hay sai lệch nhất và tại sao?\n"
                    "3. Gợi ý cải thiện?\n\n"
                    f"Dữ liệu:\n{summary_text}\n\nTối đa 200 từ."
                )
                with st.spinner("Đang phân tích..."):
                    insight = _call_gemini(
                        "Bạn là chuyên gia phân tích dự báo tồn kho.",
                        [{"role": "user", "content": prompt}]
                    )
                st.markdown(insight)

            st.dataframe(
                df_completed[["fdcode","store","proposed_qty","approved_qty","actual_qty","accuracy","confidence"]]
                .rename(columns={
                    "fdcode":"Mã SP","store":"Store","proposed_qty":"Đề xuất",
                    "approved_qty":"Duyệt","actual_qty":"Thực tế",
                    "accuracy":"Chính xác (%)","confidence":"Tin cậy"
                }),
                use_container_width=True
            )

            csv = df_completed.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "📥 Tải CSV",
                data=csv,
                file_name=f"feedback_{datetime.today().strftime('%Y%m%d')}.csv",
                mime="text/csv",
            )

def _update_accuracy_summary(conn: sqlite3.Connection) -> None:
    """Cập nhật bảng feedback_summary sau mỗi lần nhập kết quả thực tế."""
    df = pd.read_sql(
        """SELECT fdcode, from_store AS store,
                  proposed_qty, actual_qty
           FROM proposals
           WHERE status = 'completed' AND actual_qty IS NOT NULL""",
        conn
    )
    if df.empty:
        return

    df["error"] = abs(df["actual_qty"] - df["proposed_qty"])
    df["accuracy"] = (
        1 - df["error"] / df["proposed_qty"].clip(lower=1)
    ).clip(0, 1)

    summary = df.groupby(["fdcode", "store"]).agg(
        accuracy_rate=("accuracy", "mean"),
        avg_error=("error", "mean"),
        n_samples=("accuracy", "count"),
    ).reset_index()

    now = datetime.now().isoformat()
    conn.execute("DELETE FROM feedback_summary")
    for _, row in summary.iterrows():
        conn.execute(
            """INSERT INTO feedback_summary
               (updated_at, fdcode, store, accuracy_rate, avg_error, n_samples)
               VALUES (?,?,?,?,?,?)""",
            (now, row["fdcode"], row["store"],
             row["accuracy_rate"], row["avg_error"], row["n_samples"])
        )
    conn.commit()

def get_accuracy_by_sku(fdcode: str = None, store: str = None) -> pd.DataFrame:
    """
    Lấy thông tin độ chính xác lịch sử cho một SKU hoặc store cụ thể.
    Dùng để điều chỉnh forecast: nếu accuracy thấp → tăng safety buffer.
    """
    conn = _init_feedback_db()
    query = "SELECT * FROM feedback_summary WHERE 1=1"
    params = []
    if fdcode:
        query += " AND fdcode = ?"
        params.append(fdcode)
    if store:
        query += " AND store = ?"
        params.append(store)
    df = pd.read_sql(query, conn, params=params)
    conn.close()
    return df