#!/usr/bin/env python3
"""
pbi/advisor.py — Đọc model từ Power BI Desktop đang mở, dùng 3 Claude agent
phân tích measures + viết DAX mới + gợi ý cải thiện report.

Yêu cầu:
  pip install pyadomd anthropic python-dotenv

Cách dùng:
  1. Mở file .pbix trong Power BI Desktop
  2. python pbi/advisor.py
  3. Xem kết quả trong thư mục pbi/output/
"""

import csv, glob, json, os, sys, textwrap
from dotenv import load_dotenv
load_dotenv()
from datetime import datetime
from pathlib import Path

# để import core.* từ thư mục gốc
sys.path.insert(0, str(Path(__file__).parent.parent))

# ══════════════════════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════════════════════

BUSINESS_CONTEXT = """
Công ty bán lẻ thời trang (giày, túi). Có 2 nhóm báo cáo chính:
- SALE: doanh thu, số lượng, kênh bán (KDC store, ECOM), category/subcategory
- MARKETING: ngân sách, spend, reach, impression, engagement, follower, CPA, CPM
Kỳ báo cáo thường là tuần / tháng. So sánh với cùng kỳ năm trước (LY).
"""

# File .pbix muốn phân tích (để trống = dùng file đang mở trong PBI Desktop)
PBIX_FILES = {
    "1": r"G:\My Drive\MA\POWER BI\Products Performance - MKT.pbix",
    "2": r"G:\My Drive\MA\POWER BI\Sales Department Performance.pbix",
    "3": r"G:\My Drive\MA\POWER BI\Company Performance - 2025.pbix",
    "4": r"G:\My Drive\MA\POWER BI\Company Performance.pbix",
    "5": r"G:\My Drive\MA\POWER BI\Ecommerce Performance.pbix",
}

OUTPUT_DIR = Path(__file__).parent / "output"
MODEL_NAME  = "claude-sonnet-4-6"


# ══════════════════════════════════════════════════════════════════
#  STEP 1: TÌM CỔNG LOCAL AS CỦA PBI DESKTOP
# ══════════════════════════════════════════════════════════════════

def find_pbi_port() -> int | None:
    local_app = os.environ.get("LOCALAPPDATA", "")
    temp      = os.environ.get("TEMP", "")
    patterns  = [
        f"{local_app}/Microsoft/Power BI Desktop/AnalysisServicesWorkspaces/*/Data/msmdsrv.port.txt",
        f"{local_app}/Microsoft/Power BI Desktop Store App/AnalysisServicesWorkspaces/*/Data/msmdsrv.port.txt",
        f"{local_app}/Packages/Microsoft.MicrosoftPowerBIDesktop_8wekyb3d8bbwe/LocalState/AnalysisServicesWorkspaces/*/Data/msmdsrv.port.txt",
        f"{temp}/Microsoft/Power BI Desktop/AnalysisServicesWorkspaces/*/Data/msmdsrv.port.txt",
        f"{local_app}/**/msmdsrv.port.txt",
    ]
    for pat in patterns:
        files = sorted(glob.glob(pat, recursive=True), key=os.path.getmtime, reverse=True)
        if files:
            try:
                return int(Path(files[0]).read_text().strip())
            except Exception:
                continue

    try:
        import psutil
        for proc in psutil.process_iter(["name", "cmdline"]):
            name = (proc.info.get("name") or "").lower()
            if "msmdsrv" in name:
                cmdline = proc.info.get("cmdline") or []
                for i, arg in enumerate(cmdline):
                    if str(arg).lower() == "-port" and i + 1 < len(cmdline):
                        return int(cmdline[i + 1])
    except Exception:
        pass

    return None


# ══════════════════════════════════════════════════════════════════
#  STEP 1b: ĐỌC MODEL TỪ DAX STUDIO CSV / JSON EXPORT
# ══════════════════════════════════════════════════════════════════

def read_model_from_csv(csv_path: str) -> dict:
    """
    Đọc model từ file CSV export của DAX Studio DMV tab.
    DAX Studio → DMV tab → MDSCHEMA_MEASURES → double-click → Export CSV
    """
    measures, table_names = [], set()
    with open(csv_path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            keys = {k.upper(): v for k, v in row.items()}
            tbl  = keys.get("MEASUREGROUP_NAME") or keys.get("TABLE_NAME") or ""
            name = keys.get("MEASURE_NAME") or keys.get("MEASURE_CAPTION") or ""
            dax  = keys.get("EXPRESSION") or keys.get("MEASURE_EXPRESSION") or ""
            vis  = keys.get("MEASURE_IS_VISIBLE", "true")
            if name and str(vis).lower() not in ("false", "0") and not name.startswith("__"):
                measures.append({"table": tbl, "name": name,
                                 "dax": dax.strip(), "description": ""})
                table_names.add(tbl)
    return {"measures": measures, "tables": sorted(table_names)}


def read_model_from_json(json_path: str) -> dict:
    """
    Đọc model từ file JSON export của DAX Studio.
    Advanced → Export Model Metadata → lưu file .json
    """
    raw = json.loads(Path(json_path).read_text(encoding="utf-8"))

    tables_raw = (
        raw.get("model", raw).get("tables", [])
        or raw.get("tables", [])
    )

    measures, table_names = [], []
    for tbl in tables_raw:
        tname = tbl.get("name", "")
        if tname.startswith("DateTableTemplate") or tname.startswith("LocalDateTable"):
            continue
        table_names.append(tname)
        for m in tbl.get("measures", []):
            measures.append({
                "table":       tname,
                "name":        m.get("name", ""),
                "dax":         m.get("expression", "").strip(),
                "description": m.get("description", ""),
            })

    return {"measures": measures, "tables": table_names}


# ══════════════════════════════════════════════════════════════════
#  STEP 2: ĐỌC MODEL TỪ PBI DESKTOP
# ══════════════════════════════════════════════════════════════════

def read_pbi_model(port: int) -> dict:
    try:
        from pyadomd import Pyadomd
    except ImportError:
        sys.exit("❌  Thiếu pyadomd. Chạy: pip install pyadomd")

    conn_str = f"Provider=MSOLAP;Data Source=localhost:{port};Integrated Security=SSPI;"
    model = {"measures": [], "tables": [], "relationships": []}

    with Pyadomd(conn_str) as conn:
        with conn.cursor().execute(
            "SELECT [MEASUREGROUP_NAME],[MEASURE_NAME],[EXPRESSION],[DESCRIPTION]"
            " FROM $SYSTEM.MDSCHEMA_MEASURES"
        ) as cur:
            all_rows = cur.fetchall()
        for row in all_rows:
            if row[2] and str(row[2]).strip():
                model["measures"].append({
                    "table": row[0], "name": row[1],
                    "dax": row[2], "description": row[3] or "",
                })

        with conn.cursor().execute(
            "SELECT DISTINCT [DIMENSION_NAME] FROM $SYSTEM.MDSCHEMA_DIMENSIONS"
        ) as cur:
            for row in cur.fetchall():
                model["tables"].append(row[0])

    return model


# ══════════════════════════════════════════════════════════════════
#  STEP 3: ĐỌC DB SCHEMA (OPTIONAL)
# ══════════════════════════════════════════════════════════════════

def read_db_schema() -> str:
    try:
        from core.db import get_engine
        from sqlalchemy import inspect
        engine = get_engine()
        insp   = inspect(engine)
        lines  = []
        for tbl in ["sale_order", "sale_order_items", "products",
                    "categories", "stores", "sale_channel"]:
            try:
                cols = [c["name"] for c in insp.get_columns(tbl)]
                lines.append(f"  {tbl}: {', '.join(cols)}")
            except Exception:
                pass
        return "\n".join(lines) if lines else "(không đọc được schema)"
    except Exception as e:
        return f"(DB không khả dụng: {e})"


# ══════════════════════════════════════════════════════════════════
#  STEP 4: 3 CLAUDE AGENTS
# ══════════════════════════════════════════════════════════════════

def call_claude(system: str, user: str) -> str:
    try:
        import anthropic
    except ImportError:
        sys.exit("❌  Thiếu anthropic. Chạy: pip install anthropic")

    client = anthropic.Anthropic()
    msg = client.messages.create(
        model=MODEL_NAME,
        max_tokens=4096,
        system=system,
        messages=[{"role": "user", "content": user}],
    )
    return msg.content[0].text


def agent_1_analyze(model: dict) -> str:
    measures_txt = "\n".join(
        f"  [{m['table']}] {m['name']} = {m['dax'][:120]}{'...' if len(m['dax'])>120 else ''}"
        for m in model["measures"]
    )
    tables_txt = ", ".join(model["tables"])

    return call_claude(
        system=textwrap.dedent("""
            Bạn là chuyên gia Power BI và DAX. Nhiệm vụ: phân tích danh sách measures
            hiện có, xác định naming pattern, các metrics đã có, và các gaps còn thiếu.
            Trả lời bằng tiếng Việt. Ngắn gọn, súc tích, dùng bullet points.
        """).strip(),
        user=textwrap.dedent(f"""
            ## Business context
            {BUSINESS_CONTEXT}

            ## Tables trong model
            {tables_txt}

            ## Measures hiện có ({len(model['measures'])} measures)
            {measures_txt}

            Hãy phân tích:
            1. Naming convention đang dùng là gì?
            2. Các nhóm metrics đã có (LY, YoY%, RR...)?
            3. Những metrics quan trọng còn thiếu theo từng nhóm?
            4. Có measures nào cần tối ưu không (logic sai, thiếu CALCULATE, v.v.)?
        """).strip(),
    )


def agent_2_write_dax(model: dict, analysis: str, db_schema: str) -> str:
    existing_names = {m["name"] for m in model["measures"]}
    sample_measures = "\n".join(
        f"  {m['name']} = {m['dax']}"
        for m in model["measures"][:20]
    )

    return call_claude(
        system=textwrap.dedent("""
            Bạn là chuyên gia DAX và Power BI. Nhiệm vụ: viết DAX code hoàn chỉnh
            cho các measures mới được đề xuất, theo đúng style và convention của model hiện tại.
            Mỗi measure phải có: tên, DAX code đầy đủ, giải thích ngắn.
            Format output: markdown với code blocks DAX.
            Trả lời bằng tiếng Việt.
        """).strip(),
        user=textwrap.dedent(f"""
            ## Business context
            {BUSINESS_CONTEXT}

            ## DB Schema (nguồn data)
            {db_schema}

            ## Style mẫu (20 measures đầu)
            {sample_measures}

            ## Phân tích gaps từ Agent 1
            {analysis}

            Yêu cầu:
            1. Viết DAX đầy đủ cho TẤT CẢ measures còn thiếu đã xác định ở trên.
            2. Giữ đúng naming convention ({list(existing_names)[:3]}...).
            3. Với mỗi measure viết thêm 1 dòng comment giải thích.
            4. Nếu có measures cũ cần sửa, viết lại phiên bản tối ưu.
            5. Nhóm theo: [Measures mới] và [Measures cần tối ưu].
        """).strip(),
    )


def agent_3_report_suggestions(model: dict, analysis: str, dax_output: str) -> str:
    return call_claude(
        system=textwrap.dedent("""
            Bạn là chuyên gia Power BI report design. Nhiệm vụ: dựa trên model và measures
            hiện có, gợi ý cách cải thiện report (visuals, hierarchy, drillthrough, tooltips,
            KPI cards, trang báo cáo còn thiếu...).
            Thực tế, ngắn gọn. Trả lời bằng tiếng Việt.
        """).strip(),
        user=textwrap.dedent(f"""
            ## Business context
            {BUSINESS_CONTEXT}

            ## Tóm tắt model
            - Số measures: {len(model['measures'])}
            - Tables: {', '.join(model['tables'])}

            ## Phân tích gaps
            {analysis}

            ## Measures mới vừa được viết
            {dax_output[:2000]}...

            Hãy gợi ý:
            1. Các trang báo cáo nên có (với sale + mkt)
            2. Visuals phù hợp cho từng nhóm metrics
            3. Drillthrough / tooltip nên setup ở đâu
            4. KPI cards quan trọng nhất cần hiển thị
            5. Cách tổ chức measures table cho dễ quản lý
        """).strip(),
    )


# ══════════════════════════════════════════════════════════════════
#  STEP 5: XUẤT KẾT QUẢ
# ══════════════════════════════════════════════════════════════════

def write_output(model: dict, analysis: str, dax: str, suggestions: str) -> Path:
    OUTPUT_DIR.mkdir(exist_ok=True)
    ts   = datetime.now().strftime("%Y%m%d_%H%M")
    path = OUTPUT_DIR / f"pbi_advisor_{ts}.md"

    content = f"""# Power BI Advisor — {datetime.now().strftime('%d/%m/%Y %H:%M')}

## Model hiện tại
- **Số measures:** {len(model['measures'])}
- **Tables:** {', '.join(model['tables'])}
- **Measure tables:** {', '.join(sorted({m['table'] for m in model['measures']}))}

---

## 📊 Phân tích measures hiện có

{analysis}

---

## 💡 DAX Measures mới & tối ưu

{dax}

---

## 🎨 Gợi ý cải thiện Report

{suggestions}

---

## 📋 Danh sách measures hiện có (đầy đủ)

| Table | Measure | DAX (tóm tắt) |
|-------|---------|----------------|
""" + "\n".join(
        f"| {m['table']} | `{m['name']}` | `{m['dax'][:80].replace(chr(10),' ')}...` |"
        for m in sorted(model["measures"], key=lambda x: (x["table"], x["name"]))
    )

    path.write_text(content, encoding="utf-8")
    return path


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    print("\n══ PBI Advisor ══\n")

    print("Nguồn model:")
    print("  [1] DAX Studio CSV  (DMV tab → MDSCHEMA_MEASURES → Export CSV)  ← khuyên dùng")
    print("  [2] DAX Studio JSON  (Advanced → Export Model Metadata)")
    print("  [3] Power BI Desktop trực tiếp  (Import mode, cần AdomdClient DLL)")
    src = input("\nChọn (1/2/3): ").strip()

    model = None

    if src == "1":
        csv_path = input("Đường dẫn file CSV (measures.csv): ").strip().strip('"')
        if not Path(csv_path).exists():
            print(f"❌ Không tìm thấy file: {csv_path}")
            return
        print("📖 Đọc model từ CSV ...", end=" ", flush=True)
        model = read_model_from_csv(csv_path)
        print(f"✅ ({len(model['measures'])} measures, {len(model['tables'])} tables)")

    elif src == "2":
        json_path = input("Đường dẫn file JSON từ DAX Studio: ").strip().strip('"')
        if not Path(json_path).exists():
            print(f"❌ Không tìm thấy file: {json_path}")
            return
        print("📖 Đọc model từ JSON ...", end=" ", flush=True)
        model = read_model_from_json(json_path)
        print(f"✅ ({len(model['measures'])} measures, {len(model['tables'])} tables)")

    else:
        print("\nChọn file .pbix muốn phân tích:")
        for k, v in PBIX_FILES.items():
            print(f"  [{k}] {Path(v).name}")
        choice = input("\nNhập số (Enter = bỏ qua): ").strip()
        chosen_file = PBIX_FILES.get(choice)
        if chosen_file:
            print(f"\n→ Hãy mở file này trong Power BI Desktop nếu chưa mở:")
            print(f"  {chosen_file}\n")
            input("Nhấn Enter khi đã mở xong...")

        print("\n🔍 Tìm Power BI Desktop đang mở ...", end=" ", flush=True)
        port = find_pbi_port()
        if not port:
            print("❌")
            print("\n→ Mở DAX Studio → xem port ở status bar dưới cùng (VD: localhost:50078)")
            port_input = input("Nhập port thủ công: ").strip()
            if not port_input.isdigit():
                print("Port không hợp lệ.")
                return
            port = int(port_input)
        print(f"✅ (port {port})")

        print("📖 Đọc model từ PBI Desktop ...", end=" ", flush=True)
        model = read_pbi_model(port)
        print(f"✅ ({len(model['measures'])} measures, {len(model['tables'])} tables)")

    if not model or not model["measures"]:
        print("⚠  Không đọc được measures.")
        return

    print("🗄  Đọc DB schema ...", end=" ", flush=True)
    db_schema = read_db_schema()
    print("✅")

    print("\n🤖 Agent 1/3 — Phân tích measures ...", end=" ", flush=True)
    analysis = agent_1_analyze(model)
    print("✅")

    print("🤖 Agent 2/3 — Viết DAX measures mới ...", end=" ", flush=True)
    dax = agent_2_write_dax(model, analysis, db_schema)
    print("✅")

    print("🤖 Agent 3/3 — Gợi ý cải thiện report ...", end=" ", flush=True)
    suggestions = agent_3_report_suggestions(model, analysis, dax)
    print("✅")

    out = write_output(model, analysis, dax, suggestions)
    print(f"\n✅ Kết quả: {out}")
    print("   Mở file markdown trên để review DAX trước khi apply vào PBI Desktop.\n")


if __name__ == "__main__":
    main()
