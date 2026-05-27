#!/usr/bin/env python3
"""
pbi/apply_dax.py — Áp dụng DAX fixes vào Power BI Desktop đang mở.

Yêu cầu:
  - Tabular Editor 2 (miễn phí): https://github.com/TabularEditor/TabularEditor/releases
  - Power BI Desktop đang mở file .pbix (Import mode, không phải Live Connection)
  - DAX Studio đang kết nối → xem port ở status bar

Cách dùng:
  1. Mở file .pbix trong Power BI Desktop
  2. python pbi/apply_dax.py
  3. Chọn nhóm DAX muốn apply
  4. Ctrl+S trong PBI Desktop để lưu lại
"""

import glob, os, subprocess, sys, tempfile
from pathlib import Path


def get_pbi_database(port: str) -> str | None:
    """Query tên database từ PBI Desktop đang mở qua pyadomd."""
    # Tìm AdomdClient DLL
    dll_search = [
        r"C:\Program Files\DAX Studio",
        r"C:\Program Files (x86)\DAX Studio",
        os.path.expandvars(r"%LOCALAPPDATA%\Programs\DAX Studio"),
    ]
    for pattern in dll_search:
        hits = glob.glob(
            str(Path(pattern) / "**" / "Microsoft.AnalysisServices.AdomdClient.dll"),
            recursive=True,
        )
        if hits:
            dll_dir = str(Path(hits[0]).parent)
            os.environ["PATH"] = dll_dir + os.pathsep + os.environ.get("PATH", "")
            sys.path.insert(0, dll_dir)
            break

    try:
        from pyadomd import Pyadomd
        conn_str = f"Provider=MSOLAP;Data Source=localhost:{port};Integrated Security=SSPI;"
        with Pyadomd(conn_str) as conn:
            with conn.cursor().execute(
                "SELECT [CATALOG_NAME] FROM $SYSTEM.DBSCHEMA_CATALOGS"
            ) as cur:
                rows = cur.fetchall()
        # Lọc bỏ catalog hệ thống, lấy cái đầu tiên
        for r in rows:
            name = str(r[0])
            if name and not name.startswith("$"):
                return name
        return rows[0][0] if rows else None
    except Exception as e:
        print(f"⚠  Không lấy được DB name tự động: {e}")
        return None

# ── Tìm Tabular Editor ──
TE_SEARCH = [
    r"C:\Program Files (x86)\Tabular Editor\TabularEditor.exe",
    r"C:\Program Files\Tabular Editor\TabularEditor.exe",
    os.path.expandvars(r"%LOCALAPPDATA%\Programs\Tabular Editor\TabularEditor.exe"),
    os.path.expandvars(r"%APPDATA%\Tabular Editor\TabularEditor.exe"),
    # Tabular Editor 3 (trả phí)
    r"C:\Program Files\Tabular Editor 3\TabularEditor3.exe",
]

def find_tabular_editor() -> str | None:
    for p in TE_SEARCH:
        if Path(p).exists():
            return p
    # tìm trong PATH
    for name in ("TabularEditor.exe", "TabularEditor3.exe"):
        result = subprocess.run(["where", name], capture_output=True, text=True)
        if result.returncode == 0:
            return result.stdout.strip().splitlines()[0]
    return None


# ══════════════════════════════════════════════════════════════════
#  DAX FIXES — chỉnh sửa ở đây để thêm/bớt fixes
# ══════════════════════════════════════════════════════════════════

# Mỗi entry: {"table": str, "name": str, "dax": str, "action": "fix"|"new"}
DAX_FIXES = {

    "bug_fixes": {
        "label": "🔴 Sửa lỗi (2 measures quan trọng)",
        "measures": [
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_CPA_Paid_YoY%",
                "action": "fix",
                "dax":    """DIVIDE(
    [mkt_CPA_Paid] - [mkt_CPA_Paid_LY],
    [mkt_CPA_Paid_LY]
)""",
            },
            {
                "table":  "sale_final",
                "name":   "Qty Growth % 90D",
                "action": "fix",
                "dax":    """DIVIDE(
    [Qty_Ndays] - [Qty_Ndays_LY],
    [Qty_Ndays_LY]
)""",
            },
        ],
    },

    "time_intel": {
        "label": "🟡 Chuẩn hóa time intelligence (_LY dùng SAMEPERIODLASTYEAR)",
        "measures": [
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Spend_Paid_LY",
                "action": "fix",
                "dax":    "CALCULATE([mkt_Spend_Paid], SAMEPERIODLASTYEAR('Calendar'[Date]))",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Engagement_LY",
                "action": "fix",
                "dax":    "CALCULATE([mkt_Engagement], SAMEPERIODLASTYEAR('Calendar'[Date]))",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Engagement_Rate_LY",
                "action": "fix",
                "dax":    "CALCULATE([mkt_Engagement_Rate], SAMEPERIODLASTYEAR('Calendar'[Date]))",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Follower_LY",
                "action": "fix",
                "dax":    "CALCULATE([mkt_Follower], SAMEPERIODLASTYEAR('Calendar'[Date]))",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Reach_LY",
                "action": "fix",
                "dax":    "CALCULATE([mkt_Reach], SAMEPERIODLASTYEAR('Calendar'[Date]))",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Views_LY",
                "action": "fix",
                "dax":    "CALCULATE([mkt_Views], SAMEPERIODLASTYEAR('Calendar'[Date]))",
            },
        ],
    },

    "new_yoy": {
        "label": "🔴 Thêm mới: YoY% còn thiếu",
        "measures": [
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Spend_Paid_YoY%",
                "action": "new",
                "dax":    "DIVIDE([mkt_Spend_Paid] - [mkt_Spend_Paid_LY], [mkt_Spend_Paid_LY])",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Impressions_Paid_YoY%",
                "action": "new",
                "dax":    "DIVIDE([mkt_Impressions_Paid] - [mkt_Impressions_Paid_LY], [mkt_Impressions_Paid_LY])",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_CPM_Paid_YoY%",
                "action": "new",
                "dax":    "DIVIDE([mkt_CPM_Paid] - [mkt_CPM_Paid_LY], [mkt_CPM_Paid_LY])",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_Post_Count_YoY%",
                "action": "new",
                "dax":    "DIVIDE([mkt_Post_Count] - [mkt_Post_Count_LY], [mkt_Post_Count_LY])",
            },
        ],
    },

    "roas": {
        "label": "🔴 Thêm mới: ROAS (cần Revenue_ECOM trong sale_final)",
        "measures": [
            {
                "table":  "sale_final",
                "name":   "Revenue_ECOM",
                "action": "new",
                "dax":    'CALCULATE([Revenue], sale_final[channel] = "ECOM")',
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_ROAS",
                "action": "new",
                "dax":    "DIVIDE([Revenue_ECOM], [mkt_Spend_Paid])",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_ROAS_LY",
                "action": "new",
                "dax":    "CALCULATE([mkt_ROAS], SAMEPERIODLASTYEAR('Calendar'[Date]))",
            },
            {
                "table":  "_MKT_Measures",
                "name":   "mkt_ROAS_YoY%",
                "action": "new",
                "dax":    "DIVIDE([mkt_ROAS] - [mkt_ROAS_LY], [mkt_ROAS_LY])",
            },
        ],
    },

}


# ══════════════════════════════════════════════════════════════════
#  SINH C# SCRIPT CHO TABULAR EDITOR
# ══════════════════════════════════════════════════════════════════

def _escape_cs(s: str) -> str:
    """Escape string cho C# verbatim string literal (@"...")."""
    return s.replace('"', '""')


def generate_csharp(groups: list[dict]) -> str:
    lines = [
        "// Tabular Editor C# script — auto-generated by pbi/apply_dax.py",
        "",
    ]
    for grp in groups:
        lines.append(f"// {grp['label']}")
        for m in grp["measures"]:
            tbl  = _escape_cs(m["table"])
            name = _escape_cs(m["name"])
            dax  = _escape_cs(m["dax"])
            if m["action"] == "fix":
                lines += [
                    f'var _m_{name.replace(" ","_").replace("%","Pct")} = Model.Tables["{tbl}"].Measures["{name}"];',
                    f'if (_m_{name.replace(" ","_").replace("%","Pct")} != null) {{',
                    f'    _m_{name.replace(" ","_").replace("%","Pct")}.Expression = @"{dax}";',
                    f'    Info("Fixed: {name}");',
                    f'}} else {{ Warning("Not found: {name}"); }}',
                    "",
                ]
            else:  # new
                lines += [
                    f'if (!Model.Tables["{tbl}"].Measures.Contains("{name}")) {{',
                    f'    var _new_{name.replace(" ","_").replace("%","Pct")} = Model.Tables["{tbl}"].AddMeasure("{name}");',
                    f'    _new_{name.replace(" ","_").replace("%","Pct")}.Expression = @"{dax}";',
                    f'    Info("Added: {name}");',
                    f'}} else {{ Info("Already exists: {name}"); }}',
                    "",
                ]
    lines += ['Info("Done.");']
    return "\n".join(lines)


# ══════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════

def main():
    print("\n══ PBI Apply DAX ══\n")

    # 1. Tìm Tabular Editor
    te = find_tabular_editor()
    if not te:
        print("❌ Không tìm thấy Tabular Editor.")
        print("\n→ Tải miễn phí tại: https://github.com/TabularEditor/TabularEditor/releases")
        print("  Cài xong chạy lại script này.\n")
        return
    print(f"✅ Tabular Editor: {te}")

    # 2. Nhập port PBI Desktop
    print("\n→ Mở DAX Studio → xem port ở status bar (VD: localhost:50078)")
    port_input = input("Nhập port PBI Desktop đang mở: ").strip()
    if not port_input.isdigit():
        print("Port không hợp lệ.")
        return
    port = port_input

    # Lấy tên database từ PBI Desktop
    print(f"🔍 Lấy tên database từ localhost:{port} ...", end=" ", flush=True)
    db_name = get_pbi_database(port)
    if db_name:
        print(f"✅ '{db_name}'")
    else:
        print("❌")
        db_name = input("Nhập tên database thủ công (xem trong DAX Studio → server info): ").strip()
        if not db_name:
            print("Cần tên database. Thoát.")
            return
    print(f"✅ Sẽ kết nối: localhost:{port} / {db_name}")

    # 3. Chọn nhóm DAX
    print("\nChọn nhóm DAX muốn apply:")
    keys = list(DAX_FIXES.keys())
    for i, k in enumerate(keys, 1):
        grp = DAX_FIXES[k]
        n_measures = len(grp["measures"])
        print(f"  [{i}] {grp['label']}  ({n_measures} measures)")
    print(f"  [A] Tất cả ({sum(len(g['measures']) for g in DAX_FIXES.values())} measures)")
    print(f"  [Q] Thoát")

    choice = input("\nChọn: ").strip().upper()

    if choice == "Q":
        return
    elif choice == "A":
        selected = list(DAX_FIXES.values())
    elif choice.isdigit() and 1 <= int(choice) <= len(keys):
        selected = [DAX_FIXES[keys[int(choice) - 1]]]
    else:
        print("Lựa chọn không hợp lệ.")
        return

    total = sum(len(g["measures"]) for g in selected)
    print(f"\n→ Sẽ apply {total} measures. Tiếp tục? (y/n): ", end="")
    if input().strip().lower() != "y":
        print("Hủy.")
        return

    # 4. Sinh C# script
    cs_code = generate_csharp(selected)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".cs",
                                     delete=False, encoding="utf-8") as f:
        f.write(cs_code)
        script_path = f.name

    print(f"\n📝 Script tạm: {script_path}")
    print("🚀 Chạy Tabular Editor ...\n")

    # 5. Chạy Tabular Editor — syntax: TabularEditor.exe "server" "database" -S script.cs
    cmd = [te, f"localhost:{port}", db_name, "-S", script_path]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.stdout:
            print(result.stdout)
        if result.stderr:
            print(result.stderr)
        if result.returncode == 0:
            print("\n✅ Xong! Quay lại PBI Desktop → Ctrl+S để lưu.")
        else:
            print(f"\n⚠  Tabular Editor trả về code {result.returncode}")
            print("   Kiểm tra output trên — có thể một số measures không tìm thấy.")
    except subprocess.TimeoutExpired:
        print("❌ Timeout sau 60 giây.")
    except Exception as e:
        print(f"❌ {e}")
    finally:
        Path(script_path).unlink(missing_ok=True)


if __name__ == "__main__":
    main()
