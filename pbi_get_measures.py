#!/usr/bin/env python3
"""
Đọc measures từ PBI Desktop qua port của DAX Studio.
Dùng DLL AdomdClient từ thư mục cài đặt DAX Studio — không cần cài thêm gì.
"""
import csv, glob, os, sys
from pathlib import Path

PORT = 50078   # ← port từ DAX Studio status bar

# ── Tìm AdomdClient.dll từ DAX Studio hoặc SSMS ──
DLL_SEARCH = [
    r"C:\Program Files\DAX Studio",
    r"C:\Program Files (x86)\DAX Studio",
    os.path.expandvars(r"%LOCALAPPDATA%\Programs\DAX Studio"),
    os.path.expandvars(r"%LOCALAPPDATA%\DAX Studio"),
    r"C:\Program Files\Microsoft SQL Server\*\Tools\Binn\ManagementStudio",
    r"C:\Program Files (x86)\Microsoft SQL Server\*\Tools\Binn\ManagementStudio",
    r"C:\Program Files\Microsoft SQL Server\*\SDK\Assemblies",
]

def find_adomd_dll() -> str | None:
    for pattern in DLL_SEARCH:
        hits = glob.glob(
            str(Path(pattern) / "**" / "Microsoft.AnalysisServices.AdomdClient.dll"),
            recursive=True,
        )
        if hits:
            return hits[0]
    return None

dll_path = find_adomd_dll()
if dll_path:
    print(f"✅ Tìm thấy AdomdClient: {dll_path}")
    # Thêm thư mục chứa DLL vào PATH để pythonnet tìm thấy
    dll_dir = str(Path(dll_path).parent)
    os.environ["PATH"] = dll_dir + os.pathsep + os.environ.get("PATH", "")
    sys.path.insert(0, dll_dir)
else:
    print("❌ Không tìm thấy AdomdClient.dll tự động.")
    print("\nVui lòng nhập đường dẫn thư mục cài DAX Studio:")
    print("  (VD: C:\\Program Files\\DAX Studio)")
    manual = input("Path: ").strip().strip('"')
    if manual and Path(manual).exists():
        os.environ["PATH"] = manual + os.pathsep + os.environ.get("PATH", "")
        sys.path.insert(0, manual)
    else:
        print("Không tìm thấy. Thoát.")
        sys.exit(1)

# ── Kết nối qua pyadomd ──
try:
    from pyadomd import Pyadomd
except ImportError:
    sys.exit("pip install pyadomd")

conn_str = f"Provider=MSOLAP;Data Source=localhost:{PORT};Integrated Security=SSPI;"

print(f"Kết nối localhost:{PORT} ...", end=" ", flush=True)
try:
    rows = []
    with Pyadomd(conn_str) as conn:
        with conn.cursor().execute(
            "SELECT [MEASUREGROUP_NAME],[MEASURE_NAME],[EXPRESSION]"
            " FROM $SYSTEM.MDSCHEMA_MEASURES"
        ) as cur:
            all_rows = cur.fetchall()

        # filter trong Python
        rows = sorted(
            [r for r in all_rows if r[2] and str(r[2]).strip()],
            key=lambda r: (r[0] or "", r[1] or ""),
        )
    print(f"✅  {len(rows)} measures")
except Exception as e:
    print(f"\n❌ {e}")
    sys.exit(1)

# ── Ghi CSV ──
out = Path(__file__).parent / "pbi_output" / "measures.csv"
out.parent.mkdir(exist_ok=True)
with open(out, "w", newline="", encoding="utf-8-sig") as f:
    w = csv.writer(f)
    w.writerow(["MEASUREGROUP_NAME", "MEASURE_NAME", "EXPRESSION"])
    w.writerows(rows)

print(f"✅ Lưu → {out}")
print(f"\nBước tiếp: python pbi_advisor.py  →  chọn [1] → nhập path file trên")
