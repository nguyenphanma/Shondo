import os
import re
from datetime import date, datetime
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine
from decimal import Decimal, InvalidOperation

import pandas as pd
from sqlalchemy import text

SCRIPT_DIR = Path(__file__).parent.resolve()
DOWNLOAD_DIR = SCRIPT_DIR

TABLE_NAME = "warranty_history"

COL_MAP = {
    "ID": "id",
    "Ngày tạo": "created_at",
    "Kho hàng": "warehouse",
    "Khách hàng": "customer_name",
    "Số điện thoại": "customer_phone",
    "Địa chỉ": "customer_address",
    "Ngày mua": "purchase_date",
    "Sản phẩm": "product_name",
    "Mã sản phẩm": "product_code",
    "Trạng thái sản phẩm": "product_status",
    "Trạng thái phiếu bảo hành": "warranty_ticket_status",
    "IMEI": "imei",
    "Loại": "warranty_type",
    "Trung tâm bảo hành": "service_center",
    "Ngày hẹn lấy TTBH": "pickup_date_service_center",
    "Chi phí sửa chữa": "repair_cost",
    "Phí sửa chữa báo khách": "repair_fee_customer",
    "Lý do": "reason",
    "Trạng thái": "status",
    "Ngày hẹn trả": "return_due_date",
    "Ngày trả khách": "return_date",
    "Hình thức trả khách": "return_method",
    "Trả cho khách": "return_to_customer",
    "Người tiếp nhận": "received_by",
    "Người sửa": "repaired_by",
    "Linh kiện": "spare_part",
    "SL linh kiện": "spare_part_qty",
    "Giá linh kiện": "spare_part_price",
    "Ghi chú": "note",
    "Ghi chú CSKH": "customer_service_note",
}

DATE_COLS = [
    "created_at",
    "purchase_date",
    "pickup_date_service_center",
    "return_due_date",
    "return_date",
]
MONEY_COLS = ["repair_cost", "repair_fee_customer", "spare_part_price"]
INT_COLS = ["spare_part_qty"]

# ==================== DATABASE ====================
def create_table_if_not_exists(engine):
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id BIGINT,
        created_at DATETIME,
        warehouse VARCHAR(255),
        customer_name VARCHAR(255),
        customer_phone VARCHAR(50),
        customer_address TEXT,
        purchase_date DATETIME,
        product_name VARCHAR(500),
        product_code VARCHAR(100),
        product_status VARCHAR(100),
        warranty_ticket_status VARCHAR(100),
        imei VARCHAR(100),
        warranty_type VARCHAR(100),
        service_center VARCHAR(255),
        pickup_date_service_center DATETIME,
        repair_cost DECIMAL(15,2),
        repair_fee_customer DECIMAL(15,2),
        reason TEXT,
        status VARCHAR(100),
        return_due_date DATETIME,
        return_date DATETIME,
        return_method VARCHAR(100),
        return_to_customer VARCHAR(255),
        received_by VARCHAR(255),
        repaired_by VARCHAR(255),
        spare_part VARCHAR(500),
        spare_part_qty INT,
        spare_part_price DECIMAL(15,2),
        note TEXT,
        customer_service_note TEXT,
        run_from DATETIME,
        run_to DATETIME,
        loaded_at DATETIME,
        PRIMARY KEY (id, created_at),
        INDEX idx_created_at (created_at),
        INDEX idx_customer_phone (customer_phone),
        INDEX idx_imei (imei),
        INDEX idx_loaded_at (loaded_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with engine.begin() as conn:
        conn.execute(text(create_table_sql))
    print(f"✅ Table '{TABLE_NAME}' ready")

def db_delete_then_insert(engine, df: pd.DataFrame):
    """
    XÓA theo đúng các key (id, created_at) có trong df, rồi INSERT lại df.
    Không xóa theo khoảng thời gian.

    - Dùng batch delete: WHERE (id=:id0 AND created_at=:dt0) OR ...
    - params là dict => không còn lỗi "List argument must consist only of tuples..."
    """
    if df is None or df.empty:
        print("   ⚠️  Empty df, skip DB write")
        return

    if "id" not in df.columns or "created_at" not in df.columns:
        raise ValueError("df must contain 'id' and 'created_at' to delete/insert by key")

    # chuẩn hóa key
    df = df.dropna(subset=["id", "created_at"]).copy()
    df["id"] = pd.to_numeric(df["id"], errors="coerce")
    df = df.dropna(subset=["id"])
    df["id"] = df["id"].astype("int64")

    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    df = df.dropna(subset=["created_at"])

    # tránh duplicate PK khi insert
    df = df.drop_duplicates(subset=["id", "created_at"], keep="last")

    keys = list(df[["id", "created_at"]].itertuples(index=False, name=None))
    if not keys:
        print("   ⚠️  No valid keys, skip DB write")
        return

    BATCH = 500

    with engine.begin() as conn:
        deleted_total = 0

        for i in range(0, len(keys), BATCH):
            batch = keys[i : i + BATCH]

            clauses = []
            params = {}

            for j, (_id, _dt) in enumerate(batch):
                clauses.append(f"(id = :id{j} AND created_at = :dt{j})")
                params[f"id{j}"] = int(_id)
                # đảm bảo là python datetime
                params[f"dt{j}"] = _dt.to_pydatetime() if hasattr(_dt, "to_pydatetime") else _dt

            delete_sql = f"DELETE FROM {TABLE_NAME} WHERE " + " OR ".join(clauses)
            result = conn.execute(text(delete_sql), params)
            deleted_total += result.rowcount

        print(f"   🗑️  Deleted {deleted_total} existing rows (matched keys)")

        # Insert (cùng transaction/connection)
        df.to_sql(
            TABLE_NAME,
            con=conn,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=2000,
        )
        print(f"   💾 Inserted {len(df)} new rows")

# ==================== DATA PROCESSING ====================
def clean_money(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s == "" or s == "-":
        return None

    s = s.replace("\u00a0", " ").strip()

    neg = "-" if s.startswith("-") else ""
    s = s.lstrip("-")

    s = re.sub(r"[^\d\.,]", "", s)
    if s == "":
        return None

    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        s = s.replace(",", "")

    try:
        return float(Decimal(neg + s))
    except (InvalidOperation, ValueError):
        return None

def normalize_df(df: pd.DataFrame, run_from: date, run_to: date) -> pd.DataFrame:
    print(f"   📊 Original shape: {df.shape}")

    df = df.rename(columns=COL_MAP)

    keep_cols = [v for v in COL_MAP.values() if v in df.columns]
    df = df[keep_cols].copy()

    for c in DATE_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce", dayfirst=True)

    for c in MONEY_COLS:
        if c in df.columns:
            df[c] = df[c].apply(clean_money)

    for c in INT_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")

    if "id" in df.columns:
        df["id"] = pd.to_numeric(df["id"], errors="coerce").astype("Int64")

    df["run_from"] = pd.to_datetime(run_from)
    df["run_to"] = pd.to_datetime(run_to)
    df["loaded_at"] = pd.to_datetime(datetime.now())

    df = df.dropna(how="all")

    print(f"   📊 Cleaned shape: {df.shape}")
    return df

def find_warranty_files(directory: Path = SCRIPT_DIR, pattern: str = "dump_warranty*.xlsx") -> list:
    files = list(directory.glob(pattern))
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    return files

def read_excel(path: Path) -> pd.DataFrame:
    print(f"   📖 Reading: {path.name}")
    return pd.read_excel(path, engine="openpyxl")

def extract_date_range_from_filename(filename: str) -> tuple:
    match = re.search(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})", filename)
    if match:
        from_str, to_str = match.groups()
        from_date = datetime.strptime(from_str, "%Y-%m-%d").date()
        to_date = datetime.strptime(to_str, "%Y-%m-%d").date()
        return from_date, to_date

    today = date.today()
    from_date = date(today.year, today.month, 1)
    to_date = today
    print(f"   ⚠️  Could not extract dates from filename, using: {from_date} -> {to_date}")
    return from_date, to_date

# ==================== MAIN ====================
def main():
    print("=" * 60)
    print("🚀 NHANH.VN WARRANTY DATA PROCESSOR")
    print("=" * 60)

    engine = get_engine()
    print("✅ Connected to database")

    create_table_if_not_exists(engine)
    print()

    print(f"🔍 Searching for warranty files in: {SCRIPT_DIR}")
    files = find_warranty_files(SCRIPT_DIR, pattern="dump_warranty*.xlsx")

    if not files:
        print("❌ No warranty files found!")
        print(f"   Looking for files matching: dump_warranty*.xlsx")
        print(f"   In directory: {SCRIPT_DIR}")
        print()
        print("💡 Please ensure you have the Excel file in the same folder as this script")
        print(f"   Expected path example: {SCRIPT_DIR / 'dump_warranty.xlsx'}")
        return

    print(f"📁 Found {len(files)} file(s):")
    for i, f in enumerate(files, 1):
        mod_time = datetime.fromtimestamp(f.stat().st_mtime)
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"   [{i}] {f.name} ({size_mb:.2f} MB, modified: {mod_time:%Y-%m-%d %H:%M})")
    print()

    selected_files = files
    print("📦 Processing the only/newest file found..." if len(files) == 1 else "📦 Processing all found files...")
    print()

    total_rows = 0
    for idx, file_path in enumerate(selected_files, 1):
        print(f"📦 [{idx}/{len(selected_files)}] Processing: {file_path.name}")

        try:
            from_date, to_date = extract_date_range_from_filename(file_path.name)
            print(f"   📅 Date range(meta): {from_date} → {to_date}")

            df_raw = read_excel(file_path)

            print("   🔧 Normalizing data...")
            df = normalize_df(df_raw, from_date, to_date)

            # ================== FILTER (nếu bạn muốn) ==================
            # df = df[df["status"].isin(["..."])]
            # ===========================================================

            if len(df) == 0:
                print("   ⚠️  No data to process (empty after cleaning/filter)")
                print()
                continue

            print("   📊 Sample data:")
            print(f"      - Total records: {len(df)}")
            if "created_at" in df.columns:
                print(f"      - Date range in data: {df['created_at'].min()} → {df['created_at'].max()}")
            if "customer_name" in df.columns:
                print(f"      - Unique customers: {df['customer_name'].nunique()}")

            print("   💾 Saving to database (delete by keys then insert)...")
            db_delete_then_insert(engine, df)
            total_rows += len(df)

            print("   ✅ Completed")
            print()

        except Exception as e:
            print(f"   ❌ Error processing file: {e}")
            import traceback
            traceback.print_exc()
            print()
            continue

    print("=" * 60)
    print("✅ COMPLETED!")
    print(f"📊 Total rows inserted: {total_rows}")
    print(f"🗄️  Table: {TABLE_NAME}")
    print("=" * 60)

if __name__ == "__main__":
    main()