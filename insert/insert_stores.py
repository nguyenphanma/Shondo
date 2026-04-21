from datetime import datetime
from sqlalchemy import text
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from core.db import get_engine

engine = get_engine()

store_data = {
    "217633": {"id": 217633, "code": "KHOSICHECK", "name": "KHOSICHECK", "mobile": "89898989898", "address": "Kho 40"},
    "217642": {"id": 217642, "code": "ECOM HN",    "name": "ECOM HN",    "mobile": "89898989899", "address": "Kho 41"},
}

def insert_stores(data):
    try:
        with engine.connect() as conn:
            for info in data.values():
                conn.execute(text("""
                    INSERT INTO stores (store_id, name, address, phone, code_nhanh, created_at, updated_at)
                    VALUES (:store_id, :name, :address, :phone, :code_nhanh, :created_at, :updated_at)
                    ON DUPLICATE KEY UPDATE
                        name = VALUES(name), address = VALUES(address),
                        phone = VALUES(phone), code_nhanh = VALUES(code_nhanh),
                        updated_at = VALUES(updated_at)
                """), {**info, "store_id": info["id"], "phone": info["mobile"],
                       "code_nhanh": info["code"], "created_at": datetime.now(), "updated_at": datetime.now()})
            conn.commit()
        print("✅ Đã chèn dữ liệu thành công!")
    except Exception as e:
        print(f"❌ Lỗi khi chèn dữ liệu: {e}")

insert_stores(store_data)
