import os
import gspread
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()


def get_client() -> gspread.Client:
    """Trả về gspread client dùng mashondo.json."""
    creds_path = Path(os.getenv("ma_shondo_path")) / "mashondo.json"
    return gspread.service_account(creds_path)
