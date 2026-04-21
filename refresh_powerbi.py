import requests
import os
import time
from datetime import datetime

CLIENT_ID     = os.environ.get("PBI_CLIENT_ID",     "your-client-id")
CLIENT_SECRET = os.environ.get("PBI_CLIENT_SECRET", "your-client-secret")
TENANT_ID     = os.environ.get("PBI_TENANT_ID",     "your-tenant-id")

# My Workspace: workspace_id để trống
DATASETS = {
    "Company Performance": {
        "dataset_id": os.environ.get("PBI_DATASET_ID", "975914e1-e19c-4394-9956-912d63e07eef"),
    },
    # Thêm báo cáo khác nếu cần:
    # "Ecommerce Performance": {"dataset_id": "..."},
}

AUTHORITY    = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPE        = "https://analysis.windows.net/powerbi/api/.default"
PBI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"


def get_access_token():
    resp = requests.post(AUTHORITY, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         SCOPE,
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def trigger_refresh(token: str, dataset_id: str):
    # My Workspace dùng endpoint không có /groups/{id}
    url = f"{PBI_BASE_URL}/datasets/{dataset_id}/refreshes"
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 202:
        return "triggered"
    resp.raise_for_status()


def get_refresh_status(token: str, dataset_id: str) -> dict:
    url = f"{PBI_BASE_URL}/datasets/{dataset_id}/refreshes?$top=1"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    history = resp.json().get("value", [])
    return history[0] if history else {}


def refresh_and_wait(name: str, dataset_id: str, timeout_minutes: int = 30):
    print(f"\n[{datetime.now():%H:%M:%S}] Bắt đầu refresh: {name}")
    token = get_access_token()
    trigger_refresh(token, dataset_id)

    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        time.sleep(20)
        token = get_access_token()
        status = get_refresh_status(token, dataset_id)
        state = status.get("status", "Unknown")
        print(f"  [{datetime.now():%H:%M:%S}] Trạng thái: {state}")

        if state == "Completed":
            print(f"  Refresh thanh cong luc {datetime.now():%H:%M:%S}")
            return True
        if state == "Failed":
            error = status.get("serviceExceptionJson", "")
            print(f"  Refresh that bai: {error}")
            return False

    print(f"  Timeout sau {timeout_minutes} phut")
    return False


if __name__ == "__main__":
    for name, ids in DATASETS.items():
        refresh_and_wait(name, ids["dataset_id"])
