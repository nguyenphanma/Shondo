import requests
import os
import time
import argparse
from datetime import datetime

CLIENT_ID = os.environ.get("PBI_CLIENT_ID", "f6e026cb-3604-437a-9028-42dc31e68c8d")
TENANT_ID = os.environ.get("PBI_TENANT_ID", "991578f3-fbfe-49b8-b22e-4222f85f5cc2")
USERNAME  = os.environ.get("PBI_USERNAME", "")
PASSWORD  = os.environ.get("PBI_PASSWORD", "")

AUTHORITY    = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
SCOPE        = "https://analysis.windows.net/powerbi/api/Dataset.ReadWrite.All"
PBI_BASE_URL = "https://api.powerbi.com/v1.0/myorg"


def get_access_token():
    resp = requests.post(AUTHORITY, data={
        "grant_type": "password",
        "client_id":  CLIENT_ID,
        "username":   USERNAME,
        "password":   PASSWORD,
        "scope":      SCOPE,
    })
    if not resp.ok:
        print(f"  Token error: {resp.status_code} - {resp.text}")
        resp.raise_for_status()
    return resp.json()["access_token"]


def trigger_refresh(token: str, dataset_id: str):
    url  = f"{PBI_BASE_URL}/datasets/{dataset_id}/refreshes"
    resp = requests.post(url, headers={"Authorization": f"Bearer {token}"})
    if resp.status_code == 202:
        print("  Refresh triggered thanh cong")
        return
    resp.raise_for_status()


def get_refresh_status(token: str, dataset_id: str) -> dict:
    url  = f"{PBI_BASE_URL}/datasets/{dataset_id}/refreshes?$top=1"
    resp = requests.get(url, headers={"Authorization": f"Bearer {token}"})
    resp.raise_for_status()
    history = resp.json().get("value", [])
    return history[0] if history else {}


def refresh_and_wait(dataset_id: str, name: str, timeout_minutes: int = 30):
    print(f"\n[{datetime.now():%H:%M:%S}] Bat dau refresh {name}")
    token = get_access_token()
    trigger_refresh(token, dataset_id)

    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        time.sleep(20)
        token  = get_access_token()
        status = get_refresh_status(token, dataset_id)
        state  = status.get("status", "Unknown")
        print(f"  [{datetime.now():%H:%M:%S}] Trang thai: {state}")

        if state == "Completed":
            print(f"  Refresh thanh cong!")
            return True
        if state == "Failed":
            print(f"  Refresh that bai: {status.get('serviceExceptionJson', '')}")
            return False

    print(f"  Timeout sau {timeout_minutes} phut")
    return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--name", required=True)
    args = parser.parse_args()
    refresh_and_wait(args.dataset_id, args.name)
