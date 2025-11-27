from playwright.sync_api import sync_playwright
import pandas as pd
import os

def crawl_nhanh_detail_orders(): 
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        state_path = "playwright_nhanh_user_data/state.json"

        # Lưu phiên đăng nhập nếu chưa có
        if not os.path.exists(state_path):
            context = browser.new_context()
            page = context.new_page()
            page.goto("https://nhanh.vn/login")
            input("🔐 Đăng nhập Nhanh.vn thủ công rồi nhấn Enter để tiếp tục...")
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            context.storage_state(path=state_path)
            context.close()

        # Dùng lại phiên đã lưu
        context = browser.new_context(storage_state=state_path)
        page = context.new_page()

        # Base URL phân trang
        base_url = "https://nhanh.vn/order/manage/index?fromDate=2023-08-01&toDate=2025-06-22&statuses=60&customerName=L%C6%AFU%20V%C4%82N%20HU%E1%BA%A4N&businessId=92233&"
        MAX_PAGE = 2  # Bạn muốn duyệt đến trang thứ 17

        data = []

        for page_num in range(1, MAX_PAGE + 1):
            print(f"\n🌐 Đang xử lý trang {page_num}/{MAX_PAGE}")
            page.goto(f"{base_url}&page={page_num}")
            page.wait_for_timeout(3000)

            don_hangs = page.locator('a.fw-bold.ms-2.cursor-pointer')
            total = don_hangs.count()
            print(f"📦 Trang {page_num} có {total} đơn hàng.")

            for i in range(total):
                try:
                    don = don_hangs.nth(i)
                    ma_don = don.inner_text().strip()
                    print(f"➡️ ({i+1}/{total}) Mã đơn: {ma_don}")

                    # Click vào đơn hàng để mở popup
                    don.click()
                    page.wait_for_timeout(2500)

                    # Scroll đến phần "Lịch trình"
                    try:
                        lich_trinh_tieu_de = page.locator("text=Lịch trình").first
                        lich_trinh_tieu_de.scroll_into_view_if_needed()
                        page.wait_for_timeout(500)
                    except:
                        print("⚠️ Không thấy 'Lịch trình'")
                        page.keyboard.press("Escape")
                        page.wait_for_timeout(1000)
                        continue

                    # Lấy các dòng lịch sử
                    rows = page.locator("app-c-table table tbody tr")
                    lich_trinh = rows.all_inner_texts()

                    count_edit = sum(1 for row in lich_trinh if "Sửa đơn hàng" in row)
                    count_print = sum(1 for row in lich_trinh if "In đơn hàng" in row)

                    print(f"   📝 Sửa đơn: {count_edit} | In đơn: {count_print}")

                    if count_edit >= 2 and count_print >= 2:
                        data.append({
                            "Mã đơn hàng": ma_don,
                            "Số lần sửa đơn": count_edit,
                            "Số lần in đơn": count_print,
                            "Trang": page_num
                        })

                    # Đóng popup
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(1000)

                except Exception as e:
                    print(f"❌ Lỗi đơn {i+1} trang {page_num}: {e}")
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(1000)
                    continue

        # Xuất kết quả
        if data:
            df = pd.DataFrame(data)
            df.to_excel("don_anh_huan_sua_nhieu_lan.xlsx", index=False)
            print("\n✅ Đã xuất file: don_anh_huan_sua_nhieu_lan.xlsx")
        else:
            print("\n⚠️ Không có đơn hàng nào thỏa điều kiện.")

        browser.close()

if __name__ == "__main__":
    crawl_nhanh_detail_orders()
