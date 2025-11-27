from playwright.sync_api import sync_playwright
import pandas as pd
import os

def crawl_nhanh_all_pages_17():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        state_path = "playwright_nhanh_user_data/state.json"

        if not os.path.exists(state_path):
            context = browser.new_context()
            page = context.new_page()
            page.goto("https://nhanh.vn/login")
            input("🔐 Đăng nhập Nhanh.vn thủ công rồi nhấn Enter để tiếp tục...")
            os.makedirs(os.path.dirname(state_path), exist_ok=True)
            context.storage_state(path=state_path)
            context.close()

        context = browser.new_context(storage_state=state_path)
        page = context.new_page()

        base_url = "https://nhanh.vn/order/manage/index?statuses=60&customerName=V%C4%82N%20HU%E1%BA%A4N&businessId=92233&icpp=100&page={}"

        summary_data = []
        all_products_detail = []

        for page_num in range(1, 4):  # từ 1 đến 17
            print(f"\n📄 Đang xử lý trang {page_num}...")
            page.goto(base_url.format(page_num))
            page.wait_for_timeout(3000)

            don_hangs = page.locator('a.fw-bold.ms-2.cursor-pointer')
            total = don_hangs.count()
            print(f"🔎 Trang {page_num} có {total} đơn hàng.")

            for i in range(total):
                try:
                    don = don_hangs.nth(i)
                    ma_don = don.inner_text().strip()
                    print(f"➡️ ({i+1}/{total}) Mã đơn: {ma_don}")

                    don.click()
                    page.wait_for_timeout(2500)

                    try:
                        lich_trinh_tieu_de = page.locator("text=Lịch trình").first
                        lich_trinh_tieu_de.scroll_into_view_if_needed()
                        page.wait_for_timeout(500)
                    except:
                        print("⚠️ Không thấy 'Lịch trình'")
                        page.keyboard.press("Escape")
                        page.wait_for_timeout(1000)
                        continue

                    rows = page.locator("app-c-table table tbody tr")
                    lich_trinh = rows.all_inner_texts()

                    count_edit = sum(1 for row in lich_trinh if "Sửa đơn hàng" in row)
                    count_print = sum(1 for row in lich_trinh if "In đơn hàng" in row)
                    print(f"   📝 Sửa đơn: {count_edit} | In đơn: {count_print}")

                    if count_edit >= 2 and count_print >= 2:
                        pos_print = [idx for idx, row in enumerate(lich_trinh) if "In đơn hàng" in row]
                        pos_edit = [idx for idx, row in enumerate(lich_trinh) if "Sửa đơn hàng" in row]

                        if len(pos_print) >= 2:
                            second_last_print_idx = pos_print[-2]
                            valid_edit_idx = None
                            for idx in reversed(pos_edit):
                                if idx < second_last_print_idx:
                                    valid_edit_idx = idx
                                    break

                            if valid_edit_idx is not None:
                                try:
                                    row_to_click = rows.nth(valid_edit_idx).locator("td", has_text="Sửa đơn hàng")
                                    row_to_click.click()
                                    page.wait_for_timeout(2000)

                                    popup_content = page.locator("ngb-modal-window td >> table.table-bordered.table-tiny")
                                    product_rows = popup_content.locator("tbody tr")

                                    for j in range(product_rows.count()):
                                        tr = product_rows.nth(j)
                                        tds = tr.locator("td")
                                        if tds.count() >= 6:
                                            product_name = tds.nth(0).inner_text().strip()
                                            qty = tds.nth(1).inner_text().strip()
                                            price = tds.nth(2).inner_text().strip()
                                            ck = tds.nth(3).inner_text().strip()
                                            vat = tds.nth(4).inner_text().strip()
                                            note = tds.nth(5).inner_text().strip()

                                            all_products_detail.append({
                                                "Mã đơn hàng": ma_don,
                                                "Sản phẩm": product_name,
                                                "Số lượng": qty,
                                                "Giá": price,
                                                "Chiết khấu": ck,
                                                "VAT": vat,
                                                "Mô tả": note
                                            })

                                    summary_data.append({
                                        "Mã đơn hàng": ma_don,
                                        "Trang": page_num,
                                        "Số lần sửa đơn": count_edit,
                                        "Số lần in đơn": count_print,
                                        "SL sản phẩm sau sửa": product_rows.count()
                                    })

                                    page.keyboard.press("Escape")
                                    page.wait_for_timeout(1000)

                                except Exception as e:
                                    print(f"❌ Không thể mở chi tiết sửa đơn: {e}")

                    page.keyboard.press("Escape")
                    page.wait_for_timeout(1000)

                except Exception as e:
                    print(f"❌ Lỗi đơn {i+1} tại trang {page_num}: {e}")
                    page.keyboard.press("Escape")
                    page.wait_for_timeout(1000)
                    continue

        # Ghi Excel
        if summary_data:
            writer = pd.ExcelWriter("don_hang_17_trang.xlsx", engine='xlsxwriter')
            pd.DataFrame(summary_data).to_excel(writer, index=False, sheet_name="Tổng hợp")
            pd.DataFrame(all_products_detail).to_excel(writer, index=False, sheet_name="Chi tiết sản phẩm")
            writer.close()
            print("\n✅ Đã xuất file: don_hang_17_trang.xlsx")
        else:
            print("\n⚠️ Không có đơn hàng nào thỏa điều kiện.")

        browser.close()

if __name__ == "__main__":
    crawl_nhanh_all_pages_17()