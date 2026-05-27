# DAX — MKT Measures: Sửa lỗi & Thêm mới

Áp dụng trong Power BI Desktop: Home → **Enter Data** (không dùng) hoặc  
mở **Modeling → New Measure** / **Manage Measures** để paste DAX bên dưới.

---

## 🔴 Phần 1 — SỬA LỖI (2 measures)

### 1. mkt_CPA_Paid_YoY%  — sai công thức
Bảng: `_MKT_Measures`

**Hiện tại (sai):**
```dax
mkt_CPA_Paid_YoY% = [mkt_CPA_Paid] - [mkt_CPA_Paid_LY]
```

**Sửa thành:**
```dax
mkt_CPA_Paid_YoY% = 
    DIVIDE(
        [mkt_CPA_Paid] - [mkt_CPA_Paid_LY],
        [mkt_CPA_Paid_LY]
    )
```
> Công thức cũ trả về VNĐ tuyệt đối, không phải %. KPI này "thấp hơn = tốt" nên YoY% âm là tốt — đảo màu conditional formatting (âm = xanh).

---

### 2. Qty Growth % 90D  — sai mẫu số
Bảng: `sale_final`

**Hiện tại (sai):**
```dax
Qty Growth % 90D = DIVIDE([Qty_Ndays] - [Qty_Ndays_LY], [Qty_Ndays])
```

**Sửa thành:**
```dax
Qty Growth % 90D = 
    DIVIDE(
        [Qty_Ndays] - [Qty_Ndays_LY],
        [Qty_Ndays_LY]
    )
```
> Growth % = (Current − Base) / Base. Base phải là LY, không phải current.

---

## 🟡 Phần 2 — CHUẨN HÓA TIME INTELLIGENCE (2 measures)

Các measures dùng `DATEADD(pbi_ads/pbi_social[metric_date], -1, YEAR)` trực tiếp trên fact column
→ không phản ứng đúng khi filter qua Calendar slicer.

### 3. mkt_Spend_Paid_LY
Bảng: `_MKT_Measures`
```dax
mkt_Spend_Paid_LY = 
    CALCULATE(
        [mkt_Spend_Paid],
        SAMEPERIODLASTYEAR( 'Calendar'[Date] )
    )
```

### 4. mkt_Engagement_LY
Bảng: `_MKT_Measures`
```dax
mkt_Engagement_LY = 
    CALCULATE(
        [mkt_Engagement],
        SAMEPERIODLASTYEAR( 'Calendar'[Date] )
    )
```

### 5. mkt_Engagement_Rate_LY
Bảng: `_MKT_Measures`
```dax
mkt_Engagement_Rate_LY = 
    CALCULATE(
        [mkt_Engagement_Rate],
        SAMEPERIODLASTYEAR( 'Calendar'[Date] )
    )
```

### 6. mkt_Follower_LY
Bảng: `_MKT_Measures`
```dax
mkt_Follower_LY = 
    CALCULATE(
        [mkt_Follower],
        SAMEPERIODLASTYEAR( 'Calendar'[Date] )
    )
```

### 7. mkt_Reach_LY
Bảng: `_MKT_Measures`
```dax
mkt_Reach_LY = 
    CALCULATE(
        [mkt_Reach],
        SAMEPERIODLASTYEAR( 'Calendar'[Date] )
    )
```

### 8. mkt_Views_LY
Bảng: `_MKT_Measures`
```dax
mkt_Views_LY = 
    CALCULATE(
        [mkt_Views],
        SAMEPERIODLASTYEAR( 'Calendar'[Date] )
    )
```

---

## 🔴 Phần 3 — THÊM MỚI: YoY% còn thiếu

### 9. mkt_Spend_Paid_YoY%
Bảng: `_MKT_Measures`
```dax
mkt_Spend_Paid_YoY% = 
    DIVIDE(
        [mkt_Spend_Paid] - [mkt_Spend_Paid_LY],
        [mkt_Spend_Paid_LY]
    )
```

### 10. mkt_Impressions_Paid_YoY%
Bảng: `_MKT_Measures`
```dax
mkt_Impressions_Paid_YoY% = 
    DIVIDE(
        [mkt_Impressions_Paid] - [mkt_Impressions_Paid_LY],
        [mkt_Impressions_Paid_LY]
    )
```

### 11. mkt_CPM_Paid_YoY%
Bảng: `_MKT_Measures`
```dax
mkt_CPM_Paid_YoY% = 
    DIVIDE(
        [mkt_CPM_Paid] - [mkt_CPM_Paid_LY],
        [mkt_CPM_Paid_LY]
    )
```
> CPM "thấp hơn = tốt" — đảo màu conditional formatting giống CPA.

### 12. mkt_Post_Count_YoY%
Bảng: `_MKT_Measures`
```dax
mkt_Post_Count_YoY% = 
    DIVIDE(
        [mkt_Post_Count] - [mkt_Post_Count_LY],
        [mkt_Post_Count_LY]
    )
```

---

## 🔴 Phần 4 — THÊM MỚI: ROAS (quan trọng nhất cho ECOM)

> ROAS = Revenue ECOM / Spend Paid. Cần có measure `Revenue_ECOM` trong sale_final.
> Nếu chưa có, tạo measure `Revenue_ECOM` trước (Phần 4a), rồi mới tạo ROAS.

### 4a. Revenue_ECOM (tạo trước nếu chưa có)
Bảng: `sale_final`
```dax
Revenue_ECOM = 
    CALCULATE(
        [Revenue],
        sale_final[channel] = "ECOM"
    )
```

### 13. mkt_ROAS
Bảng: `_MKT_Measures`
```dax
mkt_ROAS = 
    DIVIDE(
        [Revenue_ECOM],
        [mkt_Spend_Paid]
    )
```
> ROAS = Revenue ECOM / Spend quảng cáo. Mục tiêu thường > 3x. KPI quan trọng nhất cho CMO.

### 14. mkt_ROAS_LY
Bảng: `_MKT_Measures`
```dax
mkt_ROAS_LY = 
    CALCULATE(
        [mkt_ROAS],
        SAMEPERIODLASTYEAR( 'Calendar'[Date] )
    )
```

### 15. mkt_ROAS_YoY%
Bảng: `_MKT_Measures`
```dax
mkt_ROAS_YoY% = 
    DIVIDE(
        [mkt_ROAS] - [mkt_ROAS_LY],
        [mkt_ROAS_LY]
    )
```

---

## Thứ tự áp dụng

| # | Measure | Bảng | Hành động |
|---|---------|------|-----------|
| 1 | `mkt_CPA_Paid_YoY%` | _MKT_Measures | Sửa công thức |
| 2 | `Qty Growth % 90D` | sale_final | Sửa mẫu số |
| 3 | `mkt_Spend_Paid_LY` | _MKT_Measures | Sửa time intelligence |
| 4 | `mkt_Engagement_LY` | _MKT_Measures | Sửa time intelligence |
| 5-8 | Các `_LY` còn lại | _MKT_Measures | Sửa time intelligence |
| 9-12 | `_YoY%` mới | _MKT_Measures | Tạo mới |
| 13 | `Revenue_ECOM` | sale_final | Tạo mới (nếu chưa có) |
| 14-16 | `mkt_ROAS*` | _MKT_Measures | Tạo mới |

---

## Cách áp dụng trong PBI Desktop

1. Mở file `.pbix` → tab **Model** (biểu tượng 3 hình vuông bên trái)
2. Click phải vào bảng `_MKT_Measures` → **New measure**
3. Paste DAX → Enter
4. Với measures cần sửa: double-click tên measure trong Data pane → sửa formula bar → Enter
