# ZF_TrendPicking_usstock 專案指南

## 虛擬環境（重要！）

**啟動專案前必須先啟用虛擬環境：**

```bash
source .venv/bin/activate
```

所有 Python 指令都需要在虛擬環境中執行。

---

## 專案架構

此專案同時支援 **台股** 和 **美股** 技術分析篩選，兩者完全獨立隔離。

### 台股系統
- **主程式**：`python main.py`
- **資料庫**：`data/zf_trend.db`
- **設定**：`config/settings.py`

### 美股系統
- **主程式**：`python us_main.py`
- **資料庫**：`data/zf_trend_us.db`（獨立）
- **設定**：`config/us_settings.py`

---

## 常用指令

### 台股
```bash
source .venv/bin/activate

# 初始化
python main.py init

# 每日篩選
python main.py daily

# 健康檢查
python main.py health
```

### 美股
```bash
source .venv/bin/activate

# 初始化（首次執行，約 30-60 分鐘）
python us_main.py init

# 每日篩選
python us_main.py daily

# 健康檢查
python us_main.py health
```

---

## 環境變數

美股系統需要在 `.env` 中設定：

```env
# 美股 Google Sheet ID（3 個獨立 Sheet）
US_SHEET_ID_COMPANY_MASTER=<美股公司主檔 Sheet ID>
US_SHEET_ID_VCP=<美股 VCP Sheet ID>
US_SHEET_ID_SANXIAN=<美股三線開花 Sheet ID>
```

---

## 美股新增檔案（14 個）

| 檔案 | 用途 |
|------|------|
| `config/us_settings.py` | 美股專用設定 |
| `data/us_models.py` | 美股資料模型 |
| `data/us_database.py` | 美股資料庫操作 |
| `utils/us_trading_calendar.py` | 美股交易日曆 |
| `api/us_stock_client.py` | 美股 API 抽象介面 |
| `api/us_stock_client_free.py` | 免費版（yfinance） |
| `api/us_stock_client_paid.py` | 付費版預留 |
| `calculators/us_moving_average.py` | 美股均線計算 |
| `calculators/us_vcp_filter.py` | 美股 VCP 篩選 |
| `calculators/us_sanxian_filter.py` | 美股三線開花篩選 |
| `tasks/us_daily_task.py` | 美股每日任務 |
| `tasks/us_monthly_task.py` | 美股每月任務 |
| `exporters/us_google_sheet.py` | 美股 Sheet 匯出 |
| `us_main.py` | 美股主程式入口 |

---

## 注意事項

1. **完全隔離**：美股功能不會影響台股，反之亦然
2. **虛擬環境**：每次操作前務必先 `source .venv/bin/activate`
3. **資料來源**：美股使用 yfinance（免費），可日後切換付費 API
