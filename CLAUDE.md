# ZF_TrendPicking 專案指南

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

### 前端查詢網站
- **網址**：GitHub Pages 自動部署
- **架構**：純靜態 HTML + JS，資料拆分為 `index.json` + 月份 JSON + 指標 JSON
- **資料產生**：`scripts/export_to_json_v2.py`（從 DB 匯出拆分 JSON）
- **部署流程**：`.github/workflows/deploy-site.yml`（每日篩選完成後自動觸發）

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

### 前端 JSON 匯出
```bash
# 從 DB 產生拆分 JSON（月份 + 指標）
python scripts/export_to_json_v2.py
```

### 資料維護腳本
```bash
# 補齊所有交易日篩選結果（台股）
python scripts/backfill_all_trading_days.py

# 補齊所有交易日篩選結果（美股）
python scripts/backfill_all_trading_days_us.py

# 補齊美股歷史股價（回溯至 2024-05）
python scripts/backfill_us_prices.py

# 修復缺失的 indicator_json
python scripts/fix_missing_indicators.py
python scripts/fix_missing_indicators.py --us

# 重新匯出篩選結果到 Google Sheet（從 DB 讀取，不重算）
python scripts/reexport_all_dates.py --from-db

# 資料驗證（4 層檢查）
python scripts/verify_data.py
python scripts/verify_data.py --us
```

---

## 環境變數

在 `.env` 中設定：

```env
# === 台股 ===
FINMIND_TOKEN=<FinMind API Token>
SHEET_ID_COMPANY_MASTER=<台股公司主檔 Sheet ID>
SHEET_ID_TW_VCP=<台股 VCP Sheet ID>
SHEET_ID_TW_SANXIAN=<台股三線開花 Sheet ID>
SHEET_ID_VERIFICATION=<台股驗證 Sheet ID>

# === 美股 ===
US_SHEET_ID_COMPANY_MASTER=<美股公司主檔 Sheet ID>
US_SHEET_ID_VCP=<美股 VCP Sheet ID>
US_SHEET_ID_SANXIAN=<美股三線開花 Sheet ID>
US_SHEET_ID_VERIFICATION=<美股驗證 Sheet ID>
```

---

## 前端架構（v2 拆分 JSON）

前端採用 lazy loading 架構，避免一次載入所有資料：

| 檔案 | 大小 | 說明 |
|------|------|------|
| `site/data/index.json` | ~1.2 MB | 股票主檔 + 月份清單 + 資料範圍 |
| `site/data/months/{YYYY-MM}.json` | ~1-2 MB/月 | 該月篩選結果 |
| `site/data/indicators/{YYYY-MM}.json` | ~3 MB/月 | 指標 tooltip 資料（點擊 tag 時載入） |

前端特性：
- 首次載入只下載 `index.json` + 最近 2 個月
- 搜尋股票使用 `STOCK_INDEX` 反向索引（O(1) 查找）
- 搜尋輸入 300ms debounce + 限制 50 筆結果
- Tag 指標 tooltip 按月快取，點擊時才載入
- 新/舊股票標記（與前一交易日比較）
- 排序：綜合、新股優先、20日漲幅、突破差距
- Google Sheet 匯出也有新/舊股票背景色標記

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
3. **資料來源**：台股使用 FinMind + yfinance 備援，美股使用 yfinance
4. **前端部署**：每日篩選完成後自動觸發 `Deploy Site` workflow，也可手動觸發
5. **新/舊標記**：前端跨類型比較（VCP+三線合併），Google Sheet 同類型獨立比較
