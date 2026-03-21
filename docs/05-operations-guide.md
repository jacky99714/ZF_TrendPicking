# 操作指南

> ZF_TrendPicking 環境安裝、日常操作與 GitHub Actions 排程

## 1. 環境安裝

### 1.1 系統需求

- Python 3.11+
- Git
- Google Cloud Service Account（具 Google Sheets API 權限）

### 1.2 安裝步驟

```bash
# 1. 克隆專案
git clone <repo-url>
cd ZF_TrendPicking

# 2. 建立虛擬環境
python3.11 -m venv .venv
source .venv/bin/activate

# 3. 安裝依賴
pip install -r requirements.txt

# 4. 設定環境變數
cp .env.example .env
# 編輯 .env 填入必要的環境變數（見下方）
```

### 1.3 環境變數設定

在 `.env` 中設定：

```env
# === 台股 ===
FINMIND_TOKEN=your_finmind_token

# Google Sheet IDs
SHEET_ID_COMPANY_MASTER=your_sheet_id
SHEET_ID_TW_VCP=your_sheet_id
SHEET_ID_TW_SANXIAN=your_sheet_id
SHEET_ID_VERIFICATION=your_sheet_id  # 選填

# === 美股 ===
US_SHEET_ID_COMPANY_MASTER=your_sheet_id
US_SHEET_ID_VCP=your_sheet_id
US_SHEET_ID_SANXIAN=your_sheet_id
US_SHEET_ID_VERIFICATION=your_sheet_id  # 選填
```

### 1.4 Google Service Account 設定

1. 在 [Google Cloud Console](https://console.cloud.google.com/) 建立專案
2. 啟用 Google Sheets API
3. 建立 Service Account，下載 JSON 金鑰
4. 將金鑰存為 `credentials.json`（專案根目錄）
5. 將 Service Account 的 email 加入各 Google Sheet 的共用（編輯者權限）

---

## 2. 首次初始化

### 2.1 台股初始化

```bash
source .venv/bin/activate
python main.py init
```

執行內容：
1. 建立 SQLite 資料庫 `data/zf_trend.db`
2. 建立所有資料表
3. 從 FinMind 下載台股上市櫃清單
4. 下載近 1 年歷史股價
5. 下載大盤指數
6. 匯出公司主檔到 Google Sheet

預估時間：10-20 分鐘

### 2.2 美股初始化

```bash
source .venv/bin/activate
python us_main.py init
```

執行內容：
1. 建立 SQLite 資料庫 `data/zf_trend_us.db`
2. 建立所有資料表（us_ 前綴）
3. 從 NASDAQ FTP 下載美股清單
4. 批次下載 8000+ 檔美股歷史股價
5. 下載 S&P 500 / 道瓊 / NASDAQ 指數
6. 從 yfinance 補充 sector/industry
7. 匯出公司主檔到 Google Sheet

預估時間：30-60 分鐘（受網路速度影響）

---

## 3. 日常操作指令

### 3.1 每日篩選

```bash
# 台股（使用今天日期）
python main.py daily

# 美股
python us_main.py daily

# 指定日期
python main.py daily 2026-03-20
python us_main.py daily 2026-03-20

# 強制執行（忽略假日檢查）
python main.py daily --force
python us_main.py daily --force

# 指定日期 + 強制執行
python main.py daily 2026-03-20 --force
```

### 3.2 每月更新

```bash
# 台股
python main.py monthly

# 美股
python us_main.py monthly
```

### 3.3 健康檢查

```bash
# 台股
python main.py health

# 美股
python us_main.py health
```

檢查項目：
- 資料庫連線
- 資料表完整性
- 最新資料日期
- API 連線狀態

### 3.4 歷史資料補齊

```bash
# 台股 backfill
python main.py backfill

# 重新計算並匯出所有日期
python scripts/reexport_all_dates.py

# 跳過 backfill，僅重新計算
python scripts/reexport_all_dates.py --skip-fetch
```

### 3.5 維護腳本

```bash
# 匯出單一股票驗證資料
python scripts/export_single_stock.py

# 修復零價異常（預覽）
python scripts/fix_zero_prices_in_db.py --preview

# 修復零價異常（實際執行）
python scripts/fix_zero_prices_in_db.py --fix

# 重建台股價格資料
python scripts/rebuild_price_data.py
```

---

## 4. GitHub Actions 排程設定

### 4.1 工作流程總覽

| 工作流程 | 檔案 | 排程 (UTC) | 台灣時間 | 說明 |
|---------|------|-----------|---------|------|
| Daily Stock Screening | `.github/workflows/daily.yml` | `45 9 * * 1-5` | 週一~五 17:45 | 台股每日篩選 |
| US Daily Stock Screening | `.github/workflows/us-daily.yml` | `30 21 * * 1-5` | 週一~五 05:30+1 | 美股每日篩選 |
| Monthly Stock Update | `.github/workflows/monthly.yml` | `0 1 1 * *` | 每月1日 09:00 | 台股每月更新 |
| US Monthly Stock Update | `.github/workflows/us-monthly.yml` | `30 1 1 * *` | 每月1日 09:30 | 美股每月更新 |

另有：
- `test-schedule.yml`：排程測試用（UTC 16:15）
- `export-stock.yml`：手動觸發，匯出單一股票資料

### 4.2 工作流程執行步驟

每個工作流程遵循相同模式：

```
1. Checkout repository
2. Set up Python 3.11 (pip cache)
3. Install dependencies (pip install -r requirements.txt)
4. Download database from Release (gzip 壓縮檔)
   └── 備援: Download from Artifact (台股 daily/monthly)
5. Set up Google credentials (from Secret)
6. Initialize if needed (首次執行)
7. Run task (daily / monthly)
8. Backup database to Release (gzip 壓縮，--clobber 覆蓋)
   └── 額外: Upload Artifact (台股 daily/monthly，90天有效)
9. Upload logs (always，30天有效)
```

### 4.3 GitHub Secrets 設定

在 Repository → Settings → Secrets and variables → Actions 中設定：

| Secret | 說明 | 用於 |
|--------|------|------|
| `GOOGLE_CREDENTIALS_JSON` | Service Account JSON 完整內容 | 所有工作流程 |
| `FINMIND_TOKEN` | FinMind API Token | 台股 daily/monthly |
| `SHEET_ID_COMPANY_MASTER` | 公司主檔 Sheet ID | 台股 daily/monthly |
| `SHEET_ID_TW_VCP` | 台股 VCP Sheet ID | 台股 daily |
| `SHEET_ID_TW_SANXIAN` | 台股三線開花 Sheet ID | 台股 daily |
| `SHEET_ID_VERIFICATION` | 台股驗證 Sheet ID | 台股 daily |
| `US_SHEET_ID_COMPANY_MASTER` | 美股公司主檔 Sheet ID | 美股 daily/monthly |
| `US_SHEET_ID_VCP` | 美股 VCP Sheet ID | 美股 daily |
| `US_SHEET_ID_SANXIAN` | 美股三線開花 Sheet ID | 美股 daily |
| `US_SHEET_ID_VERIFICATION` | 美股驗證 Sheet ID | 美股 daily |

> `GITHUB_TOKEN` 由 GitHub 自動提供，無需手動設定。

### 4.4 資料庫備份機制

#### 主要備份：GitHub Release

| 市場 | Release tag | 壓縮檔名 | 保存期限 |
|------|------------|---------|---------|
| 台股 | `db-backup` | `zf_trend_full.db.gz` | 永久 |
| 美股 | `us-db-backup` | `zf_trend_us.db.gz` | 永久 |

- 每次任務完成後自動壓縮上傳
- 使用 `--clobber` 覆蓋舊檔
- Release 在 GitHub 上永久保存

#### 備援備份：GitHub Artifact

| 市場 | Artifact 名稱 | 保存期限 |
|------|--------------|---------|
| 台股 | `sqlite-database` | 90 天 |

- 僅台股 daily/monthly 使用（美股未設定 Artifact 備份）
- 當 Release 下載失敗時自動使用 Artifact

#### 還原流程

```
1. 優先嘗試從 Release 下載 → gunzip 解壓
2. 若失敗 → 嘗試從 Artifact 下載（台股）
3. 若都失敗 → 執行 init 重新初始化
```

### 4.5 手動觸發

所有工作流程均支援 `workflow_dispatch` 手動觸發：

1. 前往 Repository → Actions
2. 選擇工作流程
3. 點擊 "Run workflow"
4. 台股/美股 daily 可填入：
   - `target_date`：指定日期（YYYY-MM-DD，留空用今天）
   - `force`：勾選忽略假日檢查

### 4.6 Timeout 設定

| 工作流程 | timeout-minutes |
|---------|----------------|
| 台股 daily | 無限制（預設 360 分鐘） |
| 美股 daily | 60 分鐘 |
| 台股 monthly | 無限制 |
| 美股 monthly | 30 分鐘 |

---

## 5. 故障排除

### 5.1 常見問題

| 問題 | 原因 | 解決方式 |
|------|------|---------|
| `FinMind API 429 Too Many Requests` | 超過 600 次/小時限制 | 等待限流解除，或增加間隔設定 |
| `Google Sheet API quota exceeded` | 超過 Google API 配額 | 等待配額重置（每 100 秒 100 次） |
| `yfinance 下載失敗` | Yahoo Finance 暫時不可用 | 自動重試，或手動重新執行 |
| `資料庫鎖定 (database is locked)` | 多個 process 同時寫入 | 確保僅一個 process 執行（WAL 模式已緩解） |
| `非交易日跳過執行` | 正常行為 | 使用 `--force` 強制執行 |
| `VCP/三線開花結果為空` | 當日無符合條件的股票 | 正常現象，檢查篩選條件是否合理 |
| `Release 下載失敗` | Release 不存在（首次部署） | 自動觸發 init 初始化 |

### 5.2 日誌位置

| 市場 | 日誌檔案 | 環境變數 |
|------|---------|---------|
| 台股 | `logs/zf_trend.log` | `LOG_LEVEL` |
| 美股 | `logs/zf_trend_us.log` | `US_LOG_LEVEL` |

GitHub Actions 日誌：
- Artifact 上傳：`logs-{run_id}` / `us-logs-{run_id}`
- 保存 30 天

### 5.3 交易日曆維護

交易日曆目前硬編碼在程式中（2024-2026）：

| 市場 | 檔案 | 類別 |
|------|------|------|
| 台股 | `utils/trading_calendar.py` | `TradingCalendar` |
| 美股 | `utils/us_trading_calendar.py` | `USMarketCalendar` |

**維護方式**：每年需更新假日清單（加入新年度的國定假日、提前收盤日等）。

台股假日包含：元旦、農曆春節、和平紀念日、清明節、勞動節、端午節、中秋節、國慶日。

美股假日包含：新年、MLK Day、總統日、耶穌受難日、陣亡將士紀念日、國慶日、勞動節、感恩節、聖誕節。另有提前收盤日（感恩節翌日、聖誕節前夕等）。

---

## 6. 效能考量

| 項目 | 說明 |
|------|------|
| 台股每日任務 | ~5-10 分鐘（FinMind 逐檔查詢約 1,700 檔） |
| 美股每日任務 | ~15-30 分鐘（yfinance 批次 100 檔 × ~80 批） |
| 台股每月任務 | ~2-3 分鐘 |
| 美股每月任務 | ~10-15 分鐘（含 sector/industry 補充） |
| SQLite WAL 模式 | 提升併發讀寫效能 |
| pandas 批次計算 | 均線使用 `groupby + rolling`，一次計算所有股票 |
| gzip 壓縮 | 資料庫壓縮比約 70-80%，節省傳輸和儲存 |

---

## 7. 參考文件

- [需求規格](./01-requirements-spec.md)
- [技術架構](./02-architecture.md)
- [資料規格](./03-data-spec.md)
- [演算法規格](./04-algorithm-spec.md)
