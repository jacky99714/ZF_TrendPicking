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

> 公司主檔匯出 Google Sheet 由每月任務 `python main.py monthly` 負責，`init` 不執行。

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

> `init` **不**補充 sector/industry，也**不**匯出公司主檔到 Google Sheet。NASDAQ FTP 清單本身不含產業分類；產業分類補充（從 yfinance）與公司主檔匯出 Sheet 皆由每月任務 `python us_main.py monthly` 負責。**init 後須再跑一次 `monthly` 才會有產業分類**。

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
# 台股 backfill（補齊所有交易日篩選結果）
python scripts/backfill_all_trading_days.py

# 美股 backfill
python scripts/backfill_all_trading_days_us.py

# 美股回溯下載歷史股價（2024-05 起）
python scripts/backfill_us_prices.py

# 修復缺失的 indicator_json（台股 / 美股）
python scripts/fix_missing_indicators.py
python scripts/fix_missing_indicators.py --us

# 重新匯出篩選結果到 Google Sheet（從 DB 讀取，不重算）
python scripts/reexport_all_dates.py --from-db
python scripts/reexport_all_dates.py --from-db --since 2026-03-01
python scripts/reexport_all_dates.py --from-db --last 5
```

### 3.5 前端 JSON 匯出

```bash
# 從 DB 產生拆分 JSON（v2 架構）
python scripts/export_to_json_v2.py
# 輸出：site/data/index.json + months/*.json + indicators/*.json
```

### 3.6 資料驗證

```bash
# 驗證（4 層：完整性、值合理性、新/舊邏輯、篩選準確度）
python scripts/verify_data.py

# 支援的參數：--quick（快速檢查，跳過抽樣重算）、--sample N（抽樣重算天數，預設 5）
python scripts/verify_data.py --quick
python scripts/verify_data.py --sample 10
```

### 3.7 股價缺漏補齊

```bash
# 檢查缺漏（dry-run 模式）
python scripts/backfill_missing_prices.py --tw --dry-run
python scripts/backfill_missing_prices.py --dry-run

# 正式補齊（台股 / 美股）
python scripts/backfill_missing_prices.py --tw --all
python scripts/backfill_missing_prices.py --all

# 指定股票
python scripts/backfill_missing_prices.py --tw --stock 7792
```

> 每日排程已自動執行補漏（daily_task Step 2.5），通常不需要手動跑。

### 3.8 資料完整性驗證

```bash
# 驗證篩選通過的股票
python scripts/verify_stock_gaps.py --tw
python scripts/verify_stock_gaps.py

# 驗證全部股票
python scripts/verify_stock_gaps.py --tw --all
python scripts/verify_stock_gaps.py --all

# 指定股票
python scripts/verify_stock_gaps.py --stock 2330
```

> 注意：檢查資料必須從 GitHub Release 下載線上 DB，本機 DB 不會每天更新。

### 3.9 客觀驗證報告

每日排程自動執行客觀驗證（Step 8），結果寫入驗證 Sheet 的**「驗證日誌」**分頁。

查看方式：
1. 開啟驗證 Sheet（台股 `SHEET_ID_VERIFICATION` / 美股 `US_SHEET_ID_VERIFICATION`）
2. 切到「驗證日誌」分頁
3. 每天一行，L1~L4 各欄顯示 PASS/FAIL

也可從 GitHub Actions 的 log 查看詳細報告。

### 3.10 錯誤案例記錄

`history/` 目錄記錄系統運行中遇到的問題案例，含根因分析和修正措施。

```bash
# 查看所有案例
ls history/

# 案例命名規則: {日期}_{簡述}.md
# 例: 2026-04-07_NINE_reverse_split.md
```

### 3.11 其他維護腳本

```bash
# 匯出單一股票驗證資料（需傳入股票代號）
python scripts/export_single_stock.py <股票代號>
# 範例
python scripts/export_single_stock.py 2330

# 修復缺失的 indicator_json
python scripts/fix_missing_indicators.py
python scripts/fix_missing_indicators.py --us
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
- `deploy-site.yml`：部署前端查詢網站到 GitHub Pages（每日篩選完成後自動觸發，或手動觸發）
- `test-schedule.yml`：排程測試用（UTC 16:15）
- `export-stock.yml`：手動觸發，匯出單一股票資料

### 4.2 工作流程執行步驟

每個工作流程遵循相同模式：

```
1. Checkout repository
2. Set up Python 3.11 (pip cache)
3. Install dependencies
4. Download database from Release (gzip 壓縮檔)
5. Set up Google credentials (from Secret)
6. Initialize if needed (首次執行)
7. Run daily task:
   Step 1:   確保股票清單
   Step 2:   下載今日股價
   Step 2.5: 自動補漏歷史缺口（price_gap_filler）
   Step 3:   減資/分割偵測
   Step 4:   大盤指數下載
   Step 5:   篩選（VCP + 三線開花）
   Step 6:   匯出 Google Sheet
   Step 7:   每日自動驗證（DailyVerifier 6 項檢查）
   Step 8:   客觀驗證（ObjectiveVerifier L1~L4，結果寫入驗證 Sheet「驗證日誌」）
8. Backup database to Release (gzip 壓縮，--clobber 覆蓋)
9. Upload logs (always，30天有效)
```

> 前端部署**不在** daily workflow 內：daily.yml / us-daily.yml 沒有主動觸發 deploy-site，而是由 `deploy-site.yml` 透過 `on: workflow_run` 監聽 daily 完成事件後自動觸發。

> **美股股價完整性保護**：美股每日任務當天股價筆數低於 `MIN_PRICE_COUNT = 6500` 時視為不完整，會強制重新下載（正常交易日約 6000+ 筆），避免殘缺 DB 被錯誤跳過。

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
| 美股 daily | job 180 分鐘（Run task step 150 分鐘） |
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
| 美股每日任務 | ~60-120 分鐘（yfinance 批次 40 檔 × ~200 批、批次間隔 15 秒；job timeout 180 分） |
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
