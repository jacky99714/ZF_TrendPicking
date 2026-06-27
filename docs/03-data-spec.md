# 資料規格說明書

> ZF_TrendPicking 資料庫 Schema、外部 API、Google Sheet 欄位定義

## 1. 資料庫 Schema

系統使用兩個獨立的 SQLite 資料庫，透過 SQLAlchemy ORM 管理。

### 1.1 台股資料庫 (`data/zf_trend.db`)

#### stock_info 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| stock_id | String(10) | PK | 股票代號（如 2330） |
| stock_name | String(50) | NOT NULL | 股票名稱 |
| industry_category | String(50) | nullable | 產業分類1 |
| industry_category2 | String(50) | nullable | 產業分類2 |
| stock_type | String(20) | nullable | 股票類型（twse/tpex） |
| created_at | DateTime | server_default=now() | 建立時間 |
| updated_at | DateTime | server_default=now(), onupdate=now() | 更新時間 |

#### daily_price 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| id | Integer | PK, autoincrement | 自動遞增 ID |
| stock_id | String(10) | NOT NULL, indexed | 股票代號 |
| date | Date | NOT NULL, indexed | 交易日期 |
| open_price | Numeric(12,4) | nullable | 開盤價 |
| high_price | Numeric(12,4) | nullable | 最高價 |
| low_price | Numeric(12,4) | nullable | 最低價 |
| close_price | Numeric(12,4) | nullable | 收盤價 |
| volume | BigInteger | nullable | 成交量 |
| created_at | DateTime | server_default=now() | 建立時間 |

**約束**：
- `UNIQUE(stock_id, date)` — 每檔股票每日僅一筆
- `INDEX(stock_id, date)` — 複合索引加速查詢
- `INDEX(date)` — 單一日期索引

#### market_index 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| id | Integer | PK, autoincrement | 自動遞增 ID |
| date | Date | NOT NULL, UNIQUE, indexed | 交易日期 |
| taiex | Numeric(12,4) | nullable | 加權指數 |
| created_at | DateTime | server_default=now() | 建立時間 |

#### filter_result 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| id | Integer | PK, autoincrement | 自動遞增 ID |
| filter_date | Date | NOT NULL, indexed | 篩選日期 |
| filter_type | String(20) | NOT NULL | 篩選類型（vcp / sanxian） |
| stock_id | String(10) | NOT NULL | 股票代號 |
| stock_name | String(50) | NOT NULL | 股票名稱 |
| industry_category | String(50) | nullable | 產業分類 |
| return_20d | Numeric(8,4) | nullable | 近20日漲幅（VCP 用） |
| is_strong_list | Boolean | nullable | 強勢清單（VCP 用） |
| is_new_high_list | Boolean | nullable | 新高清單（VCP 用） |
| today_price | Numeric(12,4) | nullable | 今日股價（三線開花用） |
| second_high_55d | Numeric(12,4) | nullable | 55日次高價（三線開花用） |
| gap_ratio | Numeric(8,4) | nullable | 差距比例（三線開花用） |
| indicator_json | Text | nullable | 完整指標 JSON（含 MA、高低點、報酬率等，供前端 tooltip 顯示） |
| created_at | DateTime | server_default=now() | 建立時間 |

**索引**：`INDEX(filter_date, filter_type)` — 依日期+類型快速查詢

**indicator_json 內容範例**（VCP）：
```json
{"close": 96.5, "ma50": 95.2, "ma150": 90.1, "ma200": 88.5, "ma200_slope": 0.015,
 "return_20d": 0.065, "market_return": 0.02, "high_5d": 97.0, "high_260d": 99.1}
```

**indicator_json 內容範例**（三線開花）：
```json
{"close": 102.5, "ma8": 101.2, "ma21": 99.8, "ma55": 97.3,
 "high_55d": 105.0, "second_high": 104.1}
```

---

### 1.2 美股資料庫 (`data/zf_trend_us.db`)

#### us_stock_info 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| stock_id | String(20) | PK | 股票代號（如 AAPL, MSFT） |
| stock_name | String(200) | NOT NULL | 公司名稱 |
| exchange | String(20) | nullable | 交易所（NYSE/NASDAQ/AMEX） |
| sector | String(100) | nullable | 產業分類 |
| industry | String(100) | nullable | 細分產業 |
| market_cap | Numeric(20,2) | nullable | 市值 |
| etf_flag | String(1) | nullable | 是否為 ETF（Y/N） |
| created_at | DateTime | server_default=now() | 建立時間 |
| updated_at | DateTime | server_default=now(), onupdate=now() | 更新時間 |

#### us_daily_price 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| id | Integer | PK, autoincrement | 自動遞增 ID |
| stock_id | String(20) | NOT NULL, indexed | 股票代號 |
| date | Date | NOT NULL, indexed | 交易日期 |
| open_price | Numeric(12,4) | nullable | 開盤價 |
| high_price | Numeric(12,4) | nullable | 最高價 |
| low_price | Numeric(12,4) | nullable | 最低價 |
| close_price | Numeric(12,4) | nullable | 收盤價 |
| volume | BigInteger | nullable | 成交量 |
| adj_close | Numeric(12,4) | nullable | 調整後收盤價 |
| created_at | DateTime | server_default=now() | 建立時間 |

**約束**：
- `UNIQUE(stock_id, date)`
- `INDEX(stock_id, date)`
- `INDEX(date)`

#### us_market_index 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| id | Integer | PK, autoincrement | 自動遞增 ID |
| date | Date | NOT NULL, UNIQUE, indexed | 交易日期 |
| sp500 | Numeric(12,4) | nullable | S&P 500 指數 |
| dow_jones | Numeric(12,4) | nullable | 道瓊工業指數 |
| nasdaq | Numeric(12,4) | nullable | NASDAQ 指數 |
| created_at | DateTime | server_default=now() | 建立時間 |

#### us_filter_result 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| id | Integer | PK, autoincrement | 自動遞增 ID |
| filter_date | Date | NOT NULL, indexed | 篩選日期 |
| filter_type | String(20) | NOT NULL | 篩選類型（vcp / sanxian） |
| stock_id | String(20) | NOT NULL | 股票代號 |
| stock_name | String(200) | NOT NULL | 公司名稱 |
| exchange | String(20) | nullable | 交易所 |
| sector | String(100) | nullable | 產業分類 |
| return_20d | Numeric(8,4) | nullable | 近20日漲幅（VCP 用） |
| is_strong_list | Boolean | nullable | 強勢清單（VCP 用） |
| is_new_high_list | Boolean | nullable | 新高清單（VCP 用） |
| today_price | Numeric(12,4) | nullable | 今日股價（三線開花用） |
| second_high_55d | Numeric(12,4) | nullable | 55日次高價（三線開花用） |
| gap_ratio | Numeric(8,4) | nullable | 差距比例（三線開花用） |
| indicator_json | Text | nullable | 完整指標 JSON（同台股） |
| created_at | DateTime | server_default=now() | 建立時間 |

**索引**：`INDEX(filter_date, filter_type)`

#### us_anomaly_whitelist 表

| 欄位 | 型別 | 約束 | 說明 |
|------|------|------|------|
| id | Integer | PK, autoincrement | 自動遞增 ID |
| stock_id | String(20) | NOT NULL, indexed | 股票代號 |
| anomaly_date | Date | NOT NULL | 異常發生的日期（價格跳動的日期） |
| prev_close | Numeric(12,4) | nullable | 前一日收盤價 |
| today_close | Numeric(12,4) | nullable | 當日收盤價 |
| ratio | Numeric(10,4) | nullable | 跳動比值 today/prev |
| reason | Text | nullable | 加入白名單原因 |
| created_at | DateTime | server_default=now() | 建立時間 |

**約束**：
- `UNIQUE(stock_id, anomaly_date)`
- `INDEX(stock_id)`

**用途**：記錄已驗證為「真實價格波動」的股票/日期，避免內部分割偵測重複觸發（例：BIRD 真實 6x 暴漲）。

---

### 1.3 SQLite 最佳化設定

兩個 SQLite 資料庫（台股 `SQLiteDatabase`、美股 `USSQLiteDatabase`）皆啟用以下 SQLite 最佳化：

```sql
PRAGMA journal_mode = WAL;    -- Write-Ahead Logging，提升併發讀寫效能
PRAGMA cache_size = -64000;   -- 64MB cache
PRAGMA synchronous = NORMAL;  -- 平衡安全性與效能
```

---

## 2. 外部 API 規格

### 2.1 FinMind API（台股）

| 項目 | 值 |
|------|-----|
| Base URL | `https://api.finmindtrade.com/api/v4/data` |
| 認證 | Token-based（環境變數 `FINMIND_TOKEN`） |
| 限流 | 免費帳號 600 次/小時 |
| 限流機制 | 固定間隔節流（`min_interval = 3600/calls_per_hour`，類別名 `RateLimiter`，`api/rate_limiter.py`） |

**使用的 Dataset**：

| Dataset | 用途 | 主要欄位 |
|---------|------|---------|
| `TaiwanStockInfo` | 取得台股上市櫃清單 | stock_id, stock_name, industry_category, type |
| `TaiwanStockPrice` | 取得個股日線資料 | date, stock_id, open, max, min, close, Trading_Volume |
| `TaiwanStockPrice`（`data_id="TAIEX"`） | 取得大盤指數（主要來源） | date, close |
| `TaiwanStockTotalReturnIndex` | 取得大盤指數（fallback，TAIEX 失敗時改用報酬指數） | date, price |

### 2.2 yfinance（台股備援+美股主要）

| 項目 | 值 |
|------|-----|
| 套件 | `yfinance` (Python) |
| 費用 | 免費（Yahoo Finance 非官方 API） |
| 限制 | 無官方限流，但建議自律控速 |

**台股代號格式**：
- 上市：`{代號}.TW`（如 `2330.TW`）
- 上櫃：`{代號}.TWO`（如 `6510.TWO`）

**美股代號格式**：
- 直接使用原始代號（如 `AAPL`, `MSFT`）

**批次下載**：
- 使用 `yfinance.download()` 批次下載多檔股票
- 美股設定：每批 40 檔、間隔 15 秒；workers=2（僅用於 sector/industry 抓取，價格下載由 yfinance 內部 threads 處理）
- 台股備援：自適應批次大小（根據錯誤率動態調整）

**auto_adjust 設定**：
- 台股：FinMind 路徑（`TaiwanStockPrice`）走**未調整股價**，與券商報價一致；yfinance 備援則為 `auto_adjust=True`（調整後股價）
- 美股：`auto_adjust=False`，保留**未調整 OHLC**，並額外存 Adj Close 至 `adj_close` 欄

### 2.3 NASDAQ FTP（美股股票清單）

| 項目 | 值 |
|------|-----|
| URL | `ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqtraded.txt` |
| 格式 | 管線分隔文字檔（`|` delimiter） |
| 更新頻率 | 每日更新 |

**主要欄位**：

| 欄位 | 說明 |
|------|------|
| Symbol | 股票代號 |
| Security Name | 公司名稱 |
| Listing Exchange | 上市交易所（N=NYSE, Q=NASDAQ, A=AMEX, etc.） |
| ETF | 是否為 ETF（Y/N） |
| Test Issue | 是否為測試代號（Y/N） |

**過濾邏輯**：
- 僅保留 `ETF == "N"` 且 `Test Issue == "N"` 的項目
- 代號需符合 regex `^[A-Z0-9./-]+$`（保留如 `BRK.B` 的正常特殊字元，排除測試股票）
- 代號長度 ≤ 10（排除權證或特殊商品）
- 排除空代號與檔案結尾行

### 2.4 Google Sheets API

| 項目 | 值 |
|------|-----|
| 認證方式 | Service Account (JSON key) |
| 套件 | `gspread` + `google-auth` |
| Scope | `https://www.googleapis.com/auth/spreadsheets` |
| 重試 | API 錯誤自動重試（gspread 內建） |
| 憑證路徑 | 環境變數 `GOOGLE_CREDENTIALS_PATH`（預設 `credentials.json`） |

---

## 3. Google Sheet 輸出欄位規格

### 3.1 公司主檔 — 台股分頁

| 欄位位置 | 欄位名稱 | 來源 |
|---------|---------|------|
| A | 代號 | `stock_info.stock_id` |
| B | 股名 | `stock_info.stock_name` |
| C | 公司名 | `stock_info.stock_name`（同股名） |
| D | 產業分類1 | `stock_info.industry_category` |
| E | 產業分類2 | `stock_info.industry_category2` |
| F | 產品組合 | 固定 `-`（FinMind 無此資料） |

### 3.2 公司主檔 — 美股分頁

| 欄位位置 | 欄位名稱 | 來源 |
|---------|---------|------|
| A | 代號 | `us_stock_info.stock_id` |
| B | 股名 | `us_stock_info.stock_name` |
| C | 公司名 | `us_stock_info.stock_name` |
| D | 產業分類1 | `us_stock_info.sector` |
| E | 產業分類2 | `us_stock_info.industry` |
| F | 產品組合 | 固定 `-` |

### 3.3 VCP 篩選結果

| 欄位位置 | 欄位名稱 | 來源 | 格式 |
|---------|---------|------|------|
| A | 代號 | stock_id | 文字 |
| B | 股名 | stock_name | 文字 |
| C | 公司名 | company_name | 文字 |
| D | 產業分類1 | industry_category | 文字 |
| E | 產業分類2 | industry_category2 | 文字 |
| F | 產品組合 | product_mix | 文字 |
| G | 近20日股價漲幅 | return_20d | 百分比 |
| H | 強勢清單 | is_strong_list | `O` 或空白 |
| I | 新高清單 | is_new_high_list | `O` 或空白 |

**排序**：新股優先（白色背景）→ 舊股（灰色背景），每組內依「近20日股價漲幅」降冪排序

**新/舊股判定**：與前一交易日同類型（VCP 比 VCP）的篩選結果比較

### 3.4 三線開花篩選結果

| 欄位位置 | 欄位名稱 | 來源 | 格式 |
|---------|---------|------|------|
| A | 代號 | stock_id | 文字 |
| B | 股名 | stock_name | 文字 |
| C | 公司名 | company_name | 文字 |
| D | 產業分類1 | industry_category | 文字 |
| E | 產業分類2 | industry_category2 | 文字 |
| F | 產品組合 | product_mix | 文字 |
| G | 今日股價 | today_price | 數值 |
| H | 55日內次高價 | second_high_55d | 數值 |
| I | 差距比例 | gap_ratio | 百分比 |

**排序**：新股優先（白色背景）→ 舊股（灰色背景），每組內依「差距比例」降冪排序

### 3.5 驗證日誌（驗證 Sheet 固定分頁）

客觀驗證（`ObjectiveVerifier`）每日自動追加一行到驗證 Sheet 的「驗證日誌」分頁：

| 欄位位置 | 欄位名稱 | 說明 |
|---------|---------|------|
| A | 日期 | 篩選日期（YYYY-MM-DD） |
| B | L1 價格 | DB 收盤價 vs yfinance 獨立抓取（抽 15 檔，容差 2%） |
| C | L2 重算 | 從 yfinance 原始資料獨立計算 MA/條件（抽 5 檔） |
| D | L3 Sheet | 讀回 Google Sheet 比對行數和 stock_id |
| E | L4 歷史 | 篩選數量 vs 20 天平均（偏差 > 50% 告警） |
| F | 結論 | PASS / FAIL |
| G | 詳情 | JSON 格式錯誤詳情 |
| H | 市場 | 台股 / 美股 |

---

## 4. 環境變數完整清單

### 4.1 台股系統

| 環境變數 | 必要 | 預設值 | 說明 |
|---------|------|-------|------|
| `FINMIND_TOKEN` | 是 | (空) | FinMind API Token |
| `SQLITE_DB_PATH` | 否 | `data/zf_trend.db` | SQLite 資料庫路徑 |
| `GOOGLE_CREDENTIALS_PATH` | 是 | `credentials.json` | Google Service Account 憑證 |
| `SHEET_ID_COMPANY_MASTER` | 是 | (空) | 公司主檔 Sheet ID |
| `SHEET_ID_TW_VCP` | 是 | (空) | 台股 VCP Sheet ID |
| `SHEET_ID_TW_SANXIAN` | 是 | (空) | 台股三線開花 Sheet ID |
| `SHEET_ID_VERIFICATION` | 否 | (空) | 驗證用 Sheet ID |
| `MAX_RETRIES` | 否 | `3` | 最大重試次數 |
| `LOG_LEVEL` | 否 | `INFO` | 日誌等級 |
| `GITHUB_ACTIONS` | 自動 | `false` | 是否在 GitHub Actions 環境 |

### 4.2 美股系統

| 環境變數 | 必要 | 預設值 | 說明 |
|---------|------|-------|------|
| `US_SQLITE_DB_PATH` | 否 | `data/zf_trend_us.db` | 美股 SQLite 資料庫路徑 |
| `US_DATA_PROVIDER` | 否 | `free` | 資料來源（free/polygon/eodhd/twelvedata） |
| `GOOGLE_CREDENTIALS_PATH` | 是 | `credentials.json` | Google Service Account 憑證（與台股共用） |
| `US_SHEET_ID_COMPANY_MASTER` | 是 | (空) | 美股公司主檔 Sheet ID |
| `US_SHEET_ID_VCP` | 是 | (空) | 美股 VCP Sheet ID |
| `US_SHEET_ID_SANXIAN` | 是 | (空) | 美股三線開花 Sheet ID |
| `US_SHEET_ID_VERIFICATION` | 否 | (空) | 美股驗證用 Sheet ID |
| `US_BATCH_SIZE` | 否 | `40` | 批次下載每批股票數 |
| `US_BATCH_INTERVAL` | 否 | `15` | 批次間隔（秒） |
| `US_MAX_WORKERS` | 否 | `2` | 平行 worker 數（用於 sector/industry 抓取） |
| `US_MAX_RETRIES` | 否 | `3` | 最大重試次數 |
| `US_LOG_LEVEL` | 否 | `INFO` | 日誌等級 |

### 4.3 付費 API（預留）

| 環境變數 | 必要 | 說明 |
|---------|------|------|
| `US_POLYGON_API_KEY` | 否 | Polygon.io API Key |
| `US_EODHD_API_KEY` | 否 | EODHD API Key |
| `US_TWELVEDATA_API_KEY` | 否 | Twelve Data API Key |

### 4.4 GitHub Actions Secrets

| Secret | 用途 |
|--------|------|
| `GITHUB_TOKEN` | 自動提供，用於 Release 備份 |
| `GOOGLE_CREDENTIALS_JSON` | Google Service Account JSON 完整內容 |
| `FINMIND_TOKEN` | FinMind API Token |
| `SHEET_ID_COMPANY_MASTER` | 公司主檔 Sheet ID |
| `SHEET_ID_TW_VCP` | 台股 VCP Sheet ID |
| `SHEET_ID_TW_SANXIAN` | 台股三線開花 Sheet ID |
| `SHEET_ID_VERIFICATION` | 台股驗證 Sheet ID |
| `US_SHEET_ID_COMPANY_MASTER` | 美股公司主檔 Sheet ID |
| `US_SHEET_ID_VCP` | 美股 VCP Sheet ID |
| `US_SHEET_ID_SANXIAN` | 美股三線開花 Sheet ID |
| `US_SHEET_ID_VERIFICATION` | 美股驗證 Sheet ID |

---

## 5. 參考文件

- [需求規格](./01-requirements-spec.md)
- [技術架構](./02-architecture.md)
- [演算法規格](./04-algorithm-spec.md)
- [操作指南](./05-operations-guide.md)
