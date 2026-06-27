# 技術架構說明書

> ZF_TrendPicking 系統架構與模組設計

## 1. 系統總覽

### 1.1 技術棧

| 項目 | 技術 |
|------|------|
| 語言 | Python 3.11 |
| ORM | SQLAlchemy 2.x（Mapped Column 語法） |
| 資料庫 | SQLite（WAL 模式） |
| 資料分析 | pandas, numpy |
| API 客戶端 | requests, yfinance |
| 排程 | GitHub Actions (cron) |
| 日誌 | loguru |
| 環境管理 | python-dotenv, venv |
| Google API | gspread, google-auth |

### 1.2 運行環境

| 環境 | 說明 |
|------|------|
| 本地開發 | macOS / Linux + Python 3.11 venv |
| CI/CD | GitHub Actions (ubuntu-latest) |
| 資料庫儲存 | GitHub Release (永久) + Artifact (90天) |

---

## 2. 分層模組架構

```
┌─────────────────────────────────────────────────┐
│                前端查詢網站                        │
│   site/index.html (Glassmorphism, lazy loading)  │
│   site/data/ (index.json + months/ + indicators/)│
├─────────────────────────────────────────────────┤
│                  主程式入口                        │
│              main.py / us_main.py                │
├─────────────────────────────────────────────────┤
│                   任務層                          │
│    tasks/daily_task.py    tasks/monthly_task.py   │
│    tasks/us_daily_task.py tasks/us_monthly_task.py│
├──────────────────────┬──────────────────────────┤
│      計算層           │        匯出層             │
│  calculators/         │    exporters/             │
│   vcp_filter.py       │     google_sheet.py       │
│   sanxian_filter.py   │     us_google_sheet.py    │
│   moving_average.py   │                           │
│   us_vcp_filter.py    │  scripts/                 │
│   us_sanxian_filter.py│   export_to_json_v2.py    │
│   us_moving_average.py│   verify_data.py          │
├──────────────────────┴──────────────────────────┤
│                   資料層                          │
│         data/sqlite_database.py (台股 SQLite)     │
│         data/us_database.py (美股 SQLite)         │
│         data/models.py / data/us_models.py       │
├─────────────────────────────────────────────────┤
│                 API 客戶端層                       │
│   api/hybrid_client.py (FinMind + yfinance)      │
│   api/finmind_client.py                          │
│   api/yfinance_client.py                         │
│   api/us_stock_client.py (USStockClientBase)     │
│   api/us_stock_client_free.py (NASDAQ FTP + yf)  │
│   api/us_stock_client_paid.py (付費版預留)        │
│   api/rate_limiter.py                            │
├─────────────────────────────────────────────────┤
│                   工具層                          │
│   utils/trading_calendar.py                      │
│   utils/us_trading_calendar.py                   │
│   utils/split_detector.py                        │
│   utils/us_split_detector.py                     │
│   utils/internal_split_detector.py               │
│   utils/price_gap_filler.py                      │
│   utils/daily_verifier.py                        │
│   utils/objective_verifier.py                    │
│   utils/performance.py                           │
├─────────────────────────────────────────────────┤
│                   設定層                          │
│   config/settings.py / config/us_settings.py     │
│   .env                                           │
└─────────────────────────────────────────────────┘
```

---

## 3. 模組詳細說明

### 3.1 主程式入口

| 檔案 | 說明 |
|------|------|
| `main.py` | 台股主程式，CLI 入口，支援 `init`、`daily`、`monthly`、`schedule`、`health`、`backfill` 命令 |
| `us_main.py` | 美股主程式，CLI 入口，命令結構與台股相同但使用美股設定和元件 |

支援參數：
- `target_date`：指定目標日期（YYYY-MM-DD）
- `--force`：強制執行（忽略假日判斷）

### 3.2 設定模組 (`config/`)

| 檔案 | 說明 |
|------|------|
| `config/settings.py` | 台股設定：資料庫路徑、FinMind API、Google Sheet IDs、技術指標參數、重試設定、排程設定 |
| `config/us_settings.py` | 美股設定：獨立資料庫、yfinance 批次設定、NASDAQ FTP、美股 Sheet IDs、`get_us_client()` 工廠方法 |

### 3.3 API 客戶端層 (`api/`)

| 檔案 | 類別 | 職責 |
|------|------|------|
| `api/finmind_client.py` | `FinMindClient` | FinMind API 存取：股票清單、股價、大盤指數。含限流（600次/hr）和 yfinance 補齊功能 |
| `api/yfinance_client.py` | `YFinanceClient` | 免費 yfinance 客戶端，含自適應批次下載器。支援台股代號格式（.TW/.TWO），從 TWSE/TPEX 爬取股票清單 |
| `api/hybrid_client.py` | `HybridClient` | **台股混合客戶端**。主要來源 FinMind，備援 yfinance。兩種備援處理：完整備援（主源失敗整批改 yfinance）與部分補齊（主源部分成功，只補 missing），並自動依條件切換 |
| `api/us_stock_client.py` | `USStockClientBase` | 美股 API 抽象基底類別，定義統一介面 |
| `api/us_stock_client_free.py` | `USStockClientFree` | 美股免費版（NASDAQ FTP + yfinance）。含批次下載、多執行緒 sector/industry 取得 |
| `api/us_stock_client_paid.py` | `USStockClientPolygon` 等 4 類別 | 付費版框架，支援 Polygon.io、EODHD、Twelve Data、Alpha Vantage（`USStockClientPolygon` / `USStockClientEODHD` / `USStockClientTwelveData` / `USStockClientAlphaVantage`） |
| `api/rate_limiter.py` | `RateLimiter`, `RetryHandler` | Token Bucket 限流 + 5XX/429 自動重試 |

#### HybridClient 備援機制流程

```
get_stock_price() 呼叫
  ├── FinMind 取得股價
  │     ├── 成功 → 檢查筆數是否完整
  │     │     ├── 完整 → 回傳
  │     │     └── 不完整 → yfinance 補齊缺失部分
  │     └── 失敗 → yfinance 完整備援
  └── 回傳合併結果
```

### 3.4 資料庫層 (`data/`)

| 檔案 | 類別 | 職責 |
|------|------|------|
| `data/models.py` | `StockInfo`, `DailyPrice`, `MarketIndex`, `FilterResult` | 台股 SQLAlchemy ORM 模型（`Base`） |
| `data/us_models.py` | `USStockInfo`, `USDailyPrice`, `USMarketIndex`, `USFilterResult`, `USAnomalyWhitelist` | 美股 SQLAlchemy ORM 模型（`USBase`，獨立） |
| `data/sqlite_database.py` | `SQLiteDatabase` | 台股資料庫操作（runtime 實際使用）：CRUD、批次寫入、UPSERT、WAL 模式 |
| `data/database.py` | `Database` | 舊版 PostgreSQL 實作，保留相容性、runtime 不再使用 |
| `data/us_database.py` | `USSQLiteDatabase` | 美股資料庫操作：WAL 模式、獨立 CRUD |

### 3.5 計算模組 (`calculators/`)

| 檔案 | 類別 | 職責 |
|------|------|------|
| `calculators/moving_average.py` | `MovingAverageCalculator` | 台股均線計算：SMA、高低點、報酬率、次高價、零價修正 |
| `calculators/us_moving_average.py` | `USMovingAverageCalculator` | 美股均線計算（邏輯相同，獨立模組） |
| `calculators/vcp_filter.py` | `VCPFilter` | 台股 VCP 篩選：強勢清單 + 新高清單 |
| `calculators/us_vcp_filter.py` | `USVCPFilter` | 美股 VCP 篩選 |
| `calculators/sanxian_filter.py` | `SanxianFilter` | 台股三線開花篩選 |
| `calculators/us_sanxian_filter.py` | `USSanxianFilter` | 美股三線開花篩選 |

### 3.6 任務模組 (`tasks/`)

| 檔案 | 類別 | 職責 |
|------|------|------|
| `tasks/daily_task.py` | `DailyTask` | 台股每日任務：抓股價 → **補漏** → 除權息/減資偵測（比對未調整價 vs FinMind 還原權息價，差異 >1% 重抓）→ 抓大盤 → 篩選 → 匯出 Sheet → 驗證 |
| `tasks/us_daily_task.py` | `USDailyTask` | 美股每日任務：抓股價 → **補漏** → **分割偵測** → **內部分割偵測** → 抓大盤 → 篩選 → 匯出 Sheet → 每日驗證 → 客觀驗證 |
| `tasks/monthly_task.py` | `MonthlyTask` | 台股每月任務：更新股票清單 → 匯出主檔 Sheet |
| `tasks/us_monthly_task.py` | `USMonthlyTask` | 美股每月任務：更新股票清單 → 補充 sector/industry → 匯出主檔 Sheet |

### 3.7 匯出模組 (`exporters/`)

| 檔案 | 類別 | 職責 |
|------|------|------|
| `exporters/google_sheet.py` | `GoogleSheetExporter` | 台股 Google Sheet 匯出：主檔、VCP、三線開花、驗證資料。支援分頁排序 |
| `exporters/us_google_sheet.py` | `USGoogleSheetExporter` | 美股 Google Sheet 匯出（獨立 Sheet IDs） |

### 3.8 工具模組 (`utils/`)

| 檔案 | 類別 | 職責 |
|------|------|------|
| `utils/trading_calendar.py` | `TradingCalendar` | 台股交易日曆（2024-2026 國定假日），判斷交易日、取得前/後交易日 |
| `utils/us_trading_calendar.py` | `USMarketCalendar` | 美股交易日曆（2024-2026 聯邦假日+提前收盤日） |
| `utils/split_detector.py` | `SplitDetector` | 台股除權息偵測：用 FinMind 還原價比對 DB 前日價格 |
| `utils/us_split_detector.py` | `USSplitDetector` | 美股分割/合股偵測：比對 DB 與 yfinance 歷史價格，自動標記需重新下載的股票 |
| `utils/internal_split_detector.py` | `detect_and_fix_internal_splits()` | 第三層分割偵測：掃描 DB 相鄰收盤價跳動（>1.5x/<0.67x），刪除重抓 365 天，再跳動則寫入 `us_anomaly_whitelist` 白名單 |
| `utils/price_gap_filler.py` | `fill_price_gaps()` | 股價缺漏自動補齊：用基準股票建交易日曆，逐股比對並從 yfinance 下載缺日 |
| `utils/daily_verifier.py` | `DailyVerifier` | 每日自動驗證：檢查股價筆數、篩選結果、大盤報酬等 6 項指標 |
| `utils/objective_verifier.py` | `ObjectiveVerifier` | 四層客觀驗證：L1 價格準確性、L2 獨立重算、L3 Sheet 回讀、L4 歷史一致性。結果寫入驗證 Sheet「驗證日誌」分頁 |
| `utils/performance.py` | `PerformanceMonitor` | 效能監控裝飾器，統計函數執行時間 |

### 3.9 前端查詢網站 (`site/`)

| 檔案 | 說明 |
|------|------|
| `site/index.html` | 前端單頁應用（Glassmorphism 風格），含搜尋、日期查詢、排序、新/舊標記 |
| `site/data/index.json` | 股票主檔 + 月份清單 + 資料範圍（`first_date`, `last_date`） |
| `site/data/months/{YYYY-MM}.json` | 各月篩選結果（lazy load） |
| `site/data/indicators/{YYYY-MM}.json` | 指標 tooltip 資料（點擊 tag 時載入） |

前端架構特性：
- **拆分 JSON**：避免一次載入 88MB，改為 `index.json`（~1.2MB）+ 按月 lazy load
- **反向索引**：`STOCK_INDEX` 實現 O(1) 股票搜尋
- **debounce**：搜尋輸入 300ms 防抖 + 50 筆結果限制
- **新/舊標記**：跨類型比較前一交易日（VCP + 三線開花合併）
- **排序功能**：綜合、新股優先、20日漲幅、突破差距

### 3.10 維護腳本 (`scripts/`)

| 檔案 | 用途 |
|------|------|
| `scripts/export_to_json_v2.py` | 從 DB 匯出拆分 JSON 到 `site/data/`（index + months + indicators） |
| `scripts/export_to_json.py` | 舊版單檔匯出（已棄用） |
| `scripts/export_single_stock.py` | 匯出單一股票完整驗證資料到 Google Sheet |
| `scripts/fix_zero_prices_in_db.py` | 修復資料庫中 close_price=0 的異常資料 |
| `scripts/rebuild_price_data.py` | 使用 FinMind 重建台股價格歷史 |
| `scripts/reexport_all_dates.py` | 重新匯出篩選結果到 Google Sheet（支援 `--from-db` 從 DB 直接讀取） |
| `scripts/backfill_all_trading_days.py` | 台股：補齊所有交易日的篩選結果（非僅星期五） |
| `scripts/backfill_all_trading_days_us.py` | 美股：補齊所有交易日的篩選結果 |
| `scripts/backfill_fridays.py` | 台股：補齊星期五的篩選結果（含 `run_filters_for_date`） |
| `scripts/backfill_fridays_us.py` | 美股：補齊星期五的篩選結果 |
| `scripts/backfill_tw_prices.py` | 台股：股價回補（v1） |
| `scripts/backfill_tw_prices_v2.py` | 台股：股價回補（v2，改進版） |
| `scripts/backfill_us_prices.py` | 美股：回溯下載歷史股價至 2024-05 |
| `scripts/fix_missing_indicators.py` | 修復缺失的 `indicator_json`（台股 `--tw` / 美股 `--us`） |
| `scripts/verify_data.py` | 4 層資料驗證：完整性、值合理性、新/舊邏輯、篩選準確度 |
| `scripts/backfill_missing_prices.py` | 手動補齊缺漏股價（台股 `--tw` / 美股），支援 `--dry-run` |
| `scripts/verify_stock_gaps.py` | 資料完整性驗證：比對基準交易日曆，檢查缺日是否影響計算窗口 |

### 3.11 錯誤案例記錄 (`history/`)

記錄系統運行中遇到的問題、根因分析、修正措施，供未來回顧。

| 檔案 | 說明 |
|------|------|
| `history/README.md` | 案例索引 + 命名規則 |
| `history/2026-04-07_NINE_reverse_split.md` | NINE 反向合股導致 72900% 異常報酬率 |
| `history/2026-04-07_us_rate_limit.md` | yfinance rate limit 導致 2000 檔漏抓 |
| `history/2026-04-03_tw_price_gaps.md` | 台股股價缺漏導致 15 檔均線失真 |

---

## 4. 資料流圖

### 4.1 每日任務流程

```
[排程觸發 (GitHub Actions cron)]
          │
          ▼
[檢查是否為交易日] ──否──> [跳過執行]
          │是
          ▼
[下載資料庫 from Release]
          │
          ▼
[Step 1: 確保股票清單]
          │
          ▼
[Step 2: 抓取當日個股股價] ──> [寫入 daily_price 表]
          │
          ▼
[Step 2.5: 補漏歷史缺口] ──> [用基準股票建交易日曆，從 yfinance 補齊]
          │
          ▼
[Step 3: 減資/分割偵測] ──> [偵測到則重新下載完整歷史]
          │
          ▼
[Step 4: 抓取大盤指數] ──> [寫入 market_index 表]
          │
          ▼
┌─────────┴─────────┐
│                   │
▼                   ▼
[Step 5: VCP 篩選] [Step 5: 三線開花篩選]
│                   │
▼                   ▼
[Step 6: 匯出 Sheet]
          │
          ▼
[Step 7: 每日自動驗證（DailyVerifier 6 項檢查）]
          │
          ▼
[Step 8: 客觀驗證（ObjectiveVerifier L1~L4，結果寫入驗證 Sheet）]
          │
          ▼
[備份資料庫到 Release]
          │
          ▼
[觸發 Deploy Site workflow]
          │
          ▼
[export_to_json_v2.py → 產生拆分 JSON]
          │
          ▼
[部署到 GitHub Pages]
```

### 4.2 美股每日任務額外流程：分割偵測 + 內部分割偵測

**Step 2.6：分割偵測（比對 yfinance）**

```
[下載當日+前日股價]
          │
          ▼
[比對 DB 舊值 vs yfinance 新值]
          │
          ▼
[價格變動 > 閾值？]
    │是         │否
    ▼           ▼
[標記為分割]   [正常處理]
    │
    ▼
[重新下載該股 365 天歷史]
    │
    ▼
[覆寫 DB 中的舊資料]
```

**Step 2.7：內部分割偵測（DB 自身相鄰價格跳動，補足前兩層盲點）**

```
[掃描 DB 相鄰收盤價跳動 (>1.5x / <0.67x)]
          │
          ▼
[過濾 us_anomaly_whitelist 白名單] ──白名單──> [跳過]
          │ 非白名單
          ▼
[DELETE 該股全部歷史 + 重抓 365 天]
          │
          ▼
[再次掃描相鄰跳動]
    │仍跳動        │已正常
    ▼             ▼
[真實波動 → 寫入白名單]  [修正完成]
```

### 4.3 每月任務流程

```
[排程觸發 (每月 1 日)]
          │
          ▼
[下載資料庫 from Release]
          │
          ▼
[抓取最新股票清單]
          │
          ▼
[差集比對 → 新增/更新 stock_info 表]
          │
          ▼
[匯出公司主檔 Sheet]
          │
          ▼
[備份資料庫到 Release]
```

---

## 5. 台股 vs 美股差異對照表

| 面向 | 台股 | 美股 |
|------|------|------|
| **主程式** | `main.py` | `us_main.py` |
| **設定檔** | `config/settings.py` | `config/us_settings.py` |
| **資料庫** | `data/zf_trend.db` | `data/zf_trend_us.db` |
| **ORM Base** | `Base` | `USBase`（獨立） |
| **API 客戶端** | `HybridClient`（FinMind + yfinance） | `USStockClientFree`（NASDAQ FTP + yfinance） |
| **股票清單來源** | FinMind `TaiwanStockInfo` | NASDAQ FTP `nasdaqtraded.txt` |
| **股價來源** | FinMind（主）+ yfinance（備援） | yfinance（唯一） |
| **大盤指數** | 加權指數 TAIEX（單一） | S&P 500 + 道瓊 + NASDAQ 三大指數（但 VCP 篩選報酬率僅採 S&P 500 ^GSPC） |
| **股價計算** | 使用**未調整股價**（與券商一致） | 使用**調整後股價**（adj_close） |
| **股票數量** | ~1,700 檔 | ~8,000 檔 |
| **批次策略** | 逐檔查詢（FinMind 限制） | 批次 40 檔、間隔 15 秒、序列下載（`US_MAX_WORKERS=2` 僅用於 sector/industry 抓取） |
| **分割偵測** | 有（`SplitDetector`，FinMind 還原價） | 有（`USSplitDetector`，yfinance 比對） |
| **異常白名單表** | 無 | `us_anomaly_whitelist`（內部分割偵測白名單） |
| **股價 adj_close 欄** | 無 | 有（DB 儲存還原後收盤價） |
| **產業分類補充** | FinMind 已包含 | 需額外從 yfinance 取得 |
| **代號格式** | 純數字（如 2330） | 英文（如 AAPL），最長 20 字元 |
| **Sheet 數量** | 共用公司主檔 + 2 個篩選 Sheet | 3 個獨立 Sheet |
| **日誌檔** | `logs/zf_trend.log` | `logs/zf_trend_us.log` |
| **Release tag** | `db-backup` | `us-db-backup` |
| **限流** | FinMind 600 次/小時 | yfinance 無官方限制（自律控速） |

---

## 6. 參考文件

- [需求規格](./01-requirements-spec.md)
- [資料規格](./03-data-spec.md)
- [演算法規格](./04-algorithm-spec.md)
- [操作指南](./05-operations-guide.md)
