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
│   us_vcp_filter.py    │                           │
│   us_sanxian_filter.py│                           │
│   us_moving_average.py│                           │
├──────────────────────┴──────────────────────────┤
│                   資料層                          │
│         data/database.py (台股 SQLite)            │
│         data/us_database.py (美股 SQLite)         │
│         data/models.py / data/us_models.py       │
├─────────────────────────────────────────────────┤
│                 API 客戶端層                       │
│   api/hybrid_client.py (FinMind + yfinance)      │
│   api/finmind_client.py                          │
│   api/yfinance_client.py                         │
│   api/us_stock_client_free.py (NASDAQ FTP + yf)  │
│   api/rate_limiter.py                            │
├─────────────────────────────────────────────────┤
│                   工具層                          │
│   utils/trading_calendar.py                      │
│   utils/us_trading_calendar.py                   │
│   utils/us_split_detector.py                     │
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
| `api/hybrid_client.py` | `HybridClient` | **台股混合客戶端**。主要來源 FinMind，備援 yfinance。支援三種模式：完整備援、部分補齊、自動切換 |
| `api/us_stock_client.py` | `USStockClientBase` | 美股 API 抽象基底類別，定義統一介面 |
| `api/us_stock_client_free.py` | `USStockClientFree` | 美股免費版（NASDAQ FTP + yfinance）。含批次下載、多執行緒 sector/industry 取得 |
| `api/us_stock_client_paid.py` | (預留) | 付費版框架，支援 Polygon.io、EODHD、Twelve Data |
| `api/rate_limiter.py` | `TokenBucketRateLimiter`, `RetryHandler` | Token Bucket 限流 + 5XX/429 自動重試 |

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
| `data/us_models.py` | `USStockInfo`, `USDailyPrice`, `USMarketIndex`, `USFilterResult` | 美股 SQLAlchemy ORM 模型（`USBase`，獨立） |
| `data/database.py` | `Database` | 台股資料庫操作：CRUD、批次寫入、UPSERT |
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
| `tasks/daily_task.py` | `DailyTask` | 台股每日任務：抓股價 → 抓大盤 → VCP 篩選 → 三線開花篩選 → 匯出 Sheet |
| `tasks/us_daily_task.py` | `USDailyTask` | 美股每日任務：抓股價 → **分割偵測** → VCP 篩選 → 三線開花篩選 → 匯出 Sheet |
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
| `utils/us_split_detector.py` | `USSplitDetector` | 美股分割/合股偵測：比對 DB 與 yfinance 歷史價格，自動標記需重新下載的股票 |
| `utils/performance.py` | `PerformanceMonitor` | 效能監控裝飾器，統計函數執行時間 |

### 3.9 維護腳本 (`scripts/`)

| 檔案 | 用途 |
|------|------|
| `scripts/export_single_stock.py` | 匯出單一股票完整驗證資料到 Google Sheet |
| `scripts/fix_zero_prices_in_db.py` | 修復資料庫中 close_price=0 的異常資料 |
| `scripts/rebuild_price_data.py` | 使用 FinMind 重建台股價格歷史 |
| `scripts/reexport_all_dates.py` | 重新計算並匯出所有日期的篩選結果（支援 backfill） |

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
[抓取當日個股股價] ──────> [寫入 daily_price 表]
          │
          ▼
[抓取大盤指數] ──────────> [寫入 market_index 表]
          │
          ▼
┌─────────┴─────────┐
│                   │
▼                   ▼
[VCP 篩選]       [三線開花篩選]
│                   │
▼                   ▼
[匯出 VCP Sheet]  [匯出三線開花 Sheet]
│                   │
└─────────┬─────────┘
          │
          ▼
[備份資料庫到 Release]
```

### 4.2 美股每日任務額外流程：分割偵測

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
| **大盤指數** | 加權指數 TAIEX | S&P 500 (^GSPC) |
| **股價計算** | 使用**未調整股價**（與券商一致） | 使用**調整後股價**（adj_close） |
| **股票數量** | ~1,700 檔 | ~8,000 檔 |
| **批次策略** | 逐檔查詢（FinMind 限制） | 批次 100 檔、間隔 5 秒、4 workers |
| **分割偵測** | 無（台股不常見） | 有（`USSplitDetector`） |
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
