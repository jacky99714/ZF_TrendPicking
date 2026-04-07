# ZF_TrendPicking - 快速設定指南

## 一、環境需求

- Python 3.11+
- Git
- GitHub CLI (`gh`)

## 二、安裝步驟

### 1. 建立虛擬環境

```bash
cd ZF_TrendPicking
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. 設定環境變數

```bash
cp .env.example .env
# 編輯 .env 填入實際值
```

需要設定的項目：
- `FINMIND_TOKEN`: 到 https://finmindtrade.com/ 註冊取得
- `SHEET_ID_*`: 台股各 Google Sheet ID
- `US_SHEET_ID_*`: 美股各 Google Sheet ID

### 3. 設定 Google Service Account

1. 到 [Google Cloud Console](https://console.cloud.google.com/) 建立專案
2. 啟用 Google Sheets API
3. 建立 Service Account
4. 下載 JSON 金鑰，存為 `credentials.json`
5. 將 Service Account Email 加入各 Sheet 共用（編輯者權限）

### 4. 初始化系統

```bash
source .venv/bin/activate

# 台股初始化
python main.py init

# 美股初始化（約 30-60 分鐘）
python us_main.py init
```

## 三、使用方式

### 手動執行

```bash
# 台股每日篩選
python main.py daily

# 美股每日篩選
python us_main.py daily

# 指定日期
python main.py daily 2026-01-17

# 強制執行（忽略假日檢查）
python main.py daily --force

# 每月任務（更新公司主檔）
python main.py monthly
python us_main.py monthly

# 健康檢查
python main.py health
python us_main.py health

# 前端 JSON 匯出（v2 拆分架構）
python scripts/export_to_json_v2.py
```

### 資料維護

```bash
# 補齊缺漏股價（dry-run 模式先檢查）
python scripts/backfill_missing_prices.py --tw --dry-run
python scripts/backfill_missing_prices.py --dry-run

# 正式補齊
python scripts/backfill_missing_prices.py --tw --all
python scripts/backfill_missing_prices.py --all

# 驗證資料完整性（檢查缺日）
python scripts/verify_stock_gaps.py --tw          # 台股篩選通過的
python scripts/verify_stock_gaps.py               # 美股篩選通過的
python scripts/verify_stock_gaps.py --tw --all     # 台股全部
python scripts/verify_stock_gaps.py --all          # 美股全部
python scripts/verify_stock_gaps.py --stock 2330   # 指定股票

# 資料驗證（4 層檢查）
python scripts/verify_data.py
python scripts/verify_data.py --us

# 修復缺失的 indicator_json
python scripts/fix_missing_indicators.py
python scripts/fix_missing_indicators.py --us
```

### GitHub Actions 自動排程

| 排程 | 台灣時間 | 說明 |
|------|---------|------|
| 台股 daily | 週一~五 17:45 | 每日篩選 + 匯出 Sheet |
| 美股 daily | 週一~五 05:30 | 每日篩選 + 匯出 Sheet |
| Deploy Site | 每日篩選後 | 自動部署前端查詢網站 |
| 每月更新 | 手動觸發 | 更新台股/美股公司主檔 |

### 手動觸發 CI/CD

```bash
# 台股指定日期（force 忽略假日檢查）
gh workflow run daily.yml --field target_date=2026-03-28 --field force=true

# 美股指定日期
gh workflow run us-daily.yml --field target_date=2026-03-28 --field force=true

# 部署前端網站
gh workflow run deploy-site.yml
```

## 四、每日排程流程

```
Step 1:   確保股票清單
Step 2:   下載今日股價
Step 2.5: 自動補漏歷史缺口（price_gap_filler）
Step 3:   減資/分割偵測
Step 4:   大盤指數下載
Step 5:   篩選（VCP + 三線開花）
Step 6:   匯出 Google Sheet
Step 7:   每日自動驗證
─── task 結束 ───
備份 DB → 觸發前端部署
```

> 補漏在篩選之前執行，確保 Sheet 上的數值用完整資料計算。

## 五、篩選邏輯

### VCP 強勢股

| 條件 | 邏輯 | 使用價格 |
|------|------|---------|
| ① 均線多頭排列 | close > MA50 > MA150 > MA200 | **收盤價** |
| ② MA200 趨勢向上 | MA200 今日 > MA200 20日前 | **收盤價** |
| ③ 打敗大盤 | 個股 20 日報酬 > 大盤 20 日報酬 | **收盤價** |

### VCP 新高

| 條件 | 邏輯 | 使用價格 |
|------|------|---------|
| ① 接近 52 週新高 | \|5日最高 / 260日最高 - 1\| ≤ 1% | **最高價** |
| ② 打敗大盤 | 個股 20 日報酬 > 大盤 20 日報酬 | **收盤價** |

- 52 週 = 260 交易日（52 × 5）
- 容差 = 1%
- VCP 結果 = 強勢 ∪ 新高（聯集）

### 三線開花

| 條件 | 邏輯 | 使用價格 |
|------|------|---------|
| ① 三線排列 | close > MA8 > MA21 > MA55 | **收盤價** |
| ② 55 日新高 | close ≥ 55 日最高收盤價 | **收盤價** |

- 差距比例 = (今日收盤 / 55日次高收盤) - 1

> **注意**：VCP 新高用「最高價」，其他所有條件都用「收盤價」。

## 六、專案結構

```
ZF_TrendPicking/
├── config/
│   ├── settings.py              # 台股設定
│   └── us_settings.py           # 美股設定
├── api/
│   ├── hybrid_client.py         # 台股混合客戶端（FinMind + yfinance）
│   ├── finmind_client.py        # FinMind API
│   ├── yfinance_client.py       # yfinance API
│   ├── us_stock_client.py       # 美股 API 介面
│   ├── us_stock_client_free.py  # 美股免費版（yfinance）
│   └── rate_limiter.py          # 限流控制
├── data/
│   ├── sqlite_database.py       # 台股 SQLite DB 操作
│   ├── models.py                # 台股 ORM 模型
│   ├── us_database.py           # 美股 SQLite DB 操作
│   └── us_models.py             # 美股 ORM 模型
├── calculators/
│   ├── moving_average.py        # 台股均線計算
│   ├── vcp_filter.py            # 台股 VCP 篩選
│   ├── sanxian_filter.py        # 台股三線開花
│   ├── us_moving_average.py     # 美股均線計算
│   ├── us_vcp_filter.py         # 美股 VCP 篩選
│   └── us_sanxian_filter.py     # 美股三線開花
├── exporters/
│   ├── google_sheet.py          # 台股 Sheet 匯出
│   └── us_google_sheet.py       # 美股 Sheet 匯出
├── tasks/
│   ├── daily_task.py            # 台股每日任務
│   ├── monthly_task.py          # 台股每月任務
│   ├── us_daily_task.py         # 美股每日任務
│   └── us_monthly_task.py       # 美股每月任務
├── utils/
│   ├── trading_calendar.py      # 台股交易日曆
│   ├── us_trading_calendar.py   # 美股交易日曆
│   ├── split_detector.py        # 台股除權息偵測
│   ├── us_split_detector.py     # 美股分割偵測
│   ├── price_gap_filler.py      # 股價缺漏自動補齊
│   └── daily_verifier.py        # 每日自動驗證
├── scripts/
│   ├── export_to_json_v2.py     # 前端 JSON 匯出（v2 拆分）
│   ├── backfill_missing_prices.py     # 手動補齊缺漏股價
│   ├── verify_stock_gaps.py           # 資料完整性驗證
│   ├── backfill_all_trading_days.py   # 台股補齊所有交易日
│   ├── backfill_all_trading_days_us.py # 美股補齊所有交易日
│   ├── fix_missing_indicators.py      # 修復缺失指標
│   ├── verify_data.py                 # 資料驗證（4 層）
│   ├── reexport_all_dates.py          # 重新匯出到 Sheet
│   └── ...                            # 其他維護腳本
├── tradingview/
│   ├── zf_vcp_strong.pine       # TradingView VCP 強勢指標
│   ├── zf_vcp_newhigh.pine      # TradingView VCP 新高指標
│   └── zf_sanxian.pine          # TradingView 三線開花指標
├── site/
│   ├── index.html               # 前端查詢網站
│   └── data/                    # 拆分 JSON 資料
├── main.py                      # 台股主程式
├── us_main.py                   # 美股主程式
└── requirements.txt             # 依賴清單
```

## 七、常見問題

### Q: API 呼叫超過限制？
A: 系統內建限流器（600 次/小時），正常使用不會超過。若需要大量補資料，建議分批執行。

### Q: Google Sheet 無法匯出？
A: 確認：
1. credentials.json 檔案存在
2. Service Account Email 已加入 Sheet 共用
3. Sheet ID 設定正確

### Q: 篩選結果為空？
A: 可能是非交易日或市場狀況導致無股票符合條件，這是正常現象。

### Q: Sheet 數值與 TradingView 不一致？
A: 確認：
1. TradingView 指標已更新為最新版本（有 timeframe 自動轉換）
2. 參數設定一致：52 週 = 260 天、容差 = 1%
3. 注意：VCP 新高用**最高價**，TradingView 和 Python 都一樣

### Q: 某些股票的均線是空的？
A: 該股票上市天數不足，例如 MA55 需要至少 55 個交易日。這是正常現象，不影響篩選（資料不足會自動排除）。

### Q: 不可同時跑同市場的 workflow？
A: 正確。多個 run 會搶同一個 Release DB 備份，導致資料互相覆蓋。補跑多天時必須逐個等完成再觸發下一個。

## 八、DB 備份與還原

```bash
# 下載線上最新 DB（檢查資料時必須用線上版本）
gh release download db-backup -p 'zf_trend_full.db.gz' -D /tmp --clobber && gunzip -f /tmp/zf_trend_full.db.gz
gh release download us-db-backup -p 'zf_trend_us.db.gz' -D /tmp --clobber && gunzip -f /tmp/zf_trend_us.db.gz

# 驗證
sqlite3 /tmp/zf_trend_full.db "SELECT MAX(date) FROM daily_price;"
sqlite3 /tmp/zf_trend_us.db "SELECT MAX(date) FROM us_daily_price;"
```

> **重要**：本機 DB 不會每天更新，排程在 CI/CD 上跑。檢查資料時務必從 GitHub Release 下載線上 DB。
