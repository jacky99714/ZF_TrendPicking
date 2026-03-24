# ZF_TrendPicking - 快速設定指南

## 一、環境需求

- Python 3.11+
- Git

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

### GitHub Actions 自動排程

| 排程 | 台灣時間 | 說明 |
|------|---------|------|
| 台股 daily | 週一~五 17:45 | 每日篩選 + 匯出 Sheet |
| 美股 daily | 週一~五 05:30 | 每日篩選 + 匯出 Sheet |
| Deploy Site | 每日篩選後 | 自動部署前端查詢網站 |
| 每月更新 | 每月 1 日 | 更新台股/美股公司主檔 |

## 四、專案結構

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
│   ├── database.py              # 台股 DB 操作
│   ├── models.py                # 台股 ORM 模型
│   ├── us_database.py           # 美股 DB 操作
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
│   └── us_split_detector.py     # 美股分割偵測
├── scripts/
│   ├── export_to_json_v2.py     # 前端 JSON 匯出（v2 拆分）
│   ├── backfill_all_trading_days.py    # 台股補齊所有交易日
│   ├── backfill_all_trading_days_us.py # 美股補齊所有交易日
│   ├── backfill_us_prices.py    # 美股歷史股價回溯
│   ├── fix_missing_indicators.py # 修復缺失指標
│   ├── verify_data.py           # 資料驗證腳本
│   ├── reexport_all_dates.py    # 重新匯出到 Sheet
│   └── ...                      # 其他維護腳本
├── site/
│   ├── index.html               # 前端查詢網站
│   └── data/                    # 拆分 JSON 資料
├── main.py                      # 台股主程式
├── us_main.py                   # 美股主程式
└── requirements.txt             # 依賴清單
```

## 五、輸出說明

### VCP 強勢股篩選

| 欄位 | 說明 |
|------|------|
| 代號 | 股票代號 |
| 股名 | 股票名稱 |
| 公司名 | 同股名 |
| 產業分類1 | 產業分類 |
| 產業分類2 | - |
| 產品組合 | - |
| 近20日股價漲幅 | 20日報酬率 |
| 強勢清單 | O = 符合強勢條件 |
| 新高清單 | O = 符合新高條件 |

**強勢清單條件**：
1. 收盤價 > MA50 > MA150 > MA200
2. MA200 今日 > MA200 20日前
3. 股票 20 日報酬 > 大盤 20 日報酬

**新高清單條件**：
1. 5 日高點接近 52 週高點（誤差 ≤ 1%）
2. 股票 20 日報酬 > 大盤 20 日報酬

### 三線開花篩選

| 欄位 | 說明 |
|------|------|
| 代號 | 股票代號 |
| 股名 | 股票名稱 |
| 公司名 | 同股名 |
| 產業分類1 | 產業分類 |
| 產業分類2 | - |
| 產品組合 | - |
| 今日股價 | 當日收盤價 |
| 55日內次高價 | 55 日內第二高的收盤價 |
| 差距比例 | (今日股價 / 次高價) - 1 |

**篩選條件**：
1. 收盤價 > MA8 > MA21 > MA55
2. 收盤價 = 55 日最高價

## 六、常見問題

### Q: API 呼叫超過限制？
A: 系統內建限流器（600 次/小時），正常使用不會超過。若需要大量補資料，建議分批執行。

### Q: Google Sheet 無法匯出？
A: 確認：
1. credentials.json 檔案存在
2. Service Account Email 已加入 Sheet 共用
3. Sheet ID 設定正確

### Q: 篩選結果為空？
A: 可能是非交易日或市場狀況導致無股票符合條件，這是正常現象。

## 七、API 使用量估算

| 操作 | API 呼叫次數 |
|------|-------------|
| 首次初始化 | 約 3 次 |
| 每日更新 | 2 次 |
| 每月主檔更新 | 1 次 |

註冊用戶限制：600 次/小時，足夠日常使用。
