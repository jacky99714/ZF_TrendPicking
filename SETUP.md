# 藏鋒系統 - 快速設定指南

## 一、環境需求

- Python 3.10+
- PostgreSQL 15+
- Docker (選用)

## 二、安裝步驟

### 1. 建立虛擬環境

```bash
cd /Users/wanghongxiang/Documents/stock/project/ZF_TrendPicking
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. 啟動 PostgreSQL

**方法 A：使用 Docker（推薦）**
```bash
docker-compose up -d postgres
```

**方法 B：使用本機 PostgreSQL**
```bash
# 建立資料庫
createdb zf_trend
createuser zf_user
psql -c "ALTER USER zf_user PASSWORD 'zf_password';"
psql -c "GRANT ALL PRIVILEGES ON DATABASE zf_trend TO zf_user;"
```

### 3. 設定環境變數

```bash
# 複製範例設定
cp .env.example .env

# 編輯 .env 填入實際值
```

需要設定的項目：
- `FINMIND_TOKEN`: 到 https://finmindtrade.com/ 註冊取得
- `SHEET_ID_*`: 各 Google Sheet 的 ID

### 4. 設定 Google Service Account

1. 到 [Google Cloud Console](https://console.cloud.google.com/) 建立專案
2. 啟用 Google Sheets API
3. 建立 Service Account
4. 下載 JSON 金鑰，存為 `credentials.json`
5. 將 Service Account Email 加入各 Sheet 共用（編輯者權限）

### 5. 初始化系統

```bash
# 啟動虛擬環境
source venv/bin/activate

# 初始化（建立資料表 + 取得歷史資料）
python main.py init
```

## 三、使用方式

### 手動執行

```bash
# 執行每日任務
python main.py daily

# 執行指定日期的任務
python main.py daily 2026-01-17

# 執行每月任務（更新公司主檔）
python main.py monthly

# 補齊歷史資料
python main.py backfill 30

# 健康檢查
python main.py health
```

### 啟動排程

```bash
# 前景執行
python main.py schedule

# 背景執行（使用 nohup）
nohup python main.py schedule > schedule.log 2>&1 &
```

排程時間：
- 每日任務：17:45
- 每月任務：每月 1 日 09:00

## 四、專案結構

```
zf_trend_picking/
├── config/
│   └── settings.py           # 設定檔
├── api/
│   ├── finmind_client.py     # FinMind API 封裝
│   └── rate_limiter.py       # 限流控制
├── data/
│   ├── database.py           # 資料庫操作
│   └── models.py             # SQLAlchemy 模型
├── calculators/
│   ├── moving_average.py     # 均線計算
│   ├── vcp_filter.py         # VCP 篩選
│   └── sanxian_filter.py     # 三線開花篩選
├── exporters/
│   └── google_sheet.py       # Sheet 匯出
├── tasks/
│   ├── daily_task.py         # 每日任務
│   └── monthly_task.py       # 每月任務
├── main.py                   # 主程式
├── docker-compose.yml        # Docker 設定
└── requirements.txt          # 依賴清單
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
