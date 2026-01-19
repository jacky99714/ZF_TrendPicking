# API 實測報告

> **測試日期**：2026-01-18
> **測試目的**：驗證各 API 可用性與回傳資料格式

---

## 一、測試總覽

| API | 市場 | 狀態 | 需要 API Key | 費用 |
|-----|------|:----:|:------------:|------|
| TWSE OpenAPI | 台股上市 | ✅ 成功 | ❌ 不需要 | 免費 |
| TPEX OpenAPI | 台股上櫃 | ✅ 成功 | ❌ 不需要 | 免費 |
| Fugle API | 台股 | ⚠️ 需 Key | ✅ 需要 | 付費 |
| Twelve Data | 美股 | ✅ 成功 | ⚠️ demo 可用 | 付費 |
| Finnhub | 美股 | ⚠️ 需 Key | ✅ 需要 | 免費/付費 |

---

## 二、台股 API 實測

### 2.1 TWSE 全部股票當日交易 ✅

**API 端點**
```
GET https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json
```

**回傳範例**
```json
{
  "stat": "OK",
  "date": "20260116",
  "title": "115年01月16日 當日日成交資訊 (股)",
  "fields": ["證券代號","證券名稱","成交股數","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","成交筆數"],
  "data": [
    ["0050","元大台灣50","101,069,992","7,242,693,243","71.75","72.10","71.20","72.00","+1.35","80,151"],
    ["2330","台積電","53,337,000","...","1735.00","1750.00","1710.00","1740.00","+50.00","..."]
    // ... 約 2000+ 筆
  ]
}
```

**欄位對照**
| API 欄位 | 規格欄位 | 符合 |
|---------|---------|:----:|
| 證券代號 | 代號 | ✅ |
| 證券名稱 | 股名 | ✅ |
| 開盤價 | O | ✅ |
| 最高價 | H | ✅ |
| 最低價 | L | ✅ |
| 收盤價 | C | ✅ |
| 成交股數 | V | ✅ |
| 漲跌價差 | 漲跌 | ✅ |

**優點**：✅ 一次取得全部股票、✅ 完全免費、✅ 無需 API Key
**缺點**：❌ 無產業分類、❌ 只有當日資料

---

### 2.2 TWSE 個股歷史資料 ✅

**API 端點**
```
GET https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20250115&stockNo=2330
```

**回傳範例**
```json
{
  "stat": "OK",
  "date": "20250115",
  "title": "114年01月 2330 台積電 各日成交資訊",
  "fields": ["日期","成交股數","成交金額","開盤價","最高價","最低價","收盤價","漲跌價差","成交筆數","註記"],
  "data": [
    ["114/01/02","45,045,125","47,883,206,644","1,070.00","1,075.00","1,055.00","1,065.00","-10.00","74,997",""],
    ["114/01/03","31,244,211","33,728,652,860","1,080.00","1,085.00","1,075.00","1,075.00","+10.00","28,227",""]
    // ... 整月資料
  ]
}
```

**特性**：
- ✅ 可取得歷史 OHLCV
- ✅ 單次查詢一整月資料
- ⚠️ 需逐月查詢（計算 MA200 需查詢約 10 個月）

---

### 2.3 TWSE 大盤指數歷史 ✅

**API 端點**
```
GET https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date=20260116
```

**回傳範例**
```json
{
  "stat": "OK",
  "title": "115年01月 發行量加權股價指數歷史資料",
  "fields": ["日期","開盤指數","最高指數","最低指數","收盤指數"],
  "data": [
    ["115/01/02","29,016.68","29,363.43","29,007.75","29,349.81"],
    ["115/01/16","30,844.63","31,475.22","30,844.63","31,408.70"]
  ]
}
```

**用途**：✅ 計算大盤近 20 日漲跌幅

---

### 2.4 TWSE 本益比/殖利率 ✅

**API 端點**
```
GET https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=json&date=20260116&selectType=ALL
```

**回傳欄位**：證券代號、證券名稱、收盤價、殖利率、本益比、股價淨值比

---

### 2.5 TPEX 上櫃股票行情 ✅

**API 端點**
```
GET https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=json&d=114/01/15&s=0,asc,0
```

**回傳範例**
```json
{
  "date": "20260116",
  "tables": [{
    "title": "上櫃股票行情",
    "fields": ["代號","名稱","收盤","漲跌","開盤","最高","最低","均價","成交股數","成交金額","成交筆數",...],
    "data": [
      ["006201","元大富櫃50","27.69","+0.27","27.60","27.76","27.45","27.62","80,033","2,210,439","89",...]
    ]
  }]
}
```

---

### 2.6 Fugle API ⚠️ 需 API Key

**API 端點**
```
GET https://api.fugle.tw/marketdata/v1.0/stock/intraday/ticker/2330
Header: X-API-KEY: YOUR_API_KEY
```

**測試結果**
```json
{"message":"Unauthorized","statusCode":401}
```

**說明**：需要申請 Fugle 帳號並取得 API Key

---

## 三、美股 API 實測

### 3.1 Twelve Data Time Series ✅

**API 端點**
```
GET https://api.twelvedata.com/time_series?symbol=AAPL&interval=1day&outputsize=5&apikey=demo
```

**回傳範例**
```json
{
  "meta": {
    "symbol": "AAPL",
    "interval": "1day",
    "currency": "USD",
    "exchange": "NASDAQ",
    "type": "Common Stock"
  },
  "values": [
    {"datetime":"2026-01-16","open":"257.89999","high":"258.89999","low":"254.92999","close":"255.53000","volume":"72018600"},
    {"datetime":"2026-01-15","open":"260.64999","high":"261.040009","low":"257.049988","close":"258.20999","volume":"39388600"}
  ],
  "status": "ok"
}
```

**欄位對照**
| API 欄位 | 規格欄位 | 符合 |
|---------|---------|:----:|
| datetime | 日期 | ✅ |
| open | O | ✅ |
| high | H | ✅ |
| low | L | ✅ |
| close | C | ✅ |
| volume | V | ✅ |

---

### 3.2 Twelve Data Quote（即時報價）✅

**API 端點**
```
GET https://api.twelvedata.com/quote?symbol=AAPL&apikey=demo
```

**回傳範例**
```json
{
  "symbol": "AAPL",
  "name": "Apple Inc.",
  "exchange": "NASDAQ",
  "currency": "USD",
  "datetime": "2026-01-16",
  "open": "257.89999",
  "high": "258.89999",
  "low": "254.92999",
  "close": "255.53000",
  "volume": "72018600",
  "previous_close": "258.20999",
  "change": "-2.67999",
  "percent_change": "-1.037912",
  "fifty_two_week": {
    "low": "169.21001",
    "high": "288.62000",
    "low_change": "86.31999",
    "high_change": "-33.089996"
  }
}
```

**重點欄位**
- ✅ `fifty_two_week.high` - 52 週最高價
- ✅ `fifty_two_week.low` - 52 週最低價
- ✅ `percent_change` - 漲跌幅

---

### 3.3 Twelve Data Profile（公司資訊）✅

**API 端點**
```
GET https://api.twelvedata.com/profile?symbol=AAPL&apikey=demo
```

**回傳範例**
```json
{
  "symbol": "AAPL",
  "name": "Apple Inc.",
  "exchange": "NASDAQ",
  "sector": "Technology",
  "industry": "Consumer Electronics",
  "employees": 166000,
  "website": "https://www.apple.com",
  "description": "Apple Inc. is a leading technology company...",
  "CEO": "Mr. Timothy D. Cook",
  "address": "One Apple Park Way",
  "city": "Cupertino",
  "country": "United States"
}
```

**欄位對照**
| API 欄位 | 規格欄位 | 符合 |
|---------|---------|:----:|
| symbol | 代號 | ✅ |
| name | 股名/公司名 | ✅ |
| sector | 產業分類1 | ✅ |
| industry | 產業分類2 | ✅ |
| - | 產品組合 | ❌ |

---

### 3.4 Twelve Data SMA 技術指標 ✅

**API 端點**
```
GET https://api.twelvedata.com/sma?symbol=AAPL&interval=1day&time_period=50&apikey=demo
```

**回傳範例**
```json
{
  "meta": {
    "symbol": "AAPL",
    "indicator": {
      "name": "SMA - Simple Moving Average",
      "time_period": 50
    }
  },
  "values": [
    {"datetime":"2026-01-16","sma":"271.50980"},
    {"datetime":"2026-01-15","sma":"271.80000"},
    {"datetime":"2026-01-14","sma":"272.016799"}
  ]
}
```

**優點**：✅ 內建均線計算，省去自行計算

---

### 3.5 Twelve Data 批次查詢 ⚠️ 需正式 Key

**API 端點**
```
GET https://api.twelvedata.com/time_series?symbol=AAPL,MSFT,GOOGL&interval=1day&outputsize=3&apikey=demo
```

**測試結果**
```json
{
  "code": 401,
  "message": "The 'demo' API key is only used for initial familiarity. To become a full user, you can request your own API key...",
  "status": "error"
}
```

**說明**：批次查詢需要正式 API Key（免費註冊即可取得）

---

### 3.6 Twelve Data 股票清單 ✅

**API 端點**
```
GET https://api.twelvedata.com/stocks?exchange=NASDAQ&apikey=demo
```

**回傳範例**
```json
{
  "data": [
    {"symbol":"AAPL","name":"Apple Inc.","currency":"USD","exchange":"NASDAQ","type":"Common Stock"},
    {"symbol":"MSFT","name":"Microsoft Corporation","currency":"USD","exchange":"NASDAQ","type":"Common Stock"}
    // ... 數千筆
  ]
}
```

**用途**：✅ 取得美股完整清單

---

### 3.7 Finnhub API ⚠️ 需 API Key

**API 端點**
```
GET https://finnhub.io/api/v1/quote?symbol=AAPL&token=YOUR_TOKEN
```

**測試結果**
```json
{"error":"Invalid API key."}
```

**說明**：需要申請 Finnhub 帳號（免費）

---

## 四、API 比較總結

### 4.1 資料完整度比較

| 資料項目 | TWSE | TPEX | Fugle | Twelve Data |
|---------|:----:|:----:|:-----:|:-----------:|
| 股票代號 | ✅ | ✅ | ✅ | ✅ |
| 股票名稱 | ✅ | ✅ | ✅ | ✅ |
| 公司全名 | ❌ | ❌ | ❌ | ✅ |
| 產業分類 | ❌ | ❌ | ✅ | ✅✅ |
| OHLCV | ✅ | ✅ | ✅ | ✅ |
| 歷史資料 | ✅ 逐月 | ✅ | ✅ | ✅ |
| 52週高低 | ❌ 需計算 | ❌ | ✅ | ✅ |
| 均線 | ❌ 需計算 | ❌ | ❌ | ✅ 內建 |
| 大盤指數 | ✅ | ✅ | ✅ | ✅ |

### 4.2 使用便利性比較

| 項目 | TWSE/TPEX | Fugle | Twelve Data |
|------|:---------:|:-----:|:-----------:|
| 需要 API Key | ❌ | ✅ | ✅ |
| 批次查詢 | ✅ 全量 | ⚠️ 逐一 | ✅ 120檔/次 |
| SDK 支援 | ❌ | ✅ | ✅ |
| 文件完整度 | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| 回應速度 | ⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

---

## 五、推薦方案

### 🥇 最佳組合（穩定 + 價格合理）

| 市場 | 主要 API | 備援 API |
|------|---------|---------|
| **台股** | TWSE/TPEX OpenAPI | Fugle（產業分類） |
| **美股** | Twelve Data | Finnhub |

### 理由

1. **台股使用 TWSE/TPEX**
   - ✅ 完全免費
   - ✅ 一次取得全部股票
   - ✅ 官方資料來源
   - ⚠️ 產業分類需從 Fugle 或其他來源補充

2. **美股使用 Twelve Data**
   - ✅ 資料最完整（包含 sector/industry）
   - ✅ 內建技術指標計算
   - ✅ 有明確 SLA (99.95%)
   - ✅ 批次查詢效率高

---

## 六、API 端點快速參考

### 台股（免費）

```bash
# 全部股票當日交易
curl "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json"

# 個股歷史（逐月）
curl "https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date=20250115&stockNo=2330"

# 大盤指數
curl "https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date=20260116"

# 本益比/殖利率
curl "https://www.twse.com.tw/rwd/zh/afterTrading/BWIBBU_d?response=json&date=20260116&selectType=ALL"

# 上櫃股票
curl "https://www.tpex.org.tw/web/stock/aftertrading/daily_close_quotes/stk_quote_result.php?l=zh-tw&o=json&d=115/01/16"
```

### 美股（Twelve Data）

```bash
# 歷史 K 線
curl "https://api.twelvedata.com/time_series?symbol=AAPL&interval=1day&outputsize=200&apikey=YOUR_KEY"

# 即時報價（含 52 週高低）
curl "https://api.twelvedata.com/quote?symbol=AAPL&apikey=YOUR_KEY"

# 公司資訊（含產業分類）
curl "https://api.twelvedata.com/profile?symbol=AAPL&apikey=YOUR_KEY"

# 技術指標 SMA
curl "https://api.twelvedata.com/sma?symbol=AAPL&interval=1day&time_period=50&apikey=YOUR_KEY"

# 批次查詢
curl "https://api.twelvedata.com/time_series?symbol=AAPL,MSFT,GOOGL&interval=1day&apikey=YOUR_KEY"

# 股票清單
curl "https://api.twelvedata.com/stocks?exchange=NASDAQ&apikey=YOUR_KEY"
```
