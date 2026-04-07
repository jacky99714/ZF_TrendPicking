# 演算法規格說明書

> ZF_TrendPicking 篩選邏輯、均線計算與特殊處理

## 1. 均線計算基礎

### 1.1 簡單移動平均 (SMA)

**公式**：

```
SMA(N) = (P₁ + P₂ + ... + Pₙ) / N
```

其中 `P` 為**收盤價 (close_price)**，`N` 為週期天數。

**實作**：使用 pandas `rolling(window=N, min_periods=N).mean()`

- `min_periods=N`：資料不足 N 天時回傳 NaN，確保均線可靠性
- 計算前先依 `(stock_id, date)` 排序

**使用的均線週期**：

| 用途 | 均線 | 計算用價格 |
|------|------|----------|
| 三線開花短期 | MA8, MA21 | **收盤價** |
| 三線開花中期 | MA55 | **收盤價** |
| VCP 中期 | MA50 | **收盤價** |
| VCP 長期 | MA150, MA200 | **收盤價** |

### 1.2 高低點計算

| 欄位 | 計算用價格 | 函數 | 說明 |
|------|----------|------|------|
| `high_5d` | **最高價 (high_price)** | `calculate_high_low()` | 近 5 日最高價的最大值 |
| `high_260d` | **最高價 (high_price)** | `calculate_high_low()` | 近 260 日最高價的最大值（52 週） |
| `high_55d` | **收盤價 (close_price)** | `calculate_close_high()` | 近 55 日收盤價的最大值 |
| `second_high_55d` | **收盤價 (close_price)** | `calculate_second_high()` | 近 55 日收盤價的第二高值 |

> **重要**：VCP 新高用的是**最高價**，三線開花新高用的是**收盤價**。

### 1.3 零價修正

**問題**：FinMind API 偶爾回傳 OHLC 全為 0 但有成交量的異常資料，會導致均線計算偏差和 `pct_change` 產生 `inf`。

**修正邏輯**（`MovingAverageCalculator.fix_zero_prices()`）：

```
1. 偵測 close_price == 0 的資料列
2. 將零值替換為 NaN
3. 依 stock_id 分組，使用 forward-fill 填入前一交易日的收盤價
4. 若該股票沒有前日資料（如首日即為零），保持 NaN
```

**影響欄位**：`close_price`, `open_price`, `high_price`, `low_price`

---

## 2. VCP 強勢股篩選演算法

**模組**：`calculators/vcp_filter.py` (`VCPFilter`)

### 2.1 前置計算

呼叫 `MovingAverageCalculator.prepare_vcp_data()` 依序執行：

1. **零價修正**：`fix_zero_prices()`
2. **計算均線**：`calculate_sma([50, 150, 200])` — 使用**收盤價**
3. **計算 MA200 斜率**：`calculate_ma_slope("ma200", lookback=20)` → 欄位 `ma200_slope_20d`
4. **計算 20 日報酬率**：`calculate_returns([20])` → 欄位 `return_20d` — 使用**收盤價**
5. **計算高低點**：`calculate_high_low([5, 260])` → 欄位 `high_5d`, `high_260d` — 使用**最高價**

### 2.2 大盤報酬計算

**函數**：`calculate_market_return(market_df, target_date, lookback=20)`

```python
market_return = (current_index - past_index) / past_index
```

- `current_index`：目標日期（或最近交易日）的大盤指數
- `past_index`：20 個交易日前的大盤指數
- 台股使用 TAIEX（加權指數），美股使用 S&P 500

### 2.3 強勢清單篩選

三個條件必須同時滿足（AND）：

```
條件 1（均線多頭排列）— 使用【收盤價】:
  close_price > MA50 > MA150 > MA200

  NaN 處理：
  - close_price NaN → 填 0（不滿足）
  - MA50/MA150/MA200 NaN → 填 +∞（不滿足）

條件 2（MA200 趨勢向上）:
  ma200_slope_20d > 0
  即 MA200 今日值 > MA200 20 天前的值

  NaN 處理：填 -1（不滿足）

條件 3（打敗大盤）— 使用【收盤價】計算報酬率:
  return_20d > market_return_20d

  NaN 處理：填 -∞（不滿足）
```

### 2.4 新高清單篩選

兩個條件必須同時滿足（AND）：

```
條件 1（接近 52 週新高）— 使用【最高價】:
  |high_5d / high_260d - 1| ≤ new_high_tolerance

  high_5d  = 近 5 日【最高價】的最大值
  high_260d = 近 260 日【最高價】的最大值（52 週 = 52×5 = 260 交易日）
  容差（new_high_tolerance）= 0.01（1%）

  NaN 處理：
  - high_5d NaN → 填 0（不滿足）
  - high_260d NaN → 填 1（避免除以零）
  - high_260d == 0 → 替換為 1（安全除法）
  額外條件：high_260d > 0（數據有效性）

條件 2（打敗大盤）— 使用【收盤價】計算報酬率:
  return_20d > market_return_20d
  （同強勢清單條件 3）
```

### 2.5 最終合併

```
最終結果 = 強勢清單 UNION 新高清單（聯集）
```

- 同一股票可能同時出現在兩個清單中
- `is_strong = True` 表示符合強勢清單
- `is_new_high = True` 表示符合新高清單

---

## 3. 三線開花篩選演算法

**模組**：`calculators/sanxian_filter.py` (`SanxianFilter`)

### 3.1 前置計算

呼叫 `MovingAverageCalculator.prepare_sanxian_data()` 依序執行：

1. **零價修正**：`fix_zero_prices()`
2. **計算均線**：`calculate_sma([8, 21, 55])` — 使用**收盤價**
3. **計算 55 日收盤價高點**：`calculate_close_high(periods=[55])` → 欄位 `high_55d` — 使用**收盤價**
4. **計算 55 日次高價**：`calculate_second_high(period=55)` → 欄位 `second_high_55d` — 使用**收盤價**

> 注意：三線開花的所有計算都使用**收盤價**，不使用最高價。`calculate_close_high()` 使用 `min_periods = max(period // 2, 1)`，避免新上市股票因資料不足而誤判。

### 3.2 篩選條件

兩個條件必須同時滿足（AND）：

```
條件 1（三線開花排列）— 使用【收盤價】:
  close_price > MA8 > MA21 > MA55

  NaN 處理：
  - close_price NaN → 填 0（不滿足）
  - MA8/MA21/MA55 NaN → 填 +∞（不滿足）

條件 2（55 日收盤新高）— 使用【收盤價】:
  close_price >= high_55d

  NaN 處理：high_55d NaN → 填 +∞（不滿足）
```

### 3.3 差距比例計算

```python
gap_ratio = (today_price / second_high_55d) - 1
```

- `today_price` = 當日**收盤價**
- `second_high_55d` = 55 個交易日內的第二高**收盤價**

**次高價計算邏輯**（`calculate_second_high()`）：

```
對每個股票的每個交易日：
  1. 取前 55 天（含當天）的收盤價
  2. 降冪排序
  3. 取排序後的第二個值
  4. 若資料不足 2 天，回傳 NaN
```

**除以零保護**：`second_high_55d` 為 0 或 NaN 時替換為 1

---

## 4. 價格使用總覽

### 4.1 各條件使用的價格類型

| 篩選器 | 條件 | 使用價格 |
|--------|------|---------|
| **VCP 強勢** | 均線多頭排列 close > MA50 > MA150 > MA200 | 收盤價 |
| **VCP 強勢** | MA200 趨勢向上 | 收盤價（MA200 本身用收盤價算） |
| **VCP 強勢** | 打敗大盤（20 日報酬率） | 收盤價 |
| **VCP 新高** | 接近 52 週新高 (high_5d vs high_260d) | **最高價** |
| **VCP 新高** | 打敗大盤（20 日報酬率） | 收盤價 |
| **三線開花** | 三線排列 close > MA8 > MA21 > MA55 | 收盤價 |
| **三線開花** | 55 日收盤新高 | 收盤價 |
| **三線開花** | 差距比例 (gap_ratio) | 收盤價 |

### 4.2 台股 vs 美股股價使用差異

| 項目 | 台股 | 美股 |
|------|------|------|
| 均線計算用價格 | **未調整收盤價** (close_price) | **調整後收盤價** (adj_close) |
| 原因 | 與券商報價一致，直覺理解 | 反映分割/配息，歷史資料連續 |
| yfinance 設定 | `auto_adjust=False` | 預設（auto_adjust） |
| 零價修正 | 有（FinMind 偶發） | 無需（yfinance 不會回傳零價） |

---

## 5. 每日排程流程

### 5.1 執行順序

```
Step 1: 確保股票清單
Step 2: 下載今日股價
Step 2.5: 補漏歷史缺口（price_gap_filler）
Step 3: 減資/分割偵測
Step 4: 大盤指數
Step 5: 篩選（VCP + 三線開花）
Step 6: 匯出 Sheet
Step 7: 每日驗證
```

### 5.2 補漏機制（Step 2.5）

**模組**：`utils/price_gap_filler.py`

```
1. 用基準股票（台股: 2330, 美股: AAPL）建立交易日曆
2. 逐股比對：找出上市日期範圍內缺少的交易日
3. 台股根據 stock_type（twse/tpex）決定 yfinance suffix（.TW/.TWO）
4. 從 yfinance 下載缺漏日期的股價
5. 寫入 DB（upsert，不覆蓋已存在的資料）
6. 限制每次最多補 200 檔，避免超時
```

---

## 6. 自適應批次下載演算法

**模組**：`api/yfinance_client.py`（台股備援用）

用於 yfinance 批次下載時動態調整效能參數：

```
初始設定：
  batch_size = 50（每批股票數）
  interval = 2（批次間隔秒數）

每批完成後：
  if 錯誤率 > 30%:
    batch_size = max(batch_size * 0.7, 10)    # 縮小批次
    interval = min(interval * 1.5, 30)         # 增加間隔
  elif 錯誤率 < 5%:
    batch_size = min(batch_size * 1.2, 200)    # 擴大批次
    interval = max(interval * 0.8, 1)          # 縮短間隔
```

---

## 7. 分割/合股偵測演算法

### 7.1 台股（FinMind 還原價偵測）

**模組**：`utils/split_detector.py`

```
1. 取得 FinMind 還原權息價（TaiwanStockPriceAdj）
2. 比對 DB 中前一交易日的收盤價
3. 價格偏離超過閾值 → 標記為除權息/減資
4. 重新下載受影響股票的完整歷史資料
```

### 7.2 美股（yfinance 偵測）

**模組**：`utils/us_split_detector.py` + `tasks/us_daily_task.py`

```
1. 取得前一交易日所有股票的 DB 收盤價
2. 取得相同日期的 yfinance 最新收盤價
3. 比對每檔股票：
   ratio = yfinance_price / db_price
   if ratio > 1.5 or ratio < 0.67:
     → 標記為「疑似分割/合股」
4. 對標記的股票：
   - 刪除 DB 中該股票的所有歷史價格
   - 重新下載 365 天歷史資料
   - 寫入 DB
```

---

## 8. 參數設定一覽表

### 8.1 VCP 參數

| 參數 | 設定鍵 | 值 | 說明 |
|------|--------|-----|------|
| MA50 週期 | `ma50_period` | 50 | 中期均線（收盤價） |
| MA150 週期 | `ma150_period` | 150 | 長期均線（收盤價） |
| MA200 週期 | `ma200_period` | 200 | 超長期均線（收盤價） |
| 報酬回看天數 | `lookback_20d` | 20 | 近 20 日漲跌幅（收盤價） |
| 52 週天數 | `lookback_52w` | 260 | 52 週 = 52×5 = 260 交易日 |
| 新高容差 | `new_high_tolerance` | 0.01 | 1%（台股美股相同） |

### 8.2 三線開花參數

| 參數 | 設定鍵 | 值 | 說明 |
|------|--------|-----|------|
| MA8 週期 | `ma8_period` | 8 | 短期均線（收盤價） |
| MA21 週期 | `ma21_period` | 21 | 中短期均線（收盤價） |
| MA55 週期 | `ma55_period` | 55 | 中期均線（收盤價） |

### 8.3 重試參數

| 參數 | 值 | 說明 |
|------|-----|------|
| 最大重試次數 | 3 | 可透過環境變數調整 |
| 第 1 次間隔 | 300 秒 | 5 分鐘 |
| 第 2 次間隔 | 600 秒 | 10 分鐘 |
| 第 3 次間隔 | 3600 秒 | 1 小時 |

### 8.4 美股批次下載參數

| 參數 | 預設值 | 環境變數 | 說明 |
|------|-------|---------|------|
| 批次大小 | 100 | `US_BATCH_SIZE` | 每批下載股票數 |
| 批次間隔 | 5 秒 | `US_BATCH_INTERVAL` | 批次間等待時間 |
| 平行 Workers | 4 | `US_MAX_WORKERS` | 並行下載執行緒數 |

### 8.5 API 限流參數

| 參數 | 值 | 說明 |
|------|-----|------|
| FinMind 呼叫上限 | 600 次/小時 | Token Bucket 控制 |
| 台股 yfinance 備援 | 自適應 | 動態調整批次大小 |
| 美股 yfinance | 自律控速 | 依批次設定限制 |

---

## 9. TradingView 指標

### 9.1 Timeframe 自動轉換

TradingView Pine Script 指標支援日/週/月線自動轉換：

```pine
tf_mult = timeframe.ismonthly ? 21 * timeframe.multiplier
        : timeframe.isweekly  ? 5  * timeframe.multiplier
        : timeframe.isdaily   ? timeframe.multiplier
        : 1
tf_adj(int days) => math.max(1, math.ceil(days / tf_mult))
```

- 使用**交易日**（非日曆天）：週線 ÷5、月線 ÷21
- 使用 `math.ceil`（無條件進位）確保回看範圍至少涵蓋日線的範圍

### 9.2 指標對照

| TradingView 指標 | Python 對應 | 信號標記 |
|-----------------|------------|---------|
| ZF VCP 強勢 | `vcp_filter._filter_strong_list` | 綠色 ▲「強」 |
| ZF VCP 新高 | `vcp_filter._filter_new_high_list` | 黃色 ◆「高」 |
| ZF 三線開花 | `sanxian_filter.filter` | 藍色 ●（無文字） |

---

## 10. 參考文件

- [需求規格](./01-requirements-spec.md)
- [技術架構](./02-architecture.md)
- [資料規格](./03-data-spec.md)
- [操作指南](./05-operations-guide.md)
