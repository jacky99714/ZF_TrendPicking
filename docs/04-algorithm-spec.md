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

> **台美差異**：VCP 高低點 `calculate_high_low()` 的 `min_periods`：台股 = 1（資料不足也計算）；美股 = `max(period // 2, 1)`（需至少半數資料才計算）。

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
| 均線計算用價格 | **未調整收盤價** (close_price) | **未調整原始收盤價** (close_price)；`adj_close` 雖入庫但**不用於均線** |
| 原因 | 主來源為 FinMind，與券商報價一致、直覺理解 | `auto_adjust=False` 取原始 close，未還原分割/配息 → 須靠 Step 2.6/2.7 分割偵測重抓修正 |
| yfinance 設定 | `auto_adjust=True`（僅為備援；未調整是因主來源為 FinMind） | `auto_adjust=False` |
| 零價修正 | 有（FinMind 偶發） | `prepare_*_data` 仍呼叫 `fix_zero_prices`（有實作，yfinance 少觸發） |

---

## 5. 每日排程流程

### 5.1 執行順序

**台股每日流程**（`tasks/daily_task.py`）：

```
Step 1:   確保股票清單
Step 2:   下載今日股價
Step 2.5: 補漏歷史缺口（price_gap_filler）
Step 3:   減資/分割偵測
Step 4:   大盤指數
Step 5:   篩選（VCP + 三線開花）
Step 6:   匯出 Sheet
Step 7:   每日驗證（DailyVerifier）
Step 8:   客觀驗證（ObjectiveVerifier，四層 L1-L4）
```

**美股每日流程**（`tasks/us_daily_task.py`，編號與台股略有差異）：

```
Step 1:   確保股票清單
Step 2:   下載今日股價
Step 2.5: 補漏歷史缺口
Step 2.6: 分割偵測（us_split_detector，方法 1 / 方法 2）
Step 2.7: 內部分割偵測（internal_split_detector，掃描 DB 自身相鄰跳動）
Step 3:   大盤指數
Step 4:   篩選（VCP + 三線開花）
Step 5:   匯出 Sheet
Step 6:   每日驗證（DailyVerifier）
Step 7:   客觀驗證（ObjectiveVerifier；美股客觀驗證為 Step 7，非 Step 8）
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
  batch_size = 100（initial_batch_size，每批股票數）
  interval = 5.0（initial_interval，批次間隔秒數）
  min_batch_size = 10、max_batch_size = 500
  max_interval = 30.0

每批完成後：
  if 錯誤率 > 20%（0.2）:
    batch_size = max(batch_size // 2, min_batch_size)   # 折半縮小批次
    interval = min(interval * 2, max_interval)          # 間隔加倍（上限 30 秒）
  elif 連續成功次數 ≥ 5:
    batch_size = min(batch_size * 2, max_batch_size)    # 批次加倍（上限 500）
    # 成功時不縮短 interval
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

### 7.2 美股（yfinance 偵測 — 三層檢查）

**模組**：`utils/us_split_detector.py` + `utils/internal_split_detector.py` + `tasks/us_daily_task.py`

**方法 1**（DB 前一日 vs yfinance 前一日）：
```
1. 取得前一交易日所有股票的 DB 收盤價
2. 取得相同日期的 yfinance 最新收盤價
3. 比對每檔股票：差異 > 1% → 標記為分割
```

**方法 2**（yfinance 無前日資料時，用 DB 前一日 vs 今日）：
```
1. 找出 DB 有前一日資料但 yfinance 沒回傳前一日的股票
2. 用 DB 前一日收盤價 vs 今日收盤價比對：
   ratio = today_close / db_prev_close
   if ratio > 1.5 or ratio < 0.67:
     → 標記為分割
```

> **為什麼需要方法 2**：某些 penny stock 合股後（如 NINE 0.012 → 8.20），
> yfinance 會直接刪除合股前的所有歷史，導致方法 1 無法比對。

**第三層**（內部分割偵測 — `internal_split_detector`，於 Step 2.7 執行）：
```
1. 掃描 DB 自身相鄰交易日的收盤價跳動（不比對外部來源）
2. ratio = today_close / prev_close
   if ratio >= JUMP_UP(1.5) or ratio <= JUMP_DOWN(0.67):
     → 視為疑似分割
3. 參數：
   MIN_PRICE_FOR_DETECT = 1.0   （股價低於 1.0 不偵測）
   MAX_PROCESS_PER_RUN  = 20    （每次最多處理 20 檔）
   HISTORY_DAYS         = 365   （重抓 365 天歷史）
   scan_days            = 30    （回掃近 30 天找跳動）
4. 白名單機制：重抓後若仍跳動 → 視為真實波動，加入白名單不再重複處理
```

> **第三層的意義**：方法 1/方法 2 依賴 yfinance 外部比對；第三層改掃 DB
> 自身歷史，攔截前兩層漏掉的跳動，並用白名單避免把真實大漲大跌誤判為分割。

**偵測到分割後的處理**：
```
1. DELETE DB 中該股票的所有舊資料（不是 upsert）
2. 重新下載 365 天歷史資料
3. INSERT 新資料
```

> 必須先 DELETE 再 INSERT。因為 yfinance 可能不回傳合股前的歷史，
> 用 upsert 會導致舊的 0.012 價格殘留在 DB 中。

### 7.3 異常報酬率過濾

即使分割偵測未抓到，篩選階段也會過濾異常值：

```
return_20d > 500% → 排除篩選（視為分割/合股未修正）
return_20d < -90% → 排除篩選
```

台股（`vcp_filter.py`）和美股（`us_vcp_filter.py`）都有此保護。

---

## 8. 美股 Rate Limit 重試機制

**模組**：`tasks/us_daily_task.py` (`_fetch_and_save_prices`)

美股約 8,000 檔，yfinance 批次下載時可能觸發 429 Too Many Requests。

```
第一次下載（全部 8,000 檔）
  ↓ 檢查筆數 < 6,500？（MIN_PRICE_COUNT：跳過下載 / 觸發重試的判定門檻）
  ↓ 是 → 找出缺失的股票
  ↓ 等待 300 秒（5 分） → 只重試缺的
  ↓ 檢查筆數 < 6,500？
  ↓ 是 → 等待 900 秒（15 分） → 再重試
  ↓ 最多 3 次
```

| 參數 | 值 |
|------|-----|
| MIN_PRICE_COUNT | 6,500（低於此筆數視為不完整 → 跳過下載 / 觸發重試） |
| MAX_RETRY | 3 |
| 重試間隔 | 300s / 900s / 900s（5 分 / 15 分 / 15 分；`wait_time = 300 if retry==1 else 900`） |

---

## 9. 四層客觀驗證

**模組**：`utils/objective_verifier.py` (`ObjectiveVerifier`)

在每日任務的 Step 8 執行，驗證結果寫入驗證 Sheet「驗證日誌」分頁。

| 層 | 驗證目標 | 方法 | 容差 |
|----|---------|------|------|
| L1 價格準確性 | DB 股價沒有錯 | 抽 15 檔，yfinance 獨立抓收盤價比對 | < 2% |
| L2 獨立重算 | 計算邏輯沒有 bug | 抽 5 檔，用 yfinance 原始資料獨立計算 MA/條件 | 決策一致 |
| L3 Sheet 回讀 | Sheet 匯出沒有丟資料 | 用 gspread 讀回今日 Sheet，比對行數和 stock_id | 完全一致 |
| L4 歷史一致性 | 系統行為沒有突變 | 今日篩選數量 vs 20 天平均 | 偏差 < 50% |

- L2 的計算完全獨立（不 import calculators/），確保客觀性
- 每一層用 try-except 包住，失敗不擋流程
- 結果自動追加到驗證 Sheet「驗證日誌」分頁

---

## 10. 參數設定一覽表

### 10.1 VCP 參數

| 參數 | 設定鍵 | 值 | 說明 |
|------|--------|-----|------|
| MA50 週期 | `ma50_period` | 50 | 中期均線（收盤價） |
| MA150 週期 | `ma150_period` | 150 | 長期均線（收盤價） |
| MA200 週期 | `ma200_period` | 200 | 超長期均線（收盤價） |
| 報酬回看天數 | `lookback_20d` | 20 | 近 20 日漲跌幅（收盤價） |
| 52 週天數 | `lookback_52w` | 260 | 52 週 = 52×5 = 260 交易日 |
| 新高容差 | `new_high_tolerance` | 0.01 | 1%（台股美股相同） |

### 10.2 三線開花參數

| 參數 | 設定鍵 | 值 | 說明 |
|------|--------|-----|------|
| MA8 週期 | `ma8_period` | 8 | 短期均線（收盤價） |
| MA21 週期 | `ma21_period` | 21 | 中短期均線（收盤價） |
| MA55 週期 | `ma55_period` | 55 | 中期均線（收盤價） |

### 10.3 重試參數

| 參數 | 值 | 說明 |
|------|-----|------|
| 最大重試次數 | 3 | 可透過環境變數調整 |
| 第 1 次間隔 | 300 秒 | 5 分鐘 |
| 第 2 次間隔 | 600 秒 | 10 分鐘 |
| 第 3 次間隔 | 3600 秒 | 1 小時 |

### 10.4 美股批次下載參數

| 參數 | 預設值 | 環境變數 | 說明 |
|------|-------|---------|------|
| 批次大小 | 40 | `US_BATCH_SIZE` | 每批下載股票數 |
| 批次間隔 | 15 秒 | `US_BATCH_INTERVAL` | 批次間等待時間 |
| 平行 Workers | 2 | `US_MAX_WORKERS` | 並行下載執行緒數 |

### 10.5 API 限流參數

| 參數 | 值 | 說明 |
|------|-----|------|
| FinMind 呼叫上限 | 600 次/小時 | Token Bucket 控制 |
| 台股 yfinance 備援 | 自適應 | 動態調整批次大小 |
| 美股 yfinance | 自律控速 | 依批次設定限制 |

---

## 11. TradingView 指標

### 11.1 Timeframe 自動轉換

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

### 11.2 指標對照

| TradingView 指標 | Python 對應 | 信號標記 |
|-----------------|------------|---------|
| ZF VCP 強勢 | `vcp_filter._filter_strong_list` | 綠色 ▲「強」 |
| ZF VCP 新高 | `vcp_filter._filter_new_high_list` | 黃色 ◆「高」 |
| ZF 三線開花 | `sanxian_filter.filter` | 藍色 ●（無文字） |

---

## 12. 參考文件

- [需求規格](./01-requirements-spec.md)
- [技術架構](./02-architecture.md)
- [資料規格](./03-data-spec.md)
- [操作指南](./05-operations-guide.md)
