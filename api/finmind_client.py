"""
FinMind API 客戶端
"""
from datetime import date, datetime
from typing import Optional

import pandas as pd
import requests
from loguru import logger

from config.settings import (
    FINMIND_API_URL,
    FINMIND_TOKEN,
    API_CALLS_PER_HOUR,
    RETRY_CONFIG,
)
from api.rate_limiter import RateLimiter, RetryHandler


class FinMindError(Exception):
    """FinMind API 錯誤"""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class FinMindClient:
    """
    FinMind API 客戶端

    封裝 FinMind API 呼叫，包含:
    - 自動限流
    - 錯誤重試
    - 資料轉換
    """

    def __init__(
        self,
        token: Optional[str] = None,
        calls_per_hour: Optional[int] = None
    ):
        """
        初始化客戶端

        Args:
            token: FinMind API Token
            calls_per_hour: 每小時呼叫限制
        """
        self.token = token or FINMIND_TOKEN
        self.api_url = FINMIND_API_URL

        # 初始化限流器
        self.rate_limiter = RateLimiter(
            calls_per_hour=calls_per_hour or API_CALLS_PER_HOUR
        )

        # 初始化重試處理器
        self.retry_handler = RetryHandler(
            max_retries=RETRY_CONFIG["max_retries"],
            retry_intervals=RETRY_CONFIG["retry_intervals"]
        )

        # 請求計數
        self._request_count = 0
        self._error_log: list[dict] = []

        logger.info("FinMindClient 初始化完成")

    def _make_request(self, params: dict) -> dict:
        """
        發送 API 請求（含限流與重試）

        Args:
            params: 請求參數

        Returns:
            API 回應資料

        Raises:
            FinMindError: API 錯誤
        """
        if self.token:
            params["token"] = self.token

        retry_count = 0
        last_error = None

        while True:
            # 等待限流
            with self.rate_limiter:
                try:
                    self._request_count += 1

                    response = requests.get(
                        self.api_url,
                        params=params,
                        timeout=30
                    )

                    # 成功
                    if response.status_code == 200:
                        data = response.json()

                        # 檢查 API 層級錯誤
                        if data.get("status") != 200:
                            raise FinMindError(
                                f"API Error: {data.get('msg', 'Unknown')}",
                                status_code=data.get("status")
                            )

                        return data

                    # 判斷是否重試
                    if self.retry_handler.should_retry(
                        response.status_code, retry_count
                    ):
                        self._log_error(
                            params, response.status_code, retry_count
                        )
                        self.retry_handler.wait_for_retry(retry_count)
                        retry_count += 1
                        continue

                    # 不重試的錯誤
                    self._log_error(params, response.status_code, retry_count)
                    raise FinMindError(
                        f"HTTP {response.status_code}: {response.text}",
                        status_code=response.status_code
                    )

                except requests.RequestException as e:
                    last_error = e

                    if retry_count < self.retry_handler.max_retries:
                        logger.warning(f"請求異常: {e}, 準備重試...")
                        self.retry_handler.wait_for_retry(retry_count)
                        retry_count += 1
                        continue

                    raise FinMindError(f"請求失敗: {e}")

    def _log_error(
        self,
        params: dict,
        status_code: int,
        retry_count: int
    ):
        """記錄錯誤"""
        error_entry = {
            "time": datetime.now().isoformat(),
            "params": {k: v for k, v in params.items() if k != "token"},
            "status_code": status_code,
            "retry_count": retry_count,
        }
        self._error_log.append(error_entry)

        logger.error(
            f"API 錯誤 - 狀態碼: {status_code}, "
            f"重試次數: {retry_count}, "
            f"參數: {error_entry['params']}"
        )

    def get_stock_info(self) -> pd.DataFrame:
        """
        取得台股股票清單

        Returns:
            DataFrame with columns:
            - stock_id: 股票代號
            - stock_name: 股票名稱
            - industry_category: 產業分類
            - type: 股票類型
        """
        logger.info("取得台股股票清單...")

        params = {"dataset": "TaiwanStockInfo"}
        data = self._make_request(params)

        # 安全取得 data 欄位
        raw_data = data.get("data", [])
        if not raw_data:
            logger.warning("API 回應無資料")
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)

        # 檢查必要欄位是否存在
        required_cols = ["stock_id", "stock_name", "type"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            logger.warning(f"股票清單資料缺少欄位: {missing_cols}")
            return pd.DataFrame()

        # 過濾只保留上市/上櫃股票
        df = df[df["type"].isin(["twse", "tpex"])]

        # SPEC: 須扣除ETF等其它商品
        # 台股 ETF 通常股票代號為 00xx, 006xxx 等格式（以 0 開頭且長度 >= 4）
        # 排除以 0 開頭且長度大於等於 4 的代號（如 0050, 006208）
        # 但保留一般股票（如 1101 大同等開頭為 1-9 的代號）
        original_count = len(df)
        df = df[~df["stock_id"].str.match(r"^0\d{3,}")]
        filtered_count = original_count - len(df)
        if filtered_count > 0:
            logger.info(f"已過濾 {filtered_count} 檔 ETF/其他商品")

        # 過濾指數資料（industry_category 為 'Index' 或 '大盤'）
        # 過濾權證（industry_category 為 '所有證券'，通常以 7 開頭的 6 位數）
        # 以及非數字的代號（如 ElectricMachinery, TPEx 等）
        before_index_filter = len(df)
        df = df[~df["industry_category"].isin(["Index", "大盤", "所有證券"])]
        df = df[df["stock_id"].str.match(r"^\d{4,6}$")]  # 只保留 4-6 位純數字代號
        index_filtered = before_index_filter - len(df)
        if index_filtered > 0:
            logger.info(f"已過濾 {index_filtered} 檔指數/權證/非股票資料")

        # 去除重複的 stock_id（保留第一筆）
        before_dedup = len(df)
        df = df.drop_duplicates(subset=["stock_id"], keep="first")
        dedup_count = before_dedup - len(df)
        if dedup_count > 0:
            logger.info(f"已移除 {dedup_count} 筆重複資料")

        logger.info(f"取得 {len(df)} 檔股票資訊")
        return df

    def get_stock_price(
        self,
        start_date: date,
        end_date: Optional[date] = None,
        stock_id: Optional[str] = None
    ) -> pd.DataFrame:
        """
        取得股票每日股價

        Args:
            start_date: 開始日期
            end_date: 結束日期（預設為 start_date）
            stock_id: 指定股票代號（可選，不指定則取得全部）

        Returns:
            DataFrame with columns:
            - date: 日期
            - stock_id: 股票代號
            - open: 開盤價
            - high: 最高價
            - low: 最低價
            - close: 收盤價
            - volume: 成交量
        """
        if end_date is None:
            end_date = start_date

        # 驗證日期範圍
        if start_date > end_date:
            logger.warning(f"日期範圍錯誤: start_date ({start_date}) > end_date ({end_date})，自動交換")
            start_date, end_date = end_date, start_date

        logger.info(
            f"取得股價資料: {start_date} ~ {end_date}"
            + (f", 股票: {stock_id}" if stock_id else " (全部)")
        )

        params = {
            "dataset": "TaiwanStockPrice",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        if stock_id:
            params["data_id"] = stock_id

        data = self._make_request(params)

        # 安全取得 data 欄位
        raw_data = data.get("data", [])
        if not raw_data:
            logger.warning("無股價資料")
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)

        # 檢查必要欄位是否存在
        required_cols = ["stock_id", "date", "open", "max", "min", "close"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            logger.warning(f"股價資料缺少欄位: {missing_cols}")
            return pd.DataFrame()

        # 重新命名欄位
        df = df.rename(columns={
            "Trading_Volume": "volume",
            "max": "high",
            "min": "low",
        })

        # 轉換日期
        df["date"] = pd.to_datetime(df["date"]).dt.date

        logger.info(f"取得 {len(df)} 筆股價資料")
        return df

    def get_market_index(
        self,
        start_date: date,
        end_date: Optional[date] = None
    ) -> pd.DataFrame:
        """
        取得大盤指數 (加權指數)

        Args:
            start_date: 開始日期
            end_date: 結束日期

        Returns:
            DataFrame with columns:
            - date: 日期
            - taiex: 加權指數
        """
        if end_date is None:
            end_date = start_date

        # 驗證日期範圍
        if start_date > end_date:
            logger.warning(f"日期範圍錯誤: start_date ({start_date}) > end_date ({end_date})，自動交換")
            start_date, end_date = end_date, start_date

        logger.info(f"取得大盤指數: {start_date} ~ {end_date}")

        params = {
            "dataset": "TaiwanStockTotalReturnIndex",
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
        }

        data = self._make_request(params)

        # 安全取得 data 欄位
        raw_data = data.get("data", [])
        if not raw_data:
            logger.warning("無大盤指數資料")
            return pd.DataFrame()

        df = pd.DataFrame(raw_data)

        # 檢查必要欄位是否存在
        required_cols = ["stock_id", "date", "price"]
        missing_cols = [c for c in required_cols if c not in df.columns]
        if missing_cols:
            logger.warning(f"大盤指數資料缺少欄位: {missing_cols}")
            return pd.DataFrame()

        # 過濾只要加權報酬指數
        df = df[df["stock_id"] == "TAIEX"]

        if df.empty:
            logger.warning("無 TAIEX 指數資料")
            return pd.DataFrame()

        # 轉換日期
        df["date"] = pd.to_datetime(df["date"]).dt.date

        # 重新命名
        df = df.rename(columns={"price": "taiex"})
        df = df[["date", "taiex"]]

        logger.info(f"取得 {len(df)} 筆大盤指數")
        return df

    def get_stats(self) -> dict:
        """取得客戶端統計資訊"""
        return {
            "total_requests": self._request_count,
            "error_count": len(self._error_log),
            "rate_limiter": self.rate_limiter.get_stats(),
        }

    def get_error_log(self) -> list[dict]:
        """取得錯誤日誌"""
        return self._error_log.copy()
