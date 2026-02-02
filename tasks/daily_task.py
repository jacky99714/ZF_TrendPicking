"""
每日任務
使用 HybridClient (FinMind + yfinance) + SQLite 架構
"""
from datetime import date, timedelta
from typing import Optional

import pandas as pd
from loguru import logger

from api.hybrid_client import HybridClient
from data.sqlite_database import SQLiteDatabase
from calculators.vcp_filter import VCPFilter, calculate_market_return
from calculators.sanxian_filter import SanxianFilter
from exporters.google_sheet import GoogleSheetExporter
from utils.trading_calendar import TradingCalendar


class DailyTask:
    """
    每日任務

    執行流程:
    1. 取得當日股價（yfinance 批量查詢）
    2. 取得當日大盤指數
    3. 更新 SQLite 資料庫
    4. 執行 VCP 篩選
    5. 執行三線開花篩選
    6. 匯出至 Google Sheet
    """

    def __init__(
        self,
        client: Optional[HybridClient] = None,
        db: Optional[SQLiteDatabase] = None,
        exporter: Optional[GoogleSheetExporter] = None
    ):
        """
        初始化每日任務

        Args:
            client: HybridClient API 客戶端
            db: SQLite 資料庫連線
            exporter: Google Sheet 匯出器
        """
        self.client = client or HybridClient()
        self.db = db or SQLiteDatabase()
        self.exporter = exporter or GoogleSheetExporter()

        # 篩選器
        self.vcp_filter = VCPFilter()
        self.sanxian_filter = SanxianFilter()

    def run(
        self,
        target_date: Optional[date] = None,
        skip_non_trading_day: bool = True
    ) -> dict:
        """
        執行每日任務

        Args:
            target_date: 目標日期（預設為今天）
            skip_non_trading_day: 是否在非交易日跳過執行（預設 True）

        Returns:
            執行結果統計
        """
        original_date = target_date or date.today()

        # 檢查是否為交易日
        if not TradingCalendar.is_trading_day(original_date):
            if skip_non_trading_day:
                logger.info(f"{original_date} 非交易日，跳過執行")
                return {
                    "date": original_date,
                    "success": True,  # 跳過也算成功
                    "skipped": True,
                    "reason": "非交易日",
                    "price_count": 0,
                    "vcp_count": 0,
                    "sanxian_count": 0,
                    "errors": [],
                }
            else:
                # 使用最近的交易日
                target_date = TradingCalendar.get_latest_trading_day(original_date)
                logger.info(f"{original_date} 非交易日，使用最近交易日: {target_date}")
        else:
            target_date = original_date

        logger.info(f"=== 開始執行每日任務: {target_date} ===")

        result = {
            "date": target_date,
            "success": False,
            "skipped": False,
            "price_count": 0,
            "vcp_count": 0,
            "sanxian_count": 0,
            "errors": [],
        }

        try:
            # 確保資料表存在
            self.db.create_tables()

            # Step 1: 確保有股票清單
            stock_info = self.db.get_stock_info_dict()
            if not stock_info:
                logger.info("股票清單為空，先取得股票清單...")
                stock_df = self.client.get_stock_info()
                if not stock_df.empty:
                    self.db.upsert_stock_info(stock_df)
                    stock_info = self.db.get_stock_info_dict()

            if not stock_info:
                result["errors"].append("無法取得股票清單")
                logger.error("無法取得股票清單，任務結束")
                return result

            # Step 2: 取得並儲存股價（批量查詢）
            price_count = self._fetch_and_save_prices(target_date, stock_info)
            result["price_count"] = price_count

            if price_count == 0:
                result["errors"].append("無股價資料（可能非交易日）")
                logger.warning("無股價資料，任務結束")
                return result

            # Step 3: 取得並儲存大盤指數
            market_count = self._fetch_and_save_market_index(target_date)
            if market_count == 0:
                logger.warning("無大盤指數資料，VCP 篩選可能不準確")

            # Step 4: 執行篩選
            vcp_results, sanxian_results, market_return = self._run_filters(target_date)
            result["vcp_count"] = len(vcp_results)
            result["sanxian_count"] = len(sanxian_results)

            # Step 5: 匯出至 Google Sheet（包含驗證資料）
            self._export_to_sheet(target_date, vcp_results, sanxian_results, market_return)

            result["success"] = True
            logger.info(
                f"=== 每日任務完成: VCP {len(vcp_results)} 檔, "
                f"三線開花 {len(sanxian_results)} 檔 ==="
            )

        except Exception as e:
            logger.error(f"每日任務失敗: {e}")
            result["errors"].append(str(e))

        # SPEC: 將錯誤日誌寫入 Google Sheet「台股更新紀錄」
        error_logs = self.client.get_error_log()
        if error_logs and self.exporter.health_check():
            self.exporter.log_error_to_sheet(error_logs)

        return result

    def _fetch_and_save_prices(self, target_date: date, stock_info: dict) -> int:
        """取得並儲存股價（批量查詢）"""
        logger.info("取得當日股價...")

        # 取得所有股票代號和市場類型
        stock_ids = list(stock_info.keys())
        market_types = self.db.get_stock_market_types()

        # 使用 yfinance 批量查詢
        price_df = self.client.get_stock_price(
            start_date=target_date,
            end_date=target_date,
            stock_ids=stock_ids,
            market_types=market_types
        )

        if price_df.empty:
            return 0

        # 儲存至資料庫
        count = self.db.upsert_daily_price(price_df)
        return count

    def _fetch_and_save_market_index(self, target_date: date) -> int:
        """取得並儲存大盤指數"""
        logger.info("取得大盤指數...")

        market_df = self.client.get_market_index(target_date)

        if market_df.empty:
            logger.warning("無大盤指數資料")
            return 0

        count = self.db.upsert_market_index(market_df)
        return count

    def _run_filters(self, target_date: date) -> tuple[list[dict], list[dict], float]:
        """執行篩選

        Returns:
            (vcp_results, sanxian_results, market_return_20d)
        """
        logger.info("執行篩選...")

        # 取得計算所需的歷史資料（252 天）
        start_date = target_date - timedelta(days=365)
        price_df = self.db.get_daily_prices(start_date, target_date)
        market_df = self.db.get_market_index(start_date, target_date)

        if price_df.empty:
            logger.warning("無足夠歷史資料")
            return [], [], 0.0

        # 計算大盤報酬率
        market_return = calculate_market_return(market_df, target_date, lookback=20)
        logger.info(f"大盤 20 日報酬率: {market_return:.2%}")

        # 取得股票基本資料
        stock_info = self.db.get_stock_info_dict()
        if not stock_info:
            logger.warning("股票基本資料為空，請先執行 'python main.py init'")

        # 只保留 stock_info 中的股票（過濾掉 ETF、權證等）
        valid_stock_ids = set(stock_info.keys())
        before_filter = price_df["stock_id"].nunique()
        price_df = price_df[price_df["stock_id"].isin(valid_stock_ids)]
        after_filter = price_df["stock_id"].nunique()
        logger.info(f"過濾股票: {before_filter} -> {after_filter} 檔（排除 ETF/權證）")

        # VCP 篩選
        vcp_df = self.vcp_filter.filter(price_df, market_return, target_date)
        vcp_results = self._enrich_results(vcp_df, stock_info)

        # 三線開花篩選
        sanxian_df = self.sanxian_filter.filter(price_df, target_date)
        sanxian_results = self._enrich_results(sanxian_df, stock_info)

        # 儲存篩選結果
        self.db.save_filter_results(vcp_results, "vcp", target_date)
        self.db.save_filter_results(sanxian_results, "sanxian", target_date)

        # 準備驗證資料
        self._vcp_verification_data = self._prepare_vcp_verification(
            price_df, market_return, target_date
        )
        self._sanxian_verification_data = self._prepare_sanxian_verification(
            price_df, target_date
        )

        return vcp_results, sanxian_results, market_return

    def _enrich_results(
        self,
        df,
        stock_info: dict[str, dict]
    ) -> list[dict]:
        """補充股票基本資料"""
        if df.empty:
            return []

        def _safe_str(val, default="-"):
            """將 NaN/None 轉為預設字串"""
            if val is None or (isinstance(val, float) and pd.isna(val)):
                return default
            return str(val)

        results = []
        for _, row in df.iterrows():
            stock_id = row["stock_id"]
            info = stock_info.get(stock_id, {})

            result = row.to_dict()
            # 清理 row.to_dict() 中的 NaN 值（pandas 將 NULL 轉為 float NaN）
            result = {k: (v if not (isinstance(v, float) and pd.isna(v)) else None)
                      for k, v in result.items()}
            result.update({
                "stock_name": _safe_str(info.get("stock_name"), ""),
                "company_name": _safe_str(info.get("stock_name"), ""),
                "industry_category": _safe_str(info.get("industry_category")),
                "industry_category2": _safe_str(info.get("industry_category2")),
                "product_mix": "-",
            })
            results.append(result)

        return results

    def _prepare_vcp_verification(
        self,
        price_df: pd.DataFrame,
        market_return: float,
        target_date: date
    ) -> list[dict]:
        """
        準備 VCP 驗證資料（包含所有計算欄位）
        """
        from calculators.moving_average import MovingAverageCalculator

        if price_df.empty:
            return []

        # 準備計算資料
        df = MovingAverageCalculator.prepare_vcp_data(price_df)
        if df.empty:
            return []

        # 取得目標日期的資料
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["date"] == target_date].copy()

        if df.empty:
            return []

        # 計算所有條件
        close = df["close_price"].fillna(0)
        ma50 = df["ma50"].fillna(float("inf"))
        ma150 = df["ma150"].fillna(float("inf"))
        ma200 = df["ma200"].fillna(float("inf"))

        df["cond1"] = close > ma50
        df["cond2"] = ma50 > ma150
        df["cond3"] = ma150 > ma200
        df["cond4"] = df["ma200_slope_20d"].fillna(-1) > 0
        df["cond5"] = df["return_20d"].fillna(-float("inf")) > market_return

        # 強勢清單
        df["is_strong"] = df["cond1"] & df["cond2"] & df["cond3"] & df["cond4"] & df["cond5"]

        # 新高清單
        high_5d = df["high_5d"].fillna(0)
        high_252d = df["high_252d"].fillna(1).replace(0, 1)
        df["gap_to_52w_high"] = abs(high_5d / high_252d - 1)
        df["is_new_high"] = (df["gap_to_52w_high"] <= self.vcp_filter.new_high_tolerance) & df["cond5"]

        # VCP = 強勢 OR 新高
        df["is_vcp"] = df["is_strong"] | df["is_new_high"]

        # 輸出所有股票的計算數據供驗證
        return df.to_dict("records")

    def _prepare_sanxian_verification(
        self,
        price_df: pd.DataFrame,
        target_date: date
    ) -> list[dict]:
        """
        準備三線開花驗證資料（包含所有計算欄位）
        """
        from calculators.moving_average import MovingAverageCalculator

        if price_df.empty:
            return []

        # 準備計算資料
        df = MovingAverageCalculator.prepare_sanxian_data(price_df)
        if df.empty:
            return []

        # 取得目標日期的資料
        df["date"] = pd.to_datetime(df["date"]).dt.date
        df = df[df["date"] == target_date].copy()

        if df.empty:
            return []

        # 計算所有條件
        close = df["close_price"].fillna(0)
        ma8 = df["ma8"].fillna(float("inf"))
        ma21 = df["ma21"].fillna(float("inf"))
        ma55 = df["ma55"].fillna(float("inf"))

        df["cond1"] = close > ma8
        df["cond2"] = ma8 > ma21
        df["cond3"] = ma21 > ma55
        df["cond4"] = close >= df["high_55d"].fillna(float("inf"))

        df["is_sanxian"] = df["cond1"] & df["cond2"] & df["cond3"] & df["cond4"]

        # 計算差距比例
        second_high = df["second_high_55d"].fillna(1).replace(0, 1)
        df["gap_ratio"] = (close / second_high - 1)

        # 輸出所有股票的計算數據供驗證
        return df.to_dict("records")

    def _export_to_sheet(
        self,
        target_date: date,
        vcp_results: list[dict],
        sanxian_results: list[dict],
        market_return: float = 0.0
    ):
        """匯出至 Google Sheet"""
        if not self.exporter.health_check():
            logger.warning("Google Sheet 未連線，跳過匯出")
            return

        # 匯出 VCP
        if vcp_results:
            self.exporter.export_vcp(vcp_results, target_date)

        # 匯出三線開花
        if sanxian_results:
            self.exporter.export_sanxian(sanxian_results, target_date)

        # 匯出驗證資料
        vcp_verification = getattr(self, "_vcp_verification_data", [])
        sanxian_verification = getattr(self, "_sanxian_verification_data", [])

        if vcp_verification or sanxian_verification:
            self.exporter.export_verification(
                vcp_verification,
                sanxian_verification,
                target_date,
                market_return
            )


def run_daily_task(target_date: Optional[date] = None) -> dict:
    """
    執行每日任務的便捷函數

    Args:
        target_date: 目標日期

    Returns:
        執行結果
    """
    task = DailyTask()
    return task.run(target_date)
