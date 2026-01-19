"""
每日任務
"""
from datetime import date, timedelta
from typing import Optional

from loguru import logger

from api.finmind_client import FinMindClient
from data.database import Database
from calculators.vcp_filter import VCPFilter, calculate_market_return
from calculators.sanxian_filter import SanxianFilter
from exporters.google_sheet import GoogleSheetExporter
from utils.trading_calendar import TradingCalendar


class DailyTask:
    """
    每日任務

    執行流程:
    1. 取得當日股價（API 呼叫）
    2. 取得當日大盤指數（API 呼叫）
    3. 更新資料庫
    4. 執行 VCP 篩選
    5. 執行三線開花篩選
    6. 匯出至 Google Sheet
    """

    def __init__(
        self,
        client: Optional[FinMindClient] = None,
        db: Optional[Database] = None,
        exporter: Optional[GoogleSheetExporter] = None
    ):
        """
        初始化每日任務

        Args:
            client: FinMind API 客戶端
            db: 資料庫連線
            exporter: Google Sheet 匯出器
        """
        self.client = client or FinMindClient()
        self.db = db or Database()
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
            # Step 1: 取得並儲存股價
            price_count = self._fetch_and_save_prices(target_date)
            result["price_count"] = price_count

            if price_count == 0:
                result["errors"].append("無股價資料（可能非交易日）")
                logger.warning("無股價資料，任務結束")
                return result

            # Step 2: 取得並儲存大盤指數
            market_count = self._fetch_and_save_market_index(target_date)
            if market_count == 0:
                logger.warning("無大盤指數資料，VCP 篩選可能不準確")

            # Step 3: 執行篩選
            vcp_results, sanxian_results = self._run_filters(target_date)
            result["vcp_count"] = len(vcp_results)
            result["sanxian_count"] = len(sanxian_results)

            # Step 4: 匯出至 Google Sheet
            self._export_to_sheet(target_date, vcp_results, sanxian_results)

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

    def _fetch_and_save_prices(self, target_date: date) -> int:
        """取得並儲存股價"""
        logger.info("取得當日股價...")

        # 從 API 取得股價
        price_df = self.client.get_stock_price(target_date)

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

    def _run_filters(self, target_date: date) -> tuple[list[dict], list[dict]]:
        """執行篩選"""
        logger.info("執行篩選...")

        # 取得計算所需的歷史資料（252 天）
        start_date = target_date - timedelta(days=365)
        price_df = self.db.get_daily_prices(start_date, target_date)
        market_df = self.db.get_market_index(start_date, target_date)

        if price_df.empty:
            logger.warning("無足夠歷史資料")
            return [], []

        # 計算大盤報酬率
        market_return = calculate_market_return(market_df, target_date, lookback=20)
        logger.info(f"大盤 20 日報酬率: {market_return:.2%}")

        # 取得股票基本資料
        stock_info = self.db.get_stock_info_dict()
        if not stock_info:
            logger.warning("股票基本資料為空，請先執行 'python main.py init' 或 'python main.py monthly'")

        # VCP 篩選
        vcp_df = self.vcp_filter.filter(price_df, market_return, target_date)
        vcp_results = self._enrich_results(vcp_df, stock_info)

        # 三線開花篩選
        sanxian_df = self.sanxian_filter.filter(price_df, target_date)
        sanxian_results = self._enrich_results(sanxian_df, stock_info)

        # 儲存篩選結果
        self.db.save_filter_results(vcp_results, "vcp", target_date)
        self.db.save_filter_results(sanxian_results, "sanxian", target_date)

        return vcp_results, sanxian_results

    def _enrich_results(
        self,
        df,
        stock_info: dict[str, dict]
    ) -> list[dict]:
        """補充股票基本資料"""
        if df.empty:
            return []

        results = []
        for _, row in df.iterrows():
            stock_id = row["stock_id"]
            info = stock_info.get(stock_id, {})

            result = row.to_dict()
            result.update({
                "stock_name": info.get("stock_name", ""),
                "company_name": info.get("stock_name", ""),
                "industry_category": info.get("industry_category", "-"),
                "industry_category2": "-",
                "product_mix": "-",
            })
            results.append(result)

        return results

    def _export_to_sheet(
        self,
        target_date: date,
        vcp_results: list[dict],
        sanxian_results: list[dict]
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
