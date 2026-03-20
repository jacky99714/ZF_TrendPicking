"""
重新計算並匯出所有已存在篩選日期的台股 VCP/三線開花結果到 Google Sheet

用途：修正零價異常後，重新計算 return_20d 並更新 Sheet

使用方式：
    source .venv/bin/activate
    python scripts/reexport_all_dates.py
"""
import sqlite3
import sys
import time
from datetime import date, timedelta
from pathlib import Path

# 確保可以從 scripts/ 目錄匯入專案模組
sys.path.insert(0, str(Path(__file__).parent.parent))

from loguru import logger

from calculators.moving_average import MovingAverageCalculator
from calculators.vcp_filter import VCPFilter, calculate_market_return
from calculators.sanxian_filter import SanxianFilter
from data.sqlite_database import SQLiteDatabase
from exporters.google_sheet import GoogleSheetExporter


DB_PATH = Path(__file__).parent.parent / "data" / "zf_trend.db"

# Google API 限流保護
DELAY_BETWEEN_DATES = 5  # 每個日期間隔秒數


def get_filter_dates() -> list[str]:
    """取得 DB 中所有篩選日期"""
    conn = sqlite3.connect(str(DB_PATH))
    cur = conn.execute(
        "SELECT DISTINCT filter_date FROM filter_result ORDER BY filter_date"
    )
    dates = [row[0] for row in cur.fetchall()]
    conn.close()
    return dates


def reexport_date(
    target_date: date,
    db: SQLiteDatabase,
    vcp_filter: VCPFilter,
    sanxian_filter: SanxianFilter,
    exporter: GoogleSheetExporter,
):
    """重新計算並匯出單一日期"""
    logger.info(f"=== 重新計算 {target_date} ===")

    # 取得歷史資料
    start_date = target_date - timedelta(days=365)
    price_df = db.get_daily_prices(start_date, target_date)
    market_df = db.get_market_index(start_date, target_date)

    if price_df.empty:
        logger.warning(f"{target_date}: 無歷史資料，跳過")
        return

    # 計算大盤報酬率
    market_return = calculate_market_return(market_df, target_date, lookback=20)
    logger.info(f"大盤 20 日報酬率: {market_return:.2%}")

    # 過濾只保留 stock_info 中的股票
    stock_info = db.get_stock_info_dict()
    valid_stock_ids = set(stock_info.keys())
    price_df = price_df[price_df["stock_id"].isin(valid_stock_ids)]

    # VCP 篩選
    vcp_df = vcp_filter.filter(price_df, market_return, target_date)
    vcp_results = _enrich_results(vcp_df, stock_info)

    # 三線開花篩選
    sanxian_df = sanxian_filter.filter(price_df, target_date)
    sanxian_results = _enrich_results(sanxian_df, stock_info)

    # 儲存篩選結果到 DB
    db.save_filter_results(vcp_results, "vcp", target_date)
    db.save_filter_results(sanxian_results, "sanxian", target_date)

    # 匯出到 Google Sheet
    if vcp_results:
        exporter.export_vcp(vcp_results, target_date)
    if sanxian_results:
        exporter.export_sanxian(sanxian_results, target_date)

    # 匯出驗證資料
    vcp_verification = _prepare_vcp_verification(
        price_df, market_return, target_date, vcp_filter
    )
    sanxian_verification = _prepare_sanxian_verification(price_df, target_date)

    if vcp_verification or sanxian_verification:
        exporter.export_verification(
            vcp_verification, sanxian_verification, target_date, market_return
        )

    logger.info(
        f"{target_date} 完成: VCP {len(vcp_results)} 檔, "
        f"三線開花 {len(sanxian_results)} 檔"
    )


def _enrich_results(df, stock_info: dict) -> list[dict]:
    """補充股票基本資料（從 DailyTask._enrich_results 複製）"""
    import pandas as pd

    if df.empty:
        return []

    def _safe_str(val, default="-"):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return default
        return str(val)

    results = []
    for _, row in df.iterrows():
        stock_id = row["stock_id"]
        info = stock_info.get(stock_id, {})
        result = row.to_dict()
        result = {
            k: (v if not (isinstance(v, float) and pd.isna(v)) else None)
            for k, v in result.items()
        }
        result.update({
            "stock_name": _safe_str(info.get("stock_name"), ""),
            "company_name": _safe_str(info.get("stock_name"), ""),
            "industry_category": _safe_str(info.get("industry_category")),
            "industry_category2": _safe_str(info.get("industry_category2")),
            "product_mix": "-",
        })
        results.append(result)
    return results


def _prepare_vcp_verification(price_df, market_return, target_date, vcp_filter):
    """準備 VCP 驗證資料"""
    import pandas as pd
    import numpy as np

    if price_df.empty:
        return []

    df = MovingAverageCalculator.prepare_vcp_data(price_df)
    if df.empty:
        return []

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] == target_date].copy()
    if df.empty:
        return []

    close = df["close_price"].fillna(0)
    ma50 = df["ma50"].fillna(float("inf"))
    ma150 = df["ma150"].fillna(float("inf"))
    ma200 = df["ma200"].fillna(float("inf"))

    df["cond1"] = close > ma50
    df["cond2"] = ma50 > ma150
    df["cond3"] = ma150 > ma200
    df["cond4"] = df["ma200_slope_20d"].fillna(-1) > 0
    df["cond5"] = df["return_20d"].fillna(-float("inf")) > market_return
    df["is_strong"] = df["cond1"] & df["cond2"] & df["cond3"] & df["cond4"] & df["cond5"]

    high_5d = df["high_5d"].fillna(0)
    high_252d = df["high_252d"].fillna(1).replace(0, 1)
    df["gap_to_52w_high"] = abs(high_5d / high_252d - 1)
    df["is_new_high"] = (
        df["gap_to_52w_high"] <= vcp_filter.new_high_tolerance
    ) & df["cond5"]
    df["is_vcp"] = df["is_strong"] | df["is_new_high"]

    return df.to_dict("records")


def _prepare_sanxian_verification(price_df, target_date):
    """準備三線開花驗證資料"""
    import pandas as pd

    if price_df.empty:
        return []

    df = MovingAverageCalculator.prepare_sanxian_data(price_df)
    if df.empty:
        return []

    df["date"] = pd.to_datetime(df["date"]).dt.date
    df = df[df["date"] == target_date].copy()

    return df.to_dict("records") if not df.empty else []


def main():
    dates = get_filter_dates()
    if not dates:
        logger.info("DB 中無篩選結果")
        return

    logger.info(f"找到 {len(dates)} 個篩選日期: {dates}")

    db = SQLiteDatabase()
    vcp_filter = VCPFilter()
    sanxian_filter = SanxianFilter()
    exporter = GoogleSheetExporter()

    if not exporter.health_check():
        logger.error("Google Sheet 未連線，無法匯出")
        return

    for i, date_str in enumerate(dates):
        target = date.fromisoformat(date_str)
        reexport_date(target, db, vcp_filter, sanxian_filter, exporter)

        if i < len(dates) - 1:
            logger.info(f"等待 {DELAY_BETWEEN_DATES} 秒（避免 Google API 限流）...")
            time.sleep(DELAY_BETWEEN_DATES)

    logger.info(f"=== 全部完成：{len(dates)} 個日期已重新匯出 ===")


if __name__ == "__main__":
    main()
