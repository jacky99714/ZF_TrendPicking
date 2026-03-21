"""
批次補齊過去兩年每週五的篩選結果

步驟：
1. 補齊大盤指數（yfinance 一次性下載）
2. 計算每個星期五的 VCP + 三線開花篩選
3. 結果存入 filter_result 表
4. 匯出 JSON 供網站使用

用法：
    python scripts/backfill_fridays.py
"""
import sys
import warnings
from datetime import date, timedelta
from pathlib import Path

warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", category=FutureWarning)

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

import pandas as pd
import yfinance as yf
from loguru import logger

from calculators.moving_average import MovingAverageCalculator
from calculators.sanxian_filter import SanxianFilter
from calculators.vcp_filter import VCPFilter, calculate_market_return
from data.sqlite_database import SQLiteDatabase
from utils.trading_calendar import TradingCalendar

# 設定日誌
logger.remove()
logger.add(sys.stderr, level="INFO", format="{time:HH:mm:ss} | {level} | {message}")


def get_all_fridays(start: date, end: date) -> list[date]:
    """取得日期範圍內所有星期五"""
    d = start
    while d.weekday() != 4:
        d += timedelta(days=1)
    fridays = []
    while d <= end:
        fridays.append(d)
        d += timedelta(days=7)
    return fridays


def backfill_market_index(db: SQLiteDatabase, start: date, end: date):
    """從 yfinance 補齊大盤指數"""
    logger.info(f"補齊大盤指數: {start} ~ {end}")

    # 檢查現有資料
    existing_df = db.get_market_index(start, end)
    existing_dates = set()
    if not existing_df.empty:
        existing_dates = set(pd.to_datetime(existing_df["date"]).dt.date)
    logger.info(f"已有 {len(existing_dates)} 天大盤資料")

    # 從 yfinance 下載 TAIEX (^TWII)
    ticker = yf.Ticker("^TWII")
    hist = ticker.history(start=start.isoformat(), end=(end + timedelta(days=1)).isoformat())

    if hist.empty:
        logger.warning("無法下載大盤指數")
        return

    records = []
    for idx, row in hist.iterrows():
        d = idx.date()
        if d not in existing_dates:
            records.append({"date": d, "taiex": float(row["Close"])})

    if records:
        market_df = pd.DataFrame(records)
        count = db.upsert_market_index(market_df)
        logger.info(f"新增 {count} 天大盤指數")
    else:
        logger.info("大盤指數已完整，無需補齊")


def run_filters_for_date(
    db: SQLiteDatabase,
    target_date: date,
    vcp_filter: VCPFilter,
    sanxian_filter: SanxianFilter,
    stock_info: dict,
) -> dict:
    """對指定日期執行篩選（不抓股價、不匯出 Sheet）"""

    # 取得歷史股價（往前 365 天）
    start_date = target_date - timedelta(days=400)
    price_df = db.get_daily_prices(start_date, target_date)
    market_df = db.get_market_index(start_date, target_date)

    if price_df.empty:
        return {"date": target_date, "vcp": 0, "sanxian": 0, "skipped": "no_price"}

    # 過濾只保留主檔中的股票
    valid_ids = set(stock_info.keys())
    price_df = price_df[price_df["stock_id"].isin(valid_ids)]

    if price_df.empty:
        return {"date": target_date, "vcp": 0, "sanxian": 0, "skipped": "no_valid_stock"}

    # 檢查目標日期是否有股價資料
    price_dates = pd.to_datetime(price_df["date"]).dt.date
    if target_date not in price_dates.values:
        return {"date": target_date, "vcp": 0, "sanxian": 0, "skipped": "no_data_on_date"}

    # 計算大盤報酬率
    market_return = calculate_market_return(market_df, target_date, lookback=20)

    # VCP 篩選
    vcp_df = vcp_filter.filter(price_df, market_return, target_date)
    vcp_results = _enrich(vcp_df, stock_info)

    # 三線開花篩選
    sanxian_df = sanxian_filter.filter(price_df, target_date)
    sanxian_results = _enrich(sanxian_df, stock_info)

    # 存入 DB
    if vcp_results:
        db.save_filter_results(vcp_results, "vcp", target_date)
    if sanxian_results:
        db.save_filter_results(sanxian_results, "sanxian", target_date)

    return {
        "date": target_date,
        "vcp": len(vcp_results),
        "sanxian": len(sanxian_results),
        "skipped": None,
    }


def _enrich(df: pd.DataFrame, stock_info: dict) -> list[dict]:
    """補充股票基本資料到篩選結果"""
    if df.empty:
        return []

    results = []
    for _, row in df.iterrows():
        sid = row["stock_id"]
        info = stock_info.get(sid, {})
        result = row.to_dict()
        # 清理 NaN
        result = {
            k: (v if not (isinstance(v, float) and pd.isna(v)) else None)
            for k, v in result.items()
        }
        result.update({
            "stock_name": info.get("stock_name", ""),
            "company_name": info.get("stock_name", ""),
            "industry_category": info.get("industry_category", "-") or "-",
            "industry_category2": info.get("industry_category2", "-") or "-",
            "product_mix": "-",
        })
        results.append(result)
    return results


def main():
    end_date = date(2026, 3, 21)
    start_date = date(2024, 3, 22)

    fridays = get_all_fridays(start_date, end_date)
    logger.info(f"共 {len(fridays)} 個星期五需要處理 ({start_date} ~ {end_date})")

    db = SQLiteDatabase()
    db.create_tables()

    # Step 1: 補齊大盤指數
    backfill_market_index(db, start_date - timedelta(days=30), end_date)

    # Step 2: 檢查哪些星期五已有篩選結果
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT filter_date FROM filter_result")
    existing_dates = {row[0] for row in cursor.fetchall()}
    conn.close()

    # 過濾已有結果的日期（但也包含交易日判斷）
    trading_fridays = []
    for f in fridays:
        if TradingCalendar.is_trading_day(f):
            trading_fridays.append(f)
        else:
            logger.debug(f"{f} 非交易日，跳過")

    need_process = [f for f in trading_fridays if f.isoformat() not in existing_dates]
    logger.info(
        f"交易日星期五: {len(trading_fridays)} 天, "
        f"已有結果: {len(trading_fridays) - len(need_process)} 天, "
        f"需處理: {len(need_process)} 天"
    )

    if not need_process:
        logger.info("所有星期五已有篩選結果，無需處理")
        return

    # Step 3: 取得股票基本資料
    stock_info = db.get_stock_info_dict()
    logger.info(f"股票主檔: {len(stock_info)} 檔")

    # Step 4: 逐一計算每個星期五
    vcp_filter = VCPFilter()
    sanxian_filter = SanxianFilter()

    total_vcp = 0
    total_sanxian = 0
    skipped = 0

    for i, friday in enumerate(need_process, 1):
        logger.info(f"[{i}/{len(need_process)}] 處理 {friday}...")
        result = run_filters_for_date(db, friday, vcp_filter, sanxian_filter, stock_info)

        if result["skipped"]:
            logger.warning(f"  跳過: {result['skipped']}")
            skipped += 1
        else:
            logger.info(f"  VCP: {result['vcp']} 檔, 三線開花: {result['sanxian']} 檔")
            total_vcp += result["vcp"]
            total_sanxian += result["sanxian"]

    logger.info("=" * 50)
    logger.info(f"完成！處理 {len(need_process)} 天, 跳過 {skipped} 天")
    logger.info(f"VCP 總計: {total_vcp} 筆, 三線開花總計: {total_sanxian} 筆")

    # Step 5: 匯出 JSON
    logger.info("匯出 JSON...")
    from scripts.export_to_json import main as export_json
    export_json()

    logger.info("全部完成！")


if __name__ == "__main__":
    main()
