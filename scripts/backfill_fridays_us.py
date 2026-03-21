"""
批次補齊美股過去兩年每週五的篩選結果

注意：美股股價只從 2025-01-27 開始，需要 MA200（約 200 交易日），
因此只能計算大約 2025-11 以後的篩選結果。
更早的日期會先嘗試計算，資料不足時自動跳過。

用法：
    python scripts/backfill_fridays_us.py
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

from calculators.us_moving_average import USMovingAverageCalculator
from calculators.us_sanxian_filter import USSanxianFilter
from calculators.us_vcp_filter import USVCPFilter
from data.us_database import USSQLiteDatabase
from utils.us_trading_calendar import USMarketCalendar

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


def calculate_us_market_return(
    market_df: pd.DataFrame, target_date: date, lookback: int = 20
) -> float:
    """計算美股大盤 (S&P 500) 報酬率"""
    if market_df.empty:
        return 0.0

    df = market_df.copy()
    df["date"] = pd.to_datetime(df["date"])
    target_dt = pd.to_datetime(target_date)
    df = df.sort_values("date").reset_index(drop=True)

    df_before = df[df["date"] <= target_dt]
    if df_before.empty:
        return 0.0

    target_pos = len(df_before) - 1
    if target_pos < lookback:
        lookback = target_pos
    if lookback == 0:
        return 0.0

    current = df.iloc[target_pos]["sp500"]
    past = df.iloc[target_pos - lookback]["sp500"]

    if pd.isna(current) or pd.isna(past) or past == 0:
        return 0.0

    return float((current - past) / past)


def backfill_us_market_index(db: USSQLiteDatabase, start: date, end: date):
    """從 yfinance 補齊 S&P 500 指數"""
    logger.info(f"補齊美股大盤指數: {start} ~ {end}")

    existing_df = db.get_market_index(start, end)
    existing_dates = set()
    if not existing_df.empty:
        existing_dates = set(pd.to_datetime(existing_df["date"]).dt.date)
    logger.info(f"已有 {len(existing_dates)} 天美股大盤資料")

    # 下載 S&P 500
    ticker = yf.Ticker("^GSPC")
    hist = ticker.history(
        start=start.isoformat(),
        end=(end + timedelta(days=1)).isoformat(),
    )

    if hist.empty:
        logger.warning("無法下載 S&P 500 指數")
        return

    records = []
    for idx, row in hist.iterrows():
        d = idx.date()
        if d not in existing_dates:
            records.append({"date": d, "sp500": float(row["Close"])})

    if records:
        market_df = pd.DataFrame(records)
        count = db.upsert_market_index(market_df)
        logger.info(f"新增 {count} 天美股大盤指數")
    else:
        logger.info("美股大盤指數已完整")


def run_us_filters_for_date(
    db: USSQLiteDatabase,
    target_date: date,
    vcp_filter: USVCPFilter,
    sanxian_filter: USSanxianFilter,
    stock_info: dict,
) -> dict:
    """對指定日期執行美股篩選"""

    start_date = target_date - timedelta(days=400)
    price_df = db.get_daily_prices(start_date, target_date)
    market_df = db.get_market_index(start_date, target_date)

    if price_df.empty:
        return {"date": target_date, "vcp": 0, "sanxian": 0, "skipped": "no_price"}

    # 過濾只保留主檔中的股票（排除 ETF）
    valid_ids = set(stock_info.keys())
    price_df = price_df[price_df["stock_id"].isin(valid_ids)]

    if price_df.empty:
        return {"date": target_date, "vcp": 0, "sanxian": 0, "skipped": "no_valid_stock"}

    # 檢查目標日期是否有股價資料
    price_dates = pd.to_datetime(price_df["date"]).dt.date
    if target_date not in price_dates.values:
        return {"date": target_date, "vcp": 0, "sanxian": 0, "skipped": "no_data_on_date"}

    # 計算大盤報酬率
    market_return = calculate_us_market_return(market_df, target_date, lookback=20)

    # VCP 篩選
    vcp_df = vcp_filter.filter(price_df, market_return, target_date)
    vcp_results = _enrich_us(vcp_df, stock_info)

    # 三線開花篩選
    sanxian_df = sanxian_filter.filter(price_df, target_date)
    sanxian_results = _enrich_us(sanxian_df, stock_info)

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


def _enrich_us(df: pd.DataFrame, stock_info: dict) -> list[dict]:
    """補充美股股票基本資料"""
    if df.empty:
        return []

    results = []
    for _, row in df.iterrows():
        sid = row["stock_id"]
        info = stock_info.get(sid, {})
        result = row.to_dict()
        result = {
            k: (v if not (isinstance(v, float) and pd.isna(v)) else None)
            for k, v in result.items()
        }
        result.update({
            "stock_name": info.get("stock_name", ""),
            "company_name": info.get("stock_name", ""),
            "exchange": info.get("exchange", "-") or "-",
            "sector": info.get("sector", "-") or "-",
            "industry": info.get("industry", "-") or "-",
            "industry_category": info.get("sector", "-") or "-",
            "industry_category2": info.get("industry", "-") or "-",
        })
        results.append(result)
    return results


def main():
    end_date = date(2026, 3, 21)
    start_date = date(2024, 3, 22)

    fridays = get_all_fridays(start_date, end_date)
    logger.info(f"共 {len(fridays)} 個星期五 ({start_date} ~ {end_date})")

    db = USSQLiteDatabase()
    db.create_tables()

    # Step 1: 補齊大盤指數
    backfill_us_market_index(db, start_date - timedelta(days=30), end_date)

    # Step 2: 檢查哪些星期五已有篩選結果
    import sqlite3
    conn = sqlite3.connect(db.db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT filter_date FROM us_filter_result")
    existing_dates = {row[0] for row in cursor.fetchall()}
    conn.close()

    # 只處理美股交易日的星期五
    trading_fridays = []
    for f in fridays:
        if USMarketCalendar.is_trading_day(f):
            trading_fridays.append(f)

    need_process = [f for f in trading_fridays if f.isoformat() not in existing_dates]
    logger.info(
        f"美股交易日星期五: {len(trading_fridays)} 天, "
        f"已有結果: {len(trading_fridays) - len(need_process)} 天, "
        f"需處理: {len(need_process)} 天"
    )

    if not need_process:
        logger.info("所有星期五已有篩選結果")
        return

    # Step 3: 取得股票基本資料
    stock_info = db.get_stock_info_dict()
    logger.info(f"美股主檔: {len(stock_info)} 檔")

    # Step 4: 逐一計算
    vcp_filter = USVCPFilter()
    sanxian_filter = USSanxianFilter()

    total_vcp = 0
    total_sanxian = 0
    skipped = 0

    for i, friday in enumerate(need_process, 1):
        logger.info(f"[{i}/{len(need_process)}] 處理 {friday}...")
        result = run_us_filters_for_date(
            db, friday, vcp_filter, sanxian_filter, stock_info
        )

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
