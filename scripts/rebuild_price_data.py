"""
用 FinMind 重新抓取股價資料並更新 SQLite 資料庫
"""
import sqlite3
import time
from datetime import date, timedelta

import pandas as pd
from loguru import logger

from api.finmind_client import FinMindClient


def rebuild_price_data():
    """重建股價資料"""

    # 設定
    DB_PATH = "data/zf_trend.db"
    START_DATE = date(2025, 1, 21)
    END_DATE = date(2026, 1, 21)
    BATCH_DAYS = 30  # 每批次抓取天數
    BATCH_INTERVAL = 3  # 批次間隔秒數

    logger.info("=== 開始重建股價資料 ===")
    logger.info(f"日期範圍: {START_DATE} ~ {END_DATE}")
    logger.info(f"批次大小: {BATCH_DAYS} 天")

    # 初始化 FinMind 客戶端
    client = FinMindClient()

    # 連接資料庫
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # 清空現有股價資料
    logger.info("清空現有股價資料...")
    cursor.execute("DELETE FROM daily_price")
    conn.commit()
    logger.info("已清空 daily_price 表")

    # 分批抓取資料
    all_data = []
    current_start = START_DATE
    batch_num = 0
    total_batches = ((END_DATE - START_DATE).days + BATCH_DAYS - 1) // BATCH_DAYS

    while current_start <= END_DATE:
        batch_num += 1
        current_end = min(current_start + timedelta(days=BATCH_DAYS - 1), END_DATE)

        logger.info(f"[{batch_num}/{total_batches}] 抓取 {current_start} ~ {current_end}...")

        try:
            df = client.get_stock_price(
                start_date=current_start,
                end_date=current_end,
            )

            if not df.empty:
                all_data.append(df)
                logger.info(f"  取得 {len(df)} 筆資料")
            else:
                logger.warning(f"  無資料")

        except Exception as e:
            logger.error(f"  抓取失敗: {e}")

        # 下一批次
        current_start = current_end + timedelta(days=1)

        # 批次間隔
        if current_start <= END_DATE:
            time.sleep(BATCH_INTERVAL)

    # 合併所有資料
    if not all_data:
        logger.error("無任何資料，結束")
        conn.close()
        return

    logger.info("合併資料...")
    final_df = pd.concat(all_data, ignore_index=True)
    logger.info(f"共 {len(final_df)} 筆資料")

    # 去重
    before_dedup = len(final_df)
    final_df = final_df.drop_duplicates(subset=["stock_id", "date"], keep="last")
    after_dedup = len(final_df)
    if before_dedup != after_dedup:
        logger.info(f"去重: {before_dedup} -> {after_dedup}")

    # 寫入資料庫
    logger.info("寫入資料庫...")

    # 重新命名欄位以匹配資料庫結構
    final_df = final_df.rename(columns={
        "open": "open_price",
        "high": "high_price",
        "low": "low_price",
        "close": "close_price",
    })

    # 確保欄位順序正確
    columns = ["stock_id", "date", "open_price", "high_price", "low_price", "close_price", "volume"]

    # 只保留需要的欄位
    available_cols = [c for c in columns if c in final_df.columns]
    final_df = final_df[available_cols]

    # 轉換日期格式
    final_df["date"] = pd.to_datetime(final_df["date"]).dt.strftime("%Y-%m-%d")

    # 寫入
    final_df.to_sql("daily_price", conn, if_exists="append", index=False)
    conn.commit()

    # 驗證
    cursor.execute("SELECT COUNT(*) FROM daily_price")
    count = cursor.fetchone()[0]
    cursor.execute("SELECT MIN(date), MAX(date) FROM daily_price")
    date_range = cursor.fetchone()

    logger.info(f"=== 完成 ===")
    logger.info(f"寫入 {count} 筆資料")
    logger.info(f"日期範圍: {date_range[0]} ~ {date_range[1]}")

    conn.close()


if __name__ == "__main__":
    rebuild_price_data()
