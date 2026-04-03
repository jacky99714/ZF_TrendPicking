"""
驗證美股個股資料完整性

用 AAPL 建立基準交易日曆，逐股比對缺了哪些交易日，
並判斷缺日是否影響 MA / high 計算窗口。

用法:
    python scripts/verify_stock_gaps.py                    # 驗證所有篩選通過的股票
    python scripts/verify_stock_gaps.py --date 2026-04-02  # 指定日期
    python scripts/verify_stock_gaps.py --stock INVA       # 指定股票
    python scripts/verify_stock_gaps.py --all              # 驗證全部股票
"""

import argparse
import sqlite3
import sys
from datetime import datetime
from pathlib import Path

# 計算窗口定義（影響判定用）
WINDOWS = {
    "MA8": 8,
    "MA21": 21,
    "MA50": 50,
    "MA55": 55,
    "MA150": 150,
    "MA200": 200,
    "high_5d": 5,
    "high_260d": 260,
}


def get_reference_calendar(conn: sqlite3.Connection) -> list[str]:
    """用 AAPL 建立基準交易日曆"""
    rows = conn.execute(
        "SELECT date FROM us_daily_price WHERE stock_id = 'AAPL' ORDER BY date"
    ).fetchall()
    return [r[0] for r in rows]


def check_stock(
    conn: sqlite3.Connection,
    stock_id: str,
    ref_calendar: list[str],
) -> dict:
    """檢查單一股票的資料完整性"""
    rows = conn.execute(
        "SELECT date FROM us_daily_price WHERE stock_id = ? ORDER BY date",
        (stock_id,),
    ).fetchall()
    stock_dates = [r[0] for r in rows]

    if not stock_dates:
        return {"stock_id": stock_id, "total": 0, "status": "no_data"}

    # 股票的上市範圍（第一筆到最後一筆）
    first_date = stock_dates[0]
    last_date = stock_dates[-1]

    # 基準日曆中，該股票應有的交易日
    expected = [d for d in ref_calendar if first_date <= d <= last_date]
    actual = set(stock_dates)

    # 缺少的交易日
    missing = sorted(set(expected) - actual)

    # 判斷缺日是否影響計算窗口
    # 計算窗口 = 從最新日期往回數 N 筆（DB 裡的 N 筆）
    # 但正確的 N 筆應該是連續的 N 個交易日
    affected_windows = []
    if missing and len(stock_dates) > 0:
        # 最新日期在基準日曆中的位置
        latest = stock_dates[-1]
        latest_idx_in_ref = (
            ref_calendar.index(latest) if latest in ref_calendar else None
        )

        if latest_idx_in_ref is not None:
            for window_name, window_size in WINDOWS.items():
                if len(stock_dates) < window_size:
                    continue  # 資料不足，本來就算不出

                # DB 裡倒數第 window_size 筆的日期
                db_window_start = stock_dates[-window_size]

                # 正確的窗口起始日（基準日曆倒數 window_size 天）
                correct_start_idx = latest_idx_in_ref - window_size + 1
                if correct_start_idx < 0:
                    continue
                correct_window_start = ref_calendar[correct_start_idx]

                # 窗口內缺了幾天
                missing_in_window = [
                    d
                    for d in missing
                    if correct_window_start <= d <= latest
                ]

                if missing_in_window:
                    # 計算日期偏移（DB 窗口起點 vs 正確窗口起點）
                    affected_windows.append(
                        {
                            "window": window_name,
                            "size": window_size,
                            "db_start": db_window_start,
                            "correct_start": correct_window_start,
                            "missing_count": len(missing_in_window),
                        }
                    )

    return {
        "stock_id": stock_id,
        "total": len(stock_dates),
        "expected": len(expected),
        "missing_count": len(missing),
        "missing_dates": missing[:10],  # 最多顯示 10 個
        "first_date": first_date,
        "last_date": last_date,
        "affected_windows": affected_windows,
        "status": "gap" if missing else "ok",
    }


def main():
    parser = argparse.ArgumentParser(description="驗證美股資料完整性")
    parser.add_argument("--date", default=None, help="篩選日期 (YYYY-MM-DD)")
    parser.add_argument("--stock", default=None, help="指定股票代碼")
    parser.add_argument("--all", action="store_true", help="檢查全部股票")
    parser.add_argument(
        "--db",
        default="/tmp/zf_trend_us.db",
        help="DB 路徑 (預設 /tmp/zf_trend_us.db)",
    )
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        # 嘗試本地
        local_db = Path("data/zf_trend_us.db")
        if local_db.exists():
            db_path = local_db
        else:
            print("❌ 找不到 DB，請先下載：")
            print(
                "  gh release download us-db-backup -p 'zf_trend_us.db.gz'"
                " -D /tmp --clobber && gunzip -f /tmp/zf_trend_us.db.gz"
            )
            sys.exit(1)

    conn = sqlite3.connect(str(db_path))

    # 建立基準日曆
    ref_calendar = get_reference_calendar(conn)
    print(f"基準日曆 (AAPL): {len(ref_calendar)} 天, "
          f"{ref_calendar[0]} ~ {ref_calendar[-1]}")

    # 決定要檢查哪些股票
    if args.stock:
        stocks = [args.stock]
    elif args.all:
        rows = conn.execute(
            "SELECT DISTINCT stock_id FROM us_daily_price"
        ).fetchall()
        stocks = [r[0] for r in rows]
    else:
        # 檢查篩選結果中的股票
        target_date = args.date
        if not target_date:
            row = conn.execute(
                "SELECT MAX(filter_date) FROM us_filter_result"
            ).fetchone()
            target_date = row[0]

        rows = conn.execute(
            "SELECT DISTINCT stock_id FROM us_filter_result "
            "WHERE filter_date = ?",
            (target_date,),
        ).fetchall()
        stocks = [r[0] for r in rows]
        print(f"篩選日期: {target_date}, 共 {len(stocks)} 檔")

    print(f"檢查股票: {len(stocks)} 檔")
    print()

    # 逐股檢查
    gap_stocks = []
    affected_stocks = []

    for stock_id in sorted(stocks):
        result = check_stock(conn, stock_id, ref_calendar)

        if result["status"] == "no_data":
            continue

        if result["missing_count"] > 0:
            gap_stocks.append(result)

        if result["affected_windows"]:
            affected_stocks.append(result)

    conn.close()

    # 報告
    print(f"{'=' * 60}")
    print(f"有缺日的股票: {len(gap_stocks)} / {len(stocks)} 檔")
    print(f"缺日影響計算的: {len(affected_stocks)} 檔")
    print(f"{'=' * 60}")

    if gap_stocks:
        print()
        print("=== 有缺日的股票 ===")
        for r in sorted(gap_stocks, key=lambda x: -x["missing_count"]):
            dates_str = ", ".join(r["missing_dates"][:5])
            if r["missing_count"] > 5:
                dates_str += f" ... (共 {r['missing_count']} 天)"
            impact = " ⚠️ 影響計算" if r["affected_windows"] else ""
            print(
                f"  {r['stock_id']:8} "
                f"DB:{r['total']}筆 "
                f"應有:{r['expected']}筆 "
                f"缺:{r['missing_count']}天{impact}"
            )
            if len(r["missing_dates"]) <= 10:
                print(f"           缺: {dates_str}")

    if affected_stocks:
        print()
        print("=== ⚠️ 缺日影響計算窗口的股票 ===")
        for r in affected_stocks:
            print(f"\n  {r['stock_id']} (缺 {r['missing_count']} 天):")
            for w in r["affected_windows"]:
                print(
                    f"    {w['window']:12} "
                    f"DB起點={w['db_start']} "
                    f"正確起點={w['correct_start']} "
                    f"窗口內缺{w['missing_count']}天"
                )

    if not gap_stocks:
        print("\n✅ 所有股票資料完整，無缺日")
    elif not affected_stocks:
        print(
            f"\n✅ 有 {len(gap_stocks)} 檔缺日，"
            "但都不影響目前的計算窗口"
        )
    else:
        print(
            f"\n❌ 有 {len(affected_stocks)} 檔的缺日"
            "影響了計算窗口，數值可能失真"
        )


if __name__ == "__main__":
    main()
