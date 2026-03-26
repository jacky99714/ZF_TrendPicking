"""
匯出篩選結果為分月 JSON 檔（v2 — 精簡欄位 + 按月拆分 + 無 indicator）

輸出結構：
    site/data/index.json          (~1-2MB) 股票主檔 + 月份清單
    site/data/months/2026-03.json (~1MB)   該月篩選結果
    site/data/months/2026-02.json
    ...

欄位精簡對照：
    stock_name → n, market → m, industry → i
    date → d, type → t (vcp/sx)
    is_strong → s, is_new_high → h
    return_20d → r, gap_ratio → g

用法：
    python scripts/export_to_json_v2.py
"""
import json
import math
import os
import sqlite3
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

OUTPUT_DIR = BASE_DIR / "site" / "data"
MONTHS_DIR = OUTPUT_DIR / "months"
IND_DIR = OUTPUT_DIR / "indicators"


def safe_round(value, digits=2):
    if value is None:
        return None
    try:
        f = float(value)
        if math.isinf(f) or math.isnan(f):
            return None
        return round(f, digits)
    except (ValueError, TypeError):
        return None


def query_indicators(db_path, table):
    """從 DB 查詢 indicator_json"""
    if not os.path.exists(db_path):
        return {}

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Check if indicator_json column exists
    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
    if "indicator_json" not in cols:
        conn.close()
        return {}

    rows = conn.execute(f"""
        SELECT filter_date, filter_type, stock_id, indicator_json
        FROM {table}
        WHERE indicator_json IS NOT NULL AND indicator_json != ''
    """).fetchall()
    conn.close()

    # Group by month -> { "2026-03": { "2026-03-20": { "2330": { "v": {...}, "s": {...} } } } }
    by_month = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for row in rows:
        month = row["filter_date"][:7]
        ftype = "v" if row["filter_type"] == "vcp" else "s"
        try:
            by_month[month][row["filter_date"]][row["stock_id"]][ftype] = json.loads(row["indicator_json"])
        except (json.JSONDecodeError, TypeError):
            pass

    return by_month


def query_results(db_path, table, market, sector_col="industry_category"):
    """從 DB 查詢篩選結果"""
    if not os.path.exists(db_path):
        print(f"⚠️ DB 不存在: {db_path}")
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(f"""
        SELECT filter_date, filter_type, stock_id, stock_name,
               {sector_col}, return_20d, is_strong_list, is_new_high_list,
               today_price, second_high_55d, gap_ratio
        FROM {table}
        ORDER BY filter_date DESC, stock_id
    """).fetchall()
    conn.close()

    results = []
    for row in rows:
        r = {
            "m": market,
            "d": row["filter_date"],
            "id": row["stock_id"],
            "n": row["stock_name"],
            "i": row[sector_col] or "-",
        }

        if row["filter_type"] == "vcp":
            r["t"] = "vcp"
            r["r"] = safe_round(
                row["return_20d"] * 100 if row["return_20d"] is not None else None
            )
            r["s"] = bool(row["is_strong_list"])
            r["h"] = bool(row["is_new_high_list"])
        else:
            r["t"] = "sx"
            r["g"] = safe_round(
                row["gap_ratio"] * 100 if row["gap_ratio"] is not None else None
            )

        results.append(r)

    print(f"✅ {market}: {len(results)} 筆")
    return results


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    MONTHS_DIR.mkdir(parents=True, exist_ok=True)

    tw_db = str(BASE_DIR / "data" / "zf_trend.db")
    us_db = str(BASE_DIR / "data" / "zf_trend_us.db")

    tw = query_results(tw_db, "filter_result", "tw", "industry_category")
    us = query_results(us_db, "us_filter_result", "us", "sector")
    all_results = tw + us

    # 取得台股 stock_type（twse/tpex）用於 TradingView 匯出
    tw_stock_types = {}
    if os.path.exists(tw_db):
        tw_conn = sqlite3.connect(tw_db)
        for row in tw_conn.execute("SELECT stock_id, stock_type FROM stock_info"):
            tw_stock_types[row[0]] = row[1]
        tw_conn.close()

    # === 1. 建立股票主檔 ===
    stocks = {}
    stock_months = defaultdict(set)  # stock_id -> set of months

    for r in all_results:
        sid = r["id"]
        month = r["d"][:7]  # "2026-03-20" -> "2026-03"
        stock_months[sid].add(month)

        if sid not in stocks:
            info = {"n": r["n"], "m": r["m"], "i": r["i"]}
            # 台股加入 exchange（twse/tpex）給 TradingView 用
            if r["m"] == "tw" and sid in tw_stock_types:
                info["e"] = tw_stock_types[sid]
            stocks[sid] = info

    # 加入每檔股票出現的月份列表（讓搜尋知道要載入哪些月份）
    for sid, info in stocks.items():
        info["ms"] = sorted(stock_months[sid], reverse=True)

    # === 2. 按月份拆分結果 ===
    months_data = defaultdict(list)
    all_months = set()

    for r in all_results:
        month = r["d"][:7]
        all_months.add(month)

        entry = {"d": r["d"], "id": r["id"], "t": r["t"]}

        if r["t"] == "vcp":
            if r.get("s"):
                entry["s"] = True
            if r.get("h"):
                entry["h"] = True
            if r.get("r") is not None:
                entry["r"] = r["r"]
        else:  # sanxian
            if r.get("g") is not None:
                entry["g"] = r["g"]

        months_data[month].append(entry)

    # === 3. 寫入 index.json ===
    sorted_months = sorted(all_months, reverse=True)
    all_dates = sorted({r["d"] for r in all_results})

    index = {
        "generated_at": date.today().isoformat(),
        "total_records": len(all_results),
        "total_stocks": len(stocks),
        "first_date": all_dates[0] if all_dates else "",
        "last_date": all_dates[-1] if all_dates else "",
        "months": sorted_months,
        "stocks": stocks,
    }

    index_path = OUTPUT_DIR / "index.json"
    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False, separators=(",", ":"))

    index_kb = index_path.stat().st_size / 1024
    print(f"✅ index.json: {index_kb:.1f} KB ({len(stocks)} 檔股票, {len(sorted_months)} 個月)")

    # === 4. 寫入各月份 JSON ===
    total_month_kb = 0
    for month in sorted_months:
        data = months_data[month]
        month_path = MONTHS_DIR / f"{month}.json"
        with open(month_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))

        kb = month_path.stat().st_size / 1024
        total_month_kb += kb
        dates_in_month = len(set(e["d"] for e in data))
        print(f"   {month}.json: {kb:.1f} KB ({len(data)} 筆, {dates_in_month} 天)")

    # === 5. 寫入指標 JSON（per month） ===
    IND_DIR.mkdir(parents=True, exist_ok=True)
    tw_ind = query_indicators(tw_db, "filter_result")
    us_ind = query_indicators(us_db, "us_filter_result")

    total_ind_kb = 0
    ind_count = 0
    for month in sorted_months:
        merged_ind = {}
        # Deep merge tw + us indicators
        for src in (tw_ind, us_ind):
            if month not in src:
                continue
            for dt, stocks in src[month].items():
                if dt not in merged_ind:
                    merged_ind[dt] = {}
                for sid, types in stocks.items():
                    if sid not in merged_ind[dt]:
                        merged_ind[dt][sid] = {}
                    merged_ind[dt][sid].update(types)

        if not merged_ind:
            continue

        ind_path = IND_DIR / f"{month}.json"
        with open(ind_path, "w", encoding="utf-8") as f:
            json.dump(merged_ind, f, ensure_ascii=False, separators=(",", ":"))

        kb = ind_path.stat().st_size / 1024
        total_ind_kb += kb
        ind_count += 1

    print(f"   指標檔案: {total_ind_kb:.1f} KB ({ind_count} 個月)")

    print(f"\n✅ 匯出完成:")
    print(f"   index.json: {index_kb:.1f} KB")
    print(f"   月份檔案: {total_month_kb:.1f} KB ({len(sorted_months)} 個月)")
    print(f"   指標檔案: {total_ind_kb:.1f} KB ({ind_count} 個月)")
    total = index_kb + total_month_kb + total_ind_kb
    print(f"   總計: {total:.1f} KB")


if __name__ == "__main__":
    main()
