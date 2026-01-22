"""
SQLite 資料庫連線與操作
適用於 GitHub Actions 環境
"""
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Generator, Optional

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine, text, event
from sqlalchemy.orm import Session, sessionmaker

from data.models import Base, StockInfo, DailyPrice, MarketIndex, FilterResult


class SQLiteDatabase:
    """
    SQLite 資料庫操作類別

    特點:
    - 輕量級，無需外部資料庫
    - 檔案型資料庫，適合 GitHub Actions
    - 支援所有基本 CRUD 操作
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        初始化資料庫連線

        Args:
            db_path: 資料庫檔案路徑（預設為專案根目錄下的 data/zf_trend.db）
        """
        if db_path is None:
            # 預設路徑
            project_root = Path(__file__).resolve().parent.parent
            db_path = str(project_root / "data" / "zf_trend.db")

        # 確保目錄存在
        db_file = Path(db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

        self.db_path = db_path
        self.database_url = f"sqlite:///{db_path}"

        self.engine = create_engine(
            self.database_url,
            echo=False,
            connect_args={"check_same_thread": False},
        )

        # 啟用 WAL 模式以提升效能
        @event.listens_for(self.engine, "connect")
        def set_sqlite_pragma(dbapi_conn, connection_record):
            cursor = dbapi_conn.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA cache_size=-64000")  # 64MB cache
            cursor.close()

        self.SessionLocal = sessionmaker(
            bind=self.engine,
            autocommit=False,
            autoflush=False,
        )

        logger.info(f"SQLite 資料庫初始化完成: {db_path}")

    def create_tables(self):
        """建立所有資料表"""
        Base.metadata.create_all(self.engine)
        logger.info("資料表建立完成")

    def drop_tables(self):
        """刪除所有資料表（危險操作）"""
        Base.metadata.drop_all(self.engine)
        logger.warning("所有資料表已刪除")

    @contextmanager
    def get_session(self) -> Generator[Session, None, None]:
        """取得資料庫 Session（Context Manager）"""
        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"資料庫操作失敗: {e}")
            raise
        finally:
            session.close()

    # ==================== StockInfo 操作 ====================

    def upsert_stock_info(self, df: pd.DataFrame) -> int:
        """
        批次寫入/更新股票基本資料

        Args:
            df: 包含 stock_id, stock_name, industry_category, type 的 DataFrame

        Returns:
            寫入/更新的筆數
        """
        if df.empty:
            return 0

        # 只保留需要的欄位
        required_cols = ["stock_id", "stock_name", "industry_category", "type"]
        df = df[[c for c in required_cols if c in df.columns]].copy()
        df = df.rename(columns={"type": "stock_type"})

        count = 0
        with self.get_session() as session:
            for _, row in df.iterrows():
                # SQLite 使用 INSERT OR REPLACE
                existing = session.query(StockInfo).filter(
                    StockInfo.stock_id == row["stock_id"]
                ).first()

                if existing:
                    existing.stock_name = row["stock_name"]
                    existing.industry_category = row.get("industry_category")
                    existing.stock_type = row.get("stock_type")
                else:
                    session.add(StockInfo(
                        stock_id=row["stock_id"],
                        stock_name=row["stock_name"],
                        industry_category=row.get("industry_category"),
                        stock_type=row.get("stock_type"),
                    ))
                count += 1

        logger.info(f"寫入/更新 {count} 筆股票基本資料")
        return count

    def get_all_stock_info(self) -> pd.DataFrame:
        """取得所有股票基本資料"""
        with self.get_session() as session:
            query = session.query(StockInfo)
            df = pd.read_sql(query.statement, session.bind)
        return df

    def get_stock_info_dict(self) -> dict[str, dict]:
        """取得股票資訊字典 (stock_id -> info)"""
        df = self.get_all_stock_info()
        if df.empty:
            return {}
        return {
            row["stock_id"]: {
                "stock_name": row["stock_name"],
                "industry_category": row.get("industry_category", "-"),
                "stock_type": row.get("stock_type", "twse"),
            }
            for _, row in df.iterrows()
        }

    def get_stock_market_types(self) -> dict[str, str]:
        """取得所有股票的市場類型 {stock_id: market_type}"""
        df = self.get_all_stock_info()
        if df.empty:
            return {}
        return {
            row["stock_id"]: row.get("stock_type", "twse")
            for _, row in df.iterrows()
        }

    # ==================== DailyPrice 操作 ====================

    def upsert_daily_price(self, df: pd.DataFrame) -> int:
        """
        批次寫入/更新每日股價

        Args:
            df: 包含 stock_id, date, open, high, low, close, volume 的 DataFrame

        Returns:
            寫入/更新的筆數
        """
        if df.empty:
            return 0

        # 重新命名欄位
        column_mapping = {
            "open": "open_price",
            "high": "high_price",
            "low": "low_price",
            "close": "close_price",
        }
        df = df.rename(columns=column_mapping)

        # 只保留需要的欄位
        columns = ["stock_id", "date", "open_price", "high_price",
                   "low_price", "close_price", "volume"]
        df = df[[c for c in columns if c in df.columns]].copy()

        # 去除重複
        df = df.drop_duplicates(subset=["stock_id", "date"], keep="last")

        total_count = len(df)

        # 按日期分批處理，避免跨日期刪除資料
        # 修正：原本的邏輯會刪除「所有日期 x 所有股票」的笛卡爾積
        # 現在改為每個日期獨立處理，只刪除該日期中即將插入的股票
        with self.get_session() as session:
            for target_date in df["date"].unique():
                # 取得該日期的資料
                day_df = df[df["date"] == target_date]
                day_stock_ids = day_df["stock_id"].unique().tolist()

                # 只刪除該日期中即將插入的股票資料
                session.query(DailyPrice).filter(
                    DailyPrice.date == target_date,
                    DailyPrice.stock_id.in_(day_stock_ids)
                ).delete(synchronize_session=False)

                # 插入該日期的資料
                records = day_df.to_dict("records")
                session.bulk_insert_mappings(DailyPrice, records)

        logger.info(f"寫入/更新 {total_count} 筆股價資料")
        return total_count

    def get_daily_prices(
        self,
        start_date: date,
        end_date: date,
        stock_ids: Optional[list[str]] = None
    ) -> pd.DataFrame:
        """
        取得指定期間的股價資料

        Args:
            start_date: 開始日期
            end_date: 結束日期
            stock_ids: 指定股票代號列表（可選）

        Returns:
            股價 DataFrame
        """
        with self.get_session() as session:
            query = session.query(DailyPrice).filter(
                DailyPrice.date >= start_date,
                DailyPrice.date <= end_date
            )

            if stock_ids:
                query = query.filter(DailyPrice.stock_id.in_(stock_ids))

            query = query.order_by(DailyPrice.stock_id, DailyPrice.date)
            df = pd.read_sql(query.statement, session.bind)

        return df

    def get_latest_date(self) -> Optional[date]:
        """取得資料庫中最新的股價日期"""
        with self.get_session() as session:
            result = session.query(DailyPrice.date).order_by(
                DailyPrice.date.desc()
            ).first()

        return result[0] if result else None

    # ==================== MarketIndex 操作 ====================

    def upsert_market_index(self, df: pd.DataFrame) -> int:
        """
        批次寫入/更新大盤指數

        Args:
            df: 包含 date, taiex 的 DataFrame

        Returns:
            寫入/更新的筆數
        """
        if df.empty:
            return 0

        df = df[["date", "taiex"]].copy()
        df = df.drop_duplicates(subset=["date"], keep="last")

        with self.get_session() as session:
            # 先刪除已存在的日期
            dates = df["date"].unique().tolist()
            session.query(MarketIndex).filter(
                MarketIndex.date.in_(dates)
            ).delete(synchronize_session=False)

            # 插入新資料
            records = df.to_dict("records")
            session.bulk_insert_mappings(MarketIndex, records)

        logger.info(f"寫入/更新 {len(records)} 筆大盤指數")
        return len(records)

    def get_market_index(
        self,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """取得指定期間的大盤指數"""
        with self.get_session() as session:
            query = session.query(MarketIndex).filter(
                MarketIndex.date >= start_date,
                MarketIndex.date <= end_date
            ).order_by(MarketIndex.date)

            df = pd.read_sql(query.statement, session.bind)

        return df

    # ==================== FilterResult 操作 ====================

    def save_filter_results(
        self,
        results: list[dict],
        filter_type: str,
        filter_date: date
    ) -> int:
        """
        儲存篩選結果

        Args:
            results: 篩選結果列表
            filter_type: 篩選類型 (vcp/sanxian)
            filter_date: 篩選日期

        Returns:
            儲存的筆數
        """
        if not results:
            return 0

        with self.get_session() as session:
            # 先刪除當天同類型的舊結果
            session.query(FilterResult).filter(
                FilterResult.filter_date == filter_date,
                FilterResult.filter_type == filter_type
            ).delete()

            # 批次寫入新結果
            records = [
                {
                    "filter_date": filter_date,
                    "filter_type": filter_type,
                    "stock_id": r["stock_id"],
                    "stock_name": r.get("stock_name", ""),
                    "industry_category": r.get("industry_category"),
                    "return_20d": r.get("return_20d"),
                    "is_strong_list": r.get("is_strong"),
                    "is_new_high_list": r.get("is_new_high"),
                    "today_price": r.get("today_price"),
                    "second_high_55d": r.get("second_high_55d"),
                    "gap_ratio": r.get("gap_ratio"),
                }
                for r in results
            ]
            session.bulk_insert_mappings(FilterResult, records)

        logger.info(f"儲存 {len(results)} 筆 {filter_type} 篩選結果")
        return len(results)

    def get_filter_results(
        self,
        filter_type: str,
        filter_date: date
    ) -> pd.DataFrame:
        """取得指定日期的篩選結果"""
        with self.get_session() as session:
            query = session.query(FilterResult).filter(
                FilterResult.filter_date == filter_date,
                FilterResult.filter_type == filter_type
            )
            df = pd.read_sql(query.statement, session.bind)

        return df

    # ==================== 工具方法 ====================

    def execute_sql(self, sql: str) -> None:
        """執行原始 SQL"""
        with self.get_session() as session:
            session.execute(text(sql))

    def health_check(self) -> bool:
        """檢查資料庫連線"""
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error(f"資料庫連線失敗: {e}")
            return False

    def get_db_size(self) -> str:
        """取得資料庫檔案大小"""
        db_file = Path(self.db_path)
        if db_file.exists():
            size_bytes = db_file.stat().st_size
            if size_bytes < 1024:
                return f"{size_bytes} B"
            elif size_bytes < 1024 * 1024:
                return f"{size_bytes / 1024:.1f} KB"
            else:
                return f"{size_bytes / (1024 * 1024):.1f} MB"
        return "0 B"

    def vacuum(self):
        """壓縮資料庫檔案"""
        with self.engine.connect() as conn:
            conn.execute(text("VACUUM"))
        logger.info("資料庫已壓縮")
