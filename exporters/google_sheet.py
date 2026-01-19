"""
Google Sheet 匯出模組
"""
import time
from datetime import date, datetime
from typing import Optional

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials
from loguru import logger

from config.settings import GOOGLE_CREDENTIALS_PATH, SHEET_IDS

# Google API 重試設定
GSHEET_MAX_RETRIES = 3
GSHEET_RETRY_DELAY = 5  # 秒


class GoogleSheetExporter:
    """
    Google Sheet 匯出器

    支援:
    - 公司主檔更新
    - VCP 篩選結果匯出
    - 三線開花篩選結果匯出
    """

    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    def __init__(self, credentials_path: Optional[str] = None):
        """
        初始化匯出器

        Args:
            credentials_path: Service Account 憑證路徑
        """
        self.credentials_path = credentials_path or GOOGLE_CREDENTIALS_PATH
        self.client: Optional[gspread.Client] = None

        self._connect()

    def _connect(self):
        """建立 Google Sheets 連線"""
        try:
            creds = Credentials.from_service_account_file(
                self.credentials_path,
                scopes=self.SCOPES
            )
            self.client = gspread.authorize(creds)
            logger.info("Google Sheets 連線成功")
        except FileNotFoundError:
            logger.warning(
                f"憑證檔案不存在: {self.credentials_path}, "
                "Google Sheet 功能將無法使用"
            )
            self.client = None
        except Exception as e:
            logger.error(f"Google Sheets 連線失敗: {e}")
            self.client = None

    def _get_sheet(self, sheet_id: str) -> Optional[gspread.Spreadsheet]:
        """取得 Spreadsheet 物件"""
        if not self.client:
            logger.error("未連線到 Google Sheets")
            return None

        try:
            return self.client.open_by_key(sheet_id)
        except Exception as e:
            logger.error(f"無法開啟 Sheet {sheet_id}: {e}")
            return None

    def _format_date_tab(self, target_date: date) -> str:
        """格式化日期為分頁名稱 (YYMMDD)"""
        return target_date.strftime("%y%m%d")

    # ==================== 公司主檔 ====================

    def export_company_master(
        self,
        data: list[dict],
        sheet_id: Optional[str] = None
    ) -> bool:
        """
        匯出公司主檔

        SPEC: 首次建立分頁並依代號升冪排序，第二次後採差集後插入

        Args:
            data: 公司資料列表
            sheet_id: Sheet ID

        Returns:
            是否成功
        """
        sheet_id = sheet_id or SHEET_IDS.get("company_master")
        if not sheet_id:
            logger.error("未設定公司主檔 Sheet ID")
            return False

        sheet = self._get_sheet(sheet_id)
        if not sheet:
            return False

        try:
            # 標題列
            headers = ["代號", "股名", "公司名", "產業分類1", "產業分類2", "產品組合"]

            # 取得或建立「台股公司主檔」分頁
            is_first_time = False
            try:
                worksheet = sheet.worksheet("台股公司主檔")
            except gspread.WorksheetNotFound:
                worksheet = sheet.add_worksheet(
                    title="台股公司主檔",
                    rows=len(data) + 1,
                    cols=6
                )
                is_first_time = True

            if is_first_time:
                # SPEC: 首次撈取依代號的升冪排序，寫入全部資料
                sorted_data = sorted(data, key=lambda x: x.get("stock_id", ""))
                rows = [headers] + [
                    [
                        row.get("stock_id", ""),
                        row.get("stock_name", ""),
                        row.get("company_name", row.get("stock_name", "")),
                        row.get("industry_category", "-"),
                        row.get("industry_category2", "-"),
                        row.get("product_mix", "-"),
                    ]
                    for row in sorted_data
                ]
                worksheet.update(rows, "A1")
                logger.info(f"公司主檔首次匯出完成: {len(data)} 筆")
            else:
                # SPEC: 第二次後採差集後插入
                # 取得現有股票代號
                existing_data = worksheet.get_all_values()
                existing_ids = set()
                if len(existing_data) > 1:  # 跳過標題列
                    existing_ids = {row[0] for row in existing_data[1:] if row}

                # 計算差集（新增的股票）
                new_stocks = [
                    row for row in data
                    if row.get("stock_id", "") not in existing_ids
                ]

                if new_stocks:
                    # 在現有資料後面插入新股票
                    next_row = len(existing_data) + 1
                    new_rows = [
                        [
                            row.get("stock_id", ""),
                            row.get("stock_name", ""),
                            row.get("company_name", row.get("stock_name", "")),
                            row.get("industry_category", "-"),
                            row.get("industry_category2", "-"),
                            row.get("product_mix", "-"),
                        ]
                        for row in sorted(new_stocks, key=lambda x: x.get("stock_id", ""))
                    ]
                    worksheet.update(new_rows, f"A{next_row}")
                    logger.info(f"公司主檔差集插入完成: 新增 {len(new_stocks)} 筆")
                else:
                    logger.info("公司主檔無新增股票")

            return True

        except Exception as e:
            logger.error(f"公司主檔匯出失敗: {e}")
            return False

    def update_company_master_log(
        self,
        sheet_id: Optional[str] = None,
        update_date: Optional[date] = None
    ) -> bool:
        """
        更新公司主檔更新紀錄

        Args:
            sheet_id: Sheet ID
            update_date: 更新日期

        Returns:
            是否成功
        """
        sheet_id = sheet_id or SHEET_IDS.get("company_master")
        if not sheet_id:
            return False

        sheet = self._get_sheet(sheet_id)
        if not sheet:
            return False

        try:
            # 取得或建立「台股更新紀錄」分頁
            try:
                worksheet = sheet.worksheet("台股更新紀錄")
            except gspread.WorksheetNotFound:
                worksheet = sheet.add_worksheet(
                    title="台股更新紀錄",
                    rows=100,
                    cols=2
                )

            # SPEC: 請在分頁二 "台股更新紀錄" 的 A1 欄位寫入更新時間
            now = datetime.now()
            update_time = f"{now.strftime('%Y-%m-%d %H:%M:%S')} 更新完成"
            worksheet.update([[update_time]], "A1")

            logger.info(f"公司主檔更新紀錄已記錄: {now.strftime('%Y-%m-%d %H:%M:%S')}")
            return True

        except Exception as e:
            logger.error(f"更新紀錄寫入失敗: {e}")
            return False

    def log_error_to_sheet(
        self,
        error_logs: list[dict],
        sheet_id: Optional[str] = None
    ) -> bool:
        """
        將錯誤日誌寫入 Google Sheet「台股更新紀錄」分頁

        SPEC: 每次重試皆須記錄 Time, Retry_Count 與 Last_Error_Code

        Args:
            error_logs: 錯誤日誌列表，每項包含 time, retry_count, status_code
            sheet_id: Sheet ID

        Returns:
            是否成功
        """
        if not error_logs:
            return True

        sheet_id = sheet_id or SHEET_IDS.get("company_master")
        if not sheet_id:
            return False

        sheet = self._get_sheet(sheet_id)
        if not sheet:
            return False

        try:
            # 取得或建立「台股更新紀錄」分頁
            try:
                worksheet = sheet.worksheet("台股更新紀錄")
            except gspread.WorksheetNotFound:
                worksheet = sheet.add_worksheet(
                    title="台股更新紀錄",
                    rows=100,
                    cols=4
                )

            # 找到下一個空白行（從 A2 開始，A1 用於更新時間）
            existing_data = worksheet.get_all_values()
            next_row = len(existing_data) + 1
            if next_row < 3:
                next_row = 3  # 至少從第 3 行開始（第 1 行更新時間，第 2 行表頭）

            # 如果還沒有表頭，先寫入
            if len(existing_data) < 2:
                worksheet.update([["Time", "Retry_Count", "Last_Error_Code", "備註"]], "A2")
                next_row = 3

            # 寫入錯誤日誌
            rows = [
                [
                    log.get("time", ""),
                    str(log.get("retry_count", 0)),
                    str(log.get("status_code", "")),
                    str(log.get("params", {})),
                ]
                for log in error_logs
            ]

            if rows:
                worksheet.update(rows, f"A{next_row}")
                logger.info(f"已寫入 {len(rows)} 筆錯誤日誌到 Google Sheet")

            return True

        except Exception as e:
            logger.error(f"錯誤日誌寫入失敗: {e}")
            return False

    # ==================== VCP 篩選結果 ====================

    def export_vcp(
        self,
        data: list[dict],
        target_date: date,
        sheet_id: Optional[str] = None
    ) -> bool:
        """
        匯出 VCP 篩選結果

        Args:
            data: VCP 篩選結果列表
            target_date: 篩選日期
            sheet_id: Sheet ID

        Returns:
            是否成功
        """
        sheet_id = sheet_id or SHEET_IDS.get("tw_vcp")
        if not sheet_id:
            logger.error("未設定台股VCP Sheet ID")
            return False

        sheet = self._get_sheet(sheet_id)
        if not sheet:
            return False

        try:
            tab_name = self._format_date_tab(target_date)

            # 建立新分頁（插入在第二位）
            try:
                # 如果分頁已存在，先刪除
                existing = sheet.worksheet(tab_name)
                sheet.del_worksheet(existing)
            except gspread.WorksheetNotFound:
                pass

            worksheet = sheet.add_worksheet(
                title=tab_name,
                rows=max(len(data) + 1, 2),  # 至少 2 行（標題+1資料）
                cols=9,
                index=1  # 插入在第二個位置
            )

            # 標題列
            headers = [
                "代號", "股名", "公司名", "產業分類1", "產業分類2",
                "產品組合", "近20日股價漲幅", "強勢清單", "新高清單"
            ]

            # 資料列（處理 NaN 值）
            def safe_return(val):
                """安全格式化報酬率"""
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return "-"
                return f"{val * 100:.2f}%"

            # SPEC: 資料排序依 "近20日股價漲幅" 降冪排序
            def sort_key_return(row):
                """排序用：處理 None 和 NaN"""
                val = row.get("return_20d")
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return float("-inf")
                return val

            sorted_data = sorted(data, key=sort_key_return, reverse=True)

            rows = [headers] + [
                [
                    row.get("stock_id", ""),
                    row.get("stock_name", ""),
                    row.get("company_name", row.get("stock_name", "")),
                    row.get("industry_category", "-"),
                    row.get("industry_category2", "-"),
                    row.get("product_mix", "-"),
                    safe_return(row.get("return_20d")),
                    "O" if row.get("is_strong") else "",
                    "O" if row.get("is_new_high") else "",
                ]
                for row in sorted_data
            ]

            # 批次寫入
            worksheet.update(rows, "A1")

            logger.info(f"VCP 篩選結果匯出完成: {len(data)} 筆 -> {tab_name}")
            return True

        except gspread.exceptions.APIError as e:
            # Google API 限流，嘗試重試
            if "RATE_LIMIT_EXCEEDED" in str(e) or "429" in str(e):
                # 確認 worksheet 和 rows 已定義才能重試
                if 'worksheet' in dir() and 'rows' in dir():
                    for retry in range(GSHEET_MAX_RETRIES):
                        logger.warning(f"Google API 限流，{GSHEET_RETRY_DELAY} 秒後重試 ({retry + 1}/{GSHEET_MAX_RETRIES})...")
                        time.sleep(GSHEET_RETRY_DELAY * (retry + 1))
                        try:
                            worksheet.update(rows, "A1")
                            logger.info(f"VCP 篩選結果匯出完成: {len(data)} 筆 -> {tab_name}")
                            return True
                        except Exception:
                            continue
            logger.error(f"VCP 匯出失敗: {e}")
            return False
        except Exception as e:
            logger.error(f"VCP 匯出失敗: {e}")
            return False

    # ==================== 三線開花篩選結果 ====================

    def export_sanxian(
        self,
        data: list[dict],
        target_date: date,
        sheet_id: Optional[str] = None
    ) -> bool:
        """
        匯出三線開花篩選結果

        Args:
            data: 三線開花篩選結果列表
            target_date: 篩選日期
            sheet_id: Sheet ID

        Returns:
            是否成功
        """
        sheet_id = sheet_id or SHEET_IDS.get("tw_sanxian")
        if not sheet_id:
            logger.error("未設定台股三線開花 Sheet ID")
            return False

        sheet = self._get_sheet(sheet_id)
        if not sheet:
            return False

        try:
            tab_name = self._format_date_tab(target_date)

            # 建立新分頁
            try:
                existing = sheet.worksheet(tab_name)
                sheet.del_worksheet(existing)
            except gspread.WorksheetNotFound:
                pass

            worksheet = sheet.add_worksheet(
                title=tab_name,
                rows=max(len(data) + 1, 2),  # 至少 2 行（標題+1資料）
                cols=9,
                index=1
            )

            # 標題列
            headers = [
                "代號", "股名", "公司名", "產業分類1", "產業分類2",
                "產品組合", "今日股價", "55日內次高價", "差距比例"
            ]

            # 資料列（處理 NaN 值）
            def safe_price(val):
                """安全格式化價格"""
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return "-"
                return f"{val:.2f}"

            def safe_ratio(val):
                """安全格式化比例"""
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return "-"
                return f"{val * 100:.2f}%"

            # SPEC: 資料排序依 "差距比例" 降冪排序
            def sort_key_gap(row):
                """排序用：處理 None 和 NaN"""
                val = row.get("gap_ratio")
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    return float("-inf")
                return val

            sorted_data = sorted(data, key=sort_key_gap, reverse=True)

            rows = [headers] + [
                [
                    row.get("stock_id", ""),
                    row.get("stock_name", ""),
                    row.get("company_name", row.get("stock_name", "")),
                    row.get("industry_category", "-"),
                    row.get("industry_category2", "-"),
                    row.get("product_mix", "-"),
                    safe_price(row.get("today_price")),
                    safe_price(row.get("second_high_55d")),
                    safe_ratio(row.get("gap_ratio")),
                ]
                for row in sorted_data
            ]

            # 批次寫入
            worksheet.update(rows, "A1")

            logger.info(f"三線開花篩選結果匯出完成: {len(data)} 筆 -> {tab_name}")
            return True

        except gspread.exceptions.APIError as e:
            # Google API 限流，嘗試重試
            if "RATE_LIMIT_EXCEEDED" in str(e) or "429" in str(e):
                # 確認 worksheet 和 rows 已定義才能重試
                if 'worksheet' in dir() and 'rows' in dir():
                    for retry in range(GSHEET_MAX_RETRIES):
                        logger.warning(f"Google API 限流，{GSHEET_RETRY_DELAY} 秒後重試 ({retry + 1}/{GSHEET_MAX_RETRIES})...")
                        time.sleep(GSHEET_RETRY_DELAY * (retry + 1))
                        try:
                            worksheet.update(rows, "A1")
                            logger.info(f"三線開花篩選結果匯出完成: {len(data)} 筆 -> {tab_name}")
                            return True
                        except Exception:
                            continue
            logger.error(f"三線開花匯出失敗: {e}")
            return False
        except Exception as e:
            logger.error(f"三線開花匯出失敗: {e}")
            return False

    def health_check(self) -> bool:
        """檢查 Google Sheets 連線狀態"""
        return self.client is not None
