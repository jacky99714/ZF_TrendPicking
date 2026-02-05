"""
Google Sheet 匯出模組
"""
import time
from datetime import date, datetime
from typing import Optional

import gspread
import numpy as np
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
        """取得 Spreadsheet 物件（含 503 重試機制）"""
        if not self.client:
            logger.error("未連線到 Google Sheets")
            return None

        for attempt in range(GSHEET_MAX_RETRIES + 1):
            try:
                return self.client.open_by_key(sheet_id)
            except gspread.exceptions.APIError as e:
                # 503 Service Unavailable 或其他可重試的錯誤
                if "503" in str(e) or "429" in str(e) or "500" in str(e):
                    if attempt < GSHEET_MAX_RETRIES:
                        wait_time = GSHEET_RETRY_DELAY * (attempt + 1)
                        logger.warning(
                            f"Google API 暫時不可用，{wait_time} 秒後重試 "
                            f"({attempt + 1}/{GSHEET_MAX_RETRIES})..."
                        )
                        time.sleep(wait_time)
                        continue
                logger.error(f"無法開啟 Sheet {sheet_id}: {e}")
                return None
            except Exception as e:
                logger.error(f"無法開啟 Sheet {sheet_id}: {e}")
                return None
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

            # 每月任務：清空並重寫所有資料（確保產業分類等欄位都是最新的）
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

            # 確保行數足夠
            required_rows = len(rows) + 10
            if worksheet.row_count < required_rows:
                worksheet.add_rows(required_rows - worksheet.row_count)

            # 清空並重寫
            worksheet.clear()
            worksheet.update(rows, "A1")
            logger.info(f"公司主檔匯出完成: {len(data)} 筆")

            return True

        except Exception as e:
            logger.error(f"公司主檔匯出失敗: {e}")
            return False

    def update_company_master_log(
        self,
        sheet_id: Optional[str] = None,
        note: str = "",
        success: bool = True
    ) -> bool:
        """
        更新公司主檔更新紀錄（統一表格格式）

        格式：
        A1: 更新紀錄
        A2: 時間 | 狀態 | 備註
        A3: 2026-01-22 06:28:58 | 成功 | VCP 257 檔

        Args:
            sheet_id: Sheet ID
            note: 備註（如 "VCP 257 檔, 三線 111 檔"）
            success: 是否成功

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
                    cols=3
                )

            # 取得現有資料
            existing_data = worksheet.get_all_values()

            # 準備新記錄
            now = datetime.now()
            time_str = now.strftime("%Y-%m-%d %H:%M:%S")
            status = "成功" if success else "失敗"
            new_record = [time_str, status, note]

            # 建立完整資料（標題 + 表頭 + 新記錄 + 舊記錄）
            title_row = ["更新紀錄"]
            header_row = ["時間", "狀態", "備註"]

            # 取得舊記錄（跳過標題和表頭）
            old_records = []
            if len(existing_data) > 2:
                old_records = existing_data[2:]

            # 組合：新記錄在最上面
            all_rows = [title_row, header_row, new_record] + old_records

            # 限制最多保留 100 筆記錄
            if len(all_rows) > 102:  # 標題 + 表頭 + 100筆
                all_rows = all_rows[:102]

            # 清空並重新寫入
            worksheet.clear()
            worksheet.update(all_rows, "A1")

            logger.info(f"更新紀錄已記錄: {time_str} | {status} | {note}")
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
        將錯誤日誌寫入 Google Sheet「台股更新紀錄」分頁（統一表格格式）

        使用 update_company_master_log() 寫入失敗記錄

        Args:
            error_logs: 錯誤日誌列表，每項包含 time, retry_count, status_code
            sheet_id: Sheet ID

        Returns:
            是否成功
        """
        if not error_logs:
            return True

        # 將錯誤日誌轉換為統一格式寫入
        for log in error_logs:
            status_code = log.get("status_code", "")
            retry_count = log.get("retry_count", 0)
            params = log.get("params", {})

            # 組合備註
            note = f"HTTP {status_code}"
            if retry_count > 0:
                note += f" (重試 {retry_count} 次)"
            if params:
                dataset = params.get("dataset", "")
                if dataset:
                    note += f" - {dataset}"

            # 使用統一格式寫入
            self.update_company_master_log(
                sheet_id=sheet_id,
                note=note,
                success=False
            )

        logger.info(f"已寫入 {len(error_logs)} 筆錯誤日誌到 Google Sheet")
        return True

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

            # 資料列（處理 NaN/inf 值）
            def safe_return(val):
                """安全格式化報酬率"""
                if val is None or (isinstance(val, float) and (pd.isna(val) or np.isinf(val))):
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

            # 自動排序頁籤（最新日期在前）
            self.sort_worksheets_by_date(sheet_id)

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
                            self.sort_worksheets_by_date(sheet_id)
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

            # 資料列（處理 NaN/inf 值）
            def safe_price(val):
                """安全格式化價格"""
                if val is None or (isinstance(val, float) and (pd.isna(val) or np.isinf(val))):
                    return "-"
                return f"{val:.2f}"

            def safe_ratio(val):
                """安全格式化比例"""
                if val is None or (isinstance(val, float) and (pd.isna(val) or np.isinf(val))):
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

            # 自動排序頁籤（最新日期在前）
            self.sort_worksheets_by_date(sheet_id)

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
                            self.sort_worksheets_by_date(sheet_id)
                            return True
                        except Exception:
                            continue
            logger.error(f"三線開花匯出失敗: {e}")
            return False
        except Exception as e:
            logger.error(f"三線開花匯出失敗: {e}")
            return False

    # ==================== 驗證資料匯出 ====================

    def export_verification(
        self,
        vcp_data: list[dict],
        sanxian_data: list[dict],
        target_date: date,
        market_return_20d: float = 0.0,
        sheet_id: Optional[str] = None
    ) -> bool:
        """
        匯出驗證資料（包含所有計算中間欄位）

        Args:
            vcp_data: VCP 驗證資料列表（包含所有計算欄位）
            sanxian_data: 三線開花驗證資料列表（包含所有計算欄位）
            target_date: 篩選日期
            market_return_20d: 大盤 20 日報酬率
            sheet_id: Sheet ID

        Returns:
            是否成功
        """
        sheet_id = sheet_id or SHEET_IDS.get("verification")
        if not sheet_id:
            logger.error("未設定驗證 Sheet ID")
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

            # 計算總行數（VCP + 間隔 + 三線開花）
            total_rows = max(len(vcp_data) + len(sanxian_data) + 10, 100)

            worksheet = sheet.add_worksheet(
                title=tab_name,
                rows=total_rows,
                cols=25,
                index=1
            )

            current_row = 1

            # ========== VCP 驗證區塊 ==========
            vcp_title = [[f"=== VCP 驗證資料 ({target_date}) === 大盤20日報酬: {market_return_20d:.4f}"]]
            worksheet.update(vcp_title, f"A{current_row}")
            current_row += 1

            vcp_headers = [
                "stock_id", "date", "close_price", "high_price",
                "ma50", "ma150", "ma200", "ma200_slope_20d",
                "return_20d", "high_5d", "high_252d", "gap_to_52w_high",
                "cond1_close>ma50", "cond2_ma50>ma150", "cond3_ma150>ma200",
                "cond4_ma200_up", "cond5_beat_market",
                "is_strong", "is_new_high", "is_vcp"
            ]
            worksheet.update([vcp_headers], f"A{current_row}")
            current_row += 1

            if vcp_data:
                def safe_val(val):
                    """安全格式化數值（處理 NaN/inf）"""
                    if val is None:
                        return ""
                    if isinstance(val, bool):
                        return "O" if val else ""
                    if isinstance(val, float):
                        if pd.isna(val) or np.isinf(val):
                            return ""
                        return round(val, 4)
                    return str(val)

                vcp_rows = []
                for row in vcp_data:
                    vcp_rows.append([
                        safe_val(row.get("stock_id")),
                        str(row.get("date", "")),
                        safe_val(row.get("close_price")),
                        safe_val(row.get("high_price")),
                        safe_val(row.get("ma50")),
                        safe_val(row.get("ma150")),
                        safe_val(row.get("ma200")),
                        safe_val(row.get("ma200_slope_20d")),
                        safe_val(row.get("return_20d")),
                        safe_val(row.get("high_5d")),
                        safe_val(row.get("high_252d")),
                        safe_val(row.get("gap_to_52w_high")),
                        safe_val(row.get("cond1")),
                        safe_val(row.get("cond2")),
                        safe_val(row.get("cond3")),
                        safe_val(row.get("cond4")),
                        safe_val(row.get("cond5")),
                        safe_val(row.get("is_strong")),
                        safe_val(row.get("is_new_high")),
                        safe_val(row.get("is_vcp")),
                    ])

                worksheet.update(vcp_rows, f"A{current_row}")
                current_row += len(vcp_rows)

            logger.info(f"VCP 驗證資料匯出完成: {len(vcp_data)} 筆")

            # 間隔
            current_row += 3

            # ========== 三線開花驗證區塊 ==========
            sanxian_title = [[f"=== 三線開花驗證資料 ({target_date}) ==="]]
            worksheet.update(sanxian_title, f"A{current_row}")
            current_row += 1

            sanxian_headers = [
                "stock_id", "date", "close_price",
                "ma8", "ma21", "ma55",
                "high_55d", "second_high_55d", "gap_ratio",
                "cond1_close>ma8", "cond2_ma8>ma21", "cond3_ma21>ma55",
                "cond4_new_high", "is_sanxian"
            ]
            worksheet.update([sanxian_headers], f"A{current_row}")
            current_row += 1

            if sanxian_data:
                sanxian_rows = []
                for row in sanxian_data:
                    sanxian_rows.append([
                        safe_val(row.get("stock_id")),
                        str(row.get("date", "")),
                        safe_val(row.get("close_price")),
                        safe_val(row.get("ma8")),
                        safe_val(row.get("ma21")),
                        safe_val(row.get("ma55")),
                        safe_val(row.get("high_55d")),
                        safe_val(row.get("second_high_55d")),
                        safe_val(row.get("gap_ratio")),
                        safe_val(row.get("cond1")),
                        safe_val(row.get("cond2")),
                        safe_val(row.get("cond3")),
                        safe_val(row.get("cond4")),
                        safe_val(row.get("is_sanxian")),
                    ])

                worksheet.update(sanxian_rows, f"A{current_row}")

            logger.info(f"三線開花驗證資料匯出完成: {len(sanxian_data)} 筆 -> {tab_name}")

            # 自動排序頁籤（最新日期在前）
            self.sort_worksheets_by_date(sheet_id)

            return True

        except gspread.exceptions.APIError as e:
            if "RATE_LIMIT_EXCEEDED" in str(e) or "429" in str(e):
                for retry in range(GSHEET_MAX_RETRIES):
                    logger.warning(f"Google API 限流，{GSHEET_RETRY_DELAY} 秒後重試 ({retry + 1}/{GSHEET_MAX_RETRIES})...")
                    time.sleep(GSHEET_RETRY_DELAY * (retry + 1))
                    try:
                        # 重試匯出（已包含排序）
                        return self.export_verification(
                            vcp_data, sanxian_data, target_date,
                            market_return_20d, sheet_id
                        )
                    except Exception:
                        continue
            logger.error(f"驗證資料匯出失敗: {e}")
            return False
        except Exception as e:
            logger.error(f"驗證資料匯出失敗: {e}")
            return False

    def sort_worksheets_by_date(
        self,
        sheet_id: str,
        fixed_tabs: list[str] = None
    ) -> bool:
        """
        按日期排序頁籤（最新的在前面）

        Args:
            sheet_id: Sheet ID
            fixed_tabs: 固定在最前面的頁籤名稱列表

        Returns:
            是否成功
        """
        import re

        sheet = self._get_sheet(sheet_id)
        if not sheet:
            return False

        try:
            worksheets = sheet.worksheets()
            fixed_tabs = fixed_tabs or []

            # 分離固定頁籤和日期頁籤
            fixed_worksheets = []
            date_worksheets = []

            for ws in worksheets:
                if ws.title in fixed_tabs:
                    fixed_worksheets.append(ws)
                elif re.match(r"^\d{6}$", ws.title):  # YYMMDD 格式
                    date_worksheets.append(ws)

            # 日期頁籤按名稱降序排列（最新在前）
            date_worksheets.sort(key=lambda x: x.title, reverse=True)

            # 重新排序：固定頁籤 + 日期頁籤
            new_order = fixed_worksheets + date_worksheets

            # 更新每個頁籤的 index
            for idx, ws in enumerate(new_order):
                if ws.index != idx:
                    ws.update_index(idx)

            logger.info(f"頁籤排序完成: {len(new_order)} 個頁籤")
            return True

        except Exception as e:
            logger.error(f"頁籤排序失敗: {e}")
            return False

    def health_check(self) -> bool:
        """檢查 Google Sheets 連線狀態"""
        return self.client is not None
