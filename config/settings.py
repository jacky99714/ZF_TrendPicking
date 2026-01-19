"""
藏鋒系統設定檔
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# 專案根目錄
BASE_DIR = Path(__file__).resolve().parent.parent

# 載入 .env 檔案
load_dotenv(BASE_DIR / ".env")

# FinMind API 設定
FINMIND_API_URL = "https://api.finmindtrade.com/api/v4/data"
FINMIND_TOKEN = os.getenv("FINMIND_TOKEN", "")

# API 限流設定
API_CALLS_PER_HOUR = 600  # 註冊用戶: 600, 免費: 300

# PostgreSQL 資料庫設定
# 注意：生產環境請務必透過環境變數設定 DATABASE_URL，不要使用預設值
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://zf_user:zf_password@localhost:5432/zf_trend"
)

# 檢查是否使用預設密碼（僅警告，不阻擋執行）
if "zf_password" in DATABASE_URL and os.getenv("DATABASE_URL") is None:
    import warnings
    warnings.warn(
        "使用預設資料庫密碼，生產環境請設定 DATABASE_URL 環境變數",
        UserWarning
    )

# Google Sheet 設定
GOOGLE_CREDENTIALS_PATH = os.getenv(
    "GOOGLE_CREDENTIALS_PATH",
    str(BASE_DIR / "credentials.json")
)

SHEET_IDS = {
    "company_master": os.getenv("SHEET_ID_COMPANY_MASTER", ""),
    "tw_vcp": os.getenv("SHEET_ID_TW_VCP", ""),
    "us_vcp": os.getenv("SHEET_ID_US_VCP", ""),  # 暫不使用
    "tw_sanxian": os.getenv("SHEET_ID_TW_SANXIAN", ""),
    "us_sanxian": os.getenv("SHEET_ID_US_SANXIAN", ""),  # 暫不使用
}

# 技術指標參數
MA_PERIODS = {
    "short": [8, 21],       # 三線開花短期均線
    "medium": [50, 55],     # 中期均線
    "long": [150, 200],     # 長期均線
}

# VCP 篩選參數
VCP_PARAMS = {
    "ma50_period": 50,
    "ma150_period": 150,
    "ma200_period": 200,
    "lookback_20d": 20,
    "lookback_52w": 252,    # 52週約252個交易日
    "new_high_tolerance": 0.01,  # 1% 容差
}

# 三線開花篩選參數
SANXIAN_PARAMS = {
    "ma8_period": 8,
    "ma21_period": 21,
    "ma55_period": 55,
}

# 重試設定 (根據 SPEC: 最大重試次數應可從設定檔調整)
# 可透過環境變數 MAX_RETRIES 調整，預設 3 次
_max_retries_str = os.getenv("MAX_RETRIES", "3")
try:
    _max_retries = int(_max_retries_str)
    if _max_retries < 0:
        _max_retries = 3
except ValueError:
    _max_retries = 3

RETRY_CONFIG = {
    "max_retries": _max_retries,
    "retry_intervals": [300, 600, 3600],  # 5分/10分/1小時 (秒)
}

# 排程設定
SCHEDULE_CONFIG = {
    "daily_task_time": "17:45",  # 收盤後
    "monthly_task_day": 1,
    "monthly_task_time": "09:00",
}

# 日誌設定
LOG_CONFIG = {
    "level": os.getenv("LOG_LEVEL", "INFO"),
    "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    "file": str(BASE_DIR / "logs" / "zf_trend.log"),
}
