"""
QMT Weight Sync 项目配置文件
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

# 项目根目录
PROJECT_ROOT = Path(__file__).parent

# ==================== 账户配置 ====================
ACCOUNT_ID = os.getenv("ACCOUNT_ID")
MINIQMT_PATH = os.getenv("MINIQMT_PATH")

# ==================== 路径配置 ====================
DATA_DIR = PROJECT_ROOT / "data" / "positions"
LOG_DIR = PROJECT_ROOT / "data" / "logs"
TEMP_DIR = PROJECT_ROOT / "data" / "temp"
ACCOUNT_VALUE_FILE = PROJECT_ROOT / "data" / "account_value_history.csv"
SCHEDULER_STATUS_FILE = PROJECT_ROOT / "data" / "scheduler_status.json"
TRADING_CALENDAR_CACHE_FILE = PROJECT_ROOT / "data" / "trading_calendar_cache.json"

# 确保目录存在
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

# ==================== 交易时间配置 ====================
PREPARE_TIME = "09:25"   # 每日启动准备工作的时间
RECORD_TIME = "15:10"    # 账户市值记录时间

# ==================== Streamlit 配置 ====================
LOGIN_PASSWORD = os.getenv("LOGIN_PASSWORD")
