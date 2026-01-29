"""
A股交易日历服务模块

提供交易日判断、获取下一交易日等功能
数据来源：深圳证券交易所API
降级策略：API失败时使用静态节假日数据
"""
import json
import requests
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Optional, Dict, Set
from core.logger import logger
from utils.holiday_constants import holidays, workdays


class TradingCalendar:
    """
    交易日历服务类（单例模式）

    功能：
    - 判断指定日期是否为交易日
    - 获取下一个交易日
    - 获取日期范围内的所有交易日
    - 自动缓存和刷新日历数据

    数据源：
    1. 内存缓存（最快）
    2. 本地文件缓存
    3. 深交所API实时获取
    4. 静态节假日数据 + 周末判断（降级）
    """

    _instance = None

    def __new__(cls):
        """单例模式"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """初始化交易日历服务"""
        if self._initialized:
            return

        # 深交所API配置
        self.api_base_url = "http://www.szse.cn/api/report/exchange/onepersistenthour/monthList"
        self.api_timeout = 10

        # 缓存配置
        from config import TRADING_CALENDAR_CACHE_FILE
        self.cache_file = TRADING_CALENDAR_CACHE_FILE

        # 内存缓存：存储交易日集合，用于快速查询
        self._trading_days_cache: Set[date] = set()

        # 月度缓存：存储每月的交易日数据
        self._monthly_cache: Dict[str, Dict] = {}

        # 初始化标记
        self._initialized = True

        # 加载缓存数据
        self._load_cache_from_file()

        # 预加载当月和下月数据
        self._preload_current_and_next_month()

        logger.info("交易日历服务初始化完成")

    def is_trading_day(self, check_date: date) -> bool:
        """
        判断指定日期是否为交易日

        Args:
            check_date: 要检查的日期

        Returns:
            bool: True表示是交易日，False表示不是交易日
        """
        try:
            # Level 1: 内存缓存查询
            if check_date in self._trading_days_cache:
                return True

            # Level 2: 尝试从月度缓存或API获取
            year_month = check_date.strftime("%Y-%m")
            if year_month not in self._monthly_cache or self._is_cache_expired(year_month):
                # 缓存不存在或已过期，尝试从API获取
                self._fetch_and_cache_month(check_date.year, check_date.month)

            # 再次检查内存缓存
            if check_date in self._trading_days_cache:
                return True

            # Level 3: 降级到静态节假日数据判断
            return self._is_trading_day_fallback(check_date)

        except Exception as e:
            logger.error(f"判断交易日时发生错误: {e}", exc_info=True)
            # Level 4: 最终降级，返回False（安全默认值）
            return False

    def get_next_trading_day(self, from_date: date, skip_days: int = 1) -> Optional[date]:
        """
        获取指定日期之后的第N个交易日

        Args:
            from_date: 起始日期
            skip_days: 跳过的交易日数量（默认1，即下一个交易日）

        Returns:
            Optional[date]: 下一个交易日，如果未找到则返回None
        """
        try:
            current_date = from_date
            found_count = 0
            max_search_days = 30  # 最多向后搜索30天

            for _ in range(max_search_days):
                current_date += timedelta(days=1)
                if self.is_trading_day(current_date):
                    found_count += 1
                    if found_count >= skip_days:
                        return current_date

            logger.warning(f"未能在{max_search_days}天内找到第{skip_days}个交易日")
            return None

        except Exception as e:
            logger.error(f"获取下一交易日时发生错误: {e}", exc_info=True)
            return None

    def get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """
        获取日期范围内的所有交易日

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            List[date]: 交易日列表
        """
        try:
            trading_days = []
            current_date = start_date

            while current_date <= end_date:
                if self.is_trading_day(current_date):
                    trading_days.append(current_date)
                current_date += timedelta(days=1)

            return trading_days

        except Exception as e:
            logger.error(f"获取交易日列表时发生错误: {e}", exc_info=True)
            return []

    def refresh_calendar(self) -> bool:
        """
        手动刷新日历数据

        Returns:
            bool: 刷新是否成功
        """
        try:
            logger.info("开始手动刷新交易日历...")

            # 清空内存缓存
            self._trading_days_cache.clear()
            self._monthly_cache.clear()

            # 重新加载当月和下月数据
            self._preload_current_and_next_month()

            # 保存到文件
            self._save_cache_to_file()

            logger.info("交易日历刷新完成")
            return True

        except Exception as e:
            logger.error(f"刷新交易日历时发生错误: {e}", exc_info=True)
            return False

    def _fetch_and_cache_month(self, year: int, month: int) -> bool:
        """
        从深交所API获取指定月份的交易日历并缓存

        Args:
            year: 年份
            month: 月份

        Returns:
            bool: 获取是否成功
        """
        try:
            month_str = f"{year:04d}-{month:02d}"

            # 发送API请求
            response = requests.get(
                url=self.api_base_url,
                params={"month": month_str},
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Referer": "http://www.szse.cn/disclosure/index.html"
                },
                timeout=self.api_timeout
            )

            if response.status_code != 200:
                logger.warning(f"深交所API返回状态码: {response.status_code}")
                return False

            data = response.json()
            trading_days = []

            # 解析交易日数据
            for day_data in data.get("data", []):
                if day_data.get("jybz") == "1":  # jybz=1 表示交易日
                    date_str = day_data.get("jyrq")
                    if date_str:
                        trading_day = datetime.strptime(date_str, "%Y-%m-%d").date()
                        trading_days.append(trading_day)
                        self._trading_days_cache.add(trading_day)

            # 计算缓存过期时间（下月1日）
            if month == 12:
                next_month = datetime(year + 1, 1, 1)
            else:
                next_month = datetime(year, month + 1, 1)

            # 保存到月度缓存
            self._monthly_cache[month_str] = {
                "trading_days": [d.strftime("%Y-%m-%d") for d in trading_days],
                "fetched_at": datetime.now().isoformat(),
                "expires_at": next_month.isoformat()
            }

            logger.info(f"成功从深交所API获取{month_str}的交易日历，共{len(trading_days)}个交易日")

            # 保存到文件
            self._save_cache_to_file()

            return True

        except requests.Timeout:
            logger.warning(f"深交所API请求超时（{self.api_timeout}秒）")
            return False
        except Exception as e:
            logger.error(f"从深交所API获取交易日历失败: {e}", exc_info=True)
            return False

    def _is_cache_expired(self, year_month: str) -> bool:
        """
        检查指定月份的缓存是否过期

        Args:
            year_month: 年月字符串，格式：YYYY-MM

        Returns:
            bool: True表示已过期，False表示未过期
        """
        try:
            if year_month not in self._monthly_cache:
                return True

            expires_at_str = self._monthly_cache[year_month].get("expires_at")
            if not expires_at_str:
                return True

            expires_at = datetime.fromisoformat(expires_at_str)
            return datetime.now() >= expires_at

        except Exception as e:
            logger.error(f"检查缓存过期时间失败: {e}")
            return True

    def _is_trading_day_fallback(self, check_date: date) -> bool:
        """
        使用静态节假日数据判断是否为交易日（降级方案）

        Args:
            check_date: 要检查的日期

        Returns:
            bool: True表示是交易日，False表示不是交易日
        """
        try:
            # 周末不是交易日
            if check_date.weekday() >= 5:  # 5=周六, 6=周日
                return False

            # 法定节假日不是交易日
            if check_date in holidays:
                return False

            # 调休工作日是交易日
            if check_date in workdays:
                return True

            # 其他工作日是交易日
            return True

        except Exception as e:
            logger.error(f"降级判断交易日失败: {e}")
            return False

    def _preload_current_and_next_month(self):
        """预加载当月和下月的交易日历数据"""
        try:
            now = datetime.now()

            # 加载当月
            self._fetch_and_cache_month(now.year, now.month)

            # 加载下月
            if now.month == 12:
                next_year, next_month = now.year + 1, 1
            else:
                next_year, next_month = now.year, now.month + 1

            self._fetch_and_cache_month(next_year, next_month)

        except Exception as e:
            logger.warning(f"预加载交易日历数据失败: {e}")

    def _load_cache_from_file(self):
        """从文件加载缓存数据"""
        try:
            if not self.cache_file.exists():
                logger.info("缓存文件不存在，将创建新缓存")
                return

            with open(self.cache_file, 'r', encoding='utf-8') as f:
                cache_data = json.load(f)

            # 加载月度缓存
            self._monthly_cache = cache_data.get("months", {})

            # 重建内存缓存
            self._trading_days_cache.clear()
            for month_str, month_data in self._monthly_cache.items():
                for date_str in month_data.get("trading_days", []):
                    trading_day = datetime.strptime(date_str, "%Y-%m-%d").date()
                    self._trading_days_cache.add(trading_day)

            logger.info(f"从缓存文件加载了{len(self._monthly_cache)}个月的交易日历数据")

        except Exception as e:
            logger.warning(f"加载缓存文件失败: {e}")

    def _save_cache_to_file(self):
        """保存缓存数据到文件"""
        try:
            # 确保目录存在
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)

            cache_data = {
                "cache_version": "1.0",
                "last_update": datetime.now().isoformat(),
                "months": self._monthly_cache
            }

            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)

            logger.debug("交易日历缓存已保存到文件")

        except Exception as e:
            logger.error(f"保存缓存文件失败: {e}")
