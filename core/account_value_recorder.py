"""
账户市值记录模块
每日15:10记录账户总资产、可用资金、持仓市值
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
from core.trader import FactorDirectTrader
from core.logger import logger
from config import ACCOUNT_VALUE_FILE


class AccountValueRecorder:
    """账户市值记录器"""

    def __init__(self):
        self.trader = FactorDirectTrader()
        self.csv_file = Path(ACCOUNT_VALUE_FILE)

    def record_account_value(self) -> bool:
        """
        记录当前账户市值到CSV文件

        Returns:
            bool: 记录是否成功
        """
        try:
            # 1. 连接交易账户
            logger.info("连接交易账户...")
            if not self.trader.connect():
                logger.error("连接交易账户失败")
                return False

            # 2. 获取账户信息
            logger.info("获取账户信息...")
            account_info = self.trader.get_account_info()

            # 3. 准备记录数据
            now = datetime.now()
            record = {
                'date': now.strftime('%Y%m%d'),
                'time': now.strftime('%H:%M:%S'),
                'total_asset': round(account_info['total_asset'], 2),
                'cash': round(account_info['cash'], 2),
                'market_value': round(account_info['market_value'], 2)
            }

            logger.info(f"账户信息: 总资产={record['total_asset']}, "
                       f"可用资金={record['cash']}, 持仓市值={record['market_value']}")

            # 4. 检查今天是否已经记录过
            if self._is_already_recorded(record['date']):
                logger.info(f"今天 {record['date']} 已经记录过市值，跳过")
                return True

            # 5. 追加到CSV文件
            self._append_to_csv(record)
            logger.info(f"成功记录账户市值: {record['date']} {record['time']}")

            return True

        except Exception as e:
            logger.error(f"记录账户市值失败: {e}", exc_info=True)
            return False

    def _is_already_recorded(self, date: str) -> bool:
        """
        检查指定日期是否已经记录过

        Args:
            date: 日期字符串 (格式: YYYYMMDD)

        Returns:
            bool: 是否已记录
        """
        if not self.csv_file.exists():
            return False

        try:
            df = pd.read_csv(self.csv_file)
            if 'date' in df.columns:
                return date in df['date'].astype(str).values
        except Exception as e:
            logger.warning(f"检查记录时出错: {e}")

        return False

    def _append_to_csv(self, record: dict):
        """
        追加记录到CSV文件

        Args:
            record: 记录字典
        """
        df = pd.DataFrame([record])

        # 如果文件不存在，创建并写入表头
        if not self.csv_file.exists():
            df.to_csv(self.csv_file, index=False, mode='w')
            logger.info(f"创建新的市值记录文件: {self.csv_file}")
        else:
            # 追加模式，不写入表头
            df.to_csv(self.csv_file, index=False, mode='a', header=False)

    def load_history(self) -> pd.DataFrame:
        """
        加载历史市值记录

        Returns:
            pd.DataFrame: 历史记录数据，包含列: date, time, total_asset, cash, market_value
        """
        if not self.csv_file.exists():
            logger.warning(f"市值记录文件不存在: {self.csv_file}")
            return pd.DataFrame()

        try:
            df = pd.read_csv(self.csv_file)
            logger.info(f"成功加载 {len(df)} 条历史记录")
            return df
        except Exception as e:
            logger.error(f"加载历史记录失败: {e}", exc_info=True)
            return pd.DataFrame()
