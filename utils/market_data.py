"""
市场数据获取模块
提供账户市值历史数据加载、指数数据获取、收益率计算等功能
"""
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from core.logger import logger
from config import ACCOUNT_VALUE_FILE


def load_account_value_history() -> pd.DataFrame:
    """
    加载账户市值历史数据

    Returns:
        pd.DataFrame: 历史记录数据，包含列: date, time, total_asset, cash, market_value
    """
    csv_file = Path(ACCOUNT_VALUE_FILE)

    if not csv_file.exists():
        logger.warning(f"市值记录文件不存在: {csv_file}")
        return pd.DataFrame()

    try:
        df = pd.read_csv(csv_file)

        # 转换日期格式
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')

        logger.info(f"成功加载 {len(df)} 条账户市值历史记录")
        return df
    except Exception as e:
        logger.error(f"加载账户市值历史失败: {e}", exc_info=True)
        return pd.DataFrame()


def _get_index_data_eastmoney(start_date_str: str, end_date_str: str) -> pd.DataFrame:
    """通过东方财富API获取上证指数历史数据"""
    try:
        url = 'https://push2his.eastmoney.com/api/qt/stock/kline/get'
        params = {
            'secid': '1.000001',
            'fields1': 'f1,f2,f3,f4,f5,f6,f7',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57',
            'klt': '101',
            'fqt': '0',
            'beg': start_date_str,
            'end': end_date_str,
        }

        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data_json = response.json()
            klines = (data_json.get("data") or {}).get("klines")

            if not klines:
                logger.warning("东方财富: 未获取到上证指数数据")
                return pd.DataFrame()

            df = pd.DataFrame([item.split(",") for item in klines])
            df.columns = ['date', 'open', 'close', 'high', 'low', 'volume', 'amount']
            df['date'] = pd.to_datetime(df['date'])
            df[['open', 'close', 'high', 'low', 'volume', 'amount']] = \
                df[['open', 'close', 'high', 'low', 'volume', 'amount']].astype(float)
            df.sort_values(by='date', inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df
        else:
            logger.warning(f"东方财富: HTTP状态码 {response.status_code}")
            return pd.DataFrame()

    except Exception as e:
        logger.warning(f"东方财富API不可用: {e}")
        return pd.DataFrame()


def _get_index_data_sina(start_date_str: str, end_date_str: str) -> pd.DataFrame:
    """通过新浪财经API获取上证指数历史数据（备用数据源）"""
    try:
        start_dt = pd.to_datetime(start_date_str)
        end_dt = pd.to_datetime(end_date_str)
        # 估算需要的交易日数量（日历天数 * 0.7 + 余量）
        calendar_days = (end_dt - start_dt).days + 1
        datalen = max(int(calendar_days * 0.8) + 20, 60)

        url = 'https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData'
        params = {
            'symbol': 'sh000001',
            'scale': '240',
            'ma': 'no',
            'datalen': str(datalen),
        }

        response = requests.get(url, params=params, timeout=10)

        if response.status_code == 200:
            data = response.json()
            if not data:
                logger.warning("新浪: 未获取到上证指数数据")
                return pd.DataFrame()

            df = pd.DataFrame(data)
            df = df.rename(columns={'day': 'date'})
            df['date'] = pd.to_datetime(df['date'])
            for col in ['open', 'close', 'high', 'low', 'volume']:
                df[col] = df[col].astype(float)
            if 'amount' not in df.columns:
                df['amount'] = 0.0

            # 按日期范围过滤
            df = df[(df['date'] >= start_dt) & (df['date'] <= end_dt)]
            df = df[['date', 'open', 'close', 'high', 'low', 'volume', 'amount']]
            df.sort_values(by='date', inplace=True)
            df.reset_index(drop=True, inplace=True)
            return df
        else:
            logger.warning(f"新浪: HTTP状态码 {response.status_code}")
            return pd.DataFrame()

    except Exception as e:
        logger.warning(f"新浪API不可用: {e}")
        return pd.DataFrame()


def get_index_data(start_date, end_date) -> pd.DataFrame:
    """
    获取上证指数历史数据（东方财富为主，新浪为备用）

    Args:
        start_date: 开始日期，支持格式: YYYYMMDD字符串 或 datetime对象
        end_date: 结束日期，支持格式: YYYYMMDD字符串 或 datetime对象

    Returns:
        pd.DataFrame: 指数历史数据，包含列: date, open, close, high, low, volume, amount
    """
    # 转换日期格式为YYYYMMDD
    def _to_date_str(d):
        if isinstance(d, str):
            return d.replace("-", "")
        elif isinstance(d, (datetime, pd.Timestamp)):
            return d.strftime("%Y%m%d")
        return str(d).replace("-", "")

    start_date_str = _to_date_str(start_date)
    end_date_str = _to_date_str(end_date)

    logger.info(f"获取上证指数数据: {start_date_str} 至 {end_date_str}")

    # 先尝试东方财富
    df = _get_index_data_eastmoney(start_date_str, end_date_str)
    if len(df) > 0:
        logger.info(f"成功获取 {len(df)} 条上证指数数据（东方财富）")
        return df

    # 东方财富失败，切换新浪
    logger.info("东方财富API不可用，切换到新浪数据源")
    df = _get_index_data_sina(start_date_str, end_date_str)
    if len(df) > 0:
        logger.info(f"成功获取 {len(df)} 条上证指数数据（新浪）")
        return df

    logger.error("所有数据源均无法获取上证指数数据")
    return pd.DataFrame()


def calculate_returns(df: pd.DataFrame, value_column: str, date_column: str = 'date') -> pd.DataFrame:
    """
    计算收益率（以第一天的值为基准）

    Args:
        df: 包含日期和价值列的DataFrame
        value_column: 价值列名（如 'total_asset' 或 'close'）
        date_column: 日期列名，默认为 'date'

    Returns:
        pd.DataFrame: 添加了 'return_rate' 列的DataFrame，收益率以百分比表示
    """
    if df is None or len(df) == 0:
        logger.warning("输入数据为空，无法计算收益率")
        return pd.DataFrame()

    try:
        # 复制数据避免修改原始数据
        result_df = df.copy()

        # 按日期排序
        result_df = result_df.sort_values(date_column).reset_index(drop=True)

        # 检查列是否存在
        if value_column not in result_df.columns:
            logger.error(f"列 '{value_column}' 不存在于DataFrame中")
            return pd.DataFrame()

        # 获取第一天的值作为基准
        initial_value = result_df[value_column].iloc[0]

        if initial_value == 0:
            logger.warning("初始值为0，无法计算收益率")
            result_df['return_rate'] = 0.0
            return result_df

        # 计算收益率（百分比）
        result_df['return_rate'] = (result_df[value_column] - initial_value) / initial_value * 100

        logger.info(f"成功计算 {len(result_df)} 条记录的收益率")
        return result_df

    except Exception as e:
        logger.error(f"计算收益率失败: {e}", exc_info=True)
        return pd.DataFrame()
