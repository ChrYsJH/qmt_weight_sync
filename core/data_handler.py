"""
数据处理模块 - 持仓文件解析和验证
参考: current_quant/utils.py (ReadCSV类)
"""
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import (
    DATA_DIR
)

# 允许的市场代码
ALLOWED_MARKETS = [".SH", ".SZ"]

# 股票代码市场转换映射
MARKET_CODE_MAP = {
    "XSHG": "SH",
    "XSHE": "SZ",
}
from core.logger import logger


def parse_wide_format_file(file_path: str) -> pd.DataFrame:
    """
    解析宽格式持仓文件（日期在列名中）

    文件格式:
        股票代码      46048 (Excel 日期序列号)
        688022.SH    0.004115
        301030.SZ    0.003956

    转换为长格式:
        date        stock_code  weight
        20260126    688022.SH   0.004115
        20260126    301030.SZ   0.003956

    Args:
        file_path: 文件路径

    Returns:
        pd.DataFrame: 长格式持仓数据
    """
    logger.info(f"解析宽格式持仓文件: {file_path}")

    # 读取文件（不指定 header）
    file_path_obj = Path(file_path)
    if file_path_obj.suffix == '.csv':
        df = pd.read_csv(file_path, header=None)
    elif file_path_obj.suffix in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path, header=None)
    else:
        raise ValueError(f"不支持的文件格式: {file_path_obj.suffix}")

    # 第一行第二列是日期（Excel 序列号）
    excel_date = df.iloc[0, 1]

    # 转换 Excel 日期序列号为 YYYYMMDD
    if isinstance(excel_date, (int, float)):
        # Excel 日期从 1899-12-30 开始计数
        python_date = datetime(1899, 12, 30) + timedelta(days=int(excel_date))
        date_str = python_date.strftime('%Y%m%d')
        logger.info(f"Excel 日期序列号 {excel_date} 转换为 {date_str}")
    elif isinstance(excel_date, str):
        # 处理 "1月26日" 这种中文日期格式
        if "月" in excel_date and "日" in excel_date:
            try:
                import re
                match = re.search(r'(\d+)月(\d+)日', excel_date)
                if match:
                    month = int(match.group(1))
                    day = int(match.group(2))
                    year = datetime.now().year
                    # 如果当前月份是1月，而文件是12月，可能意味着是去年的数据（虽然在这个上下文中不太可能，但为了健壮性）
                    # 这里简化处理，直接使用当前年份
                    python_date = datetime(year, month, day)
                    date_str = python_date.strftime('%Y%m%d')
                    logger.info(f"中文日期字符串 {excel_date} 转换为 {date_str}")
                else:
                    raise ValueError(f"无法解析中文日期格式: {excel_date}")
            except Exception as e:
                 raise ValueError(f"解析中文日期出错: {e}")
        else:
            # 尝试解析常规日期字符串
            try:
                python_date = pd.to_datetime(excel_date)
                date_str = python_date.strftime('%Y%m%d')
                logger.info(f"日期字符串 {excel_date} 转换为 {date_str}")
            except:
                raise ValueError(f"无法解析日期: {excel_date}")
    else:
        raise ValueError(f"未知日期格式类型: {type(excel_date)}")

    # 跳过第一行，提取股票代码和权重
    stock_data = df.iloc[1:].copy()
    stock_data.columns = ['stock_code', 'weight']

    # 添加日期列
    stock_data['date'] = date_str

    # 调整列顺序
    result = stock_data[['date', 'stock_code', 'weight']].copy()

    logger.info(f"解析完成: 日期={date_str}, 股票数={len(result)}")
    return result


def parse_position_file(file_path: str) -> pd.DataFrame:
    """
    解析持仓文件 (xlsx/csv)

    支持两种格式:
    1. 长格式: date, stock_code, weight (列名明确)
    2. 宽格式: 第一列股票代码，第二列权重，日期在表头

    Args:
        file_path: 文件路径

    Returns:
        pd.DataFrame: 标准化的持仓数据 (date, stock_code, weight)

    Raises:
        ValueError: 文件格式错误或缺少必需列
    """
    logger.info(f"开始解析持仓文件: {file_path}")

    # 先尝试读取，判断格式
    file_path_obj = Path(file_path)
    if file_path_obj.suffix == '.csv':
        df_test = pd.read_csv(file_path, nrows=1)
    elif file_path_obj.suffix in ['.xlsx', '.xls']:
        df_test = pd.read_excel(file_path, nrows=1)
    else:
        raise ValueError(f"不支持的文件格式: {file_path_obj.suffix}")

    # 权重文件格式为：第一行包含日期，第一列代码，第二列权重
    df = parse_wide_format_file(file_path)

    logger.info(f"成功解析持仓文件,共 {len(df)} 行数据")
    return df


def validate_and_filter(df: pd.DataFrame) -> pd.DataFrame:
    """
    验证和过滤持仓数据

    处理逻辑:
    1. 仅保留 .SH/.SZ 股票 (过滤北交所、科创板等)
    2. 过滤权重 <= 0 的记录
    3. 权重归一化 (总和为 1)
    4. 股票代码格式统一 (6位代码.市场)

    Args:
        df: 原始持仓数据

    Returns:
        pd.DataFrame: 验证和过滤后的持仓数据
    """
    logger.info("开始验证和过滤持仓数据")

    # 转换日期列为字符串格式 YYYYMMDD
    df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y%m%d')

    # 统一股票代码格式: XSHE -> SZ, XSHG -> SH
    df['stock_code'] = df['stock_code'].astype(str)
    for old_code, new_code in MARKET_CODE_MAP.items():
        df['stock_code'] = df['stock_code'].str.replace(old_code, new_code)

    # 仅保留 .SH/.SZ 股票
    before_filter = len(df)
    df = df[df['stock_code'].str.endswith(tuple(ALLOWED_MARKETS))].copy()
    filtered_count = before_filter - len(df)
    if filtered_count > 0:
        logger.info(f"过滤非 .SH/.SZ 股票: {filtered_count} 行")

    # 过滤权重 <= 0 的记录
    before_filter = len(df)
    df = df[df['weight'] > 0].copy()
    filtered_count = before_filter - len(df)
    if filtered_count > 0:
        logger.info(f"过滤权重 <= 0 的记录: {filtered_count} 行")

    # 按日期分组,权重归一化
    df['weight'] = df.groupby('date')['weight'].transform(lambda x: x / x.sum())

    logger.info(f"验证和过滤完成,剩余 {len(df)} 行数据")
    return df


def get_target_position(df: pd.DataFrame) -> pd.DataFrame:
    """
    获取目标仓位
    
    逻辑:
    始终尝试获取今日的目标仓位。
    如果今日没有数据，则使用数据中最近的一个日期（fallback）。

    Args:
        df: 持仓数据 (包含多个日期)

    Returns:
        pd.DataFrame: 当日目标仓位
    """
    # 获取今日日期
    today = datetime.now().strftime('%Y%m%d')
    target_date = today
    
    logger.info(f"正在获取目标仓位，目标日期: {target_date}")

    # 过滤目标日期的持仓
    target_df = df[df['date'] == target_date].copy()

    if len(target_df) == 0:
        # 如果没有目标日期的数据,使用最近的日期
        available_dates = sorted(df['date'].unique())
        if len(available_dates) > 0:
            target_date = available_dates[-1]
            logger.warning(f"未找到 {today} 的持仓数据,使用最近日期: {target_date}")
            target_df = df[df['date'] == target_date].copy()
        else:
            logger.error("持仓数据为空")
            return pd.DataFrame()

    logger.info(f"目标持仓日期: {target_date}, 共 {len(target_df)} 只股票")
    return target_df


def save_position_to_parquet(df: pd.DataFrame, date: str) -> str:
    """
    保存持仓数据到 parquet 文件

    文件命名: position_{YYYYMMDD}.parquet
    存储路径: data/positions/

    Args:
        df: 持仓数据
        date: 日期 (YYYYMMDD 格式)

    Returns:
        str: 保存的文件路径
    """
    file_name = f"position_{date}.parquet"
    file_path = DATA_DIR / file_name

    df.to_parquet(file_path, index=False)
    logger.info(f"持仓数据已保存到: {file_path}")

    return str(file_path)


def load_latest_position() -> Optional[pd.DataFrame]:
    """
    加载最新的持仓文件

    Returns:
        pd.DataFrame 或 None: 最新持仓数据,如果没有文件则返回 None
    """
    # 获取所有 parquet 文件
    parquet_files = list(DATA_DIR.glob("position_*.parquet"))

    if len(parquet_files) == 0:
        logger.warning("未找到持仓文件")
        return None

    # 按修改时间排序,获取最新文件
    latest_file = max(parquet_files, key=lambda p: p.stat().st_mtime)

    logger.info(f"加载最新持仓文件: {latest_file}")
    df = pd.read_parquet(latest_file)

    return df
