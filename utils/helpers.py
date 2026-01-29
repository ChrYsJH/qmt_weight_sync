"""
辅助函数模块
"""
from datetime import datetime


def format_date(date_str: str) -> str:
    """
    格式化日期字符串为 YYYYMMDD

    Args:
        date_str: 日期字符串 (支持多种格式)

    Returns:
        str: YYYYMMDD 格式的日期字符串
    """
    # 尝试多种日期格式
    formats = [
        "%Y%m%d",
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y.%m.%d"
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y%m%d")
        except ValueError:
            continue

    raise ValueError(f"无法解析日期字符串: {date_str}")


def format_stock_code(stock_code: str) -> str:
    """
    格式化股票代码为标准格式 (6位代码.市场)

    Args:
        stock_code: 股票代码

    Returns:
        str: 标准格式的股票代码

    Examples:
        >>> format_stock_code("000001.XSHE")
        "000001.SZ"
        >>> format_stock_code("600000.XSHG")
        "600000.SH"
    """
    stock_code = str(stock_code).strip()

    # 替换市场代码
    stock_code = stock_code.replace("XSHE", "SZ").replace("XSHG", "SH")

    return stock_code


def validate_time_format(time_str: str) -> bool:
    """
    验证时间格式是否为 HH:MM

    Args:
        time_str: 时间字符串

    Returns:
        bool: 是否有效
    """
    try:
        datetime.strptime(time_str, "%H:%M")
        return True
    except ValueError:
        return False
