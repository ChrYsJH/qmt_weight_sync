"""
日志模块 - 复用自 current_quant/mylogger.py
修改日志文件路径为 data/logs/app.log
"""
import logging
import sys
from logging import Formatter, StreamHandler, FileHandler
from pathlib import Path
import inspect

# 导入配置
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
from config import LOG_DIR


class CustomLogger:
    def __init__(self, name, log_file=None, level=logging.DEBUG):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(level)

        # 定义输出格式
        formatter = Formatter(
            '%(asctime)s - %(filename)s:%(lineno)d - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        # 控制台输出
        console_handler = StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # 文件输出
        if log_file:
            file_handler = FileHandler(log_file, mode='a', encoding='utf-8')
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def _log(self, level, msg, *args, **kwargs):
        # 获取调用日志的文件名和行号
        frame = inspect.currentframe().f_back.f_back
        filename = inspect.getframeinfo(frame).filename
        lineno = inspect.getframeinfo(frame).lineno

        # 格式化消息 - 修复: 不对已经格式化的 f-string 进行二次格式化
        # 如果用户传递了 args，说明使用的是 logger.info("msg: {}", value) 格式
        if args:
            msg = msg.format(*args)

        # kwargs (如 exc_info=True) 不参与消息格式化，直接传递给底层 logger
        # 移除 kwargs 中不属于 format 的参数
        format_kwargs = {}
        logger_kwargs = {}
        for k, v in kwargs.items():
            if k in ['exc_info', 'stack_info', 'stacklevel', 'extra']:
                logger_kwargs[k] = v
            else:
                format_kwargs[k] = v

        # 如果有格式化参数，则格式化
        if format_kwargs:
            msg = msg.format(**format_kwargs)

        # 记录日志，传递 logger 特定参数
        self.logger.log(level, msg, **logger_kwargs)

    def debug(self, msg, *args, **kwargs):
        self._log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._log(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._log(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self._log(logging.CRITICAL, msg, *args, **kwargs)


# 创建全局 logger 实例，日志文件路径改为 data/logs/app.log
logger = CustomLogger('qmt_weight_sync', log_file=str(LOG_DIR / 'app.log'))


def create_service_logger(service_name, log_file_name):
    """
    为特定服务创建独立的 logger 实例

    Args:
        service_name: 服务名称（用于 logger 名称）
        log_file_name: 日志文件名（如 'web.log'）

    Returns:
        CustomLogger: 独立的 logger 实例
    """
    log_file_path = LOG_DIR / log_file_name
    return CustomLogger(f'qmt_weight_sync.{service_name}', log_file=str(log_file_path))
