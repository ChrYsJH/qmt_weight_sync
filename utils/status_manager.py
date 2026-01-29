"""
调度器状态管理模块
"""
import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict
from config import SCHEDULER_STATUS_FILE


class SchedulerStatusManager:
    """调度器状态管理器"""

    def __init__(self, status_file: Path = SCHEDULER_STATUS_FILE):
        self.status_file = status_file
        self.status_file.parent.mkdir(parents=True, exist_ok=True)

    def read_status(self) -> Optional[Dict]:
        """
        读取调度器状态

        Returns:
            Optional[Dict]: 状态字典，如果文件不存在返回 None
        """
        if not self.status_file.exists():
            return None

        try:
            with open(self.status_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"读取状态文件失败: {e}")
            return None

    def write_status(self, status: Dict) -> None:
        """
        写入调度器状态

        Args:
            status: 状态字典
        """
        try:
            with open(self.status_file, "w", encoding="utf-8") as f:
                json.dump(status, f, indent=2, ensure_ascii=False)
        except Exception as e:
            print(f"写入状态文件失败: {e}")

    def update_next_run(self, next_run_time: str) -> None:
        """
        更新下次执行时间

        Args:
            next_run_time: 下次执行时间字符串
        """
        status = self.read_status() or {}
        status["next_run_time"] = next_run_time
        self.write_status(status)

    def mark_running(self) -> None:
        """标记为运行中"""
        status = self.read_status() or {}
        status["is_running"] = True
        status["last_run_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.write_status(status)

    def mark_completed(self, success: bool, message: str = "") -> None:
        """
        标记执行完成

        Args:
            success: 执行是否成功
            message: 执行结果消息
        """
        status = self.read_status() or {}
        status["is_running"] = False
        status["last_status"] = "success" if success else "failed"
        if message:
            status["error_message"] = message
        self.write_status(status)
