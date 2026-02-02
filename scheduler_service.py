"""
QMT Weight Sync 调度服务
独立后台进程，定时执行调仓任务
"""
import time
import signal
import sys
from pathlib import Path
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config import PREPARE_TIME, RECORD_TIME
from core.scheduler_runner import SchedulerRunner
from utils.status_manager import SchedulerStatusManager
from core.logger import create_service_logger
logger = create_service_logger('scheduler', 'scheduler.log')

# 调度器配置
SCHEDULER_CHECK_INTERVAL = 30  # 状态更新检查间隔（秒）


class SchedulerService:
    """调度服务 (基于 APScheduler)"""

    def __init__(self):
        self.runner = SchedulerRunner()
        self.status_manager = SchedulerStatusManager()
        self.is_running = False
        
        # 初始化后台调度器
        self.scheduler = BackgroundScheduler()

        # 注册信号处理器（优雅退出）
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """信号处理器"""
        logger.info(f"收到信号 {signum}, 准备退出...")
        self.stop()
        sys.exit(0)

    def _add_cron_job(self, func, time_str, job_id):
        """添加定时任务辅助函数"""
        try:
            hour, minute = time_str.split(':')
            trigger = CronTrigger(hour=hour, minute=minute)
            self.scheduler.add_job(func, trigger, id=job_id, replace_existing=True)
            logger.info(f"已安排每日 {time_str} 执行任务 [{job_id}]")
        except ValueError as e:
            logger.error(f"时间格式错误 {time_str}: {e}")

    def schedule_daily_task(self):
        """安排每日调仓任务"""
        self._add_cron_job(self.runner.execute_trading, PREPARE_TIME, "daily_trading")

    def schedule_value_recording_task(self):
        """安排每日市值记录任务"""
        self._add_cron_job(self.runner.execute_value_recording, RECORD_TIME, "value_recording")

    def _update_status(self):
        """更新下次执行时间状态"""
        jobs = self.scheduler.get_jobs()
        if not jobs:
            return

        # 找到最近的一次执行时间
        next_run_time = None
        for job in jobs:
            if job.next_run_time:
                # job.next_run_time 是带时区的 datetime
                # 转换为本地 naive datetime 以匹配之前的格式或保持一致
                run_time = job.next_run_time.replace(tzinfo=None)
                if next_run_time is None or run_time < next_run_time:
                    next_run_time = run_time
        
        if next_run_time:
            self.status_manager.update_next_run(next_run_time.strftime("%Y-%m-%d %H:%M:%S"))

    def start(self):
        """启动调度服务"""
        logger.info("=" * 80)
        logger.info("QMT Weight Sync 调度服务启动")
        logger.info("=" * 80)

        self.schedule_daily_task()
        self.schedule_value_recording_task()

        try:
            self.scheduler.start()
            self.is_running = True
            logger.info(f"调度器已启动，状态刷新间隔: {SCHEDULER_CHECK_INTERVAL} 秒")
            
            # 立即更新一次状态
            self._update_status()

            # 主循环：保持进程存活并定期更新状态文件
            while self.is_running:
                time.sleep(SCHEDULER_CHECK_INTERVAL)
                self._update_status()
                
        except (KeyboardInterrupt, SystemExit):
            self.stop()
        except Exception as e:
            logger.error(f"调度服务运行异常: {e}", exc_info=True)
            self.stop()

    def stop(self):
        """停止调度服务"""
        logger.info("正在停止调度服务...")
        self.is_running = False
        if self.scheduler.running:
            self.scheduler.shutdown()
        logger.info("调度服务已停止")


def main():
    """服务入口"""
    service = SchedulerService()
    try:
        service.start()
    except Exception as e:
        logger.error(f"调度服务异常: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
