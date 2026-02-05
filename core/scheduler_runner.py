import time
from core.trader import QMTWeightSyncTrader
from core.data_handler import load_latest_position, get_target_position
from core.account_value_recorder import AccountValueRecorder
from utils.status_manager import SchedulerStatusManager
from utils.trading_calendar import TradingCalendar
from core.logger import create_service_logger
logger = create_service_logger('scheduler', 'scheduler.log')
from datetime import datetime, timedelta

class SchedulerRunner:
    """调度执行器"""

    def __init__(self):
        self.trader = QMTWeightSyncTrader()
        self.status_manager = SchedulerStatusManager()
        self.value_recorder = AccountValueRecorder()
        self.calendar = TradingCalendar()

    def execute_trading(self) -> bool:
        """
        执行调仓流程 (分阶段执行)

        阶段 1 (9:25 启动): 准备工作
        - 检查交易日
        - 加载数据
        - 连接账户
        - 计算目标股数
        - 打印计划交易名单

        阶段 2 (等待至 9:30):
        - 挂起等待开盘

        阶段 3 (9:30): 执行
        - 发送交易指令
        
        Returns:
            bool: 执行是否成功
        """
        logger.info("=" * 80)
        logger.info("开始执行定时调仓流程 (准备阶段)")
        logger.info("=" * 80)

        # 步骤 0: 检查今天是否为交易日
        logger.info("步骤 0: 检查今天是否为交易日...")
        if not self.is_trading_day_today():
            logger.info("今天不是交易日，跳过调仓执行")
            logger.info("=" * 80)
            return True

        self.status_manager.mark_running()

        try:
            # ================= 阶段 1: 准备 =================
            
            # 1. 加载持仓数据
            logger.info("步骤 1: 加载持仓数据...")
            df = load_latest_position()
            if df is None or len(df) == 0:
                raise ValueError("未找到持仓数据")
            logger.info(f"成功加载持仓数据，共 {len(df)} 条记录")

            # 2. 获取目标仓位
            logger.info("步骤 2: 获取目标仓位...")
            target_df = get_target_position(df)
            if len(target_df) == 0:
                raise ValueError("目标仓位为空")
            logger.info(f"目标仓位包含 {len(target_df)} 只股票")

            # 3. 连接交易账户
            logger.info("步骤 3: 连接交易账户...")
            if not self.trader.connect():
                raise ConnectionError("连接交易账户失败")
            logger.info("交易账户连接成功")

            # 4. 获取账户信息
            logger.info("步骤 4: 获取账户信息...")
            account_info = self.trader.get_account_info()
            logger.info(f"总资产: {account_info['total_asset']:.2f}")
            logger.info(f"可用资金: {account_info['cash']:.2f}")

            # 5. 获取当前持仓
            logger.info("步骤 5: 获取当前持仓...")
            current_position = self.trader.get_current_position()
            logger.info(f"当前持有 {len(current_position)} 只股票")

            # ================= 阶段 2: 等待 =================
            
            logger.info("-" * 80)
            logger.info("准备工作完成，等待至 09:30 执行交易...")
            self._wait_until_open(target_hour=9, target_minute=30)
            
            # ================= 阶段 3: 执行 =================

            logger.info("-" * 80)
            logger.info("触发时间已到，开始执行交易 (09:30)")
            logger.info("-" * 80)

            # 6. 获取开盘价并计算目标股数
            logger.info("步骤 6: 获取开盘价并计算目标股数...")
            target_volumes = self.trader.calculate_target_volume(
                target_df,
                account_info['total_asset']
            )

            if not target_volumes:
                logger.error("计算目标股数失败，无法执行调仓")
                self.status_manager.mark_completed(False, "计算目标股数失败")
                logger.error("=" * 80)
                logger.error("定时调仓执行失败")
                logger.error("=" * 80)
                return False

            logger.info(f"计算完成，目标持仓 {len(target_volumes)} 只股票")

            # 打印详细的交易计划
            self._log_trade_plan(target_volumes, current_position)

            # 7. 执行调仓
            logger.info("步骤 7: 执行调仓...")
            # 注意: execute_rebalance 内部会重新获取实时行情来决定委托价格，这是正确的
            success = self.trader.execute_rebalance(target_volumes, current_position)

            if success:
                self.status_manager.mark_completed(True, "调仓执行成功")
                logger.info("=" * 80)
                logger.info("定时调仓执行成功")
                logger.info("=" * 80)
                return True
            else:
                self.status_manager.mark_completed(False, "调仓执行失败")
                logger.error("=" * 80)
                logger.error("定时调仓执行失败")
                logger.error("=" * 80)
                return False

        except Exception as e:
            error_msg = f"执行调仓时发生错误: {e}"
            self.status_manager.mark_completed(False, error_msg)
            logger.error("=" * 80)
            logger.error(error_msg, exc_info=True)
            logger.error("=" * 80)
            return False

    def _wait_until_open(self, target_hour=9, target_minute=30):
        """等待直到指定时间"""
        while True:
            now = datetime.now()
            target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
            
            if now >= target_time:
                break
                
            # 计算剩余秒数
            remaining = (target_time - now).total_seconds()
            if remaining > 60:
                logger.info(f"等待开盘... 剩余 {int(remaining)} 秒")
                time.sleep(30) # 每30秒检查一次日志
            elif remaining > 0:
                logger.info(f"即将开盘... 剩余 {int(remaining)} 秒")
                time.sleep(remaining) # 最后一次直接sleep到位
                break
            else:
                break
                
    def _log_trade_plan(self, target_volumes, current_position):
        """记录计划交易明细"""
        logger.info("交易计划预览:")
        
        # 卖出计划
        for stock_code, pos_info in current_position.items():
            current_vol = pos_info['can_use_volume']
            target_vol = target_volumes.get(stock_code, 0)
            if current_vol > target_vol:
                logger.info(f"  [计划卖出] {stock_code}: {current_vol} -> {target_vol} (卖出 {current_vol - target_vol})")
                
        # 买入计划
        for stock_code, target_vol in target_volumes.items():
            current_vol = current_position.get(stock_code, {}).get('can_use_volume', 0)
            if target_vol > current_vol:
                 logger.info(f"  [计划买入] {stock_code}: {current_vol} -> {target_vol} (买入 {target_vol - current_vol})")

    def is_trading_day_today(self) -> bool:
        """判断今天是否为 A 股交易日"""
        try:
            today = datetime.now().date()
            is_trading = self.calendar.is_trading_day(today)

            if is_trading:
                logger.info(f"今天 {today.strftime('%Y%m%d')} 是交易日")
            else:
                logger.info(f"今天 {today.strftime('%Y%m%d')} 不是交易日")

            return is_trading
        except Exception as e:
            logger.error(f"查询交易日历失败: {e}")
            return False

    def execute_value_recording(self) -> bool:
        """
        执行账户市值记录

        步骤:
        1. 检查今天是否为交易日
        2. 调用市值记录器记录账户信息
        3. 记录执行结果

        Returns:
            bool: 执行是否成功
        """
        logger.info("=" * 80)
        logger.info("开始执行账户市值记录")
        logger.info("=" * 80)

        # 步骤 1: 检查今天是否为交易日
        logger.info("步骤 1: 检查今天是否为交易日...")
        if not self.is_trading_day_today():
            logger.info("今天不是交易日，跳过市值记录")
            logger.info("=" * 80)
            return True  # 返回 True 表示正常跳过，不是错误

        try:
            # 步骤 2: 记录账户市值
            logger.info("步骤 2: 记录账户市值...")
            success = self.value_recorder.record_account_value()

            if success:
                logger.info("=" * 80)
                logger.info("账户市值记录成功")
                logger.info("=" * 80)
                return True
            else:
                logger.error("=" * 80)
                logger.error("账户市值记录失败")
                logger.error("=" * 80)
                return False

        except Exception as e:
            error_msg = f"执行市值记录时发生错误: {e}"
            logger.error("=" * 80)
            logger.error(error_msg, exc_info=True)
            logger.error("=" * 80)
            return False

