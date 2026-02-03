"""
交易执行模块 - 基于 xtquant 的交易封装
参考: current_quant/traderbackend_miniqmt.py (MiniQMT类)
"""
import time
import pandas as pd
from typing import Dict, Optional
from datetime import datetime

# xtquant 相关导入
from xtquant.xttype import StockAccount
from xtquant import xtconstant
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant import xtdata
xtdata.enable_hello = False

import sys
from pathlib import Path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from config import (
    ACCOUNT_ID,
    MINIQMT_PATH
)
from core.logger import logger

# 交易等待配置
MAX_WAIT_TIME = 300  # 等待委托成交的最大时间（秒）
CHECK_INTERVAL = 2   # 检查委托成交的时间间隔（秒）


class MyXtQuantTraderCallback(XtQuantTraderCallback):
    """交易回调类"""

    def on_disconnected(self):
        logger.warning(f"{datetime.now()} 连接断开回调")

    def on_stock_order(self, order):
        logger.info(f"{datetime.now()} 委托回调: {order.order_remark}")

    def on_stock_trade(self, trade):
        logger.info(f"{datetime.now()} 成交回调: {trade.order_remark}")

    def on_order_error(self, order_error):
        logger.error(f"委托报错回调 {order_error.order_remark} {order_error.error_msg}")

    def on_cancel_error(self, cancel_error):
        logger.error(f"{datetime.now()} 撤单错误回调")

    def on_account_status(self, status):
        logger.info(f"{datetime.now()} 账户状态回调")


class QMTWeightSyncTrader:
    """QMT Weight Sync 交易类 - 简化的交易接口"""

    def __init__(self, account_id: str = ACCOUNT_ID, miniqmt_path: str = MINIQMT_PATH):
        """
        初始化交易对象

        Args:
            account_id: 账户ID
            miniqmt_path: MiniQMT 客户端路径
        """
        self.account_id = account_id
        self.miniqmt_path = miniqmt_path

        # 创建交易线程
        self.session_id = int(time.time())
        self.xt_trader = XtQuantTrader(self.miniqmt_path, self.session_id)

        # 注册回调
        self.callback = MyXtQuantTraderCallback()
        self.xt_trader.register_callback(self.callback)

        # 创建账户对象
        self.xt_account = StockAccount(self.account_id, 'STOCK')

        logger.info(f"交易对象已创建 - 账户: {account_id}, Session: {self.session_id}")

    def connect(self) -> bool:
        """
        建立交易连接

        Returns:
            bool: 连接是否成功
        """
        logger.info("正在启动交易线程并建立连接...")
        self.xt_trader.start()

        connect_result = self.xt_trader.connect()
        if connect_result == 0:
            logger.info("账户连接成功")
            return True
        else:
            logger.error(f"账户连接失败,错误码: {connect_result}")
            return False

    def get_account_info(self) -> dict:
        """
        获取账户信息

        Returns:
            dict: {
                'total_asset': float,    # 总资产
                'cash': float,           # 可用资金
                'market_value': float,   # 持仓市值
                'frozen_cash': float     # 冻结资金
            }
        """
        res = self.xt_trader.query_stock_asset(self.xt_account)
        asset = {
            'total_asset': res.total_asset,
            'cash': res.cash,
            'market_value': res.market_value,
            'frozen_cash': res.frozen_cash
        }
        logger.info(f"账户信息: 总资产={asset['total_asset']:.2f}, 可用资金={asset['cash']:.2f}, "
                    f"持仓市值={asset['market_value']:.2f}, 冻结资金={asset['frozen_cash']:.2f}")
        return asset

    def get_current_position(self) -> dict:
        """
        获取当前持仓

        Returns:
            dict: {
                'stock_code': {
                    'volume': int,           # 持仓数量
                    'can_use_volume': int,   # 可用数量
                    'market_value': float,   # 持仓市值
                    'avg_price': float       # 持仓均价
                }
            }
        """
        positions = self.xt_trader.query_stock_positions(self.xt_account)
        cur_position_dict = {}
        filtered_count = 0

        for pos in positions:
            # 过滤负数和零值持仓
            if pos.volume <= 0:
                logger.warning(f"过滤异常持仓: {pos.stock_code}, "
                              f"volume={pos.volume}, can_use_volume={pos.can_use_volume}")
                filtered_count += 1
                continue

            cur_position_dict[pos.stock_code] = {
                'volume': pos.volume,
                'can_use_volume': pos.can_use_volume,
                'market_value': pos.market_value,
                'avg_price': pos.avg_price
            }

        if filtered_count > 0:
            logger.info(f"已过滤 {filtered_count} 条异常持仓数据")

        logger.info(f"当前持仓: {len(cur_position_dict)} 只股票")
        return cur_position_dict

    def calculate_target_volume(self, target_position: pd.DataFrame, total_asset: float) -> dict:
        """
        计算目标股数

        逻辑:
        1. 目标市值 = 总资产 * 权重
        2. 获取实时价格 (使用 xtdata.get_full_tick, 取卖3价 askPrice[2])
        3. 目标股数 = 目标市值 / 价格
        4. 四舍五入到 100 股

        Args:
            target_position: 目标持仓 DataFrame (列: stock_code, weight)
            total_asset: 总资产

        Returns:
            dict: {stock_code: target_volume}
        """
        logger.info(f"开始计算目标股数, 总资产: {total_asset:.2f}")

        target_volumes = {}
        stock_list = target_position['stock_code'].tolist()

        # 获取实时行情
        try:
            realtime_data = xtdata.get_full_tick(stock_list)
        except Exception as e:
            logger.error(f"获取实时行情失败: {e}")
            return {}

        # 计算每只股票的目标股数
        for _, row in target_position.iterrows():
            stock_code = row['stock_code']
            weight = row['weight']

            # 目标市值
            target_value = total_asset * weight

            # 获取卖3价
            if stock_code not in realtime_data:
                logger.warning(f"未找到股票 {stock_code} 的实时行情,跳过")
                continue

            price = realtime_data[stock_code]['askPrice'][2]  # 卖3价

            # 价格验证
            if price == 0:
                logger.warning(f"股票 {stock_code} 的卖3价为0,跳过")
                continue

            # 计算目标股数并四舍五入到100股
            target_volume = target_value / price
            target_volume = self._round_to_hundred(target_volume)

            target_volumes[stock_code] = target_volume
            logger.info(f"  {stock_code}: 权重={weight:.4f}, 目标市值={target_value:.2f}, "
                        f"卖3价={price:.2f}, 目标股数={target_volume}")

        return target_volumes

    @staticmethod
    def _round_to_hundred(volume: float) -> int:
        """四舍五入到 100 股"""
        return int(round(volume / 100) * 100)

    @staticmethod
    def _is_star_market(stock_code: str) -> bool:
        """
        判断是否为科创版股票

        科创版特征：股票代码以688开头，市场后缀为.SH
        例如：688022.SH

        Args:
            stock_code: 股票代码（格式：XXXXXX.XX）

        Returns:
            bool: True表示科创版股票
        """
        return stock_code.startswith('688') and stock_code.endswith('.SH')

    @staticmethod
    def _split_order_volume(volume: int, max_per_order: int = 100000) -> list:
        """
        将订单数量拆分为多笔（用于科创版限价单限制）

        Args:
            volume: 总数量
            max_per_order: 每笔最大数量（默认10万股）

        Returns:
            list: 拆分后的订单数量列表

        示例:
            250000 -> [100000, 100000, 50000]
            80000 -> [80000]
        """
        if volume <= max_per_order:
            return [volume]

        splits = []
        remaining = volume

        while remaining > 0:
            if remaining > max_per_order:
                splits.append(max_per_order)
                remaining -= max_per_order
            else:
                splits.append(remaining)
                remaining = 0

        return splits

    def execute_rebalance(self, target_volumes: dict, current_position: dict) -> bool:
        """
        执行完整调仓流程

        步骤:
        1. 计算差异: 需要卖出和买入的股票
        2. 先执行所有卖出 (使用买5价 bidPrice[4])
        3. 等待卖单完全成交
        4. 再执行所有买入 (使用卖5价 askPrice[4])
        5. 等待买单完全成交

        Args:
            target_volumes: 目标股数 {stock_code: volume}
            current_position: 当前持仓 {stock_code: {'can_use_volume': int, ...}}

        Returns:
            bool: 执行是否成功
        """
        logger.info("=" * 60)
        logger.info("开始执行调仓")
        logger.info("=" * 60)

        # 1. 计算差异
        sell_list = []  # 需要卖出的股票
        buy_list = []   # 需要买入的股票

        # 检查需要卖出的股票
        for stock_code, pos_info in current_position.items():
            current_volume = pos_info['can_use_volume']
            target_volume = target_volumes.get(stock_code, 0)

            if current_volume > target_volume:
                sell_volume = current_volume - target_volume
                sell_list.append((stock_code, sell_volume))
                logger.info(f"  [卖出] {stock_code}: 当前={current_volume}, 目标={target_volume}, 卖出={sell_volume}")

        # 检查需要买入的股票
        for stock_code, target_volume in target_volumes.items():
            current_volume = current_position.get(stock_code, {}).get('can_use_volume', 0)

            if target_volume > current_volume:
                buy_volume = target_volume - current_volume
                buy_list.append((stock_code, buy_volume))
                logger.info(f"  [买入] {stock_code}: 当前={current_volume}, 目标={target_volume}, 买入={buy_volume}")

        # 2. 执行卖出
        if sell_list:
            logger.info("-" * 60)
            logger.info(f"开始执行卖出, 共 {len(sell_list)} 只股票")
            logger.info("-" * 60)

            stock_list = [stock for stock, _ in sell_list]
            try:
                realtime_data = xtdata.get_full_tick(stock_list)
            except Exception as e:
                logger.error(f"获取卖出股票实时行情失败: {e}")
                return False

            for stock_code, volume in sell_list:
                if stock_code not in realtime_data:
                    logger.warning(f"未找到股票 {stock_code} 的实时行情,跳过卖出")
                    continue

                price = realtime_data[stock_code]['bidPrice'][4]  # 买5价

                if price == 0:
                    logger.warning(f"股票 {stock_code} 的买5价为0,跳过卖出")
                    continue

                # 判断是否为科创版股票，需要分笔下单
                if self._is_star_market(stock_code):
                    splits = self._split_order_volume(volume)
                    logger.info(f"  [科创版分笔] {stock_code}: 总数量={volume}, 拆分为{len(splits)}笔: {splits}")

                    for i, split_volume in enumerate(splits, 1):
                        self.xt_trader.order_stock(
                            self.xt_account, stock_code, xtconstant.STOCK_SELL, split_volume,
                            xtconstant.FIX_PRICE, price, "qmt_weight_sync", f"sell_stock_split_{i}"
                        )
                        logger.info(f"    [下单卖出-第{i}笔] {stock_code}: 数量={split_volume}, 价格={price:.2f}")
                else:
                    # 非科创版，正常下单
                    self.xt_trader.order_stock(
                        self.xt_account, stock_code, xtconstant.STOCK_SELL, volume,
                        xtconstant.FIX_PRICE, price, "qmt_weight_sync", "sell_stock"
                    )
                    logger.info(f"  [下单卖出] {stock_code}: 数量={volume}, 价格={price:.2f}")

            # 3. 等待卖单完全成交
            logger.info("等待卖单成交...")
            if not self.wait_for_orders_completion():
                logger.error("卖单等待成交超时")
                return False

        # 4. 执行买入
        if buy_list:
            logger.info("-" * 60)
            logger.info(f"开始执行买入, 共 {len(buy_list)} 只股票")
            logger.info("-" * 60)

            stock_list = [stock for stock, _ in buy_list]
            try:
                realtime_data = xtdata.get_full_tick(stock_list)
            except Exception as e:
                logger.error(f"获取买入股票实时行情失败: {e}")
                return False

            for stock_code, volume in buy_list:
                if stock_code not in realtime_data:
                    logger.warning(f"未找到股票 {stock_code} 的实时行情,跳过买入")
                    continue

                price = realtime_data[stock_code]['askPrice'][4]  # 卖5价

                if price == 0:
                    logger.warning(f"股票 {stock_code} 的卖5价为0,跳过买入")
                    continue

                # 判断是否为科创版股票，需要分笔下单
                if self._is_star_market(stock_code):
                    splits = self._split_order_volume(volume)
                    logger.info(f"  [科创版分笔] {stock_code}: 总数量={volume}, 拆分为{len(splits)}笔: {splits}")

                    for i, split_volume in enumerate(splits, 1):
                        self.xt_trader.order_stock(
                            self.xt_account, stock_code, xtconstant.STOCK_BUY, split_volume,
                            xtconstant.FIX_PRICE, price, "qmt_weight_sync", f"buy_stock_split_{i}"
                        )
                        logger.info(f"    [下单买入-第{i}笔] {stock_code}: 数量={split_volume}, 价格={price:.2f}")
                else:
                    # 非科创版，正常下单
                    self.xt_trader.order_stock(
                        self.xt_account, stock_code, xtconstant.STOCK_BUY, volume,
                        xtconstant.FIX_PRICE, price, "qmt_weight_sync", "buy_stock"
                    )
                    logger.info(f"  [下单买入] {stock_code}: 数量={volume}, 价格={price:.2f}")

            # 5. 等待买单完全成交
            logger.info("等待买单成交...")
            if not self.wait_for_orders_completion():
                logger.error("买单等待成交超时")
                return False

        logger.info("=" * 60)
        logger.info("调仓完成")
        logger.info("=" * 60)
        return True

    def wait_for_orders_completion(self, max_wait_time: int = MAX_WAIT_TIME,
                                    check_interval: int = CHECK_INTERVAL) -> bool:
        """
        等待委托单完全成交

        参考: traderbackend_miniqmt.py:290-333

        Args:
            max_wait_time: 最大等待时间(秒), 默认 5 分钟
            check_interval: 检查间隔(秒), 默认 2 秒

        Returns:
            bool: True 表示所有委托单已成交, False 表示超时或出现错误
        """
        start_time = time.time()
        logger.info(f"开始等待委托单成交 (最大等待时间: {max_wait_time}秒)...")

        while time.time() - start_time < max_wait_time:
            try:
                # 查询当日委托单
                orders = self.xt_trader.query_stock_orders(self.xt_account, False)

                if not orders:
                    logger.info("没有待成交的委托单")
                    return True

                # 检查未完全成交的委托单
                pending_orders = [order for order in orders if order.traded_volume < order.order_volume]

                if not pending_orders:
                    logger.info("所有委托单已完全成交")
                    return True

                # 记录待成交的委托单信息
                for order in pending_orders:
                    remaining = order.order_volume - order.traded_volume
                    progress = (order.traded_volume / order.order_volume) * 100
                    logger.info(f"  股票: {order.stock_code}, 成交进度: {order.traded_volume}/{order.order_volume} "
                                f"({progress:.1f}%), 待成交: {remaining}")

                # 等待指定间隔后再次检查
                time.sleep(check_interval)

            except Exception as e:
                logger.error(f"查询委托单状态时出错: {e}")
                time.sleep(check_interval)

        logger.warning(f"等待委托单成交超时 ({max_wait_time}秒)")
        return False
