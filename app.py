"""
Factor Direct Web 应用
基于 Streamlit 的 MiniQMT 自动权重调仓应用
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
import sys

# 添加项目根目录到路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from config import (
    LOGIN_PASSWORD,
    TEMP_DIR
)
from core.logger import logger
from core.data_handler import (
    parse_position_file,
    validate_and_filter,
    save_position_to_parquet,
    load_latest_position,
    get_target_position
)
from core.trader import QMTWeightSyncTrader
from utils.status_manager import SchedulerStatusManager
from utils.market_data import (
    load_account_value_history,
    get_index_data,
    calculate_returns
)


# 页面配置
st.set_page_config(
    page_title="QMT Weight Sync - 自动权重调仓系统",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)


def login_page():
    """登录页面"""
    if "authenticated" not in st.session_state:
        st.session_state.authenticated = False

    if not st.session_state.authenticated:
        st.title("QMT Weight Sync - 登录")
        st.markdown("---")

        password = st.text_input("请输入密码", type="password", key="password_input")

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("登录", type="primary", width='stretch'):
                if password == LOGIN_PASSWORD:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("密码错误,请重试")

        return False

    return True


def handle_file_upload(uploaded_file):
    """
    处理文件上传

    Args:
        uploaded_file: Streamlit UploadedFile 对象
    """
    if uploaded_file is None:
        return

    try:
        # 保存临时文件
        temp_file_path = TEMP_DIR / uploaded_file.name
        with open(temp_file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())

        logger.info(f"文件已上传: {uploaded_file.name}")
        st.success(f"文件已上传: {uploaded_file.name}")

        # 解析文件
        with st.spinner("正在解析文件..."):
            df = parse_position_file(str(temp_file_path))

        st.success(f"解析成功: 共 {len(df)} 行数据")

        # 验证和过滤
        with st.spinner("正在验证和过滤数据..."):
            df = validate_and_filter(df)

        st.success(f"验证完成: 剩余 {len(df)} 行有效数据")

        # 显示数据预览
        st.subheader("数据预览")
        st.dataframe(df, width='stretch')

        # 按日期分组统计
        st.subheader("日期统计")
        date_stats = df.groupby('date').agg({
            'stock_code': 'count',
            'weight': 'sum'
        }).rename(columns={'stock_code': '股票数量', 'weight': '权重总和'})
        st.dataframe(date_stats, width='stretch')

        # 保存到 parquet
        dates = df['date'].unique()
        for date in dates:
            date_df = df[df['date'] == date]
            save_position_to_parquet(date_df, date)

        st.success(f"持仓数据已保存, 共 {len(dates)} 个日期")

    except Exception as e:
        st.error(f"处理文件时发生错误: {e}")
        logger.error("处理文件失败", exc_info=True)


def show_target_position():
    """显示目标持仓"""
    st.header("目标持仓")

    # 加载最新持仓
    df = load_latest_position()

    if df is None or len(df) == 0:
        st.warning("未找到持仓数据,请先上传持仓文件")
        return

    # 获取目标仓位
    target_df = get_target_position(df)

    if len(target_df) == 0:
        st.warning("目标仓位为空")
        return

    # 显示目标日期
    target_date = target_df['date'].iloc[0]
    st.info(f"目标日期: {target_date}")

    # 显示持仓表格
    st.subheader("持仓明细")
    display_df = target_df[['stock_code', 'weight']].copy()
    display_df['权重 (%)'] = (display_df['weight'] * 100).round(2)
    display_df = display_df.rename(columns={'stock_code': '股票代码', 'weight': '权重'})
    display_df = display_df[['股票代码', '权重 (%)']]
    st.dataframe(display_df, width='stretch')

    # 显示统计信息
    col1, col2 = st.columns(2)
    with col1:
        st.metric("持仓股票数", len(target_df))
    with col2:
        weight_sum = target_df['weight'].sum()
        st.metric("权重总和", f"{weight_sum:.2%}")


def show_rebalance_details():
    """显示调仓操作明细"""
    st.header("调仓操作明细")

    if st.button("计算调仓明细", type="primary", key="calc_rebalance"):
        try:
            # 1. 加载目标持仓
            with st.spinner("正在加载目标持仓..."):
                df = load_latest_position()
                if df is None or len(df) == 0:
                    st.warning("未找到持仓数据，请先上传持仓文件")
                    return

                target_df = get_target_position(df)
                if len(target_df) == 0:
                    st.warning("目标仓位为空")
                    return

            # 2. 连接交易账户
            with st.spinner("正在连接交易账户..."):
                trader = QMTWeightSyncTrader()
                if not trader.connect():
                    st.error("连接交易账户失败")
                    return

            # 3. 获取账户信息和当前持仓
            with st.spinner("正在获取账户信息..."):
                account_info = trader.get_account_info()
                current_position = trader.get_current_position()

            # 4. 计算目标股数
            with st.spinner("正在计算目标股数..."):
                target_volume = trader.calculate_target_volume(
                    target_df,
                    account_info['total_asset']
                )

            # 5. 计算买卖差异
            to_sell = []  # 需要卖出的股票
            to_buy = []   # 需要买入的股票
            no_change = 0  # 无变化的股票

            # 处理当前持仓中的股票
            for stock_code, current_info in current_position.items():
                current_vol = current_info['volume']
                target_vol = target_volume.get(stock_code, 0)

                if current_vol > target_vol:
                    # 需要卖出
                    to_sell.append({
                        '股票代码': stock_code,
                        '当前持仓': current_vol,
                        '目标持仓': target_vol,
                        '卖出数量': current_vol - target_vol,
                        '持仓均价': round(current_info['avg_price'], 2)
                    })
                elif current_vol < target_vol:
                    # 需要买入
                    to_buy.append({
                        '股票代码': stock_code,
                        '当前持仓': current_vol,
                        '目标持仓': target_vol,
                        '买入数量': target_vol - current_vol
                    })
                else:
                    no_change += 1

            # 处理目标持仓中但当前未持有的股票
            for stock_code, target_vol in target_volume.items():
                if stock_code not in current_position and target_vol > 0:
                    to_buy.append({
                        '股票代码': stock_code,
                        '当前持仓': 0,
                        '目标持仓': target_vol,
                        '买入数量': target_vol
                    })

            # 6. 显示结果
            st.success("调仓明细计算完成")

            # 显示汇总统计
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("需要买入", f"{len(to_buy)} 只")
            with col2:
                st.metric("需要卖出", f"{len(to_sell)} 只")
            with col3:
                st.metric("无变化", f"{no_change} 只")

            # 显示买入清单
            if len(to_buy) > 0:
                st.subheader("买入清单")
                buy_df = pd.DataFrame(to_buy)
                st.dataframe(buy_df, width='stretch')
            else:
                st.info("无需买入股票")

            # 显示卖出清单
            if len(to_sell) > 0:
                st.subheader("卖出清单")
                sell_df = pd.DataFrame(to_sell)
                st.dataframe(sell_df, width='stretch')
            else:
                st.info("无需卖出股票")

        except Exception as e:
            st.error(f"计算调仓明细失败: {e}")
            logger.error("计算调仓明细失败", exc_info=True)


def show_current_position():
    """显示当前持仓"""
    st.header("当前持仓")

    if st.button("刷新持仓", type="primary", key="refresh_position"):
        try:
            with st.spinner("正在连接交易账户..."):
                trader = QMTWeightSyncTrader()
                if not trader.connect():
                    st.error("连接交易账户失败")
                    return

            with st.spinner("正在查询持仓..."):
                position = trader.get_current_position()

            if len(position) == 0:
                st.info("当前无持仓")
                return

            # 转换为 DataFrame
            position_list = []
            for stock_code, info in position.items():
                position_list.append({
                    '股票代码': stock_code,
                    '持仓数量': info['volume'],
                    '可用数量': info['can_use_volume'],
                    '持仓市值': round(info['market_value'], 2),
                    '持仓均价': round(info['avg_price'], 2)
                })

            position_df = pd.DataFrame(position_list)

            # 显示汇总信息
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("持仓股票数", len(position))
            with col2:
                total_value = position_df['持仓市值'].sum()
                st.metric("总市值", f"{total_value:,.2f}")
            with col3:
                avg_value = position_df['持仓市值'].mean()
                st.metric("平均市值", f"{avg_value:,.2f}")

            # 显示持仓明细
            st.subheader("持仓明细")
            st.dataframe(position_df, width='stretch')

        except Exception as e:
            st.error(f"查询持仓失败: {e}")
            logger.error("查询持仓失败", exc_info=True)


def show_account_overview():
    """显示账户概览"""
    st.header("账户概览")

    if st.button("刷新账户", type="primary", key="refresh_account"):
        try:
            with st.spinner("正在连接交易账户..."):
                trader = QMTWeightSyncTrader()
                if not trader.connect():
                    st.error("连接交易账户失败")
                    return

            with st.spinner("正在查询账户信息..."):
                account_info = trader.get_account_info()

            # 显示账户信息
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric(
                    "总资产",
                    f"{account_info['total_asset']:,.2f}",
                    delta=None
                )

            with col2:
                st.metric(
                    "可用资金",
                    f"{account_info['cash']:,.2f}",
                    delta=None
                )

            with col3:
                st.metric(
                    "持仓市值",
                    f"{account_info['market_value']:,.2f}",
                    delta=None
                )

            with col4:
                st.metric(
                    "冻结资金",
                    f"{account_info['frozen_cash']:,.2f}",
                    delta=None
                )

            # 显示资产分布
            total = account_info['total_asset']
            cash_pct = (account_info['cash'] / total * 100) if total > 0 else 0
            market_pct = (account_info['market_value'] / total * 100) if total > 0 else 0
            frozen_pct = (account_info['frozen_cash'] / total * 100) if total > 0 else 0

            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("可用资金占比", f"{cash_pct:.2f}%")
            with col2:
                st.metric("持仓市值占比", f"{market_pct:.2f}%")
            with col3:
                st.metric("冻结资金占比", f"{frozen_pct:.2f}%")

        except Exception as e:
            st.error(f"查询账户信息失败: {e}")
            logger.error("查询账户信息失败", exc_info=True)

    # 显示账户收益率对比图表
    st.subheader("📈 账户收益率对比")

    try:
        # 加载账户市值历史数据
        history_df = load_account_value_history()

        if len(history_df) > 0:
            # 获取日期范围
            start_date = history_df['date'].min()
            end_date = history_df['date'].max()

            st.info(f"数据范围: {start_date.strftime('%Y-%m-%d')} 至 {end_date.strftime('%Y-%m-%d')}，共 {len(history_df)} 条记录")

            # 检查是否有足够的数据用于绘制收益率图表
            if len(history_df) < 2:
                st.info("💡 需要至少2条历史记录才能绘制收益率对比图表，请等待更多数据积累")
            else:
                # 获取上证指数数据
                with st.spinner("正在获取上证指数数据..."):
                    index_df = get_index_data(start_date, end_date)

                if len(index_df) == 0:
                    st.warning("无法获取上证指数数据（可能指数数据源暂时无法访问，或日期范围内无有效交易日数据）")
                else:
                    # 计算账户收益率
                    account_returns = calculate_returns(history_df, 'total_asset', 'date')

                    # 计算指数收益率
                    index_returns = calculate_returns(index_df, 'close', 'date')

                    if len(account_returns) > 0 and len(index_returns) > 0:
                        # 绘制双曲线图
                        fig = go.Figure()

                        # 添加账户收益率曲线
                        fig.add_trace(go.Scatter(
                            x=account_returns['date'],
                            y=account_returns['return_rate'],
                            mode='lines',
                            name='账户收益率',
                            line=dict(color='#1f77b4', width=2),
                            hovertemplate='<b>日期</b>: %{x|%Y-%m-%d}<br>' +
                                          '<b>账户收益率</b>: %{y:.2f}%<br>' +
                                          '<extra></extra>'
                        ))

                        # 添加上证指数收益率曲线
                        fig.add_trace(go.Scatter(
                            x=index_returns['date'],
                            y=index_returns['return_rate'],
                            mode='lines',
                            name='上证指数',
                            line=dict(color='#ff7f0e', width=2),
                            hovertemplate='<b>日期</b>: %{x|%Y-%m-%d}<br>' +
                                          '<b>上证指数收益率</b>: %{y:.2f}%<br>' +
                                          '<extra></extra>'
                        ))

                        # 添加零线
                        fig.add_hline(y=0, line_dash="dash", line_color="gray", opacity=0.5)

                        # 更新布局
                        fig.update_layout(
                            xaxis_title='日期',
                            yaxis_title='收益率 (%)',
                            hovermode='x unified',
                            legend=dict(
                                orientation="h",
                                yanchor="bottom",
                                y=1.02,
                                xanchor="right",
                                x=1
                            ),
                            height=500,
                            margin=dict(l=50, r=50, t=50, b=50)
                        )

                        # 显示图表
                        st.plotly_chart(fig, use_container_width=True)

                        # 显示统计信息
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            account_final_return = account_returns['return_rate'].iloc[-1]
                            st.metric("账户累计收益率", f"{account_final_return:.2f}%")
                        with col2:
                            index_final_return = index_returns['return_rate'].iloc[-1]
                            st.metric("上证指数累计收益率", f"{index_final_return:.2f}%")
                        with col3:
                            excess_return = account_final_return - index_final_return
                            st.metric("超额收益", f"{excess_return:.2f}%",
                                     delta=f"{excess_return:.2f}%")
                    else:
                        st.warning("收益率计算失败")
        else:
            st.info("暂无历史数据，请等待系统在每日15:10自动记录账户市值")

    except Exception as e:
        st.error(f"加载收益率对比图表失败: {e}")
        logger.error("加载收益率对比图表失败", exc_info=True)


def execute_immediate_rebalance():
    """手动立即执行调仓"""
    try:
        # 1. 加载最新持仓数据
        df = load_latest_position()
        if df is None or len(df) == 0:
            st.error("❌ 未找到持仓数据文件")
            return False

        # 2. 获取目标持仓
        target_df = get_target_position(df)
        if len(target_df) == 0:
            st.error("❌ 目标持仓为空")
            return False

        st.info(f"📊 目标持仓包含 {len(target_df)} 只股票")

        # 3. 连接交易账户
        trader = QMTWeightSyncTrader()
        if not trader.connect():
            st.error("❌ 连接交易账户失败")
            return False

        st.success("✅ 交易账户连接成功")

        # 4. 获取账户信息
        account_info = trader.get_account_info()
        st.info(f"💰 总资产: {account_info['total_asset']:.2f}, 可用资金: {account_info['cash']:.2f}")

        # 5. 获取当前持仓
        current_position = trader.get_current_position()
        st.info(f"📦 当前持有 {len(current_position)} 只股票")

        # 6. 计算目标股数
        target_volumes = trader.calculate_target_volume(
            target_df,
            account_info['total_asset']
        )
        st.info(f"🎯 计算完成，目标持仓 {len(target_volumes)} 只股票")

        # 7. 执行调仓
        with st.spinner("正在执行调仓..."):
            success = trader.execute_rebalance(target_volumes, current_position)

        if success:
            st.success("✅ 调仓执行成功！")
            logger.info("手动调仓执行成功")
            return True
        else:
            st.error("❌ 调仓执行失败")
            logger.error("手动调仓执行失败")
            return False

    except Exception as e:
        st.error(f"❌ 执行调仓时发生错误: {e}")
        logger.error(f"手动调仓执行错误: {e}", exc_info=True)
        return False


def show_scheduler_status():
    """显示调度器状态（只读）"""
    st.header("⚙️ 调度服务状态")

    status_manager = SchedulerStatusManager()

    try:
        status = status_manager.read_status()

        if status:
            col1, col2 = st.columns(2)

            with col1:
                last_run = status.get('last_run_time', 'N/A')
                st.metric("上次执行", last_run)

            with col2:
                next_run = status.get('next_run_time', 'N/A')
                st.metric("下次执行", next_run)

            # 显示上次执行结果
            st.subheader("上次执行结果")
            last_status = status.get('last_status', 'unknown')
            if last_status == 'success':
                st.success("✅ 执行成功")
            elif last_status == 'failed':
                error_msg = status.get('error_message', '未知错误')
                st.error(f"❌ 执行失败: {error_msg}")
            else:
                st.info("⏳ 尚未执行")

        else:
            st.warning("⚠️ 无法读取调度器状态，调度服务可能未启动")

    except Exception as e:
        st.error(f"读取调度状态失败: {e}")
        logger.error("读取调度状态失败", exc_info=True)

    # 添加立即调仓按钮
    st.divider()
    st.subheader("手动操作")

    col1, col2 = st.columns([1, 3])
    with col1:
        if st.button("🚀 立即调仓", type="primary", key="immediate_rebalance"):
            execute_immediate_rebalance()

    with col2:
        st.caption("⚠️ 点击后将立即根据最新的目标持仓文件执行调仓操作")


def main_app():
    """主应用界面"""
    st.title("📊 QMT Weight Sync - 自动权重调仓系统")

    # 1. 文件上传区域
    with st.expander("📁 持仓文件上传", expanded=False):
        uploaded_file = st.file_uploader(
            "选择持仓文件 (xlsx/csv)",
            type=['xlsx', 'xls', 'csv'],
            help="文件需包含列: date (日期), stock_code (股票代码), weight (仓位权重)"
        )

        if uploaded_file is not None:
            handle_file_upload(uploaded_file)

    # 2. 目标持仓
    with st.expander("🎯 目标持仓", expanded=True):
        show_target_position()

    # 3. 调仓操作明细
    with st.expander("📋 调仓操作明细", expanded=True):
        show_rebalance_details()

    # 4. 当前持仓
    with st.expander("💼 当前持仓", expanded=False):
        show_current_position()

    # 5. 账户概览
    with st.expander("📊 账户概览", expanded=False):
        show_account_overview()

    # 6. 调度服务状态（只读）
    with st.expander("⚙️ 调度服务状态", expanded=False):
        show_scheduler_status()



def main():
    """应用入口"""
    if not login_page():
        return

    main_app()


if __name__ == "__main__":
    main()
