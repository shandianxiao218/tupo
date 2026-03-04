# -*- coding: utf-8 -*-
"""
回测引擎核心模块
"""

from typing import Dict, List, Optional, Callable
from datetime import datetime, timedelta
import pandas as pd
from .position import PositionManager
from .order import OrderManager
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class BacktestEngine:
    """
    回测引擎

    核心功能：
    - 运行回测策略
    - 管理订单和持仓
    - 记录交易历史
    - 计算账户净值
    """

    def __init__(self, initial_capital: float = 1000000,
                 commission_rate: float = 0.0003,
                 min_commission: float = 5,
                 stamp_duty_rate: float = 0.001,
                 slippage: float = 0.001,
                 max_positions: int = 5,
                 stop_loss_pct: float = 0.05,
                 take_profit_pct: float = 0.25):
        """
        初始化回测引擎

        Args:
            initial_capital: 初始资金
            commission_rate: 佣金费率
            min_commission: 最低佣金
            stamp_duty_rate: 印花税率
            slippage: 滑点率
            max_positions: 最大持仓数量
            stop_loss_pct: 止损比例
            take_profit_pct: 止盈比例
        """
        self.initial_capital = initial_capital
        self.commission_rate = commission_rate
        self.min_commission = min_commission
        self.stamp_duty_rate = stamp_duty_rate
        self.slippage = slippage
        self.max_positions = max_positions
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct

        # 初始化管理器
        self.position_manager = PositionManager()
        self.position_manager.set_initial_capital(initial_capital)

        self.order_manager = OrderManager(
            commission_rate=commission_rate,
            min_commission=min_commission,
            stamp_duty_rate=stamp_duty_rate,
            slippage=slippage
        )

        # 回测状态
        self.current_date = None
        self.is_running = False

        # 数据记录
        self.trades: List[Dict] = []  # 交易记录
        self.daily_records: List[Dict] = []  # 每日记录
        self.equity_curve: List[Dict] = []  # 净值曲线

        # 指定要处理的股票代码列表
        self.target_stocks: List[str] = []

    def run(self, strategy, memory_manager,
            start_date: str, end_date: str):
        """
        运行回测

        Args:
            strategy: 策略对象
            memory_manager: 内存数据管理器
            start_date: 开始日期
            end_date: 结束日期
        """
        # 获取交易日期
        trading_dates = self._get_trading_dates(
            memory_manager, start_date, end_date
        )

        if not trading_dates:
            print("没有找到交易日期")
            return

        self.is_running = True
        self.current_date = trading_dates[0]

        # 设置内存管理器引用
        self.memory_manager = memory_manager

        # 初始化策略
        if hasattr(strategy, 'on_init'):
            strategy.on_init(self, memory_manager)

        # 逐日回测
        for date in trading_dates:
            self.current_date = date
            self._process_day(date, strategy, memory_manager)

        # 结束策略
        if hasattr(strategy, 'on_exit'):
            strategy.on_exit(self)

        self.is_running = False

    def _get_trading_dates(self, memory_manager,
                           start_date: str, end_date: str) -> List[pd.Timestamp]:
        """获取交易日期列表"""
        # 从内存数据中获取所有交易日期
        all_dates = set()

        for code in memory_manager.get_all_codes():
            df = memory_manager.get_stock_data(code, start_date, end_date)
            if df is not None:
                all_dates.update(df.index)

        dates = sorted(all_dates)
        return dates

    def _process_day(self, date: pd.Timestamp, strategy, memory_manager):
        """
        处理单个交易日

        Args:
            date: 交易日期
            strategy: 策略对象
            memory_manager: 内存数据管理器
        """
        # 获取当日数据
        bar_data = self._get_bar_data(date, memory_manager)

        if not bar_data:
            return

        # 更新持仓价格
        self._update_position_prices(bar_data)

        # 检查止损止盈
        self._check_stop_loss_take_profit(bar_data)

        # 调用策略
        if hasattr(strategy, 'on_bar'):
            strategy.on_bar(self, bar_data, date)

        # 记录当日状态
        self._record_daily_state(date, bar_data)

    def _get_bar_data(self, date: pd.Timestamp,
                      memory_manager) -> Dict[str, pd.Series]:
        """
        获取当日所有股票的行情数据

        Args:
            date: 交易日期
            memory_manager: 内存数据管理器

        Returns:
            股票代码到行情数据的映射
        """
        bar_data = {}

        # 获取所有持仓股票和候选股票的数据
        codes = set(self.position_manager.get_position_codes())

        # 如果策略有选股功能，获取候选股票
        if hasattr(self, '_candidate_stocks'):
            codes.update(self._candidate_stocks)

        # 添加目标股票列表
        if hasattr(self, 'target_stocks') and self.target_stocks:
            codes.update(self.target_stocks)

        for code in codes:
            # 从内存管理器获取当日行情（数据已前复权）
            bar = memory_manager.get_bar(code, date)
            if bar is not None:
                bar_data[code] = bar

        return bar_data

    def _update_position_prices(self, bar_data: Dict[str, pd.Series]):
        """更新持仓价格"""
        prices = {code: bar['close'] for code, bar in bar_data.items()
                  if code in self.position_manager.get_position_codes()}
        self.position_manager.update_prices(prices)

    def _check_stop_loss_take_profit(self, bar_data: Dict[str, pd.Series]):
        """检查止损止盈"""
        for code in list(self.position_manager.get_position_codes()):
            if code not in bar_data:
                continue

            position = self.position_manager.get_position(code)
            current_price = bar_data[code]['close']

            # 计算盈亏比例
            if position.avg_cost > 0:
                profit_pct = (current_price - position.avg_cost) / position.avg_cost

                # 检查止损
                if profit_pct <= -self.stop_loss_pct:
                    self.sell(code, shares=position.shares, reason="止损")

                # 检查止盈
                elif profit_pct >= self.take_profit_pct:
                    self.sell(code, shares=position.shares, reason="止盈")

    def _record_daily_state(self, date: pd.Timestamp,
                            bar_data: Dict[str, pd.Series]):
        """记录每日状态"""
        portfolio = self.position_manager.get_portfolio_value()

        record = {
            'date': date,
            'cash': portfolio['cash'],
            'position_value': portfolio['position_value'],
            'total_value': portfolio['total_value'],
            'profit_loss': portfolio['profit_loss'],
            'profit_loss_pct': portfolio['profit_loss_pct'],
            'position_count': self.position_manager.get_position_count()
        }

        self.daily_records.append(record)
        self.equity_curve.append({
            'date': date,
            'equity': portfolio['total_value'],
            'returns': (portfolio['total_value'] / self.initial_capital - 1) * 100
        })

    def buy(self, code: str, shares: int = None,
            price: float = None, reason: str = "") -> Dict:
        """
        买入股票

        Args:
            code: 股票代码
            shares: 买入数量（None表示按等权重分配）
            price: 买入价格（None表示市价）
            reason: 买入原因

        Returns:
            交易信息字典
        """
        # 检查是否已持仓
        if self.position_manager.has_position(code):
            return {'status': 'ignored', 'reason': 'already_hold'}

        # 检查持仓数量限制
        if self.position_manager.get_position_count() >= self.max_positions:
            return {'status': 'ignored', 'reason': 'max_positions'}

        # 获取价格
        if price is None:
            # 需要获取当前价格，这里简化处理
            # 实际应该在process_day中传入
            return {'status': 'ignored', 'reason': 'no_price'}

        # 计算买入数量（等权重）
        if shares is None:
            cash = self.position_manager.get_cash()
            target_positions = self.max_positions
            shares = self.order_manager.calculate_buy_shares(
                cash, price, max_cash_pct=1.0 / target_positions
            )

        if shares <= 0:
            return {'status': 'ignored', 'reason': 'insufficient_cash'}

        # 计算佣金
        commission = self.order_manager._calculate_commission(price, shares, 'buy')

        # 执行买入
        try:
            trade_info = self.position_manager.buy(
                code, shares, price, self.current_date, commission
            )
            trade_info['reason'] = reason
            self.trades.append(trade_info)

            return {'status': 'success', 'trade': trade_info}

        except ValueError as e:
            return {'status': 'failed', 'reason': str(e)}

    def sell(self, code: str, shares: int = None,
             price: float = None, reason: str = "") -> Dict:
        """
        卖出股票

        Args:
            code: 股票代码
            shares: 卖出数量（None表示全部）
            price: 卖出价格（None表示市价）
            reason: 卖出原因

        Returns:
            交易信息字典
        """
        position = self.position_manager.get_position(code)

        if position.is_empty():
            return {'status': 'ignored', 'reason': 'no_position'}

        # 默认卖出全部
        if shares is None:
            shares = position.shares

        # 获取价格
        if price is None:
            return {'status': 'ignored', 'reason': 'no_price'}

        # 计算费用
        commission = self.order_manager._calculate_commission(price, shares, 'sell')
        stamp_duty = self.order_manager._calculate_stamp_duty(price, shares)

        # 执行卖出
        try:
            trade_info = self.position_manager.sell(
                code, shares, price, self.current_date, commission
            )
            trade_info['stamp_duty'] = stamp_duty
            trade_info['reason'] = reason
            self.trades.append(trade_info)

            return {'status': 'success', 'trade': trade_info}

        except ValueError as e:
            return {'status': 'failed', 'reason': str(e)}

    def buy_stocks(self, stock_list: List[str], prices: Dict[str, float],
                   reason: str = "") -> List[Dict]:
        """
        批量买入股票（等权重）

        Args:
            stock_list: 股票代码列表
            prices: 股票价格映射
            reason: 买入原因

        Returns:
            交易信息列表
        """
        results = []

        # 获取可用资金
        cash = self.position_manager.get_cash()

        # 计算每只股票的买入数量
        buy_prices = [prices.get(code, 0) for code in stock_list]
        shares_list = self.order_manager.calculate_equal_weight_shares(
            cash, buy_prices, target_positions=len(stock_list)
        )

        for code, shares in zip(stock_list, shares_list):
            if shares > 0 and code in prices:
                result = self.buy(code, shares, prices[code], reason)
                results.append(result)

        return results

    def get_current_date(self) -> pd.Timestamp:
        """获取当前回测日期"""
        return self.current_date

    def get_cash(self) -> float:
        """获取可用资金"""
        return self.position_manager.get_cash()

    def get_total_value(self) -> float:
        """获取总资产"""
        return self.position_manager.get_total_value()

    def get_positions(self) -> Dict:
        """获取当前持仓"""
        return self.position_manager.get_all_positions()

    def get_position_codes(self) -> List[str]:
        """获取持仓股票代码"""
        return self.position_manager.get_position_codes()

    def has_position(self, code: str) -> bool:
        """是否持有某股票"""
        return self.position_manager.has_position(code)

    def get_trades(self) -> pd.DataFrame:
        """获取交易记录DataFrame"""
        if not self.trades:
            return pd.DataFrame()

        df = pd.DataFrame(self.trades)
        df = df.sort_values('date').reset_index(drop=True)
        return df

    def get_daily_records(self) -> pd.DataFrame:
        """获取每日记录DataFrame"""
        if not self.daily_records:
            return pd.DataFrame()

        df = pd.DataFrame(self.daily_records)
        df = df.sort_values('date').reset_index(drop=True)
        return df

    def get_equity_curve(self) -> pd.DataFrame:
        """获取净值曲线DataFrame"""
        if not self.equity_curve:
            return pd.DataFrame()

        df = pd.DataFrame(self.equity_curve)
        df = df.sort_values('date').reset_index(drop=True)
        return df

    def get_performance_summary(self) -> Dict:
        """获取绩效摘要"""
        if not self.daily_records:
            return {}

        first_record = self.daily_records[0]
        last_record = self.daily_records[-1]

        total_return = (last_record['total_value'] / self.initial_capital - 1) * 100

        # 计算最大回撤
        equity_df = self.get_equity_curve()
        if len(equity_df) > 0:
            max_drawdown = self._calculate_max_drawdown(equity_df['equity'])
        else:
            max_drawdown = 0

        # 统计交易
        trades_df = self.get_trades()
        if len(trades_df) > 0:
            sell_trades = trades_df[trades_df['direction'] == 'sell']
            win_trades = sell_trades[sell_trades['profit_loss'] > 0]
            lose_trades = sell_trades[sell_trades['profit_loss'] < 0]

            win_rate = len(win_trades) / len(sell_trades) * 100 if len(sell_trades) > 0 else 0

            avg_profit = win_trades['profit_loss'].mean() if len(win_trades) > 0 else 0
            avg_loss = lose_trades['profit_loss'].mean() if len(lose_trades) > 0 else 0

            profit_loss_ratio = abs(avg_profit / avg_loss) if avg_loss != 0 else 0
        else:
            win_rate = 0
            profit_loss_ratio = 0

        return {
            'initial_capital': self.initial_capital,
            'final_value': last_record['total_value'],
            'total_return': total_return,
            'max_drawdown': max_drawdown,
            'total_trades': len(trades_df),
            'win_rate': win_rate,
            'profit_loss_ratio': profit_loss_ratio
        }

    def _calculate_max_drawdown(self, equity_series: pd.Series) -> float:
        """计算最大回撤"""
        if len(equity_series) == 0:
            return 0

        cumulative_max = equity_series.cummax()
        drawdown = (equity_series - cumulative_max) / cumulative_max
        max_drawdown = drawdown.min() * 100

        return max_drawdown

    def reset(self):
        """重置回测引擎状态"""
        self.position_manager.clear()
        self.order_manager.clear()
        self.trades.clear()
        self.daily_records.clear()
        self.equity_curve.clear()
        self.current_date = None
        self.is_running = False
