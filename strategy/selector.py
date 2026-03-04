# -*- coding: utf-8 -*-
"""
选股策略模块
"""

from typing import List, Dict, Optional
import pandas as pd
import numpy as np
from .base import BaseStrategy
from .signals import SignalGenerator


class MAConvergenceBreakoutStrategy(BaseStrategy):
    """
    均线聚拢突破选股策略

    策略逻辑：
    1. 先上涨一波：过去90日内存在明显的上涨阶段（涨幅超过20%）
    2. 多条均线聚拢：MA5、MA10、MA20三条均线粘合（标准差/均值 < 2%）
    3. 股价突破：当前股价突破所有均线，且成交量放大1.5倍以上
    4. 买入时机：突破第二日开盘买入

    买入条件：
    - 过去90日内涨幅超过20%
    - 均线聚拢度 < 2%
    - 收盘价突破所有均线
    - 成交量放大 > 1.5倍

    卖出条件：
    - 止损：跌破突破日开盘价 或 跌破买入价10%
    - 卖出：股价连续2日低于MA20
    """

    def __init__(self,
                 lookback_days: int = 90,
                 min_rise_pct: float = 0.20,
                 convergence_pct: float = 0.03,
                 volume_ratio: float = 1.5,
                 ma_periods: List[int] = None,
                 stop_loss_pct: float = 0.10,
                 take_profit_pct: float = 0.25):
        """
        初始化策略

        Args:
            lookback_days: 回看周期（日）
            min_rise_pct: 最小上涨幅度（如0.20表示20%）
            convergence_pct: 均线粘合度阈值（标准差/均值）
            volume_ratio: 成交量放大倍数
            ma_periods: 均线周期列表
            stop_loss_pct: 止损比例
            take_profit_pct: 止盈比例
        """
        super().__init__(name="MAConvergenceBreakout")
        self.lookback_days = lookback_days
        self.min_rise_pct = min_rise_pct
        self.convergence_pct = convergence_pct
        self.volume_ratio = volume_ratio
        self.ma_periods = ma_periods or [5, 10, 20]
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct

        # 突破信号记录：{code: {'date': 突破日期, 'open_price': 突破日开盘价}}
        self._breakout_signals: Dict[str, Dict] = {}

        # 持仓的突破日信息：{code: {'breakout_open': 突破日开盘价}}
        self._position_breakout_info: Dict[str, Dict] = {}

    def on_bar(self, engine, bar_data: Dict[str, pd.Series], date: pd.Timestamp):
        """
        每个交易日回调

        Args:
            engine: 回测引擎
            bar_data: 当日行情数据
            date: 当前日期
        """
        # 1. 检查卖出信号（止损/止盈/卖出条件）- 在买入之前检查
        self._check_exit_signals(engine, bar_data, date)

        # 2. 检查是否执行买入（突破次日开盘买入）
        self._execute_pending_buys(engine, bar_data, date)

        # 3. 扫描新的突破信号
        self._scan_breakout_signals(engine, bar_data, date)

    def _scan_breakout_signals(self, engine, bar_data: Dict[str, pd.Series], date: pd.Timestamp):
        """
        扫描突破信号

        Args:
            engine: 回测引擎
            bar_data: 当日行情数据
            date: 当前日期
        """
        # 获取需要检查的股票（目标股票 + 持仓股票）
        codes_to_check = set()
        if hasattr(engine, 'target_stocks') and engine.target_stocks:
            codes_to_check.update(engine.target_stocks)
        codes_to_check.update(bar_data.keys())

        for code in codes_to_check:
            if code not in bar_data:
                continue

            # 已经有待买入信号或已持仓，跳过
            if code in self._breakout_signals or engine.has_position(code):
                continue

            # 检查是否突破
            if self._check_breakout(code, date):
                bar = bar_data[code]
                # 记录突破信号，等待次日开盘买入
                self._breakout_signals[code] = {
                    'date': date,
                    'open_price': bar['open']
                }

    def _check_breakout(self, code: str, date: pd.Timestamp) -> bool:
        """
        检查是否形成突破

        Args:
            code: 股票代码
            date: 检查日期

        Returns:
            是否突破
        """
        # 获取历史数据
        end_date = date.strftime('%Y-%m-%d')
        start_date = (date - pd.Timedelta(days=self.lookback_days * 2)).strftime('%Y-%m-%d')

        df = self.get_stock_data(code, start_date, end_date, qfq=True)

        if df is None or len(df) < self.lookback_days:
            return False

        # 检查是否有选股日期的数据
        if date not in df.index:
            return False

        # 获取选股日期的数据位置
        loc = df.index.get_loc(date)
        if loc < self.lookback_days:
            return False

        # 添加技术指标
        df = self._add_indicators(df)

        # 获取选股日的数据
        current = df.loc[date]


        # 检查各个条件
        condition1 = self._check_prior_rise(df, loc)           # 前期上涨
        condition2 = self._check_ma_convergence(current)        # 均线聚拢
        condition3 = self._check_price_breakout(current)        # 价格突破
        condition4 = self._check_volume_surge(current)         # 成交量放大

        return all([condition1, condition2, condition3, condition4])

    def _execute_pending_buys(self, engine, bar_data: Dict[str, pd.Series], date: pd.Timestamp):
        """
        执行待买入订单（突破次日开盘买入）

        Args:
            engine: 回测引擎
            bar_data: 当日行情数据
            date: 当前日期
        """
        # 找出需要今天买入的股票
        to_buy = []
        for code, signal in list(self._breakout_signals.items()):
            # 检查是否是突破次日
            if date > signal['date'] and code in bar_data:
                to_buy.append(code)

        # 执行买入
        for code in to_buy:
            if engine.has_position(code):
                self._breakout_signals.pop(code, None)
                continue

            if code in bar_data and self.validate_buy(code, date, bar_data):
                bar = bar_data[code]
                breakout_info = self._breakout_signals.pop(code)

                # 以开盘价买入
                result = engine.buy(code, price=bar['open'], reason="均线聚拢突破(次日开盘)")

                if result.get('status') == 'success':
                    # 记录持仓的突破日信息
                    self._position_breakout_info[code] = {
                        'breakout_open': breakout_info['open_price'],
                        'buy_price': bar['open']
                    }

    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加技术指标"""
        df = df.copy()

        # 添加均线
        df = SignalGenerator.add_ma(df, self.ma_periods)

        # 添加成交量均线
        df = SignalGenerator.add_volume_ma(df, [20])

        # 计算均线聚拢度（只使用MA5/10/20）
        ma_cols = [f'ma{p}' for p in self.ma_periods]
        ma_values = df[ma_cols]
        df['ma_std'] = ma_values.std(axis=1)
        df['ma_mean'] = ma_values.mean(axis=1)
        df['convergence'] = df['ma_std'] / df['ma_mean']

        return df

    def _check_prior_rise(self, df: pd.DataFrame, loc: int) -> bool:
        """
        检查是否有前期上涨

        条件：过去lookback_days日内最高点相对lookback_days日前涨幅 > min_rise_pct
        """
        lookback_data = df.iloc[loc - self.lookback_days:loc]

        if len(lookback_data) < self.lookback_days:
            return False

        start_price = df.iloc[loc - self.lookback_days]['close']
        max_price = lookback_data['high'].max()

        rise_pct = (max_price - start_price) / start_price


        return rise_pct >= self.min_rise_pct

    def _check_ma_convergence(self, current: pd.Series) -> bool:
        """
        检查均线是否聚拢

        条件：均线标准差/均值 < convergence_pct
        """
        convergence = current.get('convergence', 1)


        return convergence < self.convergence_pct

    def _check_price_breakout(self, current: pd.Series) -> bool:
        """
        检查价格是否突破

        条件：收盘价 > 所有均线
        """
        for period in self.ma_periods:
            ma_col = f'ma{period}'
            if ma_col not in current:
                return False
            if current['close'] <= current[ma_col]:
                return False

        return True

    def _check_volume_surge(self, current: pd.Series) -> bool:
        """
        检查成交量是否放大

        条件：成交量 > 20日成交量均线 * volume_ratio
        """
        volume_ma20 = current.get('volume_ma20', 0)

        if volume_ma20 <= 0:
            return False

        return current['volume'] > volume_ma20 * self.volume_ratio

    def _check_exit_signals(self, engine, bar_data: Dict[str, pd.Series],
                           date: pd.Timestamp):
        """检查退出信号"""
        position_codes = engine.get_position_codes()

        for code in position_codes:
            if code not in bar_data:
                continue

            position = engine.position_manager.get_position(code)
            current_price = bar_data[code]['close']
            current_bar = bar_data[code]

            # 获取持仓的突破日信息
            breakout_info = self._position_breakout_info.get(code, {})

            # 1. 检查跌破突破日开盘价止损
            if 'breakout_open' in breakout_info:
                breakout_open = breakout_info['breakout_open']
                if current_price < breakout_open:
                    engine.sell(code, price=current_price, reason=f"止损(跌破突破日开盘价{breakout_open:.2f})")
                    self._position_breakout_info.pop(code, None)
                    continue

            # 2. 检查跌破买入价10%止损
            if position.avg_cost > 0:
                loss_pct = (current_price - position.avg_cost) / position.avg_cost
                if loss_pct <= -self.stop_loss_pct:
                    engine.sell(code, price=current_price, reason=f"止损(亏损{loss_pct*100:.1f}%)")
                    self._position_breakout_info.pop(code, None)
                    continue

            # 3. 检查连续2日低于MA20卖出
            if self._check_below_ma20_consecutive(code, date, consecutive_days=2):
                engine.sell(code, price=current_price, reason="连续2日低于MA20")
                self._position_breakout_info.pop(code, None)
                continue

    def _check_below_ma20_consecutive(self, code: str, date: pd.Timestamp,
                                      consecutive_days: int = 2) -> bool:
        """
        检查是否连续N日收盘价低于MA20

        Args:
            code: 股票代码
            date: 当前日期
            consecutive_days: 连续天数

        Returns:
            是否连续N日低于MA20
        """
        # 获取足够的历史数据（包括当前日期之前的所有交易日）
        end_date = date.strftime('%Y-%m-%d')
        start_date = (date - pd.Timedelta(days=365)).strftime('%Y-%m-%d')  # 获取一年数据

        df = self.get_stock_data(code, start_date=start_date, end_date=end_date, qfq=True)

        if df is None or len(df) < consecutive_days + 20:  # 需要足够数据计算MA20
            return False

        # 添加MA20
        df['ma20'] = df['close'].rolling(window=20).mean()

        if date not in df.index:
            return False

        loc = df.index.get_loc(date)
        if loc < consecutive_days:
            return False

        # 检查最近consecutive_days天
        below_count = 0
        for i in range(consecutive_days):
            check_date = df.index[loc - i]
            row = df.loc[check_date]
            ma20 = row.get('ma20', 0)
            if ma20 > 0 and row['close'] < ma20:
                below_count += 1

        result = below_count >= consecutive_days

        # 调试输出
        if result and date >= pd.Timestamp('2022-07-01') and date <= pd.Timestamp('2022-12-31'):
            print(f"[DEBUG] {date.strftime('%Y-%m-%d')} 检查连续2日低于MA20: {result}")
            for i in range(consecutive_days):
                check_date = df.index[loc - i]
                row = df.loc[check_date]
                ma20 = row.get('ma20', 0)
                print(f"  {check_date.strftime('%Y-%m-%d')}: 收盘={row['close']:.2f}, MA20={ma20:.2f}")

        return result

    def on_exit(self, engine):
        """策略退出回调"""
        # 清理记录
        self._breakout_signals.clear()
        self._position_breakout_info.clear()


class SimpleMAStrategy(BaseStrategy):
    """
    简单均线策略（示例）

    买入条件：MA5 > MA10（金叉）
    卖出条件：MA5 < MA10（死叉）
    """

    def __init__(self, fast_period: int = 5, slow_period: int = 10):
        super().__init__(name="SimpleMA")
        self.fast_period = fast_period
        self.slow_period = slow_period

    def on_bar(self, engine, bar_data: Dict[str, pd.Series], date: pd.Timestamp):
        """每个交易日回调"""
        # 检查卖出信号
        self._check_sell_signals(engine, bar_data, date)

        # 检查买入信号
        self._check_buy_signals(engine, bar_data, date)

    def _check_buy_signals(self, engine, bar_data: Dict[str, pd.Series],
                          date: pd.Timestamp):
        """检查买入信号"""
        # 持仓已满，不买入
        if len(engine.get_position_codes()) >= engine.max_positions:
            return

        # 检查当前持有的股票（可选）
        for code, bar in bar_data.items():
            if self._check_buy_signal(code, date):
                if self.validate_buy(code, date, bar_data):
                    engine.buy(code, price=bar['close'], reason="均线金叉")

    def _check_sell_signals(self, engine, bar_data: Dict[str, pd.Series],
                           date: pd.Timestamp):
        """检查卖出信号"""
        for code in engine.get_position_codes():
            if code in bar_data and self._check_sell_signal(code, date):
                if self.validate_sell(code, date, bar_data):
                    engine.sell(code, price=bar_data[code]['close'], reason="均线死叉")

    def _check_buy_signal(self, code: str, date: pd.Timestamp) -> bool:
        """检查单只股票买入信号"""
        df = self.get_stock_data(code)

        if df is None or len(df) < self.slow_period + 5:
            return False

        df = SignalGenerator.add_ma(df, [self.fast_period, self.slow_period])

        if date not in df.index:
            return False

        loc = df.index.get_loc(date)
        if loc < 1:
            return False

        current = df.iloc[loc]
        previous = df.iloc[loc - 1]

        fast_ma = f'ma{self.fast_period}'
        slow_ma = f'ma{self.slow_period}'

        # 金叉
        golden_cross = (previous[fast_ma] <= previous[slow_ma]) and \
                       (current[fast_ma] > current[slow_ma])

        return golden_cross

    def _check_sell_signal(self, code: str, date: pd.Timestamp) -> bool:
        """检查单只股票卖出信号"""
        df = self.get_stock_data(code)

        if df is None or len(df) < self.slow_period + 5:
            return False

        df = SignalGenerator.add_ma(df, [self.fast_period, self.slow_period])

        if date not in df.index:
            return False

        loc = df.index.get_loc(date)
        if loc < 1:
            return False

        current = df.iloc[loc]
        previous = df.iloc[loc - 1]

        fast_ma = f'ma{self.fast_period}'
        slow_ma = f'ma{self.slow_period}'

        # 死叉
        death_cross = (previous[fast_ma] >= previous[slow_ma]) and \
                      (current[fast_ma] < current[slow_ma])

        return death_cross
