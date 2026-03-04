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
    1. 先上涨一波：过去N日内存在明显的上涨阶段（涨幅超过X%）
    2. 然后回调：从高点回调一定幅度，形成整理形态
    3. 多条均线聚拢：MA5、MA10、MA20、MA60等多条均线粘合（方差小于阈值）
    4. 股价突破：当前股价突破粘合的均线区域，且成交量放大

    买入条件：
    - 均线聚拢度 < 设定阈值
    - 收盘价突破所有均线
    - 成交量放大 > 设定倍数
    - 从前期高点回调在合理范围内

    卖出条件：
    - 止损：亏损超过设定比例
    - 止盈：盈利超过设定比例
    - 均线死叉
    """

    def __init__(self,
                 lookback_days: int = 60,
                 min_rise_pct: float = 0.20,
                 min_pullback: float = 0.05,
                 max_pullback: float = 0.15,
                 convergence_pct: float = 0.05,
                 volume_ratio: float = 1.5,
                 ma_periods: List[int] = None,
                 stop_loss_pct: float = 0.08,
                 take_profit_pct: float = 0.25):
        """
        初始化策略

        Args:
            lookback_days: 回看周期（日）
            min_rise_pct: 最小上涨幅度（如0.20表示20%）
            min_pullback: 最小回调幅度
            max_pullback: 最大回调幅度
            convergence_pct: 均线粘合度阈值（标准差/均值）
            volume_ratio: 成交量放大倍数
            ma_periods: 均线周期列表
            stop_loss_pct: 止损比例
            take_profit_pct: 止盈比例
        """
        super().__init__(name="MAConvergenceBreakout")
        self.lookback_days = lookback_days
        self.min_rise_pct = min_rise_pct
        self.min_pullback = min_pullback
        self.max_pullback = max_pullback
        self.convergence_pct = convergence_pct
        self.volume_ratio = volume_ratio
        self.ma_periods = ma_periods or [5, 10, 20, 60]
        self.stop_loss_pct = stop_loss_pct
        self.take_profit_pct = take_profit_pct

        self._selected_stocks = []

    def on_bar(self, engine, bar_data: Dict[str, pd.Series], date: pd.Timestamp):
        """
        每个交易日回调

        Args:
            engine: 回测引擎
            bar_data: 当日行情数据
            date: 当前日期
        """
        # 检查止损止盈
        self._check_exit_signals(engine, bar_data, date)

        # 如果有持仓，不继续选股（可选）
        # if len(engine.get_position_codes()) >= engine.max_positions:
        #     return

        # 执行选股
        selected = self.select_stocks(date)

        if selected:
            # 买入选中的股票
            prices = {code: bar_data[code]['close']
                      for code in selected if code in bar_data}

            engine.buy_stocks(selected, prices, reason="均线聚拢突破")

    def select_stocks(self, date: pd.Timestamp) -> List[str]:
        """
        选股

        Args:
            date: 选股日期

        Returns:
            符合条件的股票代码列表
        """
        if self.data_loader is None:
            return []

        # 获取所有股票代码
        all_codes = self.data_loader.get_all_stock_codes()

        # 限制处理数量以提高性能
        # 实际使用时可以处理所有股票
        max_check = 500
        codes_to_check = all_codes[:max_check] if len(all_codes) > max_check else all_codes

        selected = []

        for code in codes_to_check:
            if self._check_stock(code, date):
                selected.append(code)

        self._selected_stocks = selected
        return selected

    def _check_stock(self, code: str, date: pd.Timestamp) -> bool:
        """
        检查单只股票是否符合条件

        Args:
            code: 股票代码
            date: 检查日期

        Returns:
            是否符合条件
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
        condition1 = self._check_prior_rise(df, loc)
        condition2 = self._check_pullback(df, loc)
        condition3 = self._check_ma_convergence(current)
        condition4 = self._check_price_breakout(current)
        condition5 = self._check_volume_surge(current)

        return all([condition1, condition2, condition3, condition4, condition5])

    def _add_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加技术指标"""
        df = df.copy()

        # 添加均线
        df = SignalGenerator.add_ma(df, self.ma_periods)

        # 添加成交量均线
        df = SignalGenerator.add_volume_ma(df, [20])

        # 计算均线聚拢度
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

    def _check_pullback(self, df: pd.DataFrame, loc: int) -> bool:
        """
        检查是否回调到位

        条件：从最高点回调幅度在 min_pullback 到 max_pullback 之间
        """
        lookback_data = df.iloc[loc - self.lookback_days:loc]

        if len(lookback_data) < self.lookback_days:
            return False

        # 找到最高点位置
        max_idx = lookback_data['high'].idxmax()
        max_price = lookback_data['high'].max()

        # 当前价格
        current_price = df.iloc[loc]['close']

        # 计算回调幅度
        pullback_pct = (max_price - current_price) / max_price

        return self.min_pullback <= pullback_pct <= self.max_pullback

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
        """检查退出信号（止损止盈）"""
        position_codes = engine.get_position_codes()

        for code in position_codes:
            if code not in bar_data:
                continue

            position = engine.position_manager.get_position(code)
            current_price = bar_data[code]['close']

            # 检查止损
            if SignalGenerator.check_stop_loss(code, position.to_dict(),
                                              current_price, self.stop_loss_pct):
                engine.sell(code, reason="止损")

            # 检查止盈
            elif SignalGenerator.check_take_profit(code, position.to_dict(),
                                                  current_price, self.take_profit_pct):
                engine.sell(code, reason="止盈")

            # 检查均线死叉
            elif self._check_death_cross(code, date):
                engine.sell(code, reason="均线死叉")

    def _check_death_cross(self, code: str, date: pd.Timestamp) -> bool:
        """
        检查均线死叉

        条件：MA5下穿MA20
        """
        df = self.get_stock_data(code)

        if df is None or len(df) < 20:
            return False

        df = SignalGenerator.add_ma(df, [5, 20])

        if date not in df.index:
            return False

        loc = df.index.get_loc(date)
        if loc < 1:
            return False

        current = df.iloc[loc]
        previous = df.iloc[loc - 1]

        # 死叉：MA5从上方穿越MA20
        death_cross = (previous['ma5'] > previous['ma20']) and \
                      (current['ma5'] <= current['ma20'])

        return death_cross


class SimpleMAStrategy(BaseStrategy):
    """
    简单均线策略（示例）

    买入条件：MA5 > MA20（金叉）
    卖出条件：MA5 < MA20（死叉）
    """

    def __init__(self, fast_period: int = 5, slow_period: int = 20):
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
