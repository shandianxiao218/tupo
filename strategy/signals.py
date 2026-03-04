# -*- coding: utf-8 -*-
"""
买卖信号生成模块
"""

from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np


class SignalGenerator:
    """
    信号生成器

    基于技术指标生成买卖信号
    """

    @staticmethod
    def add_ma(df: pd.DataFrame, periods: List[int] = None) -> pd.DataFrame:
        """
        添加移动平均线

        Args:
            df: 股票数据
            periods: 均线周期列表

        Returns:
            添加了均线的DataFrame
        """
        if periods is None:
            periods = [5, 10, 20, 60]

        df = df.copy()

        for period in periods:
            df[f'ma{period}'] = df['close'].rolling(window=period).mean()

        return df

    @staticmethod
    def add_ema(df: pd.DataFrame, periods: List[int] = None) -> pd.DataFrame:
        """
        添加指数移动平均线

        Args:
            df: 股票数据
            periods: EMA周期列表

        Returns:
            添加了EMA的DataFrame
        """
        if periods is None:
            periods = [12, 26]

        df = df.copy()

        for period in periods:
            df[f'ema{period}'] = df['close'].ewm(span=period, adjust=False).mean()

        return df

    @staticmethod
    def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26,
                 signal: int = 9) -> pd.DataFrame:
        """
        添加MACD指标

        Args:
            df: 股票数据
            fast: 快线周期
            slow: 慢线周期
            signal: 信号线周期

        Returns:
            添加了MACD的DataFrame
        """
        df = df.copy()

        # 计算EMA
        df['ema_fast'] = df['close'].ewm(span=fast, adjust=False).mean()
        df['ema_slow'] = df['close'].ewm(span=slow, adjust=False).mean()

        # MACD线
        df['macd'] = df['ema_fast'] - df['ema_slow']

        # 信号线
        df['macd_signal'] = df['macd'].ewm(span=signal, adjust=False).mean()

        # 柱状图
        df['macd_hist'] = df['macd'] - df['macd_signal']

        return df

    @staticmethod
    def add_kdj(df: pd.DataFrame, n: int = 9, m1: int = 3, m2: int = 3) -> pd.DataFrame:
        """
        添加KDJ指标

        Args:
            df: 股票数据
            n: RSV周期
            m1: K值平滑周期
            m2: D值平滑周期

        Returns:
            添加了KDJ的DataFrame
        """
        df = df.copy()

        # 计算RSV
        low_n = df['low'].rolling(window=n).min()
        high_n = df['high'].rolling(window=n).max()
        df['rsv'] = (df['close'] - low_n) / (high_n - low_n) * 100

        # 计算K、D、J
        df['k'] = df['rsv'].ewm(com=m1 - 1, adjust=False).mean()
        df['d'] = df['k'].ewm(com=m2 - 1, adjust=False).mean()
        df['j'] = 3 * df['k'] - 2 * df['d']

        return df

    @staticmethod
    def add_boll(df: pd.DataFrame, period: int = 20, std_dev: float = 2) -> pd.DataFrame:
        """
        添加布林带指标

        Args:
            df: 股票数据
            period: 周期
            std_dev: 标准差倍数

        Returns:
            添加了布林带的DataFrame
        """
        df = df.copy()

        # 中轨
        df['boll_mid'] = df['close'].rolling(window=period).mean()

        # 标准差
        std = df['close'].rolling(window=period).std()

        # 上轨
        df['boll_upper'] = df['boll_mid'] + std_dev * std

        # 下轨
        df['boll_lower'] = df['boll_mid'] - std_dev * std

        # 带宽
        df['boll_width'] = (df['boll_upper'] - df['boll_lower']) / df['boll_mid']

        return df

    @staticmethod
    def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        添加RSI指标

        Args:
            df: 股票数据
            period: 周期

        Returns:
            添加了RSI的DataFrame
        """
        df = df.copy()

        # 计算涨跌
        delta = df['close'].diff()

        # 上涨和下跌
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)

        # 平均涨跌
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()

        # RSI
        rs = avg_gain / avg_loss
        df['rsi'] = 100 - (100 / (1 + rs))

        return df

    @staticmethod
    def add_volume_ma(df: pd.DataFrame, periods: List[int] = None) -> pd.DataFrame:
        """
        添加成交量均线

        Args:
            df: 股票数据
            periods: 均线周期列表

        Returns:
            添加了成交量均线的DataFrame
        """
        if periods is None:
            periods = [5, 10]

        df = df.copy()

        for period in periods:
            df[f'volume_ma{period}'] = df['volume'].rolling(window=period).mean()

        return df

    @staticmethod
    def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        添加ATR（平均真实波幅）指标

        Args:
            df: 股票数据
            period: 周期

        Returns:
            添加了ATR的DataFrame
        """
        df = df.copy()

        # 计算真实波幅
        df['tr1'] = df['high'] - df['low']
        df['tr2'] = abs(df['high'] - df['close'].shift(1))
        df['tr3'] = abs(df['low'] - df['close'].shift(1))

        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)

        # ATR
        df['atr'] = df['tr'].rolling(window=period).mean()

        return df

    @staticmethod
    def generate_ma_cross_signal(df: pd.DataFrame, fast_period: int = 5,
                                 slow_period: int = 20) -> pd.DataFrame:
        """
        生成均线交叉信号

        Args:
            df: 股票数据
            fast_period: 快线周期
            slow_period: 慢线周期

        Returns:
            添加了信号的DataFrame
        """
        df = df.copy()

        # 添加均线
        df = SignalGenerator.add_ma(df, [fast_period, slow_period])

        # 初始化信号
        df['signal'] = 0
        df['buy_signal'] = False
        df['sell_signal'] = False

        # 判断交叉
        fast_ma = f'ma{fast_period}'
        slow_ma = f'ma{slow_period}'

        # 金叉（快线上穿慢线）
        golden_cross = (df[fast_ma] > df[slow_ma]) & \
                       (df[fast_ma].shift(1) <= df[slow_ma].shift(1))

        # 死叉（快线下穿慢线）
        death_cross = (df[fast_ma] < df[slow_ma]) & \
                      (df[fast_ma].shift(1) >= df[slow_ma].shift(1))

        df.loc[golden_cross, 'signal'] = 1
        df.loc[golden_cross, 'buy_signal'] = True

        df.loc[death_cross, 'signal'] = -1
        df.loc[death_cross, 'sell_signal'] = True

        return df

    @staticmethod
    def generate_macd_signal(df: pd.DataFrame) -> pd.DataFrame:
        """
        生成MACD信号

        Args:
            df: 股票数据

        Returns:
            添加了信号的DataFrame
        """
        df = df.copy()

        # 添加MACD
        df = SignalGenerator.add_macd(df)

        # 初始化信号
        df['signal'] = 0
        df['buy_signal'] = False
        df['sell_signal'] = False

        # MACD金叉
        macd_golden = (df['macd'] > df['macd_signal']) & \
                      (df['macd'].shift(1) <= df['macd_signal'].shift(1))

        # MACD死叉
        macd_death = (df['macd'] < df['macd_signal']) & \
                     (df['macd'].shift(1) >= df['macd_signal'].shift(1))

        df.loc[macd_golden, 'signal'] = 1
        df.loc[macd_golden, 'buy_signal'] = True

        df.loc[macd_death, 'signal'] = -1
        df.loc[macd_death, 'sell_signal'] = True

        return df

    @staticmethod
    def generate_kdj_signal(df: pd.DataFrame,
                           buy_threshold: Tuple[int, int] = (20, 20),
                           sell_threshold: Tuple[int, int] = (80, 80)) -> pd.DataFrame:
        """
        生成KDJ信号

        Args:
            df: 股票数据
            buy_threshold: 买入阈值（K, D）
            sell_threshold: 卖出阈值（K, D）

        Returns:
            添加了信号的DataFrame
        """
        df = df.copy()

        # 添加KDJ
        df = SignalGenerator.add_kdj(df)

        # 初始化信号
        df['signal'] = 0
        df['buy_signal'] = False
        df['sell_signal'] = False

        k_buy, d_buy = buy_threshold
        k_sell, d_sell = sell_threshold

        # KDJ低位金叉
        kdj_buy = (df['k'] > df['d']) & \
                  (df['k'].shift(1) <= df['d'].shift(1)) & \
                  (df['k'] < k_buy) & (df['d'] < d_buy)

        # KDJ高位死叉
        kdj_sell = (df['k'] < df['d']) & \
                   (df['k'].shift(1) >= df['d'].shift(1)) & \
                   (df['k'] > k_sell) & (df['d'] > d_sell)

        df.loc[kdj_buy, 'signal'] = 1
        df.loc[kdj_buy, 'buy_signal'] = True

        df.loc[kdj_sell, 'signal'] = -1
        df.loc[kdj_sell, 'sell_signal'] = True

        return df

    @staticmethod
    def check_stop_loss(code: str, position: Dict, current_price: float,
                       stop_loss_pct: float = 0.08) -> bool:
        """
        检查是否止损

        Args:
            code: 股票代码
            position: 持仓信息
            current_price: 当前价格
            stop_loss_pct: 止损比例

        Returns:
            是否触发止损
        """
        if position['shares'] == 0:
            return False

        avg_cost = position.get('avg_cost', 0)
        if avg_cost <= 0:
            return False

        loss_pct = (current_price - avg_cost) / avg_cost

        return loss_pct <= -stop_loss_pct

    @staticmethod
    def check_take_profit(code: str, position: Dict, current_price: float,
                         take_profit_pct: float = 0.25) -> bool:
        """
        检查是否止盈

        Args:
            code: 股票代码
            position: 持仓信息
            current_price: 当前价格
            take_profit_pct: 止盈比例

        Returns:
            是否触发止盈
        """
        if position['shares'] == 0:
            return False

        avg_cost = position.get('avg_cost', 0)
        if avg_cost <= 0:
            return False

        profit_pct = (current_price - avg_cost) / avg_cost

        return profit_pct >= take_profit_pct

    @staticmethod
    def calculate_ma_convergence(df: pd.DataFrame,
                                ma_periods: List[int] = None) -> pd.Series:
        """
        计算均线聚拢程度

        Args:
            df: 股票数据
            ma_periods: 均线周期列表

        Returns:
            均线聚拢度（标准差/均值）
        """
        if ma_periods is None:
            ma_periods = [5, 10, 20, 60]

        df = df.copy()
        df = SignalGenerator.add_ma(df, ma_periods)

        ma_cols = [f'ma{p}' for p in ma_periods]

        # 计算标准差和均值
        ma_values = df[ma_cols]
        ma_std = ma_values.std(axis=1)
        ma_mean = ma_values.mean(axis=1)

        # 聚拢度 = 标准差 / 均值
        convergence = ma_std / ma_mean

        return convergence

    @staticmethod
    def detect_ma_convergence_breakout(df: pd.DataFrame,
                                       lookback_days: int = 60,
                                       convergence_threshold: float = 0.05) -> pd.DataFrame:
        """
        检测均线聚拢突破形态

        Args:
            df: 股票数据
            lookback_days: 回看周期
            convergence_threshold: 聚拢阈值

        Returns:
            添加了信号的DataFrame
        """
        df = df.copy()

        # 添加均线
        df = SignalGenerator.add_ma(df, [5, 10, 20, 60])

        # 计算聚拢度
        df['convergence'] = SignalGenerator.calculate_ma_convergence(df)

        # 检测突破
        df['signal'] = 0
        df['buy_signal'] = False

        # 聚拢条件
        is_converged = df['convergence'] < convergence_threshold

        # 突破条件：收盘价突破所有均线
        ma_cols = ['ma5', 'ma10', 'ma20', 'ma60']
        is_breakout = df[ma_cols].apply(
            lambda row: all(row['close'] > val for val in row[ma_cols]),
            axis=1
        )

        # 成交量放大（超过20日均线1.5倍）
        df = SignalGenerator.add_volume_ma(df, [20])
        is_volume_high = df['volume'] > df['volume_ma20'] * 1.5

        # 综合条件
        buy_condition = is_converged & is_breakout & is_volume_high

        df.loc[buy_condition, 'signal'] = 1
        df.loc[buy_condition, 'buy_signal'] = True

        return df
