# -*- coding: utf-8 -*-
"""
策略基类模块
"""

from abc import ABC, abstractmethod
from typing import List, Dict, Optional
import pandas as pd
from datetime import datetime


class BaseStrategy(ABC):
    """
    策略基类

    所有交易策略应继承此类并实现相应方法
    """

    def __init__(self, name: str = "BaseStrategy"):
        """
        初始化策略

        Args:
            name: 策略名称
        """
        self.name = name
        self.engine = None
        self.memory_manager = None

    def on_init(self, engine, memory_manager):
        """
        策略初始化回调

        Args:
            engine: 回测引擎
            memory_manager: 内存数据管理器
        """
        self.engine = engine
        self.memory_manager = memory_manager

    def on_exit(self, engine):
        """
        策略退出回调

        Args:
            engine: 回测引擎
        """
        pass

    @abstractmethod
    def on_bar(self, engine, bar_data: Dict[str, pd.Series], date: pd.Timestamp):
        """
        每个交易日回调函数

        Args:
            engine: 回测引擎
            bar_data: 当日行情数据，股票代码到行情数据的映射
            date: 当前日期
        """
        pass

    def generate_signals(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        生成买卖信号

        Args:
            data: 股票数据，包含OHLCV

        Returns:
            添加了信号列的DataFrame
        """
        df = data.copy()

        # 默认信号：无信号
        df['signal'] = 0
        df['buy_signal'] = False
        df['sell_signal'] = False

        return df

    def select_stocks(self, date: pd.Timestamp) -> List[str]:
        """
        选股函数

        Args:
            date: 选股日期

        Returns:
            候选股票代码列表
        """
        return []

    def validate_buy(self, code: str, date: pd.Timestamp,
                     bar_data: Dict[str, pd.Series]) -> bool:
        """
        验证买入条件

        Args:
            code: 股票代码
            date: 当前日期
            bar_data: 当日行情数据

        Returns:
            是否可以买入
        """
        if code not in bar_data:
            return False

        bar = bar_data[code]

        # 基本检查
        if bar['close'] <= 0 or bar['volume'] <= 0:
            return False

        # 检查是否已持仓
        if self.engine.has_position(code):
            return False

        # 检查持仓数量限制
        if len(self.engine.get_position_codes()) >= self.engine.max_positions:
            return False

        return True

    def validate_sell(self, code: str, date: pd.Timestamp,
                      bar_data: Dict[str, pd.Series]) -> bool:
        """
        验证卖出条件

        Args:
            code: 股票代码
            date: 当前日期
            bar_data: 当日行情数据

        Returns:
            是否可以卖出
        """
        if code not in bar_data:
            return False

        # 检查是否有持仓
        if not self.engine.has_position(code):
            return False

        return True

    def calculate_position_size(self, code: str, price: float,
                                cash: float) -> int:
        """
        计算买入数量

        Args:
            code: 股票代码
            price: 价格
            cash: 可用资金

        Returns:
            买入数量（手）
        """
        return self.engine.order_manager.calculate_buy_shares(cash, price)

    def get_stock_data(self, code: str,
                       start_date: str = None,
                       end_date: str = None,
                       qfq: bool = True) -> Optional[pd.DataFrame]:
        """
        获取股票数据

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            qfq: 是否前复权（内存数据已前复权，此参数忽略）

        Returns:
            股票数据DataFrame
        """
        if self.memory_manager is None:
            return None

        # 内存数据已经过前复权处理
        return self.memory_manager.get_stock_data(code, start_date, end_date)


class SimpleBuyAndHoldStrategy(BaseStrategy):
    """
    简单买入持有策略（示例）

    在第一个交易日买入指定股票并持有到最后
    """

    def __init__(self, stock_codes: List[str] = None):
        super().__init__(name="SimpleBuyAndHold")
        self.stock_codes = stock_codes or []
        self.bought = False

    def on_bar(self, engine, bar_data: Dict[str, pd.Series], date: pd.Timestamp):
        """每个交易日回调"""
        if not self.bought and self.stock_codes:
            # 第一个交易日买入
            prices = {code: bar_data[code]['close']
                      for code in self.stock_codes if code in bar_data}
            engine.buy_stocks(self.stock_codes, prices, reason="买入持有")
            self.bought = True


class FixedAmountStrategy(BaseStrategy):
    """
    固定金额买入策略（示例）

    每月固定日期买入指定金额的股票
    """

    def __init__(self, stock_codes: List[str] = None,
                 buy_day: int = 1, buy_amount: float = 10000):
        super().__init__(name="FixedAmount")
        self.stock_codes = stock_codes or []
        self.buy_day = buy_day
        self.buy_amount = buy_amount

    def on_bar(self, engine, bar_data: Dict[str, pd.Series], date: pd.Timestamp):
        """每个交易日回调"""
        # 检查是否是买入日
        if date.day != self.buy_day:
            return

        if not self.stock_codes:
            return

        # 计算每只股票的买入数量
        for code in self.stock_codes:
            if code in bar_data and self.validate_buy(code, date, bar_data):
                price = bar_data[code]['close']
                shares = int(self.buy_amount / price / 100) * 100  # 整手

                if shares > 0:
                    engine.buy(code, shares, price, reason="定期定额买入")
