# -*- coding: utf-8 -*-
"""
持仓管理模块
"""

from typing import Optional, Dict, List
from datetime import datetime
import pandas as pd


class Position:
    """
    持仓类

    表示单个股票的持仓信息
    """

    def __init__(self, code: str, shares: int = 0, avg_cost: float = 0.0):
        """
        初始化持仓

        Args:
            code: 股票代码
            shares: 持股数量
            avg_cost: 平均成本价
        """
        self.code = code
        self.shares = shares
        self.avg_cost = avg_cost
        self.current_price = 0.0
        self.market_value = 0.0
        self.profit_loss = 0.0
        self.profit_loss_pct = 0.0
        self.buy_date = None
        self.buy_records = []  # 买入记录列表

    def buy(self, shares: int, price: float, date: datetime, commission: float = 0):
        """
        买入股票

        Args:
            shares: 买入数量
            price: 买入价格
            date: 买入日期
            commission: 佣金
        """
        # 计算新的平均成本
        total_cost = self.avg_cost * self.shares + price * shares + commission
        total_shares = self.shares + shares

        if total_shares > 0:
            self.avg_cost = total_cost / total_shares

        self.shares = total_shares

        # 记录买入信息
        self.buy_records.append({
            'date': date,
            'shares': shares,
            'price': price,
            'commission': commission
        })

        # 记录首次买入日期
        if self.buy_date is None:
            self.buy_date = date

        self._update_market_value(price)

    def sell(self, shares: int, price: float, date: datetime,
             commission: float = 0) -> Dict:
        """
        卖出股票

        Args:
            shares: 卖出数量
            price: 卖出价格
            date: 卖出日期
            commission: 佣金

        Returns:
            交易信息字典，包含盈亏等
        """
        if shares > self.shares:
            shares = self.shares

        # 计算盈亏
        sell_amount = price * shares
        cost_amount = self.avg_cost * shares
        profit_loss = sell_amount - cost_amount - commission
        profit_loss_pct = (profit_loss / cost_amount) * 100 if cost_amount > 0 else 0

        # 更新持仓
        self.shares -= shares

        # 如果全部卖出，重置成本
        if self.shares == 0:
            self.avg_cost = 0.0
            self.buy_date = None
            self.buy_records = []

        trade_info = {
            'code': self.code,
            'date': date,
            'direction': 'sell',
            'shares': shares,
            'price': price,
            'amount': sell_amount,
            'cost': cost_amount,
            'profit_loss': profit_loss,
            'profit_loss_pct': profit_loss_pct,
            'commission': commission,
            'hold_days': self._get_hold_days(date)
        }

        self._update_market_value(price)

        return trade_info

    def update_price(self, price: float):
        """
        更新当前价格

        Args:
            price: 当前价格
        """
        self.current_price = price
        self._update_market_value(price)

    def _update_market_value(self, price: float):
        """更新市值和浮动盈亏"""
        self.current_price = price
        self.market_value = price * self.shares

        if self.shares > 0 and self.avg_cost > 0:
            cost_value = self.avg_cost * self.shares
            self.profit_loss = self.market_value - cost_value
            self.profit_loss_pct = (self.profit_loss / cost_value) * 100
        else:
            self.profit_loss = 0.0
            self.profit_loss_pct = 0.0

    def _get_hold_days(self, sell_date: datetime) -> int:
        """计算持仓天数"""
        if self.buy_date is None:
            return 0

        if isinstance(sell_date, str):
            sell_date = pd.to_datetime(sell_date)

        delta = sell_date - self.buy_date
        return delta.days

    def get_value(self) -> float:
        """获取持仓市值"""
        return self.market_value

    def get_profit_loss(self) -> tuple:
        """
        获取盈亏信息

        Returns:
            (盈亏金额, 盈亏百分比)
        """
        return self.profit_loss, self.profit_loss_pct

    def is_empty(self) -> bool:
        """是否空仓"""
        return self.shares == 0

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'code': self.code,
            'shares': self.shares,
            'avg_cost': self.avg_cost,
            'current_price': self.current_price,
            'market_value': self.market_value,
            'profit_loss': self.profit_loss,
            'profit_loss_pct': self.profit_loss_pct,
            'buy_date': self.buy_date,
            'hold_days': self._get_hold_days(datetime.now()) if self.buy_date else 0
        }


class PositionManager:
    """
    持仓管理器

    管理所有股票的持仓
    """

    def __init__(self):
        """初始化持仓管理器"""
        self._positions: Dict[str, Position] = {}
        self._cash: float = 0.0
        self._initial_capital: float = 0.0

    def set_initial_capital(self, capital: float):
        """设置初始资金"""
        self._initial_capital = capital
        self._cash = capital

    def get_cash(self) -> float:
        """获取可用资金"""
        return self._cash

    def set_cash(self, cash: float):
        """设置可用资金"""
        self._cash = cash

    def get_total_value(self) -> float:
        """获取总资产（现金+持仓市值）"""
        total = self._cash
        for pos in self._positions.values():
            total += pos.get_value()
        return total

    def get_initial_capital(self) -> float:
        """获取初始资金"""
        return self._initial_capital

    def get_position(self, code: str) -> Position:
        """
        获取指定股票的持仓

        Args:
            code: 股票代码

        Returns:
            Position对象
        """
        if code not in self._positions:
            self._positions[code] = Position(code)
        return self._positions[code]

    def has_position(self, code: str) -> bool:
        """
        是否持有某股票

        Args:
            code: 股票代码

        Returns:
            是否持有
        """
        if code not in self._positions:
            return False
        return not self._positions[code].is_empty()

    def get_all_positions(self) -> Dict[str, Position]:
        """获取所有持仓"""
        return {code: pos for code, pos in self._positions.items()
                if not pos.is_empty()}

    def get_position_count(self) -> int:
        """获取持仓股票数量"""
        return len(self.get_all_positions())

    def get_position_codes(self) -> List[str]:
        """获取所有持仓股票代码"""
        return list(self.get_all_positions().keys())

    def update_prices(self, prices: Dict[str, float]):
        """
        批量更新持仓价格

        Args:
            prices: 股票代码到价格的映射
        """
        for code, price in prices.items():
            if code in self._positions:
                self._positions[code].update_price(price)

    def buy(self, code: str, shares: int, price: float,
            date: datetime, commission: float = 0) -> Dict:
        """
        买入股票

        Args:
            code: 股票代码
            shares: 买入数量
            price: 买入价格
            date: 买入日期
            commission: 佣金

        Returns:
            交易信息字典
        """
        # 检查资金是否足够
        required_amount = price * shares + commission
        if required_amount > self._cash:
            raise ValueError(f"资金不足: 需要 {required_amount:.2f}, 可用 {self._cash:.2f}")

        # 扣除资金
        self._cash -= required_amount

        # 更新持仓
        position = self.get_position(code)
        position.buy(shares, price, date, commission)

        return {
            'code': code,
            'date': date,
            'direction': 'buy',
            'shares': shares,
            'price': price,
            'amount': price * shares,
            'commission': commission,
            'cash': self._cash
        }

    def sell(self, code: str, shares: int, price: float,
             date: datetime, commission: float = 0) -> Dict:
        """
        卖出股票

        Args:
            code: 股票代码
            shares: 卖出数量
            price: 卖出价格
            date: 卖出日期
            commission: 佣金

        Returns:
            交易信息字典
        """
        position = self.get_position(code)

        if position.is_empty():
            raise ValueError(f"没有持仓 {code}")

        if shares > position.shares:
            shares = position.shares

        # 卖出
        trade_info = position.sell(shares, price, date, commission)

        # 增加资金
        self._cash += trade_info['amount'] - commission

        return trade_info

    def close_position(self, code: str, price: float,
                       date: datetime, commission: float = 0) -> Dict:
        """
        平仓（卖出全部持仓）

        Args:
            code: 股票代码
            price: 卖出价格
            date: 卖出日期
            commission: 佣金

        Returns:
            交易信息字典
        """
        position = self.get_position(code)
        if position.is_empty():
            return None

        return self.sell(code, position.shares, price, date, commission)

    def close_all_positions(self, prices: Dict[str, float],
                            date: datetime,
                            commission_rate: float = 0.0003) -> List[Dict]:
        """
        平掉所有持仓

        Args:
            prices: 股票代码到价格的映射
            date: 平仓日期
            commission_rate: 佣金费率

        Returns:
            所有交易信息列表
        """
        trades = []
        for code in list(self.get_position_codes()):
            if code in prices:
                commission = max(prices[code] * self._positions[code].shares * commission_rate, 5)
                trade = self.close_position(code, prices[code], date, commission)
                if trade:
                    trades.append(trade)

        return trades

    def get_portfolio_value(self) -> Dict:
        """
        获取组合价值信息

        Returns:
            组合价值字典
        """
        position_value = 0.0
        profit_loss = 0.0

        for pos in self._positions.values():
            if not pos.is_empty():
                position_value += pos.get_value()
                profit_loss += pos.profit_loss

        return {
            'cash': self._cash,
            'position_value': position_value,
            'total_value': self._cash + position_value,
            'profit_loss': profit_loss,
            'profit_loss_pct': (profit_loss / self._initial_capital * 100)
                               if self._initial_capital > 0 else 0
        }

    def get_position_summary(self) -> pd.DataFrame:
        """
        获取持仓汇总DataFrame

        Returns:
            持仓汇总表
        """
        positions = []
        for code, pos in self.get_all_positions().items():
            positions.append(pos.to_dict())

        if not positions:
            return pd.DataFrame()

        df = pd.DataFrame(positions)
        df = df.sort_values('market_value', ascending=False)
        df = df.reset_index(drop=True)

        return df

    def clear(self):
        """清空所有持仓"""
        self._positions.clear()
        self._cash = self._initial_capital
