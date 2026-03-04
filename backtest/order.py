# -*- coding: utf-8 -*-
"""
订单管理模块
"""

from typing import Dict, List, Optional
from datetime import datetime
from enum import Enum
import pandas as pd


class OrderStatus(Enum):
    """订单状态枚举"""
    PENDING = "pending"  # 待成交
    FILLED = "filled"    # 已成交
    PARTIAL = "partial"  # 部分成交
    CANCELLED = "cancelled"  # 已取消
    REJECTED = "rejected"    # 已拒绝


class OrderDirection(Enum):
    """订单方向枚举"""
    BUY = "buy"
    SELL = "sell"


class Order:
    """
    订单类

    表示单个交易订单
    """

    def __init__(self, code: str, direction: str, shares: int,
                 price: float = None, date: datetime = None,
                 order_id: str = None):
        """
        初始化订单

        Args:
            code: 股票代码
            direction: 买卖方向 (buy/sell)
            shares: 委托数量
            price: 委托价格（None表示市价单）
            date: 下单日期
            order_id: 订单ID
        """
        self.order_id = order_id or self._generate_order_id()
        self.code = code
        self.direction = direction.lower()
        self.shares = shares
        self.price = price
        self.date = date or datetime.now()

        self.status = OrderStatus.PENDING
        self.filled_shares = 0  # 已成交数量
        self.filled_price = 0.0  # 成交价格
        self.filled_amount = 0.0  # 成交金额
        self.commission = 0.0  # 佣金
        self.stamp_duty = 0.0  # 印花税

        self.create_time = datetime.now()
        self.update_time = datetime.now()
        self.fill_time = None

        self.message = ""  # 订单消息

    @staticmethod
    def _generate_order_id() -> str:
        """生成订单ID"""
        import time
        return f"{int(time.time() * 1000000)}"

    def fill(self, price: float, shares: int, commission: float = 0,
             stamp_duty: float = 0):
        """
        成交订单

        Args:
            price: 成交价格
            shares: 成交数量
            commission: 佣金
            stamp_duty: 印花税
        """
        self.filled_price = price
        self.filled_shares = shares
        self.filled_amount = price * shares
        self.commission = commission
        self.stamp_duty = stamp_duty
        self.fill_time = datetime.now()
        self.update_time = datetime.now()

        if self.filled_shares >= self.shares:
            self.status = OrderStatus.FILLED
        else:
            self.status = OrderStatus.PARTIAL

    def cancel(self):
        """取消订单"""
        self.status = OrderStatus.CANCELLED
        self.update_time = datetime.now()

    def reject(self, message: str = ""):
        """
        拒绝订单

        Args:
            message: 拒绝原因
        """
        self.status = OrderStatus.REJECTED
        self.message = message
        self.update_time = datetime.now()

    def is_filled(self) -> bool:
        """是否已完全成交"""
        return self.status == OrderStatus.FILLED

    def is_pending(self) -> bool:
        """是否待成交"""
        return self.status == OrderStatus.PENDING

    def is_cancelled(self) -> bool:
        """是否已取消"""
        return self.status == OrderStatus.CANCELLED

    def to_dict(self) -> Dict:
        """转换为字典"""
        return {
            'order_id': self.order_id,
            'code': self.code,
            'direction': self.direction,
            'shares': self.shares,
            'price': self.price,
            'date': self.date,
            'status': self.status.value,
            'filled_shares': self.filled_shares,
            'filled_price': self.filled_price,
            'filled_amount': self.filled_amount,
            'commission': self.commission,
            'stamp_duty': self.stamp_duty,
            'message': self.message
        }


class OrderManager:
    """
    订单管理器

    管理所有订单的创建、提交、成交等
    """

    def __init__(self, commission_rate: float = 0.0003,
                 min_commission: float = 5,
                 stamp_duty_rate: float = 0.001,
                 slippage: float = 0.001):
        """
        初始化订单管理器

        Args:
            commission_rate: 佣金费率
            min_commission: 最低佣金
            stamp_duty_rate: 印花税率（仅卖出）
            slippage: 滑点率
        """
        self.commission_rate = commission_rate
        self.min_commission = min_commission
        self.stamp_duty_rate = stamp_duty_rate
        self.slippage = slippage

        self._orders: Dict[str, Order] = {}
        self._order_count = 0

        # 待成交订单队列（按股票代码分组）
        self._pending_orders: Dict[str, List[Order]] = {}

    def _generate_order_id(self) -> str:
        """生成订单ID"""
        self._order_count += 1
        return f"ORDER_{self._order_count}_{int(datetime.now().timestamp())}"

    def create_order(self, code: str, direction: str, shares: int,
                     price: float = None, date: datetime = None) -> Order:
        """
        创建订单

        Args:
            code: 股票代码
            direction: 买卖方向 (buy/sell)
            shares: 委托数量
            price: 委托价格（None表示市价单）
            date: 下单日期

        Returns:
            Order对象
        """
        order = Order(
            code=code,
            direction=direction,
            shares=shares,
            price=price,
            date=date,
            order_id=self._generate_order_id()
        )

        self._orders[order.order_id] = order

        # 加入待成交队列
        if code not in self._pending_orders:
            self._pending_orders[code] = []
        self._pending_orders[code].append(order)

        return order

    def create_buy_order(self, code: str, shares: int,
                         price: float = None, date: datetime = None) -> Order:
        """创建买入订单"""
        return self.create_order(code, 'buy', shares, price, date)

    def create_sell_order(self, code: str, shares: int,
                          price: float = None, date: datetime = None) -> Order:
        """创建卖出订单"""
        return self.create_order(code, 'sell', shares, price, date)

    def execute_order(self, order: Order, execution_price: float,
                      execution_shares: int = None) -> Dict:
        """
        执行订单

        Args:
            order: 订单对象
            execution_price: 执行价格
            execution_shares: 执行数量（None表示全部）

        Returns:
            交易信息字典
        """
        if execution_shares is None:
            execution_shares = order.shares

        # 应用滑点
        actual_price = self._apply_slippage(execution_price, order.direction)

        # 计算佣金
        commission = self._calculate_commission(actual_price, execution_shares, order.direction)

        # 计算印花税（仅卖出收取）
        stamp_duty = 0
        if order.direction == 'sell':
            stamp_duty = self._calculate_stamp_duty(actual_price, execution_shares)

        # 成交订单
        order.fill(actual_price, execution_shares, commission, stamp_duty)

        # 从待成交队列移除
        self._remove_from_pending(order)

        # 构建交易信息
        trade_info = {
            'order_id': order.order_id,
            'code': order.code,
            'date': order.date,
            'direction': order.direction,
            'shares': execution_shares,
            'price': actual_price,
            'amount': actual_price * execution_shares,
            'commission': commission,
            'stamp_duty': stamp_duty,
            'total_cost': actual_price * execution_shares + commission + stamp_duty
        }

        return trade_info

    def _apply_slippage(self, price: float, direction: str) -> float:
        """
        应用滑点

        Args:
            price: 原始价格
            direction: 买卖方向

        Returns:
            应用滑点后的价格
        """
        if direction == 'buy':
            # 买入时滑点不利
            return price * (1 + self.slippage)
        else:
            # 卖出时滑点不利
            return price * (1 - self.slippage)

    def _calculate_commission(self, price: float, shares: int,
                              direction: str) -> float:
        """
        计算佣金

        Args:
            price: 成交价格
            shares: 成交数量
            direction: 买卖方向

        Returns:
            佣金金额
        """
        amount = price * shares
        commission = amount * self.commission_rate
        return max(commission, self.min_commission)

    def _calculate_stamp_duty(self, price: float, shares: int) -> float:
        """
        计算印花税（仅卖出）

        Args:
            price: 成交价格
            shares: 成交数量

        Returns:
            印花税金额
        """
        return price * shares * self.stamp_duty_rate

    def _remove_from_pending(self, order: Order):
        """从待成交队列移除订单"""
        if order.code in self._pending_orders:
            self._pending_orders[order.code] = [
                o for o in self._pending_orders[order.code]
                if o.order_id != order.order_id
            ]

    def get_order(self, order_id: str) -> Optional[Order]:
        """获取订单"""
        return self._orders.get(order_id)

    def get_pending_orders(self, code: str = None) -> List[Order]:
        """
        获取待成交订单

        Args:
            code: 股票代码（None表示获取所有）

        Returns:
            待成交订单列表
        """
        if code:
            return self._pending_orders.get(code, [])

        all_pending = []
        for orders in self._pending_orders.values():
            all_pending.extend(orders)
        return all_pending

    def cancel_order(self, order_id: str) -> bool:
        """
        取消订单

        Args:
            order_id: 订单ID

        Returns:
            是否成功取消
        """
        order = self.get_order(order_id)
        if order and order.is_pending():
            order.cancel()
            self._remove_from_pending(order)
            return True
        return False

    def cancel_all_pending_orders(self, code: str = None):
        """
        取消所有待成交订单

        Args:
            code: 股票代码（None表示取消所有）
        """
        pending_orders = self.get_pending_orders(code)
        for order in pending_orders:
            order.cancel()

        if code:
            self._pending_orders[code] = []
        else:
            self._pending_orders.clear()

    def get_all_orders(self) -> List[Order]:
        """获取所有订单"""
        return list(self._orders.values())

    def get_order_history(self) -> pd.DataFrame:
        """
        获取订单历史DataFrame

        Returns:
            订单历史表
        """
        orders = []
        for order in self._orders.values():
            orders.append(order.to_dict())

        if not orders:
            return pd.DataFrame()

        df = pd.DataFrame(orders)
        df = df.sort_values('date').reset_index(drop=True)

        return df

    def calculate_buy_shares(self, cash: float, price: float,
                             max_cash_pct: float = 1.0) -> int:
        """
        计算可买入股数（考虑手续费）

        Args:
            cash: 可用资金
            price: 股票价格
            max_cash_pct: 最大使用资金比例

        Returns:
            可买入股数（整手）
        """
        available_cash = cash * max_cash_pct

        # 考虑佣金和滑点
        estimated_cost_per_share = price * (1 + self.slippage)
        estimated_commission = estimated_cost_per_share * self.commission_rate

        # 计算股数（向下取整到100的倍数）
        shares = int((available_cash - self.min_commission) /
                     (estimated_cost_per_share + estimated_commission))
        shares = (shares // 100) * 100  # 整手

        return max(shares, 0)

    def calculate_equal_weight_shares(self, cash: float, prices: List[float],
                                      target_positions: int = None) -> List[int]:
        """
        计算等权重买入时的股数分配

        Args:
            cash: 可用资金
            prices: 各股票价格列表
            target_positions: 目标持仓数量

        Returns:
            各股票买入股数列表
        """
        if not prices:
            return []

        if target_positions is None:
            target_positions = len(prices)

        # 每只股票分配的资金
        cash_per_stock = cash / min(target_positions, len(prices))

        shares_list = []
        for price in prices:
            shares = self.calculate_buy_shares(cash_per_stock, price, max_cash_pct=1.0)
            shares_list.append(shares)

        return shares_list

    def clear(self):
        """清空所有订单"""
        self._orders.clear()
        self._pending_orders.clear()
        self._order_count = 0
