# -*- coding: utf-8 -*-
"""
绩效指标计算模块
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from datetime import datetime


class MetricsCalculator:
    """
    绩效指标计算器

    计算各种回测绩效指标
    """

    def __init__(self):
        """初始化指标计算器"""
        self.metrics = {}

    def calculate_all_metrics(self, trades_df: pd.DataFrame,
                             equity_df: pd.DataFrame,
                             daily_records_df: pd.DataFrame,
                             initial_capital: float) -> Dict:
        """
        计算所有绩效指标

        Args:
            trades_df: 交易记录DataFrame
            equity_df: 净值曲线DataFrame
            daily_records_df: 每日记录DataFrame
            initial_capital: 初始资金

        Returns:
            指标字典
        """
        metrics = {}

        # 收益指标
        metrics.update(self._calculate_return_metrics(equity_df, initial_capital))

        # 风险指标
        metrics.update(self._calculate_risk_metrics(equity_df, daily_records_df))

        # 交易指标
        metrics.update(self._calculate_trade_metrics(trades_df))

        # 持仓指标
        if daily_records_df is not None and len(daily_records_df) > 0:
            metrics['avg_positions'] = daily_records_df['position_count'].mean()
            metrics['max_positions'] = daily_records_df['position_count'].max()

        self.metrics = metrics
        return metrics

    def _calculate_return_metrics(self, equity_df: pd.DataFrame,
                                  initial_capital: float) -> Dict:
        """计算收益指标"""
        if equity_df is None or len(equity_df) == 0:
            return {
                'total_return': 0,
                'annual_return': 0,
                'final_value': initial_capital
            }

        final_value = equity_df['equity'].iloc[-1]
        total_return = (final_value / initial_capital - 1) * 100

        # 计算年化收益率
        if len(equity_df) > 1:
            start_date = equity_df['date'].iloc[0]
            end_date = equity_df['date'].iloc[-1]
            days = (end_date - start_date).days

            if days > 0:
                years = days / 365.25
                annual_return = ((final_value / initial_capital) ** (1 / years) - 1) * 100
            else:
                annual_return = 0
        else:
            annual_return = 0

        return {
            'total_return': total_return,
            'annual_return': annual_return,
            'final_value': final_value,
            'initial_capital': initial_capital
        }

    def _calculate_risk_metrics(self, equity_df: pd.DataFrame,
                               daily_records_df: pd.DataFrame) -> Dict:
        """计算风险指标"""
        if equity_df is None or len(equity_df) < 2:
            return {
                'max_drawdown': 0,
                'sharpe_ratio': 0,
                'volatility': 0
            }

        # 最大回撤
        max_drawdown = self._calculate_max_drawdown(equity_df['equity'])

        # 波动率
        returns = self._calculate_returns(equity_df['equity'])
        volatility = returns.std() * np.sqrt(252) * 100 if len(returns) > 0 else 0

        # 夏普比率（假设无风险利率为3%）
        risk_free_rate = 0.03
        annual_return = (equity_df['equity'].iloc[-1] / equity_df['equity'].iloc[0] - 1)
        sharpe_ratio = ((annual_return - risk_free_rate) / (volatility / 100)
                       if volatility > 0 else 0)

        return {
            'max_drawdown': max_drawdown,
            'sharpe_ratio': sharpe_ratio,
            'volatility': volatility
        }

    def _calculate_trade_metrics(self, trades_df: pd.DataFrame) -> Dict:
        """计算交易指标"""
        if trades_df is None or len(trades_df) == 0:
            return {
                'total_trades': 0,
                'win_rate': 0,
                'profit_loss_ratio': 0,
                'avg_hold_days': 0
            }

        # 筛选卖出交易
        sell_trades = trades_df[trades_df['direction'] == 'sell']

        if len(sell_trades) == 0:
            return {
                'total_trades': len(trades_df),
                'win_rate': 0,
                'profit_loss_ratio': 0,
                'avg_hold_days': 0
            }

        # 胜率
        win_trades = sell_trades[sell_trades['profit_loss'] > 0]
        win_rate = len(win_trades) / len(sell_trades) * 100

        # 盈亏比
        avg_profit = win_trades['profit_loss'].mean() if len(win_trades) > 0 else 0
        lose_trades = sell_trades[sell_trades['profit_loss'] <= 0]
        avg_loss = lose_trades['profit_loss'].mean() if len(lose_trades) > 0 else 0

        profit_loss_ratio = abs(avg_profit / avg_loss) if avg_loss != 0 else 0

        # 平均持仓天数
        if 'hold_days' in sell_trades.columns:
            avg_hold_days = sell_trades['hold_days'].mean()
        else:
            avg_hold_days = 0

        # 总盈利和总亏损
        total_profit = win_trades['profit_loss'].sum() if len(win_trades) > 0 else 0
        total_loss = lose_trades['profit_loss'].sum() if len(lose_trades) > 0 else 0

        # 盈利因子
        profit_factor = abs(total_profit / total_loss) if total_loss != 0 else 0

        return {
            'total_trades': len(sell_trades),
            'win_trades': len(win_trades),
            'lose_trades': len(lose_trades),
            'win_rate': win_rate,
            'profit_loss_ratio': profit_loss_ratio,
            'avg_hold_days': avg_hold_days,
            'total_profit': total_profit,
            'total_loss': total_loss,
            'profit_factor': profit_factor
        }

    def _calculate_max_drawdown(self, equity: pd.Series) -> float:
        """
        计算最大回撤

        Args:
            equity: 净值序列

        Returns:
            最大回撤（百分比）
        """
        if len(equity) == 0:
            return 0

        cumulative_max = equity.cummax()
        drawdown = (equity - cumulative_max) / cumulative_max
        max_drawdown = drawdown.min() * 100

        return max_drawdown

    def _calculate_returns(self, equity: pd.Series) -> pd.Series:
        """
        计算收益率序列

        Args:
            equity: 净值序列

        Returns:
            收益率序列
        """
        return equity.pct_change().dropna()

    def calculate_monthly_returns(self, equity_df: pd.DataFrame) -> pd.DataFrame:
        """
        计算月度收益

        Args:
            equity_df: 净值曲线DataFrame

        Returns:
            月度收益DataFrame
        """
        if equity_df is None or len(equity_df) == 0:
            return pd.DataFrame()

        df = equity_df.copy()
        df['year'] = pd.to_datetime(df['date']).dt.year
        df['month'] = pd.to_datetime(df['date']).dt.month

        # 获取每月的最后一个净值
        monthly_equity = df.groupby(['year', 'month'])['equity'].last().reset_index()

        # 计算月度收益
        monthly_equity['monthly_return'] = monthly_equity['equity'].pct_change() * 100

        # 创建透视表
        pivot = monthly_equity.pivot(index='year', columns='month', values='monthly_return')

        return pivot

    def calculate_rolling_returns(self, equity_df: pd.DataFrame,
                                  window: int = 20) -> pd.Series:
        """
        计算滚动收益率

        Args:
            equity_df: 净值曲线DataFrame
            window: 滚动窗口

        Returns:
            滚动收益率序列
        """
        if equity_df is None or len(equity_df) < window:
            return pd.Series()

        equity = equity_df['equity']
        rolling_returns = equity.pct_change(window).dropna() * 100

        return rolling_returns

    def get_drawdown_periods(self, equity_df: pd.DataFrame,
                            threshold: float = 0.05) -> List[Dict]:
        """
        获取回撤区间

        Args:
            equity_df: 净值曲线DataFrame
            threshold: 回撤阈值（如0.05表示5%）

        Returns:
            回撤区间列表
        """
        if equity_df is None or len(equity_df) == 0:
            return []

        equity = equity_df['equity'].values
        dates = equity_df['date'].values

        cumulative_max = np.maximum.accumulate(equity)
        drawdown = (equity - cumulative_max) / cumulative_max

        periods = []
        in_drawdown = False
        start_idx = 0

        for i, dd in enumerate(drawdown):
            if dd <= -threshold and not in_drawdown:
                in_drawdown = True
                start_idx = i
            elif dd > -threshold and in_drawdown:
                in_drawdown = False
                periods.append({
                    'start_date': dates[start_idx],
                    'end_date': dates[i],
                    'duration_days': (dates[i] - dates[start_idx]).astype('timedelta64[D]').astype(int),
                    'max_drawdown': drawdown[start_idx:i+1].min() * 100
                })

        return periods

    def format_metrics(self, metrics: Dict) -> str:
        """
        格式化指标为可读字符串

        Args:
            metrics: 指标字典

        Returns:
            格式化字符串
        """
        lines = []
        lines.append("=" * 50)
        lines.append("回测绩效指标")
        lines.append("=" * 50)

        # 收益指标
        lines.append("\n【收益指标】")
        lines.append(f"初始资金: {metrics.get('initial_capital', 0):,.2f} 元")
        lines.append(f"期末净值: {metrics.get('final_value', 0):,.2f} 元")
        lines.append(f"总收益率: {metrics.get('total_return', 0):.2f}%")
        lines.append(f"年化收益率: {metrics.get('annual_return', 0):.2f}%")

        # 风险指标
        lines.append("\n【风险指标】")
        lines.append(f"最大回撤: {metrics.get('max_drawdown', 0):.2f}%")
        lines.append(f"年化波动率: {metrics.get('volatility', 0):.2f}%")
        lines.append(f"夏普比率: {metrics.get('sharpe_ratio', 0):.2f}")

        # 交易指标
        lines.append("\n【交易指标】")
        lines.append(f"总交易次数: {metrics.get('total_trades', 0)}")
        lines.append(f"盈利次数: {metrics.get('win_trades', 0)}")
        lines.append(f"亏损次数: {metrics.get('lose_trades', 0)}")
        lines.append(f"胜率: {metrics.get('win_rate', 0):.2f}%")
        lines.append(f"盈亏比: {metrics.get('profit_loss_ratio', 0):.2f}")
        lines.append(f"盈利因子: {metrics.get('profit_factor', 0):.2f}")
        lines.append(f"平均持仓天数: {metrics.get('avg_hold_days', 0):.1f}")

        # 持仓指标
        if 'avg_positions' in metrics:
            lines.append("\n【持仓指标】")
            lines.append(f"平均持仓数: {metrics.get('avg_positions', 0):.1f}")
            lines.append(f"最大持仓数: {metrics.get('max_positions', 0)}")

        lines.append("=" * 50)

        return "\n".join(lines)

    def get_metrics_summary(self, metrics: Dict) -> pd.DataFrame:
        """
        获取指标摘要DataFrame

        Args:
            metrics: 指标字典

        Returns:
            指标摘要DataFrame
        """
        data = {
            '指标': [
                '初始资金',
                '期末净值',
                '总收益率',
                '年化收益率',
                '最大回撤',
                '夏普比率',
                '总交易次数',
                '胜率',
                '盈亏比',
                '盈利因子'
            ],
            '数值': [
                metrics.get('initial_capital', 0),
                metrics.get('final_value', 0),
                metrics.get('total_return', 0),
                metrics.get('annual_return', 0),
                metrics.get('max_drawdown', 0),
                metrics.get('sharpe_ratio', 0),
                metrics.get('total_trades', 0),
                metrics.get('win_rate', 0),
                metrics.get('profit_loss_ratio', 0),
                metrics.get('profit_factor', 0)
            ]
        }

        df = pd.DataFrame(data)
        return df
