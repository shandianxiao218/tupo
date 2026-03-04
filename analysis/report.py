# -*- coding: utf-8 -*-
"""
报告生成模块
"""

import os
from typing import Dict, Optional
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime
from .metrics import MetricsCalculator


class ReportGenerator:
    """
    报告生成器

    生成回测报告，包括文本报告、图表和数据导出
    """

    def __init__(self, output_dir: str = "results"):
        """
        初始化报告生成器

        Args:
            output_dir: 输出目录
        """
        self.output_dir = output_dir
        self.calculator = MetricsCalculator()

        # 创建输出目录
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

    def generate_full_report(self, engine, strategy_name: str = "Strategy") -> Dict:
        """
        生成完整报告

        Args:
            engine: 回测引擎
            strategy_name: 策略名称

        Returns:
            报告信息字典
        """
        # 获取数据
        trades_df = engine.get_trades()
        equity_df = engine.get_equity_curve()
        daily_records_df = engine.get_daily_records()

        # 计算指标
        metrics = self.calculator.calculate_all_metrics(
            trades_df, equity_df, daily_records_df, engine.initial_capital
        )

        # 生成时间戳
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = f"{strategy_name}_{timestamp}"

        report_info = {}

        # 生成文本报告
        report_file = os.path.join(self.output_dir, f"{prefix}_report.txt")
        self.generate_text_report(metrics, trades_df, report_file)
        report_info['text_report'] = report_file

        # 生成图表
        chart_file = os.path.join(self.output_dir, f"{prefix}_chart.png")
        self.generate_charts(equity_df, daily_records_df, chart_file, strategy_name)
        report_info['chart'] = chart_file

        # 导出交易记录
        trades_file = os.path.join(self.output_dir, f"{prefix}_trades.csv")
        # 注意: generate_full_report 没有engine参数，所以不传engine和memory_manager
        self.export_trades(trades_df, trades_file)
        report_info['trades'] = trades_file

        # 导出净值曲线
        equity_file = os.path.join(self.output_dir, f"{prefix}_equity.csv")
        self.export_equity(equity_df, equity_file)
        report_info['equity'] = equity_file

        # 导出月度收益
        monthly_file = os.path.join(self.output_dir, f"{prefix}_monthly.csv")
        self.export_monthly_returns(equity_df, monthly_file)
        report_info['monthly'] = monthly_file

        return report_info

    def generate_text_report(self, metrics: Dict, trades_df: pd.DataFrame,
                            output_file: str = None):
        """
        生成文本报告

        Args:
            metrics: 指标字典
            trades_df: 交易记录DataFrame
            output_file: 输出文件路径
        """
        lines = []

        # 标题
        lines.append("=" * 60)
        lines.append("通达信股票回测系统 - 回测报告")
        lines.append("=" * 60)
        lines.append(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")

        # 绩效指标
        lines.append(self.calculator.format_metrics(metrics))

        # 交易记录摘要
        if trades_df is not None and len(trades_df) > 0:
            lines.append("\n【交易记录摘要】")

            sell_trades = trades_df[trades_df['direction'] == 'sell']
            if len(sell_trades) > 0:
                lines.append(f"\n最大盈利: {sell_trades['profit_loss'].max():,.2f} 元")
                lines.append(f"最大亏损: {sell_trades['profit_loss'].min():,.2f} 元")

                # 最好和最差的交易
                best_trade = sell_trades.loc[sell_trades['profit_loss'].idxmax()]
                worst_trade = sell_trades.loc[sell_trades['profit_loss'].idxmin()]

                lines.append(f"\n最佳交易: {best_trade['code']} "
                           f"({best_trade['profit_loss_pct']:.2f}%)")
                lines.append(f"最差交易: {worst_trade['code']} "
                           f"({worst_trade['profit_loss_pct']:.2f}%)")

        lines.append("\n" + "=" * 60)

        report_text = "\n".join(lines)

        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(report_text)

        return report_text

    def generate_charts(self, equity_df: pd.DataFrame,
                       daily_records_df: pd.DataFrame,
                       output_file: str = None,
                       title: str = "回测结果"):
        """
        生成图表

        Args:
            equity_df: 净值曲线DataFrame
            daily_records_df: 每日记录DataFrame
            output_file: 输出文件路径
            title: 图表标题
        """
        if equity_df is None or len(equity_df) == 0:
            print("没有净值数据，无法生成图表")
            return

        # 创建图表
        fig, axes = plt.subplots(3, 1, figsize=(12, 10))

        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
        plt.rcParams['axes.unicode_minus'] = False

        dates = pd.to_datetime(equity_df['date'])

        # 1. 净值曲线
        ax1 = axes[0]
        ax1.plot(dates, equity_df['equity'], 'b-', linewidth=2, label='净值')
        ax1.axhline(y=equity_df['equity'].iloc[0], color='r', linestyle='--',
                   label=f"初始资金: {equity_df['equity'].iloc[0]:,.0f}")
        ax1.set_ylabel('净值 (元)', fontsize=12)
        ax1.set_title(f'{title} - 净值曲线', fontsize=14, fontweight='bold')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)
        ax1.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x/10000:.1f}万'))

        # 2. 回撤曲线
        ax2 = axes[1]
        equity = equity_df['equity'].values
        cumulative_max = pd.Series(equity).cummax().values
        drawdown = (equity - cumulative_max) / cumulative_max * 100

        ax2.fill_between(dates, drawdown, 0, color='red', alpha=0.3)
        ax2.plot(dates, drawdown, 'r-', linewidth=1)
        ax2.set_ylabel('回撤 (%)', fontsize=12)
        ax2.set_title('回撤曲线', fontsize=14, fontweight='bold')
        ax2.grid(True, alpha=0.3)

        # 3. 收益率分布
        ax3 = axes[2]
        if len(equity) > 1:
            returns = pd.Series(equity).pct_change().dropna() * 100
            ax3.hist(returns, bins=50, color='steelblue', alpha=0.7, edgecolor='black')
            ax3.axvline(x=returns.mean(), color='r', linestyle='--',
                       label=f"均值: {returns.mean():.2f}%")
            ax3.set_xlabel('日收益率 (%)', fontsize=12)
            ax3.set_ylabel('频数', fontsize=12)
            ax3.set_title('收益率分布', fontsize=14, fontweight='bold')
            ax3.legend()
            ax3.grid(True, alpha=0.3, axis='y')

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=150, bbox_inches='tight')
            print(f"图表已保存到: {output_file}")

        plt.close()

    def generate_monthly_heatmap(self, equity_df: pd.DataFrame,
                                output_file: str = None):
        """
        生成月度收益热力图

        Args:
            equity_df: 净值曲线DataFrame
            output_file: 输出文件路径
        """
        monthly_returns = self.calculator.calculate_monthly_returns(equity_df)

        if monthly_returns.empty:
            print("没有足够的月度数据")
            return

        # 创建图表
        fig, ax = plt.subplots(figsize=(12, 8))

        # 设置中文字体
        plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
        plt.rcParams['axes.unicode_minus'] = False

        # 绘制热力图
        im = ax.imshow(monthly_returns.values, cmap='RdYlGn', aspect='auto',
                      vmin=-10, vmax=10)

        # 设置坐标轴
        ax.set_xticks(range(12))
        ax.set_xticklabels(['1月', '2月', '3月', '4月', '5月', '6月',
                           '7月', '8月', '9月', '10月', '11月', '12月'])
        ax.set_yticks(range(len(monthly_returns.index)))
        ax.set_yticklabels(monthly_returns.index)

        # 添加数值标签
        for i in range(len(monthly_returns.index)):
            for j in range(12):
                value = monthly_returns.values[i, j]
                if not pd.isna(value):
                    text_color = 'white' if abs(value) > 5 else 'black'
                    ax.text(j, i, f'{value:.1f}%', ha='center', va='center',
                           color=text_color, fontsize=9)

        ax.set_title('月度收益率热力图 (%)', fontsize=14, fontweight='bold')
        ax.set_xlabel('月份', fontsize=12)
        ax.set_ylabel('年份', fontsize=12)

        # 添加颜色条
        cbar = plt.colorbar(im, ax=ax)
        cbar.set_label('收益率 (%)', fontsize=12)

        plt.tight_layout()

        if output_file:
            plt.savefig(output_file, dpi=150, bbox_inches='tight')
            print(f"月度热力图已保存到: {output_file}")

        plt.close()

    def export_trades(self, trades_df: pd.DataFrame, output_file: str,
                     engine=None, memory_manager=None):
        """
        导出交易记录到CSV

        Args:
            trades_df: 交易记录DataFrame
            output_file: 输出文件路径
            engine: 回测引擎（用于获取持仓信息）
            memory_manager: 内存数据管理器（用于获取最新价格）
        """
        if trades_df is None or len(trades_df) == 0:
            print("没有交易记录")
            return

        # 复制交易记录
        export_df = trades_df.copy()

        # 如果提供了engine和memory_manager，添加持仓浮动盈亏信息
        if engine is not None and memory_manager is not None:
            position_codes = engine.get_position_codes()

            if position_codes:
                # 获取股票的最新日期
                latest_date = engine.current_date

                for code in position_codes:
                    position = engine.position_manager.get_position(code)

                    # 获取最新价格
                    stock_data = memory_manager.get_stock_data(code)
                    if stock_data is not None and len(stock_data) > 0:
                        # 获取最新的收盘价
                        latest_price = stock_data.iloc[-1]['close']
                        latest_date_str = stock_data.index[-1].strftime('%Y-%m-%d')

                        # 计算浮动盈亏
                        cost_amount = position.avg_cost * position.shares
                        market_value = latest_price * position.shares
                        float_profit = market_value - cost_amount
                        float_profit_pct = (float_profit / cost_amount * 100) if cost_amount > 0 else 0

                        # 计算持仓天数
                        if position.buy_date:
                            hold_days = (latest_date - position.buy_date).days
                        else:
                            hold_days = 0

                        # 查找对应的买入记录以获取买入信息
                        buy_trades = trades_df[(trades_df['code'] == code) &
                                              (trades_df['direction'] == 'buy')]
                        if len(buy_trades) > 0:
                            last_buy = buy_trades.iloc[-1]
                            buy_reason = last_buy.get('reason', '')
                        else:
                            buy_reason = ''

                        # 创建持仓中记录
                        holding_record = {
                            'code': code,
                            'date': latest_date,
                            'direction': 'holding',
                            'shares': position.shares,
                            'price': position.avg_cost,
                            'amount': cost_amount,
                            'commission': 0,
                            'cash': '',
                            'reason': f'{buy_reason} [最新价:{latest_price:.2f}]',
                            'cost': cost_amount,
                            'profit_loss': float_profit,
                            'profit_loss_pct': float_profit_pct,
                            'hold_days': hold_days,
                            'stamp_duty': 0
                        }

                        # 添加到导出数据
                        export_df = pd.concat([
                            export_df,
                            pd.DataFrame([holding_record])
                        ], ignore_index=True)

        export_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"交易记录已导出到: {output_file}")

    def export_equity(self, equity_df: pd.DataFrame, output_file: str):
        """
        导出净值曲线到CSV

        Args:
            equity_df: 净值曲线DataFrame
            output_file: 输出文件路径
        """
        if equity_df is None or len(equity_df) == 0:
            print("没有净值数据")
            return

        equity_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f"净值曲线已导出到: {output_file}")

    def export_monthly_returns(self, equity_df: pd.DataFrame, output_file: str):
        """
        导出月度收益到CSV

        Args:
            equity_df: 净值曲线DataFrame
            output_file: 输出文件路径
        """
        monthly_returns = self.calculator.calculate_monthly_returns(equity_df)

        if monthly_returns.empty:
            print("没有月度收益数据")
            return

        monthly_returns.to_csv(output_file, encoding='utf-8-sig')
        print(f"月度收益已导出到: {output_file}")

    def export_to_excel(self, engine, metrics: Dict,
                       output_file: str = None,
                       strategy_name: str = "Strategy"):
        """
        导出完整报告到Excel

        Args:
            engine: 回测引擎
            metrics: 指标字典
            output_file: 输出文件路径
            strategy_name: 策略名称
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.output_dir,
                                      f"{strategy_name}_{timestamp}_report.xlsx")

        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            # 指标摘要
            metrics_df = self.calculator.get_metrics_summary(metrics)
            metrics_df.to_excel(writer, sheet_name='绩效指标', index=False)

            # 交易记录
            trades_df = engine.get_trades()
            if not trades_df.empty:
                trades_df.to_excel(writer, sheet_name='交易记录', index=False)

            # 净值曲线
            equity_df = engine.get_equity_curve()
            if not equity_df.empty:
                equity_df.to_excel(writer, sheet_name='净值曲线', index=False)

            # 月度收益
            monthly_returns = self.calculator.calculate_monthly_returns(equity_df)
            if not monthly_returns.empty:
                monthly_returns.to_excel(writer, sheet_name='月度收益')

        print(f"Excel报告已导出到: {output_file}")

    def print_summary(self, metrics: Dict):
        """
        打印绩效摘要到控制台

        Args:
            metrics: 指标字典
        """
        print("\n" + "=" * 50)
        print("回测完成 - 绩效摘要")
        print("=" * 50)

        print(f"\n【收益】")
        print(f"  总收益率: {metrics.get('total_return', 0):.2f}%")
        print(f"  年化收益率: {metrics.get('annual_return', 0):.2f}%")

        print(f"\n【风险】")
        print(f"  最大回撤: {metrics.get('max_drawdown', 0):.2f}%")
        print(f"  夏普比率: {metrics.get('sharpe_ratio', 0):.2f}")

        print(f"\n【交易】")
        print(f"  总交易次数: {metrics.get('total_trades', 0)}")
        print(f"  胜率: {metrics.get('win_rate', 0):.2f}%")
        print(f"  盈亏比: {metrics.get('profit_loss_ratio', 0):.2f}")

        print("=" * 50 + "\n")
