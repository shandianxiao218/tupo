# -*- coding: utf-8 -*-
"""
通达信股票回测系统 - 主程序入口
"""

import argparse
import sys
import os
from datetime import datetime

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import (
    TDX_DAY_DATA, TDX_SH_DAY_DATA,
    TDX_XRXD_DATA, TDX_SH_XRXD_DATA,
    INITIAL_CAPITAL, COMMISSION_RATE, SLIPPAGE,
    START_DATE, END_DATE, MAX_POSITIONS,
    STOP_LOSS_PCT, TAKE_PROFIT_PCT, OUTPUT_DIR
)
from data import DataLoader
from backtest import BacktestEngine
from strategy import MAConvergenceBreakoutStrategy, SimpleMAStrategy
from analysis import ReportGenerator, MetricsCalculator


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='通达信股票回测系统')

    # 数据路径
    parser.add_argument('--sz-data', type=str, default=TDX_DAY_DATA,
                       help='深圳日线数据目录')
    parser.add_argument('--sh-data', type=str, default=TDX_SH_DAY_DATA,
                       help='上海日线数据目录')
    parser.add_argument('--sz-xr', type=str, default=TDX_XRXD_DATA,
                       help='深圳除权除息数据目录')
    parser.add_argument('--sh-xr', type=str, default=TDX_SH_XRXD_DATA,
                       help='上海除权除息数据目录')

    # 回测参数
    parser.add_argument('--start-date', type=str, default=START_DATE,
                       help='回测开始日期 (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=END_DATE,
                       help='回测结束日期 (YYYY-MM-DD)')
    parser.add_argument('--capital', type=float, default=INITIAL_CAPITAL,
                       help='初始资金')
    parser.add_argument('--commission', type=float, default=COMMISSION_RATE,
                       help='佣金费率')
    parser.add_argument('--slippage', type=float, default=SLIPPAGE,
                       help='滑点率')
    parser.add_argument('--max-positions', type=int, default=MAX_POSITIONS,
                       help='最大持仓数量')

    # 策略参数
    parser.add_argument('--strategy', type=str, default='ma_convergence',
                       choices=['ma_convergence', 'simple_ma', 'buy_hold'],
                       help='策略类型')
    parser.add_argument('--lookback-days', type=int, default=60,
                       help='回看周期（用于均线聚拢策略）')
    parser.add_argument('--min-rise', type=float, default=0.20,
                       help='最小上涨幅度（用于均线聚拢策略）')
    parser.add_argument('--min-pullback', type=float, default=0.05,
                       help='最小回调幅度（用于均线聚拢策略）')
    parser.add_argument('--max-pullback', type=float, default=0.15,
                       help='最大回调幅度（用于均线聚拢策略）')
    parser.add_argument('--convergence', type=float, default=0.03,
                       help='均线粘合度阈值（用于均线聚拢策略）')
    parser.add_argument('--volume-ratio', type=float, default=1.5,
                       help='成交量放大倍数（用于均线聚拢策略）')
    parser.add_argument('--fast-ma', type=int, default=5,
                       help='快线周期（用于简单均线策略）')
    parser.add_argument('--slow-ma', type=int, default=20,
                       help='慢线周期（用于简单均线策略）')
    parser.add_argument('--stop-loss', type=float, default=STOP_LOSS_PCT,
                       help='止损比例')
    parser.add_argument('--take-profit', type=float, default=TAKE_PROFIT_PCT,
                       help='止盈比例')

    # 输出参数
    parser.add_argument('--output-dir', type=str, default=OUTPUT_DIR,
                       help='输出目录')
    parser.add_argument('--no-chart', action='store_true',
                       help='不生成图表')
    parser.add_argument('--no-report', action='store_true',
                       help='不生成文本报告')
    parser.add_argument('--export-excel', action='store_true',
                       help='导出Excel报告')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='显示详细输出')

    # 股票代码
    parser.add_argument('--stocks', type=str, nargs='+',
                       help='指定股票代码列表（如：000001 600000）')

    return parser.parse_args()


def create_strategy(args):
    """根据参数创建策略"""
    if args.strategy == 'ma_convergence':
        strategy = MAConvergenceBreakoutStrategy(
            lookback_days=args.lookback_days,
            min_rise_pct=args.min_rise,
            convergence_pct=args.convergence,
            volume_ratio=args.volume_ratio,
            stop_loss_pct=args.stop_loss,
            take_profit_pct=args.take_profit
        )
    elif args.strategy == 'simple_ma':
        strategy = SimpleMAStrategy(
            fast_period=args.fast_ma,
            slow_period=args.slow_ma
        )
    else:
        strategy = None

    return strategy


def print_intro(args):
    """打印程序介绍"""
    print("=" * 60)
    print("通达信股票回测系统")
    print("=" * 60)
    print(f"策略类型: {args.strategy}")
    print(f"回测期间: {args.start_date} 至 {args.end_date}")
    print(f"初始资金: {args.capital:,.0f} 元")
    print(f"最大持仓: {args.max_positions}")
    print("=" * 60)
    print()


def check_data_dirs(sz_data, sh_data):
    """检查数据目录是否存在"""
    if not os.path.exists(sz_data):
        print(f"警告: 深圳数据目录不存在: {sz_data}")

    if not os.path.exists(sh_data):
        print(f"警告: 上海数据目录不存在: {sh_data}")

    if not os.path.exists(sz_data) and not os.path.exists(sh_data):
        print("错误: 至少需要一个有效的数据目录")
        return False

    return True


def main():
    """主函数"""
    args = parse_args()

    # 打印介绍
    if args.verbose:
        print_intro(args)

    # 检查数据目录
    if not check_data_dirs(args.sz_data, args.sh_data):
        return 1

    # 创建内存数据管理器
    if args.verbose:
        print("初始化内存数据管理器...")

    from data import MemoryDataManager

    memory_manager = MemoryDataManager(
        sh_data_dir=args.sh_data,
        sz_data_dir=args.sz_data
    )

    # 加载股票数据到内存
    if args.verbose:
        print("加载股票数据到内存（进行前复权处理）...")

    # 确定要加载的股票
    if args.stocks:
        codes = [code.zfill(6) for code in args.stocks]
    else:
        # 获取所有股票代码
        all_codes = []
        if os.path.exists(args.sh_data):
            for f in os.listdir(args.sh_data):
                if f.endswith('.day'):
                    code = f.replace('sh', '').replace('.day', '')
                    if code.isdigit() and len(code) == 6:
                        all_codes.append(code)
        if os.path.exists(args.sz_data):
            for f in os.listdir(args.sz_data):
                if f.endswith('.day'):
                    code = f.replace('sz', '').replace('.day', '')
                    if code.isdigit() and len(code) == 6:
                        all_codes.append(code)
        codes = all_codes

    if args.verbose:
        print(f"准备加载 {len(codes)} 只股票的数据...")

    # 加载数据到内存
    loaded_count = memory_manager.load_all_data(codes)

    if args.verbose:
        print(f"成功加载 {loaded_count} 只股票的数据（已前复权）")

    if loaded_count == 0:
        print("错误: 没有加载到任何股票数据")
        return 1

    # 如果指定了股票，只使用指定的
    if args.stocks:
        codes = [code.zfill(6) for code in args.stocks]
        codes = [c for c in codes if c in memory_manager.get_all_codes()]
        if args.verbose:
            print(f"使用指定股票: {codes}")
    else:
        codes = memory_manager.get_all_codes()

    # 创建策略
    if args.verbose:
        print("创建策略...")

    strategy = create_strategy(args)
    if strategy is None:
        print("错误: 未知策略类型")
        return 1

    # 创建回测引擎
    if args.verbose:
        print("初始化回测引擎...")

    engine = BacktestEngine(
        initial_capital=args.capital,
        commission_rate=args.commission,
        slippage=args.slippage,
        max_positions=args.max_positions,
        stop_loss_pct=args.stop_loss,
        take_profit_pct=args.take_profit
    )

    # 设置目标股票列表
    if args.stocks:
        engine.target_stocks = [code.zfill(6) for code in args.stocks]

    # 运行回测
    if args.verbose:
        print("开始回测...")
        print()

    try:
        engine.run(strategy, memory_manager, args.start_date, args.end_date)
    except Exception as e:
        print(f"回测过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

    # 计算指标
    if args.verbose:
        print("计算绩效指标...")

    calculator = MetricsCalculator()
    metrics = calculator.calculate_all_metrics(
        engine.get_trades(),
        engine.get_equity_curve(),
        engine.get_daily_records(),
        engine.initial_capital
    )

    # 打印摘要
    print()
    ReportGenerator().print_summary(metrics)

    # 生成报告
    if not args.no_report or not args.no_chart:
        if args.verbose:
            print("生成报告...")

        report_gen = ReportGenerator(output_dir=args.output_dir)

        strategy_name = args.strategy

        if not args.no_report:
            # 文本报告
            report_file = os.path.join(args.output_dir,
                                      f"{strategy_name}_report.txt")
            report_gen.generate_text_report(
                metrics, engine.get_trades(), report_file
            )

        if not args.no_chart:
            # 图表
            chart_file = os.path.join(args.output_dir,
                                     f"{strategy_name}_chart.png")
            report_gen.generate_charts(
                engine.get_equity_curve(),
                engine.get_daily_records(),
                chart_file,
                f"{strategy_name} 回测结果"
            )

        # 导出数据
        trades_file = os.path.join(args.output_dir, f"{strategy_name}_trades.csv")
        report_gen.export_trades(engine.get_trades(), trades_file, engine, memory_manager)

        equity_file = os.path.join(args.output_dir, f"{strategy_name}_equity.csv")
        report_gen.export_equity(engine.get_equity_curve(), equity_file)

        # Excel报告
        if args.export_excel:
            excel_file = os.path.join(args.output_dir,
                                     f"{strategy_name}_report.xlsx")
            report_gen.export_to_excel(engine, metrics, excel_file, strategy_name)

    print("回测完成!")

    return 0


if __name__ == "__main__":
    sys.exit(main())
