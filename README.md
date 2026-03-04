# 通达信股票回测系统

基于 Python 的股票回测系统，使用通达信软件的数据格式进行回测。支持选股策略、买入/卖出操作、持仓管理，并能计算风险收益比、胜率等关键指标。

## 功能特性

- **数据读取**: 支持通达信日线数据（.day文件）和除权除息数据（.qfz文件）
- **前复权处理**: 自动根据除权除息数据进行前复权计算
- **策略开发**: 提供策略基类，支持自定义选股和交易信号
- **回测引擎**: 完整的订单管理、持仓管理和交易执行逻辑
- **绩效分析**: 计算总收益率、最大回撤、夏普比率、胜率等指标
- **报告生成**: 生成文本报告、可视化图表和数据导出

## 项目结构

```
stock-backtest/
├── config.py              # 配置文件（数据路径、手续费等）
├── data/                  # 数据模块
│   ├── __init__.py
│   ├── tdx_reader.py      # 通达信数据读取器
│   └── data_loader.py     # 数据加载和管理
├── strategy/              # 策略模块
│   ├── __init__.py
│   ├── base.py            # 策略基类
│   ├── selector.py        # 选股策略
│   └── signals.py         # 买卖信号生成
├── backtest/              # 回测引擎
│   ├── __init__.py
│   ├── engine.py          # 回测引擎核心
│   ├── order.py           # 订单管理
│   └── position.py        # 持仓管理
├── analysis/              # 分析模块
│   ├── __init__.py
│   ├── metrics.py         # 绩效指标计算
│   └── report.py          # 报告生成
├── main.py                # 主程序入口
├── requirements.txt       # 依赖库
└── README.md             # 本文件
```

## 安装依赖

```bash
pip install -r requirements.txt
```

## 配置说明

编辑 `config.py` 文件，设置以下参数：

```python
# 通达信数据路径
TDX_DIR = r"C:\new_tdx64"  # 修改为你的通达信安装路径

# 交易配置
COMMISSION_RATE = 0.0003  # 万三佣金
SLIPPAGE = 0.001          # 滑点 0.1%
INITIAL_CAPITAL = 1000000 # 初始资金100万

# 回测配置
START_DATE = "2020-01-01"
END_DATE = "2025-12-31"
```

## 使用方法

### 命令行使用

```bash
# 使用默认参数运行回测
python main.py

# 指定回测期间和初始资金
python main.py --start-date 2020-01-01 --end-date 2023-12-31 --capital 1000000

# 使用简单均线策略
python main.py --strategy simple_ma --fast-ma 5 --slow-ma 20

# 指定股票代码
python main.py --stocks 000001 600000 000002

# 查看详细输出
python main.py -v

# 导出Excel报告
python main.py --export-excel
```

### 代码中使用

```python
from data import DataLoader
from backtest import BacktestEngine
from strategy import MAConvergenceBreakoutStrategy
from analysis import ReportGenerator

# 创建数据加载器
data_loader = DataLoader(
    sz_data_dir="path/to/sz/data",
    sh_data_dir="path/to/sh/data",
    sz_xr_dir="path/to/sz/xr",
    sh_xr_dir="path/to/sh/xr"
)

# 创建策略
strategy = MAConvergenceBreakoutStrategy(
    lookback_days=60,
    min_rise_pct=0.20,
    convergence_pct=0.05
)

# 创建回测引擎
engine = BacktestEngine(
    initial_capital=1000000,
    commission_rate=0.0003,
    max_positions=5
)

# 运行回测
engine.run(strategy, data_loader, "2020-01-01", "2023-12-31")

# 生成报告
report_gen = ReportGenerator()
report_gen.generate_full_report(engine, "MyStrategy")
```

## 策略说明

### 均线聚拢突破策略

默认策略，逻辑如下：

1. **先上涨一波**: 过去60日内存在明显的上涨阶段（涨幅超过20%）
2. **然后回调**: 从高点回调5%-15%，形成整理形态
3. **多条均线聚拢**: MA5、MA10、MA20、MA60四条均线粘合（标准差/均值 < 5%）
4. **股价突破**: 当前股价突破所有均线，且成交量放大1.5倍以上

### 简单均线策略

基础均线交叉策略：

- **买入**: MA5上穿MA20（金叉）
- **卖出**: MA5下穿MA20（死叉）

## 绩效指标

系统计算以下关键指标：

- **收益指标**: 总收益率、年化收益率
- **风险指标**: 最大回撤、夏普比率、年化波动率
- **交易指标**: 总交易次数、胜率、盈亏比、盈利因子
- **持仓指标**: 平均持仓数、最大持仓数

## 输出文件

回测完成后会在 `results/` 目录生成：

- `{strategy}_report.txt` - 文本报告
- `{strategy}_chart.png` - 净值曲线和回撤图表
- `{strategy}_trades.csv` - 交易记录
- `{strategy}_equity.csv` - 净值曲线数据
- `{strategy}_monthly.csv` - 月度收益统计

## 开发说明

### 自定义策略

继承 `BaseStrategy` 类：

```python
from strategy import BaseStrategy
import pandas as pd

class MyStrategy(BaseStrategy):
    def __init__(self):
        super().__init__(name="MyStrategy")

    def on_bar(self, engine, bar_data, date):
        # 每个交易日回调
        for code, bar in bar_data.items():
            # 你的选股逻辑
            if self.should_buy(code, bar):
                engine.buy(code, price=bar['close'], reason="自定义信号")

    def should_buy(self, code, bar):
        # 实现你的买入条件
        return True
```

## 注意事项

1. 确保通达信数据路径正确
2. 数据文件为二进制格式，需要正确读取
3. 前复权计算依赖除权除息数据
4. 回测结果仅供参考，不构成投资建议

## 许可证

MIT License
