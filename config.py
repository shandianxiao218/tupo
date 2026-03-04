# -*- coding: utf-8 -*-
"""
通达信股票回测系统 - 配置文件
"""

import os

# ==================== 通达信数据路径 ====================
TDX_DIR = r"C:\new_tdx64"
TDX_DAY_DATA = os.path.join(TDX_DIR, "vipdoc", "sz", "lday")  # 深圳日线
TDX_SH_DAY_DATA = os.path.join(TDX_DIR, "vipdoc", "sh", "lday")  # 上海日线
TDX_MIN_DATA = os.path.join(TDX_DIR, "vipdoc", "sz", "minline")  # 深圳分钟线
TDX_SH_MIN_DATA = os.path.join(TDX_DIR, "vipdoc", "sh", "minline")  # 上海分钟线

# 除权除息数据路径
TDX_XRXD_DATA = os.path.join(TDX_DIR, "vipdoc", "sz", "gbbq")  # 深圳除权除息
TDX_SH_XRXD_DATA = os.path.join(TDX_DIR, "vipdoc", "sh", "gbbq")  # 上海除权除息

# ==================== 交易配置 ====================
COMMISSION_RATE = 0.0003  # 万三佣金
MIN_COMMISSION = 5  # 最低佣金（元）
SLIPPAGE = 0.001  # 滑点 0.1%
STAMP_DUTY = 0.001  # 印花税（仅卖出收取，0.1%）
INITIAL_CAPITAL = 1000000  # 初始资金100万

# 持仓配置
MAX_POSITIONS = 5  # 最大持仓股票数量
MAX_POSITION_PCT = 0.2  # 单只股票最大持仓比例
STOP_LOSS_PCT = 0.08  # 止损比例 8%
TAKE_PROFIT_PCT = 0.25  # 止盈比例 25%

# ==================== 回测配置 ====================
# 数据从2022年开始，因此默认回测起始日期设为2022年
START_DATE = "2022-01-01"
END_DATE = "2026-03-04"

# 数据频率
FREQUENCY = "daily"  # daily, weekly, monthly

# 基准指数
BENCHMARK_INDEX = "000300"  # 沪深300作为基准

# ==================== 策略参数 ====================
# 均线聚拢突破策略参数
STRATEGY_PARAMS = {
    "lookback_days": 60,  # 回看周期
    "min_rise_pct": 0.20,  # 最小上涨幅度
    "min_pullback": 0.05,  # 最小回调幅度
    "max_pullback": 0.15,  # 最大回调幅度
    "convergence_pct": 0.05,  # 均线粘合度（标准差/均值）
    "volume_ratio": 1.5,  # 成交量放大倍数
    "ma_periods": [5, 10, 20, 60],  # 均线周期
}

# ==================== 日志配置 ====================
LOG_LEVEL = "INFO"  # DEBUG, INFO, WARNING, ERROR
LOG_FILE = "backtest.log"

# ==================== 输出配置 ====================
OUTPUT_DIR = "results"
TRADES_FILE = "trades.csv"
POSITIONS_FILE = "positions.csv"
EQUITY_FILE = "equity.csv"

# ==================== 数据缓存配置 ====================
USE_CACHE = True  # 是否使用数据缓存
CACHE_DIR = "cache"

# ==================== 其他配置 ====================
# 股票代码前缀
SZ_PREFIX = "sz"  # 深圳
SH_PREFIX = "sh"  # 上海

# 市场标识
MARKET_SZ = 0  # 深圳市场代码
MARKET_SH = 1  # 上海市场代码

# 数据类型
DAY_DATA = 1  # 日线数据
MIN1_DATA = 2  # 1分钟线
MIN5_DATA = 3  # 5分钟线

# ==================== 工具函数 ====================

def get_market_from_code(code: str) -> int:
    """根据股票代码获取市场类型"""
    if code.startswith("6") or code.startswith("5"):
        return MARKET_SH
    else:
        return MARKET_SZ


def get_data_path(code: str, data_type=DAY_DATA) -> str:
    """根据股票代码获取数据文件路径"""
    market = get_market_from_code(code)
    if market == MARKET_SH:
        base_dir = TDX_SH_DAY_DATA if data_type == DAY_DATA else TDX_SH_MIN_DATA
    else:
        base_dir = TDX_DAY_DATA if data_type == DAY_DATA else TDX_MIN_DATA

    # 通达信文件命名格式: 6位股票代码.day
    code_6digit = code.zfill(6)
    filename = f"{code_6digit}.day"
    return os.path.join(base_dir, filename)


def get_xrxd_path(code: str) -> str:
    """根据股票代码获取除权除息文件路径"""
    market = get_market_from_code(code)
    if market == MARKET_SH:
        base_dir = TDX_SH_XRXD_DATA
    else:
        base_dir = TDX_XRXD_DATA

    # 通达信除权除息文件命名格式: 6位股票代码.qfz
    code_6digit = code.zfill(6)
    filename = f"{code_6digit}.qfz"
    return os.path.join(base_dir, filename)
