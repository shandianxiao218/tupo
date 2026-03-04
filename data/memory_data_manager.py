# -*- coding: utf-8 -*-
"""
内存数据管理模块
一次性读取所有股票数据到内存，并进行前复权处理
"""

from typing import Dict, List, Optional
import pandas as pd
import numpy as np
from pathlib import Path


class MemoryDataManager:
    """
    内存数据管理器

    功能：
    1. 一次性读取所有股票的日线数据到内存
    2. 计算前复权因子并应用到所有历史数据
    3. 提供快速的数据查询接口
    """

    def __init__(self, sh_data_dir: str, sz_data_dir: str):
        """
        初始化内存数据管理器

        Args:
            sh_data_dir: 上海市场数据目录
            sz_data_dir: 深圳市场数据目录
        """
        self.sh_data_dir = sh_data_dir
        self.sz_data_dir = sz_data_dir

        # 内存数据存储: {code: DataFrame}
        self._stock_data: Dict[str, pd.DataFrame] = {}

        # 复权因子缓存: {code: Series}
        self._adjust_factors: Dict[str, pd.Series] = {}

        # 是否已加载
        self._loaded = False

    def load_all_data(self, codes: List[str] = None) -> int:
        """
        加载所有股票数据到内存

        Args:
            codes: 要加载的股票代码列表，None表示加载所有

        Returns:
            成功加载的股票数量
        """
        if codes is None:
            codes = self._get_all_stock_codes()

        print(f"开始加载 {len(codes)} 只股票的数据...")

        count = 0
        for code in codes:
            if self._load_stock(code):
                count += 1

        self._loaded = True
        print(f"成功加载 {count} 只股票的数据")

        return count

    def _get_all_stock_codes(self) -> List[str]:
        """获取所有股票代码"""
        codes = []

        # 扫描上海市场
        if Path(self.sh_data_dir).exists():
            for f in Path(self.sh_data_dir).glob("sh*.day"):
                code = f.stem[2:]  # 去掉 "sh" 前缀
                if code.isdigit() and len(code) == 6:
                    codes.append(code)

        # 扫描深圳市场
        if Path(self.sz_data_dir).exists():
            for f in Path(self.sz_data_dir).glob("sz*.day"):
                code = f.stem[2:]  # 去掉 "sz" 前缀
                if code.isdigit() and len(code) == 6:
                    codes.append(code)

        return codes

    def _load_stock(self, code: str) -> bool:
        """
        加载单只股票的数据

        Args:
            code: 股票代码

        Returns:
            是否成功加载
        """
        from .tdx_reader import TdxReader

        # 确定数据文件路径
        if code.startswith('6') or code.startswith('5'):
            filepath = f"{self.sh_data_dir}/sh{code}.day"
        else:
            filepath = f"{self.sz_data_dir}/sz{code}.day"

        # 读取原始数据
        df = TdxReader.read_day_file(filepath)

        if df is None or len(df) == 0:
            return False

        # 计算前复权因子
        adjust_factor = self._calculate_adjust_factor(df)

        # 应用前复权
        df_adj = self._apply_adjust_factor(df, adjust_factor)

        # 存储到内存
        self._stock_data[code] = df_adj
        self._adjust_factors[code] = adjust_factor

        return True

    def _calculate_adjust_factor(self, df: pd.DataFrame) -> pd.Series:
        """
        计算前复权因子

        方法：从最新日期往回推，当遇到除权（收盘价异常跳变）时计算因子

        Args:
            df: 原始数据DataFrame

        Returns:
            复权因子Series
        """
        if len(df) < 2:
            return pd.Series(1.0, index=df.index)

        df = df.sort_values('date').reset_index(drop=True)

        # 初始化因子为1
        factors = pd.Series(1.0, index=df.index)

        # 从后往前遍历，检测除权事件
        cumulative_factor = 1.0

        for i in range(len(df) - 1, 0, -1):
            current_close = df.loc[i, 'close']
            prev_close = df.loc[i - 1, 'close']

            # 检测异常跳变（简单方法：价格变化超过20%可能意味着除权）
            # 注意：这是简化的检测方法，实际应该使用复权文件
            if prev_close > 0:
                change_ratio = abs(current_close - prev_close) / prev_close

                # 如果开盘价相对于前收盘价有大幅跳空，可能意味着除权
                if i > 0:
                    current_open = df.loc[i, 'open']
                    prev_close = df.loc[i - 1, 'close']

                    if prev_close > 0:
                        gap_ratio = (current_open - prev_close) / prev_close

                        # 跳空幅度超过10%（向下）可能是除权
                        if gap_ratio < -0.1:
                            # 计算调整因子
                            adjustment_ratio = prev_close / current_open
                            cumulative_factor *= adjustment_ratio

            factors.iloc[i] = cumulative_factor

        return pd.Series(factors.values, index=df['date'])

    def _apply_adjust_factor(self, df: pd.DataFrame, factor: pd.Series) -> pd.DataFrame:
        """
        应用前复权因子到价格数据

        Args:
            df: 原始数据
            factor: 复权因子Series

        Returns:
            复权后的DataFrame
        """
        df_adj = df.copy()

        # 对齐因子
        factor_aligned = factor.reindex(df_adj['date'])

        # 应用因子到价格列
        price_cols = ['open', 'high', 'low', 'close']
        for col in price_cols:
            df_adj[col] = df_adj[col] * factor_aligned.values

        return df_adj

    def get_stock_data(self, code: str,
                       start_date: str = None,
                       end_date: str = None) -> Optional[pd.DataFrame]:
        """
        获取股票数据

        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            DataFrame或None
        """
        if code not in self._stock_data:
            return None

        df = self._stock_data[code].copy()

        # 设置日期索引
        df = df.set_index('date')

        # 过滤日期范围
        if start_date is not None:
            df = df[df.index >= start_date]
        if end_date is not None:
            df = df[df.index <= end_date]

        return df if len(df) > 0 else None

    def get_bar(self, code: str, date: pd.Timestamp) -> Optional[pd.Series]:
        """
        获取指定日期的行情数据

        Args:
            code: 股票代码
            date: 日期

        Returns:
            Series或None
        """
        df = self.get_stock_data(code)
        if df is None or date not in df.index:
            return None

        return df.loc[date]

    def get_all_codes(self) -> List[str]:
        """获取所有已加载的股票代码"""
        return list(self._stock_data.keys())

    def is_loaded(self) -> bool:
        """是否已加载数据"""
        return self._loaded

    def clear(self):
        """清空内存数据"""
        self._stock_data.clear()
        self._adjust_factors.clear()
        self._loaded = False
