# -*- coding: utf-8 -*-
"""
通达信数据读取器
读取通达信软件的二进制数据格式文件
"""

import os
import struct
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict, List, Tuple


class TdxReader:
    """
    通达信数据读取器

    支持读取：
    - 日线数据（.day文件）
    - 除权除息数据（.qfz文件）
    - 分钟线数据
    """

    # 通达信日期计算基准日：1899-12-30（Excel兼容）
    TDX_BASE_DATE = datetime(1899, 12, 30)

    @staticmethod
    def _tdx_date_to_datetime(tdx_date: int) -> datetime:
        """
        将通达信日期格式转换为datetime

        Args:
            tdx_date: 通达信日期整数（如20250101）

        Returns:
            datetime对象
        """
        try:
            year = tdx_date // 10000
            month = (tdx_date % 10000) // 100
            day = tdx_date % 100
            return datetime(year, month, day)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _datetime_to_tdx_date(dt: datetime) -> int:
        """
        将datetime转换为通达信日期格式

        Args:
            dt: datetime对象

        Returns:
            通达信日期整数
        """
        return dt.year * 10000 + dt.month * 100 + dt.day

    @staticmethod
    def _read_day_file_format_old(filepath: str) -> Optional[pd.DataFrame]:
        """
        读取旧版通达信日线数据格式（每条记录32字节）

        文件格式：
        - 日期(4字节)：整型，如20250101
        - 开盘价(4字节)：浮点型
        - 最高价(4字节)：浮点型
        - 最低价(4字节)：浮点型
        - 收盘价(4字节)：浮点型
        - 成交额(4字节)：浮点型
        - 成交量(4字节)：浮点型
        - 保留(4字节)：保留字段

        Args:
            filepath: day文件路径

        Returns:
            DataFrame，包含日期、开、高、低、收、成交量、成交额
        """
        if not os.path.exists(filepath):
            return None

        try:
            with open(filepath, 'rb') as f:
                data = f.read()

            # 每条记录32字节
            record_size = 32
            num_records = len(data) // record_size

            if num_records == 0:
                return None

            records = []
            for i in range(num_records):
                offset = i * record_size
                record_data = data[offset:offset + record_size]

                # 解包数据：小端模式
                # 格式: i(日期) i(开) i(高) i(低) i(收) f(成交额) i(成交量) i(保留)
                # 注意：价格是整数格式，需要除以100得到实际价格
                unpacked = struct.unpack('<iiiiifii', record_data)

                date_int = unpacked[0]
                open_price = unpacked[1] / 100.0  # 除以100得到实际价格
                high_price = unpacked[2] / 100.0
                low_price = unpacked[3] / 100.0
                close_price = unpacked[4] / 100.0
                amount = unpacked[5]  # 成交额（元）
                volume = unpacked[6]  # 成交量（手）

                # 过滤无效数据
                if date_int == 0 or close_price <= 0:
                    continue

                date = TdxReader._tdx_date_to_datetime(date_int)
                if date is None:
                    continue

                records.append({
                    'date': date,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume,  # 手
                    'amount': amount  # 元
                })

            if not records:
                return None

            df = pd.DataFrame(records)
            df = df.sort_values('date').reset_index(drop=True)

            return df

        except Exception as e:
            print(f"读取日线文件失败 {filepath}: {e}")
            return None

    @staticmethod
    def _read_day_file_format_new(filepath: str) -> Optional[pd.DataFrame]:
        """
        读取新版通达信日线数据格式（每条记录40字节）

        新版格式增加了更多字段
        """
        if not os.path.exists(filepath):
            return None

        try:
            with open(filepath, 'rb') as f:
                data = f.read()

            # 每条记录40字节
            record_size = 40
            num_records = len(data) // record_size

            if num_records == 0:
                return None

            records = []
            for i in range(num_records):
                offset = i * record_size
                record_data = data[offset:offset + record_size]

                # 新版格式解包
                # 格式: i(日期) i(开) i(高) i(低) i(收) f(成交额) i(成交量) i(保留1) i(保留2) I(保留3)
                # 价格是整数格式，需要除以100
                unpacked = struct.unpack('<iiiiiifiiI', record_data[:36])

                date_int = unpacked[0]
                open_price = unpacked[1] / 100.0
                high_price = unpacked[2] / 100.0
                low_price = unpacked[3] / 100.0
                close_price = unpacked[4] / 100.0
                amount = unpacked[5]
                volume = unpacked[6]

                if date_int == 0 or close_price <= 0:
                    continue

                date = TdxReader._tdx_date_to_datetime(date_int)
                if date is None:
                    continue

                records.append({
                    'date': date,
                    'open': open_price,
                    'high': high_price,
                    'low': low_price,
                    'close': close_price,
                    'volume': volume,
                    'amount': amount
                })

            if not records:
                return None

            df = pd.DataFrame(records)
            df = df.sort_values('date').reset_index(drop=True)

            return df

        except Exception as e:
            print(f"读取新版日线文件失败 {filepath}: {e}")
            return None

    @classmethod
    def read_day_file(cls, filepath: str) -> Optional[pd.DataFrame]:
        """
        读取通达信日线数据文件

        Args:
            filepath: day文件路径

        Returns:
            DataFrame，包含日期、开、高、低、收、成交量、成交额
        """
        # 先尝试新版格式
        df = cls._read_day_file_format_new(filepath)
        if df is not None and len(df) > 0:
            return df

        # 尝试旧版格式
        return cls._read_day_file_format_old(filepath)

    @staticmethod
    def parse_stock_data(df: pd.DataFrame) -> pd.DataFrame:
        """
        解析和清理股票数据

        Args:
            df: 原始数据DataFrame

        Returns:
            清理后的DataFrame
        """
        if df is None or len(df) == 0:
            return pd.DataFrame()

        # 确保日期是datetime类型
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])

        # 设置日期为索引
        df = df.set_index('date').sort_index()

        # 去除重复数据
        df = df[~df.index.duplicated(keep='last')]

        # 填充缺失值
        df = df.ffill().bfill()

        return df

    @classmethod
    def read_xrxd_data(cls, filepath: str) -> pd.DataFrame:
        """
        读取除权除息数据文件（.qfz文件）

        除权除息数据格式：
        - 日期(4字节)：除权除息日期
        - 每股送股数(4字节)
        - 每股转增数(4字节)
        - 每股派现(4字节)：单位元
        - 每股配股数(4字节)
        - 配股价(4字节)
        - 保留字段

        Args:
            filepath: qfz文件路径

        Returns:
            DataFrame，包含日期、送股、转增、派现等
        """
        if not os.path.exists(filepath):
            return pd.DataFrame()

        try:
            with open(filepath, 'rb') as f:
                data = f.read()

            # 每条记录28字节（估算，具体格式可能有差异）
            # 实际格式可能是变长的
            records = []

            # 尝试按固定长度解析
            record_size = 28
            num_records = len(data) // record_size

            for i in range(num_records):
                offset = i * record_size
                record_data = data[offset:offset + record_size]

                unpacked = struct.unpack('<iffffff', record_data)

                date_int = unpacked[0]
                song_gu = unpacked[1]  # 每股送股数
                zhuan_zeng = unpacked[2]  # 每股转增数
                pai_xian = unpacked[3]  # 每股派现（元）
                pei_gu = unpacked[4]  # 每股配股数
                pei_price = unpacked[5]  # 配股价

                if date_int == 0:
                    continue

                date = cls._tdx_date_to_datetime(date_int)
                if date is None:
                    continue

                records.append({
                    'date': date,
                    'song_gu': song_gu,
                    'zhuan_zeng': zhuan_zeng,
                    'pai_xian': pai_xian,
                    'pei_gu': pei_gu,
                    'pei_price': pei_price
                })

            if not records:
                return pd.DataFrame()

            df = pd.DataFrame(records)
            df = df.sort_values('date').reset_index(drop=True)
            df['date'] = pd.to_datetime(df['date'])

            return df

        except Exception as e:
            print(f"读取除权除息文件失败 {filepath}: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_stock_list(data_dir: str) -> List[str]:
        """
        获取指定目录下所有股票代码列表

        Args:
            data_dir: 通达信数据目录路径

        Returns:
            股票代码列表，如['000001', '000002', ...]
        """
        if not os.path.exists(data_dir):
            return []

        try:
            files = os.listdir(data_dir)
            stock_codes = []

            for f in files:
                if f.endswith('.day') or f.endswith('.DAY'):
                    # 提取6位股票代码（支持带市场前缀的文件名，如sh600487.day）
                    code = f.replace('.day', '').replace('.DAY', '')
                    # 去掉市场前缀（sh/sz）
                    if code.startswith('sh') or code.startswith('sz'):
                        code = code[2:]
                    if code.isdigit() and len(code) == 6:
                        stock_codes.append(code)

            return sorted(stock_codes)

        except Exception as e:
            print(f"获取股票列表失败 {data_dir}: {e}")
            return []

    @classmethod
    def apply_qfq(cls, df: pd.DataFrame, xr_data: pd.DataFrame) -> pd.DataFrame:
        """
        应用前复权处理

        前复权计算原理：
        1. 计算每次除权除息的复权因子
        2. 复权因子 = (收盘价 - 每股现金红利) / (收盘价 + 送股数 + 转增数)
        3. 历史价格 = 原始价格 × 当日复权因子 / 最新复权因子

        Args:
            df: 原始价格数据
            xr_data: 除权除息数据

        Returns:
            前复权后的价格数据
        """
        if df is None or len(df) == 0:
            return df

        if xr_data is None or len(xr_data) == 0:
            return df.copy()

        # 复制数据避免修改原数据
        df = df.copy()

        # 初始化复权因子为1
        df['factor'] = 1.0

        # 计算每次除权除息的复权因子
        factors = []

        for idx, row in xr_data.iterrows():
            ex_date = row['date']

            # 找到除权除息日的收盘价
            if ex_date not in df.index:
                # 找到最近的一个交易日
                nearby_dates = df.index[df.index >= ex_date]
                if len(nearby_dates) == 0:
                    continue
                ex_date = nearby_dates[0]

            close_price = df.loc[ex_date, 'close']

            if close_price <= 0:
                continue

            # 计算调整因子
            song_gu = row.get('song_gu', 0)
            zhuan_zeng = row.get('zhuan_zeng', 0)
            pai_xian = row.get('pai_xian', 0)
            pei_gu = row.get('pei_gu', 0)
            pei_price = row.get('pei_price', 0)

            # 复权因子计算公式
            # factor = (收盘价 - 每股派现) / (收盘价 + 送股 + 转增 + 配股数 × 配股价/收盘价)
            if close_price > 0:
                adjustment = (song_gu + zhuan_zeng + pei_gu * pei_price / close_price
                              if close_price > 0 else 0)
                factor = (close_price - pai_xian) / (close_price + adjustment)

                if factor > 0:
                    factors.append({'date': ex_date, 'factor': factor})

        if not factors:
            return df

        # 按日期排序
        factors_df = pd.DataFrame(factors).sort_values('date')
        factors_df = factors_df.set_index('date')

        # 计算累积复权因子（从最新到最早）
        factors_df['cum_factor'] = factors_df['factor'].iloc[::-1].cumprod()[::-1]

        # 应用复权因子
        latest_factor = 1.0
        for date, row in factors_df.iterrows():
            # 在除权除息日之前的所有数据应用该因子
            mask = df.index < date
            factor_ratio = latest_factor / row['cum_factor']
            df.loc[mask, 'factor'] *= factor_ratio
            latest_factor = row['cum_factor']

        # 应用复权因子到价格
        price_columns = ['open', 'high', 'low', 'close']
        for col in price_columns:
            if col in df.columns:
                df[col] = df[col] * df['factor']

        # 删除复权因子列
        df = df.drop(columns=['factor'])

        return df

    @classmethod
    def read_stock_data(cls, code: str, data_dir: str = None,
                        start_date: str = None, end_date: str = None,
                        qfq: bool = True, xr_dir: str = None) -> Optional[pd.DataFrame]:
        """
        读取单只股票的完整数据

        Args:
            code: 股票代码（6位数字）
            data_dir: 日线数据目录
            start_date: 开始日期（格式：YYYY-MM-DD）
            end_date: 结束日期（格式：YYYY-MM-DD）
            qfq: 是否进行前复权处理
            xr_dir: 除权除息数据目录

        Returns:
            股票数据DataFrame
        """
        # 构建文件路径
        code_6digit = code.zfill(6)

        if data_dir is None:
            return None

        # 确定市场前缀
        if code.startswith('6') or code.startswith('5'):
            prefix = 'sh'
        else:
            prefix = 'sz'

        # 尝试多种文件名格式
        filename = f"{prefix}{code_6digit}.day"
        filepath = os.path.join(data_dir, filename)

        # 如果文件不存在，尝试不带前缀的格式
        if not os.path.exists(filepath):
            filename = f"{code_6digit}.day"
            filepath = os.path.join(data_dir, filename)

        # 读取日线数据
        df = cls.read_day_file(filepath)
        if df is None or len(df) == 0:
            return None

        # 解析数据
        df = cls.parse_stock_data(df)

        # 应用前复权
        if qfq and xr_dir:
            xr_filepath = os.path.join(xr_dir, f"{code_6digit}.qfz")
            xr_data = cls.read_xrxd_data(xr_filepath)
            if len(xr_data) > 0:
                df = cls.apply_qfq(df, xr_data)

        # 按日期范围过滤
        if start_date:
            df = df[df.index >= start_date]
        if end_date:
            df = df[df.index <= end_date]

        return df
