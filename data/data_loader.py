# -*- coding: utf-8 -*-
"""
数据加载器
管理所有股票数据的加载和缓存
"""

import os
import pickle
from typing import Dict, List, Optional
from datetime import datetime
import pandas as pd
from .tdx_reader import TdxReader


class DataLoader:
    """
    数据加载器类

    功能：
    - 加载和管理多只股票的数据
    - 数据缓存机制
    - 批量数据加载
    - 前复权处理
    """

    def __init__(self, sz_data_dir: str, sh_data_dir: str,
                 sz_xr_dir: str = None, sh_xr_dir: str = None,
                 use_cache: bool = True, cache_dir: str = "cache"):
        """
        初始化数据加载器

        Args:
            sz_data_dir: 深圳日线数据目录
            sh_data_dir: 上海日线数据目录
            sz_xr_dir: 深圳除权除息数据目录
            sh_xr_dir: 上海除权除息数据目录
            use_cache: 是否使用缓存
            cache_dir: 缓存目录
        """
        self.sz_data_dir = sz_data_dir
        self.sh_data_dir = sh_data_dir
        self.sz_xr_dir = sz_xr_dir
        self.sh_xr_dir = sh_xr_dir
        self.use_cache = use_cache
        self.cache_dir = cache_dir

        # 数据缓存
        self._data_cache: Dict[str, pd.DataFrame] = {}
        self._xr_data_cache: Dict[str, pd.DataFrame] = {}

        # 创建缓存目录
        if use_cache and not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        # 初始化数据读取器
        self.reader = TdxReader()

    def _get_market(self, code: str) -> str:
        """
        判断股票所属市场

        Args:
            code: 股票代码

        Returns:
            'sz' 或 'sh'
        """
        if code.startswith('6') or code.startswith('5'):
            return 'sh'
        return 'sz'

    def _get_data_dir(self, code: str) -> str:
        """获取股票数据目录"""
        market = self._get_market(code)
        return self.sh_data_dir if market == 'sh' else self.sz_data_dir

    def _get_xr_dir(self, code: str) -> Optional[str]:
        """获取股票除权除息数据目录"""
        market = self._get_market(code)
        if market == 'sh':
            return self.sh_xr_dir
        return self.sz_xr_dir

    def _get_cache_path(self, code: str, start_date: str = None,
                        end_date: str = None, qfq: bool = True) -> str:
        """生成缓存文件路径"""
        cache_name = f"{code}_{start_date}_{end_date}_{qfq}.pkl"
        return os.path.join(self.cache_dir, cache_name.replace('/', '-').replace(' ', '_'))

    def _load_from_cache(self, cache_path: str) -> Optional[pd.DataFrame]:
        """从缓存加载数据"""
        if not self.use_cache or not os.path.exists(cache_path):
            return None

        try:
            with open(cache_path, 'rb') as f:
                return pickle.load(f)
        except Exception as e:
            print(f"加载缓存失败 {cache_path}: {e}")
            return None

    def _save_to_cache(self, df: pd.DataFrame, cache_path: str):
        """保存数据到缓存"""
        if not self.use_cache:
            return

        try:
            with open(cache_path, 'wb') as f:
                pickle.dump(df, f)
        except Exception as e:
            print(f"保存缓存失败 {cache_path}: {e}")

    def get_stock_data(self, code: str, start_date: str = None,
                       end_date: str = None, qfq: bool = True,
                       use_cache: bool = True) -> Optional[pd.DataFrame]:
        """
        获取单只股票数据

        Args:
            code: 股票代码
            start_date: 开始日期（YYYY-MM-DD）
            end_date: 结束日期（YYYY-MM-DD）
            qfq: 是否前复权
            use_cache: 是否使用缓存

        Returns:
            股票数据DataFrame
        """
        code = code.zfill(6)

        # 检查缓存
        cache_path = self._get_cache_path(code, start_date, end_date, qfq)
        if use_cache and self.use_cache:
            cached_data = self._load_from_cache(cache_path)
            if cached_data is not None:
                return cached_data

        # 获取数据目录
        data_dir = self._get_data_dir(code)
        xr_dir = self._get_xr_dir(code)

        # 读取数据
        df = self.reader.read_stock_data(
            code=code,
            data_dir=data_dir,
            start_date=start_date,
            end_date=end_date,
            qfq=qfq,
            xr_dir=xr_dir
        )

        if df is None or len(df) == 0:
            return None

        # 添加股票代码列
        df['code'] = code

        # 保存到缓存
        if use_cache and self.use_cache:
            self._save_to_cache(df, cache_path)

        return df

    def get_all_stock_codes(self) -> List[str]:
        """
        获取所有股票代码列表

        Returns:
            股票代码列表
        """
        codes = []

        # 深圳股票
        if os.path.exists(self.sz_data_dir):
            sz_codes = self.reader.get_stock_list(self.sz_data_dir)
            codes.extend(sz_codes)

        # 上海股票
        if os.path.exists(self.sh_data_dir):
            sh_codes = self.reader.get_stock_list(self.sh_data_dir)
            codes.extend(sh_codes)

        return sorted(set(codes))

    def get_all_stocks_data(self, codes: List[str] = None,
                            start_date: str = None, end_date: str = None,
                            qfq: bool = True) -> Dict[str, pd.DataFrame]:
        """
        批量获取多只股票数据

        Args:
            codes: 股票代码列表，为None时获取所有股票
            start_date: 开始日期
            end_date: 结束日期
            qfq: 是否前复权

        Returns:
            股票代码到数据的映射字典
        """
        if codes is None:
            codes = self.get_all_stock_codes()

        result = {}
        for code in codes:
            df = self.get_stock_data(code, start_date, end_date, qfq)
            if df is not None and len(df) > 0:
                result[code] = df

        return result

    def load_xrxd_data(self, code: str) -> pd.DataFrame:
        """
        加载单只股票的除权除息数据

        Args:
            code: 股票代码

        Returns:
            除权除息数据DataFrame
        """
        code = code.zfill(6)

        # 检查缓存
        if code in self._xr_data_cache:
            return self._xr_data_cache[code]

        xr_dir = self._get_xr_dir(code)
        if xr_dir is None:
            return pd.DataFrame()

        xr_path = os.path.join(xr_dir, f"{code}.qfz")
        xr_data = self.reader.read_xrxd_data(xr_path)

        # 缓存
        self._xr_data_cache[code] = xr_data

        return xr_data

    def get_stock_list_by_market(self, market: str = 'all') -> List[str]:
        """
        按市场获取股票列表

        Args:
            market: 市场类型 - 'sz'(深圳), 'sh'(上海), 'all'(全部)

        Returns:
            股票代码列表
        """
        codes = []

        if market in ['sz', 'all']:
            if os.path.exists(self.sz_data_dir):
                codes.extend(self.reader.get_stock_list(self.sz_data_dir))

        if market in ['sh', 'all']:
            if os.path.exists(self.sh_data_dir):
                codes.extend(self.reader.get_stock_list(self.sh_data_dir))

        return sorted(codes)

    def get_trading_dates(self, start_date: str = None,
                          end_date: str = None) -> List[datetime]:
        """
        获取交易日期列表（所有市场的交易日并集）

        Args:
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            交易日期列表
        """
        # 获取所有股票的交易日期
        dates_set = set()

        codes = self.get_all_stock_codes()[:100]  # 限制数量以提高性能
        for code in codes:
            df = self.get_stock_data(code, start_date, end_date, qfq=False)
            if df is not None and len(df) > 0:
                dates_set.update(df.index.date)

        trading_dates = sorted(dates_set)
        return [pd.Timestamp(d) for d in trading_dates]

    def preload_data(self, codes: List[str], start_date: str = None,
                     end_date: str = None, qfq: bool = True):
        """
        预加载数据到内存缓存

        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            qfq: 是否前复权
        """
        for code in codes:
            self.get_stock_data(code, start_date, end_date, qfq)

    def clear_cache(self):
        """清空内存缓存"""
        self._data_cache.clear()
        self._xr_data_cache.clear()

    def get_stock_info(self, code: str) -> Dict:
        """
        获取股票基本信息

        Args:
            code: 股票代码

        Returns:
            股票信息字典
        """
        code = code.zfill(6)
        market = self._get_market(code)

        df = self.get_stock_data(code, qfq=False)

        if df is None or len(df) == 0:
            return {
                'code': code,
                'market': market,
                'name': '',
                'status': 'no_data'
            }

        # 获取最新数据
        latest = df.iloc[-1]

        return {
            'code': code,
            'market': market,
            'status': 'active',
            'data_start': df.index[0],
            'data_end': df.index[-1],
            'data_count': len(df),
            'latest_price': latest['close'],
            'latest_volume': latest['volume']
        }

    def merge_stock_data(self, codes: List[str],
                         start_date: str = None,
                         end_date: str = None) -> pd.DataFrame:
        """
        合并多只股票数据为宽表格式

        Args:
            codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期

        Returns:
            合并后的DataFrame，日期为索引，各股票收盘价为列
        """
        data_dict = {}

        for code in codes:
            df = self.get_stock_data(code, start_date, end_date, qfq=True)
            if df is not None and len(df) > 0:
                data_dict[code] = df['close']

        if not data_dict:
            return pd.DataFrame()

        merged = pd.DataFrame(data_dict)
        merged = merged.sort_index()

        return merged
