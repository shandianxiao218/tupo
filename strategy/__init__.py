# -*- coding: utf-8 -*-
"""
策略模块
"""

from .base import BaseStrategy
from .signals import SignalGenerator
from .selector import MAConvergenceBreakoutStrategy, SimpleMAStrategy

__all__ = ["BaseStrategy", "SignalGenerator", "MAConvergenceBreakoutStrategy", "SimpleMAStrategy"]
