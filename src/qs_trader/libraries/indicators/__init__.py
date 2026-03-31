"""
Indicators Library.

Technical indicators for quantitative analysis:
- Moving Averages: SMA, EMA, WMA, DEMA, TEMA, HMA, SMMA
- Momentum: RSI, MACD, Stochastic, CCI, ROC, Williams %R
- Volatility: ATR, Bollinger Bands, Standard Deviation
- Volume: VWAP, OBV, A/D, CMF
- Trend: ADX, Aroon
"""

from qs_trader.libraries.indicators.base import BaseIndicator, IndicatorPlacement
from qs_trader.libraries.indicators.buildin.momentum import CCI, MACD, ROC, RSI, Stochastic, WilliamsR
from qs_trader.libraries.indicators.buildin.moving_averages import DEMA, EMA, HMA, SMA, SMMA, TEMA, WMA
from qs_trader.libraries.indicators.buildin.trend import ADX, Aroon
from qs_trader.libraries.indicators.buildin.volatility import ATR, BollingerBands, StdDev
from qs_trader.libraries.indicators.buildin.volume import AD, CMF, OBV, VWAP

__all__ = [
    # Base
    "BaseIndicator",
    "IndicatorPlacement",
    # Moving Averages
    "SMA",
    "EMA",
    "WMA",
    "DEMA",
    "TEMA",
    "HMA",
    "SMMA",
    # Momentum
    "RSI",
    "MACD",
    "Stochastic",
    "CCI",
    "ROC",
    "WilliamsR",
    # Volatility
    "ATR",
    "BollingerBands",
    "StdDev",
    # Volume
    "VWAP",
    "OBV",
    "AD",
    "CMF",
    # Trend
    "ADX",
    "Aroon",
]
