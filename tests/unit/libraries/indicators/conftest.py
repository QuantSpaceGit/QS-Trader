"""Shared fixtures for indicator tests."""

from datetime import datetime, timedelta

import pytest

from qs_trader.services.data.models import Bar


def _create_bars(count: int, base_price: float = 50.0, trend: str = "flat") -> list[Bar]:
    """
    Create test bars with specified trend.

    Args:
        count: Number of bars to create
        base_price: Starting price
        trend: "flat", "up", "down", or "ranging"

    Returns:
        List of Bar objects
    """
    base_time = datetime(2024, 1, 1, 9, 30)
    bars = []

    for i in range(count):
        if trend == "up":
            close = base_price + i * 0.5
            low = close - 0.3
            high = close + 0.5
            open_price = low + 0.1
        elif trend == "down":
            close = base_price - i * 0.5
            high = close + 0.3
            low = close - 0.5
            open_price = high - 0.1
        elif trend == "ranging":
            close = base_price + (i % 4 - 2) * 0.3
            high = close + 0.5
            low = close - 0.5
            open_price = close - 0.1
        else:  # flat or default
            offset = i * 0.1 if trend == "sample" else 0
            close = base_price + offset
            high = close + 0.5
            low = close - 0.5
            open_price = close - 0.2

        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(minutes=i),
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=1000 + i * 100,
            )
        )

    return bars


@pytest.fixture(scope="module")
def sample_bars() -> list[Bar]:
    """Create sample bars for general testing (25 bars, optimized for Phase 3)."""
    return _create_bars(25, base_price=100.0, trend="sample")


@pytest.fixture(scope="module")
def uptrend_bars() -> list[Bar]:
    """Create bars with consistent uptrend (25 bars, optimized for Phase 3)."""
    return _create_bars(25, base_price=50.0, trend="up")


@pytest.fixture(scope="module")
def downtrend_bars() -> list[Bar]:
    """Create bars with consistent downtrend (25 bars, optimized for Phase 3)."""
    return _create_bars(25, base_price=50.0, trend="down")


@pytest.fixture(scope="module")
def ranging_bars() -> list[Bar]:
    """Create bars with ranging (sideways) movement (25 bars, optimized for Phase 3)."""
    return _create_bars(25, base_price=50.0, trend="ranging")


@pytest.fixture(scope="module")
def flat_bars() -> list[Bar]:
    """Create bars with flat prices (25 bars at 100.0, optimized for Phase 3)."""
    base_time = datetime(2024, 1, 1, 9, 30)
    bars = []
    for i in range(25):
        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(minutes=i),
                open=100.0,
                high=100.0,
                low=100.0,
                close=100.0,
                volume=1000,
            )
        )
    return bars


@pytest.fixture(scope="module")
def minimal_bars() -> list[Bar]:
    """Create minimal bars for basic testing (5 bars)."""
    return _create_bars(5, base_price=50.0, trend="sample")


# =============================================================================
# CACHED INDICATOR RESULTS (Phase 2 Optimization)
# =============================================================================
# These fixtures pre-compute indicator calculate() results to avoid redundant
# calculations in update/calculate comparison tests. Each test that compares
# update() vs calculate() can use the cached results instead of running
# calculate() again.


@pytest.fixture(scope="module")
def rsi_results(sample_bars) -> list[float | None]:
    """Pre-computed RSI results for sample_bars."""
    from qs_trader.libraries.indicators.buildin.momentum import RSI

    rsi = RSI(period=14)
    return rsi.calculate(sample_bars)


@pytest.fixture(scope="module")
def cci_results(sample_bars) -> list[float | None]:
    """Pre-computed CCI results for sample_bars."""
    from qs_trader.libraries.indicators.buildin.momentum import CCI

    cci = CCI(period=14)
    return cci.calculate(sample_bars)


@pytest.fixture(scope="module")
def roc_results(sample_bars) -> list[float | None]:
    """Pre-computed ROC results for sample_bars."""
    from qs_trader.libraries.indicators.buildin.momentum import ROC

    roc = ROC(period=5)
    return roc.calculate(sample_bars)


@pytest.fixture(scope="module")
def williamsr_results(sample_bars) -> list[float | None]:
    """Pre-computed Williams %R results for sample_bars."""
    from qs_trader.libraries.indicators.buildin.momentum import WilliamsR

    wr = WilliamsR(period=9)
    return wr.calculate(sample_bars)


@pytest.fixture(scope="module")
def atr_results(sample_bars) -> list[float | None]:
    """Pre-computed ATR results for sample_bars (period=5 to match test)."""
    from qs_trader.libraries.indicators.buildin.volatility import ATR

    atr = ATR(period=5)
    return atr.calculate(sample_bars)


@pytest.fixture(scope="module")
def stddev_results(sample_bars) -> list[float | None]:
    """Pre-computed StdDev results for sample_bars (period=10 to match test)."""
    from qs_trader.libraries.indicators.buildin.volatility import StdDev

    stddev = StdDev(period=10)
    return stddev.calculate(sample_bars)


@pytest.fixture(scope="module")
def bb_results(sample_bars) -> list[dict[str, float] | None]:
    """Pre-computed Bollinger Bands results for sample_bars (period=10 to match test)."""
    from qs_trader.libraries.indicators.buildin.volatility import BollingerBands

    bb = BollingerBands(period=10)
    return bb.calculate(sample_bars)


@pytest.fixture(scope="module")
def vwap_results(sample_bars) -> list[float | None]:
    """Pre-computed VWAP results for sample_bars."""
    from qs_trader.libraries.indicators.buildin.volume import VWAP

    vwap = VWAP()
    return vwap.calculate(sample_bars)


@pytest.fixture(scope="module")
def obv_results(sample_bars) -> list[float | None]:
    """Pre-computed OBV results for sample_bars."""
    from qs_trader.libraries.indicators.buildin.volume import OBV

    obv = OBV()
    return obv.calculate(sample_bars)


@pytest.fixture(scope="module")
def ad_results(sample_bars) -> list[float | None]:
    """Pre-computed A/D results for sample_bars."""
    from qs_trader.libraries.indicators.buildin.volume import AD

    ad = AD()
    return ad.calculate(sample_bars)


@pytest.fixture(scope="module")
def cmf_results(sample_bars) -> list[float | None]:
    """Pre-computed CMF results for sample_bars."""
    from qs_trader.libraries.indicators.buildin.volume import CMF

    cmf = CMF(period=20)
    return cmf.calculate(sample_bars)


@pytest.fixture(scope="module")
def adx_results(sample_bars) -> list[dict[str, float] | None]:
    """Pre-computed ADX results for sample_bars (period=10 to match test)."""
    from qs_trader.libraries.indicators.buildin.trend import ADX

    adx = ADX(period=10)
    return adx.calculate(sample_bars)


@pytest.fixture(scope="module")
def aroon_results(sample_bars) -> list[dict[str, float] | None]:
    """Pre-computed Aroon results for sample_bars (period=10 to match test)."""
    from qs_trader.libraries.indicators.buildin.trend import Aroon

    aroon = Aroon(period=10)
    return aroon.calculate(sample_bars)


# NOTE: MACD and Stochastic use module-specific trending_up_bars fixture
# from test_momentum.py, so we can't pre-compute those results here.
# The tests using trending_up_bars will continue to call calculate() once.
