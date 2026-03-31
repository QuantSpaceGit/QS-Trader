"""
Unit tests for qs_trader.libraries.indicators.buildin.volatility module.

Tests volatility indicator implementations:
- ATR: Average True Range
- StdDev: Standard Deviation
- BollingerBands: Bollinger Bands

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- Parametrize for multiple test cases
- Focus on functional paths
"""

from datetime import datetime, timedelta

import pytest

from qs_trader.libraries.indicators.buildin.volatility import ATR, BollingerBands, StdDev
from qs_trader.services.data.models import Bar

# ============================================================================
# Test Fixtures
# ============================================================================
# Note: Common fixtures (sample_bars, uptrend_bars, etc.) are defined in
# conftest.py with module scope for better performance.
# Volatility-specific fixtures are defined below.


@pytest.fixture(scope="module")
def volatile_bars() -> list[Bar]:
    """Create bars with high volatility."""
    base_time = datetime(2024, 1, 1, 9, 30)
    # Large price swings
    closes = [50, 55, 48, 60, 45, 62, 43, 65, 40, 68, 38, 70, 35, 72, 33, 75, 30, 78, 28, 80]

    bars = []
    for i, close in enumerate(closes):
        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(minutes=i),
                open=close - 1,
                high=close + 3,
                low=close - 3,
                close=close,
                volume=1000,
            )
        )
    return bars


@pytest.fixture(scope="module")
def low_volatility_bars() -> list[Bar]:
    """Create bars with low volatility."""
    base_time = datetime(2024, 1, 1, 9, 30)
    # Small price movements
    closes = [50.0 + i * 0.1 for i in range(20)]

    bars = []
    for i, close in enumerate(closes):
        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(minutes=i),
                open=close - 0.05,
                high=close + 0.1,
                low=close - 0.1,
                close=close,
                volume=1000,
            )
        )
    return bars


# ============================================================================
# ATR Tests
# ============================================================================


class TestATRInitialization:
    """Test ATR initialization and validation."""

    def test_atr_initialization_default(self):
        """Test ATR initialization with default parameters."""
        atr = ATR()
        assert atr.period == 14
        assert atr.value is None
        assert not atr.is_ready

    def test_atr_initialization_custom_period(self):
        """Test ATR initialization with custom period."""
        atr = ATR(period=20)
        assert atr.period == 20

    def test_atr_initialization_invalid_period(self):
        """Test ATR initialization with invalid period."""
        with pytest.raises(ValueError):
            ATR(period=0)


class TestATRCalculate:
    """Test ATR batch calculation method."""

    def test_atr_calculate_with_sample_bars(self, sample_bars):
        """Test ATR calculate method with sample bars."""
        atr = ATR(period=5)
        result = atr.calculate(sample_bars)

        # First 5 values should be None (need period + 1 for first ATR)
        assert all(v is None for v in result[:5])

        # Rest should be positive float values
        assert all(isinstance(v, float) and v > 0 for v in result[5:])

    def test_atr_calculate_empty_list(self):
        """Test ATR calculate with empty list."""
        atr = ATR(period=14)
        result = atr.calculate([])
        assert result == []

    def test_atr_higher_with_volatile_bars(self, volatile_bars, low_volatility_bars):
        """Test ATR is higher with more volatile prices."""
        atr_volatile = ATR(period=10)
        result_volatile = atr_volatile.calculate(volatile_bars)

        atr_low = ATR(period=10)
        result_low = atr_low.calculate(low_volatility_bars)

        # Average ATR should be higher for volatile bars
        avg_volatile = sum(v for v in result_volatile if v is not None) / len(
            [v for v in result_volatile if v is not None]
        )
        avg_low = sum(v for v in result_low if v is not None) / len([v for v in result_low if v is not None])

        assert avg_volatile > avg_low


class TestATRUpdate:
    """Test ATR incremental update method."""

    def test_atr_update_sequential(self, sample_bars):
        """Test ATR update method with sequential bars."""
        atr = ATR(period=5)

        # First 4 updates should return None
        for i in range(4):
            assert atr.update(sample_bars[i]) is None

        # Phase 4: Precise assertion - ATR should be positive with meaningful volatility
        value = atr.update(sample_bars[4])
        assert isinstance(value, float)
        assert 0.5 <= value <= 5.0, f"Expected ATR 0.5-5.0, got {value}"

    def test_atr_update_matches_calculate(self, sample_bars, atr_results):
        """Test ATR update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = atr_results

        atr_update = ATR(period=5)
        for bar in sample_bars:
            atr_update.update(bar)

        # Final value should match (within reasonable tolerance for Wilder's smoothing)
        assert abs(atr_update.value - calculated[-1]) < 0.05


class TestATRProperties:
    """Test ATR properties."""

    def test_atr_value_before_ready(self):
        """Test ATR value returns None before ready."""
        atr = ATR(period=14)
        assert atr.value is None

    def test_atr_is_ready_after_period(self, sample_bars):
        """Test ATR is_ready after receiving enough data."""
        atr = ATR(period=5)

        for i in range(4):
            atr.update(sample_bars[i])
            assert not atr.is_ready

        atr.update(sample_bars[4])
        assert atr.is_ready

    def test_atr_reset(self, sample_bars):
        """Test ATR reset clears state."""
        atr = ATR(period=5)

        for bar in sample_bars[:8]:
            atr.update(bar)

        assert atr.is_ready
        atr.reset()
        assert not atr.is_ready
        assert atr.value is None


# ============================================================================
# StdDev Tests
# ============================================================================


class TestStdDevInitialization:
    """Test StdDev initialization and validation."""

    def test_stddev_initialization_default(self):
        """Test StdDev initialization with default parameters."""
        stddev = StdDev()
        assert stddev.period == 20
        assert stddev.price_field == "close"
        assert stddev.ddof == 0
        assert stddev.value is None
        assert not stddev.is_ready

    def test_stddev_initialization_custom_period(self):
        """Test StdDev initialization with custom period."""
        stddev = StdDev(period=30)
        assert stddev.period == 30

    def test_stddev_initialization_custom_price_field(self):
        """Test StdDev initialization with custom price field."""
        stddev = StdDev(price_field="high")
        assert stddev.price_field == "high"

    def test_stddev_initialization_invalid_period(self):
        """Test StdDev initialization with invalid period."""
        with pytest.raises(ValueError):
            StdDev(period=0)


class TestStdDevCalculate:
    """Test StdDev batch calculation method."""

    def test_stddev_calculate_with_sample_bars(self, sample_bars):
        """Test StdDev calculate method with sample bars."""
        stddev = StdDev(period=10)
        result = stddev.calculate(sample_bars)

        # First 9 values should be None
        assert all(v is None for v in result[:9])

        # Phase 4: Precise assertion - StdDev should be positive with measurable variance
        assert all(isinstance(v, float) and 0.1 <= v <= 10.0 for v in result[9:]), (
            f"Expected StdDev 0.1-10.0, got {[v for v in result[9:]]}"
        )

    def test_stddev_calculate_empty_list(self):
        """Test StdDev calculate with empty list."""
        stddev = StdDev(period=20)
        result = stddev.calculate([])
        assert result == []

    def test_stddev_higher_with_volatile_bars(self, volatile_bars, low_volatility_bars):
        """Test StdDev is higher with more volatile prices."""
        stddev_volatile = StdDev(period=10)
        result_volatile = stddev_volatile.calculate(volatile_bars)

        stddev_low = StdDev(period=10)
        result_low = stddev_low.calculate(low_volatility_bars)

        # StdDev should be higher for volatile bars
        assert result_volatile[-1] is not None and result_low[-1] is not None
        assert result_volatile[-1] > result_low[-1]


class TestStdDevUpdate:
    """Test StdDev incremental update method."""

    def test_stddev_update_sequential(self, sample_bars):
        """Test StdDev update method with sequential bars."""
        stddev = StdDev(period=10)

        # First 9 updates should return None
        for i in range(9):
            assert stddev.update(sample_bars[i]) is None

        # Phase 4: Precise assertion - StdDev should be positive with measurable variance
        value = stddev.update(sample_bars[9])
        assert isinstance(value, float)
        assert 0.1 <= value <= 10.0, f"Expected StdDev 0.1-10.0, got {value}"

    def test_stddev_update_matches_calculate(self, sample_bars, stddev_results):
        """Test StdDev update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = stddev_results

        stddev_update = StdDev(period=10)
        for bar in sample_bars:
            stddev_update.update(bar)

        # Final value should match
        assert abs(stddev_update.value - calculated[-1]) < 0.001


class TestStdDevProperties:
    """Test StdDev properties."""

    def test_stddev_value_before_ready(self):
        """Test StdDev value returns None before ready."""
        stddev = StdDev(period=20)
        assert stddev.value is None

    def test_stddev_is_ready_after_period(self, sample_bars):
        """Test StdDev is_ready after receiving enough data."""
        stddev = StdDev(period=10)

        for i in range(9):
            stddev.update(sample_bars[i])
            assert not stddev.is_ready

        stddev.update(sample_bars[9])
        assert stddev.is_ready

    def test_stddev_reset(self, sample_bars):
        """Test StdDev reset clears state."""
        stddev = StdDev(period=10)

        for bar in sample_bars[:12]:
            stddev.update(bar)

        assert stddev.is_ready
        stddev.reset()
        assert not stddev.is_ready
        assert stddev.value is None


# ============================================================================
# BollingerBands Tests
# ============================================================================


class TestBollingerBandsInitialization:
    """Test BollingerBands initialization and validation."""

    def test_bb_initialization_default(self):
        """Test BollingerBands initialization with default parameters."""
        bb = BollingerBands()
        assert bb.period == 20
        assert bb.num_std == 2.0
        assert bb.price_field == "close"
        assert bb.value is None
        assert not bb.is_ready

    def test_bb_initialization_custom_period(self):
        """Test BollingerBands initialization with custom period."""
        bb = BollingerBands(period=30, num_std=2.5)
        assert bb.period == 30
        assert bb.num_std == 2.5

    def test_bb_initialization_invalid_period(self):
        """Test BollingerBands initialization with invalid period."""
        with pytest.raises(ValueError):
            BollingerBands(period=0)

    def test_bb_initialization_invalid_std(self):
        """Test BollingerBands initialization with invalid num_std."""
        with pytest.raises(ValueError):
            BollingerBands(num_std=-1)


class TestBollingerBandsCalculate:
    """Test BollingerBands batch calculation method."""

    def test_bb_calculate_with_sample_bars(self, sample_bars):
        """Test BollingerBands calculate method with sample bars."""
        bb = BollingerBands(period=10)
        result = bb.calculate(sample_bars)

        # First 9 values should be None
        assert all(v is None for v in result[:9])

        # Rest should be dicts with upper, middle, lower keys
        for value in result[9:]:
            assert isinstance(value, dict)
            assert "upper" in value
            assert "middle" in value
            assert "lower" in value
            # Upper should be greater than middle, middle greater than lower
            assert value["upper"] > value["middle"] > value["lower"]

    def test_bb_calculate_empty_list(self):
        """Test BollingerBands calculate with empty list."""
        bb = BollingerBands(period=20)
        result = bb.calculate([])
        assert result == []

    def test_bb_bands_relationship(self, sample_bars):
        """Test Bollinger Bands maintain proper relationship."""
        bb = BollingerBands(period=10, num_std=2.0)
        result = bb.calculate(sample_bars)

        for value in result:
            if value is not None:
                # Upper - Middle should equal Middle - Lower (symmetric)
                upper_distance = value["upper"] - value["middle"]
                lower_distance = value["middle"] - value["lower"]
                assert abs(upper_distance - lower_distance) < 0.001

    def test_bb_wider_with_volatile_bars(self, volatile_bars, low_volatility_bars):
        """Test Bollinger Bands are wider with more volatile prices."""
        bb_volatile = BollingerBands(period=10)
        result_volatile = bb_volatile.calculate(volatile_bars)

        bb_low = BollingerBands(period=10)
        result_low = bb_low.calculate(low_volatility_bars)

        # Band width should be larger for volatile bars
        assert result_volatile[-1] is not None and result_low[-1] is not None
        width_volatile = result_volatile[-1]["upper"] - result_volatile[-1]["lower"]
        width_low = result_low[-1]["upper"] - result_low[-1]["lower"]

        assert width_volatile > width_low


class TestBollingerBandsUpdate:
    """Test BollingerBands incremental update method."""

    def test_bb_update_sequential(self, sample_bars):
        """Test BollingerBands update method with sequential bars."""
        bb = BollingerBands(period=10)

        # First 9 updates should return None
        for i in range(9):
            assert bb.update(sample_bars[i]) is None

        # 10th update should return a dict
        value = bb.update(sample_bars[9])
        assert isinstance(value, dict)
        assert "upper" in value
        assert "middle" in value
        assert "lower" in value

    def test_bb_update_matches_calculate(self, sample_bars, bb_results):
        """Test BollingerBands update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = bb_results

        bb_update = BollingerBands(period=10)
        for bar in sample_bars:
            bb_update.update(bar)

        # Final values should match
        calc_final = calculated[-1]
        update_final = bb_update.value

        assert calc_final is not None and update_final is not None
        assert abs(calc_final["upper"] - update_final["upper"]) < 0.001
        assert abs(calc_final["middle"] - update_final["middle"]) < 0.001
        assert abs(calc_final["lower"] - update_final["lower"]) < 0.001


class TestBollingerBandsProperties:
    """Test BollingerBands properties."""

    def test_bb_value_before_ready(self):
        """Test BollingerBands value returns None before ready."""
        bb = BollingerBands(period=20)
        assert bb.value is None

    def test_bb_is_ready_after_period(self, sample_bars):
        """Test BollingerBands is_ready after receiving enough data."""
        bb = BollingerBands(period=10)

        for i in range(9):
            bb.update(sample_bars[i])
            assert not bb.is_ready

        bb.update(sample_bars[9])
        assert bb.is_ready

    def test_bb_reset(self, sample_bars):
        """Test BollingerBands reset clears state."""
        bb = BollingerBands(period=10)

        for bar in sample_bars[:12]:
            bb.update(bar)

        assert bb.is_ready
        bb.reset()
        assert not bb.is_ready
        assert bb.value is None


# ============================================================================
# Edge Cases
# ============================================================================


class TestEdgeCases:
    """Test edge cases for volatility indicators."""

    def test_flat_prices_stddev(self):
        """Test StdDev with flat prices returns zero."""
        base_time = datetime(2024, 1, 1, 9, 30)
        bars = [
            Bar(
                trade_datetime=base_time + timedelta(minutes=i),
                open=50.0,
                high=50.0,
                low=50.0,
                close=50.0,
                volume=1000,
            )
            for i in range(20)
        ]

        stddev = StdDev(period=10)
        result = stddev.calculate(bars)

        # StdDev should be zero for flat prices
        assert result[-1] == 0.0

    def test_single_bar(self):
        """Test indicators with single bar."""
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=50.0,
            high=51.0,
            low=49.0,
            close=50.5,
            volume=1000,
        )

        atr = ATR(period=14)
        assert atr.update(bar) is None

        stddev = StdDev(period=20)
        assert stddev.update(bar) is None

        bb = BollingerBands(period=20)
        assert bb.update(bar) is None
