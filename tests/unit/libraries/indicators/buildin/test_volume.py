"""
Unit tests for qs_trader.libraries.indicators.buildin.volume module.

Tests volume indicator implementations:
- VWAP: Volume Weighted Average Price
- OBV: On-Balance Volume
- AD: Accumulation/Distribution
- CMF: Chaikin Money Flow

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- Parametrize for multiple test cases
- Focus on functional paths
"""

from datetime import datetime, timedelta

import pytest

from qs_trader.libraries.indicators.buildin.volume import AD, CMF, OBV, VWAP
from qs_trader.services.data.models import Bar

# ============================================================================
# Test Fixtures
# ============================================================================
# Note: Common fixtures (sample_bars, ranging_bars, etc.) are defined in conftest.py.
# Volume-specific fixtures are defined below because they require specific
# close positions relative to high/low for money flow calculations.


@pytest.fixture(scope="module")
def uptrend_bars() -> list[Bar]:
    """Create bars with consistent uptrend and close near high."""
    base_time = datetime(2024, 1, 1, 9, 30)
    bars = []
    for i in range(20):
        close = 50.0 + i * 0.5
        # Close near high for buying pressure (accumulation)
        low = close - 0.5
        high = close + 0.1
        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(minutes=i),
                open=low + 0.1,
                high=high,
                low=low,
                close=close,
                volume=1000 + i * 100,
            )
        )
    return bars


@pytest.fixture(scope="module")
def downtrend_bars() -> list[Bar]:
    """Create bars with consistent downtrend and close near low."""
    base_time = datetime(2024, 1, 1, 9, 30)
    bars = []
    for i in range(20):
        close = 50.0 - i * 0.5
        # Close near low for selling pressure (distribution)
        high = close + 0.5
        low = close - 0.1
        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(minutes=i),
                open=high - 0.1,
                high=high,
                low=low,
                close=close,
                volume=1000 + i * 100,
            )
        )
    return bars


# ============================================================================
# VWAP Tests
# ============================================================================


class TestVWAPInitialization:
    """Test VWAP initialization and validation."""

    def test_vwap_initialization(self):
        """Test VWAP initialization."""
        vwap = VWAP()
        assert vwap.value is None
        assert not vwap.is_ready


class TestVWAPCalculate:
    """Test VWAP batch calculation method."""

    def test_vwap_calculate_with_sample_bars(self, sample_bars):
        """Test VWAP calculate method with sample bars."""
        vwap = VWAP()
        result = vwap.calculate(sample_bars)

        # All values should be float (typical price weighted by volume)
        assert all(isinstance(v, float) for v in result)

        # Phase 4: Precise assertion - VWAP should be in reasonable price range for sample data
        # Note: VWAP is cumulative weighted average and may be outside any single bar's range
        assert all(90.0 <= v <= 130.0 for v in result), f"Expected VWAP 90-130, got {result}"
        assert result[0] < result[-1], "VWAP should increase with upward trend in sample data"

    def test_vwap_calculate_empty_list(self):
        """Test VWAP calculate with empty list."""
        vwap = VWAP()
        result = vwap.calculate([])
        assert result == []

    def test_vwap_increases_with_uptrend(self, uptrend_bars):
        """Test VWAP generally increases with uptrending prices."""
        vwap = VWAP()
        result = vwap.calculate(uptrend_bars)

        # VWAP should show upward trend (at least most values increasing)
        increases = sum(1 for i in range(1, len(result)) if result[i] > result[i - 1])
        assert increases > len(result) * 0.7  # At least 70% increases


class TestVWAPUpdate:
    """Test VWAP incremental update method."""

    def test_vwap_update_sequential(self, sample_bars):
        """Test VWAP update method with sequential bars."""
        vwap = VWAP()

        # First update should return a value
        value = vwap.update(sample_bars[0])
        assert isinstance(value, float)

    def test_vwap_update_matches_calculate(self, sample_bars, vwap_results):
        """Test VWAP update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = vwap_results

        vwap_update = VWAP()
        for bar in sample_bars:
            vwap_update.update(bar)

        # Final value should match
        assert abs(vwap_update.value - calculated[-1]) < 0.001


class TestVWAPProperties:
    """Test VWAP properties."""

    def test_vwap_value_after_first_bar(self, sample_bars):
        """Test VWAP value returns value after first bar."""
        vwap = VWAP()
        vwap.update(sample_bars[0])
        assert vwap.value is not None

    def test_vwap_is_ready_after_first_bar(self, sample_bars):
        """Test VWAP is_ready after first bar with volume."""
        vwap = VWAP()
        vwap.update(sample_bars[0])
        assert vwap.is_ready

    def test_vwap_reset(self, sample_bars):
        """Test VWAP reset clears state."""
        vwap = VWAP()

        for bar in sample_bars[:5]:
            vwap.update(bar)

        assert vwap.is_ready
        vwap.reset()
        assert not vwap.is_ready
        assert vwap.value is None


# ============================================================================
# OBV Tests
# ============================================================================


class TestOBVInitialization:
    """Test OBV initialization and validation."""

    def test_obv_initialization(self):
        """Test OBV initialization."""
        obv = OBV()
        assert obv.value is None
        assert not obv.is_ready


class TestOBVCalculate:
    """Test OBV batch calculation method."""

    def test_obv_calculate_with_sample_bars(self, sample_bars):
        """Test OBV calculate method with sample bars."""
        obv = OBV()
        result = obv.calculate(sample_bars)

        # First value should be None
        assert result[0] is None

        # Rest should be float values
        assert all(isinstance(v, float) for v in result[1:])

    def test_obv_calculate_empty_list(self):
        """Test OBV calculate with empty list."""
        obv = OBV()
        result = obv.calculate([])
        assert result == []

    def test_obv_increases_with_uptrend(self, uptrend_bars):
        """Test OBV increases with uptrending prices."""
        obv = OBV()
        result = obv.calculate(uptrend_bars)

        # OBV should increase (accumulation)
        valid_values = [v for v in result if v is not None]
        assert valid_values[-1] >= valid_values[0]

    def test_obv_decreases_with_downtrend(self, downtrend_bars):
        """Test OBV decreases with downtrending prices."""
        obv = OBV()
        result = obv.calculate(downtrend_bars)

        # OBV should decrease (distribution)
        valid_values = [v for v in result if v is not None]
        assert valid_values[-1] <= valid_values[0]


class TestOBVUpdate:
    """Test OBV incremental update method."""

    def test_obv_update_sequential(self, sample_bars):
        """Test OBV update method with sequential bars."""
        obv = OBV()

        # First update should return None
        assert obv.update(sample_bars[0]) is None

        # Second update should return a value
        value = obv.update(sample_bars[1])
        assert isinstance(value, float)

    def test_obv_update_matches_calculate(self, sample_bars, obv_results):
        """Test OBV update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = obv_results

        obv_update = OBV()
        for bar in sample_bars:
            obv_update.update(bar)

        # Final value should match
        assert abs(obv_update.value - calculated[-1]) < 0.001


class TestOBVProperties:
    """Test OBV properties."""

    def test_obv_value_before_ready(self):
        """Test OBV value returns None before ready."""
        obv = OBV()
        assert obv.value is None

    def test_obv_is_ready_after_two_bars(self, sample_bars):
        """Test OBV is_ready after receiving two bars."""
        obv = OBV()

        obv.update(sample_bars[0])
        assert not obv.is_ready

        obv.update(sample_bars[1])
        assert obv.is_ready

    def test_obv_reset(self, sample_bars):
        """Test OBV reset clears state."""
        obv = OBV()

        for bar in sample_bars[:5]:
            obv.update(bar)

        assert obv.is_ready
        obv.reset()
        assert not obv.is_ready
        assert obv.value is None


# ============================================================================
# AD Tests
# ============================================================================


class TestADInitialization:
    """Test A/D initialization and validation."""

    def test_ad_initialization(self):
        """Test A/D initialization."""
        ad = AD()
        assert ad.value is None
        assert not ad.is_ready


class TestADCalculate:
    """Test A/D batch calculation method."""

    def test_ad_calculate_with_sample_bars(self, sample_bars):
        """Test A/D calculate method with sample bars."""
        ad = AD()
        result = ad.calculate(sample_bars)

        # All values should be float
        assert all(isinstance(v, float) for v in result)

    def test_ad_calculate_empty_list(self):
        """Test A/D calculate with empty list."""
        ad = AD()
        result = ad.calculate([])
        assert result == []

    def test_ad_increases_with_uptrend(self, uptrend_bars):
        """Test A/D increases with uptrending prices."""
        ad = AD()
        result = ad.calculate(uptrend_bars)

        # A/D should generally increase (accumulation) in uptrend
        # First value may be 0, check that trend is positive
        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 1
        assert valid_values[-1] >= valid_values[0]

    def test_ad_decreases_with_downtrend(self, downtrend_bars):
        """Test A/D decreases with downtrending prices."""
        ad = AD()
        result = ad.calculate(downtrend_bars)

        # A/D should generally decrease (distribution) in downtrend
        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 1
        assert valid_values[-1] <= valid_values[0]


class TestADUpdate:
    """Test A/D incremental update method."""

    def test_ad_update_sequential(self, sample_bars):
        """Test A/D update method with sequential bars."""
        ad = AD()

        # First update should return a value
        value = ad.update(sample_bars[0])
        assert isinstance(value, float)

    def test_ad_update_matches_calculate(self, sample_bars, ad_results):
        """Test A/D update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = ad_results

        ad_update = AD()
        for bar in sample_bars:
            ad_update.update(bar)

        # Final value should match
        assert abs(ad_update.value - calculated[-1]) < 0.001


class TestADProperties:
    """Test A/D properties."""

    def test_ad_value_after_first_bar(self, sample_bars):
        """Test A/D value returns value after first bar."""
        ad = AD()
        ad.update(sample_bars[0])
        assert ad.value is not None

    def test_ad_is_ready_after_first_bar(self, sample_bars):
        """Test A/D is_ready after first bar."""
        ad = AD()
        ad.update(sample_bars[0])
        assert ad.is_ready

    def test_ad_reset(self, sample_bars):
        """Test A/D reset clears state."""
        ad = AD()

        for bar in sample_bars[:5]:
            ad.update(bar)

        assert ad.is_ready
        ad.reset()
        assert not ad.is_ready
        assert ad.value is None


# ============================================================================
# CMF Tests
# ============================================================================


class TestCMFInitialization:
    """Test CMF initialization and validation."""

    def test_cmf_initialization_default(self):
        """Test CMF initialization with default parameters."""
        cmf = CMF()
        assert cmf.period == 20
        assert cmf.value is None
        assert not cmf.is_ready

    def test_cmf_initialization_custom_period(self):
        """Test CMF initialization with custom period."""
        cmf = CMF(period=10)
        assert cmf.period == 10

    def test_cmf_initialization_invalid_period(self):
        """Test CMF initialization with invalid period."""
        with pytest.raises(ValueError):
            CMF(period=0)


class TestCMFCalculate:
    """Test CMF batch calculation method."""

    def test_cmf_calculate_with_sample_bars(self, sample_bars):
        """Test CMF calculate method with sample bars."""
        cmf = CMF(period=10)
        result = cmf.calculate(sample_bars)

        # First 9 values should be None
        assert all(v is None for v in result[:9])

        # Rest should be float values between -1 and 1
        for v in result[9:]:
            assert isinstance(v, float)
            assert -1.0 <= v <= 1.0

    def test_cmf_calculate_empty_list(self):
        """Test CMF calculate with empty list."""
        cmf = CMF(period=20)
        result = cmf.calculate([])
        assert result == []

    def test_cmf_positive_with_uptrend(self, uptrend_bars):
        """Test CMF is positive with uptrending prices."""
        cmf = CMF(period=10)
        result = cmf.calculate(uptrend_bars)

        # CMF should be mostly positive (buying pressure)
        valid_values = [v for v in result if v is not None]
        positive_count = sum(1 for v in valid_values if v > 0)
        assert positive_count > len(valid_values) * 0.6

    def test_cmf_negative_with_downtrend(self, downtrend_bars):
        """Test CMF is negative with downtrending prices."""
        cmf = CMF(period=10)
        result = cmf.calculate(downtrend_bars)

        # CMF should be mostly negative (selling pressure)
        valid_values = [v for v in result if v is not None]
        negative_count = sum(1 for v in valid_values if v < 0)
        assert negative_count > len(valid_values) * 0.6


class TestCMFUpdate:
    """Test CMF incremental update method."""

    def test_cmf_update_sequential(self, sample_bars):
        """Test CMF update method with sequential bars."""
        cmf = CMF(period=10)

        # First 9 updates should return None
        for i in range(9):
            assert cmf.update(sample_bars[i]) is None

        # 10th update should return a value
        value = cmf.update(sample_bars[9])
        assert isinstance(value, float)
        assert -1.0 <= value <= 1.0

    def test_cmf_update_matches_calculate(self, sample_bars, cmf_results):
        """Test CMF update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = cmf_results

        cmf_update = CMF(period=20)
        for bar in sample_bars:
            cmf_update.update(bar)

        # Final value should match
        assert abs(cmf_update.value - calculated[-1]) < 0.001


class TestCMFProperties:
    """Test CMF properties."""

    def test_cmf_value_before_ready(self):
        """Test CMF value returns None before ready."""
        cmf = CMF(period=20)
        assert cmf.value is None

    def test_cmf_is_ready_after_period(self, sample_bars):
        """Test CMF is_ready after receiving enough data."""
        cmf = CMF(period=10)

        for i in range(9):
            cmf.update(sample_bars[i])
            assert not cmf.is_ready

        cmf.update(sample_bars[9])
        assert cmf.is_ready

    def test_cmf_reset(self, sample_bars):
        """Test CMF reset clears state."""
        cmf = CMF(period=10)

        for bar in sample_bars[:12]:
            cmf.update(bar)

        assert cmf.is_ready
        cmf.reset()
        assert not cmf.is_ready
        assert cmf.value is None


# ============================================================================
# Edge Cases
# ============================================================================


class TestEdgeCases:
    """Test edge cases for volume indicators."""

    def test_zero_volume_vwap(self):
        """Test VWAP with zero volume."""
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=50.0,
            high=51.0,
            low=49.0,
            close=50.5,
            volume=0,
        )

        vwap = VWAP()
        assert vwap.update(bar) is None

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

        vwap = VWAP()
        assert vwap.update(bar) is not None

        obv = OBV()
        assert obv.update(bar) is None

        ad = AD()
        assert ad.update(bar) is not None

        cmf = CMF(period=20)
        assert cmf.update(bar) is None

    def test_flat_high_low_ad(self):
        """Test A/D with flat high/low (no range)."""
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=50.0,
            high=50.0,
            low=50.0,
            close=50.0,
            volume=1000,
        )

        ad = AD()
        value = ad.update(bar)
        # With no range, money flow should be 0
        assert value == 0.0
