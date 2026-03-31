"""Unit tests for trend indicators."""

from datetime import datetime, timedelta

import pytest

from qs_trader.libraries.indicators import ADX, Aroon
from qs_trader.services.data.models import Bar

# ============================================================================
# Test Fixtures
# ============================================================================
# Note: Common fixtures (sample_bars, uptrend_bars, downtrend_bars, ranging_bars)
# are defined in conftest.py with module scope for better performance.


# ============================================================================
# ADX Tests
# ============================================================================


class TestADXInitialization:
    """Test ADX initialization and validation."""

    def test_adx_initialization_default(self):
        """Test ADX initialization with default period."""
        adx = ADX()
        assert adx.value is None
        assert not adx.is_ready

    def test_adx_initialization_custom_period(self):
        """Test ADX initialization with custom period."""
        adx = ADX(period=20)
        assert adx.value is None
        assert not adx.is_ready

    def test_adx_initialization_invalid_period(self):
        """Test ADX raises ValueError for invalid period."""
        with pytest.raises(ValueError, match="Period must be greater than 0"):
            ADX(period=0)

        with pytest.raises(ValueError, match="Period must be greater than 0"):
            ADX(period=-5)


class TestADXCalculate:
    """Test ADX batch calculation method."""

    def test_adx_calculate_with_sample_bars(self, sample_bars):
        """Test ADX calculate method with sample bars."""
        adx = ADX(period=14)
        result = adx.calculate(sample_bars)

        assert len(result) == len(sample_bars)
        # First 14 bars should be None (need period for initial smoothed values)
        # Bar 15 (index 14) should still be None (need one more for ADX)
        assert all(v is None for v in result[:15])

        # No valid ADX values with only 15 bars and period=14
        # (need period + 1 bars minimum)

    def test_adx_calculate_with_uptrend(self, uptrend_bars):
        """Test ADX calculate with uptrending prices."""
        adx = ADX(period=14)
        result = adx.calculate(uptrend_bars)

        # Get valid values (after warmup)
        valid_values = [v for v in result if v is not None]

        # Should have values
        assert len(valid_values) > 0

        # All values should have the three components
        for val in valid_values:
            assert "adx" in val
            assert "plus_di" in val
            assert "minus_di" in val
            assert 0 <= val["adx"] <= 100
            assert 0 <= val["plus_di"] <= 100
            assert 0 <= val["minus_di"] <= 100

        # In strong uptrend, +DI should be > -DI
        assert valid_values[-1]["plus_di"] > valid_values[-1]["minus_di"]

    def test_adx_calculate_with_downtrend(self, downtrend_bars):
        """Test ADX calculate with downtrending prices."""
        adx = ADX(period=14)
        result = adx.calculate(downtrend_bars)

        # Get valid values
        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 0

        # In strong downtrend, -DI should be > +DI
        assert valid_values[-1]["minus_di"] > valid_values[-1]["plus_di"]

    def test_adx_calculate_with_ranging(self, ranging_bars):
        """Test ADX with ranging market."""
        adx = ADX(period=14)
        result = adx.calculate(ranging_bars)

        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 0

        # Phase 4: Precise assertion - ranging market shows weak ADX (5-25 typical)
        avg_adx = sum(v["adx"] for v in valid_values) / len(valid_values)
        assert 5 <= avg_adx <= 25, f"Expected avg ADX 5-25 in ranging market, got {avg_adx:.2f}"

    def test_adx_calculate_empty_list(self):
        """Test ADX calculate with empty list."""
        adx = ADX()
        result = adx.calculate([])
        assert result == []


class TestADXUpdate:
    """Test ADX sequential update method."""

    def test_adx_update_sequential(self, uptrend_bars):
        """Test ADX update method processes bars sequentially."""
        adx = ADX(period=14)

        results = []
        for bar in uptrend_bars:
            value = adx.update(bar)
            results.append(value)

        # Should match calculate results
        calculated = adx.calculate(uptrend_bars)
        assert len(results) == len(calculated)

        # Compare valid values
        for i, (update_val, calc_val) in enumerate(zip(results, calculated)):
            if calc_val is None:
                assert update_val is None
            else:
                assert update_val is not None
                assert abs(update_val["adx"] - calc_val["adx"]) < 0.001
                assert abs(update_val["plus_di"] - calc_val["plus_di"]) < 0.001
                assert abs(update_val["minus_di"] - calc_val["minus_di"]) < 0.001

    def test_adx_update_matches_calculate(self, sample_bars, adx_results):
        """Test that update mode matches calculate mode."""
        adx1 = ADX(period=10)

        # Update mode
        update_results = [adx1.update(bar) for bar in sample_bars]

        # Use pre-computed results from fixture (Phase 2 optimization)
        calc_results = adx_results

        assert len(update_results) == len(calc_results)
        for update_val, calc_val in zip(update_results, calc_results):
            if calc_val is None:
                assert update_val is None
            else:
                assert update_val is not None
                assert abs(update_val["adx"] - calc_val["adx"]) < 0.001


class TestADXProperties:
    """Test ADX property methods."""

    def test_adx_value_before_ready(self):
        """Test ADX value property before indicator is ready."""
        adx = ADX(period=14)
        assert adx.value is None
        assert not adx.is_ready

    def test_adx_is_ready_after_period(self, uptrend_bars):
        """Test ADX is_ready after sufficient bars."""
        adx = ADX(period=14)

        # Feed bars
        for i, bar in enumerate(uptrend_bars):
            adx.update(bar)
            if i <= 14:
                assert not adx.is_ready
            else:
                assert adx.is_ready
                assert adx.value is not None

    def test_adx_reset(self, sample_bars):
        """Test ADX reset method."""
        adx = ADX(period=10)

        # Process some bars
        for bar in sample_bars:
            adx.update(bar)

        # Reset
        adx.reset()
        assert adx.value is None
        assert not adx.is_ready


# ============================================================================
# Aroon Tests
# ============================================================================


class TestAroonInitialization:
    """Test Aroon initialization and validation."""

    def test_aroon_initialization_default(self):
        """Test Aroon initialization with default period."""
        aroon = Aroon()
        assert aroon.value is None
        assert not aroon.is_ready

    def test_aroon_initialization_custom_period(self):
        """Test Aroon initialization with custom period."""
        aroon = Aroon(period=20)
        assert aroon.value is None
        assert not aroon.is_ready

    def test_aroon_initialization_invalid_period(self):
        """Test Aroon raises ValueError for invalid period."""
        with pytest.raises(ValueError, match="Period must be greater than 0"):
            Aroon(period=0)

        with pytest.raises(ValueError, match="Period must be greater than 0"):
            Aroon(period=-5)


class TestAroonCalculate:
    """Test Aroon batch calculation method."""

    def test_aroon_calculate_with_sample_bars(self, sample_bars):
        """Test Aroon calculate method with sample bars."""
        aroon = Aroon(period=10)
        result = aroon.calculate(sample_bars)

        assert len(result) == len(sample_bars)
        # First 9 bars should be None
        assert all(v is None for v in result[:9])

        # Remaining should have values
        valid_values = [v for v in result if v is not None]
        for val in valid_values:
            assert "aroon_up" in val
            assert "aroon_down" in val
            assert "oscillator" in val
            assert 0 <= val["aroon_up"] <= 100
            assert 0 <= val["aroon_down"] <= 100
            assert -100 <= val["oscillator"] <= 100

    def test_aroon_calculate_with_uptrend(self, uptrend_bars):
        """Test Aroon with uptrending prices."""
        aroon = Aroon(period=25)
        result = aroon.calculate(uptrend_bars)

        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 0

        # Phase 4: Precise assertion - strong uptrend shows aroon_up near 100, aroon_down near 0
        last_val = valid_values[-1]
        assert 90 <= last_val["aroon_up"] <= 100, f"Expected aroon_up 90-100, got {last_val['aroon_up']}"
        assert 0 <= last_val["aroon_down"] <= 20, f"Expected aroon_down 0-20, got {last_val['aroon_down']}"
        assert 70 <= last_val["oscillator"] <= 100, f"Expected oscillator 70-100, got {last_val['oscillator']}"

    def test_aroon_calculate_with_downtrend(self, downtrend_bars):
        """Test Aroon with downtrending prices."""
        aroon = Aroon(period=25)
        result = aroon.calculate(downtrend_bars)

        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 0

        # Phase 4: Precise assertion - strong downtrend shows aroon_down near 100, aroon_up near 0
        last_val = valid_values[-1]
        assert 90 <= last_val["aroon_down"] <= 100, f"Expected aroon_down 90-100, got {last_val['aroon_down']}"
        assert 0 <= last_val["aroon_up"] <= 20, f"Expected aroon_up 0-20, got {last_val['aroon_up']}"
        assert -100 <= last_val["oscillator"] <= -70, f"Expected oscillator -100 to -70, got {last_val['oscillator']}"

    def test_aroon_calculate_with_ranging(self, ranging_bars):
        """Test Aroon with ranging market."""
        aroon = Aroon(period=20)
        result = aroon.calculate(ranging_bars)

        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 0

        # Phase 4: Precise assertion - ranging market has oscillator averaging near 0 (< 40 absolute)
        # In ranging market, both Aroon Up and Down should be moderate
        avg_osc = sum(abs(v["oscillator"]) for v in valid_values) / len(valid_values)
        assert avg_osc < 40, f"Expected avg oscillator < 40, got {avg_osc:.2f}"

    def test_aroon_calculate_empty_list(self):
        """Test Aroon calculate with empty list."""
        aroon = Aroon()
        result = aroon.calculate([])
        assert result == []


class TestAroonUpdate:
    """Test Aroon sequential update method."""

    def test_aroon_update_sequential(self, uptrend_bars):
        """Test Aroon update method processes bars sequentially."""
        aroon = Aroon(period=25)

        results = []
        for bar in uptrend_bars:
            value = aroon.update(bar)
            results.append(value)

        # Should match calculate results
        calculated = aroon.calculate(uptrend_bars)
        assert len(results) == len(calculated)

    def test_aroon_update_matches_calculate(self, sample_bars, aroon_results):
        """Test that update mode matches calculate mode."""
        aroon1 = Aroon(period=10)

        # Update mode
        update_results = [aroon1.update(bar) for bar in sample_bars]

        # Use pre-computed results from fixture (Phase 2 optimization)
        calc_results = aroon_results

        assert len(update_results) == len(calc_results)
        for update_val, calc_val in zip(update_results, calc_results):
            if calc_val is None:
                assert update_val is None
            else:
                assert update_val is not None
                assert abs(update_val["aroon_up"] - calc_val["aroon_up"]) < 0.001
                assert abs(update_val["aroon_down"] - calc_val["aroon_down"]) < 0.001
                assert abs(update_val["oscillator"] - calc_val["oscillator"]) < 0.001


class TestAroonProperties:
    """Test Aroon property methods."""

    def test_aroon_value_before_ready(self):
        """Test Aroon value property before indicator is ready."""
        aroon = Aroon(period=25)
        assert aroon.value is None
        assert not aroon.is_ready

    def test_aroon_is_ready_after_period(self, sample_bars):
        """Test Aroon is_ready after sufficient bars."""
        aroon = Aroon(period=10)

        # Feed bars
        for i, bar in enumerate(sample_bars):
            aroon.update(bar)
            if i < 9:
                assert not aroon.is_ready
            else:
                assert aroon.is_ready
                assert aroon.value is not None

    def test_aroon_reset(self, sample_bars):
        """Test Aroon reset method."""
        aroon = Aroon(period=10)

        # Process some bars
        for bar in sample_bars:
            aroon.update(bar)

        # Reset
        aroon.reset()
        assert aroon.value is None
        assert not aroon.is_ready

    def test_aroon_boundaries(self, uptrend_bars):
        """Test that Aroon values stay within boundaries."""
        aroon = Aroon(period=25)

        for bar in uptrend_bars:
            val = aroon.update(bar)
            if val is not None:
                # Aroon Up and Down should be 0-100
                assert 0 <= val["aroon_up"] <= 100
                assert 0 <= val["aroon_down"] <= 100
                # Oscillator should be -100 to 100
                assert -100 <= val["oscillator"] <= 100
                # Oscillator should equal Up - Down
                assert abs(val["oscillator"] - (val["aroon_up"] - val["aroon_down"])) < 0.001


# ============================================================================
# Edge Cases
# ============================================================================


class TestEdgeCases:
    """Test edge cases for trend indicators."""

    def test_single_bar_adx(self, sample_bars):
        """Test ADX with single bar."""
        adx = ADX(period=14)
        result = adx.calculate([sample_bars[0]])
        assert result == [None]

    def test_single_bar_aroon(self, sample_bars):
        """Test Aroon with single bar."""
        aroon = Aroon(period=25)
        result = aroon.calculate([sample_bars[0]])
        assert result == [None]

    def test_adx_flat_prices(self):
        """Test ADX with flat prices (no movement)."""
        base_time = datetime(2024, 1, 1, 9, 30)
        bars = []
        for i in range(20):
            bars.append(
                Bar(
                    trade_datetime=base_time + timedelta(minutes=i),
                    open=50.0,
                    high=50.0,
                    low=50.0,
                    close=50.0,
                    volume=1000,
                )
            )

        adx = ADX(period=14)
        result = adx.calculate(bars)

        valid_values = [v for v in result if v is not None]
        # With flat prices, ADX should be 0 or very low
        for val in valid_values:
            assert val["adx"] < 1.0
            assert val["plus_di"] == 0.0
            assert val["minus_di"] == 0.0

    def test_aroon_at_boundaries(self):
        """Test Aroon when high/low are at period boundaries."""
        base_time = datetime(2024, 1, 1, 9, 30)
        bars = []
        # Create bars where highest high is at the most recent bar
        for i in range(26):
            bars.append(
                Bar(
                    trade_datetime=base_time + timedelta(minutes=i),
                    open=50.0 + i * 0.1,
                    high=50.5 + i * 0.1,
                    low=49.5 + i * 0.1,
                    close=50.0 + i * 0.1,
                    volume=1000,
                )
            )

        aroon = Aroon(period=25)
        result = aroon.calculate(bars)

        # Last bar should have Aroon Up = 100 (highest high is most recent)
        assert result[-1] is not None
        assert result[-1]["aroon_up"] == 100.0
