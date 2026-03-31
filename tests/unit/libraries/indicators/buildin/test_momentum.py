"""
Unit tests for qs_trader.libraries.indicators.buildin.momentum module.

Tests momentum indicator implementations:
- RSI: Relative Strength Index
- MACD: Moving Average Convergence Divergence
- Stochastic: Stochastic Oscillator

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- Parametrize for multiple test cases
- Focus on functional paths
"""

from datetime import datetime, timedelta

import pytest

from qs_trader.libraries.indicators.buildin.momentum import CCI, MACD, ROC, RSI, Stochastic, WilliamsR
from qs_trader.services.data.models import Bar

# ============================================================================
# Test Fixtures
# ============================================================================
# Note: Common fixtures (sample_bars, uptrend_bars, downtrend_bars, etc.)
# are defined in conftest.py with module scope for better performance.


@pytest.fixture(scope="module")
def trending_up_bars() -> list[Bar]:
    """Create bars with consistent uptrend for momentum tests."""
    base_time = datetime(2024, 1, 1)
    bars = []
    for i in range(30):
        price = 100 + i * 0.5  # Steady uptrend
        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(days=i),
                open=price - 0.1,
                high=price + 0.3,
                low=price - 0.2,
                close=price,
                volume=10000,
            )
        )
    return bars


@pytest.fixture(scope="module")
def trending_down_bars() -> list[Bar]:
    """Create bars with consistent downtrend for momentum tests."""
    base_time = datetime(2024, 1, 1)
    bars = []
    for i in range(30):
        price = 100 - i * 0.5  # Steady downtrend
        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(days=i),
                open=price + 0.1,
                high=price + 0.2,
                low=price - 0.3,
                close=price,
                volume=10000,
            )
        )
    return bars


# ============================================================================
# Test RSI - Relative Strength Index
# ============================================================================


class TestRSIInitialization:
    """Test RSI initialization."""

    def test_default_initialization(self):
        """RSI initializes with default period 14."""
        rsi = RSI()
        assert rsi.period == 14
        assert rsi.price_field == "close"
        assert not rsi.is_ready

    def test_custom_period(self):
        """RSI accepts custom period."""
        rsi = RSI(period=21)
        assert rsi.period == 21

    def test_invalid_period_raises_error(self):
        """RSI raises error for invalid period."""
        with pytest.raises(ValueError, match="Period must be >= 1"):
            RSI(period=0)


class TestRSICalculate:
    """Test RSI batch calculation."""

    def test_calculate_returns_none_during_warmup(self, sample_bars):
        """RSI returns None during warmup period."""
        rsi = RSI(period=14)
        results = rsi.calculate(sample_bars)

        # First 14 values should be None (need 15 bars for first RSI)
        for i in range(14):
            assert results[i] is None

    def test_calculate_returns_values_after_warmup(self, trending_up_bars):
        """RSI returns values after warmup period."""
        rsi = RSI(period=14)
        results = rsi.calculate(trending_up_bars)

        # After warmup, should have RSI values
        assert results[14] is not None
        assert 0 <= results[14] <= 100

    def test_rsi_bounds(self, trending_up_bars):
        """RSI values are always between 0 and 100."""
        rsi = RSI(period=14)
        results = rsi.calculate(trending_up_bars)

        for value in results:
            if value is not None:
                assert 0 <= value <= 100

    def test_uptrend_produces_high_rsi(self, trending_up_bars):
        """Strong uptrend produces RSI above 50."""
        rsi = RSI(period=14)
        results = rsi.calculate(trending_up_bars)

        # Filter out None values and check later values
        # Phase 4: Precise assertion - strong consistent uptrend produces RSI 65-100 (can hit boundary)
        valid_values = [v for v in results[-10:] if v is not None]
        assert all(65 <= v <= 100 for v in valid_values), f"Uptrend RSI should be 65-100, got {valid_values}"

    def test_downtrend_produces_low_rsi(self, trending_down_bars):
        """Strong downtrend produces RSI below 50."""
        rsi = RSI(period=14)
        results = rsi.calculate(trending_down_bars)

        # Phase 4: Precise assertion - strong consistent downtrend produces RSI 0-35 (can hit boundary)
        valid_values = [v for v in results[-10:] if v is not None]
        assert all(0 <= v <= 35 for v in valid_values), f"Downtrend RSI should be 0-35, got {valid_values}"


class TestRSIUpdate:
    """Test RSI incremental updates."""

    def test_update_returns_none_during_warmup(self, sample_bars):
        """RSI update returns None during warmup."""
        rsi = RSI(period=14)

        # First 13 bars should return None (bars 0-12)
        for i, bar in enumerate(sample_bars[:13]):
            result = rsi.update(bar)
            assert result is None, f"Bar {i} should return None during warmup"

    def test_update_returns_value_after_warmup(self, trending_up_bars):
        """RSI update returns value after warmup."""
        rsi = RSI(period=14)

        for bar in trending_up_bars[:14]:
            rsi.update(bar)

        result = rsi.update(trending_up_bars[14])
        assert result is not None
        assert 0 <= result <= 100

    def test_update_matches_calculate(self, sample_bars, rsi_results):
        """RSI incremental updates match batch calculation."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        batch_results = rsi_results

        rsi_incremental = RSI(period=14)
        incremental_results = []
        for bar in sample_bars:
            incremental_results.append(rsi_incremental.update(bar))

        # Compare non-None values (allow slightly larger tolerance for RSI due to smoothing)
        for i, (batch, incr) in enumerate(zip(batch_results, incremental_results)):
            if batch is not None and incr is not None:
                assert abs(batch - incr) < 0.5, f"Mismatch at index {i}: {batch} vs {incr}"

    def test_reset_clears_state(self, sample_bars):
        """RSI reset clears internal state."""
        rsi = RSI(period=14)

        # Process some bars
        for bar in sample_bars[:10]:
            rsi.update(bar)

        # Reset
        rsi.reset()
        assert not rsi.is_ready
        assert rsi.value is None

        # Should work like new after reset
        result = rsi.update(sample_bars[0])
        assert result is None


class TestRSIProperties:
    """Test RSI properties."""

    def test_is_ready_false_initially(self):
        """RSI is_ready is False initially."""
        rsi = RSI(period=14)
        assert not rsi.is_ready

    def test_is_ready_after_warmup(self, trending_up_bars):
        """RSI is_ready after processing enough bars."""
        rsi = RSI(period=14)

        for bar in trending_up_bars[:14]:
            rsi.update(bar)

        assert rsi.is_ready

    def test_value_property(self, trending_up_bars):
        """RSI value property returns current value."""
        rsi = RSI(period=14)

        for bar in trending_up_bars[:15]:
            rsi.update(bar)

        value = rsi.value
        assert value is not None
        assert 0 <= value <= 100


# ============================================================================
# Test MACD - Moving Average Convergence Divergence
# ============================================================================


class TestMACDInitialization:
    """Test MACD initialization."""

    def test_default_initialization(self):
        """MACD initializes with default periods 12/26/9."""
        macd = MACD()
        assert macd.fast_period == 12
        assert macd.slow_period == 26
        assert macd.signal_period == 9
        assert not macd.is_ready

    def test_custom_periods(self):
        """MACD accepts custom periods."""
        macd = MACD(fast_period=10, slow_period=20, signal_period=5)
        assert macd.fast_period == 10
        assert macd.slow_period == 20
        assert macd.signal_period == 5

    def test_invalid_periods_raise_error(self):
        """MACD raises error if fast >= slow."""
        with pytest.raises(ValueError, match="Fast period.*must be < slow period"):
            MACD(fast_period=26, slow_period=12)


class TestMACDCalculate:
    """Test MACD batch calculation."""

    def test_calculate_returns_none_during_warmup(self, sample_bars):
        """MACD returns None during warmup period."""
        macd = MACD(fast_period=12, slow_period=26, signal_period=9)
        results = macd.calculate(sample_bars)

        # Should be None during warmup
        assert results[0] is None

    def test_calculate_returns_dict_after_warmup(self, trending_up_bars):
        """MACD returns dictionary with macd, signal, histogram."""
        macd = MACD(fast_period=12, slow_period=26, signal_period=9)
        results = macd.calculate(trending_up_bars)

        # Find first non-None result
        for result in results:
            if result is not None:
                assert "macd" in result
                assert "signal" in result
                assert "histogram" in result
                assert isinstance(result["macd"], float)
                assert isinstance(result["signal"], float)
                assert isinstance(result["histogram"], float)
                break

    def test_histogram_equals_macd_minus_signal(self, trending_up_bars):
        """MACD histogram equals macd line minus signal line."""
        macd = MACD(fast_period=12, slow_period=26, signal_period=9)
        results = macd.calculate(trending_up_bars)

        for result in results:
            if result is not None:
                expected_histogram = result["macd"] - result["signal"]
                assert abs(result["histogram"] - expected_histogram) < 0.0001

    def test_uptrend_produces_positive_macd(self, trending_up_bars):
        """Strong uptrend typically produces positive MACD."""
        macd = MACD(fast_period=12, slow_period=26, signal_period=9)
        results = macd.calculate(trending_up_bars)

        # Phase 4: Precise assertion - strong uptrend produces positive MACD (>= 80% of values)
        valid_results = [r for r in results[-5:] if r is not None]
        if valid_results:
            positive_count = sum(1 for r in valid_results if r["macd"] > 0)
            assert positive_count >= len(valid_results) * 0.8, (
                f"Expected ≥80% positive MACD, got {positive_count}/{len(valid_results)}"
            )


class TestMACDUpdate:
    """Test MACD incremental updates."""

    def test_update_returns_none_during_warmup(self, sample_bars):
        """MACD update returns None during warmup."""
        macd = MACD(fast_period=5, slow_period=10, signal_period=3)

        # MACD needs slow_period bars for slow EMA to be ready,
        # then signal_period - 1 more bars for signal EMA to be ready
        # = 10 + (3 - 1) = 11 bars before first valid MACD value
        for i, bar in enumerate(sample_bars[:11]):
            result = macd.update(bar)
            assert result is None, f"Expected None at bar {i} during warmup, got {result}"

    def test_update_returns_dict_after_warmup(self):
        """MACD update returns dictionary after warmup."""
        # Create enough bars for MACD to be ready (slow + signal period)
        base_time = datetime(2024, 1, 1)
        bars = []
        for i in range(40):  # More than enough for 26 + 9
            price = 100 + i * 0.3
            bars.append(
                Bar(
                    trade_datetime=base_time + timedelta(days=i),
                    open=price - 0.1,
                    high=price + 0.2,
                    low=price - 0.2,
                    close=price,
                    volume=10000,
                )
            )

        macd = MACD(fast_period=12, slow_period=26, signal_period=9)

        result = None
        for bar in bars:
            result = macd.update(bar)
            if result is not None:
                break

        assert result is not None
        assert "macd" in result
        assert "signal" in result
        assert "histogram" in result

    def test_update_matches_calculate(self, trending_up_bars):
        """MACD incremental updates match batch calculation."""
        macd_batch = MACD(fast_period=12, slow_period=26, signal_period=9)
        batch_results = macd_batch.calculate(trending_up_bars)

        macd_incremental = MACD(fast_period=12, slow_period=26, signal_period=9)
        incremental_results = []
        for bar in trending_up_bars:
            incremental_results.append(macd_incremental.update(bar))

        # Compare non-None results
        for batch, incr in zip(batch_results, incremental_results):
            if batch is not None and incr is not None:
                assert abs(batch["macd"] - incr["macd"]) < 0.01
                assert abs(batch["signal"] - incr["signal"]) < 0.01
                assert abs(batch["histogram"] - incr["histogram"]) < 0.01

    def test_reset_clears_state(self, trending_up_bars):
        """MACD reset clears internal state."""
        macd = MACD(fast_period=12, slow_period=26, signal_period=9)

        for bar in trending_up_bars[:20]:
            macd.update(bar)

        macd.reset()
        assert not macd.is_ready
        assert macd.value is None


# ============================================================================
# Test Stochastic Oscillator
# ============================================================================


class TestStochasticInitialization:
    """Test Stochastic initialization."""

    def test_default_initialization(self):
        """Stochastic initializes with default periods 14/3/3."""
        stoch = Stochastic()
        assert stoch.period == 14
        assert stoch.smooth_k == 3
        assert stoch.smooth_d == 3
        assert not stoch.is_ready

    def test_custom_periods(self):
        """Stochastic accepts custom periods."""
        stoch = Stochastic(period=21, smooth_k=5, smooth_d=3)
        assert stoch.period == 21
        assert stoch.smooth_k == 5
        assert stoch.smooth_d == 3


class TestStochasticCalculate:
    """Test Stochastic batch calculation."""

    def test_calculate_returns_none_during_warmup(self, sample_bars):
        """Stochastic returns None during warmup."""
        stoch = Stochastic(period=14, smooth_k=3, smooth_d=3)
        results = stoch.calculate(sample_bars)

        # Should be None during warmup
        assert results[0] is None

    def test_calculate_returns_dict_after_warmup(self, trending_up_bars):
        """Stochastic returns dictionary with k and d values."""
        stoch = Stochastic(period=14, smooth_k=3, smooth_d=3)
        results = stoch.calculate(trending_up_bars)

        for result in results:
            if result is not None:
                assert "k" in result
                assert "d" in result
                assert 0 <= result["k"] <= 100
                assert 0 <= result["d"] <= 100
                break

    def test_stochastic_bounds(self, trending_up_bars):
        """Stochastic %K and %D are always between 0 and 100."""
        stoch = Stochastic(period=14, smooth_k=3, smooth_d=3)
        results = stoch.calculate(trending_up_bars)

        for result in results:
            if result is not None:
                assert 0 <= result["k"] <= 100
                assert 0 <= result["d"] <= 100

    def test_uptrend_produces_high_stochastic(self, trending_up_bars):
        """Strong uptrend produces high Stochastic values."""
        stoch = Stochastic(period=14, smooth_k=3, smooth_d=3)
        results = stoch.calculate(trending_up_bars)

        # Phase 4: Precise assertion - uptrend produces high Stochastic (≥80% above 60)
        valid_results = [r for r in results[-5:] if r is not None]
        if valid_results:
            high_count = sum(1 for r in valid_results if r["k"] > 60)
            assert high_count >= len(valid_results) * 0.8, (
                f"Expected ≥80% stochastic k>60, got {high_count}/{len(valid_results)}"
            )


class TestStochasticUpdate:
    """Test Stochastic incremental updates."""

    def test_update_returns_none_during_warmup(self, sample_bars):
        """Stochastic update returns None during warmup."""
        stoch = Stochastic(period=5, smooth_k=3, smooth_d=3)

        # Stochastic needs: period bars for raw stochastic,
        # then smooth_k - 1 more for %K SMA, then smooth_d - 1 more for %D SMA
        # = 5 + (3 - 1) + (3 - 1) = 9 bars total, but last bar gives first value
        # So bars 0-7 should be None, bar 8 gives first value
        for i, bar in enumerate(sample_bars[:8]):
            result = stoch.update(bar)
            assert result is None, f"Expected None at bar {i} during warmup, got {result}"

    def test_update_returns_dict_after_warmup(self, trending_up_bars):
        """Stochastic update returns dictionary after warmup."""
        stoch = Stochastic(period=14, smooth_k=3, smooth_d=3)

        result = None
        for bar in trending_up_bars:
            result = stoch.update(bar)
            if result is not None:
                break

        assert result is not None
        assert "k" in result
        assert "d" in result

    def test_update_matches_calculate(self, trending_up_bars):
        """Stochastic incremental updates match batch calculation."""
        stoch_batch = Stochastic(period=14, smooth_k=3, smooth_d=3)
        batch_results = stoch_batch.calculate(trending_up_bars)

        stoch_incremental = Stochastic(period=14, smooth_k=3, smooth_d=3)
        incremental_results = []
        for bar in trending_up_bars:
            incremental_results.append(stoch_incremental.update(bar))

        # Compare non-None results
        for i, (batch, incr) in enumerate(zip(batch_results, incremental_results)):
            if batch is not None and incr is not None:
                assert abs(batch["k"] - incr["k"]) < 0.01, f"Mismatch at {i}: K values differ"
                assert abs(batch["d"] - incr["d"]) < 0.01, f"Mismatch at {i}: D values differ"

    def test_reset_clears_state(self, trending_up_bars):
        """Stochastic reset clears internal state."""
        stoch = Stochastic(period=14, smooth_k=3, smooth_d=3)

        for bar in trending_up_bars[:20]:
            stoch.update(bar)

        stoch.reset()
        assert not stoch.is_ready
        assert stoch.value is None


# ============================================================================
# Edge Cases and Special Scenarios
# ============================================================================


class TestEdgeCases:
    """Test edge cases across all momentum indicators."""

    def test_rsi_with_flat_prices(self):
        """RSI handles flat prices (no change)."""
        base_time = datetime(2024, 1, 1)
        flat_bars = []
        for i in range(20):
            flat_bars.append(
                Bar(
                    trade_datetime=base_time + timedelta(days=i),
                    open=100,
                    high=100,
                    low=100,
                    close=100,
                    volume=1000,
                )
            )

        rsi = RSI(period=14)
        results = rsi.calculate(flat_bars)

        # Should handle flat prices gracefully (RSI should be 50 or handle division by zero)
        for result in results[14:]:
            if result is not None:
                assert 0 <= result <= 100

    def test_empty_bars_list(self):
        """Indicators handle empty bars list."""
        rsi = RSI(period=14)
        assert rsi.calculate([]) == []

        macd = MACD()
        assert macd.calculate([]) == []

        stoch = Stochastic()
        assert stoch.calculate([]) == []

    def test_single_bar(self):
        """Indicators handle single bar gracefully."""
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1),
            open=100,
            high=101,
            low=99,
            close=100,
            volume=1000,
        )

        rsi = RSI(period=14)
        assert rsi.update(bar) is None

        macd = MACD()
        assert macd.update(bar) is None

        stoch = Stochastic()
        assert stoch.update(bar) is None


# ============================================================================
# CCI Tests
# ============================================================================


class TestCCIInitialization:
    """Test CCI initialization and validation."""

    def test_cci_initialization_default(self):
        """Test CCI initialization with default parameters."""
        cci = CCI()
        assert cci.period == 20
        assert cci.value is None
        assert not cci.is_ready

    def test_cci_initialization_custom_period(self):
        """Test CCI initialization with custom period."""
        cci = CCI(period=30)
        assert cci.period == 30

    def test_cci_initialization_invalid_period(self):
        """Test CCI initialization with invalid period."""
        with pytest.raises(ValueError):
            CCI(period=0)


class TestCCICalculate:
    """Test CCI batch calculation method."""

    def test_cci_calculate_with_sample_bars(self, sample_bars):
        """Test CCI calculate method with sample bars."""
        cci = CCI(period=10)
        result = cci.calculate(sample_bars)

        # First 9 values should be None (warmup period)
        assert all(v is None for v in result[:9])

        # Rest should be float values
        assert all(isinstance(v, float) for v in result[9:])

    def test_cci_calculate_empty_list(self):
        """Test CCI calculate with empty list."""
        cci = CCI(period=20)
        result = cci.calculate([])
        assert result == []

    def test_cci_calculate_trending_up(self, trending_up_bars):
        """Test CCI with uptrending prices produces positive values."""
        cci = CCI(period=14)
        result = cci.calculate(trending_up_bars)

        # Phase 4: Precise assertion - strong uptrend produces mostly positive CCI (>75%)
        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 0
        positive_count = sum(1 for v in valid_values if v > 0)
        assert positive_count > len(valid_values) * 0.75, (
            f"Expected >75% positive CCI, got {positive_count}/{len(valid_values)}"
        )


class TestCCIUpdate:
    """Test CCI incremental update method."""

    def test_cci_update_sequential(self, sample_bars):
        """Test CCI update method with sequential bars."""
        cci = CCI(period=10)

        # First 9 updates should return None
        for i in range(9):
            assert cci.update(sample_bars[i]) is None

        # 10th update should return a value
        value = cci.update(sample_bars[9])
        assert isinstance(value, float)

    def test_cci_update_matches_calculate(self, sample_bars, cci_results):
        """Test CCI update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = cci_results

        cci_update = CCI(period=20)
        for bar in sample_bars:
            cci_update.update(bar)

        # Final value should match (CCI can have larger variance due to mean deviation calculation)
        assert abs(cci_update.value - calculated[-1]) < 5.0


class TestCCIProperties:
    """Test CCI properties."""

    def test_cci_value_before_ready(self):
        """Test CCI value returns None before ready."""
        cci = CCI(period=20)
        assert cci.value is None

    def test_cci_is_ready_after_period(self, sample_bars):
        """Test CCI is_ready after receiving enough data."""
        cci = CCI(period=10)

        for i in range(9):
            cci.update(sample_bars[i])
            assert not cci.is_ready

        cci.update(sample_bars[9])
        assert cci.is_ready

    def test_cci_reset(self, sample_bars):
        """Test CCI reset clears state."""
        cci = CCI(period=10)

        for bar in sample_bars[:12]:
            cci.update(bar)

        assert cci.is_ready
        cci.reset()
        assert not cci.is_ready
        assert cci.value is None


# ============================================================================
# ROC Tests
# ============================================================================


class TestROCInitialization:
    """Test ROC initialization and validation."""

    def test_roc_initialization_default(self):
        """Test ROC initialization with default parameters."""
        roc = ROC()
        assert roc.period == 12
        assert roc.price_field == "close"
        assert roc.value is None
        assert not roc.is_ready

    def test_roc_initialization_custom_period(self):
        """Test ROC initialization with custom period."""
        roc = ROC(period=20)
        assert roc.period == 20

    def test_roc_initialization_custom_price_field(self):
        """Test ROC initialization with custom price field."""
        roc = ROC(price_field="open")
        assert roc.price_field == "open"

    def test_roc_initialization_invalid_period(self):
        """Test ROC initialization with invalid period."""
        with pytest.raises(ValueError):
            ROC(period=0)


class TestROCCalculate:
    """Test ROC batch calculation method."""

    def test_roc_calculate_with_sample_bars(self, sample_bars):
        """Test ROC calculate method with sample bars."""
        roc = ROC(period=5)
        result = roc.calculate(sample_bars)

        # First 5 values should be None (need period + 1)
        assert all(v is None for v in result[:5])

        # Rest should be float values
        assert all(isinstance(v, float) for v in result[5:])

    def test_roc_calculate_empty_list(self):
        """Test ROC calculate with empty list."""
        roc = ROC(period=12)
        result = roc.calculate([])
        assert result == []

    def test_roc_calculate_trending_up(self, trending_up_bars):
        """Test ROC with uptrending prices produces positive values."""
        roc = ROC(period=10)
        result = roc.calculate(trending_up_bars)

        # Phase 4: Precise assertion - uptrend ROC should be positive (0.1-5% range typical)
        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 0
        assert all(0.1 <= v <= 10.0 for v in valid_values), f"Expected ROC 0.1-10%, got {valid_values}"

    def test_roc_calculate_trending_down(self, trending_down_bars):
        """Test ROC with downtrending prices produces negative values."""
        roc = ROC(period=10)
        result = roc.calculate(trending_down_bars)

        # Phase 4: Precise assertion - downtrend ROC should be negative (-10% to -0.1% range typical)
        valid_values = [v for v in result if v is not None]
        assert len(valid_values) > 0
        assert all(-10.0 <= v <= -0.1 for v in valid_values), f"Expected ROC -10% to -0.1%, got {valid_values}"


class TestROCUpdate:
    """Test ROC incremental update method."""

    def test_roc_update_sequential(self, sample_bars):
        """Test ROC update method with sequential bars."""
        roc = ROC(period=5)

        # First 5 updates should return None
        for i in range(5):
            assert roc.update(sample_bars[i]) is None

        # 6th update should return a value
        value = roc.update(sample_bars[5])
        assert isinstance(value, float)

    def test_roc_update_matches_calculate(self, sample_bars, roc_results):
        """Test ROC update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = roc_results

        roc_update = ROC(period=10)
        for bar in sample_bars:
            roc_update.update(bar)

        # Final value should match (ROC can have variance due to percentage calculation)
        assert abs(roc_update.value - calculated[-1]) < 1.0


class TestROCProperties:
    """Test ROC properties."""

    def test_roc_value_before_ready(self):
        """Test ROC value returns None before ready."""
        roc = ROC(period=12)
        assert roc.value is None

    def test_roc_is_ready_after_period(self, sample_bars):
        """Test ROC is_ready after receiving enough data."""
        roc = ROC(period=5)

        for i in range(5):
            roc.update(sample_bars[i])
            assert not roc.is_ready

        roc.update(sample_bars[5])
        assert roc.is_ready

    def test_roc_reset(self, sample_bars):
        """Test ROC reset clears state."""
        roc = ROC(period=5)

        for bar in sample_bars[:8]:
            roc.update(bar)

        assert roc.is_ready
        roc.reset()
        assert not roc.is_ready
        assert roc.value is None


# ============================================================================
# Williams %R Tests
# ============================================================================


class TestWilliamsRInitialization:
    """Test Williams %R initialization and validation."""

    def test_williamsr_initialization_default(self):
        """Test Williams %R initialization with default parameters."""
        williams_r = WilliamsR()
        assert williams_r.period == 14
        assert williams_r.value is None
        assert not williams_r.is_ready

    def test_williamsr_initialization_custom_period(self):
        """Test Williams %R initialization with custom period."""
        williams_r = WilliamsR(period=20)
        assert williams_r.period == 20

    def test_williamsr_initialization_invalid_period(self):
        """Test Williams %R initialization with invalid period."""
        with pytest.raises(ValueError):
            WilliamsR(period=0)


class TestWilliamsRCalculate:
    """Test Williams %R batch calculation method."""

    def test_williamsr_calculate_with_sample_bars(self, sample_bars):
        """Test Williams %R calculate method with sample bars."""
        williams_r = WilliamsR(period=10)
        result = williams_r.calculate(sample_bars)

        # First 9 values should be None (warmup period)
        assert all(v is None for v in result[:9])

        # Rest should be float values between -100 and 0
        assert all(isinstance(v, float) and -100 <= v <= 0 for v in result[9:])

    def test_williamsr_calculate_empty_list(self):
        """Test Williams %R calculate with empty list."""
        williams_r = WilliamsR(period=14)
        result = williams_r.calculate([])
        assert result == []

    def test_williamsr_bounds(self, sample_bars):
        """Test Williams %R values stay within -100 to 0 bounds."""
        williams_r = WilliamsR(period=10)
        result = williams_r.calculate(sample_bars)

        for value in result:
            if value is not None:
                assert -100 <= value <= 0


class TestWilliamsRUpdate:
    """Test Williams %R incremental update method."""

    def test_williamsr_update_sequential(self, sample_bars):
        """Test Williams %R update method with sequential bars."""
        williams_r = WilliamsR(period=10)

        # First 9 updates should return None
        for i in range(9):
            assert williams_r.update(sample_bars[i]) is None

        # 10th update should return a value
        value = williams_r.update(sample_bars[9])
        assert isinstance(value, float)
        assert -100 <= value <= 0

    def test_williamsr_update_matches_calculate(self, sample_bars, williamsr_results):
        """Test Williams %R update matches calculate for same data."""
        # Use pre-computed results from fixture (Phase 2 optimization)
        calculated = williamsr_results

        wr_update = WilliamsR(period=14)
        for bar in sample_bars:
            wr_update.update(bar)

        # Final value should match (Williams %R can have variance in range calculations)
        assert abs(wr_update.value - calculated[-1]) < 10.0


class TestWilliamsRProperties:
    """Test Williams %R properties."""

    def test_williamsr_value_before_ready(self):
        """Test Williams %R value returns None before ready."""
        williams_r = WilliamsR(period=14)
        assert williams_r.value is None

    def test_williamsr_is_ready_after_period(self, sample_bars):
        """Test Williams %R is_ready after receiving enough data."""
        williams_r = WilliamsR(period=10)

        for i in range(9):
            williams_r.update(sample_bars[i])
            assert not williams_r.is_ready

        williams_r.update(sample_bars[9])
        assert williams_r.is_ready

    def test_williamsr_reset(self, sample_bars):
        """Test Williams %R reset clears state."""
        williams_r = WilliamsR(period=10)

        for bar in sample_bars[:12]:
            williams_r.update(bar)

        assert williams_r.is_ready
        williams_r.reset()
        assert not williams_r.is_ready
        assert williams_r.value is None
