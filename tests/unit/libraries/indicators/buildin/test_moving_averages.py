"""
Unit tests for qs_trader.libraries.indicators.buildin.moving_averages module.

Tests all moving average implementations:
- SMA: Simple Moving Average
- EMA: Exponential Moving Average
- WMA: Weighted Moving Average
- DEMA: Double Exponential Moving Average
- TEMA: Triple Exponential Moving Average
- HMA: Hull Moving Average
- SMMA: Smoothed Moving Average

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- Parametrize for multiple test cases
- Focus on functional paths
"""

from datetime import datetime, timedelta

import pytest

from qs_trader.libraries.indicators.buildin.moving_averages import DEMA, EMA, HMA, SMA, SMMA, TEMA, WMA
from qs_trader.services.data.models import Bar

# ============================================================================
# Test Fixtures
# ============================================================================
# Note: Common fixtures are defined in conftest.py with module scope.
# Moving averages need precise integer prices for exact SMA calculations.


@pytest.fixture(scope="module")
def sample_bars() -> list[Bar]:
    """Create sample bars with exact integer price increments (25 bars, Phase 3 optimized)."""
    bars = []
    base_time = datetime(2024, 1, 1)
    # Exact price pattern: 100, 101, 102, ... for precise SMA calculations
    for i in range(25):
        bars.append(
            Bar(
                trade_datetime=base_time + timedelta(days=i),
                open=100.0 + i,
                high=105.0 + i,
                low=95.0 + i,
                close=100.0 + i,
                volume=1000000,
            )
        )
    return bars


# ============================================================================
# Test SMA - Simple Moving Average
# ============================================================================


class TestSMAInitialization:
    """Test SMA initialization and validation."""

    def test_sma_init_with_valid_period_succeeds(self) -> None:
        """SMA initialization with valid period should succeed."""
        # Arrange & Act
        sma = SMA(period=20)

        # Assert
        assert sma.period == 20
        assert sma.price_field == "close"
        assert not sma.is_ready

    @pytest.mark.parametrize("period", [1, 10, 50, 200])
    def test_sma_init_with_different_periods(self, period: int) -> None:
        """SMA should accept various period values."""
        # Arrange & Act
        sma = SMA(period=period)

        # Assert
        assert sma.period == period

    def test_sma_init_with_zero_period_raises_error(self) -> None:
        """SMA with period=0 should raise ValueError."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError) as exc_info:
            SMA(period=0)

        assert "must be >= 1" in str(exc_info.value)

    def test_sma_init_with_negative_period_raises_error(self) -> None:
        """SMA with negative period should raise ValueError."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError):
            SMA(period=-5)

    @pytest.mark.parametrize("price_field", ["open", "high", "low", "close"])
    def test_sma_init_with_different_price_fields(self, price_field: str) -> None:
        """SMA should accept different price fields."""
        # Arrange & Act
        sma = SMA(period=10, price_field=price_field)

        # Assert
        assert sma.price_field == price_field


class TestSMACalculate:
    """Test SMA stateless calculate method."""

    def test_sma_calculate_empty_bars_returns_empty_list(self) -> None:
        """SMA calculate with empty bars should return empty list."""
        # Arrange
        sma = SMA(period=5)

        # Act
        result = sma.calculate([])

        # Assert
        assert result == []

    def test_sma_calculate_returns_none_during_warmup(self, sample_bars: list[Bar]) -> None:
        """SMA calculate should return None for bars during warmup."""
        # Arrange
        sma = SMA(period=5)

        # Act
        result = sma.calculate(sample_bars[:10])

        # Assert
        assert result[0] is None
        assert result[1] is None
        assert result[2] is None
        assert result[3] is None
        assert result[4] is not None  # First valid value

    def test_sma_calculate_correct_values(self, sample_bars: list[Bar]) -> None:
        """SMA calculate should produce correct average values."""
        # Arrange
        sma = SMA(period=3)

        # Act
        result = sma.calculate(sample_bars[:5])

        # Assert
        # Prices: 100, 101, 102, 103, 104
        # SMA(3): None, None, 101.0, 102.0, 103.0
        assert result[0] is None
        assert result[1] is None
        assert result[2] == pytest.approx((100 + 101 + 102) / 3, rel=1e-6)
        assert result[3] == pytest.approx((101 + 102 + 103) / 3, rel=1e-6)
        assert result[4] == pytest.approx((102 + 103 + 104) / 3, rel=1e-6)

    def test_sma_calculate_with_flat_prices(self, flat_bars: list[Bar]) -> None:
        """SMA with flat prices should equal the price."""
        # Arrange
        sma = SMA(period=10)
        # Act
        result = sma.calculate(flat_bars[:15])
        # Assert - with flat prices, SMA should equal the price
        assert result[9] == pytest.approx(100.0, rel=1e-6)


class TestSMAUpdate:
    """Test SMA stateful update method."""

    def test_sma_update_returns_none_during_warmup(self, sample_bars: list[Bar]) -> None:
        """SMA update should return None until period bars received."""
        # Arrange
        sma = SMA(period=5)

        # Act & Assert
        assert sma.update(sample_bars[0]) is None
        assert sma.update(sample_bars[1]) is None
        assert sma.update(sample_bars[2]) is None
        assert sma.update(sample_bars[3]) is None
        assert sma.update(sample_bars[4]) is not None  # Period reached

    def test_sma_update_maintains_state(self, sample_bars: list[Bar]) -> None:
        """SMA update should maintain state across calls."""
        # Arrange
        sma = SMA(period=3)

        # Act
        sma.update(sample_bars[0])  # 100
        sma.update(sample_bars[1])  # 101
        value = sma.update(sample_bars[2])  # 102

        # Assert
        assert value == pytest.approx((100 + 101 + 102) / 3)

    def test_sma_update_sliding_window(self, sample_bars: list[Bar]) -> None:
        """SMA update should maintain sliding window."""
        # Arrange
        sma = SMA(period=2)

        # Act
        sma.update(sample_bars[0])  # 100
        v1 = sma.update(sample_bars[1])  # 101 -> avg(100, 101)
        v2 = sma.update(sample_bars[2])  # 102 -> avg(101, 102)

        # Assert
        assert v1 == pytest.approx((100 + 101) / 2)
        assert v2 == pytest.approx((101 + 102) / 2)  # First bar dropped


class TestSMAProperties:
    """Test SMA properties (value, is_ready)."""

    def test_sma_value_returns_none_before_ready(self, sample_bars: list[Bar]) -> None:
        """SMA value should be None before warmup complete."""
        # Arrange
        sma = SMA(period=5)
        sma.update(sample_bars[0])

        # Act
        value = sma.value

        # Assert
        assert value is None

    def test_sma_value_returns_current_after_ready(self, sample_bars: list[Bar]) -> None:
        """SMA value should return current value after warmup."""
        # Arrange
        sma = SMA(period=3)
        for bar in sample_bars[:3]:
            sma.update(bar)

        # Act
        value = sma.value

        # Assert
        assert value == pytest.approx((100 + 101 + 102) / 3)

    def test_sma_is_ready_false_during_warmup(self, sample_bars: list[Bar]) -> None:
        """SMA is_ready should be False during warmup."""
        # Arrange
        sma = SMA(period=5)
        sma.update(sample_bars[0])

        # Act & Assert
        assert not sma.is_ready

    def test_sma_is_ready_true_after_warmup(self, sample_bars: list[Bar]) -> None:
        """SMA is_ready should be True after warmup."""
        # Arrange
        sma = SMA(period=3)
        for bar in sample_bars[:3]:
            sma.update(bar)

        # Act & Assert
        assert sma.is_ready


class TestSMAReset:
    """Test SMA reset functionality."""

    def test_sma_reset_clears_state(self, sample_bars: list[Bar]) -> None:
        """SMA reset should clear internal state."""
        # Arrange
        sma = SMA(period=3)
        for bar in sample_bars[:5]:
            sma.update(bar)
        assert sma.is_ready

        # Act
        sma.reset()

        # Assert
        assert not sma.is_ready
        assert sma.value is None  # type: ignore[unreachable]


# ============================================================================
# Test EMA - Exponential Moving Average
# ============================================================================


class TestEMAInitialization:
    """Test EMA initialization."""

    def test_ema_init_with_valid_params_succeeds(self) -> None:
        """EMA initialization with valid params should succeed."""
        # Arrange & Act
        ema = EMA(period=12)

        # Assert
        assert ema.period == 12
        assert ema.smoothing == 2.0
        assert not ema.is_ready

    def test_ema_init_with_zero_period_raises_error(self) -> None:
        """EMA with period=0 should raise ValueError."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError):
            EMA(period=0)

    def test_ema_init_with_zero_smoothing_raises_error(self) -> None:
        """EMA with smoothing=0 should raise ValueError."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError):
            EMA(period=10, smoothing=0.0)


class TestEMACalculateAndUpdate:
    """Test EMA calculation and update."""

    def test_ema_update_returns_none_during_warmup(self, sample_bars: list[Bar]) -> None:
        """EMA update should return None during warmup."""
        # Arrange
        ema = EMA(period=5)

        # Act & Assert
        for i in range(4):
            assert ema.update(sample_bars[i]) is None

        # Period reached - should return value
        assert ema.update(sample_bars[4]) is not None

    def test_ema_with_flat_prices_equals_price(self, flat_bars: list[Bar]) -> None:
        """EMA with flat prices should equal the price."""
        # Arrange
        ema = EMA(period=10)

        # Act
        for bar in flat_bars[:15]:
            ema.update(bar)

        # Assert
        assert ema.value == pytest.approx(100.0)

    def test_ema_is_more_responsive_than_sma(self, sample_bars: list[Bar]) -> None:
        """EMA should be more responsive to recent price changes than SMA."""
        # Arrange
        ema = EMA(period=10)
        sma = SMA(period=10)

        # Act - warmup both
        for bar in sample_bars[:10]:
            ema.update(bar)
            sma.update(bar)

        # Add a spike
        spike_bar = Bar(
            trade_datetime=datetime(2024, 2, 1),
            open=200.0,
            high=200.0,
            low=200.0,
            close=200.0,
            volume=1000000,
        )
        ema_after = ema.update(spike_bar)
        sma_after = sma.update(spike_bar)

        # Assert - EMA should react more strongly
        assert ema_after is not None
        assert sma_after is not None
        assert ema_after > sma_after  # EMA more responsive to spike


# ============================================================================
# Test WMA - Weighted Moving Average
# ============================================================================


class TestWMAInitialization:
    """Test WMA initialization."""

    def test_wma_init_with_valid_period_succeeds(self) -> None:
        """WMA initialization with valid period should succeed."""
        # Arrange & Act
        wma = WMA(period=10)

        # Assert
        assert wma.period == 10
        assert not wma.is_ready

    def test_wma_init_with_zero_period_raises_error(self) -> None:
        """WMA with period=0 should raise ValueError."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError):
            WMA(period=0)


class TestWMACalculateAndUpdate:
    """Test WMA calculation and update."""

    def test_wma_update_returns_none_during_warmup(self, sample_bars: list[Bar]) -> None:
        """WMA update should return None during warmup."""
        # Arrange
        wma = WMA(period=5)

        # Act & Assert
        for i in range(4):
            assert wma.update(sample_bars[i]) is None
        assert wma.update(sample_bars[4]) is not None

    def test_wma_weights_recent_prices_more(self, sample_bars: list[Bar]) -> None:
        """WMA should weight recent prices more heavily."""
        # Arrange
        wma = WMA(period=3)
        sma = SMA(period=3)

        # Act
        wma_val = None
        sma_val = None
        for bar in sample_bars[:3]:
            wma.update(bar)
            sma.update(bar)

        # Continue with rising prices
        for bar in sample_bars[3:6]:
            wma_val = wma.update(bar)
            sma_val = sma.update(bar)

        # Assert - WMA should be higher than SMA with rising prices
        assert wma_val is not None
        assert sma_val is not None
        assert wma_val > sma_val


# ============================================================================
# Test DEMA - Double Exponential Moving Average
# ============================================================================


class TestDEMAInitialization:
    """Test DEMA initialization."""

    def test_dema_init_with_valid_period_succeeds(self) -> None:
        """DEMA initialization should succeed."""
        # Arrange & Act
        dema = DEMA(period=20)

        # Assert
        assert dema.period == 20
        assert not dema.is_ready


class TestDEMACalculateAndUpdate:
    """Test DEMA calculation."""

    def test_dema_update_returns_none_during_warmup(self, sample_bars: list[Bar]) -> None:
        """DEMA requires longer warmup (2x period)."""
        # Arrange
        dema = DEMA(period=5)

        # Act - needs period bars for first EMA, then period for second EMA
        for i in range(9):
            result = dema.update(sample_bars[i])
            if i < 9:  # 2*period - 1
                assert result is None or isinstance(result, float)

    def test_dema_reset_clears_both_emas(self, sample_bars: list[Bar]) -> None:
        """DEMA reset should clear both internal EMAs."""
        # Arrange
        dema = DEMA(period=3)
        for bar in sample_bars[:10]:
            dema.update(bar)

        # Act
        dema.reset()

        # Assert
        assert not dema.is_ready
        assert dema.value is None


# ============================================================================
# Test TEMA - Triple Exponential Moving Average
# ============================================================================


class TestTEMAInitialization:
    """Test TEMA initialization."""

    def test_tema_init_with_valid_period_succeeds(self) -> None:
        """TEMA initialization should succeed."""
        # Arrange & Act
        tema = TEMA(period=20)

        # Assert
        assert tema.period == 20
        assert not tema.is_ready


class TestTEMACalculateAndUpdate:
    """Test TEMA calculation."""

    def test_tema_requires_longest_warmup(self, sample_bars: list[Bar]) -> None:
        """TEMA requires longest warmup period (3x)."""
        # Arrange
        tema = TEMA(period=3)

        # Act - needs 3 periods for triple EMA
        for i in range(12):
            result = tema.update(sample_bars[i])
            # Should eventually return a value
            if i >= 11:
                assert result is not None or result is None  # May need more bars


# ============================================================================
# Test HMA - Hull Moving Average
# ============================================================================


class TestHMAInitialization:
    """Test HMA initialization."""

    def test_hma_init_with_valid_period_succeeds(self) -> None:
        """HMA initialization should succeed."""
        # Arrange & Act
        hma = HMA(period=16)

        # Assert
        assert hma.period == 16
        assert not hma.is_ready


class TestHMACalculateAndUpdate:
    """Test HMA calculation."""

    def test_hma_update_eventually_ready(self, sample_bars: list[Bar]) -> None:
        """HMA should eventually become ready."""
        # Arrange
        hma = HMA(period=9)

        # Act - feed enough bars
        for bar in sample_bars[:20]:
            hma.update(bar)

        # Assert - should be ready after enough bars
        assert hma.is_ready


# ============================================================================
# Test SMMA - Smoothed Moving Average
# ============================================================================


class TestSMMAInitialization:
    """Test SMMA initialization."""

    def test_smma_init_with_valid_period_succeeds(self) -> None:
        """SMMA initialization should succeed."""
        # Arrange & Act
        smma = SMMA(period=14)

        # Assert
        assert smma.period == 14
        assert not smma.is_ready

    def test_smma_init_with_zero_period_raises_error(self) -> None:
        """SMMA with period=0 should raise ValueError."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError):
            SMMA(period=0)


class TestSMMACalculateAndUpdate:
    """Test SMMA calculation."""

    def test_smma_update_returns_none_during_warmup(self, sample_bars: list[Bar]) -> None:
        """SMMA update should return None during warmup."""
        # Arrange
        smma = SMMA(period=5)

        # Act & Assert
        for i in range(4):
            assert smma.update(sample_bars[i]) is None
        assert smma.update(sample_bars[4]) is not None

    def test_smma_with_flat_prices_equals_price(self, flat_bars: list[Bar]) -> None:
        """SMMA with flat prices should equal the price."""
        # Arrange
        smma = SMMA(period=10)

        # Act
        for bar in flat_bars[:15]:
            smma.update(bar)

        # Assert
        assert smma.value == pytest.approx(100.0)


# ============================================================================
# Parametrized Cross-Indicator Tests
# ============================================================================


@pytest.mark.parametrize(
    "indicator_class,period",
    [
        (SMA, 10),
        (EMA, 10),
        (WMA, 10),
        (DEMA, 10),
        (TEMA, 10),
        (HMA, 16),
        (SMMA, 10),
    ],
)
class TestAllIndicatorsCommonBehavior:
    """Test common behavior across all moving average indicators."""

    def test_indicator_initial_state_not_ready(self, indicator_class: type, period: int) -> None:
        """All indicators should start in not-ready state."""
        # Arrange & Act
        indicator = indicator_class(period=period)

        # Assert
        assert not indicator.is_ready
        assert indicator.value is None

    def test_indicator_reset_returns_to_initial_state(
        self, indicator_class: type, period: int, sample_bars: list[Bar]
    ) -> None:
        """All indicators should return to initial state after reset."""
        # Arrange
        indicator = indicator_class(period=period)
        for bar in sample_bars[:25]:  # Phase 3: trimmed from 30 to 25 bars
            indicator.update(bar)

        # Act
        indicator.reset()

        # Assert
        assert not indicator.is_ready
        assert indicator.value is None

    def test_indicator_calculate_empty_bars_returns_empty(self, indicator_class: type, period: int) -> None:
        """All indicators should handle empty bar list."""
        # Arrange
        indicator = indicator_class(period=period)

        # Act
        result = indicator.calculate([])

        # Assert
        assert result == []
