"""
Unit tests for qs_trader.libraries.indicators.base module.

Tests the BaseIndicator abstract class contract:
- Abstract method requirements
- Property contracts (value, is_ready, name)
- Documentation and interface

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- pytest fixtures and parametrize
- Focus on contract compliance
"""

from typing import Any

import pytest

from qs_trader.libraries.indicators.base import BaseIndicator
from qs_trader.services.data.models import Bar

# ============================================================================
# Test Fixtures - Concrete Implementation
# ============================================================================


class ConcreteIndicator(BaseIndicator):
    """Minimal concrete implementation for testing abstract interface."""

    def __init__(self, period: int = 20, **params: Any):
        self.period = period
        self._values: list[float] = []

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """Stateless calculation."""
        return [bar.close for bar in bars]

    def update(self, bar: Bar) -> float | None:
        """Stateful update."""
        self._values.append(bar.close)
        if len(self._values) < self.period:
            return None
        return sum(self._values[-self.period :]) / self.period

    def reset(self) -> None:
        """Reset state."""
        self._values.clear()

    @property
    def value(self) -> float | None:
        """Current value."""
        if len(self._values) < self.period:
            return None
        return sum(self._values[-self.period :]) / self.period

    @property
    def is_ready(self) -> bool:
        """Ready check."""
        return len(self._values) >= self.period


class IncompleteIndicator(BaseIndicator):
    """Indicator missing required abstract methods."""

    def __init__(self, **params: Any):
        pass

    # Missing all abstract methods - should not be instantiable


# ============================================================================
# Test Fixtures - Test Data
# ============================================================================


@pytest.fixture
def sample_bars() -> list[Bar]:
    """Create sample bars for testing."""
    from datetime import datetime, timedelta

    bars = []
    base_time = datetime(2024, 1, 1)
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
# Test BaseIndicator Abstract Contract
# ============================================================================


class TestBaseIndicatorAbstractInterface:
    """Test that BaseIndicator enforces abstract method contracts."""

    def test_cannot_instantiate_base_indicator_directly(self) -> None:
        """BaseIndicator is abstract and cannot be instantiated."""
        # Arrange & Act & Assert
        with pytest.raises(TypeError) as exc_info:
            BaseIndicator()  # type: ignore[abstract]

        assert "abstract" in str(exc_info.value).lower()

    def test_cannot_instantiate_incomplete_indicator(self) -> None:
        """Indicator missing abstract methods cannot be instantiated."""
        # Arrange & Act & Assert
        with pytest.raises(TypeError) as exc_info:
            IncompleteIndicator()  # type: ignore[abstract]

        assert "abstract" in str(exc_info.value).lower()

    def test_concrete_indicator_can_be_instantiated(self) -> None:
        """Concrete indicator implementing all methods can be instantiated."""
        # Arrange & Act
        indicator = ConcreteIndicator(period=10)

        # Assert
        assert isinstance(indicator, BaseIndicator)
        assert indicator.period == 10


# ============================================================================
# Test BaseIndicator Required Methods
# ============================================================================


class TestBaseIndicatorRequiredMethods:
    """Test that concrete implementations provide required methods."""

    def test_calculate_method_exists(self) -> None:
        """Concrete indicator must implement calculate method."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act & Assert
        assert hasattr(indicator, "calculate")
        assert callable(indicator.calculate)

    def test_update_method_exists(self) -> None:
        """Concrete indicator must implement update method."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act & Assert
        assert hasattr(indicator, "update")
        assert callable(indicator.update)

    def test_reset_method_exists(self) -> None:
        """Concrete indicator must implement reset method."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act & Assert
        assert hasattr(indicator, "reset")
        assert callable(indicator.reset)

    def test_value_property_exists(self) -> None:
        """Concrete indicator must implement value property."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act & Assert
        assert hasattr(indicator, "value")
        # Property should be accessible
        _ = indicator.value

    def test_is_ready_property_exists(self) -> None:
        """Concrete indicator must implement is_ready property."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act & Assert
        assert hasattr(indicator, "is_ready")
        # Property should be accessible
        _ = indicator.is_ready


# ============================================================================
# Test BaseIndicator Name Property
# ============================================================================


class TestBaseIndicatorNameProperty:
    """Test the name property default implementation."""

    def test_name_property_strips_indicator_suffix(self) -> None:
        """name property should remove 'Indicator' suffix from class name."""

        # Arrange
        class TestIndicator(ConcreteIndicator):
            pass

        indicator = TestIndicator()

        # Act
        name = indicator.name

        # Assert
        assert name == "test"

    def test_name_property_converts_camelcase_to_snake_case(self) -> None:
        """name property should convert CamelCase to snake_case."""

        # Arrange
        class BollingerBandsIndicator(ConcreteIndicator):
            pass

        indicator = BollingerBandsIndicator()

        # Act
        name = indicator.name

        # Assert
        assert name == "bollinger_bands"

    def test_name_property_without_indicator_suffix(self) -> None:
        """name property should work with classes not ending in 'Indicator'."""

        # Arrange
        class SMA(ConcreteIndicator):
            pass

        indicator = SMA()

        # Act
        name = indicator.name

        # Assert - SMA is all caps, so snake_case conversion makes "s_m_a"
        assert name == "s_m_a"

    def test_name_property_can_be_overridden(self) -> None:
        """name property can be overridden in subclass."""

        # Arrange
        class CustomIndicator(ConcreteIndicator):
            @property
            def name(self) -> str:
                return "custom_name"

        indicator = CustomIndicator()

        # Act
        name = indicator.name

        # Assert
        assert name == "custom_name"


# ============================================================================
# Test BaseIndicator Stateful Behavior
# ============================================================================


class TestBaseIndicatorStatefulBehavior:
    """Test indicator stateful update behavior contract."""

    def test_update_modifies_internal_state(self, sample_bars: list[Bar]) -> None:
        """update should modify indicator internal state."""
        # Arrange
        indicator = ConcreteIndicator(period=5)

        # Act - update with first bar
        result1 = indicator.update(sample_bars[0])
        result2 = indicator.update(sample_bars[1])

        # Assert - state changes
        assert result1 is None  # Not ready
        assert result2 is None  # Still not ready
        assert len(indicator._values) == 2

    def test_value_property_reflects_current_state(self, sample_bars: list[Bar]) -> None:
        """value property should reflect current state without modification."""
        # Arrange
        indicator = ConcreteIndicator(period=3)
        for bar in sample_bars[:3]:
            indicator.update(bar)

        # Act
        value1 = indicator.value
        value2 = indicator.value  # Call again

        # Assert - same value, no state change
        assert value1 == value2
        assert value1 is not None

    def test_reset_clears_internal_state(self, sample_bars: list[Bar]) -> None:
        """reset should clear indicator state."""
        # Arrange
        indicator = ConcreteIndicator(period=3)
        for bar in sample_bars[:5]:
            indicator.update(bar)
        assert indicator.is_ready

        # Act
        indicator.reset()

        # Assert - state cleared
        assert not indicator.is_ready
        assert len(indicator._values) == 0
        assert indicator.value is None


# ============================================================================
# Test BaseIndicator Stateless Behavior
# ============================================================================


class TestBaseIndicatorStatelessBehavior:
    """Test indicator stateless calculate behavior contract."""

    def test_calculate_does_not_modify_state(self, sample_bars: list[Bar]) -> None:
        """calculate should not modify internal state."""
        # Arrange
        indicator = ConcreteIndicator(period=5)
        initial_values = list(indicator._values)

        # Act
        result = indicator.calculate(sample_bars[:10])

        # Assert - state unchanged
        assert indicator._values == initial_values
        assert len(result) == 10

    def test_calculate_with_empty_bars_returns_empty_list(self) -> None:
        """calculate with empty bars should return empty list."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act
        result = indicator.calculate([])

        # Assert
        assert result == []


# ============================================================================
# Test BaseIndicator is_ready Contract
# ============================================================================


class TestBaseIndicatorIsReady:
    """Test is_ready property behavior contract."""

    def test_is_ready_false_during_warmup(self, sample_bars: list[Bar]) -> None:
        """is_ready should be False during warmup period."""
        # Arrange
        indicator = ConcreteIndicator(period=5)

        # Act - update with less than period bars
        for bar in sample_bars[:3]:
            indicator.update(bar)

        # Assert
        assert not indicator.is_ready

    def test_is_ready_true_after_warmup(self, sample_bars: list[Bar]) -> None:
        """is_ready should be True after warmup period."""
        # Arrange
        indicator = ConcreteIndicator(period=5)

        # Act - update with exactly period bars
        for bar in sample_bars[:5]:
            indicator.update(bar)

        # Assert
        assert indicator.is_ready

    def test_is_ready_remains_true_after_warmup(self, sample_bars: list[Bar]) -> None:
        """is_ready should remain True after warmup complete."""
        # Arrange
        indicator = ConcreteIndicator(period=3)

        # Act - update with more than period bars
        for bar in sample_bars[:10]:
            indicator.update(bar)

        # Assert
        assert indicator.is_ready

    def test_is_ready_false_after_reset(self, sample_bars: list[Bar]) -> None:
        """is_ready should be False after reset."""
        # Arrange
        indicator = ConcreteIndicator(period=3)
        for bar in sample_bars[:5]:
            indicator.update(bar)
        assert indicator.is_ready

        # Act
        indicator.reset()

        # Assert
        assert not indicator.is_ready


# ============================================================================
# Test BaseIndicator Type Contracts
# ============================================================================


class TestBaseIndicatorTypeContracts:
    """Test that method signatures match contract types."""

    def test_calculate_accepts_bar_list(self, sample_bars: list[Bar]) -> None:
        """calculate should accept list[Bar] parameter."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act & Assert - should not raise
        result = indicator.calculate(sample_bars)
        assert isinstance(result, list)

    def test_update_accepts_single_bar(self, sample_bars: list[Bar]) -> None:
        """update should accept single Bar parameter."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act & Assert - should not raise
        result = indicator.update(sample_bars[0])
        assert result is None or isinstance(result, float)

    def test_value_returns_optional_float(self, sample_bars: list[Bar]) -> None:
        """value property should return float | None."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act
        value = indicator.value

        # Assert
        assert value is None or isinstance(value, float)

    def test_is_ready_returns_bool(self) -> None:
        """is_ready property should return bool."""
        # Arrange
        indicator = ConcreteIndicator()

        # Act
        ready = indicator.is_ready

        # Assert
        assert isinstance(ready, bool)
