"""
Custom Indicator Template.

This template demonstrates how to create a custom indicator that inherits from BaseIndicator.
Replace the placeholder logic with your own indicator calculation.

Example indicators you might implement:
- Custom moving averages (e.g., Hull MA, Kaufman Adaptive MA)
- Oscillators (e.g., custom RSI variants, Stochastic modifications)
- Volume indicators (e.g., custom volume-weighted metrics)
- Volatility indicators (e.g., custom ATR variants)
- Momentum indicators (e.g., custom ROC, custom MACD)
"""

from typing import Any

from qs_trader.libraries.indicators.base import BaseIndicator
from qs_trader.services.data.models import Bar


class MyCustomIndicator(BaseIndicator):
    """
    Custom indicator template.

    This example implements a simple moving average for demonstration purposes.
    Replace this with your own indicator logic.

    Args:
        period: Lookback period for the indicator
        multiplier: Optional multiplier for the indicator value (example parameter)

    Example:
        >>> indicator = MyCustomIndicator(period=20, multiplier=1.0)
        >>> for bar in bars:
        ...     value = indicator.update(bar)
        ...     if value is not None:
        ...         print(f"Indicator value: {value}")
    """

    def __init__(self, period: int = 20, multiplier: float = 1.0, **kwargs: Any):
        """
        Initialize the custom indicator.

        Args:
            period: Lookback period for calculation
            multiplier: Multiplier to apply to the result
            **kwargs: Additional parameters for extensibility
        """
        # Store parameters
        self.period = period
        self.multiplier = multiplier

        # Initialize internal state
        self._prices: list[float] = []  # Store price history
        self._current_value: float | None = None  # Cache current value

        # Input validation
        if self.period < 1:
            raise ValueError(f"period must be >= 1, got {self.period}")
        if self.multiplier <= 0:
            raise ValueError(f"multiplier must be > 0, got {self.multiplier}")

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate indicator values for a list of bars (stateless).

        This method does NOT modify the internal state of the indicator.
        It's useful for backtesting with different parameters or bulk calculations.

        Args:
            bars: List of price bars (oldest first)

        Returns:
            List of indicator values (None during warmup period)
        """
        if not bars:
            return []

        results: list[float | None] = []

        # Extract prices from bars
        prices = [bar.close for bar in bars]

        # Calculate indicator for each position
        for i in range(len(prices)):
            if i < self.period - 1:
                # Not enough data yet
                results.append(None)
            else:
                # Calculate indicator value
                # Example: Simple moving average
                window = prices[i - self.period + 1 : i + 1]
                value = sum(window) / len(window) * self.multiplier
                results.append(value)

        return results

    def update(self, bar: Bar) -> float | None:
        """
        Update indicator with new bar and return latest value (stateful).

        This method updates the internal state and returns the current value.
        Use this for incremental/streaming calculations.

        Args:
            bar: New price bar

        Returns:
            Latest indicator value or None if not ready
        """
        # Add new price to history
        self._prices.append(bar.close)

        # Check if we have enough data
        if len(self._prices) < self.period:
            self._current_value = None
            return None

        # Calculate indicator value
        # Example: Simple moving average
        window = self._prices[-self.period :]
        self._current_value = sum(window) / len(window) * self.multiplier

        return self._current_value

    def reset(self) -> None:
        """
        Reset indicator state to initial conditions.

        Clears all internal buffers and cached values.
        Parameters (period, multiplier) remain unchanged.
        """
        self._prices.clear()
        self._current_value = None

    @property
    def value(self) -> float | None:
        """
        Get current indicator value without updating.

        Returns:
            Current indicator value or None if not ready
        """
        return self._current_value

    @property
    def is_ready(self) -> bool:
        """
        Check if indicator has enough data to produce valid values.

        Returns:
            True if indicator is ready (has enough data)
        """
        return len(self._prices) >= self.period


# Example of a multi-value indicator (returns dict)
class MyCustomMultiValueIndicator(BaseIndicator):
    """
    Custom multi-value indicator template.

    This example implements Bollinger Bands-style output with upper, middle, and lower values.
    Use this pattern when your indicator produces multiple related values.

    Args:
        period: Lookback period for the indicator
        num_std: Number of standard deviations for bands

    Example:
        >>> indicator = MyCustomMultiValueIndicator(period=20, num_std=2.0)
        >>> result = indicator.update(bar)
        >>> if result:
        ...     print(f"Upper: {result['upper']}, Middle: {result['middle']}, Lower: {result['lower']}")
    """

    def __init__(self, period: int = 20, num_std: float = 2.0, **kwargs: Any):
        """
        Initialize the custom multi-value indicator.

        Args:
            period: Lookback period for calculation
            num_std: Number of standard deviations for bands
            **kwargs: Additional parameters for extensibility
        """
        self.period = period
        self.num_std = num_std
        self._prices: list[float] = []
        self._current_value: dict[str, float] | None = None

        if self.period < 2:
            raise ValueError(f"period must be >= 2, got {self.period}")
        if self.num_std <= 0:
            raise ValueError(f"num_std must be > 0, got {self.num_std}")

    def calculate(self, bars: list[Bar]) -> list[dict[str, float] | None]:
        """
        Calculate multi-value indicator for a list of bars (stateless).

        Args:
            bars: List of price bars (oldest first)

        Returns:
            List of dicts with indicator values (None during warmup)
        """
        if not bars:
            return []

        results: list[dict[str, float] | None] = []
        prices = [bar.close for bar in bars]

        for i in range(len(prices)):
            if i < self.period - 1:
                results.append(None)
            else:
                window = prices[i - self.period + 1 : i + 1]
                middle = sum(window) / len(window)

                # Calculate standard deviation
                variance = sum((p - middle) ** 2 for p in window) / len(window)
                std_dev = variance**0.5

                results.append(
                    {
                        "upper": middle + (self.num_std * std_dev),
                        "middle": middle,
                        "lower": middle - (self.num_std * std_dev),
                    }
                )

        return results

    def update(self, bar: Bar) -> dict[str, float] | None:
        """
        Update indicator with new bar and return latest values (stateful).

        Args:
            bar: New price bar

        Returns:
            Dict with upper, middle, lower values or None if not ready
        """
        self._prices.append(bar.close)

        if len(self._prices) < self.period:
            self._current_value = None
            return None

        window = self._prices[-self.period :]
        middle = sum(window) / len(window)

        # Calculate standard deviation
        variance = sum((p - middle) ** 2 for p in window) / len(window)
        std_dev = variance**0.5

        self._current_value = {
            "upper": middle + (self.num_std * std_dev),
            "middle": middle,
            "lower": middle - (self.num_std * std_dev),
        }

        return self._current_value

    def reset(self) -> None:
        """Reset indicator state to initial conditions."""
        self._prices.clear()
        self._current_value = None

    @property
    def value(self) -> dict[str, float] | None:
        """Get current indicator values without updating."""
        return self._current_value

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return len(self._prices) >= self.period


# ============================================================================
# USAGE GUIDELINES
# ============================================================================
#
# 1. Choose the appropriate template:
#    - MyCustomIndicator: For single-value indicators (RSI, SMA, etc.)
#    - MyCustomMultiValueIndicator: For multi-value indicators (Bollinger Bands, etc.)
#
# 2. Rename the class to match your indicator:
#    - Use descriptive names: CustomRSI, HullMA, KaufmanAMA, etc.
#    - The registry name is auto-derived: CustomRSI → "custom_rsi"
#
# 3. Implement your calculation logic:
#    - Replace the example SMA/Bollinger Bands logic with your algorithm
#    - Ensure both calculate() and update() produce consistent results
#    - Handle edge cases (empty data, warmup period, etc.)
#
# 4. Add appropriate parameters:
#    - Add indicator-specific parameters to __init__
#    - Include input validation for parameters
#    - Document parameters in docstrings
#
# 5. Test your indicator:
#    - Test both stateless (calculate) and stateful (update) modes
#    - Verify warmup period handling
#    - Check reset() functionality
#    - Validate edge cases
#
# 6. Use in strategies:
#    ```python
#    from library.indicators.my_indicator import MyCustomIndicator
#
#    class MyStrategy(BaseStrategy):
#        def __init__(self):
#            self.indicator = MyCustomIndicator(period=20)
#
#        def on_bar(self, event, ctx):
#            value = self.indicator.update(event)
#            if value is not None:
#                # Use indicator value in trading logic
#                pass
#    ```
#
# ============================================================================
