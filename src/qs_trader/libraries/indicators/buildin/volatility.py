"""
Volatility Indicators.

Implementations of volatility-based technical indicators:
- ATR: Average True Range
- BollingerBands: Bollinger Bands (upper, middle, lower)
- StdDev: Standard Deviation

All indicators inherit from BaseIndicator and support both stateful
and stateless computation modes.
"""

import math
from collections import deque

from qs_trader.libraries.indicators.base import BaseIndicator, IndicatorPlacement
from qs_trader.libraries.indicators.buildin.moving_averages import SMA
from qs_trader.services.data.models import Bar


class ATR(BaseIndicator):
    """
    Average True Range.

    Measures market volatility by calculating the average of true ranges.
    True Range is the greatest of:
    1. Current High - Current Low
    2. |Current High - Previous Close|
    3. |Current Low - Previous Close|

    ATR uses Wilder's smoothing (same as RSI) for the moving average.

    Formula:
        TR = max(High - Low, |High - Previous Close|, |Low - Previous Close|)
        ATR = Wilder's MA of TR over period

    Traditional interpretation:
        High ATR: High volatility, wider stops
        Low ATR: Low volatility, tighter stops
        Rising ATR: Increasing volatility
        Falling ATR: Decreasing volatility

    Parameters:
        period: Number of bars for ATR calculation (default: 14)

    Example:
        >>> atr = ATR(period=14)
        >>> for bar in bars:
        ...     value = atr.update(bar)
        ...     if value is not None:
        ...         print(f"ATR: {value:.2f}")
    """

    # Visualization metadata
    placement = IndicatorPlacement.SUBPLOT
    value_range = (0.0, None)
    default_color = "#f093fb"

    def __init__(self, period: int = 14):
        """
        Initialize ATR indicator.

        Args:
            period: Number of bars for calculation (default: 14)

        Raises:
            ValueError: If period is less than 1
        """
        if period < 1:
            raise ValueError("Period must be at least 1")

        self.period = period
        self._prev_close: float | None = None
        self._atr: float | None = None
        self._count = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate ATR for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of ATR values (None until enough data)
        """
        if not bars:
            return []

        result: list[float | None] = []
        atr: float | None = None

        for i, bar in enumerate(bars):
            if i == 0:
                # First bar, no previous close
                result.append(None)
                continue

            # Calculate True Range
            high_low = bar.high - bar.low
            high_close = abs(bar.high - bars[i - 1].close)
            low_close = abs(bar.low - bars[i - 1].close)
            true_range = max(high_low, high_close, low_close)

            if i < self.period:
                # Accumulating for initial ATR
                if atr is None:
                    atr = true_range
                else:
                    atr += true_range
                result.append(None)
            elif i == self.period:
                # Calculate initial ATR (simple average)
                assert atr is not None
                atr = (atr + true_range) / self.period
                result.append(atr)
            else:
                # Use Wilder's smoothing
                assert atr is not None
                atr = (atr * (self.period - 1) + true_range) / self.period
                result.append(atr)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update ATR with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Current ATR value or None if not enough data
        """
        self._count += 1

        # First bar, just store close
        if self._count == 1:
            self._prev_close = bar.close
            return None

        # Calculate True Range
        assert self._prev_close is not None
        high_low = bar.high - bar.low
        high_close = abs(bar.high - self._prev_close)
        low_close = abs(bar.low - self._prev_close)
        true_range = max(high_low, high_close, low_close)

        # Update previous close
        self._prev_close = bar.close

        if self._count <= self.period:
            # Accumulating for initial ATR
            if self._atr is None:
                self._atr = true_range
            else:
                self._atr += true_range

            if self._count == self.period:
                # Calculate initial ATR (simple average)
                self._atr /= self.period
                return self._atr

            return None
        else:
            # Use Wilder's smoothing
            assert self._atr is not None
            self._atr = (self._atr * (self.period - 1) + true_range) / self.period
            return self._atr

    def reset(self) -> None:
        """Reset indicator state."""
        self._prev_close = None
        self._atr = None
        self._count = 0

    @property
    def value(self) -> float | None:
        """
        Get current ATR value.

        Returns:
            Current ATR value or None if not enough data
        """
        if self._count < self.period:
            return None
        return self._atr

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._count >= self.period


class StdDev(BaseIndicator):
    """
    Standard Deviation.

    Measures the dispersion of prices around the mean (average).
    Higher values indicate higher volatility.

    Formula:
        StdDev = sqrt(sum((price - mean)^2) / n)

    Parameters:
        period: Number of bars for calculation (default: 20)
        price_field: Which price to use (default: "close")
        ddof: Delta degrees of freedom (0 for population, 1 for sample) (default: 0)

    Example:
        >>> stddev = StdDev(period=20)
        >>> for bar in bars:
        ...     value = stddev.update(bar)
        ...     if value is not None:
        ...         print(f"StdDev: {value:.2f}")
    """

    def __init__(self, period: int = 20, price_field: str = "close", ddof: int = 0):
        """
        Initialize Standard Deviation indicator.

        Args:
            period: Number of bars for calculation (default: 20)
            price_field: Which price field to use (default: "close")
            ddof: Delta degrees of freedom (default: 0)

        Raises:
            ValueError: If period is less than 1 or ddof is invalid
        """
        if period < 1:
            raise ValueError("Period must be at least 1")
        if not 0 <= ddof < period:
            raise ValueError(f"ddof must be between 0 and {period - 1}, got {ddof}")

        self.period = period
        self.price_field = price_field
        self.ddof = ddof
        self._prices: deque[float] = deque(maxlen=period)
        self._count = 0
        self._sum = 0.0  # Running sum for O(1) mean calculation
        self._sum_sq = 0.0  # Running sum of squares for O(1) variance

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate standard deviation for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of standard deviation values (None until enough data)
        """
        if not bars:
            return []

        result: list[float | None] = []
        running_sum = 0.0
        running_sum_sq = 0.0

        for i, bar in enumerate(bars):
            price = getattr(bar, self.price_field)
            running_sum += price
            running_sum_sq += price * price

            if i < self.period - 1:
                result.append(None)
                continue

            # Subtract old value if window is sliding
            if i >= self.period:
                old_price = getattr(bars[i - self.period], self.price_field)
                running_sum -= old_price
                running_sum_sq -= old_price * old_price

            # Calculate mean
            mean = running_sum / self.period

            # Calculate variance: Var(X) = E[X²] - E[X]²
            # E[X²] = sum_sq / n, E[X]² = mean²
            variance = (running_sum_sq / self.period - mean * mean) * self.period / (self.period - self.ddof)
            # Guard against floating point errors
            variance = max(0.0, variance)

            # Calculate standard deviation
            stddev = math.sqrt(variance)
            result.append(stddev)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update standard deviation with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Current standard deviation value or None if not enough data
        """
        price = getattr(bar, self.price_field)

        # If deque is at capacity, subtract the old value that will be evicted
        if len(self._prices) == self.period:
            old_price = self._prices[0]
            self._sum -= old_price
            self._sum_sq -= old_price * old_price

        self._prices.append(price)
        self._sum += price
        self._sum_sq += price * price
        self._count += 1

        if self._count < self.period:
            return None

        # Calculate mean
        mean = self._sum / self.period

        # Calculate variance: Var(X) = E[X²] - E[X]²
        variance = (self._sum_sq / self.period - mean * mean) * self.period / (self.period - self.ddof)
        # Guard against floating point errors
        variance = max(0.0, variance)

        # Calculate standard deviation
        stddev = math.sqrt(variance)
        return stddev

    def reset(self) -> None:
        """Reset indicator state."""
        self._prices.clear()
        self._count = 0
        self._sum = 0.0
        self._sum_sq = 0.0

    @property
    def value(self) -> float | None:
        """
        Get current standard deviation value.

        Returns:
            Current standard deviation value or None if not enough data
        """
        if self._count < self.period:
            return None

        # Calculate mean using running sum
        mean = self._sum / self.period

        # Calculate variance: Var(X) = E[X²] - E[X]²
        variance = (self._sum_sq / self.period - mean * mean) * self.period / (self.period - self.ddof)
        # Guard against floating point errors
        variance = max(0.0, variance)

        # Calculate standard deviation
        stddev = math.sqrt(variance)
        return stddev

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._count >= self.period


class BollingerBands(BaseIndicator):
    """
    Bollinger Bands.

    Volatility bands placed above and below a moving average.
    Bands widen during volatile periods and contract during quiet periods.

    Components:
        - Middle Band: Simple Moving Average (SMA)
        - Upper Band: Middle Band + (num_std * Standard Deviation)
        - Lower Band: Middle Band - (num_std * Standard Deviation)

    Formula:
        Middle = SMA(close, period)
        Upper = Middle + (num_std * StdDev)
        Lower = Middle - (num_std * StdDev)

    Traditional interpretation:
        Price near upper band: Overbought
        Price near lower band: Oversold
        Bands narrowing: Low volatility (squeeze)
        Bands widening: High volatility (expansion)

    Parameters:
        period: Number of bars for SMA and StdDev (default: 20)
        num_std: Number of standard deviations (default: 2.0)
        price_field: Which price to use (default: "close")

    Example:
        >>> bb = BollingerBands(period=20, num_std=2.0)
        >>> for bar in bars:
        ...     value = bb.update(bar)
        ...     if value is not None:
        ...         print(f"BB: upper={value['upper']:.2f}, "
        ...               f"middle={value['middle']:.2f}, "
        ...               f"lower={value['lower']:.2f}")
    """

    def __init__(self, period: int = 20, num_std: float = 2.0, price_field: str = "close"):
        """
        Initialize Bollinger Bands indicator.

        Args:
            period: Number of bars for calculation (default: 20)
            num_std: Number of standard deviations (default: 2.0)
            price_field: Which price field to use (default: "close")

        Raises:
            ValueError: If period is less than 1 or num_std is negative
        """
        if period < 1:
            raise ValueError("Period must be at least 1")
        if num_std < 0:
            raise ValueError("Number of standard deviations must be non-negative")

        self.period = period
        self.num_std = num_std
        self.price_field = price_field
        self._sma = SMA(period=period, price_field=price_field)
        self._stddev = StdDev(period=period, price_field=price_field, ddof=0)

    def calculate(self, bars: list[Bar]) -> list[dict[str, float] | None]:
        """
        Calculate Bollinger Bands for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of dicts with 'upper', 'middle', 'lower' keys (None until enough data)
        """
        if not bars:
            return []

        # Create temporary indicator instances for stateless calculation
        # to avoid corrupting the streaming state in _sma/_stddev
        # Match the ddof configuration from the instance StdDev
        sma_calc = SMA(period=self.period, price_field=self.price_field)
        stddev_calc = StdDev(period=self.period, price_field=self.price_field, ddof=self._stddev.ddof)

        sma_values = sma_calc.calculate(bars)
        stddev_values = stddev_calc.calculate(bars)

        result: list[dict[str, float] | None] = []

        for sma, stddev in zip(sma_values, stddev_values):
            if sma is None or stddev is None:
                result.append(None)
            else:
                middle = sma
                upper = middle + (self.num_std * stddev)
                lower = middle - (self.num_std * stddev)
                result.append({"upper": upper, "middle": middle, "lower": lower})

        return result

    def update(self, bar: Bar) -> dict[str, float] | None:
        """
        Update Bollinger Bands with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Dict with 'upper', 'middle', 'lower' keys or None if not enough data
        """
        sma_value = self._sma.update(bar)
        stddev_value = self._stddev.update(bar)

        if sma_value is None or stddev_value is None:
            return None

        middle = sma_value
        upper = middle + (self.num_std * stddev_value)
        lower = middle - (self.num_std * stddev_value)

        return {"upper": upper, "middle": middle, "lower": lower}

    def reset(self) -> None:
        """Reset indicator state."""
        self._sma.reset()
        self._stddev.reset()

    @property
    def value(self) -> dict[str, float] | None:
        """
        Get current Bollinger Bands value.

        Returns:
            Dict with 'upper', 'middle', 'lower' keys or None if not enough data
        """
        sma_value = self._sma.value
        stddev_value = self._stddev.value

        if sma_value is None or stddev_value is None:
            return None

        middle = sma_value
        upper = middle + (self.num_std * stddev_value)
        lower = middle - (self.num_std * stddev_value)

        return {"upper": upper, "middle": middle, "lower": lower}

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._sma.is_ready and self._stddev.is_ready
