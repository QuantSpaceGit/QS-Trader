"""
Moving Average Indicators.

Implementations of various moving average types:
- SMA: Simple Moving Average
- EMA: Exponential Moving Average
- WMA: Weighted Moving Average
- DEMA: Double Exponential Moving Average
- TEMA: Triple Exponential Moving Average
- HMA: Hull Moving Average
- SMMA: Smoothed Moving Average (also known as RMA)

All indicators inherit from BaseIndicator and support both stateful
and stateless computation modes.
"""

from collections import deque
from typing import Any

from qs_trader.libraries.indicators.base import BaseIndicator, IndicatorPlacement
from qs_trader.services.data.models import Bar


class SMA(BaseIndicator):
    """
    Simple Moving Average.

    Calculates the arithmetic mean of the last N periods.

    Formula:
        SMA = (P1 + P2 + ... + PN) / N

    Parameters:
        period: Number of bars to average
        price_field: Which price to use (default: "close")

    Example:
        >>> sma = SMA(period=20)
        >>> for bar in bars:
        ...     value = sma.update(bar)
        ...     if value is not None:
        ...         print(f"SMA(20): {value:.2f}")
    """

    # Visualization metadata
    placement = IndicatorPlacement.OVERLAY
    default_color = "#667eea"

    def __init__(self, period: int, price_field: str = "close", **params: Any):
        """
        Initialize SMA indicator.

        Args:
            period: Number of bars in the moving average
            price_field: Which price field to use ("open", "high", "low", "close")
            **params: Additional parameters (ignored)

        Raises:
            ValueError: If period < 1
        """
        if period < 1:
            raise ValueError(f"Period must be >= 1, got {period}")

        self.period = period
        self.price_field = price_field
        self._prices: deque[float] = deque(maxlen=period)
        self._sum = 0.0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate SMA values for all bars (stateless).

        Optimized using running sum to avoid O(n*period) complexity.
        Now O(n) with constant-time updates per bar.

        Args:
            bars: List of price bars

        Returns:
            List of SMA values (None for warmup period)
        """
        if not bars:
            return []

        prices = [getattr(bar, self.price_field) for bar in bars]
        result: list[float | None] = []
        running_sum = 0.0

        for i in range(len(prices)):
            running_sum += prices[i]

            if i < self.period - 1:
                result.append(None)
            else:
                if i >= self.period:
                    # Subtract the price that's leaving the window
                    running_sum -= prices[i - self.period]

                result.append(running_sum / self.period)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update SMA with new bar (stateful).

        Args:
            bar: New price bar

        Returns:
            Current SMA value or None if not ready
        """
        price = getattr(bar, self.price_field)

        # Remove old value if deque is full
        if len(self._prices) == self.period:
            self._sum -= self._prices[0]

        # Add new value
        self._prices.append(price)
        self._sum += price

        # Return None during warmup
        if len(self._prices) < self.period:
            return None

        return self._sum / self.period

    def update_value(self, value: float) -> float | None:
        """
        Update SMA with a raw float value (stateful).

        Useful for chaining indicators without creating synthetic Bar objects.

        Args:
            value: Raw numeric value

        Returns:
            Current SMA value or None if not ready
        """
        # Remove old value if deque is full
        if len(self._prices) == self.period:
            self._sum -= self._prices[0]

        # Add new value
        self._prices.append(value)
        self._sum += value

        # Return None during warmup
        if len(self._prices) < self.period:
            return None

        return self._sum / self.period

    def reset(self) -> None:
        """Reset indicator state."""
        self._prices.clear()
        self._sum = 0.0

    @property
    def value(self) -> float | None:
        """Get current SMA value without updating."""
        if len(self._prices) < self.period:
            return None
        return self._sum / self.period

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return len(self._prices) >= self.period


class EMA(BaseIndicator):
    """
    Exponential Moving Average.

    Applies exponentially decreasing weights to older prices.
    More responsive to recent price changes than SMA.

    Formula:
        EMA(t) = α * Price(t) + (1 - α) * EMA(t-1)
        where α = smoothing / (1 + period)

    Parameters:
        period: Number of bars for EMA calculation
        smoothing: Smoothing factor (default: 2)
        price_field: Which price to use (default: "close")

    Example:
        >>> ema = EMA(period=12)
        >>> for bar in bars:
        ...     value = ema.update(bar)
        ...     if value is not None:
        ...         print(f"EMA(12): {value:.2f}")
    """

    # Visualization metadata
    placement = IndicatorPlacement.OVERLAY
    default_color = "#764ba2"

    def __init__(self, period: int, smoothing: float = 2.0, price_field: str = "close", **params: Any):
        """
        Initialize EMA indicator.

        Args:
            period: Number of bars for EMA
            smoothing: Smoothing factor (typically 2)
            price_field: Which price field to use
            **params: Additional parameters (ignored)

        Raises:
            ValueError: If period < 1 or smoothing <= 0
        """
        if period < 1:
            raise ValueError(f"Period must be >= 1, got {period}")
        if smoothing <= 0:
            raise ValueError(f"Smoothing must be > 0, got {smoothing}")

        self.period = period
        self.smoothing = smoothing
        self.price_field = price_field
        self.multiplier = smoothing / (1 + period)

        self._ema: float | None = None
        self._count = 0
        self._sma_sum = 0.0  # For initial SMA calculation

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """Calculate EMA values for all bars (stateless)."""
        if not bars:
            return []

        prices = [getattr(bar, self.price_field) for bar in bars]
        result: list[float | None] = []
        ema: float | None = None

        for i, price in enumerate(prices):
            if i < self.period - 1:
                result.append(None)
            elif i == self.period - 1:
                # Initialize with SMA
                ema = sum(prices[: self.period]) / self.period
                result.append(ema)
            else:
                # EMA formula
                assert ema is not None  # Type narrowing
                ema = (price - ema) * self.multiplier + ema
                result.append(ema)

        return result

    def update(self, bar: Bar) -> float | None:
        """Update EMA with new bar (stateful)."""
        price = getattr(bar, self.price_field)
        self._count += 1

        if self._count < self.period:
            # Accumulate for initial SMA
            self._sma_sum += price
            return None
        elif self._count == self.period:
            # Initialize EMA with SMA
            self._sma_sum += price
            self._ema = self._sma_sum / self.period
            return self._ema
        else:
            # Calculate EMA
            self._ema = (price - self._ema) * self.multiplier + self._ema
            return self._ema

    def update_value(self, value: float) -> float | None:
        """
        Update EMA with a raw float value (stateful).

        Useful for chaining indicators without creating synthetic Bar objects.

        Args:
            value: Raw numeric value

        Returns:
            Current EMA value or None if not ready
        """
        self._count += 1

        if self._count < self.period:
            # Accumulate for initial SMA
            self._sma_sum += value
            return None
        elif self._count == self.period:
            # Initialize EMA with SMA
            self._sma_sum += value
            self._ema = self._sma_sum / self.period
            return self._ema
        else:
            # Calculate EMA
            assert self._ema is not None  # Type narrowing
            self._ema = (value - self._ema) * self.multiplier + self._ema
            return self._ema

    def reset(self) -> None:
        """Reset indicator state."""
        self._ema = None
        self._count = 0
        self._sma_sum = 0.0

    @property
    def value(self) -> float | None:
        """Get current EMA value without updating."""
        return self._ema

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._ema is not None


class WMA(BaseIndicator):
    """
    Weighted Moving Average.

    Applies linearly increasing weights to more recent prices.
    Weight increases linearly from 1 to N.

    Formula:
        WMA = (P1*1 + P2*2 + ... + PN*N) / (1 + 2 + ... + N)
        where N is the period

    Parameters:
        period: Number of bars to include
        price_field: Which price to use (default: "close")

    Example:
        >>> wma = WMA(period=10)
        >>> for bar in bars:
        ...     value = wma.update(bar)
        ...     if value is not None:
        ...         print(f"WMA(10): {value:.2f}")
    """

    def __init__(self, period: int, price_field: str = "close", **params: Any):
        """
        Initialize WMA indicator.

        Args:
            period: Number of bars in the weighted average
            price_field: Which price field to use
            **params: Additional parameters (ignored)

        Raises:
            ValueError: If period < 1
        """
        if period < 1:
            raise ValueError(f"Period must be >= 1, got {period}")

        self.period = period
        self.price_field = price_field
        self._prices: deque[float] = deque(maxlen=period)
        # Pre-calculate weight sum: 1+2+3+...+N = N*(N+1)/2
        self._weight_sum = period * (period + 1) // 2

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """Calculate WMA values for all bars (stateless)."""
        if not bars:
            return []

        prices = [getattr(bar, self.price_field) for bar in bars]
        result: list[float | None] = []

        for i in range(len(prices)):
            if i < self.period - 1:
                result.append(None)
            else:
                window = prices[i - self.period + 1 : i + 1]
                weighted_sum = sum(price * (j + 1) for j, price in enumerate(window))
                wma = weighted_sum / self._weight_sum
                result.append(wma)

        return result

    def update(self, bar: Bar) -> float | None:
        """Update WMA with new bar (stateful)."""
        price = getattr(bar, self.price_field)
        self._prices.append(price)

        if len(self._prices) < self.period:
            return None

        # Calculate weighted sum
        weighted_sum = sum(p * (i + 1) for i, p in enumerate(self._prices))
        return weighted_sum / self._weight_sum

    def reset(self) -> None:
        """Reset indicator state."""
        self._prices.clear()

    @property
    def value(self) -> float | None:
        """Get current WMA value without updating."""
        if len(self._prices) < self.period:
            return None
        weighted_sum = sum(p * (i + 1) for i, p in enumerate(self._prices))
        return weighted_sum / self._weight_sum

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return len(self._prices) >= self.period


class DEMA(BaseIndicator):
    """
    Double Exponential Moving Average.

    Reduces lag by applying EMA twice with a specific formula.
    More responsive than single EMA.

    Formula:
        DEMA = 2 * EMA(price) - EMA(EMA(price))

    Parameters:
        period: Number of bars for EMA calculations
        price_field: Which price to use (default: "close")

    Example:
        >>> dema = DEMA(period=20)
        >>> for bar in bars:
        ...     value = dema.update(bar)
        ...     if value is not None:
        ...         print(f"DEMA(20): {value:.2f}")
    """

    def __init__(self, period: int, price_field: str = "close", **params: Any):
        """
        Initialize DEMA indicator.

        Args:
            period: Number of bars for EMA calculations
            price_field: Which price field to use
            **params: Additional parameters (ignored)
        """
        self.period = period
        self.price_field = price_field
        self._ema1 = EMA(period=period, price_field=price_field)
        self._ema2 = EMA(period=period, price_field=price_field)
        self._ema1_values: deque[float] = deque()

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """Calculate DEMA values for all bars (stateless)."""
        if not bars:
            return []

        # Calculate first EMA
        ema1_values = self._ema1.calculate(bars)

        # Create synthetic bars for second EMA
        result: list[float | None] = []
        ema2_calculator = EMA(period=self.period, price_field=self.price_field)

        for ema1_val in ema1_values:
            if ema1_val is None:
                result.append(None)
                continue

            # Create synthetic bar with EMA1 value as close
            synthetic_bar = type("obj", (object,), {self.price_field: ema1_val})()
            ema2_val = ema2_calculator.update(synthetic_bar)  # pyright: ignore[reportArgumentType]

            if ema2_val is None:
                result.append(None)
            else:
                dema = 2 * ema1_val - ema2_val
                result.append(dema)

        return result

    def update(self, bar: Bar) -> float | None:
        """Update DEMA with new bar (stateful)."""
        # Update first EMA
        ema1_val = self._ema1.update(bar)

        if ema1_val is None:
            return None

        # Update second EMA with first EMA's value
        synthetic_bar = type("obj", (object,), {self.price_field: ema1_val})()
        ema2_val = self._ema2.update(synthetic_bar)  # pyright: ignore[reportArgumentType]

        if ema2_val is None:
            return None

        return 2 * ema1_val - ema2_val

    def reset(self) -> None:
        """Reset indicator state."""
        self._ema1.reset()
        self._ema2.reset()
        self._ema1_values.clear()

    @property
    def value(self) -> float | None:
        """Get current DEMA value without updating."""
        ema1_val = self._ema1.value
        ema2_val = self._ema2.value

        if ema1_val is None or ema2_val is None:
            return None

        return 2 * ema1_val - ema2_val

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._ema1.is_ready and self._ema2.is_ready


class TEMA(BaseIndicator):
    """
    Triple Exponential Moving Average.

    Further reduces lag by applying EMA three times.
    Even more responsive than DEMA.

    Formula:
        TEMA = 3*EMA(price) - 3*EMA(EMA(price)) + EMA(EMA(EMA(price)))

    Parameters:
        period: Number of bars for EMA calculations
        price_field: Which price to use (default: "close")

    Example:
        >>> tema = TEMA(period=20)
        >>> for bar in bars:
        ...     value = tema.update(bar)
        ...     if value is not None:
        ...         print(f"TEMA(20): {value:.2f}")
    """

    def __init__(self, period: int, price_field: str = "close", **params: Any):
        """
        Initialize TEMA indicator.

        Args:
            period: Number of bars for EMA calculations
            price_field: Which price field to use
            **params: Additional parameters (ignored)
        """
        self.period = period
        self.price_field = price_field
        self._ema1 = EMA(period=period, price_field=price_field)
        self._ema2 = EMA(period=period, price_field=price_field)
        self._ema3 = EMA(period=period, price_field=price_field)

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """Calculate TEMA values for all bars (stateless)."""
        if not bars:
            return []

        # Calculate first EMA
        ema1_values = self._ema1.calculate(bars)

        # Calculate second EMA (EMA of EMA)
        result: list[float | None] = []
        ema2_calc = EMA(period=self.period, price_field=self.price_field)
        ema3_calc = EMA(period=self.period, price_field=self.price_field)
        ema2_values: list[float | None] = []

        for ema1_val in ema1_values:
            if ema1_val is None:
                ema2_values.append(None)
                continue
            synthetic_bar = type("obj", (object,), {self.price_field: ema1_val})()
            ema2_val = ema2_calc.update(synthetic_bar)  # pyright: ignore[reportArgumentType]
            ema2_values.append(ema2_val)

        # Calculate third EMA (EMA of EMA of EMA)
        for i, ema2_val in enumerate(ema2_values):
            if ema2_val is None:
                result.append(None)
                continue

            synthetic_bar = type("obj", (object,), {self.price_field: ema2_val})()
            ema3_val = ema3_calc.update(synthetic_bar)  # pyright: ignore[reportArgumentType]

            if ema3_val is None or ema1_values[i] is None:
                result.append(None)
            else:
                # Type narrowing: all values are not None
                ema1_current = ema1_values[i]
                assert ema1_current is not None
                tema = 3 * ema1_current - 3 * ema2_val + ema3_val
                result.append(tema)

        return result

    def update(self, bar: Bar) -> float | None:
        """Update TEMA with new bar (stateful)."""
        # Update first EMA
        ema1_val = self._ema1.update(bar)
        if ema1_val is None:
            return None

        # Update second EMA
        synthetic_bar1 = type("obj", (object,), {self.price_field: ema1_val})()
        ema2_val = self._ema2.update(synthetic_bar1)  # pyright: ignore[reportArgumentType]
        if ema2_val is None:
            return None

        # Update third EMA
        synthetic_bar2 = type("obj", (object,), {self.price_field: ema2_val})()
        ema3_val = self._ema3.update(synthetic_bar2)  # pyright: ignore[reportArgumentType]
        if ema3_val is None:
            return None

        return 3 * ema1_val - 3 * ema2_val + ema3_val

    def reset(self) -> None:
        """Reset indicator state."""
        self._ema1.reset()
        self._ema2.reset()
        self._ema3.reset()

    @property
    def value(self) -> float | None:
        """Get current TEMA value without updating."""
        ema1_val = self._ema1.value
        ema2_val = self._ema2.value
        ema3_val = self._ema3.value

        if ema1_val is None or ema2_val is None or ema3_val is None:
            return None

        return 3 * ema1_val - 3 * ema2_val + ema3_val

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._ema1.is_ready and self._ema2.is_ready and self._ema3.is_ready


class HMA(BaseIndicator):
    """
    Hull Moving Average.

    Highly responsive moving average with minimal lag.
    Combines WMA calculations for smoothness and responsiveness.

    Formula:
        HMA = WMA(2*WMA(price, period/2) - WMA(price, period), sqrt(period))

    Parameters:
        period: Number of bars for HMA calculation
        price_field: Which price to use (default: "close")

    Example:
        >>> hma = HMA(period=16)
        >>> for bar in bars:
        ...     value = hma.update(bar)
        ...     if value is not None:
        ...         print(f"HMA(16): {value:.2f}")
    """

    def __init__(self, period: int, price_field: str = "close", **params: Any):
        """
        Initialize HMA indicator.

        Args:
            period: Number of bars for HMA calculation
            price_field: Which price field to use
            **params: Additional parameters (ignored)
        """
        import math

        self.period = period
        self.price_field = price_field
        self._half_period = period // 2
        self._sqrt_period = int(math.sqrt(period))

        self._wma_half = WMA(period=self._half_period, price_field=price_field)
        self._wma_full = WMA(period=period, price_field=price_field)
        self._wma_final = WMA(period=self._sqrt_period, price_field=price_field)

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """Calculate HMA values for all bars (stateless)."""
        if not bars:
            return []

        # Calculate WMAs
        wma_half_vals = self._wma_half.calculate(bars)
        wma_full_vals = self._wma_full.calculate(bars)

        # Calculate raw HMA values (2*WMA_half - WMA_full)
        result: list[float | None] = []
        wma_final_calc = WMA(period=self._sqrt_period, price_field=self.price_field)

        for wma_h, wma_f in zip(wma_half_vals, wma_full_vals):
            if wma_h is None or wma_f is None:
                result.append(None)
                continue

            raw_hma = 2 * wma_h - wma_f
            # Feed into final WMA
            synthetic_bar = type("obj", (object,), {self.price_field: raw_hma})()
            hma_val = wma_final_calc.update(synthetic_bar)  # pyright: ignore[reportArgumentType]
            result.append(hma_val)

        return result

    def update(self, bar: Bar) -> float | None:
        """Update HMA with new bar (stateful)."""
        # Update both WMAs
        wma_half_val = self._wma_half.update(bar)
        wma_full_val = self._wma_full.update(bar)

        if wma_half_val is None or wma_full_val is None:
            return None

        # Calculate raw HMA
        raw_hma = 2 * wma_half_val - wma_full_val

        # Feed into final WMA
        synthetic_bar = type("obj", (object,), {self.price_field: raw_hma})()
        return self._wma_final.update(synthetic_bar)  # pyright: ignore[reportArgumentType]

    def reset(self) -> None:
        """Reset indicator state."""
        self._wma_half.reset()
        self._wma_full.reset()
        self._wma_final.reset()

    @property
    def value(self) -> float | None:
        """Get current HMA value without updating."""
        return self._wma_final.value

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._wma_final.is_ready


class SMMA(BaseIndicator):
    """
    Smoothed Moving Average (also known as RMA - Running Moving Average).

    Similar to EMA but with different smoothing factor.
    Provides very smooth trend representation.

    Formula:
        SMMA(t) = (SMMA(t-1) * (N-1) + Price(t)) / N
        where N is the period

    Parameters:
        period: Number of bars for smoothing
        price_field: Which price to use (default: "close")

    Example:
        >>> smma = SMMA(period=14)
        >>> for bar in bars:
        ...     value = smma.update(bar)
        ...     if value is not None:
        ...         print(f"SMMA(14): {value:.2f}")
    """

    def __init__(self, period: int, price_field: str = "close", **params: Any):
        """
        Initialize SMMA indicator.

        Args:
            period: Number of bars for smoothing
            price_field: Which price field to use
            **params: Additional parameters (ignored)

        Raises:
            ValueError: If period < 1
        """
        if period < 1:
            raise ValueError(f"Period must be >= 1, got {period}")

        self.period = period
        self.price_field = price_field
        self._smma: float | None = None
        self._count = 0
        self._sma_sum = 0.0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """Calculate SMMA values for all bars (stateless)."""
        if not bars:
            return []

        prices = [getattr(bar, self.price_field) for bar in bars]
        result: list[float | None] = []
        smma: float | None = None

        for i, price in enumerate(prices):
            if i < self.period - 1:
                result.append(None)
            elif i == self.period - 1:
                # Initialize with SMA
                smma = sum(prices[: self.period]) / self.period
                result.append(smma)
            else:
                # SMMA formula
                assert smma is not None  # Type narrowing
                smma = (smma * (self.period - 1) + price) / self.period
                result.append(smma)

        return result

    def update(self, bar: Bar) -> float | None:
        """Update SMMA with new bar (stateful)."""
        price = getattr(bar, self.price_field)
        self._count += 1

        if self._count < self.period:
            # Accumulate for initial SMA
            self._sma_sum += price
            return None
        elif self._count == self.period:
            # Initialize SMMA with SMA
            self._sma_sum += price
            self._smma = self._sma_sum / self.period
            return self._smma
        else:
            # Calculate SMMA
            assert self._smma is not None  # Type narrowing
            self._smma = (self._smma * (self.period - 1) + price) / self.period
            return self._smma

    def reset(self) -> None:
        """Reset indicator state."""
        self._smma = None
        self._count = 0
        self._sma_sum = 0.0

    @property
    def value(self) -> float | None:
        """Get current SMMA value without updating."""
        return self._smma

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._smma is not None
