"""
Base Indicator Abstract Class.

All indicators must inherit from BaseIndicator and implement the required methods.
Indicators are stateful objects that calculate technical analysis values from price history.

Philosophy:
- Indicators are calculation engines for technical analysis
- Indicators maintain internal state for incremental updates
- Indicators can be used directly by strategies (import and instantiate)
- Indicators are NOT services (no EventBus dependency)

Registry Name: Derived from class name (e.g., SMAIndicator → "sma")
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Any

from qs_trader.services.data.models import Bar


class IndicatorPlacement(str, Enum):
    """
    Indicator chart placement classification.

    Determines where an indicator should be rendered in chart visualizations:
    - OVERLAY: Same chart as price (e.g., moving averages, Bollinger Bands)
    - SUBPLOT: Separate chart below price (e.g., RSI, MACD, ATR)
    - VOLUME: Volume-specific subplot (e.g., volume bars, OBV)

    Usage:
        class SMA(BaseIndicator):
            placement = IndicatorPlacement.OVERLAY

        class RSI(BaseIndicator):
            placement = IndicatorPlacement.SUBPLOT
    """

    OVERLAY = "overlay"
    SUBPLOT = "subplot"
    VOLUME = "volume"


class BaseIndicator(ABC):
    """
    Abstract base class for all technical indicators.

    Responsibilities:
    - Calculate indicator values from price history
    - Maintain internal state for incremental updates
    - Provide stateful and stateless computation modes
    - Validate input data

    Does NOT:
    - Subscribe to EventBus (strategies pass bars to indicators)
    - Generate trading signals (that's strategies)
    - Store historical data (strategies manage that)

    Class Attributes (for visualization):
    - placement: Where to render in charts (OVERLAY, SUBPLOT, VOLUME)
    - value_range: Expected value range as (min, max) tuple
    - default_color: Hex color for chart rendering

    Usage Patterns:

    1. Stateful (incremental updates):
        ```python
        sma = SMA(period=20)
        for bar in bars:
            value = sma.update(bar)  # Incremental
        ```

    2. Stateless (batch calculation):
        ```python
        sma = SMA(period=20)
        values = sma.calculate(bars)  # Full recalculation
        ```

    Example Implementation:
        ```python
        class SMA(BaseIndicator):
            # Visualization metadata
            placement = IndicatorPlacement.OVERLAY
            default_color = "#667eea"

            def __init__(self, period: int):
                self.period = period
                self._values = []

            def calculate(self, bars: list[Bar]) -> list[float]:
                prices = [b.close for b in bars]
                return [
                    sum(prices[i-self.period:i]) / self.period
                    for i in range(self.period, len(prices) + 1)
                ]

            def update(self, bar: Bar) -> float | None:
                self._values.append(bar.close)
                if len(self._values) < self.period:
                    return None
                return sum(self._values[-self.period:]) / self.period

            def reset(self) -> None:
                self._values.clear()

            @property
            def value(self) -> float | None:
                if len(self._values) < self.period:
                    return None
                return sum(self._values[-self.period:]) / self.period

            @property
            def is_ready(self) -> bool:
                return len(self._values) >= self.period
        ```
    """

    # Class attributes for visualization (override in subclasses)
    placement: IndicatorPlacement = IndicatorPlacement.SUBPLOT
    value_range: tuple[float | None, float | None] = (None, None)
    default_color: str = "#667eea"

    @abstractmethod
    def __init__(self, **params: Any):
        """
        Initialize indicator with parameters.

        Args:
            **params: Indicator-specific parameters (e.g., period, multiplier)

        Example:
            SMA(period=20)
            EMA(period=12, smoothing=2)
            BollingerBands(period=20, num_std=2.0)

        Note:
            Store parameters and initialize internal state (lists, counters, etc.)
        """
        pass

    @abstractmethod
    def calculate(self, bars: list[Bar]) -> list[float | None] | list[dict[str, float] | None]:
        """
        Calculate indicator values from historical bars (stateless).

        Args:
            bars: List of price bars (oldest first)

        Returns:
            List of indicator values (one per bar, None during warmup)
            - Simple indicators: list[float | None]
            - Multi-value indicators: list[dict[str, float] | None]

        Examples:
            # SMA returns single values
            >>> sma = SMA(period=20)
            >>> values = sma.calculate(bars)
            [150.2, 150.3, 150.5, ...]

            # Bollinger Bands returns dicts
            >>> bb = BollingerBands(period=20, num_std=2.0)
            >>> values = bb.calculate(bars)
            [
                {"upper": 155.0, "middle": 150.0, "lower": 145.0},
                {"upper": 155.5, "middle": 150.5, "lower": 145.5},
                ...
            ]

        Note:
            - First N values may be None/NaN during warmup period
            - Does NOT modify internal state (stateless calculation)
            - Useful for backtesting with different parameters
        """
        pass

    @abstractmethod
    def update(self, bar: Bar) -> float | dict[str, float] | None:
        """
        Update indicator with new bar and return latest value (stateful).

        Args:
            bar: New price bar

        Returns:
            Latest indicator value or None if not enough data yet
            - Simple indicators: float
            - Multi-value indicators: dict[str, float]

        Examples:
            # SMA returns single value
            >>> sma = SMA(period=20)
            >>> for bar in bars:
            ...     value = sma.update(bar)
            ...     if value is not None:
            ...         print(f"SMA: {value}")

            # Bollinger Bands returns dict
            >>> bb = BollingerBands(period=20, num_std=2.0)
            >>> result = bb.update(bar)
            {"upper": 155.0, "middle": 150.0, "lower": 145.0}

        Note:
            - Returns None during warmup period
            - Modifies internal state (stores bar data)
            - Call reset() to clear state and start over
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """
        Reset indicator state to initial conditions.

        Clears all internal state (buffers, counters, etc.) so indicator
        can be reused for new calculation sequence.

        Example:
            >>> sma = SMA(period=20)
            >>> for bar in bars1:
            ...     sma.update(bar)
            >>> sma.reset()  # Clear state
            >>> for bar in bars2:
            ...     sma.update(bar)  # Fresh calculation

        Note:
            Does NOT change indicator parameters (period, etc.)
        """
        pass

    @property
    @abstractmethod
    def value(self) -> float | dict[str, float] | None:
        """
        Get current indicator value without updating.

        Returns:
            Current indicator value or None if not ready

        Example:
            >>> sma = SMA(period=20)
            >>> sma.update(bar)
            >>> current = sma.value  # Get current value
            >>> current = sma.value  # Same value (no update)

        Note:
            Read-only property - does not modify state
        """
        pass

    @property
    @abstractmethod
    def is_ready(self) -> bool:
        """
        Check if indicator has enough data to produce valid values.

        Returns:
            True if indicator is ready (past warmup period)

        Example:
            >>> sma = SMA(period=20)
            >>> for bar in bars:
            ...     sma.update(bar)
            ...     if sma.is_ready:
            ...         value = sma.value  # Safe to use

        Note:
            Equivalent to checking: self.value is not None
        """
        pass

    @property
    def name(self) -> str:
        """
        Indicator name for registry and logging.

        Returns:
            Indicator name (defaults to snake_case of class name)

        Example:
            SMAIndicator → "sma"
            BollingerBandsIndicator → "bollinger_bands"

        Note:
            Override if you want a custom registry name.
        """
        # Convert CamelCase to snake_case
        name = self.__class__.__name__
        if name.endswith("Indicator"):
            name = name[:-9]  # Remove "Indicator" suffix

        # Simple conversion (can be overridden)
        return "".join(["_" + c.lower() if c.isupper() else c for c in name]).lstrip("_")
