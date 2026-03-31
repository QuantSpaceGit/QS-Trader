"""
Volume Indicators.

Implementations of volume-based technical indicators:
- VWAP: Volume Weighted Average Price
- OBV: On-Balance Volume
- AD: Accumulation/Distribution
- CMF: Chaikin Money Flow

All indicators inherit from BaseIndicator and support both stateful
and stateless computation modes.
"""

from collections import deque

from qs_trader.libraries.indicators.base import BaseIndicator
from qs_trader.services.data.models import Bar


class VWAP(BaseIndicator):
    """
    Volume Weighted Average Price.

    Calculates the average price weighted by volume. VWAP is typically
    calculated from the start of the trading session and is often used
    as a benchmark for execution quality.

    Formula:
        VWAP = Σ(Typical Price * Volume) / Σ(Volume)
        Typical Price = (High + Low + Close) / 3

    Traditional interpretation:
        Price > VWAP: Bullish, above average price
        Price < VWAP: Bearish, below average price
        VWAP acts as support/resistance level

    Parameters:
        None (cumulative from start)

    Note:
        VWAP is typically reset at the start of each trading session.
        This implementation calculates cumulative VWAP from the first bar.

    Example:
        >>> vwap = VWAP()
        >>> for bar in bars:
        ...     value = vwap.update(bar)
        ...     if value is not None:
        ...         print(f"VWAP: {value:.2f}")
    """

    def __init__(self) -> None:
        """Initialize VWAP indicator."""
        self._cumulative_tpv: float = 0.0  # Typical Price * Volume
        self._cumulative_volume: float = 0.0
        self._count = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate VWAP for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of VWAP values (None for first bar with zero volume)
        """
        if not bars:
            return []

        result: list[float | None] = []
        cumulative_tpv = 0.0
        cumulative_volume = 0.0

        for bar in bars:
            typical_price = (bar.high + bar.low + bar.close) / 3.0
            cumulative_tpv += typical_price * bar.volume
            cumulative_volume += bar.volume

            if cumulative_volume == 0:
                result.append(None)
            else:
                vwap = cumulative_tpv / cumulative_volume
                result.append(vwap)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update VWAP with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Current VWAP value or None if no volume
        """
        typical_price = (bar.high + bar.low + bar.close) / 3.0
        self._cumulative_tpv += typical_price * bar.volume
        self._cumulative_volume += bar.volume
        self._count += 1

        if self._cumulative_volume == 0:
            return None

        vwap = self._cumulative_tpv / self._cumulative_volume
        return vwap

    def reset(self) -> None:
        """Reset indicator state (typically done at start of trading session)."""
        self._cumulative_tpv = 0.0
        self._cumulative_volume = 0.0
        self._count = 0

    @property
    def value(self) -> float | None:
        """
        Get current VWAP value.

        Returns:
            Current VWAP value or None if no volume
        """
        if self._cumulative_volume == 0:
            return None

        return self._cumulative_tpv / self._cumulative_volume

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._count > 0 and self._cumulative_volume > 0


class OBV(BaseIndicator):
    """
    On-Balance Volume.

    Momentum indicator that uses volume flow to predict price changes.
    OBV adds volume on up days and subtracts volume on down days.

    Formula:
        If Close > Previous Close: OBV = Previous OBV + Volume
        If Close < Previous Close: OBV = Previous OBV - Volume
        If Close = Previous Close: OBV = Previous OBV

    Traditional interpretation:
        Rising OBV: Accumulation (buying pressure)
        Falling OBV: Distribution (selling pressure)
        OBV divergence from price: Potential trend reversal

    Parameters:
        None (cumulative from start)

    Example:
        >>> obv = OBV()
        >>> for bar in bars:
        ...     value = obv.update(bar)
        ...     if value is not None:
        ...         print(f"OBV: {value:.0f}")
    """

    def __init__(self) -> None:
        """Initialize OBV indicator."""
        self._obv: float = 0.0
        self._prev_close: float | None = None
        self._count = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate OBV for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of OBV values (None for first bar)
        """
        if not bars:
            return []

        result: list[float | None] = []
        obv = 0.0

        for i, bar in enumerate(bars):
            if i == 0:
                # First bar, initialize OBV
                result.append(None)
            else:
                # Compare to previous close
                if bar.close > bars[i - 1].close:
                    obv += bar.volume
                elif bar.close < bars[i - 1].close:
                    obv -= bar.volume
                # If equal, OBV unchanged

                result.append(obv)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update OBV with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Current OBV value or None if first bar
        """
        self._count += 1

        if self._count == 1:
            # First bar, just store close
            self._prev_close = bar.close
            return None

        # Compare to previous close
        assert self._prev_close is not None
        if bar.close > self._prev_close:
            self._obv += bar.volume
        elif bar.close < self._prev_close:
            self._obv -= bar.volume
        # If equal, OBV unchanged

        self._prev_close = bar.close
        return self._obv

    def reset(self) -> None:
        """Reset indicator state."""
        self._obv = 0.0
        self._prev_close = None
        self._count = 0

    @property
    def value(self) -> float | None:
        """
        Get current OBV value.

        Returns:
            Current OBV value or None if not ready
        """
        if self._count <= 1:
            return None

        return self._obv

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return bool(self._count > 1)


class AD(BaseIndicator):
    """
    Accumulation/Distribution Line.

    Volume indicator that measures cumulative flow of money into and out
    of a security. Uses the relationship between close and high-low range.

    Formula:
        Money Flow Multiplier = ((Close - Low) - (High - Close)) / (High - Low)
        Money Flow Volume = Money Flow Multiplier * Volume
        AD = Previous AD + Money Flow Volume

    Traditional interpretation:
        Rising AD: Accumulation (buying pressure)
        Falling AD: Distribution (selling pressure)
        AD divergence from price: Potential trend reversal

    Parameters:
        None (cumulative from start)

    Example:
        >>> ad = AD()
        >>> for bar in bars:
        ...     value = ad.update(bar)
        ...     if value is not None:
        ...         print(f"A/D: {value:.0f}")
    """

    def __init__(self) -> None:
        """Initialize A/D indicator."""
        self._ad: float = 0.0
        self._count = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate A/D for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of A/D values
        """
        if not bars:
            return []

        result: list[float | None] = []
        ad = 0.0

        for bar in bars:
            high_low = bar.high - bar.low

            if high_low == 0:
                # No range, no money flow
                money_flow_volume = 0.0
            else:
                money_flow_multiplier = ((bar.close - bar.low) - (bar.high - bar.close)) / high_low
                money_flow_volume = money_flow_multiplier * bar.volume

            ad += money_flow_volume
            result.append(ad)

        return result

    def update(self, bar: Bar) -> float:
        """
        Update A/D with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Current A/D value
        """
        high_low = bar.high - bar.low

        if high_low == 0:
            # No range, no money flow
            money_flow_volume = 0.0
        else:
            money_flow_multiplier = ((bar.close - bar.low) - (bar.high - bar.close)) / high_low
            money_flow_volume = money_flow_multiplier * bar.volume

        self._ad += money_flow_volume
        self._count += 1

        return self._ad

    def reset(self) -> None:
        """Reset indicator state."""
        self._ad = 0.0
        self._count = 0

    @property
    def value(self) -> float | None:
        """
        Get current A/D value.

        Returns:
            Current A/D value or None if not ready
        """
        if self._count == 0:
            return None

        return self._ad

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return bool(self._count > 0)


class CMF(BaseIndicator):
    """
    Chaikin Money Flow.

    Measures the amount of money flow volume over a specific period.
    Combines price and volume to show accumulation/distribution.

    Formula:
        Money Flow Multiplier = ((Close - Low) - (High - Close)) / (High - Low)
        Money Flow Volume = Money Flow Multiplier * Volume
        CMF = Σ(Money Flow Volume over n periods) / Σ(Volume over n periods)

    Traditional interpretation:
        CMF > 0: Buying pressure (accumulation)
        CMF < 0: Selling pressure (distribution)
        CMF near +1: Strong buying pressure
        CMF near -1: Strong selling pressure

    Parameters:
        period: Number of bars for calculation (default: 20)

    Example:
        >>> cmf = CMF(period=20)
        >>> for bar in bars:
        ...     value = cmf.update(bar)
        ...     if value is not None:
        ...         print(f"CMF: {value:.3f}")
    """

    def __init__(self, period: int = 20) -> None:
        """
        Initialize CMF indicator.

        Args:
            period: Number of bars for calculation (default: 20)

        Raises:
            ValueError: If period is less than 1
        """
        if period < 1:
            raise ValueError("Period must be at least 1")

        self.period = period
        self._money_flow_volumes: deque[float] = deque(maxlen=period)
        self._volumes: deque[float] = deque(maxlen=period)
        self._count = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate CMF for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of CMF values (None until enough data)
        """
        if not bars:
            return []

        result: list[float | None] = []

        for i, bar in enumerate(bars):
            if i < self.period - 1:
                result.append(None)
                continue

            # Get bars for the period
            period_bars = bars[i - self.period + 1 : i + 1]

            sum_money_flow_volume = 0.0
            sum_volume = 0.0

            for b in period_bars:
                high_low = b.high - b.low

                if high_low == 0:
                    money_flow_volume = 0.0
                else:
                    money_flow_multiplier = ((b.close - b.low) - (b.high - b.close)) / high_low
                    money_flow_volume = money_flow_multiplier * b.volume

                sum_money_flow_volume += money_flow_volume
                sum_volume += b.volume

            if sum_volume == 0:
                cmf = 0.0
            else:
                cmf = sum_money_flow_volume / sum_volume

            result.append(cmf)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update CMF with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Current CMF value or None if not enough data
        """
        high_low = bar.high - bar.low

        if high_low == 0:
            money_flow_volume = 0.0
        else:
            money_flow_multiplier = ((bar.close - bar.low) - (bar.high - bar.close)) / high_low
            money_flow_volume = money_flow_multiplier * bar.volume

        self._money_flow_volumes.append(money_flow_volume)
        self._volumes.append(bar.volume)
        self._count += 1

        if self._count < self.period:
            return None

        sum_money_flow_volume = sum(self._money_flow_volumes)
        sum_volume = sum(self._volumes)

        if sum_volume == 0:
            return 0.0

        cmf = sum_money_flow_volume / sum_volume
        return cmf

    def reset(self) -> None:
        """Reset indicator state."""
        self._money_flow_volumes.clear()
        self._volumes.clear()
        self._count = 0

    @property
    def value(self) -> float | None:
        """
        Get current CMF value.

        Returns:
            Current CMF value or None if not enough data
        """
        if self._count < self.period:
            return None

        sum_money_flow_volume = sum(self._money_flow_volumes)
        sum_volume = sum(self._volumes)

        if sum_volume == 0:
            return 0.0

        cmf = sum_money_flow_volume / sum_volume
        return cmf

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._count >= self.period
