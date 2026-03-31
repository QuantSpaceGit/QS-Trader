"""
Trend indicators for QS-Trader.

This module provides trend-following and directional movement indicators
that help identify trend strength and direction.
"""

from collections import deque

from qs_trader.libraries.indicators.base import BaseIndicator
from qs_trader.services.data.models import Bar


class ADX(BaseIndicator):
    """
    Average Directional Index (ADX).

    Measures trend strength regardless of direction. Developed by J. Welles Wilder.
    ADX is derived from +DI (positive directional indicator) and -DI (negative
    directional indicator).

    Formula:
        True Range (TR) = max(high - low, abs(high - prev_close), abs(low - prev_close))
        +DM = high - prev_high if high - prev_high > prev_low - low else 0
        -DM = prev_low - low if prev_low - low > high - prev_high else 0
        +DI = 100 * smoothed(+DM) / smoothed(TR)
        -DI = 100 * smoothed(-DM) / smoothed(TR)
        DX = 100 * abs(+DI - -DI) / (+DI + -DI)
        ADX = smoothed(DX) using Wilder's smoothing

    Traditional interpretation:
        ADX < 20: Weak or absent trend
        ADX 20-25: Developing trend
        ADX 25-50: Strong trend
        ADX > 50: Very strong trend
        Rising ADX: Trend strengthening
        Falling ADX: Trend weakening

    Note: ADX does not indicate trend direction, only strength.
    Use +DI and -DI for direction (+DI > -DI = uptrend, -DI > +DI = downtrend).

    Parameters:
        period: Number of periods for smoothing (default: 14)

    Example:
        >>> adx = ADX(period=14)
        >>> for bar in bars:
        ...     result = adx.update(bar)
        ...     if result is not None:
        ...         value, plus_di, minus_di = result['adx'], result['plus_di'], result['minus_di']
        ...         print(f"ADX: {value:.2f}, +DI: {plus_di:.2f}, -DI: {minus_di:.2f}")
    """

    def __init__(self, period: int = 14):
        """
        Initialize ADX indicator.

        Args:
            period: Number of periods for smoothing (must be > 0)

        Raises:
            ValueError: If period <= 0
        """
        if period <= 0:
            raise ValueError("Period must be greater than 0")

        self._period = period
        self._prev_bar: Bar | None = None
        self._smoothed_tr: float | None = None
        self._smoothed_plus_dm: float | None = None
        self._smoothed_minus_dm: float | None = None
        self._smoothed_dx: float | None = None
        self._count = 0
        self._tr_sum = 0.0
        self._plus_dm_sum = 0.0
        self._minus_dm_sum = 0.0

    def calculate(self, bars: list[Bar]) -> list[dict[str, float] | None]:
        """
        Calculate ADX for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of dicts with 'adx', 'plus_di', 'minus_di' or None values
        """
        if not bars:
            return []

        result: list[dict[str, float] | None] = []
        prev_bar: Bar | None = None
        smoothed_tr: float | None = None
        smoothed_plus_dm: float | None = None
        smoothed_minus_dm: float | None = None
        smoothed_dx: float | None = None
        count = 0
        tr_sum = 0.0
        plus_dm_sum = 0.0
        minus_dm_sum = 0.0

        for bar in bars:
            if prev_bar is None:
                result.append(None)
                prev_bar = bar
                continue

            # Calculate True Range
            high_low = bar.high - bar.low
            high_close = abs(bar.high - prev_bar.close)
            low_close = abs(bar.low - prev_bar.close)
            tr = max(high_low, high_close, low_close)

            # Calculate Directional Movement
            up_move = bar.high - prev_bar.high
            down_move = prev_bar.low - bar.low

            plus_dm = 0.0
            minus_dm = 0.0

            if up_move > down_move and up_move > 0:
                plus_dm = up_move
            if down_move > up_move and down_move > 0:
                minus_dm = down_move

            count += 1

            if count < self._period:
                # Accumulate for initial smoothed values
                tr_sum += tr
                plus_dm_sum += plus_dm
                minus_dm_sum += minus_dm
                result.append(None)
            elif count == self._period:
                # Calculate initial smoothed values (simple average)
                smoothed_tr = tr_sum / self._period
                smoothed_plus_dm = plus_dm_sum / self._period
                smoothed_minus_dm = minus_dm_sum / self._period

                # Calculate DI values
                plus_di = 100.0 * smoothed_plus_dm / smoothed_tr if smoothed_tr > 0 else 0.0
                minus_di = 100.0 * smoothed_minus_dm / smoothed_tr if smoothed_tr > 0 else 0.0

                # Calculate DX
                di_sum = plus_di + minus_di
                if di_sum > 0:
                    dx = 100.0 * abs(plus_di - minus_di) / di_sum
                    smoothed_dx = dx  # First DX value, no smoothing yet
                else:
                    dx = 0.0
                    smoothed_dx = 0.0

                result.append(None)  # Need one more period for ADX
            else:
                # Apply Wilder's smoothing: smoothed = (prev_smoothed * (period - 1) + current) / period
                assert smoothed_tr is not None
                assert smoothed_plus_dm is not None
                assert smoothed_minus_dm is not None

                smoothed_tr = (smoothed_tr * (self._period - 1) + tr) / self._period
                smoothed_plus_dm = (smoothed_plus_dm * (self._period - 1) + plus_dm) / self._period
                smoothed_minus_dm = (smoothed_minus_dm * (self._period - 1) + minus_dm) / self._period

                # Calculate DI values
                assert smoothed_tr is not None
                assert smoothed_plus_dm is not None
                assert smoothed_minus_dm is not None
                plus_di = 100.0 * smoothed_plus_dm / smoothed_tr if smoothed_tr > 0 else 0.0
                minus_di = 100.0 * smoothed_minus_dm / smoothed_tr if smoothed_tr > 0 else 0.0

                # Calculate DX
                di_sum = plus_di + minus_di
                if di_sum > 0:
                    dx = 100.0 * abs(plus_di - minus_di) / di_sum
                else:
                    dx = 0.0

                # Smooth DX to get ADX
                if smoothed_dx is None:
                    smoothed_dx = dx
                else:
                    smoothed_dx = (smoothed_dx * (self._period - 1) + dx) / self._period

                result.append({"adx": smoothed_dx, "plus_di": plus_di, "minus_di": minus_di})

            prev_bar = bar

        return result

    def update(self, bar: Bar) -> dict[str, float] | None:
        """
        Update ADX with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Dict with 'adx', 'plus_di', 'minus_di' or None if not ready
        """
        if self._prev_bar is None:
            self._prev_bar = bar
            self._count = 0
            return None

        # Calculate True Range
        high_low = bar.high - bar.low
        high_close = abs(bar.high - self._prev_bar.close)
        low_close = abs(bar.low - self._prev_bar.close)
        tr = max(high_low, high_close, low_close)

        # Calculate Directional Movement
        up_move = bar.high - self._prev_bar.high
        down_move = self._prev_bar.low - bar.low

        plus_dm = 0.0
        minus_dm = 0.0

        if up_move > down_move and up_move > 0:
            plus_dm = up_move
        if down_move > up_move and down_move > 0:
            minus_dm = down_move

        self._count += 1

        if self._count < self._period:
            # Accumulate for initial smoothed values
            self._tr_sum += tr
            self._plus_dm_sum += plus_dm
            self._minus_dm_sum += minus_dm
            self._prev_bar = bar
            return None
        elif self._count == self._period:
            # Calculate initial smoothed values (simple average)
            self._smoothed_tr = self._tr_sum / self._period
            self._smoothed_plus_dm = self._plus_dm_sum / self._period
            self._smoothed_minus_dm = self._minus_dm_sum / self._period

            # Calculate DI values
            plus_di = 100.0 * self._smoothed_plus_dm / self._smoothed_tr if self._smoothed_tr > 0 else 0.0
            minus_di = 100.0 * self._smoothed_minus_dm / self._smoothed_tr if self._smoothed_tr > 0 else 0.0

            # Calculate DX
            di_sum = plus_di + minus_di
            if di_sum > 0:
                dx = 100.0 * abs(plus_di - minus_di) / di_sum
                self._smoothed_dx = dx  # First DX value, no smoothing yet
            else:
                dx = 0.0
                self._smoothed_dx = 0.0

            self._prev_bar = bar
            return None  # Need one more period for ADX
        else:
            # Apply Wilder's smoothing
            assert self._smoothed_tr is not None
            assert self._smoothed_plus_dm is not None
            assert self._smoothed_minus_dm is not None

            self._smoothed_tr = (self._smoothed_tr * (self._period - 1) + tr) / self._period
            self._smoothed_plus_dm = (self._smoothed_plus_dm * (self._period - 1) + plus_dm) / self._period
            self._smoothed_minus_dm = (self._smoothed_minus_dm * (self._period - 1) + minus_dm) / self._period

            # Calculate DI values
            assert self._smoothed_tr is not None
            assert self._smoothed_plus_dm is not None
            assert self._smoothed_minus_dm is not None
            plus_di = 100.0 * self._smoothed_plus_dm / self._smoothed_tr if self._smoothed_tr > 0 else 0.0
            minus_di = 100.0 * self._smoothed_minus_dm / self._smoothed_tr if self._smoothed_tr > 0 else 0.0

            # Calculate DX
            di_sum = plus_di + minus_di
            if di_sum > 0:
                dx = 100.0 * abs(plus_di - minus_di) / di_sum
            else:
                dx = 0.0

            # Smooth DX to get ADX
            if self._smoothed_dx is None:
                self._smoothed_dx = dx
            else:
                self._smoothed_dx = (self._smoothed_dx * (self._period - 1) + dx) / self._period

            self._prev_bar = bar
            return {"adx": self._smoothed_dx, "plus_di": plus_di, "minus_di": minus_di}

    def reset(self) -> None:
        """Reset indicator state."""
        self._prev_bar = None
        self._smoothed_tr = None
        self._smoothed_plus_dm = None
        self._smoothed_minus_dm = None
        self._smoothed_dx = None
        self._count = 0
        self._tr_sum = 0.0
        self._plus_dm_sum = 0.0
        self._minus_dm_sum = 0.0

    @property
    def value(self) -> dict[str, float] | None:
        """
        Get current indicator value.

        Returns:
            Dict with 'adx', 'plus_di', 'minus_di' or None if not ready
        """
        if self._count <= self._period or self._smoothed_dx is None:
            return None

        assert self._smoothed_tr is not None
        assert self._smoothed_plus_dm is not None
        assert self._smoothed_minus_dm is not None

        plus_di = 100.0 * self._smoothed_plus_dm / self._smoothed_tr if self._smoothed_tr > 0 else 0.0
        minus_di = 100.0 * self._smoothed_minus_dm / self._smoothed_tr if self._smoothed_tr > 0 else 0.0

        return {"adx": self._smoothed_dx, "plus_di": plus_di, "minus_di": minus_di}

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return bool(self._count > self._period and self._smoothed_dx is not None)


class Aroon(BaseIndicator):
    """
    Aroon Indicator.

    Measures time since the highest high and lowest low over a period.
    Developed by Tushar Chande to identify trend changes and strength.

    Formula:
        Aroon Up = ((period - periods since period high) / period) * 100
        Aroon Down = ((period - periods since period low) / period) * 100
        Aroon Oscillator = Aroon Up - Aroon Down

    Traditional interpretation:
        Aroon Up > 70: Strong uptrend
        Aroon Down > 70: Strong downtrend
        Aroon Up and Down < 50: Consolidation
        Aroon Up crosses above Aroon Down: Bullish signal
        Aroon Down crosses above Aroon Up: Bearish signal
        Aroon Oscillator > 0: Uptrend
        Aroon Oscillator < 0: Downtrend

    Parameters:
        period: Number of periods to look back (default: 25)

    Example:
        >>> aroon = Aroon(period=25)
        >>> for bar in bars:
        ...     result = aroon.update(bar)
        ...     if result is not None:
        ...         up, down, osc = result['aroon_up'], result['aroon_down'], result['oscillator']
        ...         print(f"Aroon Up: {up:.2f}, Down: {down:.2f}, Osc: {osc:.2f}")
    """

    def __init__(self, period: int = 25):
        """
        Initialize Aroon indicator.

        Args:
            period: Number of periods to look back (must be > 0)

        Raises:
            ValueError: If period <= 0
        """
        if period <= 0:
            raise ValueError("Period must be greater than 0")

        self._period = period
        self._highs: deque[float] = deque(maxlen=period)
        self._lows: deque[float] = deque(maxlen=period)

    def calculate(self, bars: list[Bar]) -> list[dict[str, float] | None]:
        """
        Calculate Aroon for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of dicts with 'aroon_up', 'aroon_down', 'oscillator' or None values
        """
        if not bars:
            return []

        result: list[dict[str, float] | None] = []
        highs: deque[float] = deque(maxlen=self._period)
        lows: deque[float] = deque(maxlen=self._period)

        for bar in bars:
            highs.append(bar.high)
            lows.append(bar.low)

            if len(highs) < self._period:
                result.append(None)
                continue

            # Find periods since highest high and lowest low
            # Most recent is index -1, oldest is index 0
            periods_since_high = len(highs) - 1 - highs.index(max(highs))
            periods_since_low = len(lows) - 1 - lows.index(min(lows))

            # Calculate Aroon values
            aroon_up = ((self._period - periods_since_high) / self._period) * 100.0
            aroon_down = ((self._period - periods_since_low) / self._period) * 100.0
            oscillator = aroon_up - aroon_down

            result.append({"aroon_up": aroon_up, "aroon_down": aroon_down, "oscillator": oscillator})

        return result

    def update(self, bar: Bar) -> dict[str, float] | None:
        """
        Update Aroon with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Dict with 'aroon_up', 'aroon_down', 'oscillator' or None if not ready
        """
        self._highs.append(bar.high)
        self._lows.append(bar.low)

        if len(self._highs) < self._period:
            return None

        # Find periods since highest high and lowest low
        # Most recent is index -1, oldest is index 0
        periods_since_high = len(self._highs) - 1 - list(self._highs).index(max(self._highs))
        periods_since_low = len(self._lows) - 1 - list(self._lows).index(min(self._lows))

        # Calculate Aroon values
        aroon_up = ((self._period - periods_since_high) / self._period) * 100.0
        aroon_down = ((self._period - periods_since_low) / self._period) * 100.0
        oscillator = aroon_up - aroon_down

        return {"aroon_up": aroon_up, "aroon_down": aroon_down, "oscillator": oscillator}

    def reset(self) -> None:
        """Reset indicator state."""
        self._highs.clear()
        self._lows.clear()

    @property
    def value(self) -> dict[str, float] | None:
        """
        Get current indicator value.

        Returns:
            Dict with 'aroon_up', 'aroon_down', 'oscillator' or None if not ready
        """
        if len(self._highs) < self._period:
            return None

        # Find periods since highest high and lowest low
        periods_since_high = len(self._highs) - 1 - list(self._highs).index(max(self._highs))
        periods_since_low = len(self._lows) - 1 - list(self._lows).index(min(self._lows))

        # Calculate Aroon values
        aroon_up = ((self._period - periods_since_high) / self._period) * 100.0
        aroon_down = ((self._period - periods_since_low) / self._period) * 100.0
        oscillator = aroon_up - aroon_down

        return {"aroon_up": aroon_up, "aroon_down": aroon_down, "oscillator": oscillator}

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return bool(len(self._highs) >= self._period)
