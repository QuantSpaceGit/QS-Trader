"""
Momentum Indicators.

Implementations of momentum-based technical indicators:
- RSI: Relative Strength Index
- MACD: Moving Average Convergence Divergence
- Stochastic: Stochastic Oscillator (%K and %D)
- CCI: Commodity Channel Index
- ROC: Rate of Change
- Williams %R: Williams Percent Range

All indicators inherit from BaseIndicator and support both stateful
and stateless computation modes.
"""

from collections import deque
from typing import Any

from qs_trader.libraries.indicators.base import BaseIndicator, IndicatorPlacement
from qs_trader.libraries.indicators.buildin.moving_averages import EMA, SMA
from qs_trader.services.data.models import Bar


class RSI(BaseIndicator):
    """
    Relative Strength Index.

    Momentum oscillator that measures the speed and magnitude of price changes.
    RSI oscillates between 0 and 100, typically using 14-period lookback.

    Formula:
        RS = Average Gain / Average Loss
        RSI = 100 - (100 / (1 + RS))

    Traditional interpretation:
        RSI > 70: Overbought (potential sell signal)
        RSI < 30: Oversold (potential buy signal)
        RSI = 50: Neutral momentum

    Parameters:
        period: Number of bars for RSI calculation (default: 14)
        price_field: Which price to use (default: "close")

    Example:
        >>> rsi = RSI(period=14)
        >>> for bar in bars:
        ...     value = rsi.update(bar)
        ...     if value is not None:
        ...         if value > 70:
        ...             print(f"Overbought: RSI={value:.2f}")
        ...         elif value < 30:
        ...             print(f"Oversold: RSI={value:.2f}")
    """

    # Visualization metadata
    placement = IndicatorPlacement.SUBPLOT
    value_range = (0.0, 100.0)
    default_color = "#fa709a"

    def __init__(self, period: int = 14, price_field: str = "close", **params: Any):
        """
        Initialize RSI indicator.

        Args:
            period: Number of bars for RSI calculation
            price_field: Which price field to use
            **params: Additional parameters (ignored)

        Raises:
            ValueError: If period < 1
        """
        if period < 1:
            raise ValueError(f"Period must be >= 1, got {period}")

        self.period = period
        self.price_field = price_field

        # Track previous price and gains/losses
        self._prev_price: float | None = None
        self._avg_gain: float | None = None
        self._avg_loss: float | None = None
        self._count = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate RSI values for all bars (stateless).

        Args:
            bars: List of price bars

        Returns:
            List of RSI values (None for warmup period)
        """
        if not bars:
            return []

        prices = [getattr(bar, self.price_field) for bar in bars]
        result: list[float | None] = []

        prev_price: float | None = None
        avg_gain: float | None = None
        avg_loss: float | None = None

        for i, price in enumerate(prices):
            if prev_price is None:
                # First bar - no change to calculate
                result.append(None)
                prev_price = price
                continue

            # Calculate price change
            change = price - prev_price
            gain = max(change, 0.0)
            loss = max(-change, 0.0)

            if i < self.period:
                # Accumulate initial gains and losses
                if avg_gain is None:
                    avg_gain = gain
                    avg_loss = loss
                else:
                    avg_gain += gain
                    avg_loss += loss
                result.append(None)
            elif i == self.period:
                # Calculate initial averages (SMA for first period)
                assert avg_gain is not None and avg_loss is not None
                avg_gain = (avg_gain + gain) / self.period
                avg_loss = (avg_loss + loss) / self.period

                # Calculate RSI
                if avg_loss == 0:
                    rsi = 100.0
                else:
                    assert avg_gain is not None and avg_loss is not None
                    rs = avg_gain / avg_loss
                    rsi = 100.0 - (100.0 / (1.0 + rs))
                result.append(rsi)
            else:
                # Use smoothed moving average (Wilder's smoothing)
                assert avg_gain is not None and avg_loss is not None
                avg_gain = (avg_gain * (self.period - 1) + gain) / self.period
                avg_loss = (avg_loss * (self.period - 1) + loss) / self.period

                # Calculate RSI
                if avg_loss == 0:
                    rsi = 100.0
                else:
                    assert avg_gain is not None and avg_loss is not None
                    rs = avg_gain / avg_loss
                    rsi = 100.0 - (100.0 / (1.0 + rs))
                result.append(rsi)

            prev_price = price

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update RSI with new bar (stateful).

        Args:
            bar: New price bar

        Returns:
            Current RSI value or None if not ready
        """
        price = getattr(bar, self.price_field)

        if self._prev_price is None:
            # First bar
            self._prev_price = price
            self._count = 1
            return None

        # Calculate price change
        change = price - self._prev_price
        gain = max(change, 0.0)
        loss = max(-change, 0.0)

        self._count += 1

        if self._count <= self.period:
            # Accumulate for initial average
            if self._avg_gain is None:
                self._avg_gain = gain
                self._avg_loss = loss
            else:
                self._avg_gain += gain
                self._avg_loss += loss

            self._prev_price = price

            if self._count < self.period:
                return None

            # Calculate initial averages
            assert self._avg_gain is not None and self._avg_loss is not None
            self._avg_gain /= self.period
            self._avg_loss /= self.period
        else:
            # Apply Wilder's smoothing
            assert self._avg_gain is not None and self._avg_loss is not None
            self._avg_gain = (self._avg_gain * (self.period - 1) + gain) / self.period
            self._avg_loss = (self._avg_loss * (self.period - 1) + loss) / self.period

        # Calculate RSI
        assert self._avg_gain is not None and self._avg_loss is not None
        if self._avg_loss == 0:
            rsi = 100.0
        else:
            rs = self._avg_gain / self._avg_loss
            rsi = 100.0 - (100.0 / (1.0 + rs))

        self._prev_price = price
        return rsi

    def reset(self) -> None:
        """Reset indicator state."""
        self._prev_price = None
        self._avg_gain = None
        self._avg_loss = None
        self._count = 0

    @property
    def value(self) -> float | None:
        """Get current RSI value without updating."""
        if self._avg_gain is None or self._avg_loss is None or self._count < self.period:
            return None

        if self._avg_loss == 0:
            return 100.0

        rs = self._avg_gain / self._avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._count >= self.period and self._avg_gain is not None


class MACD(BaseIndicator):
    """
    Moving Average Convergence Divergence.

    Trend-following momentum indicator showing the relationship between
    two exponential moving averages. Returns three values: MACD line,
    signal line, and histogram.

    Formula:
        MACD Line = EMA(fast_period) - EMA(slow_period)
        Signal Line = EMA(MACD Line, signal_period)
        Histogram = MACD Line - Signal Line

    Interpretation:
        - MACD crosses above signal: Bullish signal
        - MACD crosses below signal: Bearish signal
        - Histogram > 0: Bullish momentum
        - Histogram < 0: Bearish momentum

    Parameters:
        fast_period: Fast EMA period (default: 12)
        slow_period: Slow EMA period (default: 26)
        signal_period: Signal line EMA period (default: 9)
        price_field: Which price to use (default: "close")

    Returns:
        Dictionary with three values:
            - macd: MACD line value
            - signal: Signal line value
            - histogram: MACD - Signal

    Example:
        >>> macd = MACD(fast_period=12, slow_period=26, signal_period=9)
        >>> for bar in bars:
        ...     values = macd.update(bar)
        ...     if values is not None:
        ...         if values['histogram'] > 0:
        ...             print(f"Bullish: MACD={values['macd']:.2f}")
        ...         if values['macd'] > values['signal']:
        ...             print("MACD crossed above signal")
    """

    def __init__(
        self,
        fast_period: int = 12,
        slow_period: int = 26,
        signal_period: int = 9,
        price_field: str = "close",
        **params: Any,
    ):
        """
        Initialize MACD indicator.

        Args:
            fast_period: Fast EMA period
            slow_period: Slow EMA period
            signal_period: Signal line EMA period
            price_field: Which price field to use
            **params: Additional parameters (ignored)

        Raises:
            ValueError: If fast_period >= slow_period or any period < 1
        """
        if fast_period < 1 or slow_period < 1 or signal_period < 1:
            raise ValueError("All periods must be >= 1")
        if fast_period >= slow_period:
            raise ValueError(f"Fast period ({fast_period}) must be < slow period ({slow_period})")

        self.fast_period = fast_period
        self.slow_period = slow_period
        self.signal_period = signal_period
        self.price_field = price_field

        # EMAs for MACD calculation
        self._fast_ema = EMA(period=fast_period, price_field=price_field)
        self._slow_ema = EMA(period=slow_period, price_field=price_field)
        self._signal_ema = EMA(period=signal_period, price_field=price_field)

    def calculate(self, bars: list[Bar]) -> list[dict[str, float] | None]:
        """
        Calculate MACD values for all bars (stateless).

        Args:
            bars: List of price bars

        Returns:
            List of MACD dictionaries (None for warmup period)
        """
        if not bars:
            return []

        # Create temporary EMA instances for stateless calculation
        # to avoid corrupting the streaming state in _fast_ema/_slow_ema/_signal_ema
        # Match the smoothing configuration from the instance EMAs
        fast_calc = EMA(period=self.fast_period, price_field=self.price_field, smoothing=self._fast_ema.smoothing)
        slow_calc = EMA(period=self.slow_period, price_field=self.price_field, smoothing=self._slow_ema.smoothing)

        # Calculate fast and slow EMAs
        fast_values = fast_calc.calculate(bars)
        slow_values = slow_calc.calculate(bars)

        # Calculate MACD line
        macd_values: list[float | None] = []
        for fast, slow in zip(fast_values, slow_values):
            if fast is None or slow is None:
                macd_values.append(None)
            else:
                macd_values.append(fast - slow)

        # Calculate signal line (EMA of MACD)
        result: list[dict[str, float] | None] = []
        # Match the smoothing configuration from the instance signal EMA
        signal_calc = EMA(period=self.signal_period, price_field=self.price_field, smoothing=self._signal_ema.smoothing)

        for macd_val in macd_values:
            if macd_val is None:
                result.append(None)
                continue

            # Feed MACD value into signal EMA using update_value for efficiency
            signal_val = signal_calc.update_value(macd_val)

            if signal_val is None:
                result.append(None)
            else:
                histogram = macd_val - signal_val
                result.append({"macd": macd_val, "signal": signal_val, "histogram": histogram})

        return result

    def update(self, bar: Bar) -> dict[str, float] | None:
        """
        Update MACD with new bar (stateful).

        Args:
            bar: New price bar

        Returns:
            Dictionary with macd, signal, histogram or None if not ready
        """
        # Update fast and slow EMAs
        fast_val = self._fast_ema.update(bar)
        slow_val = self._slow_ema.update(bar)

        if fast_val is None or slow_val is None:
            return None

        # Calculate MACD line
        macd_val = fast_val - slow_val

        # Update signal line using update_value for efficiency
        signal_val = self._signal_ema.update_value(macd_val)

        if signal_val is None:
            return None

        histogram = macd_val - signal_val
        return {"macd": macd_val, "signal": signal_val, "histogram": histogram}

    def reset(self) -> None:
        """Reset indicator state."""
        self._fast_ema.reset()
        self._slow_ema.reset()
        self._signal_ema.reset()

    @property
    def value(self) -> dict[str, float] | None:
        """Get current MACD values without updating."""
        fast_val = self._fast_ema.value
        slow_val = self._slow_ema.value
        signal_val = self._signal_ema.value

        if fast_val is None or slow_val is None or signal_val is None:
            return None

        macd_val = fast_val - slow_val
        histogram = macd_val - signal_val
        return {"macd": macd_val, "signal": signal_val, "histogram": histogram}

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._signal_ema.is_ready


class Stochastic(BaseIndicator):
    """
    Stochastic Oscillator.

    Momentum indicator comparing closing price to price range over time.
    Returns %K (fast) and %D (slow, smoothed %K) values between 0-100.

    Formula:
        %K = 100 * (Close - LowestLow) / (HighestHigh - LowestLow)
        %D = SMA(%K, smooth_period)

    Interpretation:
        %K or %D > 80: Overbought
        %K or %D < 20: Oversold
        %K crosses above %D: Bullish signal
        %K crosses below %D: Bearish signal

    Parameters:
        period: Lookback period for high/low (default: 14)
        smooth_k: Smoothing period for %K (default: 3)
        smooth_d: Smoothing period for %D (default: 3)
        price_field: Base field for calculations (default: "close")

    Returns:
        Dictionary with two values:
            - k: %K value (0-100)
            - d: %D value (0-100)

    Example:
        >>> stoch = Stochastic(period=14, smooth_k=3, smooth_d=3)
        >>> for bar in bars:
        ...     values = stoch.update(bar)
        ...     if values is not None:
        ...         if values['k'] > 80:
        ...             print(f"Overbought: %K={values['k']:.2f}")
        ...         if values['k'] > values['d']:
        ...             print("Bullish crossover")
    """

    def __init__(
        self,
        period: int = 14,
        smooth_k: int = 3,
        smooth_d: int = 3,
        price_field: str = "close",
        **params: Any,
    ):
        """
        Initialize Stochastic indicator.

        Args:
            period: Lookback period for high/low
            smooth_k: Smoothing for %K
            smooth_d: Smoothing for %D
            price_field: Base price field (used for context)
            **params: Additional parameters (ignored)

        Raises:
            ValueError: If any period < 1
        """
        if period < 1 or smooth_k < 1 or smooth_d < 1:
            raise ValueError("All periods must be >= 1")

        self.period = period
        self.smooth_k = smooth_k
        self.smooth_d = smooth_d
        self.price_field = price_field

        # Track recent highs, lows, closes
        self._highs: deque[float] = deque(maxlen=period)
        self._lows: deque[float] = deque(maxlen=period)
        self._closes: deque[float] = deque(maxlen=period)

        # Monotonic deques for O(1) max/min operations
        self._max_deque: deque[tuple[float, int]] = deque()  # (value, index)
        self._min_deque: deque[tuple[float, int]] = deque()  # (value, index)
        self._index = 0

        # Smoothing for %K and %D
        self._k_sma = SMA(period=smooth_k, price_field=price_field)
        self._d_sma = SMA(period=smooth_d, price_field=price_field)

    def calculate(self, bars: list[Bar]) -> list[dict[str, float] | None]:
        """
        Calculate Stochastic values for all bars (stateless).

        Args:
            bars: List of price bars

        Returns:
            List of Stochastic dictionaries (None for warmup period)
        """
        if not bars:
            return []

        result: list[dict[str, float] | None] = []
        k_sma = SMA(period=self.smooth_k, price_field=self.price_field)
        d_sma = SMA(period=self.smooth_d, price_field=self.price_field)

        highs: deque[float] = deque(maxlen=self.period)
        lows: deque[float] = deque(maxlen=self.period)
        closes: deque[float] = deque(maxlen=self.period)
        # Monotonic deques for O(1) max/min
        max_deque: deque[tuple[float, int]] = deque()
        min_deque: deque[tuple[float, int]] = deque()

        for idx, bar in enumerate(bars):
            highs.append(bar.high)
            lows.append(bar.low)
            closes.append(bar.close)

            # Maintain monotonic max deque (decreasing order)
            while max_deque and max_deque[-1][0] <= bar.high:
                max_deque.pop()
            max_deque.append((bar.high, idx))

            # Maintain monotonic min deque (increasing order)
            while min_deque and min_deque[-1][0] >= bar.low:
                min_deque.pop()
            min_deque.append((bar.low, idx))

            # Remove elements outside window
            while max_deque and max_deque[0][1] <= idx - self.period:
                max_deque.popleft()
            while min_deque and min_deque[0][1] <= idx - self.period:
                min_deque.popleft()

            if len(closes) < self.period:
                result.append(None)
                continue

            # O(1) max/min retrieval
            highest_high = max_deque[0][0]
            lowest_low = min_deque[0][0]
            close = closes[-1]

            if highest_high == lowest_low:
                raw_k = 50.0  # Neutral when no range
            else:
                raw_k = 100.0 * (close - lowest_low) / (highest_high - lowest_low)

            # Smooth %K using update_value (no synthetic bars)
            k_val = k_sma.update_value(raw_k)

            if k_val is None:
                result.append(None)
                continue

            # Smooth %D (SMA of %K)
            d_val = d_sma.update_value(k_val)

            if d_val is None:
                result.append(None)
            else:
                result.append({"k": k_val, "d": d_val})

        return result

    def update(self, bar: Bar) -> dict[str, float] | None:
        """
        Update Stochastic with new bar (stateful).

        Optimized with monotonic deques for O(1) max/min operations.

        Args:
            bar: New price bar

        Returns:
            Dictionary with k, d values or None if not ready
        """
        self._highs.append(bar.high)
        self._lows.append(bar.low)
        self._closes.append(bar.close)

        # Maintain monotonic max deque
        while self._max_deque and self._max_deque[-1][0] <= bar.high:
            self._max_deque.pop()
        self._max_deque.append((bar.high, self._index))

        # Maintain monotonic min deque
        while self._min_deque and self._min_deque[-1][0] >= bar.low:
            self._min_deque.pop()
        self._min_deque.append((bar.low, self._index))

        # Remove elements outside window
        while self._max_deque and self._max_deque[0][1] <= self._index - self.period:
            self._max_deque.popleft()
        while self._min_deque and self._min_deque[0][1] <= self._index - self.period:
            self._min_deque.popleft()

        self._index += 1

        if len(self._closes) < self.period:
            return None

        # O(1) max/min retrieval
        highest_high = self._max_deque[0][0]
        lowest_low = self._min_deque[0][0]
        close = self._closes[-1]

        if highest_high == lowest_low:
            raw_k = 50.0
        else:
            raw_k = 100.0 * (close - lowest_low) / (highest_high - lowest_low)

        # Smooth %K using update_value (no synthetic bars)
        k_val = self._k_sma.update_value(raw_k)

        if k_val is None:
            return None

        # Smooth %D
        d_val = self._d_sma.update_value(k_val)

        if d_val is None:
            return None

        return {"k": k_val, "d": d_val}

    def reset(self) -> None:
        """Reset indicator state."""
        self._highs.clear()
        self._lows.clear()
        self._closes.clear()
        self._max_deque.clear()
        self._min_deque.clear()
        self._index = 0
        self._k_sma.reset()
        self._d_sma.reset()

    @property
    def value(self) -> dict[str, float] | None:
        """Get current Stochastic values without updating."""
        if len(self._closes) < self.period:
            return None

        k_val = self._k_sma.value
        d_val = self._d_sma.value

        if k_val is None or d_val is None:
            return None

        return {"k": k_val, "d": d_val}

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return len(self._closes) >= self.period and self._d_sma.is_ready


class CCI(BaseIndicator):
    """
    Commodity Channel Index.

    Momentum oscillator that measures the variation of price from statistical mean.
    Unlike most oscillators, CCI is unbounded and typically ranges between -100 and +100.

    Formula:
        Typical Price = (High + Low + Close) / 3
        SMA = Simple Moving Average of Typical Price
        Mean Deviation = Average of |Typical Price - SMA|
        CCI = (Typical Price - SMA) / (0.015 * Mean Deviation)

    Traditional interpretation:
        CCI > +100: Overbought, strong uptrend
        CCI < -100: Oversold, strong downtrend
        Between -100 and +100: Normal trading range

    Parameters:
        period: Number of bars for CCI calculation (default: 20)

    Example:
        >>> cci = CCI(period=20)
        >>> for bar in bars:
        ...     value = cci.update(bar)
        ...     if value is not None:
        ...         if value > 100:
        ...             print(f"Strong uptrend: CCI={value:.2f}")
    """

    def __init__(self, period: int = 20):
        """
        Initialize CCI indicator.

        Args:
            period: Number of bars for calculation (default: 20)

        Raises:
            ValueError: If period is less than 1
        """
        if period < 1:
            raise ValueError("Period must be at least 1")

        self.period = period
        self._typical_prices: deque[float] = deque(maxlen=period)
        self._tp_sum = 0.0  # Running sum of typical prices
        self._count = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate CCI for a list of bars (stateless).

        Optimized using precomputed typical prices and rolling sum to avoid O(n*period) complexity.
        Now O(n) with constant-time updates per bar.

        Args:
            bars: List of Bar objects

        Returns:
            List of CCI values (None until enough data)
        """
        if not bars:
            return []

        # Precompute all typical prices once
        typical_prices = [(bar.high + bar.low + bar.close) / 3.0 for bar in bars]
        result: list[float | None] = []
        tp_sum = 0.0
        tp_deque: deque[float] = deque(maxlen=self.period)

        for i, tp in enumerate(typical_prices):
            tp_deque.append(tp)
            tp_sum += tp

            if i < self.period - 1:
                result.append(None)
                continue

            if i >= self.period:
                # Subtract the typical price leaving the window
                tp_sum -= typical_prices[i - self.period]

            # Calculate SMA of typical prices using running sum
            sma = tp_sum / self.period

            # Calculate mean deviation from deque
            mean_dev = sum(abs(t - sma) for t in tp_deque) / self.period

            # Calculate CCI
            if mean_dev == 0:
                cci = 0.0
            else:
                cci = (tp - sma) / (0.015 * mean_dev)

            result.append(cci)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update CCI with new bar (stateful).

        Optimized using rolling sum to avoid recalculating sum each iteration.

        Args:
            bar: New bar to process

        Returns:
            Current CCI value or None if not enough data
        """
        # Calculate typical price
        typical_price = (bar.high + bar.low + bar.close) / 3.0

        # Update rolling sum
        if len(self._typical_prices) == self.period:
            self._tp_sum -= self._typical_prices[0]

        self._typical_prices.append(typical_price)
        self._tp_sum += typical_price
        self._count += 1

        if self._count < self.period:
            return None

        # Calculate SMA of typical prices using running sum
        sma = self._tp_sum / self.period

        # Calculate mean deviation
        mean_dev = sum(abs(tp - sma) for tp in self._typical_prices) / self.period

        # Calculate CCI
        if mean_dev == 0:
            return 0.0

        cci = (typical_price - sma) / (0.015 * mean_dev)
        return cci

    def reset(self) -> None:
        """Reset indicator state."""
        self._typical_prices.clear()
        self._tp_sum = 0.0
        self._count = 0

    @property
    def value(self) -> float | None:
        """
        Get current CCI value.

        Returns:
            Current CCI value or None if not enough data
        """
        if self._count < self.period:
            return None

        # Calculate SMA using running sum
        sma = self._tp_sum / self.period

        # Calculate mean deviation
        mean_dev = sum(abs(tp - sma) for tp in self._typical_prices) / self.period

        if mean_dev == 0:
            return 0.0

        # Calculate CCI using most recent typical price
        current_tp = self._typical_prices[-1]
        cci = (current_tp - sma) / (0.015 * mean_dev)
        return cci

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._count >= self.period


class ROC(BaseIndicator):
    """
    Rate of Change.

    Momentum oscillator that measures the percentage change in price
    over a specified period. Positive values indicate upward momentum,
    negative values indicate downward momentum.

    Formula:
        ROC = ((Current Price - Price n periods ago) / Price n periods ago) * 100

    Traditional interpretation:
        ROC > 0: Upward momentum
        ROC < 0: Downward momentum
        ROC crossing zero: Potential trend change

    Parameters:
        period: Number of bars for ROC calculation (default: 12)
        price_field: Which price to use (default: "close")

    Example:
        >>> roc = ROC(period=12)
        >>> for bar in bars:
        ...     value = roc.update(bar)
        ...     if value is not None:
        ...         if value > 10:
        ...             print(f"Strong upward momentum: ROC={value:.2f}%")
    """

    def __init__(self, period: int = 12, price_field: str = "close"):
        """
        Initialize ROC indicator.

        Args:
            period: Number of bars for calculation (default: 12)
            price_field: Which price field to use (default: "close")

        Raises:
            ValueError: If period is less than 1
        """
        if period < 1:
            raise ValueError("Period must be at least 1")

        self.period = period
        self.price_field = price_field
        self._prices: deque[float] = deque(maxlen=period + 1)
        self._count = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate ROC for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of ROC values (None until enough data)
        """
        if not bars:
            return []

        result: list[float | None] = []

        for i, bar in enumerate(bars):
            if i < self.period:
                result.append(None)
                continue

            current_price = getattr(bar, self.price_field)
            old_price = getattr(bars[i - self.period], self.price_field)

            if old_price == 0:
                roc = 0.0
            else:
                roc = ((current_price - old_price) / old_price) * 100.0

            result.append(roc)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update ROC with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Current ROC value or None if not enough data
        """
        price = getattr(bar, self.price_field)
        self._prices.append(price)
        self._count += 1

        if self._count <= self.period:
            return None

        # Get current price and price from n periods ago
        current_price = self._prices[-1]
        old_price = self._prices[0]

        if old_price == 0:
            return 0.0

        roc = ((current_price - old_price) / old_price) * 100.0
        return roc

    def reset(self) -> None:
        """Reset indicator state."""
        self._prices.clear()
        self._count = 0

    @property
    def value(self) -> float | None:
        """
        Get current ROC value.

        Returns:
            Current ROC value or None if not enough data
        """
        if self._count <= self.period:
            return None

        current_price = self._prices[-1]
        old_price = self._prices[0]

        if old_price == 0:
            return 0.0

        roc = ((current_price - old_price) / old_price) * 100.0
        return roc

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._count > self.period


class WilliamsR(BaseIndicator):
    """
    Williams %R (Williams Percent Range).

    Momentum oscillator that measures overbought/oversold levels.
    Similar to Stochastic Oscillator but inverted and scaled to -100 to 0.

    Formula:
        %R = -100 * (Highest High - Close) / (Highest High - Lowest Low)

    Traditional interpretation:
        %R > -20: Overbought (potential sell signal)
        %R < -80: Oversold (potential buy signal)
        Between -20 and -80: Normal trading range

    Parameters:
        period: Number of bars for calculation (default: 14)

    Example:
        >>> williams_r = WilliamsR(period=14)
        >>> for bar in bars:
        ...     value = williams_r.update(bar)
        ...     if value is not None:
        ...         if value > -20:
        ...             print(f"Overbought: Williams %R={value:.2f}")
    """

    def __init__(self, period: int = 14):
        """
        Initialize Williams %R indicator.

        Args:
            period: Number of bars for calculation (default: 14)

        Raises:
            ValueError: If period is less than 1
        """
        if period < 1:
            raise ValueError("Period must be at least 1")

        self.period = period
        self._highs: deque[float] = deque(maxlen=period)
        self._lows: deque[float] = deque(maxlen=period)
        self._closes: deque[float] = deque(maxlen=period)
        self._count = 0
        # Monotonic deques for O(1) max/min: (value, index)
        self._max_deque: deque[tuple[float, int]] = deque()
        self._min_deque: deque[tuple[float, int]] = deque()
        self._index = 0

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """
        Calculate Williams %R for a list of bars (stateless).

        Args:
            bars: List of Bar objects

        Returns:
            List of Williams %R values (None until enough data)
        """
        if not bars:
            return []

        result: list[float | None] = []
        # Monotonic deques for O(1) max/min: (value, index)
        max_deque: deque[tuple[float, int]] = deque()
        min_deque: deque[tuple[float, int]] = deque()

        for i, bar in enumerate(bars):
            # Maintain monotonic decreasing deque for max
            while max_deque and max_deque[-1][0] <= bar.high:
                max_deque.pop()
            max_deque.append((bar.high, i))

            # Maintain monotonic increasing deque for min
            while min_deque and min_deque[-1][0] >= bar.low:
                min_deque.pop()
            min_deque.append((bar.low, i))

            # Remove elements outside current window
            while max_deque and max_deque[0][1] <= i - self.period:
                max_deque.popleft()
            while min_deque and min_deque[0][1] <= i - self.period:
                min_deque.popleft()

            if i < self.period - 1:
                result.append(None)
                continue

            # O(1) max/min retrieval from deque heads
            highest_high = max_deque[0][0]
            lowest_low = min_deque[0][0]

            # Calculate Williams %R
            range_val = highest_high - lowest_low
            if range_val == 0:
                williams_r = -50.0  # Neutral value when no range
            else:
                williams_r = -100.0 * (highest_high - bar.close) / range_val

            result.append(williams_r)

        return result

    def update(self, bar: Bar) -> float | None:
        """
        Update Williams %R with new bar (stateful).

        Args:
            bar: New bar to process

        Returns:
            Current Williams %R value or None if not enough data
        """
        # Maintain monotonic decreasing deque for max (highest high)
        while self._max_deque and self._max_deque[-1][0] <= bar.high:
            self._max_deque.pop()
        self._max_deque.append((bar.high, self._index))

        # Maintain monotonic increasing deque for min (lowest low)
        while self._min_deque and self._min_deque[-1][0] >= bar.low:
            self._min_deque.pop()
        self._min_deque.append((bar.low, self._index))

        # Remove elements outside current window
        while self._max_deque and self._max_deque[0][1] <= self._index - self.period:
            self._max_deque.popleft()
        while self._min_deque and self._min_deque[0][1] <= self._index - self.period:
            self._min_deque.popleft()

        self._highs.append(bar.high)
        self._lows.append(bar.low)
        self._closes.append(bar.close)
        self._count += 1
        self._index += 1

        if self._count < self.period:
            return None

        # O(1) max/min retrieval from deque heads
        highest_high = self._max_deque[0][0]
        lowest_low = self._min_deque[0][0]

        # Calculate Williams %R
        range_val = highest_high - lowest_low
        if range_val == 0:
            return -50.0  # Neutral value when no range

        williams_r = -100.0 * (highest_high - bar.close) / range_val
        return williams_r

    def reset(self) -> None:
        """Reset indicator state."""
        self._highs.clear()
        self._lows.clear()
        self._closes.clear()
        self._count = 0
        self._max_deque.clear()
        self._min_deque.clear()
        self._index = 0

    @property
    def value(self) -> float | None:
        """
        Get current Williams %R value.

        Returns:
            Current Williams %R value or None if not enough data
        """
        if self._count < self.period:
            return None

        # O(1) max/min retrieval from deque heads
        highest_high = self._max_deque[0][0]
        lowest_low = self._min_deque[0][0]
        current_close = self._closes[-1]

        range_val = highest_high - lowest_low
        if range_val == 0:
            return -50.0

        williams_r = -100.0 * (highest_high - current_close) / range_val
        return williams_r

    @property
    def is_ready(self) -> bool:
        """Check if indicator has enough data."""
        return self._count >= self.period
