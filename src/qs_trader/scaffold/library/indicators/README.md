# Custom Indicators

Custom technical indicators for QS-Trader strategies.

## Overview

QS-Trader's indicator system is based on the `BaseIndicator` abstract class. Any custom indicator that inherits from `BaseIndicator` can be used in your strategies.

## Available Templates

### MyCustomIndicator

Single-value indicator template (e.g., moving averages, RSI).

**Features:**

- Simple moving average example implementation
- Stateful (`update()`) and stateless (`calculate()`) modes
- Warmup period handling
- Parameter validation
- Reset capability

**Usage:**

```python
from qs_trader.library.indicators.template import MyCustomIndicator

indicator = MyCustomIndicator(period=20, multiplier=1.0)

# Stateful mode (streaming)
for bar in bars:
    value = indicator.update(bar)
    if value is not None:
        print(f"Indicator: {value}")

# Stateless mode (batch)
values = indicator.calculate(bars)
```

### MyCustomMultiValueIndicator

Multi-value indicator template (e.g., Bollinger Bands, MACD).

**Features:**

- Multiple output values (upper, middle, lower bands example)
- Named tuple return type for clarity
- Same stateful/stateless interface
- Warmup period handling

**Usage:**

```python
from qs_trader.library.indicators.template import MyCustomMultiValueIndicator

indicator = MyCustomMultiValueIndicator(period=20, std_dev=2.0)

# Stateful mode
for bar in bars:
    result = indicator.update(bar)
    if result is not None:
        print(f"Upper: {result.upper}, Middle: {result.middle}, Lower: {result.lower}")

# Stateless mode
results = indicator.calculate(bars)
```

## Creating Custom Indicators

### Single-Value Indicator

```python
from qs_trader.libraries.indicators.base import BaseIndicator
from qs_trader.services.data.models import Bar

class MyRSI(BaseIndicator):
    """Relative Strength Index indicator."""

    def __init__(self, period: int = 14, **kwargs):
        self.period = period
        self._gains = []
        self._losses = []
        self._prev_close = None
        self._current_value = None

        if self.period < 1:
            raise ValueError(f"period must be >= 1, got {self.period}")

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        """Calculate RSI for list of bars (stateless)."""
        if not bars:
            return []

        results = []
        gains = []
        losses = []
        prev_close = None

        for i, bar in enumerate(bars):
            if prev_close is None:
                results.append(None)
                prev_close = bar.close
                continue

            # Calculate gain/loss
            change = bar.close - prev_close
            gain = max(change, 0)
            loss = max(-change, 0)

            gains.append(gain)
            losses.append(loss)

            # Need period bars for first calculation
            if len(gains) < self.period:
                results.append(None)
            else:
                # Calculate average gain/loss
                avg_gain = sum(gains[-self.period:]) / self.period
                avg_loss = sum(losses[-self.period:]) / self.period

                # Calculate RSI
                if avg_loss == 0:
                    rsi = 100.0
                else:
                    rs = avg_gain / avg_loss
                    rsi = 100 - (100 / (1 + rs))

                results.append(rsi)

            prev_close = bar.close

        return results

    def update(self, bar: Bar) -> float | None:
        """Update RSI with new bar (stateful)."""
        if self._prev_close is None:
            self._prev_close = bar.close
            return None

        # Calculate gain/loss
        change = bar.close - self._prev_close
        gain = max(change, 0)
        loss = max(-change, 0)

        self._gains.append(gain)
        self._losses.append(loss)

        # Need period bars for first calculation
        if len(self._gains) < self.period:
            self._prev_close = bar.close
            return None

        # Calculate average gain/loss
        avg_gain = sum(self._gains[-self.period:]) / self.period
        avg_loss = sum(self._losses[-self.period:]) / self.period

        # Calculate RSI
        if avg_loss == 0:
            self._current_value = 100.0
        else:
            rs = avg_gain / avg_loss
            self._current_value = 100 - (100 / (1 + rs))

        self._prev_close = bar.close
        return self._current_value

    def reset(self) -> None:
        """Reset indicator state."""
        self._gains.clear()
        self._losses.clear()
        self._prev_close = None
        self._current_value = None

    @property
    def value(self) -> float | None:
        """Get current RSI value."""
        return self._current_value

    @property
    def is_ready(self) -> bool:
        """Check if RSI is ready."""
        return len(self._gains) >= self.period
```

### Multi-Value Indicator

```python
from typing import NamedTuple
from qs_trader.libraries.indicators.base import BaseIndicator
from qs_trader.services.data.models import Bar

class MACDResult(NamedTuple):
    """MACD indicator result."""
    macd: float
    signal: float
    histogram: float

class MyMACD(BaseIndicator):
    """Moving Average Convergence Divergence indicator."""

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9, **kwargs):
        self.fast = fast
        self.slow = slow
        self.signal = signal

        self._fast_ema = []
        self._slow_ema = []
        self._macd_values = []
        self._current_value = None

        if not (1 <= fast < slow):
            raise ValueError(f"Expected 1 <= fast < slow, got fast={fast}, slow={slow}")

    def calculate(self, bars: list[Bar]) -> list[MACDResult | None]:
        """Calculate MACD for list of bars (stateless)."""
        # Implementation details...
        pass

    def update(self, bar: Bar) -> MACDResult | None:
        """Update MACD with new bar (stateful)."""
        # Implementation details...
        pass

    def reset(self) -> None:
        """Reset indicator state."""
        self._fast_ema.clear()
        self._slow_ema.clear()
        self._macd_values.clear()
        self._current_value = None

    @property
    def value(self) -> MACDResult | None:
        """Get current MACD value."""
        return self._current_value

    @property
    def is_ready(self) -> bool:
        """Check if MACD is ready."""
        return len(self._macd_values) >= self.signal
```

## BaseIndicator Interface

All indicators must implement these methods:

### Required Methods

- `__init__(**kwargs)`: Initialize indicator with parameters
- `calculate(bars: list[Bar]) -> list[T | None]`: Stateless calculation on bar list
- `update(bar: Bar) -> T | None`: Stateful incremental update
- `reset() -> None`: Clear internal state

### Required Properties

- `value -> T | None`: Get current value without updating
- `is_ready -> bool`: Check if indicator has enough data

### Method Patterns

**Stateless Mode (`calculate`)**:

- Does NOT modify internal state
- Processes entire bar list at once
- Useful for backtesting, optimization, bulk calculations
- Returns list of values (same length as input)

**Stateful Mode (`update`)**:

- Modifies internal state
- Processes one bar at a time
- Useful for live trading, streaming data
- Returns single current value

## Best Practices

1. **Return None during warmup**: Return `None` until enough data is collected
1. **Validate parameters**: Check parameter ranges in `__init__`
1. **Use efficient data structures**: Consider deque for fixed-size windows
1. **Document parameters**: Include parameter descriptions and defaults
1. **Add type hints**: Use proper return types (float, NamedTuple, etc.)
1. **Test both modes**: Verify stateful and stateless give same results
1. **Handle edge cases**: Division by zero, empty lists, negative values
1. **Keep state minimal**: Only store what's needed for next calculation

## Common Patterns

### Fixed-Size Window (SMA, EMA)

```python
from collections import deque

def __init__(self, period: int = 20):
    self.period = period
    self._prices = deque(maxlen=period)  # Auto-drops oldest
    self._current_value = None
```

### Cumulative Calculation (Cumulative Return)

```python
def __init__(self):
    self._initial_price = None
    self._current_value = None

def update(self, bar: Bar) -> float | None:
    if self._initial_price is None:
        self._initial_price = bar.close
        return 0.0

    self._current_value = (bar.close - self._initial_price) / self._initial_price
    return self._current_value
```

### Exponential Moving Average

```python
def __init__(self, period: int = 20):
    self.period = period
    self.alpha = 2 / (period + 1)
    self._ema = None

def update(self, bar: Bar) -> float | None:
    if self._ema is None:
        self._ema = bar.close
    else:
        self._ema = self.alpha * bar.close + (1 - self.alpha) * self._ema

    return self._ema
```

### True Range (uses previous bar)

```python
def __init__(self):
    self._prev_close = None
    self._current_value = None

def update(self, bar: Bar) -> float | None:
    if self._prev_close is None:
        self._prev_close = bar.close
        return None

    # True Range = max(high-low, |high-prev_close|, |low-prev_close|)
    tr1 = bar.high - bar.low
    tr2 = abs(bar.high - self._prev_close)
    tr3 = abs(bar.low - self._prev_close)

    self._current_value = max(tr1, tr2, tr3)
    self._prev_close = bar.close

    return self._current_value
```

## Using Indicators in Strategies

### Basic Usage

```python
class MyStrategy(BaseStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.sma = MyCustomIndicator(period=20)
        self.rsi = MyRSI(period=14)

    def on_bar(self, bar: Bar) -> list[Order]:
        # Update indicators
        sma_value = self.sma.update(bar)
        rsi_value = self.rsi.update(bar)

        # Wait for indicators to be ready
        if not self.sma.is_ready or not self.rsi.is_ready:
            return []

        # Generate signals
        if bar.close > sma_value and rsi_value < 30:
            # Buy signal
            return [self.create_market_order("BUY", 100)]

        return []
```

### Multi-Value Indicator Usage

```python
class BollingerStrategy(BaseStrategy):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.bb = MyCustomMultiValueIndicator(period=20, std_dev=2.0)

    def on_bar(self, bar: Bar) -> list[Order]:
        bands = self.bb.update(bar)

        if bands is None:
            return []

        # Use named tuple fields
        if bar.close < bands.lower:
            return [self.create_market_order("BUY", 100)]
        elif bar.close > bands.upper:
            return [self.create_market_order("SELL", 100)]

        return []
```

## Testing Indicators

```python
# Test stateless mode
indicator = MyCustomIndicator(period=20)
values = indicator.calculate(bars)
assert len(values) == len(bars)
assert values[:19] == [None] * 19  # Warmup period

# Test stateful mode
indicator = MyCustomIndicator(period=20)
for i, bar in enumerate(bars):
    value = indicator.update(bar)
    expected = values[i]

    if expected is None:
        assert value is None
    else:
        assert abs(value - expected) < 1e-6  # Float comparison

# Test reset
indicator.reset()
assert indicator.value is None
assert not indicator.is_ready
```

## Built-in Indicators

QS-Trader includes many built-in indicators in `qs_trader.libraries.indicators`:

- **Moving Averages**: SMA, EMA, WMA, HMA, KAMA
- **Momentum**: RSI, Stochastic, Williams %R, ROC
- **Trend**: ADX, Aroon, Parabolic SAR
- **Volatility**: ATR, Bollinger Bands, Keltner Channels
- **Volume**: OBV, VWAP, MFI

See `src/qs_trader/libraries/indicators/` for implementations.

## References

- Base class: `src/qs_trader/libraries/indicators/base.py`
- Built-in indicators: `src/qs_trader/libraries/indicators/`
- Template: `src/qs_trader/scaffold/library/indicators/template.py`
- Bar model: `src/qs_trader/services/data/models.py`
