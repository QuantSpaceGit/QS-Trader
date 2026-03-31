# Custom Strategies

Custom trading strategies for QS-Trader backtests.

## Overview

QS-Trader strategies inherit from the `Strategy` base class and use a type-safe configuration system. Strategies define trading logic (WHAT to trade), while the Risk Manager and Portfolio Manager handle position sizing and execution (HOW MUCH to trade).

## Available Example Strategies

### BuyAndHoldStrategy

The simplest possible strategy - buy on first bar and hold forever.

**Features:**

- Minimal implementation (no indicators)
- Single signal emission
- Demonstrates strategy basics
- Always max confidence

**Usage:**

```python
from qs_trader.library.strategies.buy_and_hold import BuyAndHoldStrategy, BuyAndHoldConfig

config = BuyAndHoldConfig(
    name="buy_and_hold",
    confidence=1.0
)
strategy = BuyAndHoldStrategy(config)
```

**Configuration in experiments/\*.yaml:**

```yaml
strategy:
  name: "buy_and_hold"
  config:
    confidence: 1.0
```

### SMACrossover

Dual moving average crossover strategy (long/short).

**Strategy Logic:**

- OPEN_LONG when fast SMA crosses above slow SMA (golden cross)
- CLOSE_LONG + OPEN_SHORT when fast SMA crosses below slow SMA (death cross)
- Always in the market (either long or short)

**Features:**

- Uses `context.get_bars()` for historical data
- Tracks position direction internally
- Self-managed warmup period
- Demonstrates indicator calculation

**Usage:**

```python
from qs_trader.library.strategies.sma_crossover import SMACrossover, SMAConfig

config = SMAConfig(
    name="sma_crossover",
    fast_period=10,
    slow_period=50,
    confidence=1.0
)
strategy = SMACrossover(config)
```

**Configuration in experiments/\*.yaml:**

```yaml
strategy:
  name: "sma_crossover"
  config:
    fast_period: 10
    slow_period: 50
    confidence: 1.0
```

## Creating Custom Strategies

### Basic Strategy Template

```python
from decimal import Decimal
from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.strategy.models import SignalIntention


class MyStrategyConfig(StrategyConfig):
    """Configuration for custom strategy."""

    name: str = "my_strategy"
    display_name: str = "My Custom Strategy"
    universe: list[str] = []  # Apply to all symbols

    # Strategy-specific parameters
    lookback_period: int = 20
    threshold: Decimal = Decimal("0.05")
    confidence: Decimal = Decimal("0.8")


class MyStrategy(Strategy[MyStrategyConfig]):
    """
    Custom strategy implementation.

    Strategy Logic:
    - Describe your strategy logic here
    - Entry conditions
    - Exit conditions
    """

    def __init__(self, config: MyStrategyConfig):
        """Initialize strategy with configuration."""
        super().__init__(config)
        # Initialize state (if needed)
        self._positions = {}  # symbol -> position info

    def setup(self, context: Context) -> None:
        """
        Optional setup phase (called once before first bar).

        Use for:
        - Initializing indicators
        - Loading external data
        - Validating configuration
        """
        pass

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """
        Process price bar and generate signals.

        Args:
            event: Price bar event with OHLCV data
            context: Execution context for data access and signal emission
        """
        symbol = event.symbol

        # Get historical bars for indicator calculation
        bars = context.get_bars(symbol, n=self.config.lookback_period)

        # Self-managed warmup: check if we have enough data
        if bars is None or len(bars) < self.config.lookback_period:
            return

        # Calculate indicators
        prices = [bar.close for bar in bars]
        sma = sum(prices) / len(prices)

        # Get current position (if tracking)
        current_position = self._positions.get(symbol)

        # Trading logic
        if event.close > sma and current_position != "long":
            context.emit_signal(
                symbol=symbol,
                intention=SignalIntention.OPEN_LONG,
                confidence=self.config.confidence,
                metadata={
                    "sma": float(sma),
                    "price": float(event.close),
                    "reason": "price_above_sma"
                }
            )
        elif event.close < sma and current_position != "short":
            # Close long if we have one
            if current_position == "long":
                context.emit_signal(
                    symbol=symbol,
                    intention=SignalIntention.CLOSE_LONG,
                    confidence=self.config.confidence
                )

            # Open short
            context.emit_signal(
                symbol=symbol,
                intention=SignalIntention.OPEN_SHORT,
                confidence=self.config.confidence,
                metadata={"reason": "price_below_sma"}
            )

    def on_position_filled(self, event, context: Context) -> None:
        """
        Optional: Track position changes from fills.

        Called whenever a position is opened, closed, or modified.
        Use this to keep strategy state in sync with actual portfolio.

        Args:
            event: FillEvent with symbol, side, quantity, price
            context: Execution context
        """
        symbol = event.symbol
        side = event.side  # "buy" or "sell"

        # Update position tracking
        if side == "buy":
            self._positions[symbol] = "long"
        elif side == "sell":
            if event.quantity == 0:  # Position closed
                self._positions[symbol] = None
            else:
                self._positions[symbol] = "short"

    def teardown(self, context: Context) -> None:
        """
        Optional cleanup phase (called once after last bar).

        Use for:
        - Saving state
        - Closing connections
        - Logging final statistics
        """
        pass


# Export config for auto-discovery
CONFIG = MyStrategyConfig()
```

### Strategy with Indicators

```python
from qs_trader.libraries.indicators import SMA, RSI
from qs_trader.libraries.strategies import Strategy, StrategyConfig, Context
from qs_trader.events.events import PriceBarEvent
from qs_trader.services.strategy.models import SignalIntention


class RSIMeanReversionConfig(StrategyConfig):
    """Configuration for RSI mean reversion strategy."""

    name: str = "rsi_mean_reversion"
    display_name: str = "RSI Mean Reversion"

    # Indicator parameters
    rsi_period: int = 14
    rsi_oversold: int = 30
    rsi_overbought: int = 70
    sma_period: int = 200

    confidence: float = 0.9


class RSIMeanReversion(Strategy[RSIMeanReversionConfig]):
    """
    RSI mean reversion with SMA filter.

    Entry Logic:
    - Buy when RSI < 30 and price > SMA200 (oversold in uptrend)
    - Sell when RSI > 70 and price < SMA200 (overbought in downtrend)

    Exit Logic:
    - Close long when RSI > 50
    - Close short when RSI < 50
    """

    def __init__(self, config: RSIMeanReversionConfig):
        super().__init__(config)

        # Initialize indicators per symbol
        self._indicators = {}  # symbol -> dict of indicators
        self._positions = {}  # symbol -> position state

    def _get_indicators(self, symbol: str):
        """Get or create indicators for symbol."""
        if symbol not in self._indicators:
            self._indicators[symbol] = {
                "rsi": RSI(period=self.config.rsi_period),
                "sma": SMA(period=self.config.sma_period)
            }
        return self._indicators[symbol]

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """Process bar and generate mean reversion signals."""
        symbol = event.symbol

        # Update indicators
        indicators = self._get_indicators(symbol)
        rsi_value = indicators["rsi"].update(event)
        sma_value = indicators["sma"].update(event)

        # Wait for indicators to warm up
        if rsi_value is None or sma_value is None:
            return

        current_position = self._positions.get(symbol)

        # Entry signals
        if current_position is None:
            # Buy signal: oversold in uptrend
            if rsi_value < self.config.rsi_oversold and event.close > sma_value:
                context.emit_signal(
                    symbol=symbol,
                    intention=SignalIntention.OPEN_LONG,
                    confidence=self.config.confidence,
                    metadata={
                        "rsi": rsi_value,
                        "sma": float(sma_value),
                        "reason": "oversold_in_uptrend"
                    }
                )

            # Sell signal: overbought in downtrend
            elif rsi_value > self.config.rsi_overbought and event.close < sma_value:
                context.emit_signal(
                    symbol=symbol,
                    intention=SignalIntention.OPEN_SHORT,
                    confidence=self.config.confidence,
                    metadata={
                        "rsi": rsi_value,
                        "reason": "overbought_in_downtrend"
                    }
                )

        # Exit signals
        else:
            if current_position == "long" and rsi_value > 50:
                context.emit_signal(
                    symbol=symbol,
                    intention=SignalIntention.CLOSE_LONG,
                    confidence=self.config.confidence,
                    metadata={"rsi": rsi_value, "reason": "rsi_normalized"}
                )

            elif current_position == "short" and rsi_value < 50:
                context.emit_signal(
                    symbol=symbol,
                    intention=SignalIntention.CLOSE_SHORT,
                    confidence=self.config.confidence,
                    metadata={"rsi": rsi_value, "reason": "rsi_normalized"}
                )

    def on_position_filled(self, event, context: Context) -> None:
        """Track position changes."""
        symbol = event.symbol
        if event.side == "buy":
            self._positions[symbol] = "long" if event.quantity > 0 else None
        elif event.side == "sell":
            self._positions[symbol] = "short" if event.quantity < 0 else None

# This is needed for auto-discovery
CONFIG = RSIMeanReversionConfig()
```

## Strategy Base Class

### Required Methods

- `__init__(config: TConfig)`: Initialize with configuration
- `on_bar(event: PriceBarEvent, context: Context) -> None`: Process bars and generate signals

### Optional Methods

- `setup(context: Context) -> None`: One-time setup before first bar
- `on_position_filled(event, context: Context) -> None`: Track position changes
- `teardown(context: Context) -> None`: Cleanup after last bar

### Configuration Class

All strategies need a configuration class inheriting from `StrategyConfig`:

```python
class MyConfig(StrategyConfig):
    # Identity (required)
    name: str = "my_strategy"
    display_name: str = "My Strategy"

    # Metadata (optional but recommended)
    description: str = "Strategy description"
    author: str = "Your Name"
    version: str = "1.0.0"

    # Strategy-specific parameters
    parameter1: int = 20
    parameter2: float = 0.05
    confidence: float = 0.8
```

## Context API

The `Context` object provides access to data and signal emission:

### Data Access

```python
# Get historical bars
bars = context.get_bars(symbol, n=50)  # Last 50 bars
if bars is None or len(bars) < 50:
    return  # Not enough data yet

# Get current price
price = context.get_price(symbol)

# Get position (if tracking)
position = context.get_position(symbol)
```

### Signal Emission

```python
# Emit trading signal
context.emit_signal(
    symbol="AAPL",
    intention=SignalIntention.OPEN_LONG,  # or OPEN_SHORT, CLOSE_LONG, CLOSE_SHORT
    confidence=0.8,  # 0.0 to 1.0
    metadata={"reason": "golden_cross", "fast_sma": 105.5}  # Optional metadata
)
```

### Signal Intentions

- `SignalIntention.OPEN_LONG`: Open long position
- `SignalIntention.CLOSE_LONG`: Close long position
- `SignalIntention.OPEN_SHORT`: Open short position
- `SignalIntention.CLOSE_SHORT`: Close short position

## Best Practices

1. **Separate logic from parameters**: Use config for all tunable values
1. **Self-manage warmup**: Check if `get_bars()` returns enough data
1. **Don't calculate position sizes**: Emit signals with confidence, let Risk Manager size
1. **Track positions if needed**: Use `on_position_filled()` to stay in sync
1. **Add metadata to signals**: Include reasoning for debugging and analysis
1. **Validate config parameters**: Check ranges in `__init__`
1. **Use type hints**: Enable type-safe config access with `Strategy[TConfig]`
1. **Test both stateful and stateless**: Ensure strategy works with and without position tracking

## Common Patterns

### Trend Following

```python
def on_bar(self, event: PriceBarEvent, context: Context) -> None:
    bars = context.get_bars(event.symbol, n=50)
    if bars is None or len(bars) < 50:
        return

    # Calculate trend
    fast_sma = sum(b.close for b in bars[-10:]) / 10
    slow_sma = sum(b.close for b in bars) / 50

    if fast_sma > slow_sma:
        context.emit_signal(
            symbol=event.symbol,
            intention=SignalIntention.OPEN_LONG,
            confidence=0.8
        )
```

### Mean Reversion

```python
def on_bar(self, event: PriceBarEvent, context: Context) -> None:
    bars = context.get_bars(event.symbol, n=20)
    if bars is None or len(bars) < 20:
        return

    # Calculate deviation from mean
    prices = [b.close for b in bars]
    mean = sum(prices) / len(prices)
    std_dev = (sum((p - mean) ** 2 for p in prices) / len(prices)) ** 0.5

    z_score = (event.close - mean) / std_dev

    if z_score < -2:  # Oversold
        context.emit_signal(
            symbol=event.symbol,
            intention=SignalIntention.OPEN_LONG,
            confidence=min(abs(z_score) / 3, 1.0)
        )
    elif z_score > 2:  # Overbought
        context.emit_signal(
            symbol=event.symbol,
            intention=SignalIntention.OPEN_SHORT,
            confidence=min(abs(z_score) / 3, 1.0)
        )
```

### Breakout

```python
def on_bar(self, event: PriceBarEvent, context: Context) -> None:
    bars = context.get_bars(event.symbol, n=20)
    if bars is None or len(bars) < 20:
        return

    # Find highest high and lowest low
    high_20 = max(b.high for b in bars)
    low_20 = min(b.low for b in bars)

    # Breakout signals
    if event.close > high_20:
        context.emit_signal(
            symbol=event.symbol,
            intention=SignalIntention.OPEN_LONG,
            confidence=0.9,
            metadata={"breakout_level": float(high_20)}
        )
    elif event.close < low_20:
        context.emit_signal(
            symbol=event.symbol,
            intention=SignalIntention.OPEN_SHORT,
            confidence=0.9,
            metadata={"breakout_level": float(low_20)}
        )
```

## Testing Strategies

```python
# Create test configuration
config = MyStrategyConfig(
    name="test_strategy",
    lookback_period=20,
    threshold=0.05
)

# Initialize strategy
strategy = MyStrategy(config)

# Create mock context and events
from unittest.mock import Mock

context = Mock(spec=Context)
context.get_bars.return_value = test_bars

event = PriceBarEvent(
    symbol="AAPL",
    timestamp="2024-01-01T16:00:00-05:00",
    open=100.0,
    high=101.0,
    low=99.0,
    close=100.5,
    volume=1000000
)

# Test strategy
strategy.on_bar(event, context)

# Verify signals emitted
assert context.emit_signal.called
```

## Configuration in Backtests

In your experiment config (`experiments/my_backtest.yaml`):

```yaml
backtest:
  name: "my_strategy_test"
  start_date: "2020-01-01"
  end_date: "2023-12-31"
  initial_capital: 100000.0

data:
  sources:
    - name: "yahoo-us-equity-1d-csv"
      universe: ["AAPL", "MSFT", "GOOGL"]

strategy:
  name: "my_strategy"  # Must match config.name
  config:
    lookback_period: 20
    threshold: 0.05
    confidence: 0.8

risk_policy:
  name: "conservative"
```

## Strategy Discovery

Strategies are automatically discovered if:

1. They inherit from `Strategy[TConfig]`
1. They are in a Python file in `library/strategies/`
1. They export a `CONFIG` instance

```python
# At the end of your strategy file:
CONFIG = MyStrategyConfig()
```

## References

- Base class: `src/qs_trader/libraries/strategies/base.py`
- Context API: `src/qs_trader/libraries/strategies/context.py`
- Example strategies: `src/qs_trader/scaffold/library/strategies/`
- Signal models: `src/qs_trader/services/strategy/models.py`
- Built-in indicators: `src/qs_trader/libraries/indicators/`
