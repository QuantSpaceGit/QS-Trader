# Strategy Package Documentation

**Package**: `qs_trader.services.strategy`\
**Purpose**: Event-driven strategy execution service with auto-discovery and lifecycle management\
**Status**: Production Ready

______________________________________________________________________

## Overview

The strategy package provides a complete strategy execution framework for QS-Trader. It orchestrates strategy lifecycle, manages execution context, and routes market data and fill events to registered strategies.

**Key Features**:

- **Auto-discovery**: Automatic strategy registration from builtin and custom libraries
- **Lifecycle management**: Setup, on_bar, on_position_filled, teardown coordination
- **Context management**: Bar caching, historical data access, signal emission
- **Quarantine system**: Isolate failed strategies without stopping execution
- **Universe filtering**: Route events only to relevant strategies
- **Fill routing**: Multi-strategy support with strategy_id attribution
- **Subscription cleanup**: Proper event handler teardown
- **Self-managed warmup**: Strategies control their own initialization
- **Event-driven**: Pure publish/subscribe architecture

## Current Strategy Contract (Phase 6)

The live strategy API is stricter than many historical snippets in this document. If a lower example shows direct `PriceBarEvent` OHLC access or `get_bars(n)` without an explicit basis, treat it as historical background rather than the current contract.

- Backtests declare one run-level `price_basis: adjusted|raw`; legacy `adjustment_mode`, `strategy_adjustment_mode`, and `portfolio_adjustment_mode` inputs are rejected.
- Strategy code asks the context for basis-resolved data via `get_bars(symbol, n, basis=...)`, `get_price_series(symbol, n, basis=..., offset=...)`, and `get_price(symbol, basis=...)`.
- Those helpers return basis-resolved `BarView` values, so strategies no longer choose between raw and adjusted OHLC attributes manually.
- Prior-window calculations use `offset` on `get_price_series(...)` instead of manual slice tricks such as `bars[:-2]`.
- `context.get_position_state(symbol)` exposes `PositionState` (`FLAT`, `PENDING_OPEN_LONG`, `OPEN_LONG`, `PENDING_CLOSE_LONG`, `PENDING_OPEN_SHORT`, `OPEN_SHORT`, `PENDING_CLOSE_SHORT`).
- Manager-side lifecycle gating is authoritative: same-side opens are suppressed whenever the lifecycle projection already shows a compatible pending/open intent. Strategies should still consult `get_position_state()` so duplicates are avoided by construction.

```python
from decimal import Decimal

from qs_trader.events.price_basis import PriceBasis
from qs_trader.libraries.strategies.base import BaseStrategy


class ExampleStrategy(BaseStrategy):
    def on_bar(self, bar) -> None:
        prices = self.context.get_price_series(
            bar.symbol,
            n=50,
            basis=PriceBasis.ADJUSTED,
        )
        if prices is None:
            return

        previous_prices = self.context.get_price_series(
            bar.symbol,
            n=50,
            basis=PriceBasis.ADJUSTED,
            offset=1,
        )
        position_state = self.context.get_position_state(bar.symbol)
        if previous_prices is None or position_state.name != "FLAT":
            return

        self.emit_signal(
            symbol=bar.symbol,
            direction="buy",
            target_pct_allocation=Decimal("0.10"),
        )
```

______________________________________________________________________

## Architecture Philosophy

### Event-Driven Orchestration

```python
# Services subscribe to events (loose coupling)
strategy_service.subscribe_to_events(event_bus)

# DataService publishes bars
event_bus.publish(PriceBarEvent(symbol="AAPL", ...))

# StrategyService routes to relevant strategies
# Each strategy processes bar and emits signals
```

**Benefits**:

- No direct dependencies between services
- Easy to add/remove strategies without code changes
- Strategies are isolated (one failure doesn't affect others)
- Clear data flow through events

### Strategy Auto-Discovery

**Current Implementation**:

```python
# Auto-discovery happens in BacktestEngine.from_config()
# Strategies are loaded from configured custom_libraries path
custom_strategies_path = Path("library/strategies")  # Or path from qs_trader.yaml

strategy_registry = StrategyRegistry()
strategies = strategy_registry.load_from_directory(custom_strategies_path, recursive=False)

# Result: {"buy_and_hold": (BuyAndHoldStrategy, config), ...}
```

**Configuration** (`config/qs_trader.yaml`):

```yaml
# Custom library paths
custom_libraries:
  strategies: "library/strategies" # Path to your custom strategies
  indicators: null # null = use built-in only
  risk_policies: null # null = use built-in only
  adapters: null # null = use built-in only
  metrics: null # null = use built-in only
```

**Note**: Use `null` for components you don't customize. QS-Trader will only load built-in components. Set paths only for components you've created using `qs-trader init-library` or `qs-trader init-project`.

**Discovery Process**:

1. **Scan directory** for `.py` files (excluding `__init__.py`)
1. **Import module** dynamically
1. **Find Strategy subclasses** that inherit from `Strategy`
1. **Extract StrategyConfig** from the same file (looks for `CONFIG` variable)
1. **Validate uniqueness** of `config.name`
1. **Register** strategy class and config

**Convention Over Configuration**:

- No YAML registration files needed
- Just create a `.py` file with:
  - A class inheriting from `Strategy`
  - A `CONFIG = StrategyConfig(...)` instance
  - Both in the same file

### Self-Managed Warmup

**Old Approach** (centralized):

```python
# Engine coordinated warmup
engine.warmup_strategies(bars=20)
strategy.on_bar(bar)  # After warmup complete
```

**New Approach** (self-managed):

```python
def on_bar(self, bar: PriceBarEvent) -> None:
    # Check if we have enough data
    if self.get_bars(self._lookback) is None:
        return  # Not ready yet

    # Calculate indicator
    bars = self.get_bars(self._lookback)
    sma = sum(b.close for b in bars) / len(bars)

    # Generate signal
    if bar.close > sma:
        self.emit_signal(...)
```

**Benefits**:

- Each strategy controls its own warmup requirements
- No centralized coordination needed
- More flexible (different strategies need different warmup)
- Simpler architecture

### Context Enhancement

**Bar Caching**:

```python
# Service caches bars before calling on_bar()
self._contexts[name].cache_bar(bar)
strategy.on_bar(bar)
```

**Strategy Access**:

```python
# Get latest price
price = self.get_price()  # O(1) lookup

# Get N historical bars
bars = self.get_bars(20)  # Returns last 20 bars or None
if bars is None:
    return  # Not enough data yet

# Calculate indicator
sma = sum(b.close for b in bars) / len(bars)
```

______________________________________________________________________

## Package Structure

```
src/qs_trader/services/strategy/
├── __init__.py              # Public API exports
├── service.py               # StrategyService orchestrator
├── context.py               # StrategyContext per-strategy state
├── interface.py             # IStrategyService protocol
└── config_loader.py         # Configuration loading

src/qs_trader/libraries/
├── registry.py              # Registry management
└── strategies/
    ├── __init__.py
    ├── base.py              # BaseStrategy abstract class
    ├── loader.py            # Strategy discovery and loading
    ├── buy_and_hold.py      # Example: Buy and hold
    ├── bollinger_breakout.py  # Example: Bollinger bands
    └── sma_crossover.py     # Example: SMA crossover

library/strategies/          # User custom strategies (auto-discovered)
├── momentum.py              # Each file has CONFIG + Strategy class
└── pairs_trading.py         # No YAML registration needed
```

______________________________________________________________________

## Module: service.py

Main orchestrator for strategy execution.

### Classes

#### StrategyService

Event-driven strategy orchestrator with lifecycle management.

**Purpose**: Coordinate strategy execution, route events, manage context.

```python
from qs_trader.services.strategy.service import StrategyService
from qs_trader.events.event_bus import EventBus

# Initialize service
bus = EventBus()
service = StrategyService(
    strategies={"momentum_20": momentum_strategy},
    universe=["AAPL", "MSFT"],
    event_bus=bus,
    max_bars=500  # Bar cache size (default: 500)
)

# Subscribe to events
service.subscribe_to_events(bus)

# Publish bars (service routes to strategies)
bus.publish(PriceBarEvent(symbol="AAPL", ...))

# Teardown (cleanup subscriptions)
service.teardown()
```

**Constructor Parameters**:

- `strategies` (dict[str, BaseStrategy]): Strategy instances by name
- `universe` (list[str]): Symbols to route to strategies (empty = all symbols)
- `event_bus` (IEventBus): EventBus for publishing signals
- `max_bars` (int): Max bars to cache per symbol (default: 500)

**Key Attributes**:

- `_strategies` (dict[str, BaseStrategy]): Strategy instances
- `_contexts` (dict[str, StrategyContext]): Context per strategy
- `_universe` (set[str]): Symbols to route (empty = all)
- `_quarantined` (set[str]): Failed strategy names
- `_subscription_tokens` (list[SubscriptionToken]): Event subscriptions

______________________________________________________________________

### Lifecycle Management

#### setup()

Initialize all strategies with their contexts.

```python
service.setup()
```

**Flow**:

1. Check if strategy is quarantined (skip if so)
1. Try to call `strategy.on_setup(context)`
1. On success: Remove from quarantine (retry recovery)
1. On failure: Add to quarantine, log error, continue

**Quarantine Recovery**:

```python
# First run - strategy fails setup
service.setup()  # Strategy added to quarantine

# Fix strategy code
# ...

# Second run - strategy succeeds setup
service.setup()  # Strategy removed from quarantine
```

**Error Handling**:

- Failed strategies are quarantined (not terminated)
- Other strategies continue setup
- Metrics track quarantine status

______________________________________________________________________

#### on_bar()

Process price bar event for relevant strategies.

```python
# Called automatically via event subscription
bus.publish(PriceBarEvent(symbol="AAPL", ...))
```

**Flow**:

1. Check universe filtering (skip if not in universe)
1. Check quarantine status (skip if quarantined)
1. Cache bar in strategy context
1. Try to call `strategy.on_bar(bar)`
1. On error: Log and continue (don't quarantine on bar error)

**Universe Filtering**:

```python
# Empty universe = all symbols
service = StrategyService(universe=[], ...)
# Receives bars for: AAPL, MSFT, GOOGL, etc.

# Non-empty universe = filtered
service = StrategyService(universe=["AAPL", "MSFT"], ...)
# Receives bars for: AAPL, MSFT only
```

**Performance**:

- O(1) quarantine check
- O(1) universe check (set lookup)
- O(1) bar caching (deque append)

______________________________________________________________________

#### on_fill()

Process fill event with multi-strategy routing.

```python
# Called automatically via event subscription
bus.publish(FillEvent(
    strategy_id="momentum_20",  # Routes to specific strategy
    symbol="AAPL",
    ...
))
```

**Routing Logic**:

1. **Strategy ID-based** (preferred):

   - If `fill.strategy_id` is set, route to that strategy only
   - Enables multi-strategy portfolios with proper attribution
   - Example: Multiple strategies trade same symbol

1. **Universe-based** (fallback):

   - If no `strategy_id`, route to all strategies in universe
   - Used for single-strategy portfolios
   - Universe filtering still applies

**Flow**:

1. Determine target strategies (strategy_id or universe)
1. For each target strategy:
   - Check if strategy has `on_position_filled` method
   - Check quarantine status
   - Try to call `strategy.on_position_filled(fill)`
   - On error: Log and continue

**Example: Multi-Strategy**:

```python
# Strategy A trades AAPL and MSFT
strategy_a = MomentumStrategy(universe=["AAPL", "MSFT"])

# Strategy B trades AAPL and GOOGL
strategy_b = MeanReversionStrategy(universe=["AAPL", "GOOGL"])

# Fill for AAPL from Strategy A
fill = FillEvent(
    strategy_id="momentum_20",  # Routes to Strategy A only
    symbol="AAPL",
    ...
)
```

______________________________________________________________________

#### teardown()

Clean up resources and unsubscribe from events.

```python
try:
    service.setup()
    # ... run backtest
finally:
    service.teardown()  # Always cleanup
```

**Flow**:

1. Unsubscribe from all events (using stored tokens)
1. Call `strategy.on_teardown()` for each strategy
1. Clear subscription tokens

**Why This Matters**:

- Prevents duplicate event handlers on service recreation
- Releases resources (file handles, connections)
- Essential for long-lived daemons or repeated runs

______________________________________________________________________

### Metrics

#### get_metrics()

Get service metrics including per-strategy stats.

```python
metrics = service.get_metrics()

print(f"Total bars: {metrics['total_bars_processed']}")
print(f"Total signals: {metrics['total_signals_emitted']}")

for name, stats in metrics['strategies'].items():
    print(f"{name}: {stats['bars_processed']} bars, {stats['signals_emitted']} signals")
    if stats['quarantined']:
        print(f"  ⚠ Quarantined due to setup failure")
```

**Metrics Structure**:

```python
{
    "total_bars_processed": int,
    "total_signals_emitted": int,
    "total_errors": int,
    "quarantined_count": int,
    "strategies": {
        "strategy_name": {
            "bars_processed": int,
            "signals_emitted": int,
            "errors": int,
            "quarantined": bool
        }
    }
}
```

______________________________________________________________________

## Module: context.py

Per-strategy execution context with bar caching and data access.

### Classes

#### StrategyContext

Execution context for a single strategy.

**Purpose**: Provide strategies with bar caching, historical data access, and signal emission.

```python
from qs_trader.services.strategy.context import StrategyContext
from qs_trader.events.event_bus import EventBus

context = StrategyContext(
    strategy_id="momentum_20",
    event_bus=bus,
    max_bars=500  # Cache last 500 bars per symbol
)

# Cache bars (called by service)
context.cache_bar(bar)

# Strategy access
price = context.get_price()
bars = context.get_bars(20)
context.emit_signal(...)
```

**Constructor Parameters**:

- `strategy_id` (str): Strategy identifier
- `event_bus` (IEventBus): EventBus for publishing signals
- `max_bars` (int): Max bars to cache per symbol (default: 500)

**Key Attributes**:

- `_bar_cache` (dict\[str, deque[PriceBarEvent]\]): Bar cache per symbol
- `_signal_count` (int): Number of signals emitted
- `_event_bus` (IEventBus): EventBus reference

______________________________________________________________________

### Bar Caching

#### cache_bar()

Cache bar for historical access.

```python
# Called by service before on_bar()
context.cache_bar(bar)
strategy.on_bar(bar)
```

**Implementation**:

```python
def cache_bar(self, bar: PriceBarEvent) -> None:
    """Cache bar for get_price() and get_bars() access."""
    if bar.symbol not in self._bar_cache:
        self._bar_cache[bar.symbol] = deque(maxlen=self._max_bars)
    self._bar_cache[bar.symbol].append(bar)
```

**Features**:

- Automatic windowing (oldest bars dropped when maxlen reached)
- O(1) append performance
- Per-symbol caching

**Memory Usage**:

- ~500 bytes per cached bar (PriceBarEvent in memory)
- 500 bars × 10 symbols × 500 bytes = ~2.5 MB (manageable)
- 500 bars × 100 symbols × 500 bytes = ~25 MB (moderate)

______________________________________________________________________

### Data Access

#### get_price()

Get latest close price for symbol.

```python
price = context.get_price()

if price is None:
    return  # No data yet

if current_price > threshold:
    self.emit_signal(...)
```

**Returns**:

- `Decimal`: Latest close price
- `None`: No bars cached yet

**Performance**: O(1) - accesses last element of deque

______________________________________________________________________

#### get_bars()

Get N most recent bars in chronological order.

```python
bars = context.get_bars(20)

if bars is None:
    return  # Not enough data yet (self-managed warmup)

# Calculate SMA
sma = sum(b.close for b in bars) / len(bars)
```

**Parameters**:

- `n` (int): Number of bars to retrieve

**Returns**:

- `list[PriceBarEvent]`: Last N bars in chronological order (oldest first)
- `None`: Fewer than N bars cached

**Self-Managed Warmup**:

```python
def on_bar(self, bar: PriceBarEvent) -> None:
    # Check warmup
    if self.get_bars(self._lookback) is None:
        return  # Not ready yet

    # Ready to trade
    bars = self.get_bars(self._lookback)
    # ... calculate indicator
```

**Performance**: O(n) - slices deque and reverses

______________________________________________________________________

### Signal Emission

#### emit_signal()

Publish trading signal to EventBus.

```python
context.emit_signal(
    symbol="AAPL",
    direction="buy",
    target_pct_allocation=0.10,
    metadata={"reason": "breakout"}
)
```

**Parameters**:

- `symbol` (str): Symbol to trade
- `direction` (str): "buy", "sell", or "close"
- `target_pct_allocation` (Decimal): Target portfolio percentage
- `metadata` (dict, optional): Additional context

**Implementation**:

```python
def emit_signal(
    self,
    symbol: str,
    direction: str,
    target_pct_allocation: Decimal,
    metadata: dict | None = None,
) -> None:
    """Emit trading signal to EventBus."""
    signal = SignalEvent(
        event_type="signal",
        source_service="strategy_service",
        strategy_id=self.strategy_id,
        symbol=symbol,
        direction=direction,
        target_pct_allocation=target_pct_allocation,
        metadata=metadata or {},
    )
    self._event_bus.publish(signal)
    self._signal_count += 1
```

**Tracking**:

- `_signal_count` incremented on each emission
- Used for metrics reporting

______________________________________________________________________

## Module: base.py

Abstract base class for all strategies.

### Classes

#### BaseStrategy

Abstract strategy class defining lifecycle contract.

**Purpose**: Enforce strategy interface and provide common functionality.

```python
from qs_trader.libraries.strategies.base import BaseStrategy
from qs_trader.services.strategy.context import StrategyContext

class MyStrategy(BaseStrategy):
    """Custom trading strategy."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._lookback = config.get("lookback", 20)

    def on_setup(self, context: StrategyContext) -> None:
        """Initialize strategy with context."""
        self.context = context

    def on_bar(self, bar: PriceBarEvent) -> None:
        """Process price bar."""
        # Check warmup
        if self.get_bars(self._lookback) is None:
            return

        # Calculate indicator and emit signal
        bars = self.get_bars(self._lookback)
        # ...
```

**Required Methods**:

- `on_setup(context)`: Initialize with context
- `on_bar(bar)`: Process price bar

**Optional Methods**:

- `on_position_filled(fill)`: Track position changes (event-driven)
- `on_teardown()`: Cleanup resources

______________________________________________________________________

### Lifecycle Methods

#### on_setup()

Initialize strategy with execution context.

```python
def on_setup(self, context: StrategyContext) -> None:
    """Store context reference."""
    self.context = context
```

**Purpose**: Store context for later access to `get_price()`, `get_bars()`, `emit_signal()`

**Called**: Once at strategy initialization

______________________________________________________________________

#### on_bar()

Process incoming price bar.

```python
def on_bar(self, bar: PriceBarEvent) -> None:
    """Process bar and generate signals."""
    # Check warmup
    if self.get_bars(self._lookback) is None:
        return

    # Calculate indicator
    bars = self.get_bars(self._lookback)
    sma = sum(b.close for b in bars) / len(bars)

    # Generate signal
    if bar.close > sma:
        self.emit_signal(
            symbol=bar.symbol,
            direction="buy",
            target_pct_allocation=Decimal("0.10")
        )
```

**Called**: For each bar event matching strategy universe

______________________________________________________________________

#### on_position_filled() (optional)

Track position changes via fill events.

```python
def on_position_filled(self, fill: FillEvent) -> None:
    """Track position changes."""
    # Update internal position tracking
    if fill.side == "buy":
        self._position += fill.filled_quantity
    else:
        self._position -= fill.filled_quantity
```

**Purpose**: Event-driven position tracking (replaces query-based `get_position()`)

**Called**: After order fills

**Benefits**:

- Event-driven (no need to query portfolio)
- Optional (only implement if needed)
- Cleaner separation of concerns

______________________________________________________________________

#### on_teardown() (optional)

Clean up strategy resources.

```python
def on_teardown(self) -> None:
    """Release resources."""
    if hasattr(self, "_file_handle"):
        self._file_handle.close()
```

**Purpose**: Release resources (file handles, connections, etc.)

**Called**: Once at strategy shutdown

______________________________________________________________________

### Context Proxy Methods

BaseStrategy proxies context methods for convenience:

```python
class BaseStrategy(ABC):
    def get_price(self) -> Decimal | None:
        """Proxy to context.get_price()."""
        return self.context.get_price()

    def get_bars(self, n: int) -> list[PriceBarEvent] | None:
        """Proxy to context.get_bars()."""
        return self.context.get_bars(n)

    def emit_signal(self, symbol: str, direction: str, ...) -> None:
        """Proxy to context.emit_signal()."""
        self.context.emit_signal(symbol, direction, ...)
```

**Usage**:

```python
# Direct context access
price = self.context.get_price()

# Or use proxy method (same result)
price = self.get_price()
```

______________________________________________________________________

## Module: loader.py

Strategy discovery and loading via auto-discovery.

### Classes

#### StrategyLoader

Auto-discovers Strategy classes from Python files.

```python
from qs_trader.libraries.strategies.loader import StrategyLoader
from pathlib import Path

loader = StrategyLoader()
strategies = loader.load_from_directory(
    directory=Path("library/strategies"),
    recursive=False  # Don't scan subdirectories
)

# Result: {"momentum_20": (MomentumStrategy, config), ...}
```

**Key Methods**:

- `load_from_directory(directory, recursive)`: Auto-discover strategies
- `get_strategy_names()`: List loaded strategy names
- `clear()`: Clear loaded strategies

**Discovery Process**:

1. Scan directory for `*.py` files
1. Skip `__init__.py` and `__pycache__`
1. Dynamically import each module
1. Find classes inheriting from `Strategy`
1. Find `StrategyConfig` instance (looks for `CONFIG` variable)
1. Validate `config.name` uniqueness
1. Return dict mapping name to (class, config)

**Error Handling**:

- Logs warnings for files that fail to load
- Continues with other files (doesn't stop on error)
- Raises `StrategyLoadError` only for directory-level errors

______________________________________________________________________

## Module: registry.py

Registry management for auto-discovered strategies.

### Classes

#### StrategyRegistry

Auto-discovery registry for trading strategies.

```python
from qs_trader.libraries.registry import StrategyRegistry
from pathlib import Path

# Create registry
registry = StrategyRegistry()

# Auto-discover strategies
strategies = registry.load_from_directory(
    directory=Path("library/strategies"),
    recursive=True  # Search subdirectories
)

# List all strategies
print(registry.list_names())  # ['buy_and_hold', 'momentum_20', ...]

# Get strategy class
BuyAndHold = registry.get_strategy_class("buy_and_hold")

# Get strategy config
config = registry.get_strategy_config("buy_and_hold")
print(config.display_name)  # "Buy and Hold Strategy"
```

**Key Methods**:

- `load_from_directory(directory, recursive)`: Auto-discover and register
- `get_strategy_class(name)`: Get strategy class by name
- `get_strategy_config(name)`: Get strategy config by name
- `list_strategies()`: List all with metadata
- `list_names()`: List strategy names only

**Auto-Discovery Flow**:

1. Uses `StrategyLoader` internally
1. Registers each discovered strategy
1. Stores both class and config
1. Validates uniqueness
1. Returns dict of (class, config) tuples

______________________________________________________________________

## Configuration

### Strategy Configuration

**File**: Experiment YAML (e.g., `experiments/my_strategy/my_strategy.yaml`)

```yaml
# experiments/my_strategy/my_strategy.yaml
price_basis: adjusted

strategies:
  - strategy_id: momentum_20 # Registry name
    universe: [AAPL, MSFT] # Symbols to trade
    data_sources: # Data sources to use
      - yahoo-us-equity-1d-csv
    config: # Strategy-specific config
      lookback: 20
```

**Programmatic**:

```python
from qs_trader.engine.config import StrategyConfigItem

config = StrategyConfigItem(
    strategy_id="momentum_20",
    universe=["AAPL", "MSFT"],
    data_sources=["yahoo-us-equity-1d-csv"],
    config={"lookback": 20}
)
```

______________________________________________________________________

### Strategy File Structure

**Convention**: Each strategy is a self-contained `.py` file with:

```python
# library/strategies/buy_and_hold.py
from qs_trader.libraries.strategies.base import Strategy, StrategyConfig
from qs_trader.services.strategy.context import StrategyContext
from qs_trader.events.events import PriceBarEvent

# 1. Strategy Config (must be named CONFIG)
CONFIG = StrategyConfig(
    name="buy_and_hold",              # Unique identifier
    display_name="Buy and Hold",      # Human-readable name
    description="Simple buy and hold strategy",
)

# 2. Strategy Class (inherits from Strategy)
class BuyAndHold(Strategy):
    """Buy on first bar and hold forever."""

    def on_setup(self, context: StrategyContext) -> None:
        self.context = context

    def on_bar(self, bar: PriceBarEvent) -> None:
        # Strategy logic
        pass
```

**Auto-Discovery**:

- Path configured in `config/qs_trader.yaml` (not hardcoded)
- No manual registration files needed
- Just drop `.py` file in configured strategies directory
- Engine auto-discovers on `BacktestEngine.from_config()`
- Registry loads from `system_config.custom_libraries.strategies`

**Usage in Engine**:

```python
# Engine automatically uses system config path
from qs_trader.engine.engine import BacktestEngine
from qs_trader.engine.config import load_backtest_config

config = load_backtest_config("experiments/my_strategy/my_strategy.yaml")
with BacktestEngine.from_config(config) as engine:
    # Engine reads system_config.custom_libraries.strategies
    # Auto-discovers all strategies from that path
    result = engine.run()
```

______________________________________________________________________

## Usage Examples

### Basic Strategy Implementation

```python
from decimal import Decimal
from qs_trader.libraries.strategies.base import BaseStrategy
from qs_trader.services.strategy.context import StrategyContext
from qs_trader.events.events import PriceBarEvent, FillEvent

class SimpleMomentum(BaseStrategy):
    """Buy if price > N-day SMA, sell if below."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._lookback = config.get("lookback", 20)
        self._target_allocation = Decimal(str(config.get("target_allocation", 0.10)))

    def on_setup(self, context: StrategyContext) -> None:
        """Store context reference."""
        self.context = context

    def on_bar(self, bar: PriceBarEvent) -> None:
        """Generate signals based on SMA crossover."""
        # Self-managed warmup
        bars = self.get_bars(self._lookback)
        if bars is None:
            return  # Not enough data yet

        # Calculate SMA
        sma = sum(b.close for b in bars) / len(bars)

        # Generate signal
        if bar.close > sma:
            self.emit_signal(
                symbol=bar.symbol,
                direction="buy",
                target_pct_allocation=self._target_allocation,
                metadata={"sma": float(sma), "price": float(bar.close)}
            )
        elif bar.close < sma:
            self.emit_signal(
                symbol=bar.symbol,
                direction="close",
                target_pct_allocation=Decimal("0.0"),
                metadata={"sma": float(sma), "price": float(bar.close)}
            )

    def on_position_filled(self, fill: FillEvent) -> None:
        """Optional: Track position changes."""
        pass  # Not needed for this strategy

    def on_teardown(self) -> None:
        """Optional: Cleanup resources."""
        pass  # Not needed for this strategy
```

______________________________________________________________________

### Register Custom Strategy

**1. Create strategy file:**

```python
# library/strategies/simple_momentum.py
from decimal import Decimal
from qs_trader.libraries.strategies.base import Strategy, StrategyConfig
from qs_trader.services.strategy.context import StrategyContext
from qs_trader.events.events import PriceBarEvent

# Config (auto-discovered)
CONFIG = StrategyConfig(
    name="simple_momentum",
    display_name="Simple Momentum",
    description="Buy if price > N-day SMA",
)

# Strategy class (auto-discovered)
class SimpleMomentum(Strategy):
    def __init__(self, config: dict):
        super().__init__(config)
        self._lookback = config.get("lookback", 20)

    def on_setup(self, context: StrategyContext) -> None:
        self.context = context

    def on_bar(self, bar: PriceBarEvent) -> None:
        bars = self.get_bars(self._lookback)
        if bars is None:
            return
        # ... strategy logic
```

**2. Add to experiment config:**

```yaml
# experiments/my_strategy/my_strategy.yaml
strategies:
  - strategy_id: simple_momentum
    universe: [AAPL, MSFT, GOOGL]
    data_sources: [yahoo-us-equity-1d-csv]
    config:
      lookback: 20
      target_allocation: 0.10
```

**3. Run backtest:**

```python
from qs_trader.engine.config import load_backtest_config
from qs_trader.engine.engine import BacktestEngine

config = load_backtest_config("experiments/my_strategy/my_strategy.yaml")

with BacktestEngine.from_config(config) as engine:
    result = engine.run()
```

______________________________________________________________________

### Multi-Strategy Portfolio

```yaml
# config/portfolio_multi.yaml
strategies:
  # Momentum strategy on tech stocks
  - strategy_id: momentum_20
    universe: [AAPL, MSFT, GOOGL]
    data_sources: [yahoo-us-equity-1d-csv]
    config:
      lookback: 20
      target_allocation: 0.05

  # Mean reversion on financials
  - strategy_id: mean_reversion_10
    universe: [JPM, BAC, WFC]
    data_sources: [yahoo-us-equity-1d-csv]
    config:
      lookback: 10
      target_allocation: 0.05
```

**Fill Routing**:

```python
# Fill from momentum strategy (routes to momentum only)
fill = FillEvent(
    strategy_id="momentum_20",
    symbol="AAPL",
    ...
)

# Fill from mean reversion strategy (routes to mean reversion only)
fill = FillEvent(
    strategy_id="mean_reversion_10",
    symbol="JPM",
    ...
)
```

______________________________________________________________________

## Design Patterns

### Strategy Pattern

Each strategy encapsulates trading logic:

- Strategies implement common interface (BaseStrategy)
- Service orchestrates without knowing strategy details
- Easy to add new strategies without modifying service

### Context Pattern

StrategyContext provides isolated execution environment:

- Bar caching per strategy
- Signal emission tracking
- No shared state between strategies

### Quarantine Pattern

Failed strategies are isolated but recoverable:

- Setup failures trigger quarantine
- Quarantined strategies skip event routing
- Successful retry removes from quarantine
- Other strategies unaffected

### Publisher-Subscriber Pattern

Strategies emit signals, don't call services directly:

- Loose coupling between strategies and portfolio/risk services
- Easy to add new signal consumers
- Strategies don't need service references

______________________________________________________________________

## Best Practices

### 1. Always Implement Self-Managed Warmup

```python
# ✅ Good
def on_bar(self, bar: PriceBarEvent) -> None:
    bars = self.get_bars(self._lookback)
    if bars is None:
        return  # Not enough data yet
    # ... trade logic

# ❌ Bad
def on_bar(self, bar: PriceBarEvent) -> None:
    bars = self.get_bars(self._lookback)
    # Assumes bars is always available (may crash)
```

### 2. Use Decimal for Allocations

```python
# ✅ Good
target_pct_allocation=Decimal("0.10")

# ❌ Bad
target_pct_allocation=0.10  # Float precision loss
```

### 3. Store Context in on_setup()

```python
# ✅ Good
def on_setup(self, context: StrategyContext) -> None:
    self.context = context

# ❌ Bad
def on_setup(self, context: StrategyContext) -> None:
    pass  # Forgot to store context (get_price() will fail)
```

### 4. Use Proxy Methods for Convenience

```python
# ✅ Good (cleaner)
price = self.get_price()
bars = self.get_bars(20)

# ✅ Also Good (explicit)
price = self.context.get_price()
bars = self.context.get_bars(20)
```

### 5. Use Auto-Discovery (No Manual Registration)

```python
# ✅ Good - Auto-discovery via system config
from qs_trader.system import get_system_config
from qs_trader.libraries.registry import StrategyRegistry

system_config = get_system_config()
strategies_path = Path(system_config.custom_libraries.strategies)
registry = StrategyRegistry()
strategies = registry.load_from_directory(strategies_path)

# ❌ Bad - Hardcoded paths
registry = StrategyRegistry()
strategies = registry.load_from_directory(Path("library/strategies"))

# ❌ Bad - Manual imports
from library.strategies.momentum import MomentumStrategy
# Don't do this - let registry auto-discover
```

### 6. Implement on_position_filled() Only If Needed

```python
# ✅ Good - Stateless strategy
class TrendFollower(BaseStrategy):
    def on_bar(self, bar):
        # Doesn't track positions
        pass
    # No on_position_filled() needed

# ✅ Also Good - Stateful strategy
class PairsTrading(BaseStrategy):
    def on_position_filled(self, fill):
        # Update pair state
        pass
```

### 7. Handle Errors Gracefully

```python
def on_bar(self, bar: PriceBarEvent) -> None:
    try:
        # Strategy logic
        pass
    except Exception as e:
        # Log error, don't crash service
        logger.error(f"Strategy error: {e}")
        return
```

______________________________________________________________________

## Testing

### Unit Tests

**test_service.py** (21 tests):

- Setup lifecycle (3 tests)
- Teardown and subscription cleanup (2 tests)
- Bar routing with universe filtering (4 tests)
- Fill routing with strategy_id (6 tests)
- Quarantine mechanism (6 tests)

**test_context.py** (45 tests):

- Bar caching with maxlen (9 tests)
- get_price() access (6 tests)
- get_bars() with warmup (15 tests)
- Signal emission (9 tests)
- Edge cases (6 tests)

**test_loader.py** (16 tests):

- Registry loading (4 tests)
- Strategy instantiation (6 tests)
- Universe validation (4 tests)
- Error handling (2 tests)

**test_strategy_base.py** (30 tests):

- Lifecycle contract (10 tests)
- Context proxy methods (10 tests)
- Abstract method enforcement (5 tests)
- Configuration handling (5 tests)

### Integration Tests

**test_strategy_integration.py** (4 tests):

- End-to-end strategy execution
- Multi-strategy coordination
- Fill routing with attribution
- Metrics collection

### Running Tests

```bash
# All strategy tests
pytest tests/unit/services/strategy/ -v

# Specific test file
pytest tests/unit/services/strategy/test_service.py -v

# Integration tests
pytest tests/integration/strategy/ -v

# With coverage
pytest tests/unit/services/strategy/ --cov=src/qs_trader/services/strategy --cov-report=html
```

______________________________________________________________________

## Performance Considerations

### Bar Caching Memory

**Memory per bar**: ~500 bytes (PriceBarEvent object)

**Estimation**:

- 500 bars × 10 symbols × 500 bytes = **~2.5 MB** (light)
- 500 bars × 100 symbols × 500 bytes = **~25 MB** (moderate)
- 500 bars × 1000 symbols × 500 bytes = **~250 MB** (heavy)

**Recommendation**: Adjust `max_bars` based on:

- Available memory
- Longest indicator lookback needed
- Number of symbols

### Event Routing Overhead

**Per bar event**:

- Universe check: O(1) set lookup
- Quarantine check: O(1) set lookup
- Bar caching: O(1) deque append
- Strategy on_bar: O(strategy complexity)

**Total**: ~10-50 μs per strategy per bar (excluding strategy logic)

### Quarantine Check Efficiency

```python
# Fast quarantine check
if name in self._quarantined:
    return  # Skip immediately

# No repeated error logging
# No wasted strategy processing
```

______________________________________________________________________

## Troubleshooting

### Problem: Strategy not receiving bars

**Symptoms**: `on_bar()` never called

**Solutions**:

1. **Check universe filtering**:

```yaml
strategies:
  - strategy_id: momentum_20
    universe: [AAPL, MSFT] # Must match data universe
```

1. **Check quarantine status**:

```python
metrics = service.get_metrics()
if metrics['strategies']['momentum_20']['quarantined']:
    print("Strategy quarantined due to setup failure")
```

1. **Check event subscription**:

```python
# Ensure subscribed
service.subscribe_to_events(event_bus)
```

______________________________________________________________________

### Problem: get_bars() returns None

**Symptoms**: Strategy never trades

**Cause**: Not enough bars cached yet (self-managed warmup)

**Solution**: Check if enough bars available:

```python
def on_bar(self, bar: PriceBarEvent) -> None:
    bars = self.get_bars(self._lookback)
    if bars is None:
        print(f"Warmup: Need {self._lookback} bars, have {len(self.context._bar_cache.get(bar.symbol, []))}")
        return
```

______________________________________________________________________

### Problem: Fill events not routing correctly

**Symptoms**: Multi-strategy portfolio cross-contamination

**Solution**: Use strategy_id for attribution:

```python
# ✅ Good - Fill has strategy_id
fill = FillEvent(
    strategy_id="momentum_20",  # Routes to momentum only
    symbol="AAPL",
    ...
)

# ❌ Bad - Fill without strategy_id
fill = FillEvent(
    symbol="AAPL",
    ...  # Routes to all strategies in universe
)
```

______________________________________________________________________

### Problem: Strategy quarantined

**Symptoms**: Metrics show `quarantined=True`

**Cause**: Setup failure

**Solution**:

1. Check logs for setup error
1. Fix strategy code
1. Re-run backtest (retry recovery will remove from quarantine)

```python
# First run
2024-10-24 14:30:00 ERROR Strategy 'momentum_20' setup failed: division by zero

# Fix code
# ...

# Second run
2024-10-24 14:35:00 INFO Strategy 'momentum_20' setup successful (recovered from quarantine)
```

______________________________________________________________________

### Problem: Duplicate event handlers

**Symptoms**: Strategies process bars multiple times

**Cause**: Not calling `teardown()`

**Solution**: Always cleanup:

```python
try:
    service.setup()
    # ... run backtest
finally:
    service.teardown()  # Critical!
```

______________________________________________________________________

## Future Enhancements

### Phase 5: Additional Features

- [ ] Strategy templates (momentum, mean reversion, pairs trading)
- [ ] Multi-timeframe support (combine 1d + 1h data)
- [ ] Portfolio-level signals (rebalance across strategies)
- [ ] Dynamic universe (add/remove symbols mid-backtest)
- [ ] Strategy versioning (track code changes)

### Phase 6: Live Trading

- [ ] State persistence (save strategy state to disk)
- [ ] Dynamic strategy loading (add strategies without restart)
- [ ] Rate limiting (prevent excessive signal emission)
- [ ] Circuit breaker (pause strategy on repeated errors)
- [ ] Health checks (strategy liveness monitoring)

### Phase 7: Advanced Features

- [ ] Strategy ensembles (combine multiple signals)
- [ ] Meta-strategies (strategies that trade strategies)
- [ ] Reinforcement learning integration
- [ ] Genetic algorithm optimization
- [ ] Walk-forward analysis

______________________________________________________________________

## Related Documentation

- **Engine Package**: `docs/packages/engine.md` - Backtesting orchestration
- **Events Package**: `docs/packages/events.md` - Event types and validation
- **Data Package**: `docs/packages/data.md` - Data loading and streaming
- **Strategy Implementation Plan**: `docs/STRATEGY_IMPLEMENTATION_PLAN.md` - Complete roadmap
- **Architecture**: `docs/ARCHITECTURE_ALIGNMENT.md` - System architecture

______________________________________________________________________

## API Reference Summary

### Public API (`qs_trader.services.strategy`)

**Service**:

- `StrategyService` - Main orchestrator

**Context**:

- `StrategyContext` - Per-strategy execution context

**Interface**:

- `IStrategyService` - Protocol definition

**Base Classes** (`qs_trader.libraries.strategies`):

- `BaseStrategy` - Abstract strategy class
- `load_strategies()` - Strategy discovery and loading

**Example Import**:

```python
from qs_trader.services.strategy import (
    StrategyService,
    StrategyContext,
    IStrategyService,
)
from qs_trader.libraries.strategies import (
    BaseStrategy,
    load_strategies,
)
```

______________________________________________________________________

## Example Strategies

### Buy and Hold

```python
# src/qs_trader/libraries/strategies/buy_and_hold.py
class BuyAndHold(BaseStrategy):
    """Buy on first bar and hold."""

    def on_bar(self, bar: PriceBarEvent) -> None:
        if self.get_price() is None:
            return  # First bar

        # Buy on first bar only
        price = self.get_price()
        if len(self.context._bar_cache[bar.symbol]) == 1:
            self.emit_signal(
                symbol=bar.symbol,
                direction="buy",
                target_pct_allocation=Decimal("0.10")
            )
```

### SMA Crossover

```python
# src/qs_trader/libraries/strategies/sma_crossover.py
class SMACrossover(BaseStrategy):
    """Buy when short SMA crosses above long SMA."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._short_window = config.get("short_window", 20)
        self._long_window = config.get("long_window", 50)

    def on_bar(self, bar: PriceBarEvent) -> None:
        # Self-managed warmup
        bars = self.get_bars(self._long_window)
        if bars is None:
            return

        # Calculate SMAs
        short_sma = sum(b.close for b in bars[-self._short_window:]) / self._short_window
        long_sma = sum(b.close for b in bars) / self._long_window

        # Generate signal
        if short_sma > long_sma:
            self.emit_signal(
                symbol=bar.symbol,
                direction="buy",
                target_pct_allocation=Decimal("0.10")
            )
        else:
            self.emit_signal(
                symbol=bar.symbol,
                direction="close",
                target_pct_allocation=Decimal("0.0")
            )
```

### Bollinger Breakout

```python
# src/qs_trader/libraries/strategies/bollinger_breakout.py
class BollingerBreakout(BaseStrategy):
    """Buy when price breaks above upper band."""

    def __init__(self, config: dict):
        super().__init__(config)
        self._window = config.get("window", 20)
        self._num_std = Decimal(str(config.get("num_std", 2.0)))

    def on_bar(self, bar: PriceBarEvent) -> None:
        bars = self.get_bars(self._window)
        if bars is None:
            return

        # Calculate Bollinger Bands
        closes = [b.close for b in bars]
        mean = sum(closes) / len(closes)
        variance = sum((c - mean) ** 2 for c in closes) / len(closes)
        std = variance.sqrt()

        upper_band = mean + (std * self._num_std)
        lower_band = mean - (std * self._num_std)

        # Generate signal
        if bar.close > upper_band:
            self.emit_signal(
                symbol=bar.symbol,
                direction="buy",
                target_pct_allocation=Decimal("0.10")
            )
        elif bar.close < lower_band:
            self.emit_signal(
                symbol=bar.symbol,
                direction="close",
                target_pct_allocation=Decimal("0.0")
            )
```

______________________________________________________________________

**Last Updated**: 2024-10-28\
**Version**: 1.0\
**Status**: Production Ready ✅

**Test Coverage**: 112 tests passing\
**Features Complete**: Auto-discovery, lifecycle, context, quarantine, fill routing\
**Production Ready**: Yes
