"""
Base Strategy Abstract Class.

All strategies must inherit from Strategy and implement the required methods.
This enables the registry system to auto-discover and validate strategy implementations.

Philosophy:
- Strategies define PROCESS (algorithm/logic) - "if A then B"
- Configs define PARAMETERS (values) - "A=10, B=20"
- Separation enables parameter optimization without code changes
- Strategies do NOT know about portfolio state or risk limits
- Strategies do NOT execute orders directly
- Strategies declare their warmup requirements
- Strategies can consume multiple data sources

Registry Name: Defined in config.name field (e.g., "buy_and_hold", "sma_crossover")
"""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from qs_trader.events.events import PriceBarEvent

# Import actual Context implementation for strategies to use
# This is the runtime context that strategies receive in on_bar()
from qs_trader.services.strategy.context import Context

# Generic type variable for strategy configuration
TConfig = TypeVar("TConfig", bound="StrategyConfig")


class StrategyConfig(BaseModel):
    """
    Base configuration class for all strategies.

    All strategy configs should inherit from this and add strategy-specific parameters.
    This separates the strategy PROCESS (code/logic) from PARAMETERS (data/values).

    Philosophy:
    - Strategy class = PROCESS DEFINITION (the algorithm)
    - Strategy config = PARAMETER VALUES (tunable settings)
    - Same strategy with different configs = parameter sweep/optimization

    Required Fields:
    - name: Strategy identifier for registry (e.g., "sma_crossover")
    - display_name: Human-readable name for UI/reports (e.g., "SMA Crossover")

    Example:
        ```python
        class SMACrossoverConfig(StrategyConfig):
            # Identity
            name: str = "sma_crossover"
            display_name: str = "SMA Crossover Strategy"

            # Metadata
            description: str = "Classic dual moving average crossover strategy"
            author: str = "Trading Team"
            created: str = "2024-01-15"
            updated: str = "2024-03-20"
            version: str = "1.2.0"

            # Strategy-specific parameters
            fast_period: int = 10
            slow_period: int = 20

            # Optional: child can enforce strict validation
            model_config = ConfigDict(extra="forbid")
        ```

    Usage in backtest YAML:
        ```yaml
        strategies:
          - strategy_id: "sma_crossover"
            config:
              name: "sma_crossover_fast"  # Override for custom instance
              display_name: "Fast SMA Crossover"
              fast_period: 5   # Override default 10
              slow_period: 15  # Override default 20
        ```

    Note:
        - Use Pydantic for validation and type safety
        - Provide sensible defaults for all parameters
        - Document parameter ranges and effects
        - Calculate warmup_bars from other parameters when possible
    """

    # Required identity fields
    name: str = Field(..., description="Strategy identifier for registry (snake_case, e.g., 'sma_crossover')")
    display_name: str = Field(..., description="Human-readable name for display (e.g., 'SMA Crossover Strategy')")

    # Metadata fields
    description: str = Field(default="", description="Strategy description and logic explanation")
    author: str = Field(default="", description="Strategy author/creator")
    created: str = Field(default="", description="Creation date (YYYY-MM-DD)")
    updated: str = Field(default="", description="Last update date (YYYY-MM-DD)")
    version: str = Field(default="1.0.0", description="Strategy version (semantic versioning)")

    # Base fields (can be overridden by child configs)
    universe: list[str] = Field(
        default_factory=list,
        description="List of symbols this strategy trades. Empty list means all symbols.",
    )

    # Debugging/logging flags
    log_indicators: bool = Field(
        default=False,
        description="Enable indicator event logging for debugging. When True, strategy can call context.track_indicators() and events will be emitted.",
    )

    # Pydantic v2 configuration
    model_config = ConfigDict(
        validate_assignment=True,  # Validate on attribute assignment
        # Allow child configs to add strategy-specific fields
        # Child configs can override with extra="forbid" if desired
    )


class Strategy(ABC, Generic[TConfig]):
    """
    Abstract base class for all trading strategies.

    Type Parameters:
        TConfig: The specific StrategyConfig subclass used by this strategy.
                 Enables type-safe access to strategy-specific config fields.

    Responsibilities:
    - Define trading PROCESS (algorithm/logic)
    - Process market data (PriceBarEvent)
    - Generate trading signals (via context.emit_signal)
    - Use config for PARAMETERS (tunable values)
    - Manage strategy-specific state (indicators, counters, positions if needed)

    Does NOT:
    - Hardcode parameter values (use config instead)
    - Calculate position sizes (that's RiskService/Portfolio Manager)
    - Execute orders (that's ExecutionService)
    - Apply risk limits (that's RiskService)

    Position Tracking Philosophy:
    - Strategies can track their own positions via on_position_filled() if needed
    - Most strategies don't need position state - just emit signals
    - RiskManager handles position checks and sizing
    - For position-aware strategies: subscribe to position events

    Lifecycle:
    1. __init__(config) - Initialize with configuration
    2. setup() - Optional setup phase (connections, validation, etc.)
    3. on_bar() - Process bars and generate signals (self-manages warmup via get_bars())
    4. on_position_filled() - Optional: track position changes
    5. teardown() - Optional cleanup phase (close connections, save state, etc.)

    Example Implementation:
        ```python
        # 1. Define config (parameters)
        class SMACrossoverConfig(StrategyConfig):
            name: str = "sma_crossover"
            display_name: str = "SMA Crossover Strategy"
            fast_period: int = 10
            slow_period: int = 20

        # 2. Define strategy (process) with Generic typing
        class SMACrossoverStrategy(Strategy[SMACrossoverConfig]):
            def __init__(self, config: SMACrossoverConfig):
                super().__init__(config)
                self.positions = {}  # Optional: track positions if needed

            def on_bar(self, event: PriceBarEvent, context: Context) -> None:
                # Get bars for indicator calculation
                bars = context.get_bars(event.symbol, n=self.config.slow_period)

                # Self-managed warmup: skip if insufficient data
                if bars is None or len(bars) < self.config.slow_period:
                    return

                # Calculate indicators - self.config is now properly typed!
                fast_sma = sum(b.close for b in bars[-self.config.fast_period:]) / self.config.fast_period
                slow_sma = sum(b.close for b in bars) / self.config.slow_period

                # Trading logic
                if fast_sma > slow_sma:
                    context.emit_signal(
                        symbol=event.symbol,
                        intention="OPEN_LONG",
                        confidence=0.8
                    )

            def on_position_filled(self, event: PositionFilledEvent, context: Context) -> None:
                # Optional: track position changes
                self.positions[event.symbol] = event.quantity
        ```

    Key Design:
        - Strategy class defines PROCESS: "if fast > slow, buy"
        - Config class defines PARAMETERS: "fast=10, slow=20"
        - Generic typing provides type-safe config access
        - Same strategy, different configs = parameter optimization
        - Warmup self-managed: check get_bars() returns enough data
        - Position tracking optional: use on_position_filled() if needed
    """

    # Required instance variable (must be set in __init__)
    # Type will be resolved to specific TConfig at runtime
    config: TConfig

    @abstractmethod
    def __init__(self, config: TConfig):
        """
        Initialize strategy with configuration.

        Args:
            config: Strategy-specific configuration (inherits from StrategyConfig)
                   Contains tunable parameters like periods, thresholds, etc.

        Example:
            def __init__(self, config: SMACrossoverConfig):
                super().__init__(config)
                self.fast_sma = SMA(period=config.fast_period)
                self.slow_sma = SMA(period=config.slow_period)

        Note:
            - MUST call super().__init__(config) to store config
            - Initialize indicators with config values
            - Initialize any strategy-specific state
            - Do NOT emit signals during initialization
            - Do NOT hardcode parameter values - use config
            - For complex setup, use setup() method instead
        """
        self.config = config

    def setup(self, context: "Context") -> None:
        """
        Optional setup phase called before strategy starts trading.

        Use for:
        - Validating configuration
        - Loading external data/models
        - Establishing connections
        - Complex initialization that needs context

        Args:
            context: Strategy context (same as on_bar)

        Example:
            def setup(self, context: Context):
                # Validate config
                if self.config.fast_period >= self.config.slow_period:
                    raise ValueError("Fast period must be < slow period")

                # Load external data
                self.ml_model = load_model(self.config.model_path)

                # Pre-compute lookups
                self.symbol_sectors = context.get_metadata("sectors")

        Note:
            - Called once after __init__, before first bar
            - Default implementation does nothing (no need to override if not needed)
            - Can raise exceptions to prevent strategy from starting
            - Access to context for data queries
        """
        pass

    def teardown(self, context: "Context") -> None:
        """
        Optional cleanup phase called after strategy finishes.

        Use for:
        - Closing connections/resources
        - Saving state/models
        - Cleanup operations
        - Final logging/reporting

        Args:
            context: Strategy context (same as on_bar)

        Example:
            def teardown(self, context: Context):
                # Save trained model
                if hasattr(self, 'ml_model'):
                    self.ml_model.save(self.config.model_output_path)

                # Close database connection
                if hasattr(self, 'db_conn'):
                    self.db_conn.close()

                # Log final statistics
                print(f"Strategy {self.name} processed {self.bar_count} bars")

        Note:
            - Called once after all bars processed
            - Default implementation does nothing (no need to override if not needed)
            - Should not raise exceptions (use try/except internally)
            - Access to context for final queries
        """
        pass

    def on_position_filled(self, event: Any, context: "Context") -> None:
        """
        Optional: Handle position fill events for position tracking.

        Called when a position is opened, closed, or modified. Use this to
        maintain strategy-level position state if your strategy needs to know
        current positions.

        Args:
            event: PositionFilledEvent containing:
                  - event.symbol: Symbol ticker
                  - event.quantity: New position quantity (positive=long, negative=short, 0=flat)
                  - event.price: Fill price
                  - event.timestamp: Fill timestamp
            context: Strategy context (same as on_bar)

        Example:
            def __init__(self, config):
                self.config = config
                self.positions = {}  # Track positions

            def on_position_filled(self, event: PositionFilledEvent, context: Context):
                # Update position tracking
                self.positions[event.symbol] = event.quantity

                # Log position change
                if event.quantity == 0:
                    print(f"Closed position in {event.symbol}")
                elif event.quantity > 0:
                    print(f"Long {event.quantity} shares of {event.symbol}")
                else:
                    print(f"Short {abs(event.quantity)} shares of {event.symbol}")

            def on_bar(self, event: PriceBarEvent, context: Context):
                # Use position state in trading logic
                current_position = self.positions.get(event.symbol, 0)
                if current_position > 0:
                    # Already long, maybe scale out or hold
                    pass

        Note:
            - Default implementation does nothing (optional override)
            - Called AFTER position change is recorded
            - Strategies that don't care about positions don't need to implement this
            - For most strategies, RiskManager handles position logic - no need to track
        """
        pass

    @abstractmethod
    def on_bar(self, event: PriceBarEvent, context: "Context") -> None:
        """
        Process new price bar and optionally generate signals.

        Args:
            event: Price bar event containing OHLCV data
                  - event.symbol: Symbol ticker
                  - event.bar: Bar with OHLCV data
                  - event.timestamp: Bar timestamp
                  - event.is_warmup: True during warmup phase

            context: Strategy context providing:
                  - context.emit_signal(symbol, direction, confidence)
                  - context.get_position(symbol) - current position
                  - context.get_bars(symbol, n) - historical bars
                  - context.get_price(symbol) - latest price

        Flow:
            1. Update indicators with new bar
            2. If is_warmup=True, return (don't generate signals)
            3. Evaluate trading logic
            4. Generate signals via context.emit_signal()

        Signal Generation:
            context.emit_signal(
                symbol="AAPL",
                intention="OPEN_LONG" or "CLOSE_LONG",
                confidence=0.8,  # 0.0 to 1.0
                reason="Optional explanation"  # For logging
            )

        Note:
            - Can emit 0, 1, or multiple signals per bar
            - Signals are sent to Portfolio Manager for risk evaluation
            - Portfolio Manager decides actual position size and whether to trade
            - Don't worry about position sizing - just signal intent and confidence
        """
        pass

    @property
    def name(self) -> str:
        """
        Strategy name for registry and logging.

        Returns:
            Strategy name from config (e.g., "sma_crossover")

        Note:
            This comes from the config's name field.
            Used for registry lookups and internal identification.
        """
        return self.config.name

    @property
    def display_name(self) -> str:
        """
        Human-readable strategy name for UI and reports.

        Returns:
            Display name from config (e.g., "SMA Crossover Strategy")

        Note:
            This comes from the config's display_name field.
            Used in reports, logs, and user-facing interfaces.
        """
        return self.config.display_name

    # Note: warmup_bars property removed - strategies self-manage warmup
    # by checking if get_bars() returns sufficient data. No centralized
    # warmup coordination needed.


# ============================================================================
# Public API
# ============================================================================

__all__ = [
    "StrategyConfig",
    "Strategy",
    "Context",
]
