"""
SMA Crossover Strategy - Long/Short Implementation.

This strategy showcases the new Context capabilities:
- get_bars() for historical data access
- get_price() for current price
- Stateful decision making with position tracking

Strategy Logic:
- Calculate fast (20-period) and slow (50-period) Simple Moving Averages
- OPEN_LONG when fast SMA crosses above slow SMA (golden cross)
- CLOSE_LONG + OPEN_SHORT when fast SMA crosses below slow SMA (death cross)
- CLOSE_SHORT + OPEN_LONG when fast SMA crosses above slow SMA (golden cross)

Key Features:
- Uses context.get_bars() to access historical prices
- Tracks position direction (long/short/flat)
- Always in the market (long or short)
- Demonstrates proper separation: Strategy decides WHAT, Manager decides HOW MUCH
"""

from decimal import Decimal

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, PositionState, PriceBasis, Strategy, StrategyConfig
from qs_trader.services.strategy.models import SignalIntention


class SMAConfig(StrategyConfig):
    """Configuration for SMA Crossover strategy."""

    name: str = "sma_crossover"
    display_name: str = "SMA Crossover"
    universe: list[str] = []  # Apply to all symbols by default

    # Strategy-specific parameters
    fast_period: int = 10  # Fast SMA period
    slow_period: int = 50  # Slow SMA period (strategy needs this many bars minimum)
    confidence: Decimal = Decimal("1.0")  # Signal confidence
    log_indicators: bool = True  # Legacy no-op; indicator emission is now always-on when tracked


# Export config for auto-discovery
CONFIG = SMAConfig()


class SMACrossover(Strategy[SMAConfig]):
    """
    SMA Crossover Strategy using Phase 4 Context enhancements.

    Demonstrates:
    - Historical bar access via context.get_bars()
    - Technical indicator calculation (SMA)
    - Stateful decisions without internal state
    - Proper signal emission with metadata
    """

    def __init__(self, config: SMAConfig):
        """
        Initialize SMA Crossover strategy.

        Args:
            config: Strategy configuration with SMA periods
        """
        super().__init__(config)

    def setup(self, context: Context) -> None:
        """
        Strategy setup (called once before first bar).

        Args:
            context: Strategy execution context
        """
        pass  # No setup needed - Context handles bar caching

    def teardown(self, context: Context) -> None:
        """
        Strategy teardown (called once after last bar).

        Args:
            context: Strategy execution context
        """
        pass  # No cleanup needed

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """
        Process bar and generate signals based on SMA crossover.

        Args:
            event: Current price bar
            context: Strategy execution context
        """
        symbol = event.symbol

        # Get historical bars for SMA calculation
        # Need slow_period + 1 bars to have enough history
        # Note: We calculate indicators at bar t using ONLY bars t-n through t-1 (excluding current bar t)
        # This prevents look-ahead bias and matches industry standard (Excel, TA-Lib, etc.)
        bars = context.get_bars(symbol, n=self.config.slow_period + 1, basis=PriceBasis.ADJUSTED)

        # Wait until we have enough data
        if bars is None or len(bars) < self.config.slow_period + 1:
            return

        # Calculate CURRENT SMAs (using historical bars, excluding the current bar being processed)
        # At bar t, we use bars [t-slow_period, t-1] (does NOT include current bar t)
        current_bars = bars[:-1]  # Exclude the most recent bar (the current bar)

        # Current fast SMA (most recent fast_period historical bars)
        current_fast_prices = [bar.close for bar in current_bars[-self.config.fast_period :]]
        fast_sma = sum(current_fast_prices) / len(current_fast_prices)

        # Current slow SMA (most recent slow_period historical bars)
        current_slow_prices = [bar.close for bar in current_bars]
        slow_sma = sum(current_slow_prices) / len(current_slow_prices)

        # Calculate PREVIOUS SMAs (one bar earlier)
        prev_bars = bars[:-2]  # Exclude current bar and previous bar

        # Previous fast SMA
        prev_fast_prices = [bar.close for bar in prev_bars[-self.config.fast_period :]]
        prev_fast_sma = sum(prev_fast_prices) / len(prev_fast_prices)

        # Previous slow SMA
        prev_slow_prices = [bar.close for bar in prev_bars]
        prev_slow_sma = sum(prev_slow_prices) / len(prev_slow_prices)

        # Detect crossovers
        golden_cross = prev_fast_sma <= prev_slow_sma and fast_sma > slow_sma
        death_cross = prev_fast_sma >= prev_slow_sma and fast_sma < slow_sma

        # Track indicators for logging/observability (automatically emitted when tracked)
        # Indicators already on correct scale (backward-adjusted from data service)
        context.track_indicators(
            indicators={
                "fast_sma": float(fast_sma),
                "slow_sma": float(slow_sma),
            },
            display_names={
                "fast_sma": f"SMA({self.config.fast_period})",
                "slow_sma": f"SMA({self.config.slow_period})",
            },
            placements={
                "fast_sma": "overlay",
                "slow_sma": "overlay",
            },
            colors={
                "fast_sma": "#667eea",
                "slow_sma": "#764ba2",
            },
        )

        # Get current price for signal
        current_price = context.get_price(symbol, basis=PriceBasis.ADJUSTED)
        if current_price is None:
            return

        position_state = context.get_position_state(symbol)

        # Generate signals on crossovers - LONG ONLY STRATEGY
        if golden_cross:
            # Fast SMA crossed above slow SMA - go long (if not already long)
            if position_state != PositionState.OPEN_LONG:
                # Open long position
                context.emit_signal(
                    timestamp=event.timestamp,
                    symbol=symbol,
                    intention=SignalIntention.OPEN_LONG,
                    price=current_price,
                    confidence=self.config.confidence,
                    reason=f"Golden cross: fast SMA ({fast_sma:.2f}) > slow SMA ({slow_sma:.2f})",
                    metadata={
                        "fast_sma": float(fast_sma),
                        "slow_sma": float(slow_sma),
                        "prev_fast_sma": float(prev_fast_sma),
                        "prev_slow_sma": float(prev_slow_sma),
                        "crossover_type": "golden",
                    },
                )
                # Position tracking happens via on_position_filled()

        elif death_cross:
            # Fast SMA crossed below slow SMA - close long if open
            if position_state == PositionState.OPEN_LONG:
                # Close long position
                context.emit_signal(
                    timestamp=event.timestamp,
                    symbol=symbol,
                    intention=SignalIntention.CLOSE_LONG,
                    price=current_price,
                    confidence=self.config.confidence,
                    reason=f"Death cross: fast SMA ({fast_sma:.2f}) < slow SMA ({slow_sma:.2f})",
                    metadata={
                        "fast_sma": float(fast_sma),
                        "slow_sma": float(slow_sma),
                        "prev_fast_sma": float(prev_fast_sma),
                        "prev_slow_sma": float(prev_slow_sma),
                        "crossover_type": "death",
                    },
                )
                # After closing, position becomes None (flat)
                # Strategy will wait for next golden cross to open long again

        # Note: Strategy tracks position direction via on_position_filled() callbacks
        # This ensures strategy state stays synchronized with actual portfolio positions
        # RiskManager/PositionSizer still decides:
        # - IF to take the signal (based on risk limits)
        # - HOW MUCH to trade (based on buying power, position limits)
