"""
Weekly Monday-Friday Strategy.

This strategy implements a simple weekly trading pattern:
- BUY on the first business day of the week (Monday, or Tuesday if Monday is holiday)
- SELL on Friday of the same week
- Stay flat over weekends

Strategy Logic:
- On Monday (or first business day): Open long position
- On Friday: Close long position
- Between Tuesday-Thursday: Hold position
- Weekends: Flat (no position)

Key Features:
- Calendar-based timing strategy
- Uses Python's datetime for weekday detection
- Simple hold period (Monday to Friday)
- Demonstrates event-driven position management
"""

from datetime import datetime
from decimal import Decimal

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, PositionState, PriceBasis, Strategy, StrategyConfig
from qs_trader.services.strategy.models import SignalIntention


class WeeklyMondayFridayConfig(StrategyConfig):
    """Configuration for Weekly Monday-Friday strategy."""

    name: str = "weekly_monday_friday"
    display_name: str = "Weekly Monday-Friday"
    universe: list[str] = []  # Apply to all symbols by default

    # Strategy-specific parameters
    confidence: Decimal = Decimal("1.0")  # Signal confidence
    log_indicators: bool = False  # Enable indicator logging for visualization


# Export config for auto-discovery
CONFIG = WeeklyMondayFridayConfig()


class WeeklyMondayFriday(Strategy[WeeklyMondayFridayConfig]):
    """
    Weekly Monday-Friday trading strategy.

    Buys on Monday (first business day of week) and sells on Friday.
    This creates a weekly cycle with weekend exposure eliminated.

    Demonstrates:
    - Calendar-based timing strategies
    - Simple position lifecycle management
    - Weekday detection using Python datetime
    """

    def __init__(self, config: WeeklyMondayFridayConfig):
        """
        Initialize Weekly Monday-Friday strategy.

        Args:
            config: Strategy configuration
        """
        super().__init__(config)
        # Track if we've already traded on Monday this week
        self._traded_this_week: dict[str, str] = {}  # symbol -> week_key (e.g., "2024-W45")

    def setup(self, context: Context) -> None:
        """
        Strategy setup (called once before first bar).

        Args:
            context: Strategy execution context
        """
        pass  # No setup needed

    def teardown(self, context: Context) -> None:
        """
        Strategy teardown (called once after last bar).

        Args:
            context: Strategy execution context
        """
        pass  # No cleanup needed

    def _get_week_key(self, timestamp: datetime) -> str:
        """
        Get week identifier (year + ISO week number).

        Args:
            timestamp: Current timestamp

        Returns:
            Week key in format "YYYY-WNN" (e.g., "2024-W45")
        """
        year, week, _ = timestamp.isocalendar()
        return f"{year}-W{week:02d}"

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """
        Process bar and generate signals based on day of week.

        Logic:
        - Monday (weekday 0): BUY if no position
        - Friday (weekday 4): SELL if have position
        - Tuesday-Thursday: Hold (no action)

        Args:
            event: Current price bar
            context: Strategy execution context
        """
        symbol = event.symbol

        # Parse timestamp to get weekday
        # event.timestamp is ISO8601 string (UTC)
        timestamp = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
        weekday = timestamp.weekday()  # 0=Monday, 1=Tuesday, ..., 4=Friday, 5=Saturday, 6=Sunday

        position_state = context.get_position_state(symbol)
        has_position = position_state == PositionState.OPEN_LONG

        # Get week key to track if we've already traded this week
        week_key = self._get_week_key(timestamp)
        last_trade_week = self._traded_this_week.get(symbol, None)

        # Get current price for signal
        current_price = context.get_price(symbol, basis=PriceBasis.ADJUSTED)
        if current_price is None:
            return

        # Monday (or first business day): Open long position
        if weekday == 0:  # Monday
            # Only buy if we don't have position and haven't traded this week yet
            if not has_position and last_trade_week != week_key:
                context.emit_signal(
                    timestamp=event.timestamp,
                    symbol=symbol,
                    intention=SignalIntention.OPEN_LONG,
                    price=current_price,
                    confidence=self.config.confidence,
                    reason=f"Monday entry - Week {week_key}",
                    metadata={
                        "weekday": "Monday",
                        "week": week_key,
                        "strategy": "weekly_monday_friday",
                    },
                )
                # Mark that we've traded this week
                self._traded_this_week[symbol] = week_key

        # Friday: Close long position
        elif weekday == 4:  # Friday
            # Only sell if we have position
            if has_position:
                context.emit_signal(
                    timestamp=event.timestamp,
                    symbol=symbol,
                    intention=SignalIntention.CLOSE_LONG,
                    price=current_price,
                    confidence=self.config.confidence,
                    reason=f"Friday exit - Week {week_key}",
                    metadata={
                        "weekday": "Friday",
                        "week": week_key,
                        "strategy": "weekly_monday_friday",
                    },
                )
                # Reset position state (will be updated by on_position_filled)

        # Tuesday-Thursday: Hold (no action)
        # Weekends (Saturday-Sunday): No trading (bars typically don't exist)
