"""Buy and hold strategy for testing.

Simple strategy that buys on first bar and holds until end.
Used in integration tests - not a user-facing strategy.
"""

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.strategy.models import SignalIntention


class BuyAndHoldConfig(StrategyConfig):
    """Config for buy and hold strategy."""

    name: str = "buy_and_hold"
    display_name: str = "Buy and Hold"
    description: str = "Buy on first bar and hold"


# Convention: CONFIG variable for auto-discovery
CONFIG = BuyAndHoldConfig()


class BuyAndHold(Strategy):
    """Buy and hold strategy - buys once and holds.

    Test Strategy Configuration:
        name: "buy_and_hold"
        config: {} (no parameters needed)
    """

    def __init__(self, config: BuyAndHoldConfig):
        """Initialize buy and hold strategy.

        Args:
            config: Strategy configuration.
        """
        self.config = config
        self.has_bought = False

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """Generate buy signal on first bar only.

        Args:
            event: Bar event containing OHLCV data.
            context: Strategy context for signal emission.
        """
        # Only buy once
        if not self.has_bought:
            self.has_bought = True
            context.emit_signal(
                timestamp=event.timestamp,
                symbol=event.symbol,
                intention=SignalIntention.OPEN_LONG,
                price=event.close,
                confidence=1.0,
                reason="Buy and hold - initial purchase",
            )
