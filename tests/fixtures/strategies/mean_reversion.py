"""
Test strategy - Mean Reversion.

This is a fixture for testing strategy auto-discovery.
Tests a different naming convention (no CONFIG variable).
"""

from decimal import Decimal

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.strategy.models import SignalIntention


class MeanReversionConfig(StrategyConfig):
    """Config for mean reversion strategy."""

    name: str = "mean_reversion"
    display_name: str = "Mean Reversion"
    description: str = "Buy on oversold, sell on overbought"

    period: int = 20
    std_dev: float = 2.0


# Different naming - config lowercase
config = MeanReversionConfig()


class MeanReversionStrategy(Strategy):
    """Mean reversion strategy."""

    def __init__(self, config: MeanReversionConfig):
        self.config = config

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """Process bar and emit signals."""
        # Placeholder logic
        context.emit_signal(
            timestamp=event.timestamp,
            symbol=event.symbol,
            intention=SignalIntention.OPEN_LONG,
            price=event.close,
            confidence=Decimal("0.7"),
            reason="Mean reversion signal",
        )
