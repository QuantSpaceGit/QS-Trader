"""
Strategy without config - for testing error handling.

This strategy doesn't have a CONFIG variable, should be skipped.
"""

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig


class NoConfigStrategy(Strategy):
    """Strategy without config."""

    def __init__(self, config: StrategyConfig):
        self.config = config

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """Process bar."""
        pass


# Note: No CONFIG variable defined!
