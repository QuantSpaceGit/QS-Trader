"""
Broken strategy - for testing error handling.

This file has import errors and should be skipped gracefully.
"""

# mypy: ignore-errors

# This import will fail
from nonexistent_module import something_broken  # pyright: ignore[reportMissingImports]  # noqa: F401

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig


class BrokenStrategyConfig(StrategyConfig):
    """Config for broken strategy (for testing error handling)."""

    name: str = "broken_strategy"
    display_name: str = "Broken Strategy"


CONFIG = BrokenStrategyConfig()


class BrokenStrategy(Strategy):
    def __init__(self, config: BrokenStrategyConfig):
        self.config = config

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        pass
