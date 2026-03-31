"""
Test strategy - Simple Moving Average Crossover.

This is a fixture for testing strategy auto-discovery.
"""

from decimal import Decimal

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.strategy.models import SignalIntention


class SMAConfig(StrategyConfig):
    """Config for SMA crossover strategy."""

    name: str = "sma_crossover"
    display_name: str = "SMA Crossover"
    description: str = "Buy when fast SMA crosses above slow SMA"

    fast_period: int = 10
    slow_period: int = 20


# Convention: CONFIG variable
CONFIG = SMAConfig()


class SMAStrategy(Strategy):
    """Simple Moving Average crossover strategy."""

    def __init__(self, config: SMAConfig):
        self.config = config
        self.fast_prices: list[Decimal] = []
        self.slow_prices: list[Decimal] = []

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """Process bar and emit signals."""
        config = self.config
        assert isinstance(config, SMAConfig)

        # Store prices
        self.fast_prices.append(event.close)
        self.slow_prices.append(event.close)

        # Keep only needed history
        if len(self.fast_prices) > config.fast_period:
            self.fast_prices.pop(0)
        if len(self.slow_prices) > config.slow_period:
            self.slow_prices.pop(0)

        # Wait for warmup
        if len(self.slow_prices) < config.slow_period:
            return

        # Calculate SMAs
        fast_sma = sum(self.fast_prices) / len(self.fast_prices)
        slow_sma = sum(self.slow_prices) / len(self.slow_prices)

        # Generate signal
        if fast_sma > slow_sma:
            context.emit_signal(
                timestamp=event.timestamp,
                symbol=event.symbol,
                intention=SignalIntention.OPEN_LONG,
                price=event.close,
                confidence=Decimal("0.8"),
                reason=f"Fast SMA ({fast_sma:.2f}) > Slow SMA ({slow_sma:.2f})",
            )
