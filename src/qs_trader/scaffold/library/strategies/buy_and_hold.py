"""
Buy and Hold Strategy.

The simplest possible strategy - buy on the first bar and hold forever.
Demonstrates minimal strategy implementation with no indicators or complex logic.
"""

from pydantic import ConfigDict

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.strategy.models import SignalIntention


class BuyAndHoldConfig(StrategyConfig):
    """
    Configuration for Buy and Hold Strategy.

    This is the simplest possible config - just identity fields.
    No additional parameters needed since the strategy has no logic to tune.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # Identity
    name: str = "buy_and_hold"
    display_name: str = "Buy and Hold"

    # Metadata
    description: str = "Simplest possible strategy - buy on first bar and hold forever"
    author: str = "QS-Trader Team"
    created: str = "2024-10-23"
    updated: str = "2024-10-23"
    version: str = "1.0.0"

    # Signal confidence (always max confidence)
    confidence: float = 1.0
    log_indicators: bool = False  # Enable indicator logging for visualization


class BuyAndHoldStrategy(Strategy[BuyAndHoldConfig]):
    """
    Buy and Hold Strategy.

    Strategy Logic:
    - On first bar (t=0), emit OPEN_LONG signal with maximum confidence
    - Hold forever (no more signals)

    This demonstrates:
    1. Minimal strategy implementation
    2. No indicators needed
    3. No warmup required
    4. Single signal emission
    5. Stateful tracking (bought flag)
    """

    def __init__(self, config: BuyAndHoldConfig):
        """Initialize strategy with configuration."""
        super().__init__(config)
        self._bought = False  # Track if we've bought

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """
        Process price bar - buy on first bar only.

        Args:
            event: Price bar event
            context: Strategy context for signal emission
        """
        # Skip if already bought
        if self._bought:
            return

        # Buy on first bar
        context.emit_signal(
            timestamp=event.timestamp,
            symbol=event.symbol,
            intention=SignalIntention.OPEN_LONG,
            price=event.close,
            confidence=self.config.confidence,
            reason="Buy and hold - initial purchase",
            metadata={"price": str(event.close)},
        )

        # Mark as bought
        self._bought = True


# Config instance for auto-discovery
CONFIG = BuyAndHoldConfig()
