"""Deterministic buy-and-hold strategy used by the Phase 2A.3 benchmark overlay.

This is a minimal reference implementation: on the first bar of the run the
strategy emits a single OPEN_LONG signal at maximum confidence and then holds
indefinitely.  No indicators, no warmup, no randomness.

The strategy is intentionally a drop-in replacement for the scaffold-template
buy-and-hold under ``qs_trader.scaffold.library.strategies.buy_and_hold``: the
config ``name`` is ``buy_and_hold`` so the engine's
:class:`~qs_trader.libraries.registry.StrategyRegistry` resolves either source
to the same strategy id.

The ``reinvest_dividends`` toggle is exposed on the config to match the Phase
2A.3 :class:`~qs_trader.validation.plan.BenchmarkSpec` field, but is a no-op
until QS-Trader surfaces a dividend feed (tracked for Phase 2A.5).
"""

from __future__ import annotations

from pydantic import ConfigDict

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.strategy.models import SignalIntention


class BuyAndHoldConfig(StrategyConfig):
    """Configuration for :class:`BuyAndHoldStrategy`.

    The strategy has no tunable parameters beyond ``reinvest_dividends``
    (Phase 2A.3 benchmark contract); all other fields are identity / metadata.
    """

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    # Identity
    name: str = "buy_and_hold"
    display_name: str = "Buy and Hold"

    # Metadata
    description: str = "Deterministic buy-on-first-bar / hold-to-end benchmark strategy"
    author: str = "QS-Trader Team"
    created: str = "2024-10-23"
    updated: str = "2026-05-29"
    version: str = "1.1.0"

    # Signal confidence (always max confidence)
    confidence: float = 1.0

    # Phase 2A.3 benchmark contract — no-op until dividends are surfaced by the
    # engine (Phase 2A.5). Validated here so plans authored against the
    # ``BenchmarkSpec.reinvest_dividends`` field accept the override.
    reinvest_dividends: bool = True


class BuyAndHoldStrategy(Strategy[BuyAndHoldConfig]):
    """Buy on the first bar, hold to the end of the run."""

    def __init__(self, config: BuyAndHoldConfig):
        super().__init__(config)
        self._bought = False

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """Emit one OPEN_LONG signal on the first bar and never again."""
        if self._bought:
            return

        context.emit_signal(
            timestamp=event.timestamp,
            symbol=event.symbol,
            intention=SignalIntention.OPEN_LONG,
            price=event.close,
            confidence=self.config.confidence,
            reason="Buy and hold - initial purchase",
            metadata={"price": str(event.close)},
        )
        self._bought = True


# Config instance for auto-discovery
CONFIG = BuyAndHoldConfig()
