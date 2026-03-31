"""
Strategy Service Interface.

Defines the protocol for orchestrating strategy instances.
"""

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from qs_trader.libraries.strategies import Strategy

from qs_trader.events.event_bus import EventBus


class IStrategyService(Protocol):
    """Protocol for strategy orchestration service."""

    def __init__(self, event_bus: EventBus, strategies: dict[str, "Strategy"]) -> None:
        """
        Initialize strategy service.

        Args:
            event_bus: Event bus for publishing/subscribing to events
            strategies: Dictionary mapping strategy names to instances

        Subscribes to:
            - PriceBarEvent: Routed to strategies based on universe filtering
            - FillEvent: Routed to strategies for position tracking
        """
        ...

    def get_metrics(self) -> dict:
        """
        Get metrics for all managed strategies.

        Returns:
            Dictionary with per-strategy metrics including bars_processed,
            signals_emitted, errors_encountered
        """
        ...
