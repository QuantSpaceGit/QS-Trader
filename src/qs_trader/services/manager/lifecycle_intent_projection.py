"""Shared lifecycle projection for manager gating and strategy position state.

This projection combines authoritative realized position quantities with
non-terminal lifecycle intents so both the manager and strategy contexts read
from the same state model during fill-latency windows.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from qs_trader.events.events import FillEvent, PortfolioStateEvent
from qs_trader.events.lifecycle_events import OrderIntentEvent, OrderLifecycleEvent
from qs_trader.services.strategy.models import PositionState

_ACTIVE_INTENT_STATES = {"pending", "accepted"}
_OPEN_INTENT_TYPES = {"open", "scale_in", "reverse"}
_CLOSE_INTENT_TYPES = {"close", "scale_out", "reverse"}
_TERMINAL_ORDER_STATES = {"filled", "cancelled", "rejected", "expired"}

type ProjectionKey = tuple[str, str]


@dataclass(slots=True)
class _ActiveIntent:
    """Non-terminal lifecycle intent tracked by the shared projection."""

    intent_id: str
    strategy_id: str
    symbol: str
    intent_type: str
    direction: str
    intent_state: str

    @property
    def key(self) -> ProjectionKey:
        """Return the projection key for this intent."""
        return (self.strategy_id, self.symbol)

    def blocks_same_side_open(self, direction: str) -> bool:
        """Whether this intent suppresses another same-side open."""
        return self.direction == direction and self.intent_type in _OPEN_INTENT_TYPES

    def marks_pending_open(self, direction: str) -> bool:
        """Whether this intent should surface as a pending-open state."""
        return self.direction == direction and self.intent_type in _OPEN_INTENT_TYPES

    def marks_pending_close(self, direction: str) -> bool:
        """Whether this intent should surface as a pending-close state."""
        return self.direction == direction and self.intent_type in _CLOSE_INTENT_TYPES


class LifecycleIntentProjection:
    """Shared in-memory lifecycle projection keyed by ``(strategy_id, symbol)``."""

    def __init__(self) -> None:
        self._position_quantities: dict[ProjectionKey, Decimal] = {}
        self._active_intents_by_id: dict[str, _ActiveIntent] = {}
        self._bound_event_bus_id: int | None = None

    def bind(self, event_bus: object) -> None:
        """Subscribe the projection to runtime events once per event bus."""
        event_bus_id = id(event_bus)
        if self._bound_event_bus_id == event_bus_id:
            return
        if self._bound_event_bus_id is not None:
            raise ValueError("LifecycleIntentProjection is already bound to a different event bus")

        # The projection must update before strategy callbacks inspect position
        # state inside ``on_position_filled``.
        event_bus.subscribe("order_intent", self.on_order_intent, priority=200)  # type: ignore[attr-defined]
        event_bus.subscribe("order_lifecycle", self.on_order_lifecycle, priority=200)  # type: ignore[attr-defined]
        event_bus.subscribe("fill", self.on_fill, priority=200)  # type: ignore[attr-defined]
        self._bound_event_bus_id = event_bus_id

    def on_order_intent(self, event: OrderIntentEvent) -> None:
        """Consume canonical order-intent lifecycle rows."""
        self.apply_order_intent(event)

    def on_order_lifecycle(self, event: OrderLifecycleEvent) -> None:
        """Consume canonical order lifecycle rows."""
        self.apply_order_lifecycle(event)

    def on_fill(self, event: FillEvent) -> None:
        """Consume realized fills so open state stays aligned with execution."""
        self.apply_fill(event)

    def apply_order_intent(self, event: OrderIntentEvent) -> None:
        """Apply an order-intent lifecycle transition to the projection."""
        if event.intent_state in _ACTIVE_INTENT_STATES:
            self._active_intents_by_id[event.intent_id] = _ActiveIntent(
                intent_id=event.intent_id,
                strategy_id=event.strategy_id,
                symbol=event.symbol,
                intent_type=event.intent_type,
                direction=event.direction,
                intent_state=event.intent_state,
            )
            return

        self._active_intents_by_id.pop(event.intent_id, None)

    def apply_order_lifecycle(self, event: OrderLifecycleEvent) -> None:
        """Apply an order lifecycle transition to the projection."""
        if event.intent_id is None:
            return
        if event.order_state in _TERMINAL_ORDER_STATES:
            self._active_intents_by_id.pop(event.intent_id, None)

    def apply_fill(self, event: FillEvent) -> None:
        """Apply a realized fill to the projection's quantity state."""
        if event.strategy_id is None:
            return

        signed_quantity = event.filled_quantity if event.side.lower() == "buy" else -event.filled_quantity
        key = (event.strategy_id, event.symbol)
        self.sync_position_quantity(
            strategy_id=key[0],
            symbol=key[1],
            quantity=self._position_quantities.get(key, Decimal("0")) + signed_quantity,
        )

    def sync_position_quantity(self, strategy_id: str, symbol: str, quantity: Decimal | int | str) -> None:
        """Set the authoritative realized quantity for a strategy-symbol pair."""
        normalized_quantity = Decimal(str(quantity))
        key = (strategy_id, symbol)
        if normalized_quantity == 0:
            self._position_quantities.pop(key, None)
            return
        self._position_quantities[key] = normalized_quantity

    def sync_portfolio_state(self, event: PortfolioStateEvent) -> None:
        """Replace realized quantities from an authoritative portfolio snapshot."""
        fresh_quantities: dict[ProjectionKey, Decimal] = {}
        for strategy_group in event.strategies_groups:
            for position in strategy_group.positions:
                quantity = Decimal(str(position.open_quantity))
                if quantity != 0:
                    fresh_quantities[(strategy_group.strategy_id, position.symbol)] = quantity
        self._position_quantities = fresh_quantities

    def get_same_side_open_suppression_reason(
        self,
        strategy_id: str,
        symbol: str,
        direction: str,
        *,
        exclude_intent_id: str | None = None,
    ) -> str | None:
        """Return the suppression reason for a same-side open, if any."""
        key = (strategy_id, symbol)

        if any(
            intent.blocks_same_side_open(direction)
            for intent in self._iter_active_intents(key, exclude_intent_id=exclude_intent_id)
        ):
            return "duplicate_same_side_pending"

        quantity = self._position_quantities.get(key, Decimal("0"))
        if direction == "long" and quantity > 0:
            return "duplicate_open_without_scale_in"
        if direction == "short" and quantity < 0:
            return "duplicate_open_without_scale_in"
        return None

    def has_same_side_pending_or_open(
        self,
        strategy_id: str,
        symbol: str,
        direction: str,
        *,
        exclude_intent_id: str | None = None,
    ) -> bool:
        """Return whether a same-side open should be suppressed."""
        return (
            self.get_same_side_open_suppression_reason(
                strategy_id,
                symbol,
                direction,
                exclude_intent_id=exclude_intent_id,
            )
            is not None
        )

    def get_position_state(self, strategy_id: str, symbol: str) -> PositionState:
        """Return the strategy-facing lifecycle position state."""
        key = (strategy_id, symbol)
        quantity = self._position_quantities.get(key, Decimal("0"))

        if quantity > 0:
            if self._has_pending_close(key, direction="long"):
                return PositionState.PENDING_CLOSE_LONG
            return PositionState.OPEN_LONG

        if quantity < 0:
            if self._has_pending_close(key, direction="short"):
                return PositionState.PENDING_CLOSE_SHORT
            return PositionState.OPEN_SHORT

        if self._has_pending_open(key, direction="long"):
            return PositionState.PENDING_OPEN_LONG
        if self._has_pending_open(key, direction="short"):
            return PositionState.PENDING_OPEN_SHORT
        return PositionState.FLAT

    def _has_pending_open(self, key: ProjectionKey, direction: str) -> bool:
        """Return whether the projection currently has a pending open intent."""
        return any(intent.marks_pending_open(direction) for intent in self._iter_active_intents(key))

    def _has_pending_close(self, key: ProjectionKey, direction: str) -> bool:
        """Return whether the projection currently has a pending close intent."""
        return any(intent.marks_pending_close(direction) for intent in self._iter_active_intents(key))

    def _iter_active_intents(
        self,
        key: ProjectionKey,
        *,
        exclude_intent_id: str | None = None,
    ) -> list[_ActiveIntent]:
        """Return active intents for a single projection key."""
        return [
            intent
            for intent_id, intent in self._active_intents_by_id.items()
            if intent.key == key and intent_id != exclude_intent_id
        ]
