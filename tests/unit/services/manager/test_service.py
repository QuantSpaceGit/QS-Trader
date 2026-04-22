"""Focused unit tests for ManagerService lifecycle emission."""

from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import SignalEvent
from qs_trader.events.lifecycle_context import LifecycleRunContext
from qs_trader.events.lifecycle_events import OrderIntentEvent, OrderLifecycleEvent
from qs_trader.libraries.risk.tools.limits import LimitViolation
from qs_trader.services.manager import service as manager_service_module
from qs_trader.services.manager.service import ManagerService


def _build_manager() -> tuple[ManagerService, EventBus, InMemoryEventStore, LifecycleRunContext]:
    """Create a lifecycle-enabled manager with an attached event store."""
    event_store = InMemoryEventStore()
    event_bus = EventBus()
    event_bus.attach_store(event_store)
    lifecycle_context = LifecycleRunContext(experiment_id="exp", run_id="run-001")
    manager = ManagerService.from_config(
        {"name": "naive", "config": {}},
        event_bus,
        lifecycle_context=lifecycle_context,
    )
    manager._cached_positions = []
    manager._cached_strategy_positions = {"sma_crossover": {"AAPL": 0}}
    return manager, event_bus, event_store, lifecycle_context


def _make_signal(
    *,
    intention: str = "OPEN_LONG",
    confidence: Decimal = Decimal("1.0"),
    correlation_id: str = "550e8400-e29b-41d4-a716-446655440062",
    causation_id: str | None = "550e8400-e29b-41d4-a716-446655440061",
) -> SignalEvent:
    """Build a signal event with schema-valid lifecycle identifiers."""
    return SignalEvent(
        signal_id=correlation_id,
        timestamp="2024-01-02T14:30:00Z",
        strategy_id="sma_crossover",
        symbol="AAPL",
        intention=intention,
        price=Decimal("100.00"),
        confidence=confidence,
        source_service="strategy_service",
        correlation_id=correlation_id,
        causation_id=causation_id,
    )


def _intent_events(event_store: InMemoryEventStore) -> list[OrderIntentEvent]:
    """Return canonical intent lifecycle events from the event store."""
    return [event for event in event_store.get_by_type("order_intent") if isinstance(event, OrderIntentEvent)]


def _order_lifecycle_events(event_store: InMemoryEventStore) -> list[OrderLifecycleEvent]:
    """Return canonical order lifecycle events from the event store."""
    return [
        event for event in event_store.get_by_type("order_lifecycle") if isinstance(event, OrderLifecycleEvent)
    ]


def test_on_signal_roots_pending_intent_on_strategy_decision() -> None:
    """The first canonical intent event should be caused by the canonical strategy decision."""
    manager, event_bus, event_store, _ = _build_manager()
    manager._cached_equity = Decimal("100000")

    decision_causation_id = "550e8400-e29b-41d4-a716-446655440061"
    correlation_id = "550e8400-e29b-41d4-a716-446655440062"
    event_bus.publish(_make_signal(correlation_id=correlation_id, causation_id=decision_causation_id))

    intent_events = _intent_events(event_store)
    order_lifecycle_events = _order_lifecycle_events(event_store)

    assert [event.intent_state for event in intent_events] == ["pending", "accepted"]
    assert intent_events[0].causation_id == decision_causation_id
    assert intent_events[1].causation_id == intent_events[0].event_id
    assert order_lifecycle_events[0].order_state == "created"
    assert order_lifecycle_events[0].causation_id == intent_events[1].event_id


def test_on_signal_suppresses_when_portfolio_equity_missing() -> None:
    """Signals without cached portfolio equity should terminate in suppressed intent state."""
    _, event_bus, event_store, _ = _build_manager()

    event_bus.publish(
        _make_signal(
            correlation_id="550e8400-e29b-41d4-a716-446655440063",
            causation_id="550e8400-e29b-41d4-a716-446655440064",
        )
    )

    intent_events = _intent_events(event_store)

    assert [event.intent_state for event in intent_events] == ["pending", "suppressed"]
    assert intent_events[-1].suppression_reason == "no_cached_equity"
    assert event_store.get_by_type("order") == []


def test_on_signal_suppresses_duplicate_open_without_scale_in() -> None:
    """Opening into an existing same-side position should emit an explicit duplicate-open suppression."""
    manager, event_bus, event_store, _ = _build_manager()
    manager._cached_equity = Decimal("100000")
    manager._cached_strategy_positions = {"sma_crossover": {"AAPL": 100}}

    event_bus.publish(
        _make_signal(
            correlation_id="550e8400-e29b-41d4-a716-446655440065",
            causation_id="550e8400-e29b-41d4-a716-446655440066",
        )
    )

    intent_events = _intent_events(event_store)

    assert [event.intent_state for event in intent_events] == ["pending", "suppressed"]
    assert intent_events[-1].suppression_reason == "duplicate_open_without_scale_in"
    assert event_store.get_by_type("order") == []


def test_on_signal_suppresses_risk_limit_violations(monkeypatch: pytest.MonkeyPatch) -> None:
    """Risk-limit failures should remain visible in the canonical intent ledger."""
    manager, event_bus, event_store, _ = _build_manager()
    manager._cached_equity = Decimal("100000")

    def _mock_check_all_limits(**_: object) -> list[LimitViolation]:
        return [
            LimitViolation(
                limit_type="concentration",
                symbol="AAPL",
                proposed_exposure=Decimal("20000"),
                proposed_pct=0.2,
                limit_pct=0.1,
                message="Concentration limit exceeded for AAPL: 20.00% > 10.00%",
            )
        ]

    monkeypatch.setattr(manager_service_module.risk_limits, "check_all_limits", _mock_check_all_limits)

    event_bus.publish(
        _make_signal(
            correlation_id="550e8400-e29b-41d4-a716-446655440067",
            causation_id="550e8400-e29b-41d4-a716-446655440068",
        )
    )

    intent_events = _intent_events(event_store)

    assert [event.intent_state for event in intent_events] == ["pending", "suppressed"]
    assert intent_events[-1].suppression_reason == "risk_limit_violation"
    assert event_store.get_by_type("order") == []


def test_partial_close_signal_cancelled_after_partial_fill_marks_intent_partially_accepted() -> None:
    """Partial close flows should preserve the executed portion in the lifecycle chain."""
    manager, event_bus, event_store, lifecycle_context = _build_manager()
    manager._cached_equity = Decimal("100000")
    manager._cached_strategy_positions = {"sma_crossover": {"AAPL": 100}}

    correlation_id = "550e8400-e29b-41d4-a716-446655440069"
    event_bus.publish(
        _make_signal(
            intention="CLOSE_LONG",
            confidence=Decimal("0.4"),
            correlation_id=correlation_id,
            causation_id="550e8400-e29b-41d4-a716-446655440070",
        )
    )

    intent_events = _intent_events(event_store)
    order_lifecycle_events = _order_lifecycle_events(event_store)
    accepted_intent = intent_events[1]
    created_order = order_lifecycle_events[0]

    terminal_event = OrderLifecycleEvent(
        experiment_id=lifecycle_context.experiment_id,
        run_id=lifecycle_context.run_id,
        occurred_at=created_order.occurred_at,
        order_id=created_order.order_id,
        intent_id=accepted_intent.intent_id,
        strategy_id="sma_crossover",
        symbol="AAPL",
        order_state="cancelled",
        side="sell",
        quantity=Decimal("40"),
        filled_quantity=Decimal("20"),
        order_type="market",
        time_in_force="IOC",
        price_basis=lifecycle_context.execution_price_basis,
        idempotency_key="partial-close-order",
        cancellation_reason="partial_fill_ioc_cancel",
        source_service="execution_service",
        correlation_id=correlation_id,
        causation_id=created_order.event_id,
    )

    event_bus.publish(terminal_event)

    intent_events = _intent_events(event_store)

    assert accepted_intent.target_quantity == Decimal("40")
    assert created_order.quantity == Decimal("40")
    assert [event.intent_state for event in intent_events] == ["pending", "accepted", "partially_accepted"]
    assert intent_events[-1].causation_id == terminal_event.event_id
    assert intent_events[-1].target_quantity == Decimal("40")


def test_terminal_rejection_marks_intent_cancelled() -> None:
    """Terminal zero-fill outcomes should roll accepted intents into cancelled state."""
    manager, event_bus, event_store, lifecycle_context = _build_manager()
    manager._cached_equity = Decimal("100000")

    correlation_id = "550e8400-e29b-41d4-a716-446655440071"
    event_bus.publish(
        _make_signal(
            correlation_id=correlation_id,
            causation_id="550e8400-e29b-41d4-a716-446655440072",
        )
    )

    accepted_intent = _intent_events(event_store)[1]
    created_order = _order_lifecycle_events(event_store)[0]

    rejection_event = OrderLifecycleEvent(
        experiment_id=lifecycle_context.experiment_id,
        run_id=lifecycle_context.run_id,
        occurred_at=created_order.occurred_at,
        order_id=created_order.order_id,
        intent_id=accepted_intent.intent_id,
        strategy_id="sma_crossover",
        symbol="AAPL",
        order_state="rejected",
        side="buy",
        quantity=created_order.quantity,
        filled_quantity=Decimal("0"),
        order_type="market",
        time_in_force="GTC",
        price_basis=lifecycle_context.execution_price_basis,
        idempotency_key="rejected-open-order",
        rejection_reason="broker_rejected_order",
        source_service="execution_service",
        correlation_id=correlation_id,
        causation_id=created_order.event_id,
    )

    event_bus.publish(rejection_event)

    intent_events = _intent_events(event_store)

    assert [event.intent_state for event in intent_events] == ["pending", "accepted", "cancelled"]
    assert intent_events[-1].cancellation_reason == "broker_rejected_order"
    assert intent_events[-1].causation_id == rejection_event.event_id
