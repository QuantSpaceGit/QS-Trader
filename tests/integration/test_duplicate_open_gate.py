"""Integration regression coverage for the Phase 3 lifecycle gate."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import FillEvent, PortfolioPosition, PortfolioStateEvent, StrategyGroup
from qs_trader.events.lifecycle_context import LifecycleRunContext
from qs_trader.events.lifecycle_events import OrderIntentEvent, OrderLifecycleEvent, StrategyDecisionEvent
from qs_trader.services.manager import LifecycleIntentProjection, ManagerService
from qs_trader.services.reporting.lifecycle_event_collector import collect_run_lifecycle_events
from qs_trader.services.strategy.context import Context
from qs_trader.services.strategy.models import LifecycleIntentType, PositionState, SignalIntention

FIXTURES_DIR = Path(__file__).resolve().parent.parent / "fixtures"
DUPLICATE_WINDOW_PATH = FIXTURES_DIR / "data" / "sma_crossover_duplicate_window.json"


def _load_duplicate_window_series() -> list[tuple[str, Decimal]]:
    """Load the checked-in duplicate-open reconstruction window for Phase 3 evidence."""
    rows = json.loads(DUPLICATE_WINDOW_PATH.read_text(encoding="utf-8"))
    return [(row["timestamp"], Decimal(str(row["close"]))) for row in rows]


def _legacy_duplicate_open_dates(
    series: list[tuple[str, Decimal]],
    *,
    fast_period: int,
    slow_period: int,
) -> list[str]:
    """Return the OPEN_LONG dates produced by the pre-fix SMA window math."""

    closes = [price for _, price in series]
    open_long_dates: list[str] = []

    for index, (timestamp, _) in enumerate(series):
        bars = closes[index - slow_period : index + 1]
        if len(bars) < slow_period + 1:
            continue

        current_bars = bars[:-1]
        previous_bars = bars[:-2]
        fast_sma = sum(current_bars[-fast_period:]) / fast_period
        slow_sma = sum(current_bars) / len(current_bars)
        prev_fast_sma = sum(previous_bars[-fast_period:]) / fast_period
        prev_slow_sma = sum(previous_bars) / len(previous_bars)

        if prev_fast_sma <= prev_slow_sma and fast_sma > slow_sma:
            open_long_dates.append(timestamp[:10])

    return open_long_dates


def _portfolio_state(
    *,
    quantity: int = 0,
    current_portfolio_equity: Decimal = Decimal("100000.00"),
) -> PortfolioStateEvent:
    """Build a minimal portfolio snapshot for manager state sync."""
    market_value = Decimal("0.00") if quantity == 0 else Decimal(str(abs(quantity) * 100))
    cash_balance = current_portfolio_equity - market_value if quantity > 0 else current_portfolio_equity
    positions = []
    if quantity != 0:
        positions = [
            PortfolioPosition(
                symbol="AAPL",
                side="long" if quantity > 0 else "short",
                open_quantity=quantity,
                average_fill_price=Decimal("100.00"),
                commission_paid=Decimal("0.00"),
                cost_basis=Decimal("10000.00"),
                market_price=Decimal("100.00"),
                gross_market_value=Decimal(str(abs(quantity) * 100)),
                unrealized_pl=Decimal("0.00"),
                realized_pl=Decimal("0.00"),
                dividends_received=Decimal("0.00"),
                dividends_paid=Decimal("0.00"),
                total_position_value=Decimal(str(abs(quantity) * 100)),
                currency="USD",
                last_updated="2024-01-02T14:29:00Z",
            )
        ]

    groups = [StrategyGroup(strategy_id="sma_crossover", positions=positions)] if positions else []
    return PortfolioStateEvent(
        portfolio_id="test-portfolio",
        start_datetime="2024-01-01T00:00:00Z",
        snapshot_datetime="2024-01-02T14:29:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=current_portfolio_equity,
        cash_balance=cash_balance,
        current_portfolio_equity=current_portfolio_equity,
        total_market_value=market_value,
        total_unrealized_pl=Decimal("0.00"),
        total_realized_pl=Decimal("0.00"),
        total_pl=Decimal("0.00"),
        long_exposure=market_value if quantity > 0 else Decimal("0.00"),
        short_exposure=market_value if quantity < 0 else Decimal("0.00"),
        net_exposure=Decimal(str(quantity * 100)),
        gross_exposure=market_value,
        leverage=(market_value / current_portfolio_equity) if quantity != 0 else Decimal("0.00"),
        strategies_groups=groups,
    )


def test_duplicate_same_side_open_is_suppressed_during_pending_fill_latency() -> None:
    """Manager and strategy context should share the same pending/open lifecycle view."""
    event_store = InMemoryEventStore()
    event_bus = EventBus()
    event_bus.attach_store(event_store)
    lifecycle_context = LifecycleRunContext(experiment_id="exp", run_id="run-001")
    projection = LifecycleIntentProjection()
    projection.bind(event_bus)

    manager = ManagerService.from_config(
        {"name": "naive", "config": {}},
        event_bus,
        lifecycle_context=lifecycle_context,
        lifecycle_projection=projection,
    )
    context = Context(
        strategy_id="sma_crossover",
        event_bus=event_bus,
        lifecycle_context=lifecycle_context,
        lifecycle_projection=projection,
    )

    event_bus.publish(_portfolio_state())

    context.emit_signal(
        timestamp="2024-01-02T14:30:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("100.00"),
        confidence=Decimal("1.0"),
        reason="first open",
    )

    assert context.get_position_state("AAPL") == PositionState.PENDING_OPEN_LONG
    assert len(event_store.get_by_type("order")) == 1

    context.emit_signal(
        timestamp="2024-01-02T14:30:30Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("100.00"),
        confidence=Decimal("1.0"),
        reason="duplicate open during latency",
    )

    intent_events = [
        event for event in event_store.get_by_type("order_intent") if isinstance(event, OrderIntentEvent)
    ]
    created_order = [
        event for event in event_store.get_by_type("order_lifecycle") if isinstance(event, OrderLifecycleEvent)
    ][0]

    assert [event.intent_state for event in intent_events] == ["pending", "accepted", "pending", "suppressed"]
    assert intent_events[-1].suppression_reason == "duplicate_same_side_pending"
    assert len(event_store.get_by_type("order")) == 1

    filled_order_event = OrderLifecycleEvent(
        experiment_id=lifecycle_context.experiment_id,
        run_id=lifecycle_context.run_id,
        occurred_at=datetime(2024, 1, 2, 14, 31, tzinfo=timezone.utc),
        order_id=created_order.order_id,
        intent_id=intent_events[1].intent_id,
        strategy_id="sma_crossover",
        symbol="AAPL",
        order_state="filled",
        side="buy",
        quantity=created_order.quantity,
        filled_quantity=created_order.quantity,
        order_type="market",
        time_in_force="GTC",
        price_basis=lifecycle_context.execution_price_basis,
        idempotency_key=created_order.idempotency_key,
        source_service="execution_service",
        correlation_id=created_order.correlation_id,
        causation_id=created_order.event_id,
    )
    event_bus.publish(filled_order_event)
    event_bus.publish(
        FillEvent(
            fill_id="550e8400-e29b-41d4-a716-446655440212",
            source_order_id=created_order.order_id,
            timestamp="2024-01-02T14:31:00Z",
            symbol="AAPL",
            side="buy",
            filled_quantity=created_order.quantity,
            fill_price=Decimal("100.00"),
            strategy_id="sma_crossover",
            source_service="execution_service",
            correlation_id=created_order.correlation_id,
            causation_id=filled_order_event.event_id,
        )
    )

    assert context.get_position_state("AAPL") == PositionState.OPEN_LONG

    lifecycle_rows = collect_run_lifecycle_events("exp", "run-001", event_store)
    assert lifecycle_rows
    assert all("price_basis" in json.loads(row["payload_json"]) for row in lifecycle_rows)
    assert {json.loads(row["payload_json"])["price_basis"] for row in lifecycle_rows} == {
        "adjusted_ohlc_adj_columns"
    }


def test_reconstructed_duplicate_window_2021_10_25_is_suppressed_and_recorded() -> None:
    """Reconstruct the documented 2021-10-22 / 2021-10-25 duplicate-open window.

    The exact `r-001-68fe9c2c` payload is not checked into this repository. This
    regression uses `tests/fixtures/data/sma_crossover_duplicate_window.json`, a
    local checked-in reconstruction derived from the already-landed Research
    fixture, to prove the manager accepts the first `OPEN_LONG` on 2021-10-22,
    suppresses the reconstructed duplicate `OPEN_LONG` on 2021-10-25 while the
    first intent is still pending, and records that suppression as a canonical
    lifecycle row.
    """
    reconstructed_series = _load_duplicate_window_series()

    assert [timestamp[:10] for timestamp, _ in reconstructed_series] == [
        "2021-10-14",
        "2021-10-15",
        "2021-10-18",
        "2021-10-19",
        "2021-10-20",
        "2021-10-21",
        "2021-10-22",
        "2021-10-25",
    ]
    assert _legacy_duplicate_open_dates(
        reconstructed_series,
        fast_period=2,
        slow_period=5,
    ) == ["2021-10-22", "2021-10-25"]

    timestamps_by_date = {timestamp[:10]: timestamp for timestamp, _ in reconstructed_series}
    prices_by_date = {timestamp[:10]: price for timestamp, price in reconstructed_series}

    event_store = InMemoryEventStore()
    event_bus = EventBus()
    event_bus.attach_store(event_store)
    lifecycle_context = LifecycleRunContext(experiment_id="exp", run_id="run-001")
    projection = LifecycleIntentProjection()
    projection.bind(event_bus)

    ManagerService.from_config(
        {"name": "naive", "config": {}},
        event_bus,
        lifecycle_context=lifecycle_context,
        lifecycle_projection=projection,
    )
    context = Context(
        strategy_id="sma_crossover",
        event_bus=event_bus,
        lifecycle_context=lifecycle_context,
        lifecycle_projection=projection,
    )

    event_bus.publish(_portfolio_state())

    context.emit_signal(
        timestamp=timestamps_by_date["2021-10-22"],
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=prices_by_date["2021-10-22"],
        confidence=Decimal("1.0"),
        reason="reconstructed 2021-10-22 incident-window open",
    )
    context.emit_signal(
        timestamp=timestamps_by_date["2021-10-25"],
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=prices_by_date["2021-10-25"],
        confidence=Decimal("1.0"),
        reason="reconstructed 2021-10-25 duplicate open from checked-in fixture",
    )

    decision_events = [
        event for event in event_store.get_by_type("strategy_decision") if isinstance(event, StrategyDecisionEvent)
    ]
    intent_events = [
        event for event in event_store.get_by_type("order_intent") if isinstance(event, OrderIntentEvent)
    ]
    accepted_intent = next(event for event in intent_events if event.intent_state == "accepted")
    suppressed_intent = next(event for event in intent_events if event.intent_state == "suppressed")

    assert [event.bar_timestamp[:10] for event in decision_events] == ["2021-10-22", "2021-10-25"]
    assert [event.intent_state for event in intent_events] == ["pending", "accepted", "pending", "suppressed"]
    assert accepted_intent.occurred_at.date().isoformat() == "2021-10-22"
    assert suppressed_intent.occurred_at.date().isoformat() == "2021-10-25"
    assert suppressed_intent.suppression_reason == "duplicate_same_side_pending"
    assert context.get_position_state("AAPL") == PositionState.PENDING_OPEN_LONG
    assert len(event_store.get_by_type("order")) == 1

    lifecycle_rows = collect_run_lifecycle_events("exp", "run-001", event_store)
    accepted_dates = [
        row["event_timestamp"].date().isoformat()
        for row in lifecycle_rows
        if row["lifecycle_family"] == "order_intent"
        and json.loads(row["payload_json"])["intent_state"] == "accepted"
    ]
    suppressed_rows = [
        row
        for row in lifecycle_rows
        if row["lifecycle_family"] == "order_intent"
        and json.loads(row["payload_json"])["intent_state"] == "suppressed"
    ]

    assert accepted_dates == ["2021-10-22"]
    assert len(suppressed_rows) == 1

    suppressed_row = suppressed_rows[0]
    suppressed_payload = json.loads(suppressed_row["payload_json"])

    assert suppressed_row["event_timestamp"].date().isoformat() == "2021-10-25"
    assert suppressed_payload["intent_type"] == "open"
    assert suppressed_payload["direction"] == "long"
    assert suppressed_payload["suppression_reason"] == "duplicate_same_side_pending"
    assert suppressed_payload["price_basis"] == "adjusted_ohlc_adj_columns"


def test_explicit_scale_in_is_accepted_from_context_through_manager_runtime_path() -> None:
    """Explicit scale-in opt-ins should survive Context → Manager runtime handling unchanged."""
    event_store = InMemoryEventStore()
    event_bus = EventBus()
    event_bus.attach_store(event_store)
    lifecycle_context = LifecycleRunContext(experiment_id="exp", run_id="run-001")
    projection = LifecycleIntentProjection()
    projection.bind(event_bus)

    ManagerService.from_config(
        {"name": "naive", "config": {}},
        event_bus,
        lifecycle_context=lifecycle_context,
        lifecycle_projection=projection,
    )
    context = Context(
        strategy_id="sma_crossover",
        event_bus=event_bus,
        lifecycle_context=lifecycle_context,
        lifecycle_projection=projection,
    )

    event_bus.publish(_portfolio_state(quantity=100, current_portfolio_equity=Decimal("300000.00")))

    context.emit_signal(
        timestamp="2024-01-02T14:30:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        intent_type=LifecycleIntentType.SCALE_IN,
        price=Decimal("100.00"),
        confidence=Decimal("1.0"),
        reason="add to existing long",
    )

    decision_events = [
        event for event in event_store.get_by_type("strategy_decision") if isinstance(event, StrategyDecisionEvent)
    ]
    intent_events = [
        event for event in event_store.get_by_type("order_intent") if isinstance(event, OrderIntentEvent)
    ]

    assert context.get_position_state("AAPL") == PositionState.OPEN_LONG
    assert len(event_store.get_by_type("order")) == 1
    assert decision_events[-1].decision_type == "scale_in"
    assert [event.intent_state for event in intent_events] == ["pending", "accepted"]
    assert [event.intent_type for event in intent_events] == ["scale_in", "scale_in"]