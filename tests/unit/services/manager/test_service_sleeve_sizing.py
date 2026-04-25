"""Focused unit tests for sleeve-aware manager sizing and lifecycle propagation."""

from decimal import Decimal

from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import BaseEvent, OrderEvent, PortfolioStateEvent, SignalEvent
from qs_trader.events.lifecycle_context import LifecycleRunContext
from qs_trader.events.lifecycle_events import OrderIntentEvent, OrderLifecycleEvent
from qs_trader.libraries.risk.models import (
    ConcentrationLimit,
    LeverageLimit,
    RiskConfig,
    ShortingPolicy,
    SizingConfig,
    StrategyBudget,
    SleeveBudget,
    SleeveId,
)
from qs_trader.services.manager.service import ManagerService


def _build_service(allocated_equity: str) -> tuple[ManagerService, EventBus, InMemoryEventStore]:
    event_bus = EventBus()
    event_store = InMemoryEventStore()
    event_bus.attach_store(event_store)

    sleeve_budget = SleeveBudget(
        sleeve_id=SleeveId(strategy_id="sma_crossover", sleeve_key="AAPL"),
        allocated_equity=Decimal(allocated_equity),
        symbols=("AAPL",),
    )
    risk_config = RiskConfig(
        budgets=[StrategyBudget(strategy_id="sma_crossover", capital_weight=0.95)],
        sizing={
            "sma_crossover": SizingConfig(
                model="fixed_fraction",
                fraction=Decimal("0.10"),
                min_quantity=1,
                lot_size=1,
            )
        },
        concentration=ConcentrationLimit(max_position_pct=1.0),
        leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
        shorting=ShortingPolicy(allow_short_positions=False),
        cash_buffer_pct=0.05,
        sleeve_budget=sleeve_budget,
    )
    lifecycle_context = LifecycleRunContext(
        experiment_id="exp",
        run_id="run-001",
        sleeve_id=str(sleeve_budget.sleeve_id),
    )
    service = ManagerService(risk_config, event_bus, lifecycle_context=lifecycle_context)
    service.on_portfolio_state(
        PortfolioStateEvent(
            portfolio_id="portfolio-001",
            start_datetime="2024-01-01T00:00:00Z",
            snapshot_datetime="2024-01-02T00:00:00Z",
            reporting_currency="USD",
            initial_portfolio_equity=Decimal("100000"),
            cash_balance=Decimal("100000"),
            current_portfolio_equity=Decimal("100000"),
            total_market_value=Decimal("0"),
            total_unrealized_pl=Decimal("0"),
            total_realized_pl=Decimal("0"),
            total_pl=Decimal("0"),
            long_exposure=Decimal("0"),
            short_exposure=Decimal("0"),
            net_exposure=Decimal("0"),
            gross_exposure=Decimal("0"),
            leverage=Decimal("0"),
            strategies_groups=[],
        )
    )
    return service, event_bus, event_store


def _make_signal(symbol: str = "AAPL") -> SignalEvent:
    return SignalEvent(
        signal_id=f"sig-{symbol.lower()}",
        timestamp="2024-01-02T16:00:00Z",
        strategy_id="sma_crossover",
        symbol=symbol,
        intention="OPEN_LONG",
        price=Decimal("100.00"),
        confidence=Decimal("1.00"),
        source_service="strategy_service",
        correlation_id="550e8400-e29b-41d4-a716-446655440301",
        causation_id="550e8400-e29b-41d4-a716-446655440302",
    )


def test_manager_sizes_open_orders_from_frozen_sleeve_equity() -> None:
    service, event_bus, _ = _build_service("20000")
    orders: list[OrderEvent] = []

    def capture_order(event: BaseEvent) -> None:
        assert isinstance(event, OrderEvent)
        orders.append(event)

    event_bus.subscribe("order", capture_order)

    service.on_signal(_make_signal())

    assert len(orders) == 1
    assert orders[0].quantity == Decimal("20")


def test_manager_falls_back_to_strategy_budget_when_signal_symbol_is_not_in_sleeve() -> None:
    service, event_bus, _ = _build_service("20000")
    orders: list[OrderEvent] = []

    def capture_order(event: BaseEvent) -> None:
        assert isinstance(event, OrderEvent)
        orders.append(event)

    event_bus.subscribe("order", capture_order)

    service.on_signal(_make_signal(symbol="MSFT"))

    assert len(orders) == 1
    assert orders[0].quantity == Decimal("95")


def test_manager_lifecycle_events_include_sleeve_id_when_bound() -> None:
    service, _, event_store = _build_service("20000")

    service.on_signal(_make_signal())

    intent_events = [event for event in event_store.get_by_type("order_intent") if isinstance(event, OrderIntentEvent)]
    order_events = [
        event for event in event_store.get_by_type("order_lifecycle") if isinstance(event, OrderLifecycleEvent)
    ]

    assert [event.sleeve_id for event in intent_events] == ["sma_crossover:AAPL", "sma_crossover:AAPL"]
    assert [event.sleeve_id for event in order_events] == ["sma_crossover:AAPL"]
