"""
Full Lifecycle Integration Test (Phase 5).

Tests the complete event-driven backtest cycle:
    Bar → PortfolioService → PortfolioStateEvent
        → ManagerService caches state
        → Strategy → SignalEvent
        → ManagerService (uses cached state) → OrderEvent
        → ExecutionService → FillEvent
        → PortfolioService → updates positions

Verifies:
- Real equity used in sizing calculations (not fake metadata)
- Real positions used in limit checks (not empty list)
- Fills properly update portfolio state
- Complete audit trail with signal_id, intent_id, idempotency_key
"""

import uuid
from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import FillEvent, OrderEvent, PortfolioStateEvent, PriceBarEvent, SignalEvent
from qs_trader.libraries.risk import load_policy
from qs_trader.services.execution.config import ExecutionConfig
from qs_trader.services.execution.service import ExecutionService
from qs_trader.services.manager.service import ManagerService
from qs_trader.services.portfolio.models import PortfolioConfig
from qs_trader.services.portfolio.service import PortfolioService


@pytest.fixture
def event_bus():
    """Create a fresh event bus for each test."""
    return EventBus()


@pytest.fixture
def portfolio_service(event_bus):
    """Create PortfolioService with $100,000 initial capital."""
    config = PortfolioConfig(
        initial_cash=Decimal("100000.00"),
    )
    return PortfolioService(config=config, event_bus=event_bus)


@pytest.fixture
def execution_service(event_bus):
    """Create ExecutionService with default config."""
    config = ExecutionConfig()
    return ExecutionService(config=config, event_bus=event_bus)


@pytest.fixture
def manager_service(event_bus):
    """Create ManagerService with test_naive risk policy (5% sizing for predictable tests)."""
    risk_config = load_policy("test_naive")
    return ManagerService(risk_config=risk_config, event_bus=event_bus)


def test_full_lifecycle_bar_to_fill(event_bus, portfolio_service, execution_service, manager_service):
    """
    Test complete lifecycle: Bar → Portfolio → Signal → Manager → Order → Execution → Fill → Portfolio.

    Flow:
        1. Publish PriceBarEvent
        2. PortfolioService marks to market → PortfolioStateEvent
        3. ManagerService caches portfolio state
        4. Publish SignalEvent (strategy emits signal)
        5. ManagerService uses cached equity → OrderEvent
        6. ExecutionService simulates fill → FillEvent
        7. PortfolioService updates position
        8. Verify position exists with correct quantity
    """
    # Capture events for verification
    captured_events = []

    def capture_event(event):
        captured_events.append(event)

    event_bus.subscribe("portfolio_state", capture_event)
    event_bus.subscribe("order", capture_event)
    event_bus.subscribe("fill", capture_event)

    # Step 1: Publish bar (simulates DataService)
    bar = PriceBarEvent(
        timestamp="2020-01-02T16:00:00Z",
        symbol="AAPL",
        interval="1d",
        open=Decimal("150.00"),
        high=Decimal("152.00"),
        low=Decimal("149.00"),
        close=Decimal("151.00"),
        volume=1000000,
        source="test",
    )
    event_bus.publish(bar)

    # Step 2: Verify PortfolioStateEvent was published
    portfolio_events = [e for e in captured_events if isinstance(e, PortfolioStateEvent)]
    assert len(portfolio_events) == 1, "PortfolioStateEvent should be published after bar"

    state = portfolio_events[0]
    assert state.current_portfolio_equity == Decimal("100000.00"), "Initial equity should be $100,000"
    assert state.cash_balance == Decimal("100000.00"), "All cash, no positions yet"
    assert len(state.strategies_groups) == 0, "No positions yet"

    # Step 3: Verify ManagerService cached the state
    assert manager_service._cached_equity == Decimal("100000.00"), "Manager should cache equity"
    assert manager_service._cached_positions == [], "No positions cached yet"

    # Step 4: Emit signal (simulates Strategy)
    signal_id = f"momentum-{uuid.uuid4()}"
    signal = SignalEvent(
        signal_id=signal_id,
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="momentum",
        symbol="AAPL",
        intention="OPEN_LONG",
        price=Decimal("151.00"),
        confidence=Decimal("0.75"),
        # Note: NO metadata equity - manager should use cached equity
    )
    event_bus.publish(signal)

    # Step 5: Verify OrderEvent was emitted
    order_events = [e for e in captured_events if isinstance(e, OrderEvent)]
    assert len(order_events) == 1, "OrderEvent should be emitted"

    order = order_events[0]
    assert order.symbol == "AAPL"
    assert order.side == "buy"
    assert order.quantity > 0, "Quantity should be calculated from cached equity"
    assert order.intent_id == signal_id, "Order should reference signal"
    assert signal_id in order.idempotency_key, "Idempotency key should include signal_id"

    # Step 5b: Publish another bar to trigger fill (ExecutionService processes fills on bar)
    # Note: Market orders are queued for 1 bar by default, so we need TWO bars after the order
    bar2 = PriceBarEvent(
        timestamp="2020-01-03T16:00:00Z",
        symbol="AAPL",
        interval="1d",
        open=Decimal("151.00"),
        high=Decimal("152.00"),
        low=Decimal("150.00"),
        close=Decimal("151.50"),
        volume=1000000,
        source="test",
    )
    event_bus.publish(bar2)  # This queues the order (bars_queued=1)

    bar3 = PriceBarEvent(
        timestamp="2020-01-04T16:00:00Z",
        symbol="AAPL",
        interval="1d",
        open=Decimal("151.50"),
        high=Decimal("153.00"),
        low=Decimal("151.00"),
        close=Decimal("152.00"),
        volume=1000000,
        source="test",
    )
    event_bus.publish(bar3)  # This fills the order (bars_queued=1, threshold met)

    # Step 6: Verify FillEvent was emitted
    fill_events = [e for e in captured_events if isinstance(e, FillEvent)]
    assert len(fill_events) == 1, "FillEvent should be emitted"

    fill = fill_events[0]
    assert fill.symbol == "AAPL"
    assert fill.side == "buy"
    assert fill.filled_quantity == order.quantity, "Fill quantity should match order"
    assert fill.fill_price > 0, "Fill price should be set"
    assert fill.commission >= 0, "Commission should be non-negative"

    # Step 7: Verify PortfolioService updated positions
    portfolio_state = portfolio_service.get_state()
    assert len(portfolio_state.positions) == 1, "Should have 1 position after fill"

    position = list(portfolio_state.positions.values())[0]  # Get first position from dict
    assert position.symbol == "AAPL"
    assert position.quantity == fill.filled_quantity, "Position quantity should match fill"
    assert position.side == "long", "Should be long position"
    assert portfolio_state.cash < Decimal("100000.00"), "Cash should decrease after buy"


def test_full_lifecycle_uses_real_equity_not_metadata(event_bus, portfolio_service, execution_service, manager_service):
    """
    Test that ManagerService uses cached equity from PortfolioStateEvent, NOT signal metadata.

    This verifies Phase 5 integration is working correctly.
    """
    # Step 1: Publish bar to trigger portfolio state
    bar = PriceBarEvent(
        timestamp="2020-01-02T16:00:00Z",
        symbol="AAPL",
        interval="1d",
        open=Decimal("150.00"),
        high=Decimal("152.00"),
        low=Decimal("149.00"),
        close=Decimal("151.00"),
        volume=1000000,
        source="test",
    )
    event_bus.publish(bar)

    # Verify cached equity
    assert manager_service._cached_equity == Decimal("100000.00")

    # Step 2: Emit signal with WRONG equity in metadata
    signal = SignalEvent(
        signal_id=f"test-{uuid.uuid4()}",
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="momentum",
        symbol="AAPL",
        intention="OPEN_LONG",
        price=Decimal("151.00"),
        confidence=Decimal("0.75"),
        metadata={"equity": 1000000.0},  # WRONG: 10x actual equity
    )

    captured_orders = []
    event_bus.subscribe("order", lambda e: captured_orders.append(e))

    event_bus.publish(signal)

    # Step 3: Verify order uses CACHED equity (100k), NOT metadata equity (1M)
    assert len(captured_orders) == 1
    order = captured_orders[0]

    # With $100k equity, 5% sizing, confidence 0.75, price $151
    # quantity = (100000 * 0.95 * 0.05 * 0.75) / 151 ≈ 23 shares
    # With $1M equity (if metadata was used):
    # quantity = (1000000 * 0.95 * 0.05 * 0.75) / 151 ≈ 235 shares

    # Verify quantity is consistent with $100k, not $1M
    assert order.quantity < 50, f"Quantity {order.quantity} suggests cached equity used (100k), not metadata (1M)"
    assert order.quantity > 10, "Quantity should be reasonable for $100k equity"


def test_full_lifecycle_multiple_bars_updates_equity(event_bus, portfolio_service, execution_service, manager_service):
    """
    Test that portfolio equity updates correctly across multiple bars.

    Verifies:
    - First bar: $100k equity → order sized for $100k
    - Fill applied: equity decreases by cost
    - Second bar: mark-to-market → new equity
    - Manager caches updated equity
    """
    captured_orders = []
    captured_state = []
    event_bus.subscribe("order", lambda e: captured_orders.append(e))
    event_bus.subscribe("portfolio_state", lambda e: captured_state.append(e))

    # Bar 1: AAPL at $100
    bar1 = PriceBarEvent(
        timestamp="2020-01-02T16:00:00Z",
        symbol="AAPL",
        interval="1d",
        open=Decimal("100.00"),
        high=Decimal("102.00"),
        low=Decimal("99.00"),
        close=Decimal("100.00"),
        volume=1000000,
        source="test",
    )
    event_bus.publish(bar1)

    # Verify initial equity
    assert manager_service._cached_equity == Decimal("100000.00")

    # Signal 1: Buy AAPL
    signal1 = SignalEvent(
        signal_id=f"sig1-{uuid.uuid4()}",
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="momentum",
        symbol="AAPL",
        intention="OPEN_LONG",
        price=Decimal("100.00"),
        confidence=Decimal("0.8"),
    )
    event_bus.publish(signal1)

    # Verify order created
    assert len(captured_orders) == 1
    assert captured_orders[0].quantity > 0

    # Bar 2: Queue the order (bars_queued=1)
    bar2 = PriceBarEvent(
        timestamp="2020-01-03T16:00:00Z",
        symbol="AAPL",
        interval="1d",
        open=Decimal("105.00"),
        high=Decimal("106.00"),
        low=Decimal("104.00"),
        close=Decimal("105.00"),
        volume=1500000,
        source="test",
    )
    event_bus.publish(bar2)

    # Bar 3: Fill the order at open ($105) and AAPL moves to $110 (close)
    bar3 = PriceBarEvent(
        timestamp="2020-01-04T16:00:00Z",
        symbol="AAPL",
        interval="1d",
        open=Decimal("105.00"),  # Fill happens at open
        high=Decimal("112.00"),
        low=Decimal("104.00"),
        close=Decimal("110.00"),  # Position marked to market at close
        volume=1500000,
        source="test",
    )
    event_bus.publish(bar3)

    # The fill-applied portfolio snapshot is published at the fill price first.
    # Publish the next bar so the open AAPL position is marked to market at $110
    # before sizing the next signal.
    bar4 = PriceBarEvent(
        timestamp="2020-01-05T16:00:00Z",
        symbol="AAPL",
        interval="1d",
        open=Decimal("110.00"),
        high=Decimal("111.00"),
        low=Decimal("109.00"),
        close=Decimal("110.00"),
        volume=1500000,
        source="test",
    )
    event_bus.publish(bar4)

    # Verify equity increased after the next mark-to-market update.
    new_equity = manager_service._cached_equity
    assert new_equity is not None
    assert new_equity > Decimal("100000.00"), "Equity should increase after AAPL is marked from the $105 fill to $110"

    # Signal 2: Another buy
    signal2 = SignalEvent(
        signal_id=f"sig2-{uuid.uuid4()}",
        timestamp="2020-01-05T16:00:00Z",
        strategy_id="momentum",
        symbol="MSFT",
        intention="OPEN_LONG",
        price=Decimal("200.00"),
        confidence=Decimal("0.8"),
    )
    event_bus.publish(signal2)

    # Verify new order uses updated equity
    assert len(captured_orders) == 2
    order2 = captured_orders[1]

    # With higher equity, order size should potentially be larger
    # (though this depends on specific sizing parameters)
    assert order2.quantity > 0, "Second order should be sized based on updated equity"


def test_full_lifecycle_signal_without_portfolio_state_falls_back(event_bus, manager_service):
    """
    Test that signal is rejected when no cached equity (PortfolioStateEvent not received).

    Manager requires PortfolioStateEvent before processing signals to know current equity.
    """
    captured_orders = []
    event_bus.subscribe("order", lambda e: captured_orders.append(e))

    # Emit signal WITHOUT publishing PortfolioStateEvent first (no cached state)
    signal = SignalEvent(
        signal_id=f"fallback-{uuid.uuid4()}",
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="momentum",
        symbol="AAPL",
        intention="OPEN_LONG",
        price=Decimal("150.00"),
        confidence=Decimal("0.75"),
        metadata={"equity": 100000.0},  # Metadata equity is no longer used
    )
    event_bus.publish(signal)

    # Signal should be rejected (no cached equity from PortfolioStateEvent)
    assert len(captured_orders) == 0, "Should reject signal when no cached equity"


def test_full_lifecycle_signal_without_equity_rejected(event_bus, manager_service):
    """
    Test that signal without cached equity AND without metadata is rejected.
    """
    captured_orders = []
    event_bus.subscribe("order", lambda e: captured_orders.append(e))

    # Emit signal without cached state and without metadata
    signal = SignalEvent(
        signal_id=f"no-equity-{uuid.uuid4()}",
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="momentum",
        symbol="AAPL",
        intention="OPEN_LONG",
        price=Decimal("150.00"),
        confidence=Decimal("0.75"),
        # NO metadata
    )
    event_bus.publish(signal)

    # Should be rejected
    assert len(captured_orders) == 0, "Signal without equity should be rejected"
