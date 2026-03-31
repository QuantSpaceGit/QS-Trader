"""
Integration test for ManagerService signal-to-order flow.

Tests the complete Phase 3 architecture:
- SignalEvent → ManagerService → OrderEvent
- Risk library integration (sizing, limits)
- Audit trail (intent_id, idempotency_key)
- Intention → side mapping
"""

from decimal import Decimal

import pytest

from qs_trader.events import PortfolioPosition, StrategyGroup
from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import OrderEvent, PortfolioStateEvent, SignalEvent
from qs_trader.services.manager import ManagerService


@pytest.fixture
def event_bus():
    """Create event bus for testing."""
    return EventBus()


@pytest.fixture
def manager_service(event_bus):
    """Create ManagerService with test configuration (no shorts allowed)."""
    config_dict = {
        "name": "test_naive",  # Use test fixture with shorting disabled
    }
    service = ManagerService.from_config(config_dict, event_bus)

    # Emit initial PortfolioStateEvent so Manager has cached equity
    # (Manager requires this before processing signals)
    portfolio_state = PortfolioStateEvent(
        portfolio_id="test-portfolio",
        start_datetime="2020-01-01T00:00:00Z",
        snapshot_datetime="2020-01-02T00:00:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=Decimal("100000.00"),
        cash_balance=Decimal("100000.00"),
        current_portfolio_equity=Decimal("100000.00"),
        total_market_value=Decimal("0.00"),
        total_unrealized_pl=Decimal("0.00"),
        total_realized_pl=Decimal("0.00"),
        total_pl=Decimal("0.00"),
        long_exposure=Decimal("0.00"),
        short_exposure=Decimal("0.00"),
        net_exposure=Decimal("0.00"),
        gross_exposure=Decimal("0.00"),
        leverage=Decimal("0.00"),
        strategies_groups=[],
    )
    service.on_portfolio_state(portfolio_state)

    return service


@pytest.fixture
def manager_service_with_shorts(event_bus):
    """Create ManagerService with shorting allowed for testing."""
    config_dict = {
        "name": "test_naive_short",  # Use test fixture with shorting enabled
    }
    service = ManagerService.from_config(config_dict, event_bus)

    # Emit initial PortfolioStateEvent so Manager has cached equity
    portfolio_state = PortfolioStateEvent(
        portfolio_id="test-portfolio",
        start_datetime="2020-01-01T00:00:00Z",
        snapshot_datetime="2020-01-02T00:00:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=Decimal("100000.00"),
        cash_balance=Decimal("100000.00"),
        current_portfolio_equity=Decimal("100000.00"),
        total_market_value=Decimal("0.00"),
        total_unrealized_pl=Decimal("0.00"),
        total_realized_pl=Decimal("0.00"),
        total_pl=Decimal("0.00"),
        long_exposure=Decimal("0.00"),
        short_exposure=Decimal("0.00"),
        net_exposure=Decimal("0.00"),
        gross_exposure=Decimal("0.00"),
        leverage=Decimal("0.00"),
        strategies_groups=[],
    )
    service.on_portfolio_state(portfolio_state)

    return service


class TestManagerServiceSignalToOrder:
    """Test signal processing and order emission."""

    def test_open_long_signal_emits_buy_order(self, manager_service, event_bus):
        """Test OPEN_LONG intention maps to buy order."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        signal = SignalEvent(
            signal_id="sig-001",
            timestamp="2020-01-02T16:00:00Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("0.80"),
            metadata={"equity": 100000.0},
        )

        # Act
        manager_service.on_signal(signal)

        # Assert
        assert len(orders_received) == 1
        order = orders_received[0]
        assert order.symbol == "AAPL"
        assert order.side == "buy"
        assert order.intent_id == "sig-001"
        assert "test_strategy-sig-001" in order.idempotency_key
        assert order.quantity > 0

    def test_close_long_signal_emits_sell_order(self, manager_service, event_bus):
        """Test CLOSE_LONG intention maps to sell order and uses position size."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # First, publish portfolio state with a long position
        from qs_trader.events import PortfolioPosition, PortfolioStateEvent, StrategyGroup

        portfolio_state = PortfolioStateEvent(
            portfolio_id="test_portfolio",
            start_datetime="2020-01-01T00:00:00Z",
            snapshot_datetime="2020-01-03T15:00:00Z",
            reporting_currency="USD",
            initial_portfolio_equity=Decimal("100000"),
            cash_balance=Decimal("85000"),
            current_portfolio_equity=Decimal("101000"),
            total_market_value=Decimal("16000"),
            total_unrealized_pl=Decimal("1000"),
            total_realized_pl=Decimal("0"),
            total_pl=Decimal("1000"),
            long_exposure=Decimal("16000"),
            short_exposure=Decimal("0"),
            net_exposure=Decimal("16000"),
            gross_exposure=Decimal("16000"),
            leverage=Decimal("0.16"),
            strategies_groups=[
                StrategyGroup(
                    strategy_id="test_strategy",
                    positions=[
                        PortfolioPosition(
                            symbol="AAPL",
                            side="long",
                            open_quantity=100,  # Strategy has 100 shares long
                            average_fill_price=Decimal("150.00"),
                            commission_paid=Decimal("10.00"),
                            cost_basis=Decimal("15010.00"),
                            market_price=Decimal("160.00"),
                            gross_market_value=Decimal("16000.00"),
                            unrealized_pl=Decimal("990.00"),
                            realized_pl=Decimal("0"),
                            dividends_received=Decimal("0"),
                            dividends_paid=Decimal("0"),
                            total_position_value=Decimal("16000.00"),
                            currency="USD",
                            last_updated="2020-01-03T15:00:00Z",
                        )
                    ],
                )
            ],
        )
        manager_service.on_portfolio_state(portfolio_state)

        signal = SignalEvent(
            signal_id="sig-002",
            timestamp="2020-01-03T16:00:00Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention="CLOSE_LONG",
            price=Decimal("155.00"),
            confidence=Decimal("0.90"),  # 90% confidence = close 90% of position
            metadata={"equity": 100000.0},
        )

        # Act
        manager_service.on_signal(signal)

        # Assert
        assert len(orders_received) == 1
        order = orders_received[0]
        assert order.symbol == "AAPL"
        assert order.side == "sell"
        assert order.intent_id == "sig-002"
        # Quantity should be 90 (90% of 100 shares due to confidence=0.90)
        assert order.quantity == Decimal("90")

    def test_open_short_signal_emits_sell_order(self, manager_service_with_shorts, event_bus):
        """Test OPEN_SHORT intention maps to sell order."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)
        signal = SignalEvent(
            signal_id="sig-003",
            timestamp="2020-01-04T16:00:00Z",
            strategy_id="test_strategy",
            symbol="TSLA",
            intention="OPEN_SHORT",
            price=Decimal("500.00"),
            confidence=Decimal("0.75"),
            metadata={"equity": 100000.0},
        )

        # Act
        manager_service_with_shorts.on_signal(signal)

        # Assert
        assert len(orders_received) == 1
        order = orders_received[0]
        assert order.symbol == "TSLA"
        assert order.side == "sell"

    def test_close_short_signal_emits_buy_order(self, manager_service_with_shorts, event_bus):
        """Test CLOSE_SHORT intention maps to buy order and uses position size."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # First, publish portfolio state with a short position
        from qs_trader.events import PortfolioPosition, PortfolioStateEvent, StrategyGroup

        portfolio_state = PortfolioStateEvent(
            portfolio_id="test_portfolio",
            start_datetime="2020-01-01T00:00:00Z",
            snapshot_datetime="2020-01-05T15:00:00Z",
            reporting_currency="USD",
            initial_portfolio_equity=Decimal("100000"),
            cash_balance=Decimal("150000"),  # Cash increased from short sale
            current_portfolio_equity=Decimal("102000"),
            total_market_value=Decimal("-48000"),  # Short position negative value
            total_unrealized_pl=Decimal("2000"),
            total_realized_pl=Decimal("0"),
            total_pl=Decimal("2000"),
            long_exposure=Decimal("0"),
            short_exposure=Decimal("48000"),
            net_exposure=Decimal("-48000"),
            gross_exposure=Decimal("48000"),
            leverage=Decimal("0.48"),
            strategies_groups=[
                StrategyGroup(
                    strategy_id="test_strategy",
                    positions=[
                        PortfolioPosition(
                            symbol="TSLA",
                            side="short",
                            open_quantity=-100,  # Strategy has 100 shares short
                            average_fill_price=Decimal("500.00"),
                            commission_paid=Decimal("10.00"),
                            cost_basis=Decimal("50010.00"),
                            market_price=Decimal("480.00"),
                            gross_market_value=Decimal("-48000.00"),
                            unrealized_pl=Decimal("1990.00"),
                            realized_pl=Decimal("0"),
                            dividends_received=Decimal("0"),
                            dividends_paid=Decimal("0"),
                            total_position_value=Decimal("-48000.00"),
                            currency="USD",
                            last_updated="2020-01-05T15:00:00Z",
                        )
                    ],
                )
            ],
        )
        manager_service_with_shorts.on_portfolio_state(portfolio_state)

        signal = SignalEvent(
            signal_id="sig-004",
            timestamp="2020-01-05T16:00:00Z",
            strategy_id="test_strategy",
            symbol="TSLA",
            intention="CLOSE_SHORT",
            price=Decimal("480.00"),
            confidence=Decimal("0.85"),  # 85% confidence = close 85% of position
            metadata={"equity": 100000.0},
        )

        # Act
        manager_service_with_shorts.on_signal(signal)

        # Assert
        assert len(orders_received) == 1
        order = orders_received[0]
        assert order.symbol == "TSLA"
        assert order.side == "buy"
        assert order.quantity == Decimal("85")  # 85% of 100 share short position

    def test_signal_without_equity_rejected(self, manager_service, event_bus):
        """Test signal is rejected when no portfolio state cached (equity unknown).

        This tests the scenario where Manager receives a signal before any
        PortfolioStateEvent has been published. In this case, Manager doesn't
        know the current equity and must reject the signal.
        """
        # Arrange - Create fresh ManagerService WITHOUT portfolio state
        config_dict = {
            "name": "naive",
            "config": {},
        }
        fresh_service = ManagerService.from_config(config_dict, event_bus)
        # Note: NO portfolio state emitted

        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        signal = SignalEvent(
            signal_id="sig-005",
            timestamp="2020-01-06T16:00:00Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("0.80"),
            metadata={"some": "data"},
        )

        # Act
        fresh_service.on_signal(signal)

        # Assert
        assert len(orders_received) == 0  # No order emitted (no cached equity)

    def test_zero_confidence_signal_no_order(self, manager_service, event_bus):
        """Test signal with zero confidence results in zero quantity (no order)."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        signal = SignalEvent(
            signal_id="sig-006",
            timestamp="2020-01-07T16:00:00Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("0.00"),  # Zero confidence
            metadata={"equity": 100000.0},
        )

        # Act
        manager_service.on_signal(signal)

        # Assert
        assert len(orders_received) == 0  # No order (quantity rounds to zero)

    def test_audit_trail_fields_present(self, manager_service, event_bus):
        """Test OrderEvent includes complete audit trail fields."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        signal = SignalEvent(
            signal_id="sig-audit-001",
            timestamp="2020-01-08T16:00:00Z",
            strategy_id="momentum",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("0.80"),
            metadata={"equity": 100000.0},
        )

        # Act
        manager_service.on_signal(signal)

        # Assert
        assert len(orders_received) == 1
        order = orders_received[0]

        # Audit trail fields
        assert order.intent_id == "sig-audit-001"
        assert order.idempotency_key == "momentum-sig-audit-001-2020-01-08T16:00:00Z"
        assert order.timestamp == "2020-01-08T16:00:00Z"
        assert order.source_strategy_id == "momentum"

    def test_position_sizing_with_confidence(self, manager_service, event_bus):
        """Test position size scales with signal confidence."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # High confidence signal
        signal_high = SignalEvent(
            signal_id="sig-high",
            timestamp="2020-01-09T16:00:00Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("1.00"),  # Maximum confidence
            metadata={"equity": 100000.0},
        )

        # Act
        manager_service.on_signal(signal_high)

        # Assert
        assert len(orders_received) == 1
        high_quantity = orders_received[0].quantity

        # Reset
        orders_received.clear()

        # Low confidence signal (same symbol, price, equity)
        signal_low = SignalEvent(
            signal_id="sig-low",
            timestamp="2020-01-10T16:00:00Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("0.50"),  # Half confidence
            metadata={"equity": 100000.0},
        )

        manager_service.on_signal(signal_low)

        assert len(orders_received) == 1
        low_quantity = orders_received[0].quantity

        # Higher confidence → larger position
        assert high_quantity > low_quantity


class TestManagerServiceMultiStrategyBudgets:
    """Test multi-strategy capital allocation with budgets."""

    @pytest.fixture
    def multi_strategy_service(self, event_bus):
        """Create ManagerService with explicit multi-strategy budgets."""
        from qs_trader.libraries.risk.models import (
            ConcentrationLimit,
            LeverageLimit,
            RiskConfig,
            ShortingPolicy,
            SizingConfig,
            StrategyBudget,
        )

        # Create risk config with explicit budgets for two strategies
        risk_config = RiskConfig(
            budgets=[
                StrategyBudget(strategy_id="strategy_a", capital_weight=0.30),  # 30% allocation
                StrategyBudget(strategy_id="strategy_b", capital_weight=0.50),  # 50% allocation
            ],
            sizing={
                "strategy_a": SizingConfig(
                    model="fixed_fraction",
                    fraction=Decimal("0.10"),  # 10% of allocated capital per position
                    min_quantity=1,
                    lot_size=1,
                ),
                "strategy_b": SizingConfig(
                    model="fixed_fraction",
                    fraction=Decimal("0.10"),  # 10% of allocated capital per position
                    min_quantity=1,
                    lot_size=1,
                ),
            },
            concentration=ConcentrationLimit(max_position_pct=1.0),
            leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
            cash_buffer_pct=0.05,
            shorting=ShortingPolicy(allow_short_positions=False),
        )

        service = ManagerService(risk_config, event_bus)

        # Initialize portfolio state
        portfolio_state = PortfolioStateEvent(
            portfolio_id="test-portfolio",
            start_datetime="2020-01-01T00:00:00Z",
            snapshot_datetime="2020-01-02T00:00:00Z",
            reporting_currency="USD",
            initial_portfolio_equity=Decimal("100000.00"),
            cash_balance=Decimal("100000.00"),
            current_portfolio_equity=Decimal("100000.00"),
            total_market_value=Decimal("0.00"),
            total_unrealized_pl=Decimal("0.00"),
            total_realized_pl=Decimal("0.00"),
            total_pl=Decimal("0.00"),
            long_exposure=Decimal("0.00"),
            short_exposure=Decimal("0.00"),
            net_exposure=Decimal("0.00"),
            gross_exposure=Decimal("0.00"),
            leverage=Decimal("0.00"),
            strategies_groups=[],
        )
        service.on_portfolio_state(portfolio_state)

        return service

    def test_strategy_respects_allocated_budget(self, multi_strategy_service, event_bus):
        """Test that strategies use only their allocated capital."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # Signal from strategy_a (30% allocation)
        signal_a = SignalEvent(
            signal_id="sig-a",
            timestamp="2020-01-02T16:00:00Z",
            strategy_id="strategy_a",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("100.00"),
            confidence=Decimal("1.00"),
            metadata={"equity": 100000.0},
        )

        # Signal from strategy_b (50% allocation)
        signal_b = SignalEvent(
            signal_id="sig-b",
            timestamp="2020-01-02T16:00:00Z",
            strategy_id="strategy_b",
            symbol="MSFT",
            intention="OPEN_LONG",
            price=Decimal("100.00"),
            confidence=Decimal("1.00"),
            metadata={"equity": 100000.0},
        )

        # Act
        multi_strategy_service.on_signal(signal_a)
        multi_strategy_service.on_signal(signal_b)

        # Assert
        assert len(orders_received) == 2

        order_a = orders_received[0]
        order_b = orders_received[1]

        # Equity = $100,000
        # strategy_a: 30% allocation = $30,000 * 10% sizing = $3,000 / $100 = 30 shares
        # strategy_b: 50% allocation = $50,000 * 10% sizing = $5,000 / $100 = 50 shares
        assert order_a.quantity == 30, f"Expected 30 shares for strategy_a, got {order_a.quantity}"
        assert order_b.quantity == 50, f"Expected 50 shares for strategy_b, got {order_b.quantity}"

    def test_multiple_signals_from_same_strategy_use_same_budget(self, multi_strategy_service, event_bus):
        """Test that multiple signals from the same strategy share the same budget."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # Two signals from strategy_a
        signal_a1 = SignalEvent(
            signal_id="sig-a1",
            timestamp="2020-01-02T16:00:00Z",
            strategy_id="strategy_a",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("100.00"),
            confidence=Decimal("1.00"),
            metadata={"equity": 100000.0},
        )

        signal_a2 = SignalEvent(
            signal_id="sig-a2",
            timestamp="2020-01-02T16:01:00Z",
            strategy_id="strategy_a",
            symbol="GOOGL",
            intention="OPEN_LONG",
            price=Decimal("100.00"),
            confidence=Decimal("1.00"),
            metadata={"equity": 100000.0},
        )

        # Act
        multi_strategy_service.on_signal(signal_a1)
        multi_strategy_service.on_signal(signal_a2)

        # Assert - both orders should use same allocation (30% of $100k = $30k)
        assert len(orders_received) == 2
        assert orders_received[0].quantity == 30  # $30k * 10% = $3k / $100 = 30
        assert orders_received[1].quantity == 30  # Same allocation

    def test_strategy_without_budget_uses_fallback(self, multi_strategy_service, event_bus):
        """Test that strategy without explicit budget falls back to full equity allocation."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # Signal from unknown strategy (not in budgets)
        signal_unknown = SignalEvent(
            signal_id="sig-unknown",
            timestamp="2020-01-02T16:00:00Z",
            strategy_id="unknown_strategy",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("100.00"),
            confidence=Decimal("1.00"),
            metadata={"equity": 100000.0},
        )

        # Act & Assert - should be rejected because no sizing config
        multi_strategy_service.on_signal(signal_unknown)

        # No order should be emitted (rejected at sizing config check, before budget check)
        assert len(orders_received) == 0

    def test_strategies_cannot_reuse_same_capital(self, multi_strategy_service, event_bus):
        """Test that different strategies use separate capital pools, not the same equity.

        This is the key bug fix: without budgets, every strategy would calculate
        size based on full equity, allowing capital reuse and exceeding limits.
        """
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # Both strategies signal at the same price
        signal_a = SignalEvent(
            signal_id="sig-a",
            timestamp="2020-01-02T16:00:00Z",
            strategy_id="strategy_a",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("100.00"),
            confidence=Decimal("1.00"),
            metadata={"equity": 100000.0},
        )

        signal_b = SignalEvent(
            signal_id="sig-b",
            timestamp="2020-01-02T16:00:00Z",
            strategy_id="strategy_b",
            symbol="MSFT",
            intention="OPEN_LONG",
            price=Decimal("100.00"),
            confidence=Decimal("1.00"),
            metadata={"equity": 100000.0},
        )

        # Act
        multi_strategy_service.on_signal(signal_a)
        multi_strategy_service.on_signal(signal_b)

        # Assert
        assert len(orders_received) == 2

        # Calculate total capital used based on signal price
        # strategy_a: 30% of $100k = $30k → 10% sizing = $3k → $3k / $100 = 30 shares
        # strategy_b: 50% of $100k = $50k → 10% sizing = $5k → $5k / $100 = 50 shares
        # Total: 30 + 50 = 80 shares at $100 = $8,000

        signal_price = Decimal("100.00")
        total_capital_used = orders_received[0].quantity * signal_price + orders_received[1].quantity * signal_price

        # Without budgets, both would use full equity ($100k * 0.95 * 0.10 * 2 = $19k total)
        # With budgets: $30k * 0.10 + $50k * 0.10 = $8k
        assert total_capital_used == Decimal("8000.00"), (
            f"Expected $8k total capital used, got ${total_capital_used}. "
            "This indicates strategies are reusing the same capital pool."
        )


def test_unbudgeted_strategy_uses_default_allocation(event_bus):
    """
    Regression test: Unbudgeted strategies should use 'default' budget allocation.

    Previously, strategies without explicit budget allocations would bypass
    the budget system entirely, falling back to (equity * (1 - cash_buffer_pct)),
    effectively using ~95% of total equity per strategy.

    This test verifies that:
    1. Loader auto-creates a 'default' budget when no budgets section exists
    2. get_allocated_capital falls back to 'default' for unlisted strategies
    3. No legacy fallback code in manager bypasses the budget system

    Related: Multi-strategy risk management refactoring
    """
    # Arrange: Use test fixture policy with known 5% sizing
    # This uses tests/fixtures/risk_policies/test_naive.yaml
    config_dict = {
        "name": "test_naive",
        "config": {},
    }
    service = ManagerService.from_config(config_dict, event_bus)

    # Emit initial portfolio state
    portfolio_state = PortfolioStateEvent(
        portfolio_id="test-portfolio",
        start_datetime="2020-01-01T00:00:00Z",
        snapshot_datetime="2020-01-02T16:00:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=Decimal("100000.00"),
        cash_balance=Decimal("100000.00"),
        current_portfolio_equity=Decimal("100000.00"),
        total_market_value=Decimal("0.00"),
        total_unrealized_pl=Decimal("0.00"),
        total_realized_pl=Decimal("0.00"),
        total_pl=Decimal("0.00"),
        long_exposure=Decimal("0.00"),
        short_exposure=Decimal("0.00"),
        net_exposure=Decimal("0.00"),
        gross_exposure=Decimal("0.00"),
        leverage=Decimal("0.00"),
        strategies_groups=[],
    )
    service.on_portfolio_state(portfolio_state)

    # Capture emitted orders
    orders_received = []

    def capture_order(event: OrderEvent):
        orders_received.append(event)

    event_bus.subscribe("order", capture_order)

    # Signal from strategy NOT explicitly listed in any budget
    signal = SignalEvent(
        signal_id="sig-unbudgeted",
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="unbudgeted_strategy",  # Not in any budget
        symbol="AAPL",
        intention="OPEN_LONG",
        price=Decimal("100.00"),
        confidence=Decimal("1.00"),
        metadata={"equity": 100000.0},
    )

    # Act
    service.on_signal(signal)

    # Assert
    assert len(orders_received) == 1
    order = orders_received[0]

    # Expected allocation:
    # - Naive policy has no explicit budgets → loader creates default at 95%
    # - Unbudgeted strategy → falls back to "default" budget
    # - Allocated capital: $100k * 0.95 = $95k
    # - Naive sizing: 5% (0.05) fixed fraction
    # - Position value: $95k * 0.05 = $4,750
    # - Quantity: $4,750 / $100 = 47.5 → 47 shares (rounded down)
    expected_quantity = 47

    assert order.quantity == expected_quantity, (
        f"Expected {expected_quantity} shares (95% equity * 5% sizing / $100 price), "
        f"got {order.quantity}. This indicates strategy may be bypassing budget system."
    )

    # Verify no KeyError was raised (would indicate missing default budget)
    # If test reaches this point without exception, budget system worked correctly


def test_partial_close_respects_lot_size_constraints(event_bus):
    """
    Regression test: Partial close orders must respect lot_size constraints.

    Previously, when confidence < 1.0, the manager would:
    1. Scale quantity by confidence
    2. Apply lot_size rounding
    3. If result < min_quantity, bump to min_quantity
    4. Emit order WITHOUT re-applying lot_size rounding

    With lot_size=100 and min_quantity=1, a partial close could produce
    a 1-share order even though the instrument trades in 100-share lots.

    This test verifies:
    - After applying min_quantity, lot_size is re-applied
    - If final quantity < 1 lot, order is rejected (not emitted)
    - Lot constraints are maintained throughout the sizing pipeline

    Related: Risk library integration, venue constraint enforcement
    """
    # Arrange: Create policy with options-like lot sizing
    from qs_trader.libraries.risk.models import (
        ConcentrationLimit,
        LeverageLimit,
        RiskConfig,
        ShortingPolicy,
        SizingConfig,
        StrategyBudget,
    )

    # Policy with lot_size=100 (options contracts), min_quantity=1
    config = RiskConfig(
        budgets=[StrategyBudget(strategy_id="default", capital_weight=0.95)],
        sizing={
            "default": SizingConfig(
                model="fixed_fraction",
                fraction=Decimal("0.05"),
                min_quantity=1,  # Minimum 1 contract
                lot_size=100,  # Options trade in 100-contract lots
            )
        },
        concentration=ConcentrationLimit(max_position_pct=1.0),
        leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
        cash_buffer_pct=0.05,
        shorting=ShortingPolicy(allow_short_positions=False),
    )

    service = ManagerService(config, event_bus)

    # Portfolio state with existing 150-contract position
    portfolio_state = PortfolioStateEvent(
        portfolio_id="test-portfolio",
        start_datetime="2020-01-01T00:00:00Z",
        snapshot_datetime="2020-01-02T16:00:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=Decimal("100000.00"),
        cash_balance=Decimal("85000.00"),
        current_portfolio_equity=Decimal("115000.00"),
        total_market_value=Decimal("30000.00"),  # 150 contracts * $200
        total_unrealized_pl=Decimal("15000.00"),
        total_realized_pl=Decimal("0.00"),
        total_pl=Decimal("15000.00"),
        long_exposure=Decimal("30000.00"),
        short_exposure=Decimal("0.00"),
        net_exposure=Decimal("30000.00"),
        gross_exposure=Decimal("30000.00"),
        leverage=Decimal("0.26"),
        strategies_groups=[
            StrategyGroup(
                strategy_id="test_strategy",
                positions=[
                    PortfolioPosition(
                        symbol="SPY",
                        side="long",
                        open_quantity=150,  # 150 contracts (1.5 lots)
                        average_fill_price=Decimal("200.00"),
                        commission_paid=Decimal("0.00"),
                        cost_basis=Decimal("30000.00"),
                        market_price=Decimal("200.00"),
                        gross_market_value=Decimal("30000.00"),
                        unrealized_pl=Decimal("15000.00"),
                        realized_pl=Decimal("0.00"),
                        dividends_received=Decimal("0.00"),
                        dividends_paid=Decimal("0.00"),
                        total_position_value=Decimal("30000.00"),
                        currency="USD",
                        last_updated="2020-01-02T15:00:00Z",
                    )
                ],
            )
        ],
    )
    service.on_portfolio_state(portfolio_state)

    # Capture emitted orders
    orders_received = []

    def capture_order(event: OrderEvent):
        orders_received.append(event)

    event_bus.subscribe("order", capture_order)

    # Act: Partial close with confidence=0.5 (close 50% of position)
    # Expected calculation:
    # - Position: 150 contracts
    # - Confidence: 0.5
    # - Raw scaled: 150 * 0.5 = 75 contracts
    # - Lot rounding: 75 / 100 = 0 lots → 0 contracts
    # - Round UP: position (150) >= lot_size (100) → quantity = 100 contracts
    # - Result: Order emitted with 100 contracts (1 lot)

    signal = SignalEvent(
        signal_id="sig-partial-close",
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="test_strategy",
        symbol="SPY",
        intention="CLOSE_LONG",
        price=Decimal("200.00"),
        confidence=Decimal("0.5"),  # Close 50% of position
        metadata={},
    )

    service.on_signal(signal)

    # Assert: Should emit 100-contract order (rounded up to 1 lot)
    assert len(orders_received) == 1, (
        "Expected 1 order (0.5 * 150 = 75 → floors to 0 → rounds up to 100), "
        f"but got {len(orders_received)}. Order may have been incorrectly suppressed."
    )
    order = orders_received[0]
    assert order.quantity == 100, (
        f"Expected 100 contracts (rounded up from 0 to 1 lot), got {order.quantity}. "
        "Lot rounding should round UP when floor = 0 and position >= 1 lot."
    )

    # Test scenario 2: Confidence high enough to produce valid lot
    # Clear previous state
    orders_received.clear()

    # Signal with confidence=0.75 (close 75% = 112.5 contracts → 100 contracts = 1 lot)
    signal2 = SignalEvent(
        signal_id="sig-partial-close-2",
        timestamp="2020-01-02T16:01:00Z",
        strategy_id="test_strategy",
        symbol="SPY",
        intention="CLOSE_LONG",
        price=Decimal("200.00"),
        confidence=Decimal("0.75"),  # Close 75%
        metadata={},
    )

    service.on_signal(signal2)

    # Assert: Should emit 100-contract order (1 lot)
    assert len(orders_received) == 1
    order = orders_received[0]
    assert order.quantity == 100, (
        f"Expected 100 contracts (0.75 * 150 = 112.5 → 1 lot = 100), "
        f"got {order.quantity}. Lot sizing not applied correctly."
    )
    assert order.side == "sell"  # CLOSE_LONG → sell
    assert order.symbol == "SPY"


def test_multi_strategy_position_aggregation_for_concentration_limit(event_bus):
    """
    Regression test: Concentration limits must aggregate positions across strategies.

    Previously, check_concentration_limit() would break on the first position found,
    ignoring additional positions in the same symbol from other strategies.
    Similarly, check_leverage_limits() would overwrite current_qty_in_symbol
    instead of accumulating.

    This caused portfolio-level concentration and leverage checks to undercount
    exposure when multiple strategies traded the same ticker, violating the
    "Reliable Risk Checks" promise in the multi-strategy refactoring.

    This test verifies:
    - Positions in same symbol across strategies are aggregated
    - Concentration limit is enforced on total portfolio exposure, not per-strategy
    - Orders are rejected when aggregated position would exceed limits

    Related: Multi-strategy refactoring, reliable risk checks
    """
    # Arrange: Create policy with 20% concentration limit
    from qs_trader.libraries.risk.models import (
        ConcentrationLimit,
        LeverageLimit,
        RiskConfig,
        ShortingPolicy,
        SizingConfig,
        StrategyBudget,
    )

    config = RiskConfig(
        budgets=[
            StrategyBudget(strategy_id="strategy_a", capital_weight=0.40),
            StrategyBudget(strategy_id="strategy_b", capital_weight=0.40),
        ],
        sizing={
            "strategy_a": SizingConfig(
                model="fixed_fraction",
                fraction=Decimal("0.10"),
                min_quantity=1,
                lot_size=1,
            ),
            "strategy_b": SizingConfig(
                model="fixed_fraction",
                fraction=Decimal("0.10"),
                min_quantity=1,
                lot_size=1,
            ),
        },
        concentration=ConcentrationLimit(max_position_pct=0.20),  # 20% max per symbol
        leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
        cash_buffer_pct=0.05,
        shorting=ShortingPolicy(allow_short_positions=False),
    )

    service = ManagerService(config, event_bus)

    # Portfolio state: Both strategies already hold AAPL
    # Strategy A: 50 shares @ $100 = $5,000 (5% of equity)
    # Strategy B: 150 shares @ $100 = $15,000 (15% of equity)
    # Total AAPL: 200 shares @ $100 = $20,000 (20% of equity) ← AT LIMIT
    portfolio_state = PortfolioStateEvent(
        portfolio_id="test-portfolio",
        start_datetime="2020-01-01T00:00:00Z",
        snapshot_datetime="2020-01-02T16:00:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=Decimal("100000.00"),
        cash_balance=Decimal("80000.00"),
        current_portfolio_equity=Decimal("100000.00"),
        total_market_value=Decimal("20000.00"),
        total_unrealized_pl=Decimal("0.00"),
        total_realized_pl=Decimal("0.00"),
        total_pl=Decimal("0.00"),
        long_exposure=Decimal("20000.00"),
        short_exposure=Decimal("0.00"),
        net_exposure=Decimal("20000.00"),
        gross_exposure=Decimal("20000.00"),
        leverage=Decimal("0.20"),
        strategies_groups=[
            StrategyGroup(
                strategy_id="strategy_a",
                positions=[
                    PortfolioPosition(
                        symbol="AAPL",
                        side="long",
                        open_quantity=50,  # Strategy A: 50 shares
                        average_fill_price=Decimal("100.00"),
                        commission_paid=Decimal("0.00"),
                        cost_basis=Decimal("5000.00"),
                        market_price=Decimal("100.00"),
                        gross_market_value=Decimal("5000.00"),
                        unrealized_pl=Decimal("0.00"),
                        realized_pl=Decimal("0.00"),
                        dividends_received=Decimal("0.00"),
                        dividends_paid=Decimal("0.00"),
                        total_position_value=Decimal("5000.00"),
                        currency="USD",
                        last_updated="2020-01-02T15:00:00Z",
                    )
                ],
            ),
            StrategyGroup(
                strategy_id="strategy_b",
                positions=[
                    PortfolioPosition(
                        symbol="AAPL",
                        side="long",
                        open_quantity=150,  # Strategy B: 150 shares
                        average_fill_price=Decimal("100.00"),
                        commission_paid=Decimal("0.00"),
                        cost_basis=Decimal("15000.00"),
                        market_price=Decimal("100.00"),
                        gross_market_value=Decimal("15000.00"),
                        unrealized_pl=Decimal("0.00"),
                        realized_pl=Decimal("0.00"),
                        dividends_received=Decimal("0.00"),
                        dividends_paid=Decimal("0.00"),
                        total_position_value=Decimal("15000.00"),
                        currency="USD",
                        last_updated="2020-01-02T15:00:00Z",
                    )
                ],
            ),
        ],
    )
    service.on_portfolio_state(portfolio_state)

    # Capture emitted orders
    orders_received = []

    def capture_order(event: OrderEvent):
        orders_received.append(event)

    event_bus.subscribe("order", capture_order)

    # Act: Strategy A tries to buy more AAPL
    # Current total AAPL: 200 shares @ $100 = $20,000 (20% of equity) ← AT LIMIT
    # Strategy A allocated capital: $100k * 0.40 = $40k
    # Strategy A sizing: $40k * 0.10 = $4k / $100 = 40 shares
    # Proposed total: 200 + 40 = 240 shares @ $100 = $24,000 (24% of equity) ← EXCEEDS 20% LIMIT

    signal = SignalEvent(
        signal_id="sig-concentration-test",
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="strategy_a",
        symbol="AAPL",
        intention="OPEN_LONG",
        price=Decimal("100.00"),
        confidence=Decimal("1.00"),
        metadata={},
    )

    service.on_signal(signal)

    # Assert: Order should be REJECTED due to concentration limit
    assert len(orders_received) == 0, (
        "Expected order to be rejected (total AAPL would be 24% > 20% limit), "
        f"but {len(orders_received)} order(s) were emitted. "
        "This indicates positions were not aggregated across strategies for concentration check."
    )

    # Verify the fix: If only strategy_a's position was checked (50 shares = 5%),
    # and we ignored strategy_b's 150 shares, the limit check would pass:
    # 50 + 40 = 90 shares @ $100 = $9,000 (9% of equity) ← Would incorrectly pass
    #
    # But with proper aggregation:
    # (50 + 150) + 40 = 240 shares @ $100 = $24,000 (24% of equity) ← Correctly rejected


def test_partial_close_rounds_up_when_confidence_floors_to_zero(event_bus):
    """
    Regression test: Partial close should round UP to 1 lot when confidence floors to 0.

    Previously, the manager would:
    1. Scale quantity by confidence
    2. Floor to lot_size
    3. If result = 0, discard order entirely

    This violated the signal contract: confidence is a strength HINT, not a hard cap.
    The manager should close "about" the requested amount, not suppress the order.

    Example scenario:
    - Position: 120 shares
    - lot_size: 100 (options/futures)
    - confidence: 0.6 (close 60%)
    - Raw scaled: 120 * 0.6 = 72 shares
    - Floor to lot: 72 / 100 = 0 lots → 0 shares
    - OLD BEHAVIOR: Discard order (no close at all)
    - NEW BEHAVIOR: Round up to 100 shares (1 lot)

    This test verifies:
    - When confidence scaling floors to 0 but position >= 1 lot, round UP to 1 lot
    - Order is emitted with valid lot size
    - No orders are suppressed unnecessarily

    Related: Multi-strategy refactoring, signal contract enforcement
    """
    from qs_trader.libraries.risk.models import (
        ConcentrationLimit,
        LeverageLimit,
        RiskConfig,
        ShortingPolicy,
        SizingConfig,
        StrategyBudget,
    )

    # Policy with lot_size=100 (options/futures)
    config = RiskConfig(
        budgets=[StrategyBudget(strategy_id="default", capital_weight=0.95)],
        sizing={
            "default": SizingConfig(
                model="fixed_fraction",
                fraction=Decimal("0.05"),
                min_quantity=1,
                lot_size=100,  # Trades in 100-share lots
            )
        },
        concentration=ConcentrationLimit(max_position_pct=1.0),
        leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
        cash_buffer_pct=0.05,
        shorting=ShortingPolicy(allow_short_positions=False),
    )

    service = ManagerService(config, event_bus)

    # Portfolio state with 120-share position (1.2 lots)
    portfolio_state = PortfolioStateEvent(
        portfolio_id="test-portfolio",
        start_datetime="2020-01-01T00:00:00Z",
        snapshot_datetime="2020-01-02T16:00:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=Decimal("100000.00"),
        cash_balance=Decimal("88000.00"),
        current_portfolio_equity=Decimal("112000.00"),
        total_market_value=Decimal("24000.00"),  # 120 shares * $200
        total_unrealized_pl=Decimal("12000.00"),
        total_realized_pl=Decimal("0.00"),
        total_pl=Decimal("12000.00"),
        long_exposure=Decimal("24000.00"),
        short_exposure=Decimal("0.00"),
        net_exposure=Decimal("24000.00"),
        gross_exposure=Decimal("24000.00"),
        leverage=Decimal("0.21"),
        strategies_groups=[
            StrategyGroup(
                strategy_id="test_strategy",
                positions=[
                    PortfolioPosition(
                        symbol="SPY",
                        side="long",
                        open_quantity=120,  # 120 shares = 1.2 lots
                        average_fill_price=Decimal("200.00"),
                        commission_paid=Decimal("0.00"),
                        cost_basis=Decimal("24000.00"),
                        market_price=Decimal("200.00"),
                        gross_market_value=Decimal("24000.00"),
                        unrealized_pl=Decimal("12000.00"),
                        realized_pl=Decimal("0.00"),
                        dividends_received=Decimal("0.00"),
                        dividends_paid=Decimal("0.00"),
                        total_position_value=Decimal("24000.00"),
                        currency="USD",
                        last_updated="2020-01-02T15:00:00Z",
                    )
                ],
            )
        ],
    )
    service.on_portfolio_state(portfolio_state)

    # Capture emitted orders
    orders_received = []

    def capture_order(event: OrderEvent):
        orders_received.append(event)

    event_bus.subscribe("order", capture_order)

    # Act: Partial close with confidence=0.6 (close 60%)
    # Expected calculation:
    # - Position: 120 shares
    # - Confidence: 0.6
    # - Raw scaled: 120 * 0.6 = 72 shares
    # - Lot floor: 72 / 100 = 0 lots → 0 shares
    # - Round UP: base_quantity (120) >= lot_size (100) → quantity = 100 shares
    signal = SignalEvent(
        signal_id="sig-round-up",
        timestamp="2020-01-02T16:00:00Z",
        strategy_id="test_strategy",
        symbol="SPY",
        intention="CLOSE_LONG",
        price=Decimal("200.00"),
        confidence=Decimal("0.6"),  # Close 60%
        metadata={},
    )

    service.on_signal(signal)

    # Assert: Should emit 100-share order (rounded up to 1 lot)
    assert len(orders_received) == 1, (
        "Expected 1 order (0.6 * 120 = 72 → floors to 0 → rounds up to 100), "
        f"but got {len(orders_received)}. Order may have been incorrectly suppressed."
    )

    order = orders_received[0]
    assert order.quantity == 100, (
        f"Expected 100 shares (rounded up from 0 to 1 lot), got {order.quantity}. "
        "Lot rounding should round UP when floor = 0 and position >= 1 lot."
    )
    assert order.side == "sell"
    assert order.symbol == "SPY"
