"""
Integration test: Strategy → Signal → Manager → Order flow.

Tests the complete event flow from strategy signal emission to order creation:
1. Strategy emits SignalEvent via context
2. ManagerService subscribes to SignalEvent
3. ManagerService processes signal (sizing, limits)
4. ManagerService emits OrderEvent

This verifies Phase 3 + Phase 4 integration is working correctly.
"""

from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import OrderEvent, PortfolioStateEvent, PriceBarEvent
from qs_trader.services.manager.service import ManagerService
from qs_trader.services.strategy.context import Context
from qs_trader.services.strategy.models import SignalIntention


class TestSignalToOrderFlow:
    """Test complete signal → order event flow."""

    @pytest.fixture
    def event_bus(self):
        """Create event bus for testing."""
        return EventBus()

    @pytest.fixture
    def manager_service(self, event_bus):
        """Create ManagerService with naive policy and cached portfolio state."""
        config_dict = {
            "name": "naive",  # Uses default naive policy from risk library
        }
        service = ManagerService.from_config(config_dict=config_dict, event_bus=event_bus)

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

    @pytest.fixture
    def manager_service_with_shorts(self, event_bus):
        """Create ManagerService with shorting enabled for testing short signals."""
        config_dict = {
            "name": "test_naive_short",  # Uses test fixture with shorting enabled
        }
        service = ManagerService.from_config(config_dict=config_dict, event_bus=event_bus)

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

    @pytest.fixture
    def context(self, event_bus):
        """Create strategy context for emitting signals."""
        return Context(strategy_id="test_strategy", event_bus=event_bus, max_bars=100)

    def test_signal_to_order_flow(self, context, manager_service, event_bus):
        """Test strategy signal triggers manager to create order."""
        # Arrange: Set up order capture
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # Arrange: Feed a bar to context so get_price() works
        bar = PriceBarEvent(
            symbol="AAPL",
            timestamp="2020-01-02T16:00:00Z",
            interval="1d",
            open=Decimal("150.00"),
            high=Decimal("152.00"),
            low=Decimal("149.00"),
            close=Decimal("151.00"),
            volume=1000000,
            source="test",
        )
        context.cache_bar(bar)

        # Act: Strategy emits signal via context
        context.emit_signal(
            timestamp="2020-01-02T16:00:00Z",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            confidence=Decimal("0.80"),
            price=Decimal("151.00"),
            reason="Test signal",
            metadata={"equity": 100000.0},  # Phase 3 MVP: equity in metadata
        )

        # Assert: Manager should have created an order
        assert len(orders_received) == 1, "Expected 1 order event"

        order = orders_received[0]
        assert order.symbol == "AAPL"
        assert order.side == "buy"  # OPEN_LONG → buy
        assert order.quantity > 0
        assert order.order_type == "market"
        assert order.source_strategy_id == "test_strategy"

        # Verify audit trail
        assert hasattr(order, "intent_id"), "Order should have intent_id"
        assert hasattr(order, "idempotency_key"), "Order should have idempotency_key"
        assert order.intent_id is not None
        assert order.idempotency_key is not None

    def test_multiple_signals_create_multiple_orders(self, context, manager_service_with_shorts, event_bus):
        """Test multiple signals create multiple orders (no batching)."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # Feed bars for both symbols
        for symbol in ["AAPL", "MSFT"]:
            bar = PriceBarEvent(
                symbol=symbol,
                timestamp="2020-01-02T16:00:00Z",
                interval="1d",
                open=Decimal("150.00"),
                high=Decimal("152.00"),
                low=Decimal("149.00"),
                close=Decimal("151.00"),
                volume=1000000,
                source="test",
            )
            context.cache_bar(bar)

        # Act: Emit signals for two different symbols
        context.emit_signal(
            timestamp="2020-01-02T16:00:00Z",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            confidence=Decimal("0.80"),
            price=Decimal("151.00"),
            reason="Buy AAPL",
            metadata={"equity": 100000.0},
        )

        context.emit_signal(
            timestamp="2020-01-02T16:00:00Z",
            symbol="MSFT",
            intention=SignalIntention.OPEN_SHORT,
            confidence=Decimal("0.75"),
            price=Decimal("151.00"),
            reason="Short MSFT",
            metadata={"equity": 100000.0},
        )

        # Assert: Should have 2 orders (immediate processing, no batching)
        assert len(orders_received) == 2, "Expected 2 orders (one per signal)"

        # Verify first order (OPEN_LONG)
        order1 = orders_received[0]
        assert order1.symbol == "AAPL"
        assert order1.side == "buy"

        # Verify second order (OPEN_SHORT)
        order2 = orders_received[1]
        assert order2.symbol == "MSFT"
        assert order2.side == "sell"

    def test_signal_intention_mapping(self, context, manager_service_with_shorts, event_bus):
        """Test all signal intentions map to correct order sides."""
        # Arrange
        orders_received = []

        def capture_order(event: OrderEvent):
            orders_received.append(event)

        event_bus.subscribe("order", capture_order)

        # Feed bar
        bar = PriceBarEvent(
            symbol="AAPL",
            timestamp="2020-01-02T16:00:00Z",
            interval="1d",
            open=Decimal("150.00"),
            high=Decimal("152.00"),
            low=Decimal("149.00"),
            close=Decimal("151.00"),
            volume=1000000,
            source="test",
        )
        context.cache_bar(bar)

        # Create portfolio state with positions for close signals
        from qs_trader.events.events import PortfolioPosition, PortfolioStateEvent, StrategyGroup

        portfolio_state = PortfolioStateEvent(
            portfolio_id="test_portfolio",
            start_datetime="2020-01-01T00:00:00Z",
            snapshot_datetime="2020-01-02T16:00:00Z",
            reporting_currency="USD",
            initial_portfolio_equity=Decimal("100000"),
            cash_balance=Decimal("75000"),
            current_portfolio_equity=Decimal("120000"),
            total_market_value=Decimal("45000"),
            total_unrealized_pl=Decimal("20000"),
            total_realized_pl=Decimal("0"),
            total_pl=Decimal("20000"),
            long_exposure=Decimal("35000"),
            short_exposure=Decimal("10000"),
            net_exposure=Decimal("25000"),
            gross_exposure=Decimal("45000"),
            leverage=Decimal("0.375"),
            strategies_groups=[
                StrategyGroup(
                    strategy_id="test_strategy",
                    positions=[
                        PortfolioPosition(
                            symbol="AAPL",
                            side="long",
                            open_quantity=100,  # Long position for CLOSE_LONG
                            average_fill_price=Decimal("150.00"),
                            commission_paid=Decimal("10.00"),
                            cost_basis=Decimal("15010.00"),
                            market_price=Decimal("151.00"),
                            gross_market_value=Decimal("15100.00"),
                            unrealized_pl=Decimal("90.00"),
                            realized_pl=Decimal("0"),
                            dividends_received=Decimal("0"),
                            dividends_paid=Decimal("0"),
                            total_position_value=Decimal("15100.00"),
                            currency="USD",
                            last_updated="2020-01-02T16:00:00Z",
                        ),
                        PortfolioPosition(
                            symbol="TSLA",
                            side="short",
                            open_quantity=-50,  # Short position for CLOSE_SHORT
                            average_fill_price=Decimal("200.00"),
                            commission_paid=Decimal("10.00"),
                            cost_basis=Decimal("10010.00"),  # Positive for shorts (absolute value + commission)
                            market_price=Decimal("200.00"),
                            gross_market_value=Decimal("-10000.00"),
                            unrealized_pl=Decimal("-10.00"),
                            realized_pl=Decimal("0"),
                            dividends_received=Decimal("0"),
                            dividends_paid=Decimal("0"),
                            total_position_value=Decimal("-10000.00"),
                            currency="USD",
                            last_updated="2020-01-02T16:00:00Z",
                        ),
                    ],
                )
            ],
        )
        manager_service_with_shorts.on_portfolio_state(portfolio_state)

        # Test cases: (symbol, intention, expected_side)
        # Use flat symbols for OPEN_* signals so this test exercises side mapping
        # instead of the duplicate-open suppression rules.
        test_cases = [
            ("MSFT", SignalIntention.OPEN_LONG, "buy"),
            ("AAPL", SignalIntention.CLOSE_LONG, "sell"),
            ("GOOG", SignalIntention.OPEN_SHORT, "sell"),
            ("TSLA", SignalIntention.CLOSE_SHORT, "buy"),
        ]

        # Act: Emit signal for each intention
        for symbol, intention, expected_side in test_cases:
            context.emit_signal(
                timestamp="2020-01-02T16:00:00Z",
                symbol=symbol,
                intention=intention,
                confidence=Decimal("0.80"),
                price=Decimal("151.00"),
                reason=f"Test {intention}",
                metadata={"equity": 100000.0},
            )

        # Assert: Verify each order has correct side
        assert len(orders_received) == 4, "Expected 4 orders (one per intention)"

        for idx, (symbol, intention, expected_side) in enumerate(test_cases):
            order = orders_received[idx]
            assert order.side == expected_side, (
                f"Intention {intention} should map to side={expected_side}, got {order.side}"
            )
