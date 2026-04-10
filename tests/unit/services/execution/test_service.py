"""Unit tests for ExecutionService."""

from datetime import datetime
from decimal import Decimal

import pytest

from qs_trader.events.events import PriceBarEvent
from qs_trader.services.data.models import Bar
from qs_trader.services.execution.config import CommissionConfig, ExecutionConfig, SlippageConfig
from qs_trader.services.execution.models import Order, OrderSide, OrderState, OrderType
from qs_trader.services.execution.service import ExecutionService


def make_config(market_order_queue_bars: int = 1, slippage_bps: Decimal = Decimal("5")) -> ExecutionConfig:
    """Helper to create ExecutionConfig with fixed BPS slippage."""
    return ExecutionConfig(
        market_order_queue_bars=market_order_queue_bars,
        slippage=SlippageConfig(model="fixed_bps", params={"bps": slippage_bps}),
    )


class TestExecutionServiceBasics:
    """Test basic ExecutionService operations."""

    def test_create_service(self) -> None:
        """Test creating execution service."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        assert service.config == config
        assert service.fill_policy is not None
        assert service.commission_calculator is not None

    def test_submit_order(self) -> None:
        """Test submitting an order."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        order_id = service.submit_order(order)

        assert order_id == order.order_id
        assert order.state == OrderState.SUBMITTED
        assert service.get_order(order_id) == order

    def test_submit_duplicate_order_raises(self) -> None:
        """Test submitting duplicate order raises."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        service.submit_order(order)

        with pytest.raises(ValueError, match="Order ID already exists"):
            service.submit_order(order)

    def test_get_order_not_found(self) -> None:
        """Test get_order returns None for unknown ID."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        assert service.get_order("unknown_id") is None

    def test_get_pending_orders_empty(self) -> None:
        """Test get_pending_orders returns empty list initially."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        assert service.get_pending_orders() == []
        assert service.get_pending_orders(symbol="AAPL") == []

    def test_get_pending_orders_after_submit(self) -> None:
        """Test get_pending_orders returns submitted orders."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        order1 = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order2 = Order(
            symbol="MSFT",
            side=OrderSide.SELL,
            quantity=Decimal("200"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("300"),
            created_at=datetime.now(),
        )

        service.submit_order(order1)
        service.submit_order(order2)

        all_pending = service.get_pending_orders()
        assert len(all_pending) == 2

        aapl_pending = service.get_pending_orders(symbol="AAPL")
        assert len(aapl_pending) == 1
        assert aapl_pending[0].symbol == "AAPL"

    def test_cancel_order(self) -> None:
        """Test cancelling an order."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        order_id = service.submit_order(order)
        assert service.cancel_order(order_id) is True

        retrieved_order = service.get_order(order_id)
        assert retrieved_order is not None
        assert retrieved_order.state == OrderState.CANCELLED

        # Should not be in pending anymore
        pending = service.get_pending_orders()
        assert order not in pending

    def test_cancel_nonexistent_order(self) -> None:
        """Test cancelling non-existent order returns False."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        assert service.cancel_order("unknown_id") is False

    def test_cancel_already_filled_order(self) -> None:
        """Test cancelling filled order returns False."""
        config = ExecutionConfig(market_order_queue_bars=1)
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1  # Pre-queue

        order_id = service.submit_order(order)

        # Process bar to fill order
        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)
        fills = service.on_bar(bar)

        assert len(fills) == 1

        # Try to cancel filled order
        assert service.cancel_order(order_id) is False


class TestExecutionServiceFills:
    """Test fill generation."""

    def test_on_bar_event_uses_adjusted_open_for_split_adjusted_mode(self) -> None:
        """split_adjusted fills should use the adjusted ClickHouse open when available."""
        config = make_config(market_order_queue_bars=1, slippage_bps=Decimal("5"))
        service = ExecutionService(config, adjustment_mode="split_adjusted")

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1
        service.submit_order(order)

        event = PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-02T16:00:00Z",
            open=Decimal("150.00"),
            high=Decimal("151.00"),
            low=Decimal("149.00"),
            close=Decimal("150.50"),
            open_adj=Decimal("15.00"),
            high_adj=Decimal("15.10"),
            low_adj=Decimal("14.90"),
            close_adj=Decimal("15.05"),
            volume=1000000,
            source="test",
            interval="1d",
        )

        service.on_bar_event(event)

        assert order.state == OrderState.FILLED
        assert order.filled_quantity == Decimal("100")
        assert order.avg_fill_price == Decimal("15.0") * Decimal("1.0005")

    def test_market_order_queued_no_fill(self) -> None:
        """Test market order queued doesn't fill immediately."""
        config = ExecutionConfig(market_order_queue_bars=1)
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        service.submit_order(order)

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        fills = service.on_bar(bar)

        assert len(fills) == 0
        assert order.bars_queued == 1

    def test_market_order_fills_after_queue(self) -> None:
        """Test market order fills after queueing."""
        config = make_config(market_order_queue_bars=1, slippage_bps=Decimal("5"))
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1  # Pre-queue

        service.submit_order(order)

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        fills = service.on_bar(bar)

        assert len(fills) == 1
        fill = fills[0]

        assert fill.symbol == "AAPL"
        assert fill.side == "buy"
        assert fill.quantity == Decimal("100")
        assert fill.price == Decimal("150.0") * Decimal("1.0005")  # open + slippage
        assert fill.commission >= Decimal("0")  # Has commission

        # Order should be filled
        assert order.state == OrderState.FILLED
        assert order.filled_quantity == Decimal("100")

    def test_limit_order_fills_when_touched(self) -> None:
        """Test limit order fills when price touched."""
        config = ExecutionConfig()
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("150.0"),
            created_at=datetime.now(),
        )

        service.submit_order(order)

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.5,
            high=151.0,
            low=149.0,  # Touches limit
            close=150.5,
            volume=1000000,
        )

        fills = service.on_bar(bar)

        assert len(fills) == 1
        fill = fills[0]

        assert fill.symbol == "AAPL"
        assert fill.side == "buy"
        assert fill.quantity == Decimal("100")
        assert fill.price == Decimal("150.0")  # Fills at limit

    def test_stop_order_triggers_and_fills(self) -> None:
        """Test stop order triggers and fills."""
        config = make_config(slippage_bps=Decimal("5"))
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.STOP,
            stop_price=Decimal("150.0"),
            created_at=datetime.now(),
        )

        service.submit_order(order)

        bar = Bar(
            trade_datetime=datetime.now(),
            open=149.5,
            high=151.0,  # Triggers stop
            low=149.0,
            close=149.5,
            volume=1000000,
        )

        fills = service.on_bar(bar)

        assert len(fills) == 1
        fill = fills[0]

        assert fill.symbol == "AAPL"
        assert fill.side == "buy"
        # Price should be max(stop, close) + slippage = 150.0 + slippage
        expected_price = Decimal("150.0") * Decimal("1.0005")
        assert fill.price == expected_price

    def test_moc_order_fills_at_close(self) -> None:
        """Test MOC order fills at close."""
        config = make_config(slippage_bps=Decimal("5"))
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET_ON_CLOSE,
            created_at=datetime.now(),
        )

        service.submit_order(order)

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        fills = service.on_bar(bar)

        assert len(fills) == 1
        fill = fills[0]

        assert fill.symbol == "AAPL"
        assert fill.side == "buy"
        # Price should be close + slippage
        expected_price = Decimal("150.5") * Decimal("1.0005")
        assert fill.price == expected_price

    def test_partial_fill_due_to_volume(self) -> None:
        """Test partial fill due to volume participation limit."""
        config = ExecutionConfig(market_order_queue_bars=1, max_participation_rate=Decimal("0.10"))
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("10000"),  # Want 10,000
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1  # Pre-queue

        service.submit_order(order)

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.0,
            high=151.0,
            low=149.0,
            close=150.5,
            volume=50000,  # Max fill = 5000
        )

        fills = service.on_bar(bar)

        assert len(fills) == 1
        fill = fills[0]

        assert fill.quantity == Decimal("5000")  # Only 50% filled

        # Order should be partially filled
        assert order.state == OrderState.PARTIAL
        assert order.filled_quantity == Decimal("5000")
        assert order.remaining_quantity == Decimal("5000")

        # Should still be pending
        pending = service.get_pending_orders(symbol="AAPL")
        assert order in pending

    def test_multiple_partial_fills(self) -> None:
        """Test order filled across multiple bars."""
        config = ExecutionConfig(market_order_queue_bars=1, max_participation_rate=Decimal("0.10"))
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("10000"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1

        service.submit_order(order)

        # Bar 1: Fill 5000
        bar1 = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=50000)
        fills1 = service.on_bar(bar1)
        assert len(fills1) == 1
        assert fills1[0].quantity == Decimal("5000")

        # Bar 2: Fill remaining 5000
        bar2 = Bar(trade_datetime=datetime.now(), open=150.5, high=151.5, low=150.0, close=151.0, volume=50000)
        fills2 = service.on_bar(bar2)
        assert len(fills2) == 1
        assert fills2[0].quantity == Decimal("5000")

        # Order should now be filled
        assert order.state == OrderState.FILLED
        assert order.filled_quantity == Decimal("10000")

        # Should not be pending anymore
        pending = service.get_pending_orders(symbol="AAPL")
        assert order not in pending

    def test_commission_calculated(self) -> None:
        """Test commission is calculated and included in fill."""
        config = ExecutionConfig(
            market_order_queue_bars=1,
            commission=CommissionConfig(per_share=Decimal("0.01"), minimum=Decimal("5.00")),
        )
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1

        service.submit_order(order)

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)
        fills = service.on_bar(bar)

        assert len(fills) == 1
        fill = fills[0]

        # Commission = max(100 * 0.01, 5.00) = max(1.00, 5.00) = 5.00
        assert fill.commission == Decimal("5.00")

    def test_multiple_orders_same_symbol(self) -> None:
        """Test multiple orders for same symbol."""
        config = ExecutionConfig(market_order_queue_bars=1)
        service = ExecutionService(config)

        order1 = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order1.bars_queued = 1

        order2 = Order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("50"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order2.bars_queued = 1

        service.submit_order(order1)
        service.submit_order(order2)

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)
        fills = service.on_bar(bar)

        # Should have 2 fills
        assert len(fills) == 2

        # Check both filled
        buy_fill = next(f for f in fills if f.side == "buy")
        sell_fill = next(f for f in fills if f.side == "sell")

        assert buy_fill.quantity == Decimal("100")
        assert sell_fill.quantity == Decimal("50")

    def test_get_filled_orders(self) -> None:
        """Test get_filled_orders returns completed orders."""
        config = ExecutionConfig(market_order_queue_bars=1)
        service = ExecutionService(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1

        service.submit_order(order)

        # Initially no filled orders
        assert len(service.get_filled_orders()) == 0

        # Fill order
        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)
        service.on_bar(bar)

        # Now should have 1 filled order
        filled = service.get_filled_orders()
        assert len(filled) == 1
        assert filled[0].order_id == order.order_id

        # Filter by symbol
        aapl_filled = service.get_filled_orders(symbol="AAPL")
        assert len(aapl_filled) == 1

        msft_filled = service.get_filled_orders(symbol="MSFT")
        assert len(msft_filled) == 0
