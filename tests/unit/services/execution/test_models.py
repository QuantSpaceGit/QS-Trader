"""Unit tests for execution models."""

from datetime import datetime
from decimal import Decimal

import pytest

from qs_trader.services.execution.models import Fill, FillDecision, Order, OrderSide, OrderState, OrderType, TimeInForce


class TestOrderState:
    """Test OrderState enum."""

    def test_all_states_defined(self) -> None:
        """Test all expected states are defined."""
        assert OrderState.PENDING.value == "pending"
        assert OrderState.SUBMITTED.value == "submitted"
        assert OrderState.PARTIAL.value == "partial"
        assert OrderState.FILLED.value == "filled"
        assert OrderState.CANCELLED.value == "cancelled"
        assert OrderState.REJECTED.value == "rejected"
        assert OrderState.EXPIRED.value == "expired"


class TestOrderSide:
    """Test OrderSide enum."""

    def test_buy_sell_defined(self) -> None:
        """Test buy and sell sides defined."""
        assert OrderSide.BUY.value == "buy"
        assert OrderSide.SELL.value == "sell"


class TestOrderType:
    """Test OrderType enum."""

    def test_all_types_defined(self) -> None:
        """Test all order types defined."""
        assert OrderType.MARKET.value == "market"
        assert OrderType.LIMIT.value == "limit"
        assert OrderType.STOP.value == "stop"
        assert OrderType.MARKET_ON_CLOSE.value == "moc"


class TestTimeInForce:
    """Test TimeInForce enum."""

    def test_all_types_defined(self) -> None:
        """Test all time-in-force types defined."""
        assert TimeInForce.DAY.value == "day"
        assert TimeInForce.GTC.value == "gtc"
        assert TimeInForce.IOC.value == "ioc"
        assert TimeInForce.FOK.value == "fok"


class TestOrder:
    """Test Order model."""

    def test_create_market_order(self) -> None:
        """Test creating a market order."""
        ts = datetime(2020, 1, 2, 9, 30)
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts,
        )

        assert order.symbol == "AAPL"
        assert order.side == OrderSide.BUY
        assert order.quantity == Decimal("100")
        assert order.order_type == OrderType.MARKET
        assert order.state == OrderState.PENDING
        assert order.created_at == ts
        assert order.last_updated == ts
        assert order.filled_quantity == Decimal("0")
        assert order.avg_fill_price == Decimal("0")
        assert order.time_in_force == TimeInForce.DAY
        assert len(order.order_id) > 0  # UUID generated

    def test_create_limit_order(self) -> None:
        """Test creating a limit order."""
        ts = datetime(2020, 1, 2, 9, 30)
        order = Order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("150.00"),
            created_at=ts,
        )

        assert order.order_type == OrderType.LIMIT
        assert order.limit_price == Decimal("150.00")

    def test_create_stop_order(self) -> None:
        """Test creating a stop order."""
        ts = datetime(2020, 1, 2, 9, 30)
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.STOP,
            stop_price=Decimal("151.00"),
            created_at=ts,
        )

        assert order.order_type == OrderType.STOP
        assert order.stop_price == Decimal("151.00")

    def test_create_moc_order(self) -> None:
        """Test creating a market-on-close order."""
        ts = datetime(2020, 1, 2, 9, 30)
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET_ON_CLOSE,
            created_at=ts,
        )

        assert order.order_type == OrderType.MARKET_ON_CLOSE

    def test_validation_negative_quantity(self) -> None:
        """Test validation rejects negative quantity."""
        with pytest.raises(ValueError, match="quantity must be positive"):
            Order(
                symbol="AAPL",
                side=OrderSide.BUY,
                quantity=Decimal("-100"),
                order_type=OrderType.MARKET,
                created_at=datetime.now(),
            )

    def test_validation_zero_quantity(self) -> None:
        """Test validation rejects zero quantity."""
        with pytest.raises(ValueError, match="quantity must be positive"):
            Order(
                symbol="AAPL",
                side=OrderSide.BUY,
                quantity=Decimal("0"),
                order_type=OrderType.MARKET,
                created_at=datetime.now(),
            )

    def test_validation_limit_requires_price(self) -> None:
        """Test limit orders require limit_price."""
        with pytest.raises(ValueError, match="Limit orders require limit_price"):
            Order(
                symbol="AAPL",
                side=OrderSide.BUY,
                quantity=Decimal("100"),
                order_type=OrderType.LIMIT,
                created_at=datetime.now(),
            )

    def test_validation_stop_requires_price(self) -> None:
        """Test stop orders require stop_price."""
        with pytest.raises(ValueError, match="Stop orders require stop_price"):
            Order(
                symbol="AAPL",
                side=OrderSide.BUY,
                quantity=Decimal("100"),
                order_type=OrderType.STOP,
                created_at=datetime.now(),
            )

    def test_validation_negative_limit_price(self) -> None:
        """Test validation rejects negative limit price."""
        with pytest.raises(ValueError, match="Limit price must be positive"):
            Order(
                symbol="AAPL",
                side=OrderSide.BUY,
                quantity=Decimal("100"),
                order_type=OrderType.LIMIT,
                limit_price=Decimal("-150"),
                created_at=datetime.now(),
            )

    def test_validation_negative_stop_price(self) -> None:
        """Test validation rejects negative stop price."""
        with pytest.raises(ValueError, match="Stop price must be positive"):
            Order(
                symbol="AAPL",
                side=OrderSide.BUY,
                quantity=Decimal("100"),
                order_type=OrderType.STOP,
                stop_price=Decimal("-150"),
                created_at=datetime.now(),
            )

    def test_remaining_quantity(self) -> None:
        """Test remaining_quantity property."""
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        assert order.remaining_quantity == Decimal("100")

        # Partially fill
        order.update_fill(Decimal("30"), Decimal("150"), datetime.now())
        assert order.remaining_quantity == Decimal("70")

        # Fill more
        order.update_fill(Decimal("70"), Decimal("150"), datetime.now())
        assert order.remaining_quantity == Decimal("0")

    def test_is_complete(self) -> None:
        """Test is_complete property."""
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        assert not order.is_complete

        # Fill completely
        order.update_fill(Decimal("100"), Decimal("150"), datetime.now())
        assert order.state == OrderState.FILLED
        assert order.is_complete

    def test_is_complete_cancelled(self) -> None:
        """Test is_complete returns True for cancelled orders."""
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        order.cancel(datetime.now())
        assert order.is_complete

    def test_is_active(self) -> None:
        """Test is_active property."""
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        assert order.is_active

        order.submit(datetime.now())
        assert order.is_active

        order.update_fill(Decimal("50"), Decimal("150"), datetime.now())
        assert order.is_active

        order.update_fill(Decimal("50"), Decimal("150"), datetime.now())
        assert not order.is_active

    def test_update_fill_first_fill(self) -> None:
        """Test update_fill on first fill."""
        ts1 = datetime(2020, 1, 2, 9, 30)
        ts2 = datetime(2020, 1, 2, 9, 31)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts1,
        )

        order.update_fill(Decimal("100"), Decimal("150.00"), ts2)

        assert order.filled_quantity == Decimal("100")
        assert order.avg_fill_price == Decimal("150.00")
        assert order.state == OrderState.FILLED
        assert order.last_updated == ts2

    def test_update_fill_partial(self) -> None:
        """Test update_fill with partial fill."""
        ts1 = datetime(2020, 1, 2, 9, 30)
        ts2 = datetime(2020, 1, 2, 9, 31)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts1,
        )

        order.update_fill(Decimal("30"), Decimal("150.00"), ts2)

        assert order.filled_quantity == Decimal("30")
        assert order.avg_fill_price == Decimal("150.00")
        assert order.state == OrderState.PARTIAL
        assert order.remaining_quantity == Decimal("70")

    def test_update_fill_weighted_average(self) -> None:
        """Test update_fill calculates weighted average price."""
        ts = datetime(2020, 1, 2, 9, 30)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts,
        )

        # First fill: 30 @ 150
        order.update_fill(Decimal("30"), Decimal("150"), ts)
        assert order.avg_fill_price == Decimal("150")

        # Second fill: 70 @ 151
        # Weighted avg = (30*150 + 70*151) / 100 = (4500 + 10570) / 100 = 150.7
        order.update_fill(Decimal("70"), Decimal("151"), ts)
        assert order.avg_fill_price == Decimal("150.7")

    def test_update_fill_exceeds_quantity(self) -> None:
        """Test update_fill rejects fill exceeding remaining quantity."""
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        with pytest.raises(ValueError, match="exceeds remaining"):
            order.update_fill(Decimal("101"), Decimal("150"), datetime.now())

    def test_update_fill_already_complete(self) -> None:
        """Test update_fill rejects fill on completed order."""
        ts = datetime.now()
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts,
        )

        order.update_fill(Decimal("100"), Decimal("150"), ts)

        with pytest.raises(ValueError, match="Cannot fill order in state"):
            order.update_fill(Decimal("10"), Decimal("150"), ts)

    def test_update_fill_negative_quantity(self) -> None:
        """Test update_fill rejects negative quantity."""
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        with pytest.raises(ValueError, match="Fill quantity must be positive"):
            order.update_fill(Decimal("-10"), Decimal("150"), datetime.now())

    def test_update_fill_negative_price(self) -> None:
        """Test update_fill rejects negative price."""
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        with pytest.raises(ValueError, match="Fill price must be positive"):
            order.update_fill(Decimal("10"), Decimal("-150"), datetime.now())

    def test_cancel(self) -> None:
        """Test cancel method."""
        ts1 = datetime(2020, 1, 2, 9, 30)
        ts2 = datetime(2020, 1, 2, 9, 31)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts1,
        )

        order.cancel(ts2)

        assert order.state == OrderState.CANCELLED
        assert order.last_updated == ts2

    def test_cancel_filled_order(self) -> None:
        """Test cancel rejects filled order."""
        ts = datetime.now()
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts,
        )

        order.update_fill(Decimal("100"), Decimal("150"), ts)

        with pytest.raises(ValueError, match="Cannot cancel filled order"):
            order.cancel(ts)

    def test_reject(self) -> None:
        """Test reject method."""
        ts1 = datetime(2020, 1, 2, 9, 30)
        ts2 = datetime(2020, 1, 2, 9, 31)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts1,
        )

        order.reject(ts2)

        assert order.state == OrderState.REJECTED
        assert order.last_updated == ts2

    def test_expire(self) -> None:
        """Test expire method."""
        ts1 = datetime(2020, 1, 2, 9, 30)
        ts2 = datetime(2020, 1, 2, 16, 0)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("150"),
            created_at=ts1,
        )

        order.expire(ts2)

        assert order.state == OrderState.EXPIRED
        assert order.last_updated == ts2

    def test_expire_filled_order(self) -> None:
        """Test expire rejects filled order."""
        ts = datetime.now()
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts,
        )

        order.update_fill(Decimal("100"), Decimal("150"), ts)

        with pytest.raises(ValueError, match="Cannot expire filled order"):
            order.expire(ts)

    def test_submit(self) -> None:
        """Test submit method."""
        ts1 = datetime(2020, 1, 2, 9, 30)
        ts2 = datetime(2020, 1, 2, 9, 30, 1)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts1,
        )

        order.submit(ts2)

        assert order.state == OrderState.SUBMITTED
        assert order.last_updated == ts2

    def test_submit_not_pending(self) -> None:
        """Test submit rejects non-pending order."""
        ts = datetime.now()
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=ts,
        )

        order.submit(ts)

        with pytest.raises(ValueError, match="Cannot submit order in state"):
            order.submit(ts)


class TestFill:
    """Test Fill model."""

    def test_create_fill(self) -> None:
        """Test creating a fill."""
        ts = datetime(2020, 1, 2, 9, 30)
        fill = Fill(
            order_id="order_123",
            timestamp=ts,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("1.00"),
        )

        assert fill.order_id == "order_123"
        assert fill.timestamp == ts
        assert fill.symbol == "AAPL"
        assert fill.side == "buy"
        assert fill.quantity == Decimal("100")
        assert fill.price == Decimal("150.00")
        assert fill.commission == Decimal("1.00")
        assert len(fill.fill_id) > 0  # UUID generated

    def test_fill_immutable(self) -> None:
        """Test fill is immutable."""
        fill = Fill(
            order_id="order_123",
            timestamp=datetime.now(),
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
        )

        with pytest.raises(AttributeError):
            fill.price = Decimal("151.00")  # type: ignore

    def test_validation_negative_quantity(self) -> None:
        """Test validation rejects negative quantity."""
        with pytest.raises(ValueError, match="quantity must be positive"):
            Fill(
                order_id="order_123",
                timestamp=datetime.now(),
                symbol="AAPL",
                side="buy",
                quantity=Decimal("-100"),
                price=Decimal("150.00"),
            )

    def test_validation_negative_price(self) -> None:
        """Test validation rejects negative price."""
        with pytest.raises(ValueError, match="price must be positive"):
            Fill(
                order_id="order_123",
                timestamp=datetime.now(),
                symbol="AAPL",
                side="buy",
                quantity=Decimal("100"),
                price=Decimal("-150.00"),
            )

    def test_validation_negative_commission(self) -> None:
        """Test validation rejects negative commission."""
        with pytest.raises(ValueError, match="Commission cannot be negative"):
            Fill(
                order_id="order_123",
                timestamp=datetime.now(),
                symbol="AAPL",
                side="buy",
                quantity=Decimal("100"),
                price=Decimal("150.00"),
                commission=Decimal("-1.00"),
            )

    def test_validation_invalid_side(self) -> None:
        """Test validation rejects invalid side."""
        with pytest.raises(ValueError, match="must be 'buy' or 'sell'"):
            Fill(
                order_id="order_123",
                timestamp=datetime.now(),
                symbol="AAPL",
                side="invalid",  # type: ignore
                quantity=Decimal("100"),
                price=Decimal("150.00"),
            )

    def test_gross_value(self) -> None:
        """Test gross_value property."""
        fill = Fill(
            order_id="order_123",
            timestamp=datetime.now(),
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("1.00"),
        )

        assert fill.gross_value == Decimal("15000.00")

    def test_net_value(self) -> None:
        """Test net_value property."""
        fill = Fill(
            order_id="order_123",
            timestamp=datetime.now(),
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("1.00"),
        )

        assert fill.net_value == Decimal("15001.00")


class TestFillDecision:
    """Test FillDecision model."""

    def test_create_should_fill(self) -> None:
        """Test creating a fill decision that should fill."""
        decision = FillDecision(
            should_fill=True,
            fill_price=Decimal("150.00"),
            fill_quantity=Decimal("100"),
            reason="Market order at next bar open",
        )

        assert decision.should_fill is True
        assert decision.fill_price == Decimal("150.00")
        assert decision.fill_quantity == Decimal("100")
        assert decision.reason == "Market order at next bar open"
        assert decision.queue_for_next_bar is False

    def test_create_should_not_fill(self) -> None:
        """Test creating a fill decision that should not fill."""
        decision = FillDecision(
            should_fill=False,
            reason="Limit price not touched",
            queue_for_next_bar=True,
        )

        assert decision.should_fill is False
        assert decision.reason == "Limit price not touched"
        assert decision.queue_for_next_bar is True

    def test_validation_should_fill_requires_price(self) -> None:
        """Test validation requires fill_price when should_fill=True."""
        with pytest.raises(ValueError, match="Fill price must be positive"):
            FillDecision(
                should_fill=True,
                reason="Test",
                fill_quantity=Decimal("100"),
            )

    def test_validation_should_fill_requires_quantity(self) -> None:
        """Test validation requires fill_quantity when should_fill=True."""
        with pytest.raises(ValueError, match="Fill quantity must be positive"):
            FillDecision(
                should_fill=True,
                fill_price=Decimal("150"),
                reason="Test",
            )
