"""Tests for order expiry and time-in-force logic.

Tests cover:
- Queue bars expiry (orders expire after N bars)
- DAY order expiry (expire at end of trading day)
- GTC order persistence (don't expire across days)
- IOC orders (Immediate or Cancel)
- FOK orders (Fill or Kill)
- Interaction between TIF and partial fills
"""

from datetime import datetime
from decimal import Decimal

import pytest

from qs_trader.services.data.models import Bar
from qs_trader.services.execution.config import CommissionConfig, ExecutionConfig, SlippageConfig
from qs_trader.services.execution.models import Order, OrderSide, OrderState, TimeInForce
from qs_trader.services.execution.service import ExecutionService


@pytest.fixture
def execution_config():
    """Standard execution config with queue_bars=3 for testing."""
    return ExecutionConfig(
        market_order_queue_bars=1,
        max_participation_rate=Decimal("0.10"),
        queue_bars=3,  # Expire after 3 bars queued
        slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
        commission=CommissionConfig(per_share=Decimal("0.005")),
    )


@pytest.fixture
def execution_service(execution_config):
    """Create execution service with test config."""
    return ExecutionService(execution_config)


@pytest.fixture
def sample_bar():
    """Create sample bar for testing."""
    return Bar(
        trade_datetime=datetime(2024, 1, 15, 10, 30),
        open=150.00,
        high=151.00,
        low=149.50,
        close=150.50,
        volume=1000000,
    )


@pytest.fixture
def next_day_bar():
    """Create bar for next trading day."""
    return Bar(
        trade_datetime=datetime(2024, 1, 16, 10, 30),  # Next day
        open=151.00,
        high=152.00,
        low=150.00,
        close=151.50,
        volume=1000000,
    )


# =============================================================================
# Queue Bars Expiry Tests
# =============================================================================


class TestQueueBarsExpiry:
    """Test order expiry based on queue_bars limit."""

    def test_limit_order_expires_after_queue_bars(self):
        """Limit order that's never touched expires after queue_bars limit."""
        # Use shorter queue_bars for this test
        config = ExecutionConfig(
            market_order_queue_bars=1,
            max_participation_rate=Decimal("0.10"),
            queue_bars=3,  # Expire after 3 bars queued
            slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
            commission=CommissionConfig(per_share=Decimal("0.005")),
        )
        service = ExecutionService(config)

        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("140.00"),  # Not touched by bars at 150
            time_in_force=TimeInForce.DAY,
        )

        service.submit_order(order)

        # Bars 1-3: Order queued but not touched (bars_queued increments each bar)
        for i in range(3):
            bar = Bar(
                trade_datetime=datetime(2024, 1, 15, 10, 30 + i),
                open=150.00,
                high=151.00,
                low=149.50,
                close=150.50,
                volume=1000000,
            )
            fills = service.on_bar(bar)
            assert len(fills) == 0
            assert order.state == OrderState.SUBMITTED
            assert order.bars_queued == i + 1

        # Bar 4: Should expire (bars_queued=3 >= queue_bars=3)
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 13, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )
        fills = service.on_bar(bar)
        assert len(fills) == 0
        assert order.state == OrderState.EXPIRED
        assert len(service.get_pending_orders()) == 0

    def test_market_order_fills_before_queue_expiry(self, execution_service, sample_bar):
        """Market order fills before reaching queue_bars limit."""
        order = Order.market_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.DAY,
        )

        execution_service.submit_order(order)

        # Bar 1: Queued (market_order_queue_bars=1)
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 0
        assert order.bars_queued == 1
        assert order.state == OrderState.SUBMITTED

        # Bar 2: Should fill (after 1 bar queued with market_order_queue_bars=1)
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED
        # Order filled so doesn't continue queueing
        assert order.bars_queued == 1


# =============================================================================
# DAY Order Expiry Tests
# =============================================================================


class TestDayOrderExpiry:
    """Test DAY order expiry at end of trading day."""

    def test_day_order_expires_at_day_boundary(self, sample_bar, next_day_bar):
        """DAY limit order expires when crossing day boundary."""
        # Create order with Jan 15 created_at
        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("140.00"),  # Won't be touched
            time_in_force=TimeInForce.DAY,
            created_at=datetime(2024, 1, 15, 10, 0),
        )

        config = ExecutionConfig(
            market_order_queue_bars=1,
            max_participation_rate=Decimal("0.10"),
            queue_bars=10,  # High so it doesn't interfere
            slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
            commission=CommissionConfig(per_share=Decimal("0.005")),
        )
        service = ExecutionService(config)

        # Submit on Jan 15
        service.submit_order(order)
        assert order.submitted_date is not None
        assert order.submitted_date.date() == datetime(2024, 1, 15).date()

        # Process bar on Jan 15 - should not fill (limit not touched)
        fills = service.on_bar(sample_bar)
        assert len(fills) == 0
        assert order.state == OrderState.SUBMITTED

        # Process bar on Jan 16 - should expire (day boundary crossed)
        fills = service.on_bar(next_day_bar)
        assert len(fills) == 0
        assert order.state == OrderState.EXPIRED
        assert len(service.get_pending_orders()) == 0

    def test_day_order_multiple_bars_same_day(self, execution_service):
        """DAY order processes multiple bars on same day without expiry."""
        order = Order.market_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.DAY,
        )

        # Submit on Jan 15 at 10:00
        execution_service.submit_order(order)

        # Process multiple bars on same day
        for hour in range(10, 16):  # 10 AM to 3 PM
            bar = Bar(
                trade_datetime=datetime(2024, 1, 15, hour, 30),
                open=150.00,
                high=151.00,
                low=149.50,
                close=150.50,
                volume=1000000,
            )
            _fills = execution_service.on_bar(bar)
            # Should still be active (same day)
            if hour < 11:  # First bar is queued, second fills
                assert order.state == OrderState.SUBMITTED
            else:
                # After second bar, should fill
                break

        assert order.state == OrderState.FILLED

    def test_day_limit_order_expires_at_day_boundary(self, next_day_bar):
        """DAY limit order expires at day boundary even if not filled."""
        # Create order with Jan 15 timestamp
        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("140.00"),  # Not touched
            time_in_force=TimeInForce.DAY,
            created_at=datetime(2024, 1, 15, 10, 0),
        )

        config = ExecutionConfig(
            market_order_queue_bars=1,
            max_participation_rate=Decimal("0.10"),
            queue_bars=10,  # High so it doesn't interfere
            slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
            commission=CommissionConfig(per_share=Decimal("0.005")),
        )
        service = ExecutionService(config)

        # Submit on Jan 15
        service.submit_order(order)

        # Bar on Jan 15 - doesn't touch limit
        bar1 = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )
        fills = service.on_bar(bar1)
        assert len(fills) == 0
        assert order.state == OrderState.SUBMITTED

        # Bar on Jan 16 - should expire
        fills = service.on_bar(next_day_bar)
        assert len(fills) == 0
        assert order.state == OrderState.EXPIRED


# =============================================================================
# GTC Order Persistence Tests
# =============================================================================


class TestGTCOrderPersistence:
    """Test GTC (Good-Till-Cancelled) order persistence."""

    def test_gtc_order_persists_across_days(self, execution_service, sample_bar, next_day_bar):
        """GTC order persists across day boundary."""
        order = Order.market_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.GTC,
        )

        # Submit on Jan 15
        execution_service.submit_order(order)

        # Process bar on Jan 15 - should queue
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 0
        assert order.state == OrderState.SUBMITTED

        # Process bar on Jan 16 - should fill (not expire)
        fills = execution_service.on_bar(next_day_bar)
        assert len(fills) == 1  # Should fill, not expire
        assert order.state == OrderState.FILLED

    def test_gtc_limit_order_persists_until_filled(self):
        """GTC limit order persists across multiple days until filled."""
        # Use higher queue_bars so order doesn't expire during test
        config = ExecutionConfig(
            market_order_queue_bars=1,
            max_participation_rate=Decimal("0.10"),
            queue_bars=10,  # Higher so order doesn't expire after 3 bars
            slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
            commission=CommissionConfig(per_share=Decimal("0.005")),
        )
        service = ExecutionService(config)

        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("149.00"),
            time_in_force=TimeInForce.GTC,
        )

        service.submit_order(order)

        # Days 1-3: Price doesn't touch limit
        for day in range(15, 18):
            bar = Bar(
                trade_datetime=datetime(2024, 1, day, 10, 30),
                open=150.00,
                high=151.00,
                low=149.50,  # Doesn't touch 149.00
                close=150.50,
                volume=1000000,
            )
            fills = service.on_bar(bar)
            assert len(fills) == 0
            assert order.state == OrderState.SUBMITTED  # Still active

        # Day 4: Price touches limit
        bar_touch = Bar(
            trade_datetime=datetime(2024, 1, 18, 10, 30),
            open=150.00,
            high=150.00,
            low=148.50,  # Touches 149.00
            close=149.50,
            volume=1000000,
        )
        fills = service.on_bar(bar_touch)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED

    def test_gtc_expires_by_queue_bars_not_day(self, execution_service, sample_bar):
        """GTC order expires by queue_bars limit, not day boundary."""
        config = ExecutionConfig(
            market_order_queue_bars=1,
            max_participation_rate=Decimal("0.10"),
            queue_bars=2,  # Short queue for testing
            slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
            commission=CommissionConfig(per_share=Decimal("0.005")),
        )
        service = ExecutionService(config)

        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("140.00"),  # Not touched
            time_in_force=TimeInForce.GTC,
        )

        service.submit_order(order)

        # Process bars without touching limit
        for i in range(3):
            bar = Bar(
                trade_datetime=datetime(2024, 1, 15 + i, 10, 30),
                open=150.00,
                high=151.00,
                low=149.50,
                close=150.50,
                volume=1000000,
            )
            _fills = service.on_bar(bar)

        # Should expire by queue_bars, not day boundary
        assert order.state == OrderState.EXPIRED


# =============================================================================
# IOC (Immediate or Cancel) Tests
# =============================================================================


class TestIOCOrders:
    """Test IOC (Immediate or Cancel) order behavior."""

    def test_ioc_market_order_fills_immediately(self, execution_service, sample_bar):
        """IOC market order fills immediately on first bar."""
        order = Order.market_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.IOC,
        )

        execution_service.submit_order(order)

        # First bar: Should fill immediately (not queued like normal market orders)
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED
        assert fills[0].quantity == Decimal("100")

    def test_ioc_market_order_partial_fill_cancels_rest(self, execution_service):
        """IOC market order with partial fill cancels remaining quantity."""
        # Create low volume bar to force partial fill
        low_volume_bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=500,  # Low volume, 10% = 50 shares fillable
        )

        order = Order.market_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),  # Want 100, can only fill 50
            time_in_force=TimeInForce.IOC,
        )

        execution_service.submit_order(order)

        # First bar: Partial fill, should cancel rest
        fills = execution_service.on_bar(low_volume_bar)
        assert len(fills) == 1
        assert fills[0].quantity == Decimal("50")  # 10% of 500 volume
        assert order.state == OrderState.CANCELLED
        assert order.filled_quantity == Decimal("50")
        assert order.remaining_quantity == Decimal("50")

    def test_ioc_limit_order_not_touched_cancels(self, execution_service, sample_bar):
        """IOC limit order that's not touched cancels immediately."""
        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("140.00"),  # Not touched
            time_in_force=TimeInForce.IOC,
        )

        execution_service.submit_order(order)

        # First bar: Limit not touched, should cancel
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 0
        assert order.state == OrderState.CANCELLED

    def test_ioc_limit_order_touched_fills_immediately(self, execution_service):
        """IOC limit order that's touched fills immediately."""
        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("150.00"),
            time_in_force=TimeInForce.IOC,
        )

        execution_service.submit_order(order)

        # Bar touches limit
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=151.00,
            high=151.00,
            low=149.50,  # Touches 150.00
            close=150.50,
            volume=1000000,
        )

        fills = execution_service.on_bar(bar)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED
        assert fills[0].quantity == Decimal("100")


# =============================================================================
# FOK (Fill or Kill) Tests
# =============================================================================


class TestFOKOrders:
    """Test FOK (Fill or Kill) order behavior."""

    def test_fok_market_order_full_fill_succeeds(self, execution_service, sample_bar):
        """FOK market order with sufficient volume fills completely."""
        order = Order.market_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.FOK,
        )

        execution_service.submit_order(order)

        # First bar: Sufficient volume to fill completely
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED
        assert fills[0].quantity == Decimal("100")

    def test_fok_market_order_partial_fill_cancels_all(self, execution_service):
        """FOK market order with insufficient volume cancels entirely."""
        # Create low volume bar that can't fill complete order
        low_volume_bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=500,  # Low volume, 10% = 50 shares fillable
        )

        order = Order.market_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),  # Want 100, can only fill 50
            time_in_force=TimeInForce.FOK,
        )

        execution_service.submit_order(order)

        # First bar: Cannot fill completely, should cancel without any fill
        fills = execution_service.on_bar(low_volume_bar)
        assert len(fills) == 0  # No partial fill
        assert order.state == OrderState.CANCELLED
        assert order.filled_quantity == Decimal("0")

    def test_fok_limit_order_not_touched_cancels(self, execution_service, sample_bar):
        """FOK limit order that's not touched cancels immediately."""
        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("140.00"),  # Not touched
            time_in_force=TimeInForce.FOK,
        )

        execution_service.submit_order(order)

        # First bar: Limit not touched, should cancel
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 0
        assert order.state == OrderState.CANCELLED

    def test_fok_limit_order_touched_fills_completely(self, execution_service):
        """FOK limit order that's touched fills completely."""
        order = Order.limit_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("150.00"),
            time_in_force=TimeInForce.FOK,
        )

        execution_service.submit_order(order)

        # Bar touches limit with sufficient volume
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=151.00,
            high=151.00,
            low=149.50,  # Touches 150.00
            close=150.50,
            volume=1000000,
        )

        fills = execution_service.on_bar(bar)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED
        assert fills[0].quantity == Decimal("100")

    def test_fok_stop_order_partial_fill_cancels(self, execution_service):
        """FOK stop order with insufficient volume cancels without fill."""
        order = Order.stop_order(
            order_id="ORD1",
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            stop_price=Decimal("149.00"),
            time_in_force=TimeInForce.FOK,
        )

        execution_service.submit_order(order)

        # Bar triggers stop but insufficient volume
        low_volume_bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=150.00,
            low=148.50,  # Triggers stop
            close=149.50,
            volume=500,  # 10% = 50 shares fillable
        )

        fills = execution_service.on_bar(low_volume_bar)
        assert len(fills) == 0  # No partial fill for FOK
        assert order.state == OrderState.CANCELLED


# =============================================================================
# Complex Scenarios
# =============================================================================


class TestComplexExpiryScenarios:
    """Test complex interactions between expiry mechanisms."""

    def test_multiple_orders_independent_expiry(self, sample_bar, next_day_bar):
        """Multiple orders expire independently based on their TIF."""
        # Create config
        config = ExecutionConfig(
            market_order_queue_bars=1,
            max_participation_rate=Decimal("0.10"),
            queue_bars=10,  # High so it doesn't interfere
            slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
            commission=CommissionConfig(per_share=Decimal("0.005")),
        )
        service = ExecutionService(config)

        # DAY order - created on Jan 15
        day_order = Order.market_order(
            order_id="DAY1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.DAY,
            created_at=datetime(2024, 1, 15, 10, 0),
        )

        # GTC order - created on Jan 15
        gtc_order = Order.market_order(
            order_id="GTC1",
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.GTC,
            created_at=datetime(2024, 1, 15, 10, 0),
        )

        # Submit both on Jan 15
        service.submit_order(day_order)
        service.submit_order(gtc_order)

        # Process bar on Jan 15 - both queue
        fills = service.on_bar(sample_bar)
        assert len(fills) == 0
        assert day_order.state == OrderState.SUBMITTED
        assert gtc_order.state == OrderState.SUBMITTED

        # Process bar on Jan 16 - DAY expires, GTC fills
        fills = service.on_bar(next_day_bar)
        assert len(fills) == 1  # Only GTC fills
        assert day_order.state == OrderState.EXPIRED
        assert gtc_order.state == OrderState.FILLED
