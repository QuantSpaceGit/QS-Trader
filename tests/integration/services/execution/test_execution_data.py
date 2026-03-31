"""Integration tests for ExecutionService with DataService.

Tests the execution service behavior with various data scenarios:
1. Missing bars (no data for symbol)
2. Close-only bars (no OHLCV data)
3. Market halts and gaps
4. Volume constraints with real data
"""

from datetime import datetime
from decimal import Decimal
from unittest.mock import Mock

import pytest

from qs_trader.services.data.interface import IDataService
from qs_trader.services.data.models import Bar
from qs_trader.services.execution.config import ExecutionConfig, SlippageConfig
from qs_trader.services.execution.models import Order, OrderSide, OrderState, TimeInForce
from qs_trader.services.execution.service import ExecutionService


@pytest.fixture
def execution_config():
    """Standard execution config."""
    return ExecutionConfig(
        market_order_queue_bars=1,
        max_participation_rate=Decimal("0.20"),
        slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
    )


@pytest.fixture
def execution_service(execution_config):
    """Create execution service."""
    return ExecutionService(execution_config)


@pytest.fixture
def mock_data_service():
    """Create mock data service."""
    return Mock(spec=IDataService)


class TestMissingBars:
    """Test execution when bars are missing."""

    def test_order_waits_when_no_bar_data(self, execution_service):
        """Order remains in queue when no bar data arrives."""
        order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
        )
        execution_service.submit_order(order)

        # First bar queues
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar)
        assert len(fills) == 0
        assert order.state == OrderState.SUBMITTED

        # No second bar arrives - order should remain queued
        # (In real system, DataService would not emit bar)
        assert order.state == OrderState.SUBMITTED
        assert order.filled_quantity == Decimal("0")

    def test_limit_order_waits_for_price_touch(self, execution_service):
        """Limit order waits through multiple bars until price touches."""
        order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("145.00"),
        )
        execution_service.submit_order(order)

        # Bar 1: Price above limit
        bar1 = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar1)
        assert len(fills) == 0

        # Bar 2: Still above limit
        bar2 = Bar(
            trade_datetime=datetime(2024, 1, 15, 11, 30),
            open=149.00,
            high=150.00,
            low=148.00,
            close=149.50,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar2)
        assert len(fills) == 0

        # Bar 3: Price finally touches limit
        bar3 = Bar(
            trade_datetime=datetime(2024, 1, 15, 12, 30),
            open=146.00,
            high=147.00,
            low=144.00,  # Touches 145.00
            close=146.50,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar3)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED


class TestVolumeConstraints:
    """Test volume-based constraints with realistic scenarios."""

    def test_low_volume_limits_fill_size(self, execution_service):
        """Large order fills incrementally when volume is low."""
        order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("10000"),  # Large order
        )
        execution_service.submit_order(order)

        # Low volume bar (20% participation = 200 shares)
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000,  # Very low volume
        )

        execution_service.on_bar(bar)  # Queue
        fills = execution_service.on_bar(bar)  # Partial fill

        assert len(fills) == 1
        assert fills[0].quantity == Decimal("200")  # 20% of 1000
        assert order.state == OrderState.PARTIAL
        assert order.filled_quantity == Decimal("200")
        assert order.remaining_quantity == Decimal("9800")

    def test_high_volume_allows_full_fill(self, execution_service):
        """Order fills completely when volume is sufficient."""
        order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
        )
        execution_service.submit_order(order)

        # High volume bar
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=10000000,  # Very high volume
        )

        execution_service.on_bar(bar)  # Queue
        fills = execution_service.on_bar(bar)  # Full fill

        assert len(fills) == 1
        assert fills[0].quantity == Decimal("100")  # Full order
        assert order.state == OrderState.FILLED

    def test_zero_volume_bar_no_fill(self, execution_service):
        """Order cannot fill on zero-volume bar."""
        order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
        )
        execution_service.submit_order(order)

        # Zero volume bar (market halt scenario)
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=150.00,
            low=150.00,
            close=150.00,
            volume=0,  # No volume
        )

        execution_service.on_bar(bar)  # Queue
        fills = execution_service.on_bar(bar)  # Cannot fill

        assert len(fills) == 0
        assert order.state == OrderState.SUBMITTED  # Still queued


class TestPriceGaps:
    """Test execution behavior with price gaps."""

    def test_limit_buy_fills_on_gap_down(self, execution_service):
        """Limit buy fills at limit price even when market gaps through it."""
        order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("150.00"),
        )
        execution_service.submit_order(order)

        # Gap down through limit price (open below limit)
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=145.00,  # Gaps down through 150.00
            high=146.00,
            low=144.00,
            close=145.50,
            volume=1000000,
        )

        fills = execution_service.on_bar(bar)
        assert len(fills) == 1
        # Should fill at limit price, not open
        assert fills[0].price <= order.limit_price
        assert order.state == OrderState.FILLED

    def test_limit_sell_fills_on_gap_up(self, execution_service):
        """Limit sell fills at limit price when market gaps through it."""
        order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            limit_price=Decimal("150.00"),
        )
        execution_service.submit_order(order)

        # Gap up through limit price
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=155.00,  # Gaps up through 150.00
            high=156.00,
            low=154.00,
            close=155.50,
            volume=1000000,
        )

        fills = execution_service.on_bar(bar)
        assert len(fills) == 1
        assert fills[0].price >= order.limit_price
        assert order.state == OrderState.FILLED

    def test_stop_order_triggers_on_gap(self, execution_service):
        """Stop order triggers when market gaps through stop price."""
        order = Order.stop_order(
            symbol="AAPL",
            side=OrderSide.SELL,  # Stop loss
            quantity=Decimal("100"),
            stop_price=Decimal("145.00"),
        )
        execution_service.submit_order(order)

        # Gap down through stop
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=140.00,  # Gaps down through 145.00
            high=141.00,
            low=139.00,
            close=140.50,
            volume=1000000,
        )

        fills = execution_service.on_bar(bar)
        assert len(fills) == 1
        # Fills at next available price (open)
        assert order.state == OrderState.FILLED


class TestMarketHalts:
    """Test behavior during market halts and resumed trading."""

    def test_order_persists_through_halt(self, execution_service):
        """Order remains active through zero-volume halt bars."""
        order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("150.00"),
        )
        execution_service.submit_order(order)

        # Normal bar - no fill (price too high)
        bar1 = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=155.00,
            high=156.00,
            low=154.00,
            close=155.50,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar1)
        assert len(fills) == 0

        # Halt bar (zero volume, flat price)
        halt_bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 35),
            open=155.50,
            high=155.50,
            low=155.50,
            close=155.50,
            volume=0,  # Halted
        )
        fills = execution_service.on_bar(halt_bar)
        assert len(fills) == 0
        assert order.state == OrderState.SUBMITTED  # Still active

        # Resume trading - price touches limit
        bar2 = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 40),
            open=152.00,
            high=153.00,
            low=149.00,  # Touches limit
            close=150.50,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar2)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED


class TestRealisticDataScenarios:
    """Test with realistic market data patterns."""

    def test_intraday_volatility_limit_order(self, execution_service):
        """Limit order fills during intraday volatility spike."""
        order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("500"),
            limit_price=Decimal("148.00"),
        )
        execution_service.submit_order(order)

        # Morning: Price above limit
        bar1 = Bar(
            trade_datetime=datetime(2024, 1, 15, 9, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=5000000,
        )
        fills = execution_service.on_bar(bar1)
        assert len(fills) == 0

        # Mid-morning: Brief dip touches limit
        bar2 = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.25,
            high=150.75,
            low=147.50,  # Volatility spike down
            close=149.00,
            volume=3000000,
        )
        fills = execution_service.on_bar(bar2)
        assert len(fills) == 1
        assert fills[0].quantity == Decimal("500")
        assert order.state == OrderState.FILLED

    def test_end_of_day_moc_order(self, execution_service):
        """MOC order fills like market order (close-time detection not implemented).

        NOTE: Current implementation treats MOC as standard market order.
        TODO Week 4: Implement proper MOC handling with close-time detection.
        """
        order = Order.moc_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
        )
        execution_service.submit_order(order)

        # First bar - queue
        bar1 = Bar(
            trade_datetime=datetime(2024, 1, 15, 14, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar1)
        assert len(fills) == 1  # Currently fills immediately (not correct MOC behavior)
        assert order.state == OrderState.FILLED


class TestMultipleOrdersOnSameBar:
    """Test multiple orders processing on the same bar."""

    def test_multiple_limit_orders_same_symbol(self, execution_service):
        """Multiple limit orders at different prices fill correctly."""
        # Buy order at 149
        buy_order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("149.00"),
        )
        execution_service.submit_order(buy_order)

        # Sell order at 151
        sell_order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            limit_price=Decimal("151.00"),
        )
        execution_service.submit_order(sell_order)

        # Bar with range that touches both limits
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=152.00,  # Touches sell limit
            low=148.00,  # Touches buy limit
            close=150.50,
            volume=1000000,
        )

        fills = execution_service.on_bar(bar)
        assert len(fills) == 2  # Both orders fill
        assert buy_order.state == OrderState.FILLED
        assert sell_order.state == OrderState.FILLED

    def test_ioc_and_gtc_orders_different_behavior(self, execution_service):
        """IOC market order fills immediately (no queueing), GTC waits for better price."""
        # IOC market order - should fill on first bar (no queue)
        ioc_order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.IOC,
        )
        execution_service.submit_order(ioc_order)

        # GTC limit order - waits for limit price
        gtc_order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("145.00"),
        )
        execution_service.submit_order(gtc_order)

        # First bar - price at 150
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )

        # IOC fills immediately (skips queue), limit order waits
        fills = execution_service.on_bar(bar)
        assert len(fills) == 1  # Only IOC fills
        assert ioc_order.state == OrderState.FILLED
        assert gtc_order.state == OrderState.SUBMITTED  # Still waiting for better price
