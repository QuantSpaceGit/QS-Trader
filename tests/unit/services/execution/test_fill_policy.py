"""Unit tests for fill policy."""

from datetime import datetime
from decimal import Decimal

import pytest

from qs_trader.services.data.models import Bar
from qs_trader.services.execution.config import ExecutionConfig, SlippageConfig
from qs_trader.services.execution.fill_policy import FillPolicy
from qs_trader.services.execution.models import Order, OrderSide, OrderType


def make_config(market_order_queue_bars: int = 1, slippage_bps: Decimal = Decimal("5")) -> ExecutionConfig:
    """Helper to create ExecutionConfig with fixed BPS slippage."""
    return ExecutionConfig(
        market_order_queue_bars=market_order_queue_bars,
        slippage=SlippageConfig(model="fixed_bps", params={"bps": slippage_bps}),
    )


class TestFillPolicyMarket:
    """Test market order evaluation."""

    def test_market_order_queued(self) -> None:
        """Test market order queued for first bar."""
        config = ExecutionConfig(market_order_queue_bars=1)
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is False
        assert "queued" in decision.reason.lower()
        assert decision.queue_for_next_bar is True

    def test_market_order_fills_after_queue(self) -> None:
        """Test market order fills after queueing."""
        config = make_config(market_order_queue_bars=1, slippage_bps=Decimal("5"))
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1  # Simulate queueing

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Buy market: open + slippage = 150 * (1 + 0.0005) = 150.075
        assert decision.fill_price == Decimal("150.0") * Decimal("1.0005")
        assert decision.fill_quantity == Decimal("100")  # No volume constraint

    def test_market_order_partial_fill_volume_limit(self) -> None:
        """Test market order partial fill due to volume participation."""
        config = ExecutionConfig(
            market_order_queue_bars=1,
            max_participation_rate=Decimal("0.10"),  # 10% of volume
        )
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("10000"),  # Want 10,000 shares
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.0,
            high=151.0,
            low=149.0,
            close=150.5,
            volume=50000,  # Max fill = 50000 * 0.10 = 5000
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        assert decision.fill_quantity == Decimal("5000")
        assert decision.queue_for_next_bar is True  # Still have 5000 remaining

    def test_market_order_sell_slippage(self) -> None:
        """Test market sell order gets negative slippage."""
        config = make_config(market_order_queue_bars=1, slippage_bps=Decimal("5"))
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Sell market: open - slippage = 150 * (1 - 0.0005) = 149.925
        assert decision.fill_price == Decimal("150.0") * Decimal("0.9995")


class TestFillPolicyLimit:
    """Test limit order evaluation."""

    def test_buy_limit_not_touched(self) -> None:
        """Test buy limit not filled when bar.low > limit."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("148.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.0,
            high=151.0,
            low=149.0,
            close=150.5,
            volume=1000000,  # low=149 > limit=148
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is False
        assert "not touched" in decision.reason.lower()
        assert decision.queue_for_next_bar is True

    def test_buy_limit_touched_fills_at_min_limit_close(self) -> None:
        """Test buy limit fills at min(limit, close) when touched."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("150.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.5,
            high=151.0,
            low=149.0,  # Touches limit
            close=150.5,
            volume=1000000,
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Buy limit: min(150.0, 150.5) = 150.0
        assert decision.fill_price == Decimal("150.0")
        assert decision.fill_quantity == Decimal("100")

    def test_buy_limit_fills_at_close_when_close_better(self) -> None:
        """Test buy limit fills at close when close < limit."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("150.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.5,
            high=151.0,
            low=149.0,  # Touches limit
            close=149.5,  # Better than limit
            volume=1000000,
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Buy limit: min(150.0, 149.5) = 149.5
        assert decision.fill_price == Decimal("149.5")

    def test_sell_limit_not_touched(self) -> None:
        """Test sell limit not filled when bar.high < limit."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("152.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.0,
            high=151.0,
            low=149.0,
            close=150.5,
            volume=1000000,  # high=151 < limit=152
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is False
        assert decision.queue_for_next_bar is True

    def test_sell_limit_touched_fills_at_max_limit_close(self) -> None:
        """Test sell limit fills at max(limit, close) when touched."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            order_type=OrderType.LIMIT,
            limit_price=Decimal("150.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=149.5,
            high=151.0,  # Touches limit
            low=149.0,
            close=149.5,
            volume=1000000,
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Sell limit: max(150.0, 149.5) = 150.0
        assert decision.fill_price == Decimal("150.0")


class TestFillPolicyStop:
    """Test stop order evaluation."""

    def test_buy_stop_not_triggered(self) -> None:
        """Test buy stop not filled when bar.high < stop."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.STOP,
            stop_price=Decimal("152.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.0,
            high=151.0,
            low=149.0,
            close=150.5,
            volume=1000000,  # high=151 < stop=152
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is False
        assert "not triggered" in decision.reason.lower()

    def test_buy_stop_triggered_fills_with_slippage(self) -> None:
        """Test buy stop fills at max(stop, close) + slippage when triggered."""
        config = make_config(slippage_bps=Decimal("5"))
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.STOP,
            stop_price=Decimal("150.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=149.5,
            high=151.0,  # Triggers stop
            low=149.0,
            close=149.5,
            volume=1000000,
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Buy stop: max(150.0, 149.5) = 150.0, then + slippage = 150.0 * 1.0005 = 150.075
        expected_price = Decimal("150.0") * Decimal("1.0005")
        assert decision.fill_price == expected_price

    def test_sell_stop_not_triggered(self) -> None:
        """Test sell stop not filled when bar.low > stop."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            order_type=OrderType.STOP,
            stop_price=Decimal("148.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.0,
            high=151.0,
            low=149.0,
            close=150.5,
            volume=1000000,  # low=149 > stop=148
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is False

    def test_sell_stop_triggered_fills_with_slippage(self) -> None:
        """Test sell stop fills at min(stop, close) - slippage when triggered."""
        config = make_config(slippage_bps=Decimal("5"))
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            order_type=OrderType.STOP,
            stop_price=Decimal("150.0"),
            created_at=datetime.now(),
        )

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.5,
            high=151.0,
            low=149.0,  # Triggers stop
            close=150.5,
            volume=1000000,
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Sell stop: min(150.0, 150.5) = 150.0, then - slippage = 150.0 * 0.9995 = 149.925
        expected_price = Decimal("150.0") * Decimal("0.9995")
        assert decision.fill_price == expected_price


class TestFillPolicyMOC:
    """Test market-on-close order evaluation."""

    def test_moc_fills_at_close_with_slippage(self) -> None:
        """Test MOC fills at close + slippage."""
        config = make_config(slippage_bps=Decimal("5"))
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET_ON_CLOSE,
            created_at=datetime.now(),
        )

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Buy MOC: close + slippage = 150.5 * 1.0005 = 150.57525
        expected_price = Decimal("150.5") * Decimal("1.0005")
        assert decision.fill_price == expected_price

    def test_moc_sell_with_slippage(self) -> None:
        """Test MOC sell with negative slippage."""
        config = make_config(slippage_bps=Decimal("5"))
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET_ON_CLOSE,
            created_at=datetime.now(),
        )

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        # Sell MOC: close - slippage = 150.5 * 0.9995 = 150.42475
        expected_price = Decimal("150.5") * Decimal("0.9995")
        assert decision.fill_price == expected_price


class TestFillPolicyEdgeCases:
    """Test edge cases and error conditions."""

    def test_inactive_order_not_evaluated(self) -> None:
        """Test inactive orders return should_fill=False."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.cancel(datetime.now())

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is False
        assert "terminal state" in decision.reason.lower()

    def test_zero_volume_bar_no_fill(self) -> None:
        """Test zero volume bar prevents fill."""
        config = ExecutionConfig(market_order_queue_bars=1)
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=0)

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is False
        assert "zero volume" in decision.reason.lower()
        assert decision.queue_for_next_bar is True

    def test_partial_fill_queues_for_next_bar(self) -> None:
        """Test partial fill sets queue_for_next_bar=True."""
        config = ExecutionConfig(market_order_queue_bars=1, max_participation_rate=Decimal("0.10"))
        policy = FillPolicy(config)

        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("10000"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.bars_queued = 1

        bar = Bar(
            trade_datetime=datetime.now(),
            open=150.0,
            high=151.0,
            low=149.0,
            close=150.5,
            volume=1000,  # Max fill = 100
        )

        decision = policy.evaluate_order(order, bar)

        assert decision.should_fill is True
        assert decision.fill_quantity == Decimal("100")
        assert decision.queue_for_next_bar is True

    def test_unsupported_order_type_raises(self) -> None:
        """Test unsupported order type raises ValueError."""
        config = ExecutionConfig()
        policy = FillPolicy(config)

        # Create order with invalid type (by manipulating after creation)
        order = Order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
            created_at=datetime.now(),
        )
        order.order_type = "invalid"  # type: ignore

        bar = Bar(trade_datetime=datetime.now(), open=150.0, high=151.0, low=149.0, close=150.5, volume=1000000)

        with pytest.raises(ValueError, match="Unsupported order type"):
            policy.evaluate_order(order, bar)
