"""Integration tests for ExecutionService with PortfolioService.

Tests the complete order-to-position flow:
1. Submit order via ExecutionService
2. Process bar to generate fills
3. Apply fills to PortfolioService
4. Verify positions updated correctly
"""

from datetime import datetime
from decimal import Decimal

import pytest

from qs_trader.services.data.models import Bar
from qs_trader.services.execution.config import CommissionConfig, ExecutionConfig, SlippageConfig
from qs_trader.services.execution.models import Order, OrderSide, OrderState, TimeInForce
from qs_trader.services.execution.service import ExecutionService
from qs_trader.services.portfolio.models import PortfolioConfig
from qs_trader.services.portfolio.service import PortfolioService


@pytest.fixture
def execution_config():
    """Standard execution config for integration tests."""
    return ExecutionConfig(
        market_order_queue_bars=1,
        max_participation_rate=Decimal("0.20"),
        queue_bars=10,
        slippage=SlippageConfig(model="fixed_bps", params={"bps": Decimal("5")}),
        commission=CommissionConfig(per_share=Decimal("0.005"), minimum=Decimal("1.00")),
    )


@pytest.fixture
def portfolio_config():
    """Standard portfolio config for integration tests."""
    return PortfolioConfig(
        initial_cash=Decimal("100000.00"),
        base_currency="USD",
    )


@pytest.fixture
def execution_service(execution_config):
    """Create execution service."""
    return ExecutionService(execution_config)


@pytest.fixture
def portfolio_service(portfolio_config):
    """Create portfolio service."""
    return PortfolioService(portfolio_config)


@pytest.fixture
def sample_bar():
    """Create a sample bar for testing."""
    return Bar(
        trade_datetime=datetime(2024, 1, 15, 10, 30),
        open=150.00,
        high=151.00,
        low=149.50,
        close=150.50,
        volume=1000000,
    )


class TestBasicOrderFlow:
    """Test basic order submission and execution flow."""

    def test_market_order_buy_creates_position(self, execution_service, portfolio_service, sample_bar):
        """Market buy order creates long position in portfolio."""
        # Submit market buy order
        order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            time_in_force=TimeInForce.DAY,
        )
        execution_service.submit_order(order)

        # Queue on first bar
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 0
        assert order.state == OrderState.SUBMITTED

        # Fill on second bar
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED

        # Apply fill to portfolio
        fill = fills[0]
        portfolio_service.apply_fill(
            fill_id=fill.fill_id,
            timestamp=fill.timestamp,
            symbol=fill.symbol,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            commission=fill.commission,
        )

        # Verify position created
        position = portfolio_service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("100")
        assert position.symbol == "AAPL"

        # Verify cash decreased by cost + commission
        current_cash = portfolio_service.get_cash()
        expected_cash = Decimal("100000.00") - (Decimal("100") * fill.price + fill.commission)
        assert current_cash == expected_cash

    def test_market_order_sell_reduces_position(self, execution_service, portfolio_service, sample_bar):
        """Market sell order reduces existing long position."""
        # First, create a position by buying
        buy_order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("200"),
        )
        execution_service.submit_order(buy_order)
        execution_service.on_bar(sample_bar)  # Queue
        buy_fills = execution_service.on_bar(sample_bar)  # Fill

        # Apply buy fill
        for fill in buy_fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        # Verify position
        position = portfolio_service.get_position("AAPL")
        assert position.quantity == Decimal("200")

        # Now sell half
        sell_order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
        )
        execution_service.submit_order(sell_order)
        execution_service.on_bar(sample_bar)  # Queue
        sell_fills = execution_service.on_bar(sample_bar)  # Fill

        # Apply sell fill
        for fill in sell_fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        # Verify position reduced
        position = portfolio_service.get_position("AAPL")
        assert position.quantity == Decimal("100")

    def test_limit_order_buy_when_price_touches(self, execution_service, portfolio_service):
        """Limit buy order fills when price touches limit."""
        # Submit limit buy order
        order = Order.limit_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            limit_price=Decimal("149.50"),
        )
        execution_service.submit_order(order)

        # Bar 1: Price doesn't touch limit
        bar1 = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=150.00,
            close=150.50,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar1)
        assert len(fills) == 0

        # Bar 2: Price touches limit
        bar2 = Bar(
            trade_datetime=datetime(2024, 1, 15, 11, 30),
            open=150.00,
            high=150.50,
            low=149.00,  # Touches 149.50
            close=149.75,
            volume=1000000,
        )
        fills = execution_service.on_bar(bar2)
        assert len(fills) == 1
        assert order.state == OrderState.FILLED

        # Apply fill
        fill = fills[0]
        portfolio_service.apply_fill(
            fill_id=fill.fill_id,
            timestamp=fill.timestamp,
            symbol=fill.symbol,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            commission=fill.commission,
        )

        # Verify position
        position = portfolio_service.get_position("AAPL")
        assert position.quantity == Decimal("100")


class TestMultiSymbolScenarios:
    """Test execution with multiple symbols."""

    def test_multiple_symbols_independent_execution(self, execution_service, portfolio_service, sample_bar):
        """Orders for different symbols execute independently."""
        # Submit orders for two symbols
        aapl_order = Order.market_order(symbol="AAPL", side=OrderSide.BUY, quantity=Decimal("100"))
        msft_order = Order.market_order(symbol="MSFT", side=OrderSide.BUY, quantity=Decimal("50"))

        execution_service.submit_order(aapl_order)
        execution_service.submit_order(msft_order)

        # Process bar once - queues both orders
        execution_service.on_bar(sample_bar)

        # Second bar - fills both orders
        # NOTE: ExecutionService processes ALL pending orders for all symbols
        # In real usage, DataService would send separate bars per symbol
        bar2 = Bar(
            trade_datetime=datetime(2024, 1, 15, 11, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )

        fills = execution_service.on_bar(bar2)

        # Both orders should fill (ExecutionService fills all eligible orders)
        assert len(fills) == 2

        # Find fills for each symbol
        aapl_fills = [f for f in fills if f.symbol == "AAPL"]
        msft_fills = [f for f in fills if f.symbol == "MSFT"]

        assert len(aapl_fills) == 1
        assert len(msft_fills) == 1

        # Apply fills
        for fill in fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        # Verify both positions
        assert portfolio_service.get_position("AAPL").quantity == Decimal("100")
        assert portfolio_service.get_position("MSFT").quantity == Decimal("50")

    def test_simultaneous_buy_and_sell_different_symbols(self, execution_service, portfolio_service):
        """Can buy one symbol while selling another."""
        # First create MSFT position
        msft_buy = Order.market_order(symbol="MSFT", side=OrderSide.BUY, quantity=Decimal("100"))
        execution_service.submit_order(msft_buy)

        bar1 = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=300.00,
            high=302.00,
            low=299.00,
            close=301.00,
            volume=500000,
        )
        execution_service.on_bar(bar1)  # Queue
        fills = execution_service.on_bar(bar1)  # Fill

        for fill in fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        # Verify initial MSFT position
        assert portfolio_service.get_position("MSFT").quantity == Decimal("100")

        # Now simultaneously buy AAPL and sell MSFT
        aapl_buy = Order.market_order(symbol="AAPL", side=OrderSide.BUY, quantity=Decimal("50"))
        msft_sell = Order.market_order(symbol="MSFT", side=OrderSide.SELL, quantity=Decimal("50"))

        execution_service.submit_order(aapl_buy)
        execution_service.submit_order(msft_sell)

        # Process bar - queues both orders
        bar2 = Bar(
            trade_datetime=datetime(2024, 1, 15, 11, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )
        execution_service.on_bar(bar2)  # Queue both

        # Fill both orders
        fills = execution_service.on_bar(bar2)
        assert len(fills) == 2  # Both fill

        # Apply fills
        for fill in fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        # Verify positions
        assert portfolio_service.get_position("AAPL").quantity == Decimal("50")
        assert portfolio_service.get_position("MSFT").quantity == Decimal("50")


class TestPartialFillIntegration:
    """Test partial fills with portfolio integration."""

    def test_partial_fill_multiple_bars(self, execution_service, portfolio_service):
        """Large order fills across multiple bars due to volume limits."""
        # Submit large order (will be limited by max_participation_rate=20%)
        order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("1000"),  # Large order
        )
        execution_service.submit_order(order)

        # Bar 1: Queue
        bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000,  # Low volume: 20% = 200 shares
        )
        fills = execution_service.on_bar(bar)
        assert len(fills) == 0  # Queued

        # Bar 2: Partial fill
        fills = execution_service.on_bar(bar)
        assert len(fills) == 1
        assert fills[0].quantity == Decimal("200")  # 20% of 1000 volume
        assert order.state == OrderState.PARTIAL

        # Apply first fill
        portfolio_service.apply_fill(
            fill_id=fills[0].fill_id,
            timestamp=fills[0].timestamp,
            symbol=fills[0].symbol,
            side=fills[0].side,
            quantity=fills[0].quantity,
            price=fills[0].price,
            commission=fills[0].commission,
        )

        # Verify partial position
        position = portfolio_service.get_position("AAPL")
        assert position.quantity == Decimal("200")

        # Bar 3: Second partial fill
        fills = execution_service.on_bar(bar)
        assert len(fills) == 1
        assert fills[0].quantity == Decimal("200")

        # Apply second fill
        portfolio_service.apply_fill(
            fill_id=fills[0].fill_id,
            timestamp=fills[0].timestamp,
            symbol=fills[0].symbol,
            side=fills[0].side,
            quantity=fills[0].quantity,
            price=fills[0].price,
            commission=fills[0].commission,
        )

        # Verify accumulated position
        position = portfolio_service.get_position("AAPL")
        assert position.quantity == Decimal("400")


class TestCommissionIntegration:
    """Test commission calculations impact portfolio correctly."""

    def test_commission_reduces_cash(self, execution_service, portfolio_service, sample_bar):
        """Commission is properly deducted from cash."""
        initial_cash = portfolio_service.get_cash()

        # Submit order
        order = Order.market_order(
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
        )
        execution_service.submit_order(order)

        execution_service.on_bar(sample_bar)  # Queue
        fills = execution_service.on_bar(sample_bar)  # Fill

        # Apply fill
        fill = fills[0]
        portfolio_service.apply_fill(
            fill_id=fill.fill_id,
            timestamp=fill.timestamp,
            symbol=fill.symbol,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            commission=fill.commission,
        )

        # Verify cash decreased by cost + commission
        final_cash = portfolio_service.get_cash()
        cost = fill.quantity * fill.price
        expected_cash = initial_cash - cost - fill.commission

        assert final_cash == expected_cash
        assert fill.commission > Decimal("0")  # Commission was charged

    def test_commission_on_sell_increases_cash_less(self, execution_service, portfolio_service, sample_bar):
        """Commission on sell reduces proceeds."""
        # First buy
        buy_order = Order.market_order(symbol="AAPL", side=OrderSide.BUY, quantity=Decimal("100"))
        execution_service.submit_order(buy_order)
        execution_service.on_bar(sample_bar)
        buy_fills = execution_service.on_bar(sample_bar)

        for fill in buy_fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        cash_after_buy = portfolio_service.get_cash()

        # Now sell
        sell_order = Order.market_order(symbol="AAPL", side=OrderSide.SELL, quantity=Decimal("100"))
        execution_service.submit_order(sell_order)
        execution_service.on_bar(sample_bar)
        sell_fills = execution_service.on_bar(sample_bar)

        for fill in sell_fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        # Verify cash increased by proceeds minus commission
        final_cash = portfolio_service.get_cash()
        proceeds = sell_fills[0].quantity * sell_fills[0].price
        expected_cash = cash_after_buy + proceeds - sell_fills[0].commission

        assert final_cash == expected_cash


class TestOrderCancellation:
    """Test order cancellation integration."""

    def test_cancelled_order_no_portfolio_impact(self, execution_service, portfolio_service, sample_bar):
        """Cancelled order does not affect portfolio."""
        initial_cash = portfolio_service.get_cash()

        # Submit and immediately cancel
        order = Order.market_order(symbol="AAPL", side=OrderSide.BUY, quantity=Decimal("100"))
        order_id = execution_service.submit_order(order)
        execution_service.cancel_order(order_id)

        # Process bar (should generate no fills)
        fills = execution_service.on_bar(sample_bar)
        assert len(fills) == 0
        assert order.state == OrderState.CANCELLED

        # Verify no portfolio changes
        assert portfolio_service.get_cash() == initial_cash
        assert portfolio_service.get_position("AAPL") is None


class TestComplexScenarios:
    """Test complex real-world scenarios."""

    def test_round_trip_trade_pnl(self, execution_service, portfolio_service):
        """Complete buy-sell round trip calculates P&L correctly."""
        initial_cash = portfolio_service.get_cash()

        # Buy at 150
        buy_bar = Bar(
            trade_datetime=datetime(2024, 1, 15, 10, 30),
            open=150.00,
            high=151.00,
            low=149.50,
            close=150.50,
            volume=1000000,
        )

        buy_order = Order.market_order(symbol="AAPL", side=OrderSide.BUY, quantity=Decimal("100"))
        execution_service.submit_order(buy_order)
        execution_service.on_bar(buy_bar)
        buy_fills = execution_service.on_bar(buy_bar)

        for fill in buy_fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        buy_price = buy_fills[0].price
        buy_commission = buy_fills[0].commission

        # Sell at 155 (profit)
        sell_bar = Bar(
            trade_datetime=datetime(2024, 1, 16, 10, 30),
            open=155.00,
            high=156.00,
            low=154.50,
            close=155.50,
            volume=1000000,
        )

        sell_order = Order.market_order(symbol="AAPL", side=OrderSide.SELL, quantity=Decimal("100"))
        execution_service.submit_order(sell_order)
        execution_service.on_bar(sell_bar)
        sell_fills = execution_service.on_bar(sell_bar)

        for fill in sell_fills:
            portfolio_service.apply_fill(
                fill_id=fill.fill_id,
                timestamp=fill.timestamp,
                symbol=fill.symbol,
                side=fill.side,
                quantity=fill.quantity,
                price=fill.price,
                commission=fill.commission,
            )

        sell_price = sell_fills[0].price
        sell_commission = sell_fills[0].commission

        # Calculate expected P&L
        cost = Decimal("100") * buy_price + buy_commission
        proceeds = Decimal("100") * sell_price - sell_commission
        expected_pnl = proceeds - cost

        # Verify final cash
        final_cash = portfolio_service.get_cash()
        expected_final_cash = initial_cash + expected_pnl

        assert final_cash == expected_final_cash

        # Position should be closed (quantity = 0)
        position = portfolio_service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("0")  # Position fully closed
