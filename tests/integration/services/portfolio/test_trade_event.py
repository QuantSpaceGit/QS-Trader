"""Quick test to verify TradeEvent emission."""

from decimal import Decimal

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import FillEvent, PriceBarEvent
from qs_trader.services.portfolio.models import PortfolioConfig
from qs_trader.services.portfolio.service import PortfolioService
from qs_trader.system import LoggerFactory

logger = LoggerFactory.get_logger()


def test_trade_event_emission():
    """Test that PortfolioService emits TradeEvent when fills occur."""
    event_bus = EventBus()

    # Create config
    config = PortfolioConfig(portfolio_id="test-portfolio", initial_cash=Decimal("100000"), reporting_currency="USD")

    # Create portfolio service
    _ = PortfolioService(config=config, event_bus=event_bus)

    # Capture TradeEvents
    captured_trades = []
    event_bus.subscribe("trade", lambda e: captured_trades.append(e))

    # Emit bar to initialize portfolio state
    bar = PriceBarEvent(
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
    event_bus.publish(bar)

    # Emit a fill directly (simulating ExecutionService)
    fill = FillEvent(
        fill_id="a7e6d5c4-b3a2-1098-7654-321fedcba098",
        source_order_id="b7e6d5c4-b3a2-1098-7654-321fedcba098",
        timestamp="2020-01-02T16:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("100.00"),
        commission=Decimal("1.00"),
        strategy_id="momentum",
    )
    event_bus.publish(fill)

    # Verify TradeEvent was emitted
    assert len(captured_trades) >= 1, "Expected at least one TradeEvent"

    trade = captured_trades[0]
    print(f"\n✅ TradeEvent emitted!")
    print(f"   trade_id: {trade.trade_id}")
    print(f"   symbol: {trade.symbol}")
    print(f"   strategy_id: {trade.strategy_id}")
    print(f"   status: {trade.status}")
    print(f"   side: {trade.side}")
    print(f"   fills: {trade.fills}")
    print(f"   entry_price: {trade.entry_price}")
    print(f"   current_quantity: {trade.current_quantity}")
    print(f"   commission_total: {trade.commission_total}")

    assert trade.trade_id.startswith("T"), f"Expected trade_id to start with 'T', got {trade.trade_id}"
    assert trade.symbol == "AAPL"
    assert trade.strategy_id == "momentum"
    assert trade.status == "open"  # Position still open
    assert trade.side == "long"
    assert len(trade.fills) == 1  # One fill so far

    print("\n✅ All assertions passed!")


if __name__ == "__main__":
    test_trade_event_emission()
