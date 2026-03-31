"""Test that closed trade gets into trades.parquet with canonical trade_id."""

from decimal import Decimal
from pathlib import Path

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import FillEvent, PriceBarEvent
from qs_trader.services.portfolio.models import PortfolioConfig
from qs_trader.services.portfolio.service import PortfolioService
from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.service import ReportingService
from qs_trader.system import LoggerFactory

logger = LoggerFactory.get_logger()


def test_trades_parquet_with_canonical_trade_id():
    """Test that trades.parquet uses canonical trade_id from TradeEvent."""
    event_bus = EventBus()

    # Create config
    portfolio_config = PortfolioConfig(
        portfolio_id="test-portfolio", initial_cash=Decimal("100000"), reporting_currency="USD"
    )

    # Create portfolio service
    _ = PortfolioService(config=portfolio_config, event_bus=event_bus)

    # Create reporting service
    reporting_config = ReportingConfig()
    reporting = ReportingService(event_bus=event_bus, config=reporting_config, output_dir=Path("output/test"))

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

    # Emit a fill to open position
    fill1 = FillEvent(
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
    event_bus.publish(fill1)

    # Verify trade opened
    assert len(captured_trades) == 1
    assert captured_trades[0].status == "open"
    assert captured_trades[0].trade_id == "T00001"
    print(f"\n✅ Trade opened: {captured_trades[0].trade_id}")

    # Emit another bar
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

    # Emit a fill to close position
    fill2 = FillEvent(
        fill_id="c7e6d5c4-b3a2-1098-7654-321fedcba099",
        source_order_id="d7e6d5c4-b3a2-1098-7654-321fedcba099",
        timestamp="2020-01-03T16:00:00Z",
        symbol="AAPL",
        side="sell",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("105.00"),
        commission=Decimal("1.00"),
        strategy_id="momentum",
    )
    event_bus.publish(fill2)

    # Verify trade closed
    assert len(captured_trades) == 2
    closed_trade = captured_trades[1]
    assert closed_trade.status == "closed"
    assert closed_trade.trade_id == "T00001"  # Same trade_id!
    assert closed_trade.entry_price == Decimal("100.00")
    assert closed_trade.exit_price == Decimal("105.00")
    assert closed_trade.realized_pnl is not None

    print(f"\n✅ Trade closed: {closed_trade.trade_id}")
    print(f"   Entry price: ${closed_trade.entry_price}")
    print(f"   Exit price: ${closed_trade.exit_price}")
    print(f"   Realized P&L: ${closed_trade.realized_pnl}")
    print(f"   Commission: ${closed_trade.commission_total}")

    # Check that ReportingService received the trade
    assert reporting._trade_stats_calc.total_trades == 1
    trade_record = reporting._trade_stats_calc.trades[0]
    assert trade_record.trade_id == "T00001"  # ✅ Canonical trade_id!
    assert trade_record.symbol == "AAPL"
    assert trade_record.entry_price == Decimal("100.00")
    assert trade_record.exit_price == Decimal("105.00")

    print(f"\n✅ TradeRecord in reporting:")
    print(f"   trade_id: {trade_record.trade_id} ← Canonical from PortfolioService!")
    print(f"   P&L: ${trade_record.pnl}")
    print(f"   P&L%: {trade_record.pnl_pct:.2f}%")

    print("\n✅ All assertions passed!")
    print("\n📊 trades.parquet will now use canonical trade_id: T00001")


if __name__ == "__main__":
    test_trades_parquet_with_canonical_trade_id()
