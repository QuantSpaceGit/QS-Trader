"""
Integration tests for Portfolio Service.

Tests complex multi-step scenarios: open → close → reopen sequences,
multiple partial closes, position transitions, etc.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.services.portfolio import PortfolioConfig, PortfolioService


@pytest.fixture
def timestamp() -> datetime:
    """Fixed timestamp for testing."""
    return datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)


@pytest.fixture
def service() -> PortfolioService:
    """Create service with default config."""
    config = PortfolioConfig(initial_cash=Decimal("100000"))
    return PortfolioService(config)


class TestOpenCloseReopenSequences:
    """Test complex position lifecycle scenarios."""

    def test_open_close_reopen_long(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test open → full close → reopen long position."""
        # 1. Open: Buy 100 @ $150
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("100")

        # 2. Close: Sell 100 @ $160
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("160.00"),
            commission=Decimal("10.00"),
        )

        position = service.get_position("AAPL")
        assert position is None or position.quantity == Decimal("0")

        # Check realized P&L from first trade
        realized_pnl_1 = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl_1 == Decimal("980.00")  # (160-150)*100 - $20

        # 3. Reopen: Buy 50 @ $155
        service.apply_fill(
            fill_id="buy_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("155.00"),
            commission=Decimal("5.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("50")
        assert position.avg_price == Decimal("155.00")

        # 4. Close again: Sell 50 @ $165
        service.apply_fill(
            fill_id="sell_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("165.00"),
            commission=Decimal("5.00"),
        )

        # Total realized P&L should accumulate both trades
        total_realized_pnl = service.get_realized_pnl(symbol="AAPL")
        # Trade 1: (160-150)*100 - $20 = $980
        # Trade 2: (165-155)*50 - $10 = $490
        # Total: $1,470
        assert total_realized_pnl == Decimal("1470.00")

    def test_multiple_partial_closes(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test opening position and closing in multiple chunks."""
        # Open: Buy 200 @ $150
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("200"),
            price=Decimal("150.00"),
            commission=Decimal("20.00"),
        )

        # Partial close 1: Sell 50 @ $155
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("155.00"),
            commission=Decimal("5.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("150")

        # Partial close 2: Sell 75 @ $160
        service.apply_fill(
            fill_id="sell_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("75"),
            price=Decimal("160.00"),
            commission=Decimal("7.50"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("75")

        # Partial close 3: Sell 75 @ $157
        service.apply_fill(
            fill_id="sell_003",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("75"),
            price=Decimal("157.00"),
            commission=Decimal("7.50"),
        )

        # Should be flat
        position = service.get_position("AAPL")
        assert position is None or position.quantity == Decimal("0")

        # Calculate expected P&L with proper commission allocation:
        # Entry: Buy 200 @ $150 with $20 commission
        #
        # Close 1: Sell 50 @ $155 (25% of position)
        #   Entry commission: $20 * (50/200) = $5
        #   Exit commission: $5
        #   Total commission: $10
        #   Gross P&L: (155-150) * 50 = $250
        #   Net P&L: $250 - $10 = $240
        #
        # Close 2: Sell 75 @ $160 (from remaining 150, so 75/150 = 50%)
        #   Remaining lot after close 1 had: $20 * (150/200) = $15 commission
        #   Entry commission for this close: $15 * (75/150) = $7.50
        #   Exit commission: $7.50
        #   Total commission: $15
        #   Gross P&L: (160-150) * 75 = $750
        #   Net P&L: $750 - $15 = $735
        #
        # Close 3: Sell 75 @ $157 (final 75 shares)
        #   Remaining lot after close 2 had: $15 * (75/150) = $7.50 commission
        #   Entry commission for this close: $7.50
        #   Exit commission: $7.50
        #   Total commission: $15
        #   Gross P&L: (157-150) * 75 = $525
        #   Net P&L: $525 - $15 = $510
        #
        # Total realized P&L: $240 + $735 + $510 = $1,485
        # Total entry commission used: $5 + $7.50 + $7.50 = $20 ✓
        # Total exit commission: $5 + $7.50 + $7.50 = $20
        # Total commissions: $40 (correct)
        realized_pnl = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl == Decimal("1485.00")

    def test_long_to_short_transition(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test closing long and opening short in same symbol."""
        # 1. Open long: Buy 100 @ $150
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # 2. Close long: Sell 100 @ $160
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("160.00"),
            commission=Decimal("10.00"),
        )

        realized_pnl_long = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl_long == Decimal("980.00")

        # 3. Open short: Sell 50 @ $160
        service.apply_fill(
            fill_id="sell_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("160.00"),
            commission=Decimal("5.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("-50")

        # 4. Close short: Buy 50 @ $155 (profit)
        service.apply_fill(
            fill_id="buy_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("155.00"),
            commission=Decimal("5.00"),
        )

        # Total P&L should accumulate both trades
        # Long: (160-150)*100 - $20 = $980
        # Short: (160-155)*50 - $10 = $240
        # Total: $1,220
        total_pnl = service.get_realized_pnl(symbol="AAPL")
        assert total_pnl == Decimal("1220.00")


class TestMultiLotScenarios:
    """Test scenarios with multiple lots at different prices."""

    def test_fifo_across_price_levels(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test FIFO matching closes lots in order regardless of price."""
        # Buy at three different prices
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),  # Oldest
            commission=Decimal("10.00"),
        )

        service.apply_fill(
            fill_id="buy_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("155.00"),  # Middle
            commission=Decimal("10.00"),
        )

        service.apply_fill(
            fill_id="buy_003",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("145.00"),  # Newest (but lowest price)
            commission=Decimal("10.00"),
        )

        # Close 150 shares (should close oldest first: 100 + 50 from middle)
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("150"),
            price=Decimal("160.00"),
            commission=Decimal("15.00"),
        )

        # Remaining position should be 150 shares
        # 50 from lot2 @ $155 + 100 from lot3 @ $145
        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("150")

        # Verify FIFO by checking remaining lots
        # Should have 2 lots
        assert len(position.lots) == 2

        # Check that we have lot3 (complete) and lot2_remaining (partial)
        lot_ids = [lot.lot_id for lot in position.lots]
        assert "buy_003_lot" in lot_ids  # Complete lot3 @ $145
        assert any("buy_002" in lid and "_remaining" in lid for lid in lot_ids)  # Partial lot2 @ $155

        # Find the lots
        lot3 = next(lot for lot in position.lots if lot.lot_id == "buy_003_lot")
        lot2_remaining = next(lot for lot in position.lots if "buy_002" in lot.lot_id)

        # Verify lot3 is complete (wasn't touched by FIFO close)
        assert lot3.quantity == Decimal("100")
        assert lot3.entry_price == Decimal("145.00")

        # Verify lot2 is partial (50 remaining from original 100)
        assert lot2_remaining.quantity == Decimal("50")
        assert lot2_remaining.entry_price == Decimal("155.00")

    def test_accumulate_realized_pnl_across_symbols(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test realized P&L accumulates correctly across multiple symbols."""
        # Trade AAPL
        service.apply_fill(
            fill_id="buy_aapl",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        service.apply_fill(
            fill_id="sell_aapl",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("160.00"),
            commission=Decimal("10.00"),
        )

        # Trade MSFT
        service.apply_fill(
            fill_id="buy_msft",
            timestamp=timestamp,
            symbol="MSFT",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("200.00"),
            commission=Decimal("5.00"),
        )

        service.apply_fill(
            fill_id="sell_msft",
            timestamp=timestamp,
            symbol="MSFT",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("210.00"),
            commission=Decimal("5.00"),
        )

        # Total realized P&L should combine both
        # AAPL: (160-150)*100 - $20 = $980
        # MSFT: (210-200)*50 - $10 = $490
        # Total: $1,470
        total_pnl = service.get_realized_pnl()
        assert total_pnl == Decimal("1470.00")

        # Per-symbol P&L
        aapl_pnl = service.get_realized_pnl(symbol="AAPL")
        msft_pnl = service.get_realized_pnl(symbol="MSFT")
        assert aapl_pnl == Decimal("980.00")
        assert msft_pnl == Decimal("490.00")


class TestLedgerIntegration:
    """Test ledger entries for complex scenarios."""

    def test_ledger_tracks_all_closes(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that ledger records all open and close operations."""
        # Open
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Partial close 1
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("30"),
            price=Decimal("155.00"),
            commission=Decimal("3.00"),
        )

        # Partial close 2
        service.apply_fill(
            fill_id="sell_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("70"),
            price=Decimal("160.00"),
            commission=Decimal("7.00"),
        )

        # Verify ledger has 3 entries
        entries = service.get_ledger()
        assert len(entries) == 3

        # Check each entry has realized P&L where appropriate
        buy_entry = entries[0]
        assert buy_entry.fill_id == "buy_001"
        assert buy_entry.realized_pnl is None  # Opens don't have realized P&L

        sell_entry_1 = entries[1]
        assert sell_entry_1.fill_id == "sell_001"
        assert sell_entry_1.realized_pnl is not None
        assert sell_entry_1.realized_pnl > Decimal("0")

        sell_entry_2 = entries[2]
        assert sell_entry_2.fill_id == "sell_002"
        assert sell_entry_2.realized_pnl is not None
        assert sell_entry_2.realized_pnl > Decimal("0")
