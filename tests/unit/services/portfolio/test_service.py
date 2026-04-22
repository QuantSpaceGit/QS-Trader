"""Unit tests for PortfolioService - basic fill processing (Week 1).

Tests cover:
- Opening long positions (buy with no existing short)
- Opening short positions (sell with no existing long)
- Cash flow calculations
- Position tracking
- Query methods
- Input validation
- Ledger entry creation
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import FillEvent
from qs_trader.events.lifecycle_context import LifecycleRunContext
from qs_trader.events.lifecycle_events import (
    FillLifecycleEvent,
    OrderIntentEvent,
    OrderLifecycleEvent,
    PortfolioLifecycleEvent,
    PositionLifecycleEvent,
    TradeLifecycleEvent,
)
from qs_trader.services.portfolio.models import LedgerEntryType, PortfolioConfig
from qs_trader.services.portfolio.service import PortfolioService


@pytest.fixture
def timestamp() -> datetime:
    """Standard timestamp for tests."""
    return datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def basic_config() -> PortfolioConfig:
    """Standard portfolio configuration."""
    return PortfolioConfig(
        initial_cash=Decimal("100000.00"),
        lot_method_long="fifo",
        lot_method_short="lifo",
    )


@pytest.fixture
def service(basic_config: PortfolioConfig) -> PortfolioService:
    """Standard portfolio service instance."""
    return PortfolioService(config=basic_config)


class TestServiceInitialization:
    """Test service initialization."""

    def test_initialize_with_config(self, basic_config: PortfolioConfig) -> None:
        """Test service initializes with configuration."""
        service = PortfolioService(config=basic_config)

        assert service.get_cash() == Decimal("100000.00")
        assert service.get_positions() == {}
        assert service.get_equity() == Decimal("100000.00")


class TestOpenLongPosition:
    """Test opening long positions (buy with no existing short)."""

    def test_buy_deducts_cash(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test buying stock deducts cash correctly."""
        # Buy 100 AAPL @ $150.00 with $10 commission
        # Cost = 100 * $150.00 + $10 = $15,010.00
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        expected_cash = Decimal("100000.00") - Decimal("15010.00")
        assert service.get_cash() == expected_cash

    def test_buy_creates_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test buying stock creates position entry."""
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.symbol == "AAPL"
        assert position.quantity == Decimal("100")
        # total_cost excludes commission (tracked separately in lot)
        assert position.total_cost == Decimal("15000.00")

    def test_buy_creates_ledger_entry(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test buying stock creates ledger entry."""
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        entries = service.get_ledger()
        assert len(entries) == 1
        assert entries[0].entry_type == LedgerEntryType.FILL
        assert entries[0].symbol == "AAPL"
        assert entries[0].quantity == Decimal("100")
        # cash_flow is negative for buy (cash out), includes commission
        assert entries[0].cash_flow == Decimal("-15010.00")
        assert entries[0].commission == Decimal("10.00")

    def test_buy_multiple_symbols(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test buying multiple symbols."""
        # Buy 100 AAPL @ $150.00
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Buy 50 TSLA @ $200.00
        service.apply_fill(
            fill_id="fill_002",
            timestamp=timestamp,
            symbol="TSLA",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("200.00"),
            commission=Decimal("5.00"),
        )

        positions = service.get_positions()
        assert len(positions) == 2
        assert positions[("unattributed", "AAPL")].quantity == Decimal("100")
        assert positions[("unattributed", "TSLA")].quantity == Decimal("50")

    def test_buy_adds_to_existing_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test adding to existing long position."""
        # First buy: 100 AAPL @ $150.00
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Second buy: 50 AAPL @ $155.00
        service.apply_fill(
            fill_id="fill_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("155.00"),
            commission=Decimal("5.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("150")  # 100 + 50
        # Cost: $15,000 + $7,750 = $22,750 (excludes commissions)
        assert position.total_cost == Decimal("22750.00")


class TestOpenShortPosition:
    """Test opening short positions (sell with no existing long)."""

    def test_short_sell_adds_cash(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test short selling credits cash correctly."""
        # Sell short 100 AAPL @ $150.00 with $10 commission
        # Proceeds = 100 * $150.00 - $10 = $14,990.00
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        expected_cash = Decimal("100000.00") + Decimal("14990.00")
        assert service.get_cash() == expected_cash

    def test_short_sell_creates_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test short selling creates position entry."""
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.symbol == "AAPL"
        assert position.quantity == Decimal("-100")  # Negative quantity
        # total_cost excludes commission (tracked separately)
        assert position.total_cost == Decimal("-15000.00")

    def test_short_sell_creates_ledger_entry(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test short selling creates ledger entry."""
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        entries = service.get_ledger()
        assert len(entries) == 1
        assert entries[0].entry_type == LedgerEntryType.FILL
        assert entries[0].symbol == "AAPL"
        assert entries[0].quantity == Decimal("-100")
        # cash_flow is positive for sell (cash in), minus commission
        assert entries[0].cash_flow == Decimal("14990.00")
        assert entries[0].commission == Decimal("10.00")


class TestInputValidation:
    """Test input validation for apply_fill."""

    def test_reject_zero_quantity(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that zero quantity raises ValueError."""
        with pytest.raises(ValueError, match="must be positive"):
            service.apply_fill(
                fill_id="fill_001",
                timestamp=timestamp,
                symbol="AAPL",
                side="buy",
                quantity=Decimal("0"),
                price=Decimal("150.00"),
                commission=Decimal("10.00"),
            )

    def test_reject_negative_price(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that negative price raises ValueError."""
        with pytest.raises(ValueError, match="Price must be positive"):
            service.apply_fill(
                fill_id="fill_001",
                timestamp=timestamp,
                symbol="AAPL",
                side="buy",
                quantity=Decimal("100"),
                price=Decimal("-150.00"),
                commission=Decimal("10.00"),
            )

    def test_reject_negative_commission(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that negative commission raises ValueError."""
        with pytest.raises(ValueError, match="Commission cannot be negative"):
            service.apply_fill(
                fill_id="fill_001",
                timestamp=timestamp,
                symbol="AAPL",
                side="buy",
                quantity=Decimal("100"),
                price=Decimal("150.00"),
                commission=Decimal("-10.00"),
            )

    def test_reject_duplicate_fill_id(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that duplicate fill_id raises ValueError."""
        # First fill succeeds
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Second fill with same ID fails
        with pytest.raises(ValueError, match="Fill ID fill_001 already exists"):
            service.apply_fill(
                fill_id="fill_001",
                timestamp=timestamp,
                symbol="AAPL",
                side="buy",
                quantity=Decimal("50"),
                price=Decimal("155.00"),
                commission=Decimal("5.00"),
            )


class TestUpdatePrices:
    """Test update_prices method."""

    def test_update_prices_changes_market_value(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that update_prices changes position market values."""
        # Open position: 100 AAPL @ $150.00
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Update price to $160.00
        service.update_prices({"AAPL": Decimal("160.00")})

        position = service.get_position("AAPL")
        assert position is not None
        assert position.market_value == Decimal("16000.00")  # 100 * $160.00
        # Unrealized P&L = $16,000 - $15,000 (commission excluded from cost basis)
        assert position.unrealized_pnl == Decimal("1000.00")

    def test_update_prices_affects_equity(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that price updates affect total equity."""
        # Open position: 100 AAPL @ $150.00
        service.apply_fill(
            fill_id="fill_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Update price to $160.00
        service.update_prices({"AAPL": Decimal("160.00")})

        # New equity = cash + market_value = $84,990 + $16,000
        new_equity = service.get_equity()
        assert new_equity == Decimal("100990.00")


class TestCloseLongPosition:
    """Test closing long positions (FIFO)."""

    def test_close_full_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test closing entire long position."""
        # Open: Buy 100 @ $150
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        initial_cash = service.get_cash()

        # Close: Sell 100 @ $160
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("160.00"),
            commission=Decimal("10.00"),
        )

        # Position should be flat
        position = service.get_position("AAPL")
        assert position is None or position.quantity == Decimal("0")

        # Cash flow:
        # Initial buy: -$15,010 (100 * $150 + $10)
        # Sell: +$15,990 (100 * $160 - $10)
        # Net: +$980
        final_cash = service.get_cash()
        assert final_cash == initial_cash + Decimal("15990.00")

        # Realized P&L: (160 - 150) * 100 - commissions
        # = $1,000 - $20 = $980
        realized_pnl = service.get_realized_pnl()
        assert realized_pnl == Decimal("980.00")

    def test_close_partial_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test partial close of long position."""
        # Open: Buy 100 @ $150
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Close partial: Sell 60 @ $160
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("60"),
            price=Decimal("160.00"),
            commission=Decimal("6.00"),
        )

        # Position should have 40 shares remaining
        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("40")

        # Realized P&L on 60 shares: (160 - 150) * 60 - (entry_comm + exit_comm)
        # Entry commission for 60 shares: $10 * (60/100) = $6
        # Exit commission: $6
        # = $600 - $12 = $588
        realized_pnl = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl == Decimal("588.00")

    def test_close_fifo_multiple_lots(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test FIFO closes oldest lots first."""
        # Buy 100 @ $150 (oldest)
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Buy 50 @ $155 (middle)
        service.apply_fill(
            fill_id="buy_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("155.00"),
            commission=Decimal("5.00"),
        )

        # Buy 75 @ $160 (newest)
        service.apply_fill(
            fill_id="buy_003",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("75"),
            price=Decimal("160.00"),
            commission=Decimal("7.50"),
        )

        # Sell 130 @ $165 (should close oldest 100 + 30 from middle lot)
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("130"),
            price=Decimal("165.00"),
            commission=Decimal("13.00"),
        )

        # Remaining position: 95 shares (20 from lot2 + 75 from lot3)
        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("95")

        # Realized P&L:
        # Lot 1 (100 shares): (165-150)*100 - ($10 entry + $10 exit) = $1,480
        #   Exit commission allocation: $13 * (100/130) = $10
        # Lot 2 (30 shares): (165-155)*30 - ($3 entry + $3 exit) = $294
        #   Entry commission allocation: $5 * (30/50) = $3
        #   Exit commission allocation: $13 * (30/130) = $3
        # Total = $1,480 + $294 = $1,774
        realized_pnl = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl == Decimal("1774.00")

    def test_close_creates_ledger_entry(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that closing creates proper ledger entry."""
        # Open position
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Close position
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("160.00"),
            commission=Decimal("10.00"),
        )

        # Check ledger entries
        entries = service.get_ledger()
        assert len(entries) == 2

        # Second entry should be the close
        close_entry = entries[1]
        assert close_entry.entry_type == LedgerEntryType.FILL
        assert close_entry.symbol == "AAPL"
        assert close_entry.quantity == Decimal("-100")  # Negative for sell
        assert close_entry.price == Decimal("160.00")
        assert close_entry.cash_flow == Decimal("15990.00")  # 100 * 160 - 10
        assert close_entry.realized_pnl == Decimal("980.00")
        assert "FIFO" in close_entry.description

    def test_multiple_partial_closes_preserve_commission(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that commission is correctly allocated across multiple partial closes.

        Critical for audit-ready accounting: commission must be preserved through
        lot splits so that total costs match reality across all closes.
        """
        # Open: Buy 100 @ $150 with $10 commission
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # First partial close: Sell 30 @ $160 with $3 commission
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("30"),
            price=Decimal("160.00"),
            commission=Decimal("3.00"),
        )

        # P&L calculation for first close (30 shares):
        # Entry commission for 30 shares: $10 * (30/100) = $3.00
        # Exit commission: $3.00
        # Total commissions: $6.00
        # Gross P&L: (160 - 150) * 30 = $300
        # Net P&L: $300 - $6 = $294
        realized_pnl_1 = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl_1 == Decimal("294.00"), "First partial close P&L incorrect"

        # Second partial close: Sell 30 @ $165 with $3 commission
        service.apply_fill(
            fill_id="sell_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("30"),
            price=Decimal("165.00"),
            commission=Decimal("3.00"),
        )

        # P&L calculation for second close (30 shares from remaining 70):
        # Remaining lot had commission: $10 * (70/100) = $7.00
        # Entry commission for 30 shares: $7 * (30/70) = $3.00
        # Exit commission: $3.00
        # Total commissions: $6.00
        # Gross P&L: (165 - 150) * 30 = $450
        # Net P&L: $450 - $6 = $444
        # Cumulative: $294 + $444 = $738
        realized_pnl_2 = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl_2 == Decimal("738.00"), "Second partial close cumulative P&L incorrect"

        # Final close: Sell remaining 40 @ $170 with $4 commission
        service.apply_fill(
            fill_id="sell_003",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("40"),
            price=Decimal("170.00"),
            commission=Decimal("4.00"),
        )

        # P&L calculation for final close (40 shares from remaining 40):
        # Remaining lot had commission: $7 * (40/70) = $4.00
        # Entry commission for 40 shares: $4.00
        # Exit commission: $4.00
        # Total commissions: $8.00
        # Gross P&L: (170 - 150) * 40 = $800
        # Net P&L: $800 - $8 = $792
        # Cumulative: $738 + $792 = $1530
        realized_pnl_final = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl_final == Decimal("1530.00"), "Final cumulative P&L incorrect"

        # Verify total entry commission used: $3 + $3 + $4 = $10 ✓
        # Verify total exit commission: $3 + $3 + $4 = $10
        # Verify total commissions: $20 (entry + exit)
        # Verify gross P&L: $300 + $450 + $800 = $1550
        # Verify net P&L: $1550 - $20 = $1530 ✓

        # Position should be fully closed
        position = service.get_position("AAPL")
        assert position is None or position.quantity == Decimal("0")


class TestCloseShortPosition:
    """Test closing short positions (LIFO)."""

    def test_close_full_short_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test closing entire short position."""
        # Open: Sell short 100 @ $150
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        initial_cash = service.get_cash()

        # Close: Buy to cover 100 @ $140
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("140.00"),
            commission=Decimal("10.00"),
        )

        # Position should be flat
        position = service.get_position("AAPL")
        assert position is None or position.quantity == Decimal("0")

        # Cash flow:
        # Initial short: +$14,990 (100 * $150 - $10)
        # Buy to cover: -$14,010 (100 * $140 + $10)
        # Net: +$980
        final_cash = service.get_cash()
        assert final_cash == initial_cash - Decimal("14010.00")

        # Realized P&L: (150 - 140) * 100 - commissions
        # = $1,000 - $20 = $980
        realized_pnl = service.get_realized_pnl()
        assert realized_pnl == Decimal("980.00")

    def test_close_partial_short_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test partial close of short position."""
        # Open: Sell short 100 @ $150
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Close partial: Buy to cover 60 @ $140
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("60"),
            price=Decimal("140.00"),
            commission=Decimal("6.00"),
        )

        # Position should have -40 shares remaining
        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("-40")

        # Realized P&L on 60 shares: (150 - 140) * 60 - (entry_comm + exit_comm)
        # Entry commission for 60 shares: $10 * (60/100) = $6
        # Exit commission: $6
        # = $600 - $12 = $588
        realized_pnl = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl == Decimal("588.00")

    def test_close_lifo_multiple_lots(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test LIFO closes newest lots first."""
        # Sell short 100 @ $150 (oldest)
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Sell short 50 @ $155 (middle)
        service.apply_fill(
            fill_id="sell_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("155.00"),
            commission=Decimal("5.00"),
        )

        # Sell short 75 @ $160 (newest)
        service.apply_fill(
            fill_id="sell_003",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("75"),
            price=Decimal("160.00"),
            commission=Decimal("7.50"),
        )

        # Buy to cover 100 @ $145 (should close newest 75 + 25 from middle lot)
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("145.00"),
            commission=Decimal("10.00"),
        )

        # Remaining position: -125 shares (oldest 100 + 25 from middle)
        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("-125")

        # Realized P&L:
        # Lot 3 (75 shares LIFO newest): (160-145)*75 - ($7.50 entry + $7.50 exit) = $1,110
        #   Exit commission allocation: $10 * (75/100) = $7.50
        # Lot 2 (25 shares): (155-145)*25 - ($2.50 entry + $2.50 exit) = $245
        #   Entry commission allocation: $5 * (25/50) = $2.50
        #   Exit commission allocation: $10 * (25/100) = $2.50
        # Total = $1,110 + $245 = $1,355
        realized_pnl = service.get_realized_pnl(symbol="AAPL")
        assert realized_pnl == Decimal("1355.00")

    def test_multiple_partial_closes_preserve_commission_short(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that commission is correctly allocated across multiple partial short closes.

        Critical for audit-ready accounting: commission must be preserved through
        lot splits so that total costs match reality across all closes.
        """
        # Open: Sell short 120 @ $200 with $12 commission
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="TSLA",
            side="sell",
            quantity=Decimal("120"),
            price=Decimal("200.00"),
            commission=Decimal("12.00"),
        )

        # First partial cover: Buy 40 @ $190 with $4 commission
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="TSLA",
            side="buy",
            quantity=Decimal("40"),
            price=Decimal("190.00"),
            commission=Decimal("4.00"),
        )

        # P&L calculation for first cover (40 shares):
        # Entry commission for 40 shares: $12 * (40/120) = $4.00
        # Exit commission: $4.00
        # Total commissions: $8.00
        # Gross P&L: (200 - 190) * 40 = $400
        # Net P&L: $400 - $8 = $392
        realized_pnl_1 = service.get_realized_pnl(symbol="TSLA")
        assert realized_pnl_1 == Decimal("392.00"), "First partial cover P&L incorrect"

        # Second partial cover: Buy 50 @ $185 with $5 commission
        service.apply_fill(
            fill_id="buy_002",
            timestamp=timestamp,
            symbol="TSLA",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("185.00"),
            commission=Decimal("5.00"),
        )

        # P&L calculation for second cover (50 shares from remaining 80):
        # Remaining lot had commission: $12 * (80/120) = $8.00
        # Entry commission for 50 shares: $8 * (50/80) = $5.00
        # Exit commission: $5.00
        # Total commissions: $10.00
        # Gross P&L: (200 - 185) * 50 = $750
        # Net P&L: $750 - $10 = $740
        # Cumulative: $392 + $740 = $1132
        realized_pnl_2 = service.get_realized_pnl(symbol="TSLA")
        assert realized_pnl_2 == Decimal("1132.00"), "Second partial cover cumulative P&L incorrect"

        # Final cover: Buy remaining 30 @ $180 with $3 commission
        service.apply_fill(
            fill_id="buy_003",
            timestamp=timestamp,
            symbol="TSLA",
            side="buy",
            quantity=Decimal("30"),
            price=Decimal("180.00"),
            commission=Decimal("3.00"),
        )

        # P&L calculation for final cover (30 shares from remaining 30):
        # Remaining lot had commission: $8 * (30/80) = $3.00
        # Entry commission for 30 shares: $3.00
        # Exit commission: $3.00
        # Total commissions: $6.00
        # Gross P&L: (200 - 180) * 30 = $600
        # Net P&L: $600 - $6 = $594
        # Cumulative: $1132 + $594 = $1726
        realized_pnl_final = service.get_realized_pnl(symbol="TSLA")
        assert realized_pnl_final == Decimal("1726.00"), "Final cumulative P&L incorrect"

        # Verify total entry commission used: $4 + $5 + $3 = $12 ✓
        # Verify total exit commission: $4 + $5 + $3 = $12
        # Verify total commissions: $24 (entry + exit)
        # Verify gross P&L: $400 + $750 + $600 = $1750
        # Verify net P&L: $1750 - $24 = $1726 ✓

        # Position should be fully closed
        position = service.get_position("TSLA")
        assert position is None or position.quantity == Decimal("0")

    def test_short_loss_scenario(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test closing short at a loss."""
        # Sell short 100 @ $150
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Buy to cover at higher price @ $160 (loss)
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("160.00"),
            commission=Decimal("10.00"),
        )

        # Realized P&L: (150 - 160) * 100 - $20
        # = -$1,000 - $20 = -$1,020
        realized_pnl = service.get_realized_pnl()
        assert realized_pnl == Decimal("-1020.00")


class TestPortfolioServiceLifecycle:
    """Focused canonical lifecycle-ledger tests for PortfolioService."""

    @staticmethod
    def _build_service(
        basic_config: PortfolioConfig,
    ) -> tuple[PortfolioService, EventBus, InMemoryEventStore, LifecycleRunContext]:
        """Create a portfolio service with event-store-backed lifecycle tracking."""
        event_store = InMemoryEventStore()
        event_bus = EventBus()
        event_bus.attach_store(event_store)
        lifecycle_context = LifecycleRunContext(experiment_id="exp", run_id="run-001")
        service = PortfolioService(config=basic_config, event_bus=event_bus, lifecycle_context=lifecycle_context)
        return service, event_bus, event_store, lifecycle_context

    def test_rejected_open_intent_rolls_pending_position_back_to_flat(
        self,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """Accepted intents that fail downstream should emit explicit pending and rollback states."""
        _, event_bus, event_store, lifecycle_context = self._build_service(basic_config)
        correlation_id = "550e8400-e29b-41d4-a716-446655440031"
        intent_id = "550e8400-e29b-41d4-a716-446655440032"
        order_id = "550e8400-e29b-41d4-a716-446655440033"

        intent_event = OrderIntentEvent(
            experiment_id=lifecycle_context.experiment_id,
            run_id=lifecycle_context.run_id,
            occurred_at=timestamp,
            intent_id=intent_id,
            strategy_id="sma_crossover",
            symbol="AAPL",
            intent_type="open",
            intent_state="accepted",
            direction="long",
            target_quantity=Decimal("100"),
            source_service="manager_service",
            correlation_id=correlation_id,
        )
        rejection_event = OrderLifecycleEvent(
            experiment_id=lifecycle_context.experiment_id,
            run_id=lifecycle_context.run_id,
            occurred_at=timestamp + timedelta(seconds=1),
            order_id=order_id,
            intent_id=intent_id,
            strategy_id="sma_crossover",
            symbol="AAPL",
            order_state="rejected",
            side="buy",
            quantity=Decimal("100"),
            filled_quantity=Decimal("0"),
            order_type="market",
            time_in_force="GTC",
            price_basis=lifecycle_context.execution_price_basis,
            idempotency_key="order-key-001",
            rejection_reason="duplicate order",
            source_service="execution_service",
            correlation_id=correlation_id,
            causation_id=intent_event.event_id,
        )

        event_bus.publish(intent_event)
        event_bus.publish(rejection_event)

        position_events = [
            event for event in event_store.get_by_type("position_lifecycle") if isinstance(event, PositionLifecycleEvent)
        ]

        assert [event.position_state for event in position_events] == ["pending_open", "flat"]
        assert position_events[0].transition_reason == "intent_accepted"
        assert position_events[1].transition_reason == "intent_cancelled"
        assert position_events[1].causation_id == rejection_event.event_id

    def test_cancel_after_partial_open_keeps_position_open(
        self,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """A partial open fill followed by cancellation must not roll the position back to flat."""
        _, event_bus, event_store, lifecycle_context = self._build_service(basic_config)
        correlation_id = "550e8400-e29b-41d4-a716-446655440034"
        intent_id = "550e8400-e29b-41d4-a716-446655440035"
        order_id = "550e8400-e29b-41d4-a716-446655440036"

        intent_event = OrderIntentEvent(
            experiment_id=lifecycle_context.experiment_id,
            run_id=lifecycle_context.run_id,
            occurred_at=timestamp,
            intent_id=intent_id,
            strategy_id="sma_crossover",
            symbol="AAPL",
            intent_type="open",
            intent_state="accepted",
            direction="long",
            target_quantity=Decimal("100"),
            source_service="manager_service",
            correlation_id=correlation_id,
        )
        fill_lifecycle_event = FillLifecycleEvent(
            experiment_id=lifecycle_context.experiment_id,
            run_id=lifecycle_context.run_id,
            occurred_at=timestamp + timedelta(seconds=1),
            fill_id="550e8400-e29b-41d4-a716-446655440037",
            order_id=order_id,
            intent_id=intent_id,
            strategy_id="sma_crossover",
            symbol="AAPL",
            side="buy",
            filled_quantity=Decimal("40"),
            fill_price=Decimal("150.00"),
            price_basis=lifecycle_context.execution_price_basis,
            commission=Decimal("0.40"),
            gross_value=Decimal("6000.00"),
            net_value=Decimal("-6000.40"),
            source_service="execution_service",
            correlation_id=correlation_id,
        )
        fill_event = FillEvent(
            fill_id=fill_lifecycle_event.fill_id,
            source_order_id=order_id,
            timestamp=(timestamp + timedelta(seconds=1)).isoformat().replace("+00:00", "Z"),
            symbol="AAPL",
            side="buy",
            filled_quantity=Decimal("40"),
            fill_price=Decimal("150.00"),
            commission=Decimal("0.40"),
            strategy_id="sma_crossover",
            source_service="execution_service",
            correlation_id=correlation_id,
            causation_id=fill_lifecycle_event.event_id,
        )
        cancel_event = OrderLifecycleEvent(
            experiment_id=lifecycle_context.experiment_id,
            run_id=lifecycle_context.run_id,
            occurred_at=timestamp + timedelta(seconds=2),
            order_id=order_id,
            intent_id=intent_id,
            strategy_id="sma_crossover",
            symbol="AAPL",
            order_state="cancelled",
            side="buy",
            quantity=Decimal("100"),
            filled_quantity=Decimal("40"),
            order_type="market",
            time_in_force="IOC",
            price_basis=lifecycle_context.execution_price_basis,
            idempotency_key="order-key-open-partial",
            cancellation_reason="policy_decision",
            source_service="execution_service",
            correlation_id=correlation_id,
            causation_id=fill_lifecycle_event.event_id,
        )

        event_bus.publish(intent_event)
        event_bus.publish(fill_lifecycle_event)
        event_bus.publish(fill_event)
        event_bus.publish(cancel_event)

        position_events = [
            event for event in event_store.get_by_type("position_lifecycle") if isinstance(event, PositionLifecycleEvent)
        ]

        assert [event.position_state for event in position_events] == ["pending_open", "open"]

    def test_cancel_after_partial_close_rolls_position_back_to_open(
        self,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """A partial close fill followed by cancellation should end in open, not pending_close."""
        service, event_bus, event_store, lifecycle_context = self._build_service(basic_config)
        service.apply_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440052",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("1.00"),
            strategy_id="sma_crossover",
        )

        correlation_id = "550e8400-e29b-41d4-a716-446655440038"
        intent_id = "550e8400-e29b-41d4-a716-446655440039"
        order_id = "550e8400-e29b-41d4-a716-446655440040"

        intent_event = OrderIntentEvent(
            experiment_id=lifecycle_context.experiment_id,
            run_id=lifecycle_context.run_id,
            occurred_at=timestamp + timedelta(seconds=1),
            intent_id=intent_id,
            strategy_id="sma_crossover",
            symbol="AAPL",
            intent_type="close",
            intent_state="accepted",
            direction="long",
            target_quantity=Decimal("100"),
            source_service="manager_service",
            correlation_id=correlation_id,
        )
        fill_lifecycle_event = FillLifecycleEvent(
            experiment_id=lifecycle_context.experiment_id,
            run_id=lifecycle_context.run_id,
            occurred_at=timestamp + timedelta(seconds=2),
            fill_id="550e8400-e29b-41d4-a716-446655440051",
            order_id=order_id,
            intent_id=intent_id,
            strategy_id="sma_crossover",
            symbol="AAPL",
            side="sell",
            filled_quantity=Decimal("40"),
            fill_price=Decimal("155.00"),
            price_basis=lifecycle_context.execution_price_basis,
            commission=Decimal("0.40"),
            gross_value=Decimal("6200.00"),
            net_value=Decimal("6199.60"),
            source_service="execution_service",
            correlation_id=correlation_id,
        )
        fill_event = FillEvent(
            fill_id=fill_lifecycle_event.fill_id,
            source_order_id=order_id,
            timestamp=(timestamp + timedelta(seconds=2)).isoformat().replace("+00:00", "Z"),
            symbol="AAPL",
            side="sell",
            filled_quantity=Decimal("40"),
            fill_price=Decimal("155.00"),
            commission=Decimal("0.40"),
            strategy_id="sma_crossover",
            source_service="execution_service",
            correlation_id=correlation_id,
            causation_id=fill_lifecycle_event.event_id,
        )
        cancel_event = OrderLifecycleEvent(
            experiment_id=lifecycle_context.experiment_id,
            run_id=lifecycle_context.run_id,
            occurred_at=timestamp + timedelta(seconds=3),
            order_id=order_id,
            intent_id=intent_id,
            strategy_id="sma_crossover",
            symbol="AAPL",
            order_state="cancelled",
            side="sell",
            quantity=Decimal("100"),
            filled_quantity=Decimal("40"),
            order_type="market",
            time_in_force="IOC",
            price_basis=lifecycle_context.execution_price_basis,
            idempotency_key="order-key-close-partial",
            cancellation_reason="policy_decision",
            source_service="execution_service",
            correlation_id=correlation_id,
            causation_id=fill_lifecycle_event.event_id,
        )

        event_bus.publish(intent_event)
        event_bus.publish(fill_lifecycle_event)
        event_bus.publish(fill_event)
        event_bus.publish(cancel_event)

        position_events = [
            event for event in event_store.get_by_type("position_lifecycle") if isinstance(event, PositionLifecycleEvent)
        ]

        assert [event.position_state for event in position_events] == ["pending_close", "partially_closing", "open"]

    def test_fill_sequence_emits_trade_position_and_portfolio_lifecycle_events(
        self,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """Open, partial close, and close fills should produce explicit canonical lifecycle states."""
        _, event_bus, event_store, lifecycle_context = self._build_service(basic_config)
        correlation_id = "550e8400-e29b-41d4-a716-446655440041"

        def publish_fill(
            *,
            fill_id: str,
            order_id: str,
            occurred_at: datetime,
            side: str,
            quantity: Decimal,
            price: Decimal,
            commission: Decimal,
            correlation_id: str,
            intent_id: str,
        ) -> FillLifecycleEvent:
            fill_lifecycle_event = FillLifecycleEvent(
                experiment_id=lifecycle_context.experiment_id,
                run_id=lifecycle_context.run_id,
                occurred_at=occurred_at,
                fill_id=fill_id,
                order_id=order_id,
                intent_id=intent_id,
                strategy_id="sma_crossover",
                symbol="AAPL",
                side=side,
                filled_quantity=quantity,
                fill_price=price,
                price_basis=lifecycle_context.execution_price_basis,
                commission=commission,
                gross_value=quantity * price,
                net_value=(-(quantity * price + commission) if side == "buy" else (quantity * price - commission)),
                source_service="execution_service",
                correlation_id=correlation_id,
            )
            fill_event = FillEvent(
                fill_id=fill_id,
                source_order_id=order_id,
                timestamp=occurred_at.isoformat().replace("+00:00", "Z"),
                symbol="AAPL",
                side=side,
                filled_quantity=quantity,
                fill_price=price,
                commission=commission,
                strategy_id="sma_crossover",
                source_service="execution_service",
                correlation_id=correlation_id,
                causation_id=fill_lifecycle_event.event_id,
            )

            event_bus.publish(fill_lifecycle_event)
            event_bus.publish(fill_event)
            return fill_lifecycle_event

        open_fill = publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440042",
            order_id="550e8400-e29b-41d4-a716-446655440043",
            occurred_at=timestamp,
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("1.00"),
            correlation_id=correlation_id,
            intent_id="550e8400-e29b-41d4-a716-446655440044",
        )
        partial_fill = publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440045",
            order_id="550e8400-e29b-41d4-a716-446655440046",
            occurred_at=timestamp + timedelta(minutes=1),
            side="sell",
            quantity=Decimal("40"),
            price=Decimal("155.00"),
            commission=Decimal("0.40"),
            correlation_id=correlation_id,
            intent_id="550e8400-e29b-41d4-a716-446655440047",
        )
        publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440048",
            order_id="550e8400-e29b-41d4-a716-446655440049",
            occurred_at=timestamp + timedelta(minutes=2),
            side="sell",
            quantity=Decimal("60"),
            price=Decimal("160.00"),
            commission=Decimal("0.60"),
            correlation_id=correlation_id,
            intent_id="550e8400-e29b-41d4-a716-446655440050",
        )

        trade_events = [
            event for event in event_store.get_by_type("trade_lifecycle") if isinstance(event, TradeLifecycleEvent)
        ]
        position_events = [
            event for event in event_store.get_by_type("position_lifecycle") if isinstance(event, PositionLifecycleEvent)
        ]
        portfolio_events = [
            event for event in event_store.get_by_type("portfolio_lifecycle") if isinstance(event, PortfolioLifecycleEvent)
        ]
        portfolio_lifecycle_types = [event.model_dump()["lifecycle_type"] for event in portfolio_events]

        assert [event.trade_state for event in trade_events] == ["opening", "open", "partially_closing", "closed"]
        assert [event.position_state for event in position_events] == ["open", "partially_closing", "flat"]
        assert portfolio_lifecycle_types.count("fill_applied") == 3
        assert portfolio_lifecycle_types.count("trade_open") == 1
        assert portfolio_lifecycle_types.count("trade_close") == 1
        assert trade_events[0].causation_id == open_fill.event_id
        assert trade_events[1].causation_id == trade_events[0].event_id
        assert trade_events[2].causation_id == partial_fill.event_id
        assert trade_events[-1].exit_price == Decimal("158.00")
        assert position_events[-1].causation_id == trade_events[-1].event_id
        assert all(event.correlation_id == correlation_id for event in trade_events + position_events + portfolio_events)

    def test_trade_lifecycle_partial_close_exit_price_uses_cumulative_vwap(
        self,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """Partially-closing trade lifecycle rows should expose cumulative exit VWAP, not just the latest fill."""
        _, event_bus, event_store, lifecycle_context = self._build_service(basic_config)
        correlation_id = "550e8400-e29b-41d4-a716-446655440053"

        def publish_fill(
            *,
            fill_id: str,
            order_id: str,
            occurred_at: datetime,
            side: str,
            quantity: Decimal,
            price: Decimal,
            commission: Decimal,
            intent_id: str,
        ) -> None:
            fill_lifecycle_event = FillLifecycleEvent(
                experiment_id=lifecycle_context.experiment_id,
                run_id=lifecycle_context.run_id,
                occurred_at=occurred_at,
                fill_id=fill_id,
                order_id=order_id,
                intent_id=intent_id,
                strategy_id="sma_crossover",
                symbol="AAPL",
                side=side,
                filled_quantity=quantity,
                fill_price=price,
                price_basis=lifecycle_context.execution_price_basis,
                commission=commission,
                gross_value=quantity * price,
                net_value=(-(quantity * price + commission) if side == "buy" else (quantity * price - commission)),
                source_service="execution_service",
                correlation_id=correlation_id,
            )
            fill_event = FillEvent(
                fill_id=fill_id,
                source_order_id=order_id,
                timestamp=occurred_at.isoformat().replace("+00:00", "Z"),
                symbol="AAPL",
                side=side,
                filled_quantity=quantity,
                fill_price=price,
                commission=commission,
                strategy_id="sma_crossover",
                source_service="execution_service",
                correlation_id=correlation_id,
                causation_id=fill_lifecycle_event.event_id,
            )

            event_bus.publish(fill_lifecycle_event)
            event_bus.publish(fill_event)

        publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440054",
            order_id="550e8400-e29b-41d4-a716-446655440055",
            occurred_at=timestamp,
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("1.00"),
            intent_id="550e8400-e29b-41d4-a716-446655440056",
        )
        publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440057",
            order_id="550e8400-e29b-41d4-a716-446655440058",
            occurred_at=timestamp + timedelta(minutes=1),
            side="sell",
            quantity=Decimal("25"),
            price=Decimal("155.00"),
            commission=Decimal("0.25"),
            intent_id="550e8400-e29b-41d4-a716-446655440059",
        )
        publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440060",
            order_id="550e8400-e29b-41d4-a716-446655440061",
            occurred_at=timestamp + timedelta(minutes=2),
            side="sell",
            quantity=Decimal("25"),
            price=Decimal("160.00"),
            commission=Decimal("0.25"),
            intent_id="550e8400-e29b-41d4-a716-446655440062",
        )
        publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440063",
            order_id="550e8400-e29b-41d4-a716-446655440064",
            occurred_at=timestamp + timedelta(minutes=3),
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("165.00"),
            commission=Decimal("0.50"),
            intent_id="550e8400-e29b-41d4-a716-446655440065",
        )

        trade_events = [
            event for event in event_store.get_by_type("trade_lifecycle") if isinstance(event, TradeLifecycleEvent)
        ]
        partial_close_events = [event for event in trade_events if event.trade_state == "partially_closing"]

        assert [event.exit_price for event in partial_close_events] == [Decimal("155.00"), Decimal("157.50")]
        assert trade_events[-1].exit_price == Decimal("161.25")

    def test_reversal_fill_sequence_emits_flat_then_new_short_trade_with_canonical_chain(
        self,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """A long-close reversal into a short should produce explicit flat and new-short lifecycle rows."""
        _, event_bus, event_store, lifecycle_context = self._build_service(basic_config)
        open_long_correlation = "550e8400-e29b-41d4-a716-446655440073"
        close_long_correlation = "550e8400-e29b-41d4-a716-446655440074"
        open_short_correlation = "550e8400-e29b-41d4-a716-446655440075"

        def publish_fill(
            *,
            fill_id: str,
            order_id: str,
            occurred_at: datetime,
            side: str,
            quantity: Decimal,
            price: Decimal,
            commission: Decimal,
            correlation_id: str,
            intent_id: str,
        ) -> FillLifecycleEvent:
            fill_lifecycle_event = FillLifecycleEvent(
                experiment_id=lifecycle_context.experiment_id,
                run_id=lifecycle_context.run_id,
                occurred_at=occurred_at,
                fill_id=fill_id,
                order_id=order_id,
                intent_id=intent_id,
                strategy_id="sma_crossover",
                symbol="AAPL",
                side=side,
                filled_quantity=quantity,
                fill_price=price,
                price_basis=lifecycle_context.execution_price_basis,
                commission=commission,
                gross_value=quantity * price,
                net_value=(-(quantity * price + commission) if side == "buy" else (quantity * price - commission)),
                source_service="execution_service",
                correlation_id=correlation_id,
            )
            fill_event = FillEvent(
                fill_id=fill_id,
                source_order_id=order_id,
                timestamp=occurred_at.isoformat().replace("+00:00", "Z"),
                symbol="AAPL",
                side=side,
                filled_quantity=quantity,
                fill_price=price,
                commission=commission,
                strategy_id="sma_crossover",
                source_service="execution_service",
                correlation_id=correlation_id,
                causation_id=fill_lifecycle_event.event_id,
            )

            event_bus.publish(fill_lifecycle_event)
            event_bus.publish(fill_event)
            return fill_lifecycle_event

        open_long_fill = publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440076",
            order_id="550e8400-e29b-41d4-a716-446655440077",
            occurred_at=timestamp,
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("1.00"),
            correlation_id=open_long_correlation,
            intent_id="550e8400-e29b-41d4-a716-446655440078",
        )
        close_long_fill = publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440079",
            order_id="550e8400-e29b-41d4-a716-446655440080",
            occurred_at=timestamp + timedelta(minutes=1),
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("155.00"),
            commission=Decimal("1.00"),
            correlation_id=close_long_correlation,
            intent_id="550e8400-e29b-41d4-a716-446655440081",
        )
        open_short_fill = publish_fill(
            fill_id="550e8400-e29b-41d4-a716-446655440082",
            order_id="550e8400-e29b-41d4-a716-446655440083",
            occurred_at=timestamp + timedelta(minutes=2),
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("145.00"),
            commission=Decimal("1.00"),
            correlation_id=open_short_correlation,
            intent_id="550e8400-e29b-41d4-a716-446655440084",
        )

        trade_events = [
            event for event in event_store.get_by_type("trade_lifecycle") if isinstance(event, TradeLifecycleEvent)
        ]
        position_events = [
            event for event in event_store.get_by_type("position_lifecycle") if isinstance(event, PositionLifecycleEvent)
        ]
        portfolio_events = [
            event for event in event_store.get_by_type("portfolio_lifecycle") if isinstance(event, PortfolioLifecycleEvent)
        ]
        portfolio_lifecycle_types = [event.model_dump()["lifecycle_type"] for event in portfolio_events]

        assert [event.trade_state for event in trade_events] == ["opening", "open", "closed", "opening", "open"]
        assert [event.position_state for event in position_events] == ["open", "flat", "open"]
        assert trade_events[0].side == "long"
        assert trade_events[2].side == "long"
        assert trade_events[3].side == "short"
        assert trade_events[4].side == "short"
        assert position_events[0].side == "long"
        assert position_events[1].side == "none"
        assert position_events[2].side == "short"
        assert portfolio_lifecycle_types.count("fill_applied") == 3
        assert portfolio_lifecycle_types.count("trade_open") == 2
        assert portfolio_lifecycle_types.count("trade_close") == 1

        assert trade_events[0].causation_id == open_long_fill.event_id
        assert trade_events[1].causation_id == trade_events[0].event_id
        assert trade_events[2].causation_id == close_long_fill.event_id
        assert trade_events[3].causation_id == open_short_fill.event_id
        assert trade_events[4].causation_id == trade_events[3].event_id
        assert position_events[0].causation_id == trade_events[1].event_id
        assert position_events[1].causation_id == trade_events[2].event_id
        assert position_events[2].causation_id == trade_events[4].event_id

        assert [trade_events[0].correlation_id, trade_events[1].correlation_id] == [
            open_long_correlation,
            open_long_correlation,
        ]
        assert trade_events[2].correlation_id == close_long_correlation
        assert [trade_events[3].correlation_id, trade_events[4].correlation_id] == [
            open_short_correlation,
            open_short_correlation,
        ]
        assert [event.correlation_id for event in position_events] == [
            open_long_correlation,
            close_long_correlation,
            open_short_correlation,
        ]
        assert trade_events[0].trade_id != trade_events[3].trade_id
