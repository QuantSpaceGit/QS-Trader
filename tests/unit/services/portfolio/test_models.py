"""Unit tests for portfolio service models."""

from datetime import datetime
from decimal import Decimal

import pytest

from qs_trader.services.portfolio.models import (
    Ledger,
    LedgerEntry,
    LedgerEntryType,
    Lot,
    LotSide,
    PortfolioConfig,
    PortfolioState,
    Position,
)


class TestLot:
    """Test Lot model."""

    def test_create_long_lot(self):
        """Test creating a long lot."""
        lot = Lot(
            lot_id="lot_001",
            symbol="AAPL",
            side=LotSide.LONG,
            quantity=Decimal("100"),
            entry_price=Decimal("150.00"),
            entry_timestamp=datetime(2020, 1, 2, 9, 30),
            entry_fill_id="fill_001",
        )

        assert lot.lot_id == "lot_001"
        assert lot.symbol == "AAPL"
        assert lot.side == LotSide.LONG
        assert lot.quantity == Decimal("100")
        assert lot.entry_price == Decimal("150.00")
        assert lot.entry_commission == Decimal("0")
        assert lot.realized_pnl == Decimal("0")

    def test_create_short_lot(self):
        """Test creating a short lot."""
        lot = Lot(
            lot_id="lot_002",
            symbol="AAPL",
            side=LotSide.SHORT,
            quantity=Decimal("-100"),
            entry_price=Decimal("150.00"),
            entry_timestamp=datetime(2020, 1, 2, 9, 30),
            entry_fill_id="fill_002",
        )

        assert lot.side == LotSide.SHORT
        assert lot.quantity == Decimal("-100")

    def test_lot_is_immutable(self):
        """Test that lots are frozen (immutable)."""
        lot = Lot(
            lot_id="lot_001",
            symbol="AAPL",
            side=LotSide.LONG,
            quantity=Decimal("100"),
            entry_price=Decimal("150.00"),
            entry_timestamp=datetime(2020, 1, 2, 9, 30),
            entry_fill_id="fill_001",
        )

        with pytest.raises(Exception):  # Pydantic ValidationError or AttributeError
            lot.quantity = Decimal("200")

    def test_lot_validates_quantity_nonzero(self):
        """Test that lot quantity cannot be zero."""
        with pytest.raises(ValueError, match="quantity cannot be zero"):
            Lot(
                lot_id="lot_001",
                symbol="AAPL",
                side=LotSide.LONG,
                quantity=Decimal("0"),
                entry_price=Decimal("150.00"),
                entry_timestamp=datetime(2020, 1, 2, 9, 30),
                entry_fill_id="fill_001",
            )

    def test_lot_validates_entry_price_positive(self):
        """Test that entry price must be positive."""
        with pytest.raises(ValueError, match="Entry price must be positive"):
            Lot(
                lot_id="lot_001",
                symbol="AAPL",
                side=LotSide.LONG,
                quantity=Decimal("100"),
                entry_price=Decimal("-150.00"),
                entry_timestamp=datetime(2020, 1, 2, 9, 30),
                entry_fill_id="fill_001",
            )


class TestPosition:
    """Test Position model."""

    def test_create_empty_position(self):
        """Test creating an empty position."""
        position = Position(
            symbol="AAPL",
            quantity=Decimal("0"),
            lots=[],
        )

        assert position.symbol == "AAPL"
        assert position.quantity == Decimal("0")
        assert position.side == "flat"
        assert len(position.lots) == 0

    def test_create_long_position(self):
        """Test creating a long position."""
        lot = Lot(
            lot_id="lot_001",
            symbol="AAPL",
            side=LotSide.LONG,
            quantity=Decimal("100"),
            entry_price=Decimal("150.00"),
            entry_timestamp=datetime(2020, 1, 2, 9, 30),
            entry_fill_id="fill_001",
        )

        position = Position(
            symbol="AAPL",
            quantity=Decimal("100"),
            lots=[lot],
            total_cost=Decimal("15000"),
            avg_price=Decimal("150.00"),
        )

        assert position.quantity == Decimal("100")
        assert position.side == "long"
        assert position.total_cost == Decimal("15000")
        assert position.avg_price == Decimal("150.00")

    def test_create_short_position(self):
        """Test creating a short position."""
        position = Position(
            symbol="AAPL",
            quantity=Decimal("-100"),
            lots=[],
        )

        assert position.side == "short"

    def test_position_update_market_value(self):
        """Test updating position with market price."""
        position = Position(
            symbol="AAPL",
            quantity=Decimal("100"),
            lots=[],
            total_cost=Decimal("15000"),
            avg_price=Decimal("150.00"),
        )

        # Update with higher price
        position.update_market_value(Decimal("155.00"))

        assert position.current_price == Decimal("155.00")
        assert position.market_value == Decimal("15500")  # 100 * 155
        assert position.unrealized_pnl == Decimal("500")  # 15500 - 15000

    def test_position_is_mutable(self):
        """Test that positions are mutable (not frozen)."""
        position = Position(
            symbol="AAPL",
            quantity=Decimal("100"),
            lots=[],
        )

        # Should be able to modify
        position.quantity = Decimal("200")
        assert position.quantity == Decimal("200")


class TestLedgerEntry:
    """Test LedgerEntry model."""

    def test_create_fill_entry(self):
        """Test creating a fill entry."""
        entry = LedgerEntry(
            entry_id="entry_001",
            timestamp=datetime(2020, 1, 2, 9, 30),
            entry_type=LedgerEntryType.FILL,
            symbol="AAPL",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            cash_flow=Decimal("-15001.00"),
            commission=Decimal("1.00"),
            fill_id="fill_001",
            description="Buy 100 AAPL @ $150.00",
        )

        assert entry.entry_type == LedgerEntryType.FILL
        assert entry.symbol == "AAPL"
        assert entry.quantity == Decimal("100")
        assert entry.cash_flow == Decimal("-15001.00")
        assert entry.commission == Decimal("1.00")

    def test_ledger_entry_is_immutable(self):
        """Test that ledger entries are frozen."""
        entry = LedgerEntry(
            entry_id="entry_001",
            timestamp=datetime(2020, 1, 2, 9, 30),
            entry_type=LedgerEntryType.FILL,
            cash_flow=Decimal("-15001.00"),
            description="Test",
        )

        with pytest.raises(Exception):
            entry.cash_flow = Decimal("0")


class TestPortfolioConfig:
    """Test PortfolioConfig model."""

    def test_create_default_config(self):
        """Test creating config with defaults."""
        config = PortfolioConfig()

        assert config.initial_cash == Decimal("100000.00")
        assert config.default_commission_per_share == Decimal("0.00")
        assert config.default_borrow_rate_apr == Decimal("0.05")
        assert config.margin_rate_apr == Decimal("0.07")
        assert config.day_count_convention == 360
        assert config.lot_method_long == "fifo"
        assert config.lot_method_short == "lifo"
        assert config.keep_position_history is True

    def test_create_custom_config(self):
        """Test creating config with custom values."""
        config = PortfolioConfig(
            initial_cash=Decimal("50000"),
            default_borrow_rate_apr=Decimal("0.08"),
            margin_rate_apr=Decimal("0.10"),
        )

        assert config.initial_cash == Decimal("50000")
        assert config.default_borrow_rate_apr == Decimal("0.08")
        assert config.margin_rate_apr == Decimal("0.10")

    def test_config_validates_initial_cash_positive(self):
        """Test that initial cash must be positive."""
        with pytest.raises(ValueError, match="Initial cash must be positive"):
            PortfolioConfig(initial_cash=Decimal("-1000"))

    def test_config_validates_day_count(self):
        """Test that day count must be 360 or 365."""
        with pytest.raises(ValueError, match="Day count must be 360 or 365"):
            PortfolioConfig(day_count_convention=364)

    def test_config_validates_lot_method_long(self):
        """Test that lot method for longs must be fifo."""
        with pytest.raises(ValueError, match="Phase 2 only supports 'fifo' for longs"):
            PortfolioConfig(lot_method_long="lifo")

    def test_config_validates_lot_method_short(self):
        """Test that lot method for shorts must be lifo."""
        with pytest.raises(ValueError, match="Phase 2 only supports 'lifo' for shorts"):
            PortfolioConfig(lot_method_short="fifo")

    def test_config_is_immutable(self):
        """Test that config is frozen."""
        config = PortfolioConfig()

        with pytest.raises(Exception):
            config.initial_cash = Decimal("200000")


class TestPortfolioState:
    """Test PortfolioState model."""

    def test_create_portfolio_state(self):
        """Test creating a portfolio state snapshot."""
        state = PortfolioState(
            timestamp=datetime(2020, 1, 2, 16, 0),
            cash=Decimal("100000"),
            positions={},
            equity=Decimal("100000"),
            market_value=Decimal("0"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("0"),
            long_exposure=Decimal("0"),
            short_exposure=Decimal("0"),
            net_exposure=Decimal("0"),
            gross_exposure=Decimal("0"),
            leverage=Decimal("0"),
            total_commissions=Decimal("0"),
            total_borrow_fees=Decimal("0"),
            total_margin_interest=Decimal("0"),
            total_dividends_received=Decimal("0"),
            total_dividends_paid=Decimal("0"),
        )

        assert state.cash == Decimal("100000")
        assert state.equity == Decimal("100000")
        assert len(state.positions) == 0

    def test_portfolio_state_is_immutable(self):
        """Test that portfolio state is frozen."""
        state = PortfolioState(
            timestamp=datetime(2020, 1, 2, 16, 0),
            cash=Decimal("100000"),
            positions={},
            equity=Decimal("100000"),
            market_value=Decimal("0"),
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("0"),
            long_exposure=Decimal("0"),
            short_exposure=Decimal("0"),
            net_exposure=Decimal("0"),
            gross_exposure=Decimal("0"),
            leverage=Decimal("0"),
            total_commissions=Decimal("0"),
            total_borrow_fees=Decimal("0"),
            total_margin_interest=Decimal("0"),
            total_dividends_received=Decimal("0"),
            total_dividends_paid=Decimal("0"),
        )

        with pytest.raises(Exception):
            state.cash = Decimal("200000")


class TestLedger:
    """Test Ledger class."""

    def test_create_empty_ledger(self):
        """Test creating an empty ledger."""
        ledger = Ledger()

        assert ledger.get_entry_count() == 0
        assert ledger.get_entries() == []

    def test_add_entry(self):
        """Test adding entry to ledger."""
        ledger = Ledger()

        entry = LedgerEntry(
            entry_id="entry_001",
            timestamp=datetime(2020, 1, 2, 9, 30),
            entry_type=LedgerEntryType.FILL,
            cash_flow=Decimal("-15000"),
            description="Test",
        )

        ledger.add_entry(entry)

        assert ledger.get_entry_count() == 1
        entries = ledger.get_entries()
        assert len(entries) == 1
        assert entries[0].entry_id == "entry_001"

    def test_add_multiple_entries(self):
        """Test adding multiple entries."""
        ledger = Ledger()

        for i in range(5):
            entry = LedgerEntry(
                entry_id=f"entry_{i:03d}",
                timestamp=datetime(2020, 1, 2, 9, 30 + i),
                entry_type=LedgerEntryType.FILL,
                cash_flow=Decimal("-1000"),
                description=f"Entry {i}",
            )
            ledger.add_entry(entry)

        assert ledger.get_entry_count() == 5

    def test_reject_duplicate_entry_id(self):
        """Test that duplicate entry_id is rejected."""
        ledger = Ledger()

        entry1 = LedgerEntry(
            entry_id="entry_001",
            timestamp=datetime(2020, 1, 2, 9, 30),
            entry_type=LedgerEntryType.FILL,
            cash_flow=Decimal("-1000"),
            description="First",
        )
        ledger.add_entry(entry1)

        entry2 = LedgerEntry(
            entry_id="entry_001",  # Duplicate!
            timestamp=datetime(2020, 1, 2, 10, 0),
            entry_type=LedgerEntryType.FILL,
            cash_flow=Decimal("-2000"),
            description="Second",
        )

        with pytest.raises(ValueError, match="Entry ID entry_001 already exists"):
            ledger.add_entry(entry2)

    def test_query_by_time(self):
        """Test querying entries by time."""
        ledger = Ledger()

        # Add entries at different times
        times = [
            datetime(2020, 1, 2, 9, 30),
            datetime(2020, 1, 2, 10, 0),
            datetime(2020, 1, 2, 10, 30),
        ]

        for i, ts in enumerate(times):
            entry = LedgerEntry(
                entry_id=f"entry_{i:03d}",
                timestamp=ts,
                entry_type=LedgerEntryType.FILL,
                cash_flow=Decimal("-1000"),
                description=f"Entry {i}",
            )
            ledger.add_entry(entry)

        # Query since 10:00
        entries = ledger.get_entries(since=datetime(2020, 1, 2, 10, 0))
        assert len(entries) == 2  # 10:00 and 10:30

    def test_query_by_entry_type(self):
        """Test querying entries by type."""
        ledger = Ledger()

        # Add different types
        types = [
            LedgerEntryType.FILL,
            LedgerEntryType.FILL,
            LedgerEntryType.DIVIDEND,
            LedgerEntryType.SPLIT,
        ]

        for i, entry_type in enumerate(types):
            entry = LedgerEntry(
                entry_id=f"entry_{i:03d}",
                timestamp=datetime(2020, 1, 2, 9, 30),
                entry_type=entry_type,
                cash_flow=Decimal("-1000"),
                description=f"Entry {i}",
            )
            ledger.add_entry(entry)

        # Query fills only
        fills = ledger.get_entries(entry_type=LedgerEntryType.FILL)
        assert len(fills) == 2

        # Query dividends only
        dividends = ledger.get_entries(entry_type=LedgerEntryType.DIVIDEND)
        assert len(dividends) == 1

    def test_query_by_symbol(self):
        """Test querying entries by symbol."""
        ledger = Ledger()

        # Add entries for different symbols
        symbols = ["AAPL", "AAPL", "MSFT", "GOOGL"]

        for i, symbol in enumerate(symbols):
            entry = LedgerEntry(
                entry_id=f"entry_{i:03d}",
                timestamp=datetime(2020, 1, 2, 9, 30),
                entry_type=LedgerEntryType.FILL,
                symbol=symbol,
                cash_flow=Decimal("-1000"),
                description=f"Entry {i}",
            )
            ledger.add_entry(entry)

        # Query AAPL only
        aapl_entries = ledger.get_entries(symbol="AAPL")
        assert len(aapl_entries) == 2

    def test_ledger_max_entries(self):
        """Test ledger respects max_entries limit."""
        ledger = Ledger(max_entries=3)

        # Add 5 entries
        for i in range(5):
            entry = LedgerEntry(
                entry_id=f"entry_{i:03d}",
                timestamp=datetime(2020, 1, 2, 9, 30 + i),
                entry_type=LedgerEntryType.FILL,
                cash_flow=Decimal("-1000"),
                description=f"Entry {i}",
            )
            ledger.add_entry(entry)

        # Should only keep last 3
        assert ledger.get_entry_count() == 3

        entries = ledger.get_entries()
        assert entries[0].entry_id == "entry_002"  # Oldest kept
        assert entries[2].entry_id == "entry_004"  # Newest

    def test_clear_ledger(self):
        """Test clearing ledger."""
        ledger = Ledger()

        # Add entries
        for i in range(3):
            entry = LedgerEntry(
                entry_id=f"entry_{i:03d}",
                timestamp=datetime(2020, 1, 2, 9, 30),
                entry_type=LedgerEntryType.FILL,
                cash_flow=Decimal("-1000"),
                description=f"Entry {i}",
            )
            ledger.add_entry(entry)

        assert ledger.get_entry_count() == 3

        # Clear
        ledger.clear()

        assert ledger.get_entry_count() == 0
        assert ledger.get_entries() == []
