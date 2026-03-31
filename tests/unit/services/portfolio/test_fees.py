"""Unit tests for mark-to-market and fee accruals - Week 3."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.services.portfolio import PortfolioConfig, PortfolioService
from qs_trader.services.portfolio.models import LedgerEntryType


@pytest.fixture
def timestamp() -> datetime:
    """Standard timestamp for tests."""
    return datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def basic_config() -> PortfolioConfig:
    """Standard portfolio configuration."""
    return PortfolioConfig(
        initial_cash=Decimal("100000.00"),
        default_borrow_rate_apr=Decimal("0.05"),  # 5% annual
        margin_rate_apr=Decimal("0.07"),  # 7% annual
        day_count_convention=360,
    )


@pytest.fixture
def service(basic_config: PortfolioConfig) -> PortfolioService:
    """Standard portfolio service instance."""
    return PortfolioService(config=basic_config)


class TestBorrowFees:
    """Test borrow fee calculation on short positions."""

    def test_borrow_fee_short_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test borrow fee accrual on short position."""
        # Open short position: Sell 100 @ $150
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Update price to $150 (market value = -$15,000)
        service.update_prices({"AAPL": Decimal("150.00")})

        initial_cash = service.get_cash()

        # Mark to market - accrue borrow fee
        # Daily borrow fee = abs(market_value) * annual_rate / day_count
        # = $15,000 * 0.05 / 360 = $2.0833...
        eod = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod)

        # Cash should decrease by borrow fee
        expected_fee = Decimal("15000") * Decimal("0.05") / Decimal("360")
        expected_cash = initial_cash - expected_fee
        assert service.get_cash() == expected_cash

        # Check cumulative borrow fees
        state = service.get_state()
        assert state.total_borrow_fees == expected_fee

        # Check ledger entry
        entries = service.get_ledger()
        borrow_entries = [e for e in entries if e.entry_type == LedgerEntryType.BORROW_FEE]
        assert len(borrow_entries) == 1
        assert borrow_entries[0].symbol == "AAPL"
        assert borrow_entries[0].cash_flow == -expected_fee

    def test_borrow_fee_no_charge_on_long(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that long positions don't incur borrow fees."""
        # Open long position: Buy 100 @ $150
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        service.update_prices({"AAPL": Decimal("150.00")})

        initial_cash = service.get_cash()

        # Mark to market - no borrow fee for long positions
        eod = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod)

        # Cash should remain unchanged (no borrow fee)
        assert service.get_cash() == initial_cash

        # Check no borrow fees
        state = service.get_state()
        assert state.total_borrow_fees == Decimal("0")

    def test_borrow_fee_multiple_short_positions(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test borrow fees on multiple short positions."""
        # Short AAPL: -100 @ $150 (value = -$15,000)
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Short TSLA: -50 @ $200 (value = -$10,000)
        service.apply_fill(
            fill_id="sell_002",
            timestamp=timestamp,
            symbol="TSLA",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("200.00"),
            commission=Decimal("5.00"),
        )

        service.update_prices({"AAPL": Decimal("150.00"), "TSLA": Decimal("200.00")})

        initial_cash = service.get_cash()

        # Mark to market
        eod = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod)

        # Total borrow fees:
        # AAPL: $15,000 * 0.05 / 360 = $2.0833...
        # TSLA: $10,000 * 0.05 / 360 = $1.3888...
        # Total = $3.472...
        expected_fee_aapl = Decimal("15000") * Decimal("0.05") / Decimal("360")
        expected_fee_tsla = Decimal("10000") * Decimal("0.05") / Decimal("360")
        expected_total_fee = expected_fee_aapl + expected_fee_tsla

        expected_cash = initial_cash - expected_total_fee
        assert service.get_cash() == expected_cash

        # Check cumulative
        state = service.get_state()
        assert state.total_borrow_fees == expected_total_fee

    def test_borrow_fee_custom_rate(
        self,
        timestamp: datetime,
    ) -> None:
        """Test borrow fee with symbol-specific rate."""
        # Create config with custom borrow rate for AAPL
        config = PortfolioConfig(
            initial_cash=Decimal("100000.00"),
            default_borrow_rate_apr=Decimal("0.05"),  # 5% default
            borrow_rate_by_symbol={"AAPL": Decimal("0.15")},  # 15% for AAPL
            day_count_convention=360,
        )
        service = PortfolioService(config)

        # Short AAPL
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        service.update_prices({"AAPL": Decimal("150.00")})

        initial_cash = service.get_cash()

        # Mark to market with custom rate
        eod = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod)

        # Borrow fee with 15% rate: $15,000 * 0.15 / 360 = $6.25
        expected_fee = Decimal("15000") * Decimal("0.15") / Decimal("360")
        expected_cash = initial_cash - expected_fee
        assert service.get_cash() == expected_cash


class TestMarginInterest:
    """Test margin interest calculation on negative cash."""

    def test_margin_interest_negative_cash(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test margin interest accrual on negative cash balance."""
        # Create negative cash situation
        # Buy more than we have cash for
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("1000"),
            price=Decimal("150.00"),
            commission=Decimal("100.00"),
        )

        # Cash is now negative: $100,000 - $150,100 = -$50,100
        cash_before_mtm = service.get_cash()
        assert cash_before_mtm < 0

        # Mark to market - accrue margin interest
        # Daily interest = abs(cash) * annual_rate / day_count
        # = $50,100 * 0.07 / 360 = $9.7416...
        eod = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod)

        # Cash should decrease further (more negative) by margin interest
        expected_interest = abs(cash_before_mtm) * Decimal("0.07") / Decimal("360")
        expected_cash = cash_before_mtm - expected_interest
        assert service.get_cash() == expected_cash

        # Check cumulative margin interest
        state = service.get_state()
        assert state.total_margin_interest == expected_interest

        # Check ledger entry
        entries = service.get_ledger()
        interest_entries = [e for e in entries if e.entry_type == LedgerEntryType.MARGIN_INTEREST]
        assert len(interest_entries) == 1
        assert interest_entries[0].cash_flow == -expected_interest

    def test_margin_interest_no_charge_on_positive_cash(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that positive cash doesn't incur margin interest."""
        # Cash is positive
        initial_cash = service.get_cash()
        assert initial_cash > 0

        # Mark to market - no margin interest
        eod = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod)

        # Cash should remain unchanged
        assert service.get_cash() == initial_cash

        # Check no margin interest
        state = service.get_state()
        assert state.total_margin_interest == Decimal("0")

    def test_margin_interest_accumulates(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that margin interest compounds over multiple days."""
        # Create negative cash
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("1000"),
            price=Decimal("150.00"),
            commission=Decimal("100.00"),
        )

        cash_after_buy = service.get_cash()
        assert cash_after_buy < 0

        # Day 1 mark to market
        eod_day1 = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod_day1)

        interest_day1 = abs(cash_after_buy) * Decimal("0.07") / Decimal("360")
        cash_after_day1 = cash_after_buy - interest_day1

        # Day 2 mark to market (interest on increased negative balance)
        eod_day2 = eod_day1.replace(day=16)
        service.mark_to_market(eod_day2)

        interest_day2 = abs(cash_after_day1) * Decimal("0.07") / Decimal("360")
        expected_cash_day2 = cash_after_day1 - interest_day2

        assert service.get_cash() == expected_cash_day2

        # Check cumulative interest
        state = service.get_state()
        assert state.total_margin_interest == interest_day1 + interest_day2


class TestMarkToMarket:
    """Test complete mark-to-market process."""

    def test_mark_to_market_combined_fees(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test mark-to-market with both borrow fees and margin interest."""
        # Create short position (incurs borrow fee)
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Buy large position to create negative cash (incurs margin interest)
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="MSFT",
            side="buy",
            quantity=Decimal("1200"),  # Increased to ensure negative cash
            price=Decimal("100.00"),
            commission=Decimal("100.00"),
        )

        # Update prices
        service.update_prices({"AAPL": Decimal("150.00"), "MSFT": Decimal("100.00")})

        cash_before = service.get_cash()
        assert cash_before < 0, f"Cash should be negative but is {cash_before}"

        # Mark to market
        eod = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod)

        # Calculate expected fees (note: borrow fee is applied first, then margin interest)
        # Borrow fee on AAPL short: $15,000 * 0.05 / 360
        borrow_fee = Decimal("15000") * Decimal("0.05") / Decimal("360")

        # After borrow fee, cash becomes more negative
        cash_after_borrow = cash_before - borrow_fee

        # Margin interest on negative cash AFTER borrow fee: abs(cash_after_borrow) * 0.07 / 360
        margin_interest = abs(cash_after_borrow) * Decimal("0.07") / Decimal("360")

        # Total fees
        total_fees = borrow_fee + margin_interest
        expected_cash = cash_before - total_fees

        assert service.get_cash() == expected_cash

        # Check cumulative tracking
        state = service.get_state()
        assert state.total_borrow_fees == borrow_fee
        assert state.total_margin_interest == margin_interest

        # Check ledger entries
        entries = service.get_ledger()
        borrow_entries = [e for e in entries if e.entry_type == LedgerEntryType.BORROW_FEE]
        interest_entries = [e for e in entries if e.entry_type == LedgerEntryType.MARGIN_INTEREST]
        assert len(borrow_entries) == 1
        assert len(interest_entries) == 1

    def test_mark_to_market_no_fees_all_positive(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test mark-to-market with no fees (long only, positive cash)."""
        # Buy position with plenty of cash
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        service.update_prices({"AAPL": Decimal("155.00")})

        cash_before = service.get_cash()
        assert cash_before > 0

        # Mark to market
        eod = timestamp.replace(hour=16, minute=0)
        service.mark_to_market(eod)

        # Cash should remain unchanged (no fees)
        assert service.get_cash() == cash_before

        # Check no fees
        state = service.get_state()
        assert state.total_borrow_fees == Decimal("0")
        assert state.total_margin_interest == Decimal("0")

    def test_mark_to_market_creates_ledger_entries(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that mark-to-market creates appropriate ledger entries."""
        # Short position
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        service.update_prices({"AAPL": Decimal("150.00")})

        # Mark to market multiple days
        eod_day1 = timestamp.replace(hour=16, minute=0)
        eod_day2 = eod_day1.replace(day=16)
        eod_day3 = eod_day1.replace(day=17)

        service.mark_to_market(eod_day1)
        service.mark_to_market(eod_day2)
        service.mark_to_market(eod_day3)

        # Should have 3 borrow fee entries
        entries = service.get_ledger()
        borrow_entries = [e for e in entries if e.entry_type == LedgerEntryType.BORROW_FEE]
        assert len(borrow_entries) == 3

        # Each should have unique entry_id
        entry_ids = [e.entry_id for e in borrow_entries]
        assert len(set(entry_ids)) == 3  # All unique
