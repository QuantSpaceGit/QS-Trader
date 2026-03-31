"""Unit tests for corporate actions (splits, dividends) - Week 3."""

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


class TestStockSplits:
    """Test stock split processing."""

    def test_split_long_position_regular(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test regular split (4-for-1) on long position."""
        # Open position: Buy 100 @ $400
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("400.00"),
            commission=Decimal("10.00"),
        )

        initial_value = Decimal("40000.00")  # 100 * $400
        position_before = service.get_position("AAPL")
        assert position_before is not None
        assert position_before.quantity == Decimal("100")
        assert position_before.avg_price == Decimal("400.00")

        # Process 4-for-1 split
        split_date = timestamp.replace(day=16)
        service.process_split(symbol="AAPL", split_date=split_date, ratio=Decimal("4.0"))

        # After split: 400 shares @ $100
        position_after = service.get_position("AAPL")
        assert position_after is not None
        assert position_after.quantity == Decimal("400")
        assert position_after.avg_price == Decimal("100.00")

        # Value should be preserved
        assert position_after.total_cost == initial_value

        # Check ledger entry
        entries = service.get_ledger()
        split_entries = [e for e in entries if e.entry_type == LedgerEntryType.SPLIT]
        assert len(split_entries) == 1
        assert split_entries[0].symbol == "AAPL"
        assert split_entries[0].cash_flow == Decimal("0")  # No cash impact

    def test_split_short_position_regular(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test regular split (2-for-1) on short position."""
        # Open short position: Sell 50 @ $200
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="TSLA",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("200.00"),
            commission=Decimal("5.00"),
        )

        initial_value = Decimal("-10000.00")  # -50 * $200
        position_before = service.get_position("TSLA")
        assert position_before is not None
        assert position_before.quantity == Decimal("-50")
        assert position_before.avg_price == Decimal("200.00")

        # Process 2-for-1 split
        split_date = timestamp.replace(day=16)
        service.process_split(symbol="TSLA", split_date=split_date, ratio=Decimal("2.0"))

        # After split: -100 shares @ $100
        position_after = service.get_position("TSLA")
        assert position_after is not None
        assert position_after.quantity == Decimal("-100")
        assert position_after.avg_price == Decimal("100.00")

        # Value should be preserved
        assert position_after.total_cost == initial_value

    def test_reverse_split(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test reverse split (1-for-4) on long position."""
        # Open position: Buy 400 @ $10
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="COIN",
            side="buy",
            quantity=Decimal("400"),
            price=Decimal("10.00"),
            commission=Decimal("10.00"),
        )

        initial_value = Decimal("4000.00")  # 400 * $10
        position_before = service.get_position("COIN")
        assert position_before is not None
        assert position_before.quantity == Decimal("400")
        assert position_before.avg_price == Decimal("10.00")

        # Process 1-for-4 reverse split (ratio = 0.25)
        split_date = timestamp.replace(day=16)
        service.process_split(symbol="COIN", split_date=split_date, ratio=Decimal("0.25"))

        # After split: 100 shares @ $40
        position_after = service.get_position("COIN")
        assert position_after is not None
        assert position_after.quantity == Decimal("100")
        assert position_after.avg_price == Decimal("40.00")

        # Value should be preserved
        assert position_after.total_cost == initial_value

    def test_split_with_multiple_lots(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test split adjusts all lots correctly."""
        # Buy at two different prices
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )
        service.apply_fill(
            fill_id="buy_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("160.00"),
            commission=Decimal("5.00"),
        )

        # Total: 150 shares, avg price = (15000 + 8000) / 150 = $153.33
        position_before = service.get_position("AAPL")
        assert position_before is not None
        assert position_before.quantity == Decimal("150")
        assert len(position_before.lots) == 2

        # Process 2-for-1 split
        split_date = timestamp.replace(day=16)
        service.process_split(symbol="AAPL", split_date=split_date, ratio=Decimal("2.0"))

        # After split: 300 shares
        position_after = service.get_position("AAPL")
        assert position_after is not None
        assert position_after.quantity == Decimal("300")
        assert len(position_after.lots) == 2  # Still 2 lots

        # Check individual lots adjusted
        lot1, lot2 = position_after.lots
        assert lot1.quantity == Decimal("200")  # Was 100
        assert lot1.entry_price == Decimal("75.00")  # Was $150
        assert lot2.quantity == Decimal("100")  # Was 50
        assert lot2.entry_price == Decimal("80.00")  # Was $160

    def test_split_invalid_ratio_zero(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that zero ratio raises ValueError."""
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        with pytest.raises(ValueError, match="ratio must be positive"):
            service.process_split(symbol="AAPL", split_date=timestamp, ratio=Decimal("0"))

    def test_split_invalid_ratio_negative(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that negative ratio raises ValueError."""
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        with pytest.raises(ValueError, match="ratio must be positive"):
            service.process_split(symbol="AAPL", split_date=timestamp, ratio=Decimal("-2.0"))

    def test_split_no_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that split on non-existent position is silently ignored."""
        # Should not raise, just log and return
        service.process_split(symbol="AAPL", split_date=timestamp, ratio=Decimal("2.0"))

        # Verify no position created
        assert service.get_position("AAPL") is None


class TestDividends:
    """Test dividend processing."""

    def test_dividend_long_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test dividend payment on long position (cash in)."""
        # Open position: Buy 100 @ $150
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

        # Process dividend: $0.82 per share
        effective_date = timestamp.replace(day=20)
        service.process_dividend(symbol="AAPL", effective_date=effective_date, amount_per_share=Decimal("0.82"))

        # Cash should increase by $82 (100 * $0.82)
        expected_cash = initial_cash + Decimal("82.00")
        assert service.get_cash() == expected_cash

        # Check cumulative dividends received
        state = service.get_state()
        assert state.total_dividends_received == Decimal("82.00")
        assert state.total_dividends_paid == Decimal("0")

        # Check ledger entry
        entries = service.get_ledger()
        div_entries = [e for e in entries if e.entry_type == LedgerEntryType.DIVIDEND]
        assert len(div_entries) == 1
        assert div_entries[0].symbol == "AAPL"
        assert div_entries[0].cash_flow == Decimal("82.00")

    def test_dividend_short_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test dividend payment on short position (cash out)."""
        # Open short position: Sell 50 @ $200
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="TSLA",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("200.00"),
            commission=Decimal("5.00"),
        )

        initial_cash = service.get_cash()

        # Process dividend: $1.50 per share (short position pays)
        effective_date = timestamp.replace(day=20)
        service.process_dividend(symbol="TSLA", effective_date=effective_date, amount_per_share=Decimal("1.50"))

        # Cash should decrease by $75 (-50 * $1.50 = -$75)
        expected_cash = initial_cash - Decimal("75.00")
        assert service.get_cash() == expected_cash

        # Check cumulative dividends paid
        state = service.get_state()
        assert state.total_dividends_received == Decimal("0")
        assert state.total_dividends_paid == Decimal("75.00")

        # Check ledger entry
        entries = service.get_ledger()
        div_entries = [e for e in entries if e.entry_type == LedgerEntryType.DIVIDEND]
        assert len(div_entries) == 1
        assert div_entries[0].symbol == "TSLA"
        assert div_entries[0].cash_flow == Decimal("-75.00")

    def test_dividend_multiple_symbols(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test dividends on multiple symbols."""
        # Buy AAPL
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        # Buy MSFT
        service.apply_fill(
            fill_id="buy_002",
            timestamp=timestamp,
            symbol="MSFT",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("300.00"),
            commission=Decimal("5.00"),
        )

        initial_cash = service.get_cash()

        # Process dividends
        effective_date = timestamp.replace(day=20)
        service.process_dividend(symbol="AAPL", effective_date=effective_date, amount_per_share=Decimal("0.82"))
        service.process_dividend(symbol="MSFT", effective_date=effective_date, amount_per_share=Decimal("2.24"))

        # Total dividends: $82 (AAPL) + $112 (MSFT) = $194
        expected_cash = initial_cash + Decimal("194.00")
        assert service.get_cash() == expected_cash

        # Check cumulative
        state = service.get_state()
        assert state.total_dividends_received == Decimal("194.00")

    def test_dividend_invalid_negative_amount(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that negative dividend amount raises ValueError."""
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        with pytest.raises(ValueError, match="cannot be negative"):
            service.process_dividend(symbol="AAPL", effective_date=timestamp, amount_per_share=Decimal("-0.82"))

    def test_dividend_no_position(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that dividend on non-existent position is silently ignored."""
        initial_cash = service.get_cash()

        # Should not raise, just log and return
        service.process_dividend(symbol="AAPL", effective_date=timestamp, amount_per_share=Decimal("0.82"))

        # Verify cash unchanged
        assert service.get_cash() == initial_cash
