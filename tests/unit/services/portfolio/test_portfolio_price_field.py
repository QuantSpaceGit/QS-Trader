"""Test PortfolioService price field configuration and dividend handling."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import PriceBarEvent
from qs_trader.services.portfolio.models import PortfolioConfig
from qs_trader.services.portfolio.service import PortfolioService


@pytest.fixture
def event_bus():
    """Create event bus for testing."""
    return EventBus()


@pytest.fixture
def sample_bar():
    """Create sample bar with both base and adjusted close values."""
    return PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),
        open_adj=Decimal("105.00"),
        high_adj=Decimal("106.00"),
        low_adj=Decimal("104.00"),
        close_adj=Decimal("105.50"),
        volume=1000,
        source="test",
        interval="1d",
    )


class TestPortfolioConfigAdjustmentMode:
    """Test PortfolioConfig adjustment_mode validation."""

    def test_default_adjustment_mode_is_split_adjusted(self):
        """PortfolioConfig defaults to 'split_adjusted'."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
        )
        assert config.adjustment_mode == "split_adjusted"

    def test_can_configure_total_return(self):
        """PortfolioConfig can be set to 'total_return'."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            adjustment_mode="total_return",
        )
        assert config.adjustment_mode == "total_return"

    def test_invalid_adjustment_mode_raises_error(self):
        """PortfolioConfig validates adjustment_mode values."""
        with pytest.raises(ValueError, match="adjustment_mode must be one of"):
            PortfolioConfig(
                initial_cash=Decimal("100000"),
                adjustment_mode="invalid",
            )


class TestPortfolioServiceSplitAdjusted:
    """Test PortfolioService with adjustment_mode='split_adjusted'."""

    def test_on_bar_updates_prices_with_adjusted_close(self, event_bus, sample_bar):
        """on_bar updates prices using close_adj when adjustment_mode='split_adjusted'."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            adjustment_mode="split_adjusted",
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        portfolio.on_bar(sample_bar)

        assert portfolio._latest_prices["AAPL"] == Decimal("105.50")

    def test_process_dividend_with_split_adjusted_adds_cash(self, event_bus):
        """process_dividend adds cash when adjustment_mode='split_adjusted'."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            adjustment_mode="split_adjusted",
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        # Open long position
        portfolio.apply_fill(
            fill_id="fill-001",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("100.00"),
            commission=Decimal("1.00"),
        )

        initial_cash = portfolio._cash

        # Process dividend
        portfolio.process_dividend(
            symbol="AAPL",
            effective_date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            amount_per_share=Decimal("0.82"),
        )

        # Cash should increase by 100 shares * $0.82 = $82
        assert portfolio._cash == initial_cash + Decimal("82.00")
        assert portfolio._total_dividends_received == Decimal("82.00")


class TestPortfolioServiceTotalReturn:
    """Test PortfolioService with adjustment_mode='total_return'."""

    def test_on_bar_updates_prices_with_close_adj(self, event_bus, sample_bar):
        """on_bar updates prices using 'close_adj' when adjustment_mode='total_return'."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            adjustment_mode="total_return",
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        portfolio.on_bar(sample_bar)

        assert portfolio._latest_prices["AAPL"] == Decimal("105.50")

    def test_process_dividend_with_total_return_skips_cash_in(self, event_bus):
        """process_dividend skips cash-in when adjustment_mode='total_return'."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            adjustment_mode="total_return",
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        # Open long position
        portfolio.apply_fill(
            fill_id="fill-001",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("100.00"),
            commission=Decimal("1.00"),
        )

        initial_cash = portfolio._cash

        # Process dividend
        portfolio.process_dividend(
            symbol="AAPL",
            effective_date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            amount_per_share=Decimal("0.82"),
        )

        # Cash should NOT change (total_return mode skips dividends)
        assert portfolio._cash == initial_cash
        assert portfolio._total_dividends_received == Decimal("0.00")

    def test_dividend_skip_logged_with_info_level(self, event_bus, caplog):
        """Dividend skip is logged at debug level when using total_return."""
        import logging

        caplog.set_level(logging.DEBUG)

        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            adjustment_mode="total_return",
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        # Open long position
        portfolio.apply_fill(
            fill_id="fill-001",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("100.00"),
            commission=Decimal("1.00"),
        )

        # Process dividend
        portfolio.process_dividend(
            symbol="AAPL",
            effective_date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            amount_per_share=Decimal("0.82"),
        )

        # Check log contains skip message
        assert "dividend_skipped" in caplog.text.lower() or "total_return" in caplog.text


class TestPortfolioServiceAdjustmentModeConsistency:
    """Test adjustment mode consistency across portfolio operations."""

    def test_same_adjustment_mode_for_bars_and_mark_to_market(self, event_bus, sample_bar):
        """Portfolio uses same adjustment mode for on_bar and mark-to-market."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            adjustment_mode="total_return",
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        # Process bar
        portfolio.on_bar(sample_bar)

        # Cached price should be close_adj
        assert portfolio._latest_prices["AAPL"] == Decimal("105.50")

        # Mark to market uses cached prices
        portfolio._publish_portfolio_state(sample_bar.timestamp)
        # If this doesn't raise, mark-to-market uses cached close_adj prices


class TestPortfolioDividendDoubleCounting:
    """Test that dividends are not double-counted."""

    def test_no_double_counting_with_total_return(self, event_bus):
        """Using total_return adjustment mode prevents dividend double-counting."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            adjustment_mode="total_return",
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        # Buy 100 shares at $100
        portfolio.apply_fill(
            fill_id="fill-001",
            timestamp=datetime(2024, 1, 1, tzinfo=timezone.utc),
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("100.00"),
            commission=Decimal("0.00"),
        )

        # Cash after buy: 100000 - 10000 = 90000
        assert portfolio._cash == Decimal("90000.00")

        # Process dividend (should be skipped)
        portfolio.process_dividend(
            symbol="AAPL",
            effective_date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            amount_per_share=Decimal("0.82"),
        )

        # Cash unchanged (dividend already in price appreciation via close_adj)
        assert portfolio._cash == Decimal("90000.00")

        # Process bar with close_adj reflecting dividend
        bar = PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-15T16:00:00Z",
            open=Decimal("100.00"),
            high=Decimal("101.00"),
            low=Decimal("99.00"),
            close=Decimal("99.18"),  # Dropped by $0.82 dividend
            open_adj=Decimal("100.00"),
            high_adj=Decimal("101.00"),
            low_adj=Decimal("99.00"),
            close_adj=Decimal("100.00"),  # Smooth, no drop
            volume=1000,
            source="test",
            interval="1d",
        )
        portfolio.on_bar(bar)

        # Position valued at close_adj ($100), dividend already reflected
        position = portfolio.get_position("AAPL")
        assert position is not None
        assert position.market_value == Decimal("10000.00")  # 100 shares * $100

        # Total equity = cash + market_value = 90000 + 10000 = 100000
        # No double counting - dividend effect only in price appreciation
