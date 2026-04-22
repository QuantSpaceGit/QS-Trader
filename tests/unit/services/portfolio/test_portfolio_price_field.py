"""Test PortfolioService price-basis selection and dividend handling."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import PriceBarEvent
from qs_trader.events.price_basis import PriceBasis
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


class TestPortfolioConfigPriceBasis:
    """Test PortfolioConfig price_basis validation."""

    def test_default_price_basis_is_adjusted(self):
        """PortfolioConfig defaults to adjusted pricing."""
        config = PortfolioConfig(initial_cash=Decimal("100000"))

        assert config.price_basis == PriceBasis.ADJUSTED

    def test_can_configure_raw_price_basis(self):
        """PortfolioConfig accepts the runnable raw basis."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            price_basis="raw",
        )

        assert config.price_basis == PriceBasis.RAW

    def test_invalid_price_basis_raises_error(self):
        """PortfolioConfig rejects unsupported basis names."""
        with pytest.raises(ValueError, match="Unsupported price basis"):
            PortfolioConfig(
                initial_cash=Decimal("100000"),
                price_basis="invalid",
            )


class TestPortfolioServicePriceBasis:
    """Test PortfolioService mark-to-market price selection."""

    @pytest.mark.parametrize(
        ("price_basis", "expected_close"),
        [
            (PriceBasis.ADJUSTED, Decimal("105.50")),
            (PriceBasis.RAW, Decimal("100.50")),
        ],
    )
    def test_on_bar_updates_prices_using_selected_basis(self, event_bus, sample_bar, price_basis, expected_close):
        """on_bar should cache prices from the selected run-level basis."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            price_basis=price_basis,
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        portfolio.on_bar(sample_bar)

        assert portfolio._latest_prices["AAPL"] == expected_close


class TestPortfolioServiceDividendAccounting:
    """Test that dividend cash flows remain explicit regardless of selected basis."""

    @pytest.mark.parametrize("price_basis", [PriceBasis.ADJUSTED, PriceBasis.RAW])
    def test_process_dividend_adds_cash_regardless_of_basis(self, event_bus, price_basis):
        """Dividend cash accounting should not depend on the chosen bar basis."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            price_basis=price_basis,
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

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

        portfolio.process_dividend(
            symbol="AAPL",
            effective_date=datetime(2024, 1, 15, tzinfo=timezone.utc),
            amount_per_share=Decimal("0.82"),
        )

        assert portfolio._cash == initial_cash + Decimal("82.00")
        assert portfolio._total_dividends_received == Decimal("82.00")

    @pytest.mark.parametrize("price_basis", [PriceBasis.ADJUSTED, PriceBasis.RAW])
    def test_same_basis_is_used_for_cached_prices_and_mark_to_market(self, event_bus, sample_bar, price_basis):
        """Mark-to-market should use the same selected basis as the bar cache."""
        config = PortfolioConfig(
            initial_cash=Decimal("100000"),
            price_basis=price_basis,
        )
        portfolio = PortfolioService(config=config, event_bus=event_bus)

        portfolio.on_bar(sample_bar)
        portfolio._publish_portfolio_state(sample_bar.timestamp)

        expected_close = Decimal("105.50") if price_basis == PriceBasis.ADJUSTED else Decimal("100.50")
        assert portfolio._latest_prices["AAPL"] == expected_close
