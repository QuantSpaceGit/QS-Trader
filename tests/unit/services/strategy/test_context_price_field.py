"""Test Context price field configuration."""

from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import PriceBarEvent
from qs_trader.services.strategy.context import Context


@pytest.fixture
def event_bus():
    """Create event bus for testing."""
    return EventBus()


@pytest.fixture
def sample_bar():
    """Create sample bar with both close and close_adj."""
    return PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),  # Split-adjusted
        open_adj=Decimal("105.00"),
        high_adj=Decimal("106.00"),
        low_adj=Decimal("104.00"),
        close_adj=Decimal("105.50"),  # Total-return adjusted
        volume=1000,
        source="test",
        interval="1d",
    )


class TestContextSplitAdjusted:
    """Test Context with default adjustment_mode='split_adjusted'."""

    def test_default_adjustment_mode_is_split_adjusted(self, event_bus):
        """Context defaults to 'split_adjusted' adjustment mode."""
        context = Context(strategy_id="test", event_bus=event_bus)
        assert context._adjustment_mode == "split_adjusted"

    def test_get_price_returns_close(self, event_bus, sample_bar):
        """get_price returns close (split-adjusted) when adjustment_mode='split_adjusted'."""
        context = Context(strategy_id="test", event_bus=event_bus, adjustment_mode="split_adjusted")
        context.cache_bar(sample_bar)

        price = context.get_price("AAPL")

        assert price == Decimal("100.50")  # close, not close_adj

    def test_get_price_series_returns_close_values(self, event_bus, sample_bar):
        """get_price_series returns close prices when adjustment_mode='split_adjusted'."""
        context = Context(strategy_id="test", event_bus=event_bus, adjustment_mode="split_adjusted")

        # Cache multiple bars
        for i in range(5):
            bar = PriceBarEvent(
                symbol="AAPL",
                timestamp=f"2024-01-0{i + 1}T16:00:00Z",
                open=Decimal("100.00"),
                high=Decimal("101.00"),
                low=Decimal("99.00"),
                close=Decimal(f"{100 + i}.00"),  # 100, 101, 102, 103, 104
                open_adj=Decimal("105.00"),
                high_adj=Decimal("106.00"),
                low_adj=Decimal("104.00"),
                close_adj=Decimal(f"{105 + i}.00"),  # 105, 106, 107, 108, 109
                volume=1000,
                source="test",
                interval="1d",
            )
            context.cache_bar(bar)

        prices = context.get_price_series("AAPL", n=5)

        assert len(prices) == 5
        assert prices == [Decimal("100.00"), Decimal("101.00"), Decimal("102.00"), Decimal("103.00"), Decimal("104.00")]


class TestContextTotalReturn:
    """Test Context with adjustment_mode='total_return'."""

    def test_can_configure_total_return_adjustment_mode(self, event_bus):
        """Context can be configured to use 'total_return'."""
        context = Context(strategy_id="test", event_bus=event_bus, adjustment_mode="total_return")
        assert context._adjustment_mode == "total_return"

    def test_get_price_returns_close_adj(self, event_bus, sample_bar):
        """get_price returns close_adj (total-return) when adjustment_mode='total_return'."""
        context = Context(strategy_id="test", event_bus=event_bus, adjustment_mode="total_return")
        context.cache_bar(sample_bar)

        price = context.get_price("AAPL")

        assert price == Decimal("105.50")  # close_adj, not close

    def test_get_price_series_returns_close_adj_values(self, event_bus, sample_bar):
        """get_price_series returns close_adj prices when adjustment_mode='total_return'."""
        context = Context(strategy_id="test", event_bus=event_bus, adjustment_mode="total_return")

        # Cache multiple bars
        for i in range(5):
            bar = PriceBarEvent(
                symbol="AAPL",
                timestamp=f"2024-01-0{i + 1}T16:00:00Z",
                open=Decimal("100.00"),
                high=Decimal("101.00"),
                low=Decimal("99.00"),
                close=Decimal(f"{100 + i}.00"),  # 100, 101, 102, 103, 104
                open_adj=Decimal("105.00"),
                high_adj=Decimal("106.00"),
                low_adj=Decimal("104.00"),
                close_adj=Decimal(f"{105 + i}.00"),  # 105, 106, 107, 108, 109
                volume=1000,
                source="test",
                interval="1d",
            )
            context.cache_bar(bar)

        prices = context.get_price_series("AAPL", n=5)

        assert len(prices) == 5
        assert prices == [Decimal("105.00"), Decimal("106.00"), Decimal("107.00"), Decimal("108.00"), Decimal("109.00")]

    def test_get_bars_still_returns_full_price_bar_events(self, event_bus, sample_bar):
        """get_bars returns full PriceBarEvent objects (not filtered by adjustment_mode)."""
        context = Context(strategy_id="test", event_bus=event_bus, adjustment_mode="total_return")
        context.cache_bar(sample_bar)

        bars = context.get_bars("AAPL", n=1)

        assert len(bars) == 1
        assert bars[0].close == Decimal("100.50")  # Both fields still available
        assert bars[0].close_adj == Decimal("105.50")


class TestContextAdjustmentModeConsistency:
    """Test adjustment mode consistency across Context methods."""

    def test_get_price_and_get_price_series_use_same_field(self, event_bus):
        """get_price and get_price_series use the same configured adjustment mode."""
        context = Context(strategy_id="test", event_bus=event_bus, adjustment_mode="total_return")

        # Cache bar
        bar = PriceBarEvent(
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
        context.cache_bar(bar)

        # Both should return close_adj
        single_price = context.get_price("AAPL")
        series_prices = context.get_price_series("AAPL", n=1)

        assert single_price == Decimal("105.50")
        assert series_prices == [Decimal("105.50")]
        assert single_price == series_prices[0]
