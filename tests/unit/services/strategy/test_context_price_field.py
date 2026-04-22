"""Targeted basis-resolution tests for strategy Context price accessors."""

from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import PriceBarEvent
from qs_trader.events.price_basis import PriceBasis
from qs_trader.services.strategy.context import Context


@pytest.fixture
def event_bus() -> EventBus:
    """Create a real event bus for lightweight context tests."""
    return EventBus()


@pytest.fixture
def sample_bar() -> PriceBarEvent:
    """Create a sample bar with both raw and adjusted OHLC data."""
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


def test_get_price_uses_raw_or_adjusted_basis_consistently(event_bus: EventBus, sample_bar: PriceBarEvent) -> None:
    """Single-price lookups should align with the requested basis."""
    context = Context(strategy_id="test", event_bus=event_bus)
    context.cache_bar(sample_bar)

    assert context.get_price("AAPL", basis=PriceBasis.RAW) == Decimal("100.50")
    assert context.get_price("AAPL", basis=PriceBasis.ADJUSTED) == Decimal("105.50")


def test_get_bars_returns_basis_specific_ohlc_views(event_bus: EventBus, sample_bar: PriceBarEvent) -> None:
    """Bar views should expose basis-resolved OHLC values, not raw event fields."""
    context = Context(strategy_id="test", event_bus=event_bus)
    context.cache_bar(sample_bar)

    raw_bars = context.get_bars("AAPL", 1, basis=PriceBasis.RAW)
    adjusted_bars = context.get_bars("AAPL", 1, basis=PriceBasis.ADJUSTED)

    assert raw_bars is not None and adjusted_bars is not None
    assert raw_bars[0].open == Decimal("100.00")
    assert raw_bars[0].close == Decimal("100.50")
    assert adjusted_bars[0].open == Decimal("105.00")
    assert adjusted_bars[0].close == Decimal("105.50")


def test_adjusted_series_raises_when_any_adjusted_field_is_missing(event_bus: EventBus) -> None:
    """Adjusted series should fail loudly instead of silently mixing raw values."""
    context = Context(strategy_id="test", event_bus=event_bus)
    context.cache_bar(
        PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-01T16:00:00Z",
            open=Decimal("100.00"),
            high=Decimal("101.00"),
            low=Decimal("99.00"),
            close=Decimal("100.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
    )
    context.cache_bar(
        PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-02T16:00:00Z",
            open=Decimal("101.00"),
            high=Decimal("102.00"),
            low=Decimal("100.00"),
            close=Decimal("101.50"),
            close_adj=Decimal("106.50"),
            volume=1001,
            source="test",
            interval="1d",
        )
    )

    with pytest.raises(ValueError, match="Adjusted price requested"):
        context.get_price_series("AAPL", 2, basis=PriceBasis.ADJUSTED)


def test_offset_windows_preserve_exact_length(event_bus: EventBus) -> None:
    """Offset windows should return the exact previous slice strategies requested."""
    context = Context(strategy_id="test", event_bus=event_bus)
    for day in range(1, 6):
        context.cache_bar(
            PriceBarEvent(
                symbol="AAPL",
                timestamp=f"2024-01-{day:02d}T16:00:00Z",
                open=Decimal("100.00"),
                high=Decimal("101.00"),
                low=Decimal("99.00"),
                close=Decimal(str(100 + day)),
                close_adj=Decimal(str(200 + day)),
                volume=1000,
                source="test",
                interval="1d",
            )
        )

    assert context.get_price_series("AAPL", 2, basis=PriceBasis.ADJUSTED, offset=2) == [Decimal("202"), Decimal("203")]
