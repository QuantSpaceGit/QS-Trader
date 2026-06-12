"""Unit tests for Context identity accessors (Task Group 6).

Verifies get_instrument, get_security_id, and get_display_symbol work
correctly with and without an InstrumentResolver.
"""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import PriceBarEvent
from qs_trader.services.data.models import IdentitySource
from qs_trader.services.strategy.context import Context


@pytest.fixture
def event_bus() -> MagicMock:
    return MagicMock(spec=EventBus)


@pytest.fixture
def context(event_bus: MagicMock) -> Context:
    return Context(strategy_id="test_strategy", event_bus=event_bus)


def make_bar(day: int, symbol: str = "AAPL") -> PriceBarEvent:
    return PriceBarEvent(
        symbol=symbol,
        timestamp=f"2024-01-{day:02d}T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),
        volume=1000,
        source="test",
        interval="1d",
    )


# ---------------------------------------------------------------------------
# Without InstrumentResolver (fallback behavior)
# ---------------------------------------------------------------------------


class TestIdentityAccessorsNoResolver:
    """Identity accessors should work without an InstrumentResolver."""

    def test_get_instrument_returns_bare_instrument(self, context: Context) -> None:
        """Without a resolver, get_instrument returns a minimal Instrument."""
        instrument = context.get_instrument("AAPL")

        assert instrument is not None
        assert instrument.symbol == "AAPL"
        assert instrument.secid is None
        assert instrument.display_symbol is None
        assert instrument.ticker_at_date is None
        assert instrument.identity_source is None

    def test_get_security_id_returns_none(self, context: Context) -> None:
        """Without a resolver, get_security_id returns None."""
        assert context.get_security_id("AAPL") is None

    def test_get_display_symbol_returns_symbol(self, context: Context) -> None:
        """Without a resolver, get_display_symbol returns the input symbol."""
        assert context.get_display_symbol("AAPL") == "AAPL"
        assert context.get_display_symbol("AAPL", date="2024-01-15") == "AAPL"


# ---------------------------------------------------------------------------
# With InstrumentResolver (mocked)
# ---------------------------------------------------------------------------


class TestIdentityAccessorsWithResolver:
    """Identity accessors should use the InstrumentResolver when available."""

    def _make_resolved(self) -> MagicMock:
        resolved = MagicMock()
        resolved.secid = 3513095
        resolved.display_symbol = "META"
        resolved.ticker_at_date = "FB"
        resolved.identity_source = IdentitySource.TICKER_POINT_IN_TIME
        return resolved

    def test_get_instrument_uses_resolver(self, event_bus: MagicMock) -> None:
        """get_instrument should call the resolver and return an Instrument."""
        resolver = MagicMock()
        resolver.resolve_by_ticker.return_value = self._make_resolved()

        ctx = Context(
            strategy_id="test_strategy",
            event_bus=event_bus,
            instrument_resolver=resolver,
        )
        ctx.cache_bar(make_bar(1, symbol="FB"))
        ctx.cache_bar(make_bar(10, symbol="FB"))

        instrument = ctx.get_instrument("FB")

        assert instrument is not None
        assert instrument.secid == 3513095
        assert instrument.display_symbol == "META"
        assert instrument.ticker_at_date == "FB"
        assert instrument.identity_source == IdentitySource.TICKER_POINT_IN_TIME
        resolver.resolve_by_ticker.assert_called_once()

    def test_get_instrument_fallback_on_resolver_error(self, event_bus: MagicMock) -> None:
        """Resolver failures should not break strategy execution."""
        resolver = MagicMock()
        resolver.resolve_by_ticker.side_effect = Exception("secmaster down")

        ctx = Context(
            strategy_id="test_strategy",
            event_bus=event_bus,
            instrument_resolver=resolver,
        )

        instrument = ctx.get_instrument("AAPL")

        assert instrument is not None
        assert instrument.symbol == "AAPL"
        assert instrument.secid is None

    def test_get_instrument_uses_bar_cache_for_date_range(self, event_bus: MagicMock) -> None:
        """Resolver should be called with date range inferred from bar cache."""
        resolver = MagicMock()
        resolver.resolve_by_ticker.return_value = self._make_resolved()

        ctx = Context(
            strategy_id="test_strategy",
            event_bus=event_bus,
            instrument_resolver=resolver,
        )
        ctx.cache_bar(make_bar(1, symbol="FB"))
        ctx.cache_bar(make_bar(15, symbol="FB"))

        ctx.get_instrument("FB")

        call_args = resolver.resolve_by_ticker.call_args
        date_range = call_args.kwargs.get("date_range") or call_args.args[1]
        assert date_range == (date(2024, 1, 1), date(2024, 1, 15))

    def test_get_instrument_default_date_range_when_no_bars(self, event_bus: MagicMock) -> None:
        """When no bars are cached, a wide default date range is used."""
        resolver = MagicMock()
        resolver.resolve_by_ticker.return_value = self._make_resolved()

        ctx = Context(
            strategy_id="test_strategy",
            event_bus=event_bus,
            instrument_resolver=resolver,
        )

        ctx.get_instrument("FB")

        call_args = resolver.resolve_by_ticker.call_args
        date_range = call_args.kwargs.get("date_range") or call_args.args[1]
        assert date_range == (date(2000, 1, 1), date(2030, 12, 31))

    def test_get_security_id_returns_resolved_secid(self, event_bus: MagicMock) -> None:
        """get_security_id should return the secid from the resolved instrument."""
        resolver = MagicMock()
        resolver.resolve_by_ticker.return_value = self._make_resolved()

        ctx = Context(
            strategy_id="test_strategy",
            event_bus=event_bus,
            instrument_resolver=resolver,
        )
        ctx.cache_bar(make_bar(1, symbol="FB"))

        secid = ctx.get_security_id("FB")

        assert secid == 3513095

    def test_get_display_symbol_returns_display_symbol(self, event_bus: MagicMock) -> None:
        """get_display_symbol should return the resolved display_symbol."""
        resolver = MagicMock()
        resolver.resolve_by_ticker.return_value = self._make_resolved()

        ctx = Context(
            strategy_id="test_strategy",
            event_bus=event_bus,
            instrument_resolver=resolver,
        )
        ctx.cache_bar(make_bar(1, symbol="FB"))

        display = ctx.get_display_symbol("FB")

        assert display == "META"

    def test_get_display_symbol_with_date_prefers_ticker_at_date(self, event_bus: MagicMock) -> None:
        """When a date is given and ticker_at_date is set, prefer it."""
        resolver = MagicMock()
        resolved = self._make_resolved()
        resolved.ticker_at_date = "FB"
        resolver.resolve_by_ticker.return_value = resolved

        ctx = Context(
            strategy_id="test_strategy",
            event_bus=event_bus,
            instrument_resolver=resolver,
        )
        ctx.cache_bar(make_bar(1, symbol="FB"))

        display = ctx.get_display_symbol("FB", date="2012-06-01")

        assert display == "FB"

    def test_get_display_symbol_falls_back_to_symbol(self, event_bus: MagicMock) -> None:
        """When no display_symbol is resolved, fall back to the input symbol."""
        resolver = MagicMock()
        resolved = self._make_resolved()
        resolved.display_symbol = None
        resolved.ticker_at_date = None
        resolver.resolve_by_ticker.return_value = resolved

        ctx = Context(
            strategy_id="test_strategy",
            event_bus=event_bus,
            instrument_resolver=resolver,
        )
        ctx.cache_bar(make_bar(1, symbol="UNKNOWN"))

        display = ctx.get_display_symbol("UNKNOWN")

        assert display == "UNKNOWN"
