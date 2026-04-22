"""Unit tests for the explicit-basis strategy Context API."""

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from typing import cast
from unittest.mock import MagicMock
from uuid import NAMESPACE_DNS, uuid5

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import FillEvent, PriceBarEvent, SignalEvent
from qs_trader.events.price_basis import PriceBasis
from qs_trader.services.strategy.context import Context
from qs_trader.services.strategy.models import PositionState, SignalIntention


@pytest.fixture
def event_bus() -> MagicMock:
    """Create a mocked event bus."""
    return MagicMock(spec=EventBus)


@pytest.fixture
def context(event_bus: MagicMock) -> Context:
    """Create a strategy context backed by the mocked event bus."""
    return Context(strategy_id="test_strategy", event_bus=event_bus)


def make_bar(
    day: int,
    *,
    symbol: str = "AAPL",
    with_adjusted: bool = True,
) -> PriceBarEvent:
    """Build a synthetic daily bar with optional adjusted OHLC fields."""
    base_open = Decimal(str(100 + day))
    base_high = Decimal(str(101 + day))
    base_low = Decimal(str(99 + day))
    base_close = Decimal(str(100.5 + day))
    adjusted_offset = Decimal("5.0")

    if with_adjusted:
        return PriceBarEvent(
            symbol=symbol,
            timestamp=f"2024-01-{day:02d}T16:00:00Z",
            open=base_open,
            high=base_high,
            low=base_low,
            close=base_close,
            open_adj=base_open + adjusted_offset,
            high_adj=base_high + adjusted_offset,
            low_adj=base_low + adjusted_offset,
            close_adj=base_close + adjusted_offset,
            volume=1000 + day,
            source="test",
            interval="1d",
        )

    return PriceBarEvent(
        symbol=symbol,
        timestamp=f"2024-01-{day:02d}T16:00:00Z",
        open=base_open,
        high=base_high,
        low=base_low,
        close=base_close,
        volume=1000 + day,
        source="test",
        interval="1d",
    )


def make_fill(
    *,
    symbol: str = "AAPL",
    side: str,
    quantity: str,
    fill_id: str,
) -> FillEvent:
    """Build a fill event for position-state tests."""
    return FillEvent(
        fill_id=str(uuid5(NAMESPACE_DNS, fill_id)),
        source_order_id=f"order-{fill_id}",
        timestamp="2024-01-10T16:00:00Z",
        symbol=symbol,
        side=side,
        filled_quantity=Decimal(quantity),
        fill_price=Decimal("150.00"),
    )


class TestEmitSignal:
    """Signal emission behavior."""

    def test_emit_signal_publishes_signal_and_tracks_count(self, context: Context, event_bus: MagicMock) -> None:
        """Emitted signals are published immediately and counted."""
        result = context.emit_signal(
            timestamp="2024-01-02T16:00:00Z",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("150.25"),
            confidence=Decimal("0.85"),
        )

        assert isinstance(result, SignalEvent)
        assert result.strategy_id == "test_strategy"
        assert result.symbol == "AAPL"
        assert result.intention == SignalIntention.OPEN_LONG
        assert result.price == Decimal("150.25")
        assert result.confidence == Decimal("0.85")
        assert context.signal_count == 1
        event_bus.publish.assert_called_once_with(result)

    def test_emit_signal_converts_numeric_inputs_to_decimal(self, context: Context) -> None:
        """Float and string values are normalized to ``Decimal``."""
        result = context.emit_signal(
            timestamp="2024-01-02T16:00:00Z",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=150.25,
            confidence="0.85",
            stop_loss=145.0,
            take_profit="160.0",
        )

        assert result.price == Decimal("150.25")
        assert result.confidence == Decimal("0.85")
        assert result.stop_loss == Decimal("145.0")
        assert result.take_profit == Decimal("160.0")


class TestPriceAccess:
    """Price, bar, and series accessors."""

    def test_get_price_returns_none_when_symbol_has_no_cached_bars(self, context: Context) -> None:
        """Price lookups return ``None`` until bars are cached."""
        assert context.get_price("AAPL", basis=PriceBasis.ADJUSTED) is None

    @pytest.mark.parametrize(
        ("basis", "expected"),
        [
            (PriceBasis.RAW, Decimal("101.5")),
            (PriceBasis.ADJUSTED, Decimal("106.5")),
            ("adjusted", Decimal("106.5")),
        ],
    )
    def test_get_price_resolves_requested_basis(
        self,
        context: Context,
        basis: PriceBasis | str,
        expected: Decimal,
    ) -> None:
        """Latest-price lookup honors the requested basis."""
        context.cache_bar(make_bar(1))

        assert context.get_price("AAPL", basis=basis) == expected

    def test_get_price_raises_when_adjusted_field_is_missing(self, context: Context) -> None:
        """Adjusted lookups fail loudly when the cached bar lacks adjusted prices."""
        context.cache_bar(make_bar(1, with_adjusted=False))

        with pytest.raises(ValueError, match="Adjusted price requested"):
            context.get_price("AAPL", basis=PriceBasis.ADJUSTED)

    def test_get_bars_returns_chronological_bar_views_with_requested_basis(self, context: Context) -> None:
        """Historical bar lookups return immutable ``BarView`` objects."""
        for day in range(1, 4):
            context.cache_bar(make_bar(day))

        bars = context.get_bars("AAPL", 2, basis=PriceBasis.ADJUSTED)

        assert bars is not None
        assert len(bars) == 2
        assert bars[0].close == Decimal("107.5")
        assert bars[1].close == Decimal("108.5")
        assert bars[0].basis == PriceBasis.ADJUSTED
        assert bars[0].timestamp == datetime(2024, 1, 2, 16, 0, tzinfo=timezone.utc)
        assert bars[1].timestamp == datetime(2024, 1, 3, 16, 0, tzinfo=timezone.utc)

    def test_get_bars_returns_none_when_exact_window_is_unavailable(self, context: Context) -> None:
        """Bar access requires the exact requested window length."""
        context.cache_bar(make_bar(1))
        context.cache_bar(make_bar(2))

        assert context.get_bars("AAPL", 3, basis=PriceBasis.RAW) is None

    def test_get_price_series_respects_offset_for_prior_windows(self, context: Context) -> None:
        """Offset-based windows avoid manual slicing mistakes in strategies."""
        for day in range(1, 6):
            context.cache_bar(make_bar(day))

        current_window = context.get_price_series("AAPL", 3, basis=PriceBasis.ADJUSTED, offset=0)
        prior_window = context.get_price_series("AAPL", 3, basis=PriceBasis.ADJUSTED, offset=1)

        assert current_window == [Decimal("108.5"), Decimal("109.5"), Decimal("110.5")]
        assert prior_window == [Decimal("107.5"), Decimal("108.5"), Decimal("109.5")]

    def test_get_price_series_returns_none_when_offset_pushes_window_out_of_range(self, context: Context) -> None:
        """Offset still requires an exact window length after skipping recent bars."""
        for day in range(1, 4):
            context.cache_bar(make_bar(day))

        assert context.get_price_series("AAPL", 3, basis=PriceBasis.RAW, offset=1) is None

    @pytest.mark.parametrize(
        ("n", "offset", "message"),
        [
            (0, 0, "n must be positive"),
            (2, -1, "offset must be non-negative"),
        ],
    )
    def test_get_price_series_validates_window_arguments(
        self,
        context: Context,
        n: int,
        offset: int,
        message: str,
    ) -> None:
        """Invalid window parameters fail fast with a clear error."""
        with pytest.raises(ValueError, match=message):
            context.get_price_series("AAPL", n, basis=PriceBasis.ADJUSTED, offset=offset)


class TestPositionState:
    """Fill-backed position-state behavior."""

    def test_get_position_state_defaults_to_flat(self, context: Context) -> None:
        """Symbols start flat before any fills are recorded."""
        assert context.get_position_state("AAPL") == PositionState.FLAT

    def test_record_fill_tracks_long_flat_and_short_transitions(self, context: Context) -> None:
        """Position state follows the signed net filled quantity per symbol."""
        context.record_fill(make_fill(side="buy", quantity="10", fill_id="fill-1"))
        assert context.get_position_state("AAPL") == PositionState.OPEN_LONG

        context.record_fill(make_fill(side="sell", quantity="10", fill_id="fill-2"))
        assert context.get_position_state("AAPL") == PositionState.FLAT

        context.record_fill(make_fill(side="sell", quantity="5", fill_id="fill-3"))
        assert context.get_position_state("AAPL") == PositionState.OPEN_SHORT

    def test_record_fill_rejects_unknown_sides(self, context: Context) -> None:
        """Unexpected fill directions fail loudly instead of corrupting state."""
        invalid_fill = SimpleNamespace(
            symbol="AAPL",
            side="hold",
            filled_quantity=Decimal("1"),
        )

        with pytest.raises(ValueError, match="Unsupported fill side"):
            context.record_fill(cast(FillEvent, invalid_fill))


class TestCacheBehavior:
    """Rolling cache behavior."""

    def test_cache_bar_respects_max_bars_limit(self, event_bus: MagicMock) -> None:
        """Older bars are evicted automatically once the cache reaches capacity."""
        limited_context = Context(strategy_id="limited", event_bus=event_bus, max_bars=3)
        for day in range(1, 6):
            limited_context.cache_bar(make_bar(day))

        bars = limited_context.get_bars("AAPL", 3, basis=PriceBasis.RAW)

        assert bars is not None
        assert [bar.close for bar in bars] == [Decimal("103.5"), Decimal("104.5"), Decimal("105.5")]
