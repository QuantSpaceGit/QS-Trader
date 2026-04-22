"""Unit tests for Phase 1 price-basis domain types and manifest contract."""

import json
from dataclasses import FrozenInstanceError
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.events.price_basis import BarView, PriceBasis
from qs_trader.libraries.strategies import BarView as StrategyBarView
from qs_trader.libraries.strategies import PositionState
from qs_trader.libraries.strategies import PriceBasis as StrategyPriceBasis
from qs_trader.services.reporting.manifest import ClickHouseInputManifest


class TestBarView:
    """Tests for the immutable strategy-facing bar view."""

    def test_bar_view_is_frozen(self) -> None:
        """BarView should be immutable once constructed."""
        bar = BarView(
            symbol="AAPL",
            timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
            open=Decimal("100.00"),
            high=Decimal("101.00"),
            low=Decimal("99.50"),
            close=Decimal("100.75"),
            volume=1_000,
            basis=PriceBasis.ADJUSTED,
        )

        with pytest.raises(FrozenInstanceError):
            setattr(bar, "close", Decimal("101.25"))

    def test_strategy_library_reexports_phase1_domain_types(self) -> None:
        """Strategies should be able to import Phase 1 types from a single package."""
        assert StrategyBarView is BarView
        assert StrategyPriceBasis is PriceBasis
        assert PositionState.OPEN_LONG.value == "open_long"


class TestManifestPriceBasisRoundTrip:
    """Tests for manifest JSON serialization of the Phase 1 price-basis contract."""

    def test_manifest_json_round_trip_preserves_price_basis(self) -> None:
        """Manifest JSON should round-trip `price_basis` without falling back to legacy fields."""
        manifest = ClickHouseInputManifest(
            source_name="qs-datamaster-equity-1d",
            database="market",
            bars_table="as_us_equity_ohlc_daily",
            symbols=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 31),
            price_basis=PriceBasis.RAW,
        )

        raw_json = manifest.to_json()
        payload = json.loads(raw_json)

        assert payload["price_basis"] == "raw"
        assert "strategy_adjustment_mode" not in payload
        assert "portfolio_adjustment_mode" not in payload

        round_tripped = ClickHouseInputManifest.from_json(raw_json)
        assert round_tripped.price_basis == PriceBasis.RAW
