"""Tests for ReportingService feature-gate (feature_enabled flag).

Verifies that bar buffering and feature_bar subscriptions are activated only
when feature_enabled=True, preventing OHLCV deduplication into DuckDB on
ordinary (non-feature) backtest runs.
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import FeatureBarEvent, PriceBarEvent
from qs_trader.services.reporting.service import ReportingService


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_price_bar_event(symbol: str = "AAPL", timestamp: str = "2024-01-02T21:00:00+00:00") -> PriceBarEvent:
    return PriceBarEvent(
        symbol=symbol,
        asset_class="equity",
        interval="1d",
        timestamp=timestamp,
        open=Decimal("185.00"),
        high=Decimal("186.00"),
        low=Decimal("184.00"),
        close=Decimal("185.50"),
        volume=1_000_000,
        source="test",
    )


def _make_feature_bar_event(
    symbol: str = "AAPL", timestamp: str = "2024-01-02T21:00:00+00:00"
) -> FeatureBarEvent:
    return FeatureBarEvent(
        timestamp=timestamp,
        symbol=symbol,
        features={"trend_strength": 0.8, "trend_regime": "bull"},
        feature_set_version="v1",
    )


def _make_reporting_service(feature_enabled: bool) -> ReportingService:
    """Create a minimal ReportingService with a live EventBus."""
    event_bus = EventBus()
    svc = ReportingService(event_bus=event_bus, feature_enabled=feature_enabled)
    return svc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestReportingServiceFeatureGate:
    """ReportingService bar-buffering gated behind feature_enabled."""

    def test_bar_not_buffered_when_feature_disabled(self) -> None:
        """Publishing a PriceBarEvent should NOT populate _bar_rows when feature_enabled=False."""
        svc = _make_reporting_service(feature_enabled=False)
        event = _make_price_bar_event()
        svc.event_bus.publish(event)
        assert svc._bar_rows == {}

    def test_feature_bar_not_buffered_when_feature_disabled(self) -> None:
        """Publishing a FeatureBarEvent should NOT affect _bar_rows when feature_enabled=False."""
        svc = _make_reporting_service(feature_enabled=False)
        svc.event_bus.publish(_make_price_bar_event())
        svc.event_bus.publish(_make_feature_bar_event())
        assert svc._bar_rows == {}

    def test_bar_buffered_when_feature_enabled(self) -> None:
        """Publishing a PriceBarEvent SHOULD populate _bar_rows when feature_enabled=True."""
        svc = _make_reporting_service(feature_enabled=True)
        event = _make_price_bar_event()
        svc.event_bus.publish(event)
        assert len(svc._bar_rows) == 1
        key = ("AAPL", "2024-01-02T21:00:00+00:00")
        row = svc._bar_rows[key]
        assert row["symbol"] == "AAPL"
        assert row["close"] == pytest.approx(185.50)
        assert row["features"] is None  # not yet merged

    def test_feature_bar_merged_into_buffered_row(self) -> None:
        """FeatureBarEvent should merge feature data into the existing _bar_rows entry."""
        svc = _make_reporting_service(feature_enabled=True)
        svc.event_bus.publish(_make_price_bar_event())
        svc.event_bus.publish(_make_feature_bar_event())
        key = ("AAPL", "2024-01-02T21:00:00+00:00")
        row = svc._bar_rows[key]
        assert row["features"] == {"trend_strength": 0.8, "trend_regime": "bull"}
        assert row["feature_set_version"] == "v1"

    def test_feature_enabled_flag_stored(self) -> None:
        """_feature_enabled attribute should reflect the constructor argument."""
        assert _make_reporting_service(feature_enabled=False)._feature_enabled is False
        assert _make_reporting_service(feature_enabled=True)._feature_enabled is True
