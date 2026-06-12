"""Unit tests for identity fields on event types (Task Group 5).

Verifies that all event types with a ``symbol`` field carry optional identity
fields (secid, display_symbol, ticker_at_date, identity_source) and that the
corresponding JSON schemas accept them.
"""

from decimal import Decimal
from uuid import NAMESPACE_DNS, uuid5

import pytest

from qs_trader.events.events import (
    FeatureBarEvent,
    FillEvent,
    IndicatorEvent,
    OrderEvent,
    PriceBarEvent,
    RuntimeFeaturesEvent,
    SignalEvent,
    TradeEvent,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

IDENTITY_FIELDS = ("secid", "display_symbol", "ticker_at_date", "identity_source")

IDENTITY_KWARGS = {
    "secid": 12345,
    "display_symbol": "Apple Inc",
    "ticker_at_date": "AAPL",
    "identity_source": "explicit_secid",
}


def _minimal_price_bar() -> dict:
    return {
        "source_service": "test",
        "symbol": "AAPL",
        "timestamp": "2024-01-01T00:00:00Z",
        "open": Decimal("150.00"),
        "high": Decimal("155.00"),
        "low": Decimal("149.00"),
        "close": Decimal("154.50"),
        "volume": 1_000_000,
        "source": "test_source",
        "interval": "1d",
    }


def _minimal_signal() -> dict:
    return {
        "source_service": "test",
        "signal_id": "sig-001",
        "timestamp": "2024-01-01T00:00:00Z",
        "strategy_id": "test_strategy",
        "symbol": "AAPL",
        "intention": "OPEN_LONG",
        "price": Decimal("150.00"),
        "confidence": Decimal("0.85"),
    }


def _minimal_indicator() -> dict:
    return {
        "source_service": "test",
        "strategy_id": "test_strategy",
        "symbol": "AAPL",
        "timestamp": "2024-01-01T00:00:00Z",
        "indicators": {"sma": 150.0},
    }


def _minimal_runtime_features() -> dict:
    return {
        "source_service": "test",
        "strategy_id": "test_strategy",
        "symbol": "AAPL",
        "timestamp": "2024-01-01T00:00:00Z",
        "runtime_features": {"momentum": 0.5},
    }


def _minimal_order() -> dict:
    return {
        "source_service": "test",
        "intent_id": "sig-001",
        "idempotency_key": "key-001",
        "timestamp": "2024-01-01T00:00:00Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": Decimal("100"),
        "order_type": "market",
    }


def _minimal_fill() -> dict:
    return {
        "source_service": "test",
        "fill_id": str(uuid5(NAMESPACE_DNS, "fill-001")),
        "source_order_id": "order-001",
        "timestamp": "2024-01-01T00:00:00Z",
        "symbol": "AAPL",
        "side": "buy",
        "filled_quantity": Decimal("100"),
        "fill_price": Decimal("150.00"),
    }


def _minimal_trade() -> dict:
    return {
        "source_service": "test",
        "trade_id": "T00001",
        "timestamp": "2024-01-01T00:00:00Z",
        "strategy_id": "test_strategy",
        "symbol": "AAPL",
        "status": "open",
        "fills": [str(uuid5(NAMESPACE_DNS, "fill-001"))],
    }


# ---------------------------------------------------------------------------
# Identity fields are present and optional on all event types
# ---------------------------------------------------------------------------


class TestIdentityFieldsOnEvents:
    """Verify identity fields exist and are optional on every symbol-bearing event."""

    @pytest.mark.parametrize(
        ("cls", "kwargs"),
        [
            (PriceBarEvent, _minimal_price_bar()),
            (FeatureBarEvent, {"timestamp": "2024-01-01T00:00:00Z", "symbol": "AAPL"}),
            (RuntimeFeaturesEvent, _minimal_runtime_features()),
            (IndicatorEvent, _minimal_indicator()),
            (SignalEvent, _minimal_signal()),
            (OrderEvent, _minimal_order()),
            (FillEvent, _minimal_fill()),
            (TradeEvent, _minimal_trade()),
        ],
    )
    def test_identity_fields_are_optional(self, cls, kwargs):
        """Every event type should construct without identity fields."""
        event = cls(**kwargs)
        for field in IDENTITY_FIELDS:
            assert getattr(event, field) is None, f"{cls.__name__}.{field} should default to None"

    @pytest.mark.parametrize(
        ("cls", "kwargs"),
        [
            (PriceBarEvent, _minimal_price_bar()),
            (FeatureBarEvent, {"timestamp": "2024-01-01T00:00:00Z", "symbol": "AAPL"}),
            (RuntimeFeaturesEvent, _minimal_runtime_features()),
            (IndicatorEvent, _minimal_indicator()),
            (SignalEvent, _minimal_signal()),
            (OrderEvent, _minimal_order()),
            (FillEvent, _minimal_fill()),
            (TradeEvent, _minimal_trade()),
        ],
    )
    def test_identity_fields_accept_values(self, cls, kwargs):
        """Every event type should accept identity field values."""
        event = cls(**kwargs, **IDENTITY_KWARGS)
        assert event.secid == 12345
        assert event.display_symbol == "Apple Inc"
        assert event.ticker_at_date == "AAPL"
        assert event.identity_source == "explicit_secid"

    @pytest.mark.parametrize(
        ("cls", "kwargs"),
        [
            (PriceBarEvent, _minimal_price_bar()),
            (FeatureBarEvent, {"timestamp": "2024-01-01T00:00:00Z", "symbol": "AAPL"}),
            (RuntimeFeaturesEvent, _minimal_runtime_features()),
            (IndicatorEvent, _minimal_indicator()),
            (SignalEvent, _minimal_signal()),
            (OrderEvent, _minimal_order()),
            (FillEvent, _minimal_fill()),
            (TradeEvent, _minimal_trade()),
        ],
    )
    def test_identity_fields_survive_json_roundtrip(self, cls, kwargs):
        """Identity fields should survive JSON serialization and deserialization."""
        original = cls(**kwargs, **IDENTITY_KWARGS)
        json_str = original.model_dump_json()
        reconstructed = cls.model_validate_json(json_str)

        assert reconstructed.secid == 12345
        assert reconstructed.display_symbol == "Apple Inc"
        assert reconstructed.ticker_at_date == "AAPL"
        assert reconstructed.identity_source == "explicit_secid"


# ---------------------------------------------------------------------------
# Schema validation for events with JSON schemas
# ---------------------------------------------------------------------------


class TestIdentityFieldsInSchemas:
    """Verify JSON schemas accept identity fields."""

    def test_signal_event_with_identity_fields(self):
        """SignalEvent should validate against schema with identity fields."""
        event = SignalEvent(**_minimal_signal(), **IDENTITY_KWARGS)
        assert event.secid == 12345

    def test_indicator_event_with_identity_fields(self):
        """IndicatorEvent should validate against schema with identity fields."""
        event = IndicatorEvent(**_minimal_indicator(), **IDENTITY_KWARGS)
        assert event.secid == 12345

    def test_order_event_with_identity_fields(self):
        """OrderEvent should validate against schema with identity fields."""
        event = OrderEvent(**_minimal_order(), **IDENTITY_KWARGS)
        assert event.secid == 12345

    def test_fill_event_with_identity_fields(self):
        """FillEvent should validate against schema with identity fields."""
        event = FillEvent(**_minimal_fill(), **IDENTITY_KWARGS)
        assert event.secid == 12345

    def test_trade_event_with_identity_fields(self):
        """TradeEvent should validate against schema with identity fields."""
        event = TradeEvent(**_minimal_trade(), **IDENTITY_KWARGS)
        assert event.secid == 12345

    def test_price_bar_event_with_identity_fields(self):
        """PriceBarEvent should validate against schema with identity fields."""
        event = PriceBarEvent(**_minimal_price_bar(), **IDENTITY_KWARGS)
        assert event.secid == 12345

    def test_runtime_features_event_with_identity_fields(self):
        """RuntimeFeaturesEvent should validate against schema with identity fields."""
        event = RuntimeFeaturesEvent(**_minimal_runtime_features(), **IDENTITY_KWARGS)
        assert event.secid == 12345
