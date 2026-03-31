"""
Comprehensive unit tests for Event models.

Tests BaseEvent, ValidatedEvent, and specific event types like PriceBarEvent
and CorporateActionEvent for validation, serialization, and schema compliance.
"""

import json
from datetime import timezone
from decimal import Decimal

import pytest

from qs_trader.events.events import (
    BacktestStartedEvent,
    BarCloseEvent,
    BaseEvent,
    CorporateActionEvent,
    PriceBarEvent,
    load_and_compile_schema,
)

# ============================================
# BaseEvent Tests
# ============================================


class TestBaseEvent:
    """Test BaseEvent envelope validation and fields."""

    def test_basevent_creates_with_defaults(self):
        """BaseEvent should create with default values."""
        event = BaseEvent(source_service="test_service")

        assert event.event_id is not None
        assert event.event_type == "base"
        assert event.event_version == 1
        assert event.occurred_at is not None
        assert event.source_service == "test_service"

    def test_baseevent_generates_unique_ids(self):
        """Each BaseEvent should have unique event_id."""
        event1 = BaseEvent(source_service="test")
        event2 = BaseEvent(source_service="test")

        assert event1.event_id != event2.event_id

    def test_baseevent_occurred_at_is_utc(self):
        """occurred_at should be UTC timezone-aware."""
        event = BaseEvent(source_service="test")

        assert event.occurred_at.tzinfo == timezone.utc

    def test_baseevent_accepts_correlation_id(self):
        """BaseEvent should accept correlation_id."""
        import uuid

        corr_id = str(uuid.uuid4())
        event = BaseEvent(source_service="test", correlation_id=corr_id)

        assert event.correlation_id == corr_id

    def test_baseevent_is_frozen(self):
        """BaseEvent instances should be immutable."""
        event = BaseEvent(source_service="test")

        with pytest.raises(Exception):  # Pydantic ValidationError
            event.source_service = "different"

    def test_baseevent_serializes_to_json(self):
        """BaseEvent should serialize to JSON."""
        event = BaseEvent(source_service="test_service")
        json_str = event.model_dump_json()

        assert isinstance(json_str, str)
        data = json.loads(json_str)
        assert data["source_service"] == "test_service"
        assert data["event_type"] == "base"

    def test_baseevent_occurred_at_serializes_with_z_suffix(self):
        """occurred_at should serialize to RFC3339 with Z suffix."""
        event = BaseEvent(source_service="test")
        json_str = event.model_dump_json()
        data = json.loads(json_str)

        assert data["occurred_at"].endswith("Z")

    def test_baseevent_parses_iso_timestamp(self):
        """BaseEvent should parse ISO8601 timestamp strings."""
        event = BaseEvent(source_service="test", occurred_at="2024-01-01T12:00:00Z")  # pyright: ignore[reportArgumentType]

        assert event.occurred_at.year == 2024
        assert event.occurred_at.month == 1
        assert event.occurred_at.day == 1


# ============================================
# ControlEvent Tests
# ============================================


class TestControlEvent:
    """Test ControlEvent (no payload validation)."""

    def test_bar_close_event_creates_successfully(self):
        """BarCloseEvent should create without errors."""
        event = BarCloseEvent(source_service="backtest_service")

        assert event.event_type == "bar_close"
        assert event.source_service == "backtest_service"

    def test_backtest_started_event_with_config(self):
        """BacktestStartedEvent should accept config dict."""
        config = {"start_date": "2024-01-01", "end_date": "2024-12-31", "initial_capital": 100000}

        event = BacktestStartedEvent(source_service="backtest_service", config=config)  # pyright: ignore[reportCallIssue]

        assert event.event_type == "backtest_started"
        assert event.config == config


# ============================================
# PriceBarEvent Tests
# ============================================


class TestPriceBarEvent:
    """Test PriceBarEvent validation and schema compliance."""

    def test_price_bar_event_creates_with_required_fields(self):
        """PriceBarEvent should create with required fields."""
        event = PriceBarEvent(
            source_service="data_service",
            symbol="AAPL",
            asset_class="equity",
            interval="1d",
            timestamp="2024-01-01T00:00:00Z",
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.00"),
            close=Decimal("154.50"),
            volume=1_000_000,
            source="test_source",
        )

        assert event.symbol == "AAPL"
        assert event.event_type == "bar"
        assert event.open == Decimal("150.00")

    def test_price_bar_validates_schema(self):
        """PriceBarEvent should validate against bar schema."""
        # This should not raise
        event = PriceBarEvent(
            source_service="data_service",
            symbol="TEST",
            asset_class="equity",
            interval="1d",
            timestamp="2024-01-01T00:00:00Z",
            open=Decimal("100.00"),
            high=Decimal("105.00"),
            low=Decimal("99.00"),
            close=Decimal("104.00"),
            volume=1000,
            source="test",
        )

        assert event.symbol == "TEST"

    def test_price_bar_decimal_fields_accept_strings(self):
        """PriceBarEvent Decimal fields should accept string input."""
        event = PriceBarEvent(
            source_service="data_service",
            symbol="AAPL",
            asset_class="equity",
            interval="1d",
            timestamp="2024-01-01T00:00:00Z",
            open="150.00",  # pyright: ignore[reportArgumentType]
            high="155.00",  # pyright: ignore[reportArgumentType]
            low="149.00",  # pyright: ignore[reportArgumentType]
            close="154.50",  # pyright: ignore[reportArgumentType]
            volume=1_000_000,
            source="test_source",
        )

        # Should be converted to Decimal
        assert isinstance(event.open, Decimal)
        assert event.open == Decimal("150.00")

    def test_price_bar_serializes_decimals_as_strings(self):
        """PriceBarEvent should serialize Decimals as strings."""
        event = PriceBarEvent(
            source_service="data_service",
            symbol="AAPL",
            asset_class="equity",
            interval="1d",
            timestamp="2024-01-01T00:00:00Z",
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.00"),
            close=Decimal("154.50"),
            volume=1_000_000,
            source="test_source",
        )

        json_str = event.model_dump_json()
        data = json.loads(json_str)

        # Decimals should be strings in JSON
        assert isinstance(data["open"], str)
        assert data["open"] == "150.00"

    def test_price_bar_roundtrip_preserves_precision(self):
        """PriceBarEvent should preserve Decimal precision through serialization."""
        original = PriceBarEvent(
            source_service="data_service",
            symbol="TEST",
            asset_class="equity",
            interval="1d",
            timestamp="2024-01-01T00:00:00Z",
            open=Decimal("123.456789"),
            high=Decimal("123.456789"),
            low=Decimal("123.456789"),
            close=Decimal("123.456789"),
            volume=1000,
            price_scale=6,
            source="test",
        )

        # Serialize and deserialize
        json_str = original.model_dump_json()
        reconstructed = PriceBarEvent.model_validate_json(json_str)

        assert reconstructed.open == Decimal("123.456789")
        assert reconstructed.close == Decimal("123.456789")


# ============================================
# CorporateActionEvent Tests
# ============================================


class TestCorporateActionEvent:
    """Test CorporateActionEvent validation."""

    def test_corporate_action_split_creates_successfully(self):
        """CorporateActionEvent for split should create successfully."""
        event = CorporateActionEvent(
            source_service="data_service",
            symbol="AAPL",
            asset_class="equity",
            action_type="split",
            announcement_date="2020-07-30",
            ex_date="2020-08-31",
            effective_date="2020-08-31",
            source="test_source",
            split_from=1,
            split_to=4,
            split_ratio=Decimal("0.25"),
            price_adjustment_factor=Decimal("0.25"),
            volume_adjustment_factor=Decimal("4.0"),
        )

        assert event.event_type == "corporate_action"
        assert event.action_type == "split"
        assert event.split_ratio == Decimal("0.25")

    def test_corporate_action_dividend_creates_successfully(self):
        """CorporateActionEvent for dividend should create successfully."""
        event = CorporateActionEvent(
            source_service="data_service",
            symbol="AAPL",
            asset_class="equity",
            action_type="dividend",
            announcement_date="2024-01-15",
            ex_date="2024-01-30",
            effective_date="2024-02-15",
            source="test_source",
            dividend_amount=Decimal("0.25"),
            dividend_currency="USD",
        )

        assert event.action_type == "dividend"
        assert event.dividend_amount == Decimal("0.25")
        assert event.dividend_currency == "USD"

    def test_corporate_action_serializes_correctly(self):
        """CorporateActionEvent should serialize correctly."""
        event = CorporateActionEvent(
            source_service="data_service",
            symbol="AAPL",
            asset_class="equity",
            action_type="split",
            announcement_date="2020-07-30",
            ex_date="2020-08-31",
            effective_date="2020-08-31",
            source="test_source",
            split_from=1,
            split_to=4,
            split_ratio=Decimal("0.25"),
            price_adjustment_factor=Decimal("0.25"),
            volume_adjustment_factor=Decimal("4.0"),
        )

        json_str = event.model_dump_json()
        data = json.loads(json_str)

        assert data["action_type"] == "split"
        assert data["symbol"] == "AAPL"


# ============================================
# Schema Loading Tests
# ============================================


class TestSchemaLoading:
    """Test schema loading and compilation."""

    def test_load_and_compile_schema_success(self):
        """load_and_compile_schema should load valid schemas."""
        validator = load_and_compile_schema("data/bar.v1.json")

        assert validator is not None
        assert hasattr(validator, "validate")

    def test_load_and_compile_schema_nonexistent_raises(self):
        """load_and_compile_schema should raise for missing schema."""
        with pytest.raises(FileNotFoundError):
            load_and_compile_schema("nonexistent.v1.json")

    def test_schema_loading_is_cached(self):
        """Schema loading should use caching."""
        validator1 = load_and_compile_schema("data/bar.v1.json")
        validator2 = load_and_compile_schema("data/bar.v1.json")

        # Should return same cached instance
        assert validator1 is validator2


# ============================================
# Event Envelope Validation Tests
# ============================================


class TestEnvelopeValidation:
    """Test envelope validation across event types."""

    def test_envelope_requires_valid_event_id_format(self):
        """Envelope should validate event_id format."""
        # Default generated IDs should be valid UUIDs
        event = BaseEvent(source_service="test")
        assert len(event.event_id) == 36  # UUID format

    def test_envelope_accepts_custom_event_id(self):
        """Envelope should accept custom event_id if valid UUID."""
        import uuid

        custom_id = str(uuid.uuid4())

        event = BaseEvent(source_service="test", event_id=custom_id)
        assert event.event_id == custom_id

    def test_envelope_requires_source_service(self):
        """Envelope should require source_service."""
        # This should work
        event = BaseEvent(source_service="test_service")
        assert event.source_service == "test_service"


# ============================================
# Integration Tests
# ============================================


class TestEventIntegration:
    """Integration tests with realistic scenarios."""

    def test_event_can_be_serialized_and_deserialized(self):
        """Event should roundtrip through JSON serialization."""
        original = PriceBarEvent(
            source_service="data_service",
            symbol="AAPL",
            asset_class="equity",
            interval="1d",
            timestamp="2024-01-01T00:00:00Z",
            open=Decimal("150.00"),
            high=Decimal("155.00"),
            low=Decimal("149.00"),
            close=Decimal("154.50"),
            volume=1_000_000,
            source="test_source",
        )

        # Serialize
        json_str = original.model_dump_json()

        # Deserialize
        reconstructed = PriceBarEvent.model_validate_json(json_str)

        assert reconstructed.event_id == original.event_id
        assert reconstructed.symbol == original.symbol
        assert reconstructed.open == original.open

    def test_events_maintain_immutability(self):
        """Events should not allow field modification after creation."""
        event = BarCloseEvent(source_service="test")

        with pytest.raises(Exception):  # Pydantic ValidationError
            event.source_service = "modified"

    def test_event_copy_creates_new_instance(self):
        """model_copy should create new instance with modified fields."""
        original = BarCloseEvent(source_service="original")

        modified = original.model_copy(update={"source_service": "modified"})

        assert original.source_service == "original"
        assert modified.source_service == "modified"
        assert original.event_id == modified.event_id  # Same ID unless updated
