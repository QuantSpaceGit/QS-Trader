"""
Unit tests for SignalEvent - validates against signal.v1.json contract.

Tests:
- Event creation with valid data
- Schema validation against signal.v1.json
- Field serialization (Decimal → string)
- Intention enum validation
- Required vs optional fields
- Contract compliance

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- pytest fixtures
- Focus on contract validation
"""

from decimal import Decimal

import pytest

from qs_trader.events.events import SignalEvent
from qs_trader.services.strategy.models import SignalIntention


class TestSignalEventCreation:
    """Test SignalEvent instantiation with valid data."""

    def test_create_with_required_fields_only(self) -> None:
        """SignalEvent can be created with only required fields."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            source_service="strategy_service",
        )

        # Assert
        assert event.timestamp == "2024-03-15T14:35:22Z"
        assert event.strategy_id == "test_strategy"
        assert event.symbol == "AAPL"
        assert event.intention == "OPEN_LONG"
        assert event.price == Decimal("145.75")
        assert event.confidence == Decimal("0.85")
        assert event.reason is None
        assert event.metadata is None
        assert event.stop_loss is None
        assert event.take_profit is None

    def test_create_with_all_fields(self) -> None:
        """SignalEvent can be created with all optional fields."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="sma_crossover",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            reason="Fast SMA crossed above Slow SMA",
            metadata={"fast_sma": 145.23, "slow_sma": 143.87},
            stop_loss=Decimal("140.50"),
            take_profit=Decimal("152.00"),
            source_service="strategy_service",
        )

        # Assert
        assert event.reason == "Fast SMA crossed above Slow SMA"
        assert event.metadata == {"fast_sma": 145.23, "slow_sma": 143.87}
        assert event.stop_loss == Decimal("140.50")
        assert event.take_profit == Decimal("152.00")

    def test_intention_accepts_enum(self) -> None:
        """SignalEvent accepts SignalIntention enum for intention field."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test",
            symbol="AAPL",
            intention=SignalIntention.CLOSE_LONG,
            price=Decimal("150.00"),
            confidence=Decimal("0.9"),
            source_service="strategy_service",
        )

        # Assert - converted to string
        assert event.intention == "CLOSE_LONG"
        assert isinstance(event.intention, str)

    def test_intention_accepts_string(self) -> None:
        """SignalEvent accepts valid string for intention field."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test",
            symbol="AAPL",
            intention="OPEN_SHORT",
            price=Decimal("150.00"),
            confidence=Decimal("0.8"),
            source_service="strategy_service",
        )

        # Assert
        assert event.intention == "OPEN_SHORT"


class TestSignalEventSerialization:
    """Test SignalEvent serialization for wire format."""

    def test_serialize_decimals_to_strings(self) -> None:
        """Decimal fields serialize to strings for wire format."""
        # Arrange
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            stop_loss=Decimal("140.50"),
            take_profit=Decimal("152.00"),
            source_service="strategy_service",
        )

        # Act
        data = event.model_dump()

        # Assert - decimals as strings
        assert data["price"] == "145.75"
        assert isinstance(data["price"], str)
        assert data["confidence"] == "0.85"
        assert isinstance(data["confidence"], str)
        assert data["stop_loss"] == "140.50"
        assert isinstance(data["stop_loss"], str)
        assert data["take_profit"] == "152.00"
        assert isinstance(data["take_profit"], str)

    def test_serialize_intention_to_string(self) -> None:
        """Intention field serializes to string."""
        # Arrange
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("150.00"),
            confidence=Decimal("0.9"),
            source_service="strategy_service",
        )

        # Act
        data = event.model_dump()

        # Assert
        assert data["intention"] == "OPEN_LONG"
        assert isinstance(data["intention"], str)


class TestSignalEventValidation:
    """Test SignalEvent validation against signal.v1.json schema."""

    def test_validates_against_schema(self) -> None:
        """SignalEvent validates payload against signal.v1.json."""
        # Arrange & Act - should not raise
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            source_service="strategy_service",
        )

        # Assert - validation passed
        assert event.event_type == "signal"
        assert event.SCHEMA_BASE == "strategy/signal"

    def test_invalid_intention_fails(self) -> None:
        """SignalEvent with invalid intention value fails validation."""
        # Arrange & Act & Assert
        with pytest.raises(ValueError, match="Invalid intention value"):
            SignalEvent(
                signal_id="signal-test-123",
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test",
                symbol="AAPL",
                intention="INVALID_ACTION",
                price=Decimal("150.00"),
                confidence=Decimal("0.9"),
                source_service="strategy_service",
            )

    def test_missing_required_field_fails(self) -> None:
        """SignalEvent without required field fails."""
        # Arrange & Act & Assert
        with pytest.raises(Exception):  # Pydantic ValidationError
            SignalEvent(
                signal_id="signal-test-123",
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test",
                symbol="AAPL",
                # Missing intention
                price=Decimal("150.00"),
                confidence=Decimal("0.9"),
                source_service="strategy_service",
            )  # type: ignore[call-arg]


class TestSignalEventEnvelope:
    """Test SignalEvent envelope fields."""

    def test_has_envelope_fields(self) -> None:
        """SignalEvent has all required envelope fields."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("0.9"),
            source_service="strategy_service",
        )

        # Assert
        assert hasattr(event, "event_id")
        assert hasattr(event, "event_type")
        assert hasattr(event, "event_version")
        assert hasattr(event, "occurred_at")
        assert hasattr(event, "source_service")
        assert event.event_type == "signal"
        assert event.source_service == "strategy_service"

    def test_envelope_validates(self) -> None:
        """SignalEvent envelope validates against envelope.v1.json."""
        # Arrange & Act - should not raise
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("0.9"),
            correlation_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            causation_id="b2c3d4e5-f6a7-8901-bcde-f12345678901",
            source_service="strategy_service",
        )

        # Assert
        assert event.correlation_id == "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        assert event.causation_id == "b2c3d4e5-f6a7-8901-bcde-f12345678901"


class TestSignalEventImmutability:
    """Test SignalEvent immutability (frozen model)."""

    def test_event_is_frozen(self) -> None:
        """SignalEvent is immutable after creation."""
        # Arrange
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("150.00"),
            confidence=Decimal("0.9"),
            source_service="strategy_service",
        )

        # Act & Assert - cannot modify
        with pytest.raises(Exception):  # ValidationError - frozen model
            event.price = Decimal("160.00")


class TestSignalEventIntentions:
    """Test all SignalIntention enum values work."""

    @pytest.mark.parametrize(
        "intention",
        [
            SignalIntention.OPEN_LONG,
            SignalIntention.CLOSE_LONG,
            SignalIntention.OPEN_SHORT,
            SignalIntention.CLOSE_SHORT,
        ],
    )
    def test_all_intentions_valid(self, intention: SignalIntention) -> None:
        """All SignalIntention enum values are valid."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-123",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test",
            symbol="AAPL",
            intention=intention,
            price=Decimal("150.00"),
            confidence=Decimal("0.9"),
            source_service="strategy_service",
        )

        # Assert
        assert event.intention == intention.value
        assert event.intention in [
            "OPEN_LONG",
            "CLOSE_LONG",
            "OPEN_SHORT",
            "CLOSE_SHORT",
        ]
