"""
Unit tests for SignalEvent JSON Schema contract validation.

Tests the signal.v1.json schema to ensure:
- Required fields are enforced
- Field types and formats are validated
- Enum values are constrained correctly
- Optional fields work as expected
- Schema rejects invalid data

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- pytest fixtures
- Focus on contract validation
"""

from decimal import Decimal

import pytest

from qs_trader.events.events import SignalEvent
from qs_trader.services.strategy.models import LifecycleIntentType, SignalIntention


class TestSignalSchemaRequiredFields:
    """Test that signal schema enforces all required fields."""

    def test_signal_schema_requires_essential_fields(self) -> None:
        """Signal schema requires: signal_id, timestamp, strategy_id, symbol, intention, price, confidence."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-001",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            source_service="strategy_service",
        )

        # Assert - all required fields present
        assert event.signal_id == "signal-test-001"
        assert event.timestamp == "2024-03-15T14:35:22Z"
        assert event.strategy_id == "test_strategy"
        assert event.symbol == "AAPL"
        assert event.intention == "OPEN_LONG"
        assert event.price == Decimal("145.75")
        assert event.confidence == Decimal("0.85")

    def test_signal_schema_rejects_missing_signal_id(self) -> None:
        """Signal schema rejects signal without signal_id."""
        # Arrange, Act & Assert
        with pytest.raises(ValueError, match="signal_id"):
            SignalEvent(  # type: ignore[call-arg]
                # Missing signal_id
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test_strategy",
                symbol="AAPL",
                intention=SignalIntention.OPEN_LONG,
                price=Decimal("145.75"),
                confidence=Decimal("0.85"),
                source_service="strategy_service",
            )

    def test_signal_schema_rejects_missing_timestamp(self) -> None:
        """Signal schema rejects signal without timestamp."""
        # Arrange, Act & Assert
        with pytest.raises(ValueError, match="timestamp"):
            SignalEvent(  # type: ignore[call-arg]
                signal_id="signal-test-001",
                # Missing timestamp
                strategy_id="test_strategy",
                symbol="AAPL",
                intention=SignalIntention.OPEN_LONG,
                price=Decimal("145.75"),
                confidence=Decimal("0.85"),
                source_service="strategy_service",
            )

    def test_signal_schema_rejects_missing_strategy_id(self) -> None:
        """Signal schema rejects signal without strategy_id."""
        # Arrange, Act & Assert
        with pytest.raises(ValueError, match="strategy_id"):
            SignalEvent(  # type: ignore[call-arg]
                signal_id="signal-test-001",
                timestamp="2024-03-15T14:35:22Z",
                # Missing strategy_id
                symbol="AAPL",
                intention=SignalIntention.OPEN_LONG,
                price=Decimal("145.75"),
                confidence=Decimal("0.85"),
                source_service="strategy_service",
            )

    def test_signal_schema_rejects_missing_symbol(self) -> None:
        """Signal schema rejects signal without symbol."""
        # Arrange, Act & Assert
        with pytest.raises(ValueError, match="symbol"):
            SignalEvent(  # type: ignore[call-arg]
                signal_id="signal-test-001",
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test_strategy",
                # Missing symbol
                intention=SignalIntention.OPEN_LONG,
                price=Decimal("145.75"),
                confidence=Decimal("0.85"),
                source_service="strategy_service",
            )

    def test_signal_schema_rejects_missing_intention(self) -> None:
        """Signal schema rejects signal without intention."""
        # Arrange, Act & Assert
        with pytest.raises(ValueError, match="intention"):
            SignalEvent(  # type: ignore[call-arg]
                signal_id="signal-test-001",
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test_strategy",
                symbol="AAPL",
                # Missing intention
                price=Decimal("145.75"),
                confidence=Decimal("0.85"),
                source_service="strategy_service",
            )

    def test_signal_schema_rejects_missing_price(self) -> None:
        """Signal schema rejects signal without price."""
        # Arrange, Act & Assert
        with pytest.raises(ValueError, match="price"):
            SignalEvent(  # type: ignore[call-arg]
                signal_id="signal-test-001",
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test_strategy",
                symbol="AAPL",
                intention=SignalIntention.OPEN_LONG,
                # Missing price
                confidence=Decimal("0.85"),
                source_service="strategy_service",
            )

    def test_signal_schema_rejects_missing_confidence(self) -> None:
        """Signal schema rejects signal without confidence."""
        # Arrange, Act & Assert
        with pytest.raises(ValueError, match="confidence"):
            SignalEvent(  # type: ignore[call-arg]
                signal_id="signal-test-001",
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test_strategy",
                symbol="AAPL",
                intention=SignalIntention.OPEN_LONG,
                price=Decimal("145.75"),
                # Missing confidence
                source_service="strategy_service",
            )


class TestSignalSchemaValidData:
    """Test that valid signals pass schema validation."""

    def test_signal_schema_accepts_valid_signal_minimal(self) -> None:
        """Signal schema accepts valid signal with only required fields."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-001",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="sma_crossover",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            source_service="strategy_service",
        )

        # Assert
        assert event.signal_id == "signal-test-001"
        assert event.strategy_id == "sma_crossover"
        assert event.symbol == "AAPL"

    def test_signal_schema_accepts_valid_signal_with_all_fields(self) -> None:
        """Signal schema accepts valid signal with all optional fields."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-002",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="sma_crossover",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            reason="SMA crossover detected",
            metadata={"fast_sma": 145.23, "slow_sma": 143.87},
            stop_loss=Decimal("140.50"),
            take_profit=Decimal("152.00"),
            source_service="strategy_service",
        )

        # Assert
        assert event.signal_id == "signal-test-002"
        assert event.reason == "SMA crossover detected"
        assert event.metadata == {"fast_sma": 145.23, "slow_sma": 143.87}
        assert event.stop_loss == Decimal("140.50")
        assert event.take_profit == Decimal("152.00")

    def test_signal_schema_accepts_explicit_scale_in_intent_type(self) -> None:
        """Signal schema accepts explicit scale-in lifecycle intent opt-ins."""
        event = SignalEvent(
            signal_id="signal-test-002b",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="sma_crossover",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            intent_type=LifecycleIntentType.SCALE_IN,
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            source_service="strategy_service",
        )

        assert event.intent_type == "scale_in"


class TestSignalSchemaIntentionEnum:
    """Test that schema validates intention enum correctly."""

    @pytest.mark.parametrize(
        "intention",
        [
            SignalIntention.OPEN_LONG,
            SignalIntention.CLOSE_LONG,
            SignalIntention.OPEN_SHORT,
            SignalIntention.CLOSE_SHORT,
        ],
    )
    def test_signal_schema_accepts_all_valid_intentions(self, intention: SignalIntention) -> None:
        """Signal schema accepts all valid intention enum values."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-003",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention=intention,
            price=Decimal("145.75"),
            confidence=Decimal("0.85"),
            source_service="strategy_service",
        )

        # Assert
        assert event.intention == intention.value

    def test_signal_schema_rejects_invalid_intention(self) -> None:
        """Signal schema rejects invalid intention values."""
        # Arrange, Act & Assert
        with pytest.raises(ValueError, match="Invalid intention"):
            SignalEvent(
                signal_id="signal-test-004",
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test_strategy",
                symbol="AAPL",
                intention="INVALID_ACTION",
                price=Decimal("145.75"),
                confidence=Decimal("0.85"),
                source_service="strategy_service",
            )

    def test_signal_schema_rejects_incompatible_explicit_intent_type(self) -> None:
        """Lifecycle opt-ins must align with the directional intention."""
        with pytest.raises(ValueError, match="CLOSE_LONG signals only support lifecycle intent types"):
            SignalEvent(
                signal_id="signal-test-004b",
                timestamp="2024-03-15T14:35:22Z",
                strategy_id="test_strategy",
                symbol="AAPL",
                intention=SignalIntention.CLOSE_LONG,
                intent_type=LifecycleIntentType.SCALE_IN,
                price=Decimal("145.75"),
                confidence=Decimal("0.85"),
                source_service="strategy_service",
            )


class TestSignalSchemaConfidenceBounds:
    """Test that schema validates confidence bounds [0.0, 1.0]."""

    def test_signal_schema_accepts_confidence_zero(self) -> None:
        """Signal schema accepts confidence = 0.0 (minimum valid)."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-005",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("145.75"),
            confidence=Decimal("0.0"),
            source_service="strategy_service",
        )

        # Assert
        assert event.confidence == Decimal("0.0")

    def test_signal_schema_accepts_confidence_one(self) -> None:
        """Signal schema accepts confidence = 1.0 (maximum valid)."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-006",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("145.75"),
            confidence=Decimal("1.0"),
            source_service="strategy_service",
        )

        # Assert
        assert event.confidence == Decimal("1.0")

    def test_signal_schema_accepts_confidence_mid_range(self) -> None:
        """Signal schema accepts confidence in valid range (0.0 < x < 1.0)."""
        # Arrange & Act
        event = SignalEvent(
            signal_id="signal-test-007",
            timestamp="2024-03-15T14:35:22Z",
            strategy_id="test_strategy",
            symbol="AAPL",
            intention=SignalIntention.OPEN_LONG,
            price=Decimal("145.75"),
            confidence=Decimal("0.567"),
            source_service="strategy_service",
        )

        # Assert
        assert event.confidence == Decimal("0.567")
