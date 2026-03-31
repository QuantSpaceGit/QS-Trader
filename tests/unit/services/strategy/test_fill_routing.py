"""
Tests for FillEvent routing in StrategyService.

Tests that StrategyService correctly routes FillEvents to strategy.on_position_filled()
with proper universe filtering.
"""

from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from qs_trader.events import FillEvent
from qs_trader.events.event_bus import EventBus
from qs_trader.services.strategy.service import StrategyService

from .test_service import MockStrategy, MockStrategyConfig


# Helper function to generate valid UUIDs for tests
def gen_uuid() -> str:
    """Generate a valid UUID string for testing."""
    return str(uuid4())


@pytest.fixture
def fill_event():
    """Create a sample fill event for testing."""
    return FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-456",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
        commission=Decimal("1.00"),
    )


# ============================================
# Fill Routing Tests
# ============================================


def test_service_routes_fills_to_strategy():
    """Service routes FillEvents to strategy.on_position_filled()."""
    # Arrange
    event_bus = EventBus()
    config = MockStrategyConfig(name="test_strategy")
    strategy = MockStrategy(config)
    strategy.on_position_filled = MagicMock()

    service = StrategyService(event_bus=event_bus, strategies={"test": strategy})

    fill = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-456",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
    )

    # Act
    service.on_fill(fill)

    # Assert
    strategy.on_position_filled.assert_called_once()
    call_args = strategy.on_position_filled.call_args
    assert call_args[0][0] == fill  # First arg is the fill event
    assert call_args[0][1] is not None  # Second arg is context


def test_service_routes_multiple_fills():
    """Service routes multiple fills to strategy."""
    # Arrange
    event_bus = EventBus()
    config = MockStrategyConfig(name="test_strategy")
    strategy = MockStrategy(config)
    strategy.on_position_filled = MagicMock()

    service = StrategyService(event_bus=event_bus, strategies={"test": strategy})

    fills = [
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-1",
            timestamp="2024-01-01T10:00:00Z",
            symbol="AAPL",
            side="buy",
            filled_quantity=Decimal("100"),
            fill_price=Decimal("150.00"),
        ),
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-2",
            timestamp="2024-01-01T11:00:00Z",
            symbol="AAPL",
            side="sell",
            filled_quantity=Decimal("50"),
            fill_price=Decimal("155.00"),
        ),
    ]

    # Act
    for fill in fills:
        service.on_fill(fill)

    # Assert
    assert strategy.on_position_filled.call_count == 2


def test_fill_universe_filtering():
    """Service only routes fills for symbols in strategy universe."""
    # Arrange
    event_bus = EventBus()
    config = MockStrategyConfig(name="test_strategy", universe=["AAPL", "MSFT"])
    strategy = MockStrategy(config)
    strategy.on_position_filled = MagicMock()

    service = StrategyService(event_bus=event_bus, strategies={"test": strategy})

    fills = [
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-1",
            timestamp="2024-01-01T10:00:00Z",
            symbol="AAPL",  # In universe
            side="buy",
            filled_quantity=Decimal("100"),
            fill_price=Decimal("150.00"),
        ),
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-2",
            timestamp="2024-01-01T11:00:00Z",
            symbol="TSLA",  # NOT in universe
            side="buy",
            filled_quantity=Decimal("50"),
            fill_price=Decimal("200.00"),
        ),
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-3",
            timestamp="2024-01-01T12:00:00Z",
            symbol="MSFT",  # In universe
            side="buy",
            filled_quantity=Decimal("75"),
            fill_price=Decimal("300.00"),
        ),
    ]

    # Act
    for fill in fills:
        service.on_fill(fill)

    # Assert - only 2 fills should be routed (AAPL and MSFT, not TSLA)
    assert strategy.on_position_filled.call_count == 2

    # Verify correct symbols were routed
    calls = strategy.on_position_filled.call_args_list
    routed_symbols = [call[0][0].symbol for call in calls]
    assert routed_symbols == ["AAPL", "MSFT"]


def test_fill_empty_universe_accepts_all_symbols():
    """Empty universe means strategy receives fills for all symbols."""
    # Arrange
    event_bus = EventBus()
    config = MockStrategyConfig(name="test_strategy", universe=[])  # Empty = all
    strategy = MockStrategy(config)
    strategy.on_position_filled = MagicMock()

    service = StrategyService(event_bus=event_bus, strategies={"test": strategy})

    fills = [
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-1",
            timestamp="2024-01-01T10:00:00Z",
            symbol="AAPL",
            side="buy",
            filled_quantity=Decimal("100"),
            fill_price=Decimal("150.00"),
        ),
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-2",
            timestamp="2024-01-01T11:00:00Z",
            symbol="TSLA",
            side="buy",
            filled_quantity=Decimal("50"),
            fill_price=Decimal("200.00"),
        ),
    ]

    # Act
    for fill in fills:
        service.on_fill(fill)

    # Assert - both fills should be routed
    assert strategy.on_position_filled.call_count == 2


def test_fill_universe_filtering_multiple_strategies():
    """Different strategies receive fills based on their universes."""
    # Arrange
    event_bus = EventBus()

    config1 = MockStrategyConfig(name="apple_strategy", universe=["AAPL"])
    strategy1 = MockStrategy(config1)
    strategy1.on_position_filled = MagicMock()

    config2 = MockStrategyConfig(name="tech_strategy", universe=["AAPL", "MSFT", "GOOGL"])
    strategy2 = MockStrategy(config2)
    strategy2.on_position_filled = MagicMock()

    config3 = MockStrategyConfig(name="all_strategy", universe=[])  # All symbols
    strategy3 = MockStrategy(config3)
    strategy3.on_position_filled = MagicMock()

    service = StrategyService(
        event_bus=event_bus,
        strategies={
            "apple": strategy1,
            "tech": strategy2,
            "all": strategy3,
        },
    )

    fills = [
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-1",
            timestamp="2024-01-01T10:00:00Z",
            symbol="AAPL",
            side="buy",
            filled_quantity=Decimal("100"),
            fill_price=Decimal("150.00"),
        ),
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-2",
            timestamp="2024-01-01T11:00:00Z",
            symbol="MSFT",
            side="buy",
            filled_quantity=Decimal("50"),
            fill_price=Decimal("300.00"),
        ),
        FillEvent(
            fill_id=gen_uuid(),
            source_order_id="order-3",
            timestamp="2024-01-01T12:00:00Z",
            symbol="TSLA",
            side="buy",
            filled_quantity=Decimal("25"),
            fill_price=Decimal("200.00"),
        ),
    ]

    # Act
    for fill in fills:
        service.on_fill(fill)

    # Assert
    assert strategy1.on_position_filled.call_count == 1  # Only AAPL
    assert strategy2.on_position_filled.call_count == 2  # AAPL + MSFT
    assert strategy3.on_position_filled.call_count == 3  # All


def test_on_fill_error_doesnt_crash_service():
    """Strategy error in on_position_filled doesn't crash service."""
    # Arrange
    event_bus = EventBus()
    config = MockStrategyConfig(name="error_strategy")
    strategy = MockStrategy(config)

    def raise_error(*args, **kwargs):
        raise ValueError("Test error in on_position_filled")

    strategy.on_position_filled = raise_error

    service = StrategyService(event_bus=event_bus, strategies={"error": strategy})

    fill = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-456",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
    )

    # Act - should not raise
    service.on_fill(fill)

    # Assert - error tracked in metrics
    metrics = service.get_metrics()
    assert metrics["error"]["errors"] == 1


def test_on_fill_error_tracked_in_metrics():
    """Errors in on_position_filled tracked per strategy."""
    # Arrange
    event_bus = EventBus()

    config1 = MockStrategyConfig(name="good_strategy")
    strategy1 = MockStrategy(config1)
    strategy1.on_position_filled = MagicMock()

    config2 = MockStrategyConfig(name="bad_strategy")
    strategy2 = MockStrategy(config2)

    def raise_error(*args, **kwargs):
        raise ValueError("Error")

    strategy2.on_position_filled = raise_error

    service = StrategyService(
        event_bus=event_bus,
        strategies={"good": strategy1, "bad": strategy2},
    )

    fill = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-456",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
    )

    # Act
    service.on_fill(fill)
    service.on_fill(fill)  # Second fill

    metrics = service.get_metrics()

    # Assert
    assert metrics["good"]["errors"] == 0
    assert metrics["bad"]["errors"] == 2  # Two errors
    assert strategy1.on_position_filled.call_count == 2  # Still called despite other strategy error


# ============================================
# Strategy ID-Based Fill Routing Tests
# ============================================


def test_fill_with_strategy_id_routes_only_to_that_strategy():
    """Fill with strategy_id routes ONLY to that strategy, not all strategies trading symbol."""
    # Arrange
    event_bus = EventBus()

    config1 = MockStrategyConfig(name="strategy_a", universe=["AAPL"])
    strategy1 = MockStrategy(config1)
    strategy1.on_position_filled = MagicMock()

    config2 = MockStrategyConfig(name="strategy_b", universe=["AAPL"])
    strategy2 = MockStrategy(config2)
    strategy2.on_position_filled = MagicMock()

    service = StrategyService(
        event_bus=event_bus,
        strategies={"strategy_a": strategy1, "strategy_b": strategy2},
    )

    fill = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-123",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
        strategy_id="strategy_a",  # Attributed to strategy_a
    )

    # Act
    service.on_fill(fill)

    # Assert - only strategy_a receives fill, not strategy_b
    strategy1.on_position_filled.assert_called_once()
    strategy2.on_position_filled.assert_not_called()


def test_fill_without_strategy_id_fans_to_all_strategies_in_universe():
    """Fill WITHOUT strategy_id falls back to universe filtering (legacy behavior)."""
    # Arrange
    event_bus = EventBus()

    config1 = MockStrategyConfig(name="strategy_a", universe=["AAPL"])
    strategy1 = MockStrategy(config1)
    strategy1.on_position_filled = MagicMock()

    config2 = MockStrategyConfig(name="strategy_b", universe=["AAPL"])
    strategy2 = MockStrategy(config2)
    strategy2.on_position_filled = MagicMock()

    service = StrategyService(
        event_bus=event_bus,
        strategies={"strategy_a": strategy1, "strategy_b": strategy2},
    )

    fill = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-123",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
        # No strategy_id - should fan to both
    )

    # Act
    service.on_fill(fill)

    # Assert - both strategies receive fill
    strategy1.on_position_filled.assert_called_once()
    strategy2.on_position_filled.assert_called_once()


def test_fill_with_unknown_strategy_id_logs_warning_and_skips():
    """Fill with unknown strategy_id logs warning and doesn't crash."""
    # Arrange
    event_bus = EventBus()

    config = MockStrategyConfig(name="existing_strategy")
    strategy = MockStrategy(config)
    strategy.on_position_filled = MagicMock()

    service = StrategyService(event_bus=event_bus, strategies={"existing": strategy})

    fill = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-123",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
        strategy_id="unknown_strategy",  # Does not exist
    )

    # Act - should not crash
    service.on_fill(fill)

    # Assert - no strategy received the fill
    strategy.on_position_filled.assert_not_called()


def test_multi_strategy_portfolio_with_shared_symbol():
    """Multi-strategy portfolio: each fill goes only to originating strategy."""
    # Arrange
    event_bus = EventBus()

    # Both strategies trade AAPL
    config1 = MockStrategyConfig(name="momentum", universe=["AAPL", "MSFT"])
    strategy1 = MockStrategy(config1)
    strategy1.on_position_filled = MagicMock()

    config2 = MockStrategyConfig(name="mean_reversion", universe=["AAPL", "GOOGL"])
    strategy2 = MockStrategy(config2)
    strategy2.on_position_filled = MagicMock()

    service = StrategyService(
        event_bus=event_bus,
        strategies={"momentum": strategy1, "mean_reversion": strategy2},
    )

    # Fill attributed to momentum strategy
    fill1 = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-momentum-1",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
        strategy_id="momentum",
    )

    # Fill attributed to mean_reversion strategy
    fill2 = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-mean-reversion-1",
        timestamp="2024-01-01T11:00:00Z",
        symbol="AAPL",
        side="sell",
        filled_quantity=Decimal("50"),
        fill_price=Decimal("151.00"),
        strategy_id="mean_reversion",
    )

    # Act
    service.on_fill(fill1)
    service.on_fill(fill2)

    # Assert - each strategy only receives its own fills
    assert strategy1.on_position_filled.call_count == 1
    assert strategy2.on_position_filled.call_count == 1

    # Verify correct fills went to correct strategies
    call1 = strategy1.on_position_filled.call_args[0][0]
    assert call1.source_order_id == "order-momentum-1"

    call2 = strategy2.on_position_filled.call_args[0][0]
    assert call2.source_order_id == "order-mean-reversion-1"


def test_fill_with_none_strategy_id_uses_universe_filtering():
    """Fill with None strategy_id falls back to universe filtering."""
    # Arrange
    event_bus = EventBus()

    config1 = MockStrategyConfig(name="strategy_a", universe=["AAPL"])
    strategy1 = MockStrategy(config1)
    strategy1.on_position_filled = MagicMock()

    config2 = MockStrategyConfig(name="strategy_b", universe=["MSFT"])
    strategy2 = MockStrategy(config2)
    strategy2.on_position_filled = MagicMock()

    service = StrategyService(
        event_bus=event_bus,
        strategies={"strategy_a": strategy1, "strategy_b": strategy2},
    )

    fill = FillEvent(
        fill_id=gen_uuid(),
        source_order_id="order-123",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
        strategy_id=None,  # None - should fall back to universe filtering
    )

    # Act
    service.on_fill(fill)

    # Assert - only strategy_a receives fill (universe filtering)
    strategy1.on_position_filled.assert_called_once()
    strategy2.on_position_filled.assert_not_called()
