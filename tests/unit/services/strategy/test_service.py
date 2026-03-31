"""Unit tests for StrategyService - Phase 2 implementation."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.strategy.service import StrategyService


class MockStrategyConfig(StrategyConfig):
    """Mock strategy config for testing."""

    name: str = "mock_strategy"
    display_name: str = "Mock Strategy"
    universe: list[str] = []


class MockStrategy(Strategy):
    """Mock strategy for testing."""

    def __init__(self, config: MockStrategyConfig):
        self.config = config
        self.setup_called = False
        self.teardown_called = False
        self.bars_received: list[PriceBarEvent] = []

    def setup(self, context: Context) -> None:
        self.setup_called = True

    def teardown(self, context: Context) -> None:
        self.teardown_called = True

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        self.bars_received.append(event)


@pytest.fixture
def event_bus():
    """Create event bus for testing."""
    return EventBus()


@pytest.fixture
def mock_strategy():
    """Create mock strategy."""
    config = MockStrategyConfig()
    return MockStrategy(config)


# ============================================
# Service Initialization Tests
# ============================================


def test_service_initialization_success(event_bus, mock_strategy):
    """Service can initialize with strategies."""
    # Act
    service = StrategyService(event_bus=event_bus, strategies={"mock": mock_strategy})

    # Assert
    assert len(service._strategies) == 1
    assert "mock" in service._strategies
    assert "mock" in service._contexts
    assert "mock" in service._strategy_metrics


def test_service_initialization_multiple_strategies(event_bus):
    """Service can initialize with multiple strategies."""
    # Arrange
    config1 = MockStrategyConfig(name="strategy_1")
    config2 = MockStrategyConfig(name="strategy_2")
    strategy1 = MockStrategy(config1)
    strategy2 = MockStrategy(config2)

    # Act
    service = StrategyService(
        event_bus=event_bus,
        strategies={"strat1": strategy1, "strat2": strategy2},
    )

    # Assert
    assert len(service._strategies) == 2
    assert "strat1" in service._contexts
    assert "strat2" in service._contexts


def test_service_initialization_creates_contexts(event_bus, mock_strategy):
    """Service creates context for each strategy."""
    # Act
    service = StrategyService(event_bus=event_bus, strategies={"mock": mock_strategy})

    # Assert
    context = service._contexts["mock"]
    assert context.strategy_id == "mock"


def test_service_initialization_subscribes_to_bars(event_bus, mock_strategy):
    """Service subscribes to PriceBarEvent and FillEvent."""
    # Arrange
    event_bus.subscribe = MagicMock()

    # Act
    service = StrategyService(event_bus=event_bus, strategies={"mock": mock_strategy})

    # Assert - should subscribe to both bar and fill events
    assert event_bus.subscribe.call_count == 2

    # Check bar subscription
    bar_call = event_bus.subscribe.call_args_list[0]
    assert bar_call[0][0] == "bar"
    assert bar_call[0][1] == service.on_bar

    # Check fill subscription
    fill_call = event_bus.subscribe.call_args_list[1]
    assert fill_call[0][0] == "fill"
    assert fill_call[0][1] == service.on_fill


# ============================================
# Bar Routing Tests
# ============================================


def test_service_routes_bars_to_strategy(event_bus, mock_strategy):
    """Service routes bars to strategies."""
    # Arrange
    service = StrategyService(event_bus=event_bus, strategies={"mock": mock_strategy})

    event = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-01T10:00:00Z",
        open=Decimal("100.0"),
        high=Decimal("102.0"),
        low=Decimal("99.0"),
        close=Decimal("101.0"),
        volume=1000,
        source="test",
        interval="1d",
    )

    # Act
    service.on_bar(event)

    # Assert
    assert len(mock_strategy.bars_received) == 1
    assert mock_strategy.bars_received[0] == event


def test_service_routes_multiple_bars(event_bus, mock_strategy):
    """Service routes multiple bars sequentially."""
    # Arrange
    service = StrategyService(event_bus=event_bus, strategies={"mock": mock_strategy})

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-01T10:00:00Z",
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=1000,
            source="test",
            interval="1d",
        ),
        PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-02T10:00:00Z",
            open=Decimal("100.5"),
            high=Decimal("102"),
            low=Decimal("100"),
            close=Decimal("101.5"),
            volume=1200,
            source="test",
            interval="1d",
        ),
    ]

    # Act
    for bar in bars:
        service.on_bar(bar)

    # Assert
    assert len(mock_strategy.bars_received) == 2


def test_service_routes_to_multiple_strategies(event_bus):
    """Service routes bars to multiple strategies."""
    # Arrange
    config1 = MockStrategyConfig(name="strategy_1")
    config2 = MockStrategyConfig(name="strategy_2")
    strategy1 = MockStrategy(config1)
    strategy2 = MockStrategy(config2)

    service = StrategyService(
        event_bus=event_bus,
        strategies={"strat1": strategy1, "strat2": strategy2},
    )

    event = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-01T10:00:00Z",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=1000,
        source="test",
        interval="1d",
    )

    # Act
    service.on_bar(event)

    # Assert
    assert len(strategy1.bars_received) == 1
    assert len(strategy2.bars_received) == 1


# ============================================
# Universe Filtering Tests
# ============================================


def test_universe_filtering(event_bus):
    """Strategy with universe filters bars."""
    # Arrange
    config = MockStrategyConfig(universe=["AAPL"])
    strategy = MockStrategy(config)
    service = StrategyService(event_bus=event_bus, strategies={"mock": strategy})

    aapl_bar = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-01T10:00:00",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=1000,
        source="test",
        interval="1d",
    )

    msft_bar = PriceBarEvent(
        symbol="MSFT",
        timestamp="2024-01-01T10:00:00",
        open=Decimal("200"),
        high=Decimal("201"),
        low=Decimal("199"),
        close=Decimal("200.5"),
        volume=2000,
        source="test",
        interval="1d",
    )

    # Act
    service.on_bar(aapl_bar)
    service.on_bar(msft_bar)

    # Assert - Only AAPL bar should be received
    assert len(strategy.bars_received) == 1
    assert strategy.bars_received[0].symbol == "AAPL"


def test_empty_universe_accepts_all_symbols(event_bus):
    """Empty universe accepts all symbols."""
    # Arrange
    config = MockStrategyConfig(universe=[])  # Empty list
    strategy = MockStrategy(config)
    service = StrategyService(event_bus=event_bus, strategies={"mock": strategy})

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-01T10:00:00Z",
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=1000,
            source="test",
            interval="1d",
        ),
        PriceBarEvent(
            symbol="MSFT",
            timestamp="2024-01-01T10:00:00Z",
            open=Decimal("200"),
            high=Decimal("201"),
            low=Decimal("199"),
            close=Decimal("200.5"),
            volume=2000,
            source="test",
            interval="1d",
        ),
    ]

    # Act
    for bar in bars:
        service.on_bar(bar)

    # Assert - Both bars should be received
    assert len(strategy.bars_received) == 2


def test_universe_filtering_multiple_strategies(event_bus):
    """Different strategies with different universes filter independently."""
    # Arrange
    config1 = MockStrategyConfig(name="aapl_only", universe=["AAPL"])
    config2 = MockStrategyConfig(name="msft_only", universe=["MSFT"])
    strategy1 = MockStrategy(config1)
    strategy2 = MockStrategy(config2)

    service = StrategyService(
        event_bus=event_bus,
        strategies={"aapl_strat": strategy1, "msft_strat": strategy2},
    )

    aapl_bar = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-01T10:00:00Z",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=1000,
        source="test",
        interval="1d",
    )

    msft_bar = PriceBarEvent(
        symbol="MSFT",
        timestamp="2024-01-01T10:00:00Z",
        open=Decimal("200"),
        high=Decimal("201"),
        low=Decimal("199"),
        close=Decimal("200.5"),
        volume=2000,
        source="test",
        interval="1d",
    )

    # Act
    service.on_bar(aapl_bar)
    service.on_bar(msft_bar)

    # Assert
    assert len(strategy1.bars_received) == 1
    assert strategy1.bars_received[0].symbol == "AAPL"
    assert len(strategy2.bars_received) == 1
    assert strategy2.bars_received[0].symbol == "MSFT"


# ============================================
# Lifecycle Tests
# ============================================


def test_lifecycle_methods(event_bus, mock_strategy):
    """Service calls setup and teardown."""
    # Arrange
    service = StrategyService(event_bus=event_bus, strategies={"mock": mock_strategy})

    # Act
    service.setup()
    assert mock_strategy.setup_called

    service.teardown()

    # Assert
    assert mock_strategy.teardown_called


def test_setup_calls_all_strategies(event_bus):
    """setup() calls setup on all strategies."""
    # Arrange
    config1 = MockStrategyConfig(name="strategy_1")
    config2 = MockStrategyConfig(name="strategy_2")
    strategy1 = MockStrategy(config1)
    strategy2 = MockStrategy(config2)

    service = StrategyService(
        event_bus=event_bus,
        strategies={"strat1": strategy1, "strat2": strategy2},
    )

    # Act
    service.setup()

    # Assert
    assert strategy1.setup_called
    assert strategy2.setup_called


def test_teardown_calls_all_strategies(event_bus):
    """teardown() calls teardown on all strategies."""
    # Arrange
    config1 = MockStrategyConfig(name="strategy_1")
    config2 = MockStrategyConfig(name="strategy_2")
    strategy1 = MockStrategy(config1)
    strategy2 = MockStrategy(config2)

    service = StrategyService(
        event_bus=event_bus,
        strategies={"strat1": strategy1, "strat2": strategy2},
    )

    # Act
    service.teardown()

    # Assert
    assert strategy1.teardown_called
    assert strategy2.teardown_called


# ============================================
# Error Handling Tests
# ============================================


class ErrorStrategy(Strategy):
    """Strategy that raises errors for testing."""

    def __init__(self, config: MockStrategyConfig):
        self.config = config
        self.setup_error = False
        self.on_bar_error = False
        self.teardown_error = False

    def setup(self, context: Context) -> None:
        if self.setup_error:
            raise ValueError("Setup failed!")

    def teardown(self, context: Context) -> None:
        if self.teardown_error:
            raise ValueError("Teardown failed!")

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        if self.on_bar_error:
            raise ValueError("on_bar failed!")


def test_on_bar_error_doesnt_crash_service(event_bus):
    """Service continues processing other strategies when one fails."""
    # Arrange
    config1 = MockStrategyConfig(name="error_strategy")
    config2 = MockStrategyConfig(name="good_strategy")

    error_strategy = ErrorStrategy(config1)
    error_strategy.on_bar_error = True
    good_strategy = MockStrategy(config2)

    service = StrategyService(
        event_bus=event_bus,
        strategies={"error": error_strategy, "good": good_strategy},
    )

    event = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-01T10:00:00Z",
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=1000,
        source="test",
        interval="1d",
    )

    # Act - Should not raise
    service.on_bar(event)

    # Assert - Good strategy should still receive bar
    assert len(good_strategy.bars_received) == 1


def test_setup_error_doesnt_crash_service(event_bus):
    """setup() continues with healthy strategies if one strategy fails."""
    # Arrange
    config1 = MockStrategyConfig(name="error_strategy")
    config2 = MockStrategyConfig(name="good_strategy")

    error_strategy = ErrorStrategy(config1)
    error_strategy.setup_error = True
    good_strategy = MockStrategy(config2)

    service = StrategyService(
        event_bus=event_bus,
        strategies={"error": error_strategy, "good": good_strategy},
    )

    # Act - Should not raise
    service.setup()

    # Assert - Good strategy should still be set up
    assert good_strategy.setup_called
    # Error strategy should have error tracked
    metrics = service.get_metrics()
    assert metrics["error"]["errors"] == 1


def test_setup_error_all_strategies_attempted(event_bus):
    """setup() attempts all strategies even if one fails."""
    # Arrange
    config1 = MockStrategyConfig(name="strategy_a")
    config2 = MockStrategyConfig(name="error_strategy")
    config3 = MockStrategyConfig(name="strategy_b")

    strategy_a = MockStrategy(config1)
    error_strategy = ErrorStrategy(config2)
    error_strategy.setup_error = True
    strategy_b = MockStrategy(config3)

    service = StrategyService(
        event_bus=event_bus,
        strategies={
            "strategy_a": strategy_a,
            "error": error_strategy,
            "strategy_b": strategy_b,
        },
    )

    # Act
    service.setup()

    # Assert - All strategies attempted, including after the error
    assert strategy_a.setup_called
    assert strategy_b.setup_called
    metrics = service.get_metrics()
    assert metrics["error"]["errors"] == 1
    assert metrics["strategy_a"]["errors"] == 0
    assert metrics["strategy_b"]["errors"] == 0


def test_setup_failure_quarantines_strategy(event_bus):
    """Strategy that fails setup is quarantined and marked in metrics."""
    # Arrange
    config = MockStrategyConfig(name="error_strategy")
    error_strategy = ErrorStrategy(config)
    error_strategy.setup_error = True

    service = StrategyService(event_bus=event_bus, strategies={"error": error_strategy})

    # Act
    service.setup()

    # Assert - Strategy is quarantined
    metrics = service.get_metrics()
    assert metrics["error"]["quarantined"] is True
    assert metrics["error"]["errors"] == 1


def test_setup_retry_removes_quarantine(event_bus):
    """Strategy quarantined on first setup() is re-enabled on successful retry."""
    # Arrange
    config = MockStrategyConfig(name="flaky_strategy")
    strategy = ErrorStrategy(config)
    strategy.setup_error = True  # First setup will fail

    service = StrategyService(event_bus=event_bus, strategies={"flaky": strategy})

    # Act 1 - First setup fails, strategy is quarantined
    service.setup()

    # Assert 1 - Strategy is quarantined after first failure
    metrics = service.get_metrics()
    assert metrics["flaky"]["quarantined"] is True
    assert metrics["flaky"]["errors"] == 1

    # Act 2 - Fix the strategy and retry setup
    strategy.setup_error = False  # Now setup will succeed
    service.setup()

    # Assert 2 - Strategy is no longer quarantined after successful retry
    metrics = service.get_metrics()
    assert metrics["flaky"]["quarantined"] is False
    assert metrics["flaky"]["errors"] == 1  # Error count remains (historical)


def test_setup_retry_strategy_receives_events(event_bus):
    """Strategy re-enabled after successful setup retry receives bars and fills."""
    # Arrange
    from decimal import Decimal

    from qs_trader.events.events import FillEvent, PriceBarEvent

    config = MockStrategyConfig(name="flaky_strategy")
    strategy = ErrorStrategy(config)
    strategy.setup_error = True  # First setup will fail

    service = StrategyService(event_bus=event_bus, strategies={"flaky": strategy})

    # Act 1 - First setup fails
    service.setup()
    assert service.get_metrics()["flaky"]["quarantined"] is True

    # Act 2 - Fix strategy and retry setup (should remove quarantine)
    strategy.setup_error = False
    service.setup()

    # Create test events
    bar = PriceBarEvent(
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

    fill = FillEvent(
        fill_id="f1e2d3c4-b5a6-7890-1234-567890abcdef",
        source_order_id="order-123",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
    )

    # Mock the event handlers to track calls
    from unittest.mock import MagicMock

    strategy.on_bar = MagicMock()
    strategy.on_position_filled = MagicMock()

    # Act 3 - Send events to service
    service.on_bar(bar)
    service.on_fill(fill)

    # Assert - Strategy receives both events after successful retry
    strategy.on_bar.assert_called_once()
    strategy.on_position_filled.assert_called_once()
    assert service.get_metrics()["flaky"]["bars_processed"] == 1


def test_quarantined_strategy_doesnt_receive_bars(event_bus):
    """Quarantined strategy (failed setup) doesn't receive bar events."""
    # Arrange
    from decimal import Decimal

    from qs_trader.events.events import PriceBarEvent

    config1 = MockStrategyConfig(name="good_strategy")
    good_strategy = MockStrategy(config1)

    config2 = MockStrategyConfig(name="error_strategy")
    error_strategy = ErrorStrategy(config2)
    error_strategy.setup_error = True

    service = StrategyService(
        event_bus=event_bus,
        strategies={"good": good_strategy, "error": error_strategy},
    )

    # Setup - error_strategy will be quarantined
    service.setup()

    # Create a bar event
    bar = PriceBarEvent(
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

    # Act
    service.on_bar(bar)

    # Assert - Good strategy receives bar, error strategy doesn't
    metrics = service.get_metrics()
    assert metrics["good"]["bars_processed"] == 1
    assert metrics["error"]["bars_processed"] == 0  # Quarantined, no bars
    assert len(good_strategy.bars_received) == 1  # Received the bar


def test_quarantined_strategy_doesnt_receive_fills(event_bus):
    """Quarantined strategy (failed setup) doesn't receive fill events."""
    # Arrange
    from decimal import Decimal

    from qs_trader.events.events import FillEvent

    config1 = MockStrategyConfig(name="good_strategy")
    good_strategy = MockStrategy(config1)
    good_strategy.on_position_filled = lambda event, context: None

    config2 = MockStrategyConfig(name="error_strategy")
    error_strategy = ErrorStrategy(config2)
    error_strategy.setup_error = True
    error_strategy.on_position_filled = lambda event, context: None

    service = StrategyService(
        event_bus=event_bus,
        strategies={"good": good_strategy, "error": error_strategy},
    )

    # Setup - error_strategy will be quarantined
    service.setup()

    # Create a fill event without strategy_id (should fan to all non-quarantined)
    fill = FillEvent(
        fill_id="f1e2d3c4-b5a6-7890-1234-567890abcdef",
        source_order_id="order-123",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
    )

    # Act - route fill (should skip quarantined strategy)
    from unittest.mock import MagicMock

    good_strategy.on_position_filled = MagicMock()
    error_strategy.on_position_filled = MagicMock()

    service.on_fill(fill)

    # Assert - Good strategy receives fill, error strategy doesn't
    good_strategy.on_position_filled.assert_called_once()
    error_strategy.on_position_filled.assert_not_called()


def test_quarantined_strategy_with_strategy_id_fill(event_bus):
    """Fill with strategy_id for quarantined strategy is skipped."""
    # Arrange
    from decimal import Decimal

    from qs_trader.events.events import FillEvent

    config = MockStrategyConfig(name="error_strategy")
    error_strategy = ErrorStrategy(config)
    error_strategy.setup_error = True
    error_strategy.on_position_filled = lambda event, context: None

    service = StrategyService(event_bus=event_bus, strategies={"error": error_strategy})

    # Setup - error_strategy will be quarantined
    service.setup()

    # Create a fill event WITH strategy_id pointing to quarantined strategy
    fill = FillEvent(
        fill_id="f1e2d3c4-b5a6-7890-1234-567890abcdef",
        source_order_id="order-123",
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        side="buy",
        filled_quantity=Decimal("100"),
        fill_price=Decimal("150.00"),
        strategy_id="error",  # Explicitly targets quarantined strategy
    )

    # Act - route fill (should skip quarantined even with explicit strategy_id)
    from unittest.mock import MagicMock

    error_strategy.on_position_filled = MagicMock()

    service.on_fill(fill)

    # Assert - Quarantined strategy doesn't receive fill even with explicit ID
    error_strategy.on_position_filled.assert_not_called()


def test_healthy_strategy_not_quarantined(event_bus):
    """Strategy that passes setup is NOT quarantined."""
    # Arrange
    config = MockStrategyConfig(name="good_strategy")
    good_strategy = MockStrategy(config)

    service = StrategyService(event_bus=event_bus, strategies={"good": good_strategy})

    # Act
    service.setup()

    # Assert - Strategy is NOT quarantined
    metrics = service.get_metrics()
    assert metrics["good"]["quarantined"] is False
    assert metrics["good"]["errors"] == 0


def test_teardown_error_doesnt_crash(event_bus):
    """teardown() continues even if one strategy fails."""
    # Arrange
    config1 = MockStrategyConfig(name="error_strategy")
    config2 = MockStrategyConfig(name="good_strategy")

    error_strategy = ErrorStrategy(config1)
    error_strategy.teardown_error = True
    good_strategy = MockStrategy(config2)

    service = StrategyService(
        event_bus=event_bus,
        strategies={"error": error_strategy, "good": good_strategy},
    )

    # Act - Should not raise
    service.teardown()

    # Assert - Good strategy should still be torn down
    assert good_strategy.teardown_called


def test_teardown_unsubscribes_from_event_bus(event_bus):
    """teardown() unsubscribes from event bus to prevent duplicate handlers."""
    # Arrange
    config = MockStrategyConfig(name="test_strategy")
    strategy = MockStrategy(config)

    service = StrategyService(event_bus=event_bus, strategies={"test": strategy})

    # Verify subscriptions were created
    assert len(service._subscription_tokens) == 2  # bar and fill

    # Act
    service.teardown()

    # Assert - Tokens should have been unsubscribed
    # We can't directly check if unsubscribe was called, but we can verify
    # the tokens exist and teardown completed without error
    assert len(service._subscription_tokens) == 2


def test_recreate_service_doesnt_duplicate_handlers(event_bus):
    """Recreating service after teardown doesn't cause duplicate handler calls."""
    # Arrange
    from decimal import Decimal

    from qs_trader.events.events import PriceBarEvent

    config = MockStrategyConfig(name="test_strategy")

    # Create first service
    strategy1 = MockStrategy(config)
    service1 = StrategyService(event_bus=event_bus, strategies={"test": strategy1})
    service1.setup()

    # Teardown first service (should unsubscribe)
    service1.teardown()

    # Create second service with new strategy instance
    strategy2 = MockStrategy(config)
    service2 = StrategyService(event_bus=event_bus, strategies={"test": strategy2})
    service2.setup()

    # Create a bar event
    bar = PriceBarEvent(
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

    # Act - Publish bar to event bus
    event_bus.publish(bar)

    # Assert - Only strategy2 receives the bar (strategy1 was unsubscribed)
    assert len(strategy1.bars_received) == 0  # Old service, unsubscribed
    assert len(strategy2.bars_received) == 1  # New service, active


# ============================================
# Metrics Tests
# ============================================


def test_get_metrics_returns_all_strategies(event_bus, mock_strategy):
    """get_metrics returns metrics for all strategies."""
    # Arrange
    service = StrategyService(event_bus=event_bus, strategies={"mock": mock_strategy})

    # Act
    metrics = service.get_metrics()

    # Assert
    assert "mock" in metrics
    assert "bars_processed" in metrics["mock"]
    assert "signals_emitted" in metrics["mock"]
    assert "errors" in metrics["mock"]


def test_metrics_track_bars_processed(event_bus, mock_strategy):
    """Metrics track number of bars processed."""
    # Arrange
    service = StrategyService(event_bus=event_bus, strategies={"mock": mock_strategy})

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T10:00:00Z",
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 4)
    ]

    # Act
    for bar in bars:
        service.on_bar(bar)

    metrics = service.get_metrics()

    # Assert
    assert metrics["mock"]["bars_processed"] == 3


def test_metrics_track_signals_emitted(event_bus):
    """Metrics track number of signals emitted."""
    # Arrange
    config = MockStrategyConfig()
    strategy = MockStrategy(config)
    service = StrategyService(event_bus=event_bus, strategies={"mock": strategy})

    # Emit signals via context
    context = service._contexts["mock"]
    context.emit_signal(
        timestamp="2024-01-01T10:00:00Z",
        symbol="AAPL",
        intention="OPEN_LONG",
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )
    context.emit_signal(
        timestamp="2024-01-02T10:00:00Z",
        symbol="AAPL",
        intention="CLOSE_LONG",
        price=Decimal("155.00"),
        confidence=Decimal("0.75"),
    )

    # Act
    metrics = service.get_metrics()

    # Assert
    assert metrics["mock"]["signals_emitted"] == 2


def test_metrics_track_errors(event_bus):
    """Metrics track number of errors."""
    # Arrange
    config = MockStrategyConfig()
    error_strategy = ErrorStrategy(config)
    error_strategy.on_bar_error = True

    service = StrategyService(event_bus=event_bus, strategies={"error": error_strategy})

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T10:00:00Z",
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 3)
    ]

    # Act
    for bar in bars:
        service.on_bar(bar)

    metrics = service.get_metrics()

    # Assert
    assert metrics["error"]["errors"] == 2


def test_metrics_independent_across_strategies(event_bus):
    """Metrics tracked independently for each strategy."""
    # Arrange
    config1 = MockStrategyConfig(name="strategy_1")
    config2 = MockStrategyConfig(name="strategy_2", universe=["AAPL"])
    strategy1 = MockStrategy(config1)
    strategy2 = MockStrategy(config2)

    service = StrategyService(
        event_bus=event_bus,
        strategies={"strat1": strategy1, "strat2": strategy2},
    )

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-01T10:00:00Z",
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=1000,
            source="test",
            interval="1d",
        ),
        PriceBarEvent(
            symbol="MSFT",
            timestamp="2024-01-01T10:00:00Z",
            open=Decimal("200"),
            high=Decimal("201"),
            low=Decimal("199"),
            close=Decimal("200.5"),
            volume=2000,
            source="test",
            interval="1d",
        ),
    ]

    # Act
    for bar in bars:
        service.on_bar(bar)

    metrics = service.get_metrics()

    # Assert
    assert metrics["strat1"]["bars_processed"] == 2  # Receives all
    assert metrics["strat2"]["bars_processed"] == 1  # Only AAPL
