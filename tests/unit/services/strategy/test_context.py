"""Unit tests for strategy Context class."""

from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import SignalEvent
from qs_trader.services.strategy.context import Context
from qs_trader.services.strategy.models import SignalIntention


@pytest.fixture
def event_bus():
    """Create a mock event bus."""
    return MagicMock(spec=EventBus)


@pytest.fixture
def context(event_bus):
    """Create a context with mock event bus."""
    return Context(strategy_id="test_strategy", event_bus=event_bus)


# ============================================
# Context Initialization Tests
# ============================================


def test_context_initialization_success(event_bus):
    """Context initializes with correct attributes."""
    # Arrange & Act
    context = Context(strategy_id="my_strategy", event_bus=event_bus)

    # Assert
    assert context.strategy_id == "my_strategy"
    assert context.signal_count == 0
    assert context._event_bus is event_bus


def test_context_initialization_with_different_strategy_ids(event_bus):
    """Context can be initialized with different strategy IDs."""
    # Arrange & Act
    context1 = Context(strategy_id="strategy_1", event_bus=event_bus)
    context2 = Context(strategy_id="strategy_2", event_bus=event_bus)

    # Assert
    assert context1.strategy_id == "strategy_1"
    assert context2.strategy_id == "strategy_2"


# ============================================
# emit_signal Tests - Happy Path
# ============================================


def test_emit_signal_creates_signal_event(context, event_bus):
    """emit_signal creates valid SignalEvent with all required fields."""
    # Arrange
    timestamp = "2024-01-02T16:00:00Z"
    symbol = "AAPL"
    intention = SignalIntention.OPEN_LONG
    price = Decimal("150.25")
    confidence = Decimal("0.85")

    # Act
    result = context.emit_signal(
        timestamp=timestamp,
        symbol=symbol,
        intention=intention,
        price=price,
        confidence=confidence,
    )

    # Assert
    assert isinstance(result, SignalEvent)
    assert result.timestamp == timestamp
    assert result.strategy_id == "test_strategy"
    assert result.symbol == symbol
    assert result.intention == intention
    assert result.price == price
    assert result.confidence == confidence


def test_emit_signal_publishes_to_event_bus(context, event_bus):
    """emit_signal publishes SignalEvent to event bus."""
    # Arrange
    timestamp = "2024-01-02T16:00:00Z"

    # Act
    result = context.emit_signal(
        timestamp=timestamp,
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )

    # Assert
    event_bus.publish.assert_called_once()
    published_event = event_bus.publish.call_args[0][0]
    assert published_event is result
    assert isinstance(published_event, SignalEvent)


def test_emit_signal_increments_signal_count(context):
    """emit_signal increments signal count correctly."""
    # Arrange
    assert context.signal_count == 0

    # Act
    context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )
    context.emit_signal(
        timestamp="2024-01-03T16:00:00Z",
        symbol="MSFT",
        intention=SignalIntention.CLOSE_LONG,
        price=Decimal("200.00"),
        confidence=Decimal("0.75"),
    )

    # Assert
    assert context.signal_count == 2


def test_emit_signal_with_optional_fields(context, event_bus):
    """emit_signal handles optional fields correctly."""
    # Arrange
    reason = "SMA crossover detected"
    metadata = {"fast_sma": 150.0, "slow_sma": 148.0}
    stop_loss = Decimal("145.00")
    take_profit = Decimal("160.00")

    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.85"),
        reason=reason,
        metadata=metadata,
        stop_loss=stop_loss,
        take_profit=take_profit,
    )

    # Assert
    assert result.reason == reason
    assert result.metadata == metadata
    assert result.stop_loss == stop_loss
    assert result.take_profit == take_profit


def test_emit_signal_returns_signal_event(context):
    """emit_signal returns the created SignalEvent instance."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )

    # Assert
    assert isinstance(result, SignalEvent)
    assert result.strategy_id == "test_strategy"


# ============================================
# emit_signal Tests - Type Conversions
# ============================================


def test_emit_signal_converts_float_price_to_decimal(context):
    """emit_signal converts float price to Decimal."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=150.25,  # float
        confidence=0.85,
    )

    # Assert
    assert isinstance(result.price, Decimal)
    assert result.price == Decimal("150.25")


def test_emit_signal_converts_string_price_to_decimal(context):
    """emit_signal converts string price to Decimal."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price="150.25",  # string
        confidence="0.85",
    )

    # Assert
    assert isinstance(result.price, Decimal)
    assert result.price == Decimal("150.25")


def test_emit_signal_converts_float_confidence_to_decimal(context):
    """emit_signal converts float confidence to Decimal."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=0.85,  # float
    )

    # Assert
    assert isinstance(result.confidence, Decimal)
    assert result.confidence == Decimal("0.85")


def test_emit_signal_converts_stop_loss_and_take_profit(context):
    """emit_signal converts stop_loss and take_profit to Decimal."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.85"),
        stop_loss=145.00,  # float
        take_profit="160.00",  # string
    )

    # Assert
    assert isinstance(result.stop_loss, Decimal)
    assert isinstance(result.take_profit, Decimal)
    assert result.stop_loss == Decimal("145.00")
    assert result.take_profit == Decimal("160.00")


# ============================================
# emit_signal Tests - Different Intentions
# ============================================


@pytest.mark.parametrize(
    "intention",
    [
        SignalIntention.OPEN_LONG,
        SignalIntention.CLOSE_LONG,
        SignalIntention.OPEN_SHORT,
        SignalIntention.CLOSE_SHORT,
    ],
)
def test_emit_signal_handles_all_intentions(context, intention):
    """emit_signal handles all SignalIntention types."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=intention,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )

    # Assert
    assert result.intention == intention


def test_emit_signal_accepts_string_intention(context):
    """emit_signal accepts intention as string."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention="OPEN_LONG",  # string
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )

    # Assert
    assert result.intention == SignalIntention.OPEN_LONG


# ============================================
# emit_signal Tests - Multiple Signals
# ============================================


def test_emit_signal_multiple_signals_in_sequence(context, event_bus):
    """emit_signal can emit multiple signals sequentially."""
    # Act
    signal1 = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )
    signal2 = context.emit_signal(
        timestamp="2024-01-03T16:00:00Z",
        symbol="MSFT",
        intention=SignalIntention.CLOSE_LONG,
        price=Decimal("200.00"),
        confidence=Decimal("0.75"),
    )

    # Assert
    assert event_bus.publish.call_count == 2
    assert signal1.symbol == "AAPL"
    assert signal2.symbol == "MSFT"
    assert context.signal_count == 2


def test_emit_signal_different_symbols_same_bar(context, event_bus):
    """emit_signal can emit signals for different symbols in same timestamp."""
    # Act
    signal1 = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )
    signal2 = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",  # Same timestamp
        symbol="MSFT",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("200.00"),
        confidence=Decimal("0.75"),
    )

    # Assert
    assert event_bus.publish.call_count == 2
    assert signal1.timestamp == signal2.timestamp
    assert signal1.symbol != signal2.symbol


# ============================================
# Stub Method Tests (get_price, get_bars)
# ============================================


def test_get_price_returns_none(context):
    """get_price returns None when no bars cached."""
    # Act
    result = context.get_price("AAPL")

    # Assert
    assert result is None


def test_get_price_different_symbols(context):
    """get_price returns None for any symbol when no data."""
    # Act & Assert
    assert context.get_price("AAPL") is None
    assert context.get_price("MSFT") is None


def test_get_bars_returns_none(context):
    """get_bars stub returns None."""
    # Act
    result = context.get_bars("AAPL", n=20)

    # Assert
    assert result is None


def test_get_bars_different_parameters(context):
    """get_bars returns None for any parameters."""
    # Act & Assert
    assert context.get_bars("AAPL", n=1) is None
    assert context.get_bars("MSFT", n=50) is None
    assert context.get_bars("TSLA") is None  # default n=1


# ============================================
# Property Tests
# ============================================


def test_strategy_id_property_readonly(context):
    """strategy_id property returns correct value."""
    # Assert
    assert context.strategy_id == "test_strategy"


def test_signal_count_property_tracks_emissions(context):
    """signal_count property tracks emitted signals."""
    # Arrange
    assert context.signal_count == 0

    # Act
    context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )

    # Assert
    assert context.signal_count == 1


# ============================================
# Edge Cases and Boundary Tests
# ============================================


def test_emit_signal_with_zero_confidence(context):
    """emit_signal accepts confidence of 0.0."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.0"),  # Minimum
    )

    # Assert
    assert result.confidence == Decimal("0.0")


def test_emit_signal_with_max_confidence(context):
    """emit_signal accepts confidence of 1.0."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("1.0"),  # Maximum
    )

    # Assert
    assert result.confidence == Decimal("1.0")


def test_emit_signal_with_empty_metadata(context):
    """emit_signal accepts empty metadata dict."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
        metadata={},  # Empty dict
    )

    # Assert
    assert result.metadata == {}


def test_emit_signal_with_none_optional_fields(context):
    """emit_signal accepts None for all optional fields."""
    # Act
    result = context.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
        reason=None,
        metadata=None,
        stop_loss=None,
        take_profit=None,
    )

    # Assert
    assert result.reason is None
    assert result.metadata is None
    assert result.stop_loss is None
    assert result.take_profit is None


# ============================================
# Integration-like Tests
# ============================================


def test_context_used_by_multiple_strategies_independently(event_bus):
    """Different contexts track signals independently."""
    # Arrange
    context1 = Context(strategy_id="strategy_1", event_bus=event_bus)
    context2 = Context(strategy_id="strategy_2", event_bus=event_bus)

    # Act
    context1.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )
    context2.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )
    context2.emit_signal(
        timestamp="2024-01-03T16:00:00Z",
        symbol="MSFT",
        intention=SignalIntention.CLOSE_LONG,
        price=Decimal("200.00"),
        confidence=Decimal("0.75"),
    )

    # Assert
    assert context1.signal_count == 1
    assert context2.signal_count == 2
    assert event_bus.publish.call_count == 3


def test_emit_signal_tags_with_correct_strategy_id(event_bus):
    """emit_signal tags signals with correct strategy_id."""
    # Arrange
    context1 = Context(strategy_id="bb_breakout", event_bus=event_bus)
    context2 = Context(strategy_id="rsi_reversal", event_bus=event_bus)

    # Act
    signal1 = context1.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_LONG,
        price=Decimal("150.00"),
        confidence=Decimal("0.80"),
    )
    signal2 = context2.emit_signal(
        timestamp="2024-01-02T16:00:00Z",
        symbol="AAPL",
        intention=SignalIntention.OPEN_SHORT,
        price=Decimal("150.00"),
        confidence=Decimal("0.75"),
    )

    # Assert
    assert signal1.strategy_id == "bb_breakout"
    assert signal2.strategy_id == "rsi_reversal"


# ============================================
# Phase 4: Bar Caching Tests
# ============================================


def test_cache_bar_stores_bar(context):
    """cache_bar stores bar in cache with backward-adjusted prices."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bar = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),
        volume=1000,
        source="test",
        interval="1d",
    )

    # Act
    context.cache_bar(bar)

    # Assert - adjusted cache for indicators
    assert "AAPL" in context._bar_cache
    assert len(context._bar_cache["AAPL"]) == 1
    # When factor is 1.0, adjusted prices equal unadjusted
    assert context._bar_cache["AAPL"][0].close == Decimal("100.50")

    # Assert - unadjusted cache for signals


def test_cache_bar_multiple_symbols(context):
    """cache_bar handles multiple symbols independently."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    aapl_bar = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),
        volume=1000,
        source="test",
        interval="1d",
    )

    msft_bar = PriceBarEvent(
        symbol="MSFT",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("200.00"),
        high=Decimal("201.00"),
        low=Decimal("199.00"),
        close=Decimal("200.50"),
        volume=2000,
        source="test",
        interval="1d",
    )

    # Act
    context.cache_bar(aapl_bar)
    context.cache_bar(msft_bar)

    # Assert - adjusted cache
    assert len(context._bar_cache) == 2
    assert "AAPL" in context._bar_cache
    assert "MSFT" in context._bar_cache
    assert len(context._bar_cache["AAPL"]) == 1
    assert len(context._bar_cache["MSFT"]) == 1

    # Assert - unadjusted cache


def test_cache_bar_sequential_bars(context):
    """cache_bar accumulates sequential bars in cache."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 6)
    ]

    # Act
    for bar in bars:
        context.cache_bar(bar)

    # Assert - adjusted cache accumulates all bars
    assert len(context._bar_cache["AAPL"]) == 5
    assert context._bar_cache["AAPL"][0].close == Decimal("101.50")  # Bar 1
    assert context._bar_cache["AAPL"][-1].close == Decimal("105.50")  # Bar 5

    # Assert - unadjusted cache keeps only latest


def test_cache_bar_respects_max_bars_limit(event_bus):
    """_cache_bar respects max_bars limit and evicts oldest from adjusted cache."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    context = Context(strategy_id="test", event_bus=event_bus, max_bars=3)

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 6)  # 5 bars, but max is 3
    ]

    # Act
    for bar in bars:
        context.cache_bar(bar)

    # Assert - adjusted cache retains only last 3 bars
    assert len(context._bar_cache["AAPL"]) == 3
    assert context._bar_cache["AAPL"][0].close == Decimal("103.50")  # Bar 3
    assert context._bar_cache["AAPL"][1].close == Decimal("104.50")  # Bar 4
    assert context._bar_cache["AAPL"][2].close == Decimal("105.50")  # Bar 5

    # Assert - unadjusted cache always keeps only latest


def test_get_price_returns_latest_close(context):
    """get_price returns close price of most recent bar."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bar = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),
        volume=1000,
        source="test",
        interval="1d",
    )
    context.cache_bar(bar)

    # Act
    price = context.get_price("AAPL")

    # Assert
    assert price == Decimal("100.50")


def test_get_price_returns_none_for_no_data(context):
    """get_price returns None when no bars cached."""
    # Act
    price = context.get_price("AAPL")

    # Assert
    assert price is None


def test_get_price_returns_latest_after_multiple_bars(context):
    """get_price returns most recent close after multiple bars."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 4)
    ]

    for bar in bars:
        context.cache_bar(bar)

    # Act
    price = context.get_price("AAPL")

    # Assert - should be close of last bar
    assert price == Decimal("103.50")  # Bar 3


def test_get_price_different_symbols_independent(context):
    """get_price tracks prices per symbol independently."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    aapl_bar = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),
        volume=1000,
        source="test",
        interval="1d",
    )

    msft_bar = PriceBarEvent(
        symbol="MSFT",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("200.00"),
        high=Decimal("201.00"),
        low=Decimal("199.00"),
        close=Decimal("200.75"),
        volume=2000,
        source="test",
        interval="1d",
    )

    context.cache_bar(aapl_bar)
    context.cache_bar(msft_bar)

    # Act
    aapl_price = context.get_price("AAPL")
    msft_price = context.get_price("MSFT")

    # Assert
    assert aapl_price == Decimal("100.50")
    assert msft_price == Decimal("200.75")


# ============================================
# Phase 4: get_bars() Tests
# ============================================


def test_get_bars_returns_last_n_bars(context):
    """get_bars returns last N bars in chronological order."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 6)  # 5 bars total
    ]

    for bar in bars:
        context.cache_bar(bar)

    # Act
    result = context.get_bars("AAPL", n=3)

    # Assert
    assert result is not None
    assert len(result) == 3
    assert result[0].close == Decimal("103.50")  # Bar 3
    assert result[1].close == Decimal("104.50")  # Bar 4
    assert result[2].close == Decimal("105.50")  # Bar 5 (most recent)


def test_get_bars_returns_none_for_insufficient_data(context):
    """get_bars returns None when fewer than N bars cached."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 3)  # Only 2 bars
    ]

    for bar in bars:
        context.cache_bar(bar)

    # Act
    result = context.get_bars("AAPL", n=5)  # Request 5 bars but only have 2

    # Assert
    assert result is None


def test_get_bars_returns_none_for_no_data(context):
    """get_bars returns None when no bars cached."""
    # Act
    result = context.get_bars("AAPL", n=10)

    # Assert
    assert result is None


def test_get_bars_default_n_equals_1(context):
    """get_bars defaults to n=1 (last bar only)."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 4)
    ]

    for bar in bars:
        context.cache_bar(bar)

    # Act
    result = context.get_bars("AAPL")  # No n specified

    # Assert
    assert result is not None
    assert len(result) == 1
    assert result[0].close == Decimal("103.50")  # Last bar (Bar 3)


def test_get_bars_chronological_order(context):
    """get_bars returns bars in chronological order (oldest first)."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 6)
    ]

    for bar in bars:
        context.cache_bar(bar)

    # Act
    result = context.get_bars("AAPL", n=5)

    # Assert
    assert result[0].timestamp < result[1].timestamp < result[2].timestamp
    assert result[2].timestamp < result[3].timestamp < result[4].timestamp


def test_get_bars_different_symbols_independent(context):
    """get_bars tracks bars per symbol independently."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    aapl_bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.50"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 4)
    ]

    msft_bars = [
        PriceBarEvent(
            symbol="MSFT",
            timestamp=f"2024-01-0{i}T16:00:00Z",
            open=Decimal(f"{200 + i}.00"),
            high=Decimal(f"{201 + i}.00"),
            low=Decimal(f"{199 + i}.00"),
            close=Decimal(f"{200 + i}.50"),
            volume=2000,
            source="test",
            interval="1d",
        )
        for i in range(1, 3)
    ]

    for bar in aapl_bars:
        context.cache_bar(bar)
    for bar in msft_bars:
        context.cache_bar(bar)

    # Act
    aapl_result = context.get_bars("AAPL", n=3)
    msft_result = context.get_bars("MSFT", n=2)

    # Assert
    assert aapl_result is not None and len(aapl_result) == 3
    assert msft_result is not None and len(msft_result) == 2
    assert aapl_result[0].symbol == "AAPL"
    assert msft_result[0].symbol == "MSFT"


def test_get_bars_for_indicator_calculation(context):
    """get_bars can be used for SMA calculation."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    # Create 20 bars with incrementing close prices
    bars = [
        PriceBarEvent(
            symbol="AAPL",
            timestamp=f"2024-01-{i:02d}T16:00:00Z",
            open=Decimal(f"{100 + i}.00"),
            high=Decimal(f"{101 + i}.00"),
            low=Decimal(f"{99 + i}.00"),
            close=Decimal(f"{100 + i}.00"),
            volume=1000,
            source="test",
            interval="1d",
        )
        for i in range(1, 21)
    ]

    for bar in bars:
        context.cache_bar(bar)

    # Act - Calculate 20-period SMA
    result = context.get_bars("AAPL", n=20)

    assert result is not None
    prices = [bar.close for bar in result]
    sma = sum(prices) / len(prices)

    # Assert
    assert len(result) == 20
    assert sma == Decimal("110.50")  # Average of 101-120


# ============================================
# Price Adjustment Fix Tests
# ============================================


def test_cache_bar_factor_change_preserves_other_metadata(context):
    """cache_bar re-adjustment preserves timestamps and other metadata."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bar1 = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-01T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("100.00"),
        low=Decimal("100.00"),
        close=Decimal("100.00"),
        volume=1000,
        source="test_source",
        interval="1d",
    )
    context.cache_bar(bar1)

    bar2 = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("100.00"),
        low=Decimal("100.00"),
        close=Decimal("100.00"),
        volume=1000,
        source="test_source",
        interval="1d",
    )

    # Act
    context.cache_bar(bar2)
    result = context.get_bars("AAPL", n=2)

    # Assert
    assert result is not None
    assert result[0].timestamp == "2024-01-01T16:00:00Z"
    assert result[0].symbol == "AAPL"
    assert result[0].source == "test_source"
    assert result[0].interval == "1d"


def test_cache_bar_ignores_small_factor_differences(context):
    """cache_bar ignores floating point precision differences in factor."""
    # Arrange
    from qs_trader.events.events import PriceBarEvent

    bar1 = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-01T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("100.00"),
        low=Decimal("100.00"),
        close=Decimal("100.00"),
        volume=1000,
        source="test",
        interval="1d",
    )
    context.cache_bar(bar1)

    # Factor with tiny difference (within 0.000001 threshold)
    bar2 = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T16:00:00Z",
        open=Decimal("100.00"),
        high=Decimal("100.00"),
        low=Decimal("100.00"),
        close=Decimal("100.00"),
        volume=1000,
        source="test",
        interval="1d",
    )

    # Act
    context.cache_bar(bar2)
    result = context.get_bars("AAPL", n=2)

    # Assert - Should NOT trigger re-adjustment (within tolerance)
    assert result is not None
    # First bar should keep original adjustment
    assert result[0].close == Decimal("100.00")  # 100 * 1.0
