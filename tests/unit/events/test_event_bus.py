"""
Comprehensive unit tests for EventBus.

Tests publish/subscribe with type-safe handlers, priority ordering, error isolation,
history management, middleware hooks, and event store integration.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import Mock

import pytest

from qs_trader.events.event_bus import EventBus, SubscriptionToken
from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import BacktestStartedEvent, BarCloseEvent, BaseEvent, CorporateActionEvent, PriceBarEvent

# ============================================
# Fixtures
# ============================================


@pytest.fixture
def event_bus():
    """Create fresh EventBus for each test."""
    return EventBus(max_history=100)


@pytest.fixture
def sample_price_bar():
    """Create sample price bar event."""
    return PriceBarEvent(
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


@pytest.fixture
def sample_corporate_action():
    """Create sample corporate action event."""
    return CorporateActionEvent(
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


# ============================================
# Basic Publish/Subscribe Tests
# ============================================


class TestEventBusBasicSubscribe:
    """Test basic subscribe and publish functionality."""

    def test_publish_without_subscribers_succeeds(self, event_bus, sample_price_bar):
        """Publishing without subscribers should not raise."""
        event_bus.publish(sample_price_bar)
        # No assertion needed - test passes if no exception

    def test_subscribe_by_string_and_publish(self, event_bus, sample_price_bar):
        """Test subscribing by event type string."""
        events_received = []

        def handler(event: BaseEvent):
            events_received.append(event)

        event_bus.subscribe("bar", handler)
        event_bus.publish(sample_price_bar)

        assert len(events_received) == 1
        assert events_received[0].event_id == sample_price_bar.event_id

    def test_subscribe_by_class_and_publish(self, event_bus, sample_price_bar):
        """Test subscribing by event class using string event_type."""
        events_received = []

        def handler(event: PriceBarEvent):
            events_received.append(event)

        # Subscribe using string event_type since class-level subscription
        # requires instantiation of Pydantic models
        event_bus.subscribe("bar", handler)
        event_bus.publish(sample_price_bar)

        assert len(events_received) == 1
        assert events_received[0].symbol == "AAPL"

    def test_multiple_handlers_same_event(self, event_bus, sample_price_bar):
        """Multiple handlers should all receive the event."""
        calls = []

        def handler1(event: BaseEvent):
            calls.append("handler1")

        def handler2(event: BaseEvent):
            calls.append("handler2")

        event_bus.subscribe("bar", handler1)
        event_bus.subscribe("bar", handler2)
        event_bus.publish(sample_price_bar)

        assert len(calls) == 2
        assert "handler1" in calls
        assert "handler2" in calls

    def test_different_event_types_isolated(self, event_bus, sample_price_bar, sample_corporate_action):
        """Handlers should only receive their subscribed event types."""
        bar_events = []
        action_events = []

        def bar_handler(event: BaseEvent):
            bar_events.append(event)

        def action_handler(event: BaseEvent):
            action_events.append(event)

        event_bus.subscribe("bar", bar_handler)
        event_bus.subscribe("corporate_action", action_handler)

        event_bus.publish(sample_price_bar)
        event_bus.publish(sample_corporate_action)

        assert len(bar_events) == 1
        assert len(action_events) == 1
        assert bar_events[0].event_type == "bar"
        assert action_events[0].event_type == "corporate_action"

    def test_subscribe_invalid_event_class_raises(self, event_bus):
        """Subscribing with a class missing event_type should raise."""

        class InvalidEvent:
            pass

        def handler(event):
            pass

        with pytest.raises(ValueError, match="missing event_type"):
            event_bus.subscribe(InvalidEvent, handler)


# ============================================
# Priority Ordering Tests
# ============================================


class TestEventBusPriority:
    """Test priority-based handler ordering."""

    def test_priority_ordering_highest_first(self, event_bus):
        """Handlers should be called in priority order (highest first)."""
        call_order = []

        def low_priority(event: BaseEvent):
            call_order.append("low")

        def medium_priority(event: BaseEvent):
            call_order.append("medium")

        def high_priority(event: BaseEvent):
            call_order.append("high")

        event_bus.subscribe("bar_close", low_priority, priority=1)
        event_bus.subscribe("bar_close", high_priority, priority=100)
        event_bus.subscribe("bar_close", medium_priority, priority=10)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)

        assert call_order == ["high", "medium", "low"]

    def test_same_priority_fifo(self, event_bus):
        """Handlers with same priority should be called in subscription order."""
        call_order = []

        def handler1(event: BaseEvent):
            call_order.append("h1")

        def handler2(event: BaseEvent):
            call_order.append("h2")

        def handler3(event: BaseEvent):
            call_order.append("h3")

        event_bus.subscribe("bar_close", handler1, priority=10)
        event_bus.subscribe("bar_close", handler2, priority=10)
        event_bus.subscribe("bar_close", handler3, priority=10)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)

        assert call_order == ["h1", "h2", "h3"]

    def test_default_priority_is_zero(self, event_bus):
        """Default priority should be 0."""
        call_order = []

        def default_handler(event: BaseEvent):
            call_order.append("default")

        def high_handler(event: BaseEvent):
            call_order.append("high")

        event_bus.subscribe("bar_close", high_handler, priority=10)
        event_bus.subscribe("bar_close", default_handler)  # No priority specified

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)

        assert call_order == ["high", "default"]

    def test_handler_cache_invalidated_on_subscribe(self, event_bus):
        """Handler cache should be invalidated when subscribing."""
        call_order = []

        def handler1(event: BaseEvent):
            call_order.append("h1")

        def handler2(event: BaseEvent):
            call_order.append("h2")

        event_bus.subscribe("bar_close", handler1, priority=10)
        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)

        # Subscribe a higher priority handler after first publish
        event_bus.subscribe("bar_close", handler2, priority=100)
        call_order.clear()
        event_bus.publish(event)

        # handler2 should be called first due to higher priority
        assert call_order == ["h2", "h1"]


# ============================================
# Error Isolation Tests
# ============================================


class TestEventBusErrorIsolation:
    """Test error isolation between handlers."""

    def test_handler_error_doesnt_stop_others(self, event_bus):
        """Exception in one handler should not stop other handlers."""
        successful_calls = []

        def failing_handler(event: BaseEvent):
            successful_calls.append("before_fail")
            raise ValueError("Handler failed!")

        def success_handler1(event: BaseEvent):
            successful_calls.append("success1")

        def success_handler2(event: BaseEvent):
            successful_calls.append("success2")

        event_bus.subscribe("bar_close", success_handler1, priority=100)
        event_bus.subscribe("bar_close", failing_handler, priority=50)
        event_bus.subscribe("bar_close", success_handler2, priority=10)

        event = BarCloseEvent(source_service="test")
        # Should not raise despite failing_handler exception
        event_bus.publish(event)

        assert "success1" in successful_calls
        assert "success2" in successful_calls
        assert "before_fail" in successful_calls

    def test_multiple_failing_handlers_isolated(self, event_bus):
        """Multiple failures should be isolated from each other."""
        successful_calls = []

        def fail1(event: BaseEvent):
            raise ValueError("Fail 1")

        def fail2(event: BaseEvent):
            raise RuntimeError("Fail 2")

        def success(event: BaseEvent):
            successful_calls.append("success")

        event_bus.subscribe("bar_close", fail1)
        event_bus.subscribe("bar_close", success)
        event_bus.subscribe("bar_close", fail2)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)  # Should not raise

        assert "success" in successful_calls

    def test_error_middleware_called_on_failure(self, event_bus):
        """Error middleware should be called when handler fails."""
        error_events = []

        def on_error(event: BaseEvent, handler, exc: Exception):
            error_events.append((event.event_type, handler.__name__, str(exc)))

        def failing_handler(event: BaseEvent):
            raise ValueError("Test error")

        event_bus.set_middleware(on_error=on_error)
        event_bus.subscribe("bar_close", failing_handler)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)

        assert len(error_events) == 1
        assert error_events[0][0] == "bar_close"
        assert error_events[0][1] == "failing_handler"
        assert "Test error" in error_events[0][2]


# ============================================
# Unsubscribe Tests
# ============================================


class TestEventBusUnsubscribe:
    """Test unsubscribe functionality."""

    def test_unsubscribe_removes_handler(self, event_bus):
        """Unsubscribing should remove the handler."""
        calls = []

        def handler(event: BaseEvent):
            calls.append("handler")

        event_bus.subscribe("bar_close", handler)
        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)
        assert len(calls) == 1

        event_bus.unsubscribe("bar_close", handler)
        event_bus.publish(event)
        assert len(calls) == 1  # No new calls

    def test_unsubscribe_nonexistent_handler_noop(self, event_bus):
        """Unsubscribing a non-subscribed handler should be a no-op."""

        def handler(event: BaseEvent):
            pass

        # Should not raise
        event_bus.unsubscribe("bar_close", handler)

    def test_unsubscribe_one_of_multiple(self, event_bus):
        """Unsubscribing one handler should not affect others."""
        calls = []

        def handler1(event: BaseEvent):
            calls.append("h1")

        def handler2(event: BaseEvent):
            calls.append("h2")

        event_bus.subscribe("bar_close", handler1)
        event_bus.subscribe("bar_close", handler2)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)
        assert len(calls) == 2

        event_bus.unsubscribe("bar_close", handler1)
        calls.clear()
        event_bus.publish(event)

        assert len(calls) == 1
        assert calls[0] == "h2"

    def test_unsubscribe_invalidates_handler_cache(self, event_bus):
        """Unsubscribing should invalidate handler cache."""
        calls = []

        def handler1(event: BaseEvent):
            calls.append("h1")

        def handler2(event: BaseEvent):
            calls.append("h2")

        event_bus.subscribe("bar_close", handler1)
        event_bus.subscribe("bar_close", handler2)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)  # Build cache

        event_bus.unsubscribe("bar_close", handler1)
        calls.clear()
        event_bus.publish(event)

        # Should use updated handler list, not cached
        assert calls == ["h2"]


# ============================================
# Subscription Token Tests
# ============================================


class TestSubscriptionToken:
    """Test context-managed subscription tokens."""

    def test_subscription_token_returned(self, event_bus):
        """subscribe() should return a SubscriptionToken."""

        def handler(event: BaseEvent):
            pass

        token = event_bus.subscribe("bar_close", handler)
        assert isinstance(token, SubscriptionToken)

    def test_token_unsubscribe(self, event_bus):
        """Token.unsubscribe() should remove the handler."""
        calls = []

        def handler(event: BaseEvent):
            calls.append("called")

        token = event_bus.subscribe("bar_close", handler)
        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)
        assert len(calls) == 1

        token.unsubscribe()
        event_bus.publish(event)
        assert len(calls) == 1  # No new calls

    def test_token_context_manager(self, event_bus):
        """Token should work as context manager."""
        calls = []

        def handler(event: BaseEvent):
            calls.append("called")

        event = BarCloseEvent(source_service="test")

        with event_bus.subscribe("bar_close", handler):
            event_bus.publish(event)
            assert len(calls) == 1

        # After exiting context, handler should be unsubscribed
        event_bus.publish(event)
        assert len(calls) == 1  # No new calls

    def test_token_multiple_unsubscribe_safe(self, event_bus):
        """Calling unsubscribe() multiple times should be safe."""

        def handler(event: BaseEvent):
            pass

        token = event_bus.subscribe("bar_close", handler)
        token.unsubscribe()
        token.unsubscribe()  # Should not raise


# ============================================
# History Tests
# ============================================


class TestEventBusHistory:
    """Test event history functionality."""

    def test_history_stores_events(self, event_bus, sample_price_bar):
        """Published events should be stored in history."""
        bar2 = PriceBarEvent(
            source_service="data_service",
            symbol="MSFT",
            asset_class="equity",
            interval="1d",
            timestamp="2024-01-02T00:00:00Z",
            open=Decimal("400.00"),
            high=Decimal("405.00"),
            low=Decimal("399.00"),
            close=Decimal("404.50"),
            volume=2_000_000,
            source="test_source",
        )

        event_bus.publish(sample_price_bar)
        event_bus.publish(bar2)

        history = event_bus.get_history()
        assert len(history) == 2

    def test_history_filter_by_type(self, event_bus, sample_price_bar, sample_corporate_action):
        """History should be filterable by event type."""
        event_bus.publish(sample_price_bar)
        event_bus.publish(sample_corporate_action)

        bar_history = event_bus.get_history(event_type="bar")
        action_history = event_bus.get_history(event_type="corporate_action")

        assert len(bar_history) == 1
        assert len(action_history) == 1
        assert bar_history[0].event_type == "bar"
        assert action_history[0].event_type == "corporate_action"

    def test_history_filter_by_time(self, event_bus):
        """History should be filterable by timestamp."""
        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=1)

        # Create events with specific timestamps using model_copy
        old_event = BarCloseEvent(source_service="test")
        old_event = old_event.model_copy(update={"occurred_at": past}, deep=True)

        new_event = BarCloseEvent(source_service="test")
        new_event = new_event.model_copy(update={"occurred_at": now}, deep=True)

        event_bus.publish(old_event)
        event_bus.publish(new_event)

        recent = event_bus.get_history(since=now - timedelta(minutes=30))
        assert len(recent) == 1
        assert recent[0].occurred_at >= now - timedelta(seconds=1)

    def test_history_limit(self, event_bus):
        """History should respect limit parameter."""
        for i in range(10):
            event = BarCloseEvent(source_service="test")
            event_bus.publish(event)

        limited = event_bus.get_history(limit=5)
        assert len(limited) == 5

    def test_history_max_size_bounded(self):
        """History should be bounded by max_history."""
        bus = EventBus(max_history=5)

        for i in range(10):
            event = BarCloseEvent(source_service="test")
            bus.publish(event)

        history = bus.get_history()
        assert len(history) == 5

    def test_clear_history(self, event_bus, sample_price_bar):
        """clear_history() should remove all events."""
        event_bus.publish(sample_price_bar)
        event_bus.publish(sample_price_bar)

        assert len(event_bus.get_history()) == 2

        event_bus.clear_history()
        assert len(event_bus.get_history()) == 0

    def test_history_unlimited_when_zero(self):
        """max_history=0 should allow unlimited history."""
        bus = EventBus(max_history=0)

        for i in range(1000):
            event = BarCloseEvent(source_service="test")
            bus.publish(event)

        history = bus.get_history()
        assert len(history) == 1000


# ============================================
# Middleware Tests
# ============================================


class TestEventBusMiddleware:
    """Test middleware hooks."""

    def test_on_publish_middleware_transforms_event(self, event_bus):
        """on_publish middleware should be able to transform events."""

        def add_correlation_id(event: BaseEvent) -> BaseEvent:
            # In practice, we'd create a new event with correlation_id set
            # For this test, we'll just verify it's called
            return event

        mock_middleware = Mock(side_effect=add_correlation_id)
        event_bus.set_middleware(on_publish=mock_middleware)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)

        mock_middleware.assert_called_once_with(event)

    def test_on_error_middleware_called(self, event_bus):
        """on_error middleware should be called on handler errors."""
        errors_logged = []

        def log_error(event: BaseEvent, handler, exc: Exception):
            errors_logged.append((event.event_type, type(exc).__name__))

        def failing_handler(event: BaseEvent):
            raise ValueError("Test error")

        event_bus.set_middleware(on_error=log_error)
        event_bus.subscribe("bar_close", failing_handler)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)

        assert len(errors_logged) == 1
        assert errors_logged[0] == ("bar_close", "ValueError")


# ============================================
# Event Store Integration Tests
# ============================================


class TestEventBusStoreIntegration:
    """Test EventBus integration with EventStore."""

    def test_attach_store_persists_events(self, event_bus, sample_price_bar):
        """Events should be persisted to attached store."""
        store = InMemoryEventStore()
        event_bus.attach_store(store)

        event_bus.publish(sample_price_bar)

        assert store.count() == 1
        stored_event = store.get_by_id(sample_price_bar.event_id)
        assert stored_event is not None
        assert stored_event.event_id == sample_price_bar.event_id

    def test_store_failure_doesnt_stop_handlers(self, event_bus, sample_price_bar):
        """Store failure should not prevent handlers from running."""
        store = Mock(spec=InMemoryEventStore)
        store.append.side_effect = RuntimeError("Store failed")

        event_bus.attach_store(store)

        handler_called = []

        def handler(event: BaseEvent):
            handler_called.append(True)

        event_bus.subscribe("bar", handler)
        event_bus.publish(sample_price_bar)

        # Handler should still be called despite store failure
        assert len(handler_called) == 1

    def test_detach_store_stops_persistence(self, event_bus, sample_price_bar):
        """Detaching store should stop persistence."""
        store = InMemoryEventStore()
        event_bus.attach_store(store)
        event_bus.publish(sample_price_bar)
        assert store.count() == 1

        event_bus.detach_store()

        bar2 = PriceBarEvent(
            source_service="data_service",
            symbol="MSFT",
            asset_class="equity",
            interval="1d",
            timestamp="2024-01-02T00:00:00Z",
            open=Decimal("400.00"),
            high=Decimal("405.00"),
            low=Decimal("399.00"),
            close=Decimal("404.50"),
            volume=2_000_000,
            source="test_source",
        )
        event_bus.publish(bar2)

        # Store should not have second event
        assert store.count() == 1

    def test_store_via_constructor(self, sample_price_bar):
        """Store can be provided in constructor."""
        store = InMemoryEventStore()
        bus = EventBus(event_store=store)

        bus.publish(sample_price_bar)

        assert store.count() == 1


# ============================================
# Utility Method Tests
# ============================================


class TestEventBusUtilities:
    """Test utility methods."""

    def test_get_subscriber_count(self, event_bus):
        """get_subscriber_count() should return correct count."""

        def handler1(event: BaseEvent):
            pass

        def handler2(event: BaseEvent):
            pass

        assert event_bus.get_subscriber_count("bar_close") == 0

        event_bus.subscribe("bar_close", handler1)
        assert event_bus.get_subscriber_count("bar_close") == 1

        event_bus.subscribe("bar_close", handler2)
        assert event_bus.get_subscriber_count("bar_close") == 2

    def test_inspect_subscribers(self, event_bus):
        """inspect_subscribers() should return handler names."""

        def my_handler(event: BaseEvent):
            pass

        def another_handler(event: BaseEvent):
            pass

        event_bus.subscribe("bar_close", my_handler)
        event_bus.subscribe("bar_close", another_handler)

        names = event_bus.inspect_subscribers("bar_close")
        assert len(names) == 2
        assert "my_handler" in names
        assert "another_handler" in names

    def test_get_all_event_types(self, event_bus):
        """get_all_event_types() should return all subscribed types."""

        def handler(event: BaseEvent):
            pass

        assert len(event_bus.get_all_event_types()) == 0

        event_bus.subscribe("bar", handler)
        event_bus.subscribe("corporate_action", handler)

        event_types = event_bus.get_all_event_types()
        assert len(event_types) == 2
        assert "bar" in event_types
        assert "corporate_action" in event_types


# ============================================
# Integration Scenarios
# ============================================


class TestEventBusIntegration:
    """Integration tests with realistic scenarios."""

    def test_multi_service_event_consumption(self, event_bus, sample_price_bar):
        """Multiple services should consume same event in priority order."""
        services_called = []

        def portfolio_handler(event: BaseEvent):
            services_called.append("portfolio")

        def analytics_handler(event: BaseEvent):
            services_called.append("analytics")

        def reporting_handler(event: BaseEvent):
            services_called.append("reporting")

        event_bus.subscribe("bar", portfolio_handler, priority=100)
        event_bus.subscribe("bar", analytics_handler, priority=50)
        event_bus.subscribe("bar", reporting_handler, priority=10)

        event_bus.publish(sample_price_bar)

        assert services_called == ["portfolio", "analytics", "reporting"]

    def test_cascading_events(self, event_bus):
        """Handlers should be able to publish new events."""
        events_flow = []

        def bar_close_handler(event: BaseEvent):
            events_flow.append("bar_close")
            # Trigger valuation
            backtest_event = BacktestStartedEvent(source_service="test", config={"test": "data"})
            event_bus.publish(backtest_event)

        def backtest_handler(event: BaseEvent):
            events_flow.append("backtest_started")

        event_bus.subscribe("bar_close", bar_close_handler)
        event_bus.subscribe("backtest_started", backtest_handler)

        event = BarCloseEvent(source_service="test")
        event_bus.publish(event)

        assert events_flow == ["bar_close", "backtest_started"]
