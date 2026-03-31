"""
Comprehensive unit tests for EventStore implementations.

Tests InMemoryEventStore and SQLiteEventStore for append-only persistence,
querying, idempotency, and serialization/deserialization.
"""

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest

from qs_trader.events.event_store import (
    InMemoryEventStore,
    ParquetEventStore,
    SQLiteEventStore,
    register_event_class,
    resolve_event_class,
)
from qs_trader.events.events import BarCloseEvent, BaseEvent, CorporateActionEvent, PriceBarEvent

# ============================================
# Fixtures
# ============================================


@pytest.fixture
def event_store():
    """Use InMemoryEventStore for reliable testing."""
    store = InMemoryEventStore()
    yield store
    store.close()


@pytest.fixture
def memory_store():
    """Dedicated InMemoryEventStore fixture."""
    return InMemoryEventStore()


@pytest.fixture
def sqlite_store(tmp_path):
    """Dedicated SQLiteEventStore fixture."""
    db_path = tmp_path / "test_events.db"
    store = SQLiteEventStore(db_path)
    yield store
    store.close()


@pytest.fixture
def parquet_store(tmp_path):
    """Dedicated ParquetEventStore fixture."""
    parquet_path = tmp_path / "test_events.parquet"
    store = ParquetEventStore(parquet_path)
    yield store
    store.close()


@pytest.fixture
def sample_bar_event():
    """Create sample bar event."""
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
def sample_action_event():
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
# Event Class Registry Tests
# ============================================


class TestEventClassRegistry:
    """Test event class discovery and registration."""

    def test_register_event_class_adds_to_registry(self):
        """register_event_class should add custom event types to registry."""

        class UniqueCustomEvent(BaseEvent):
            # Need to override as class variable, not instance variable
            pass

        # Set as class attribute after definition
        UniqueCustomEvent.event_type = "unique_custom_xyz123"

        # Register the class
        register_event_class(UniqueCustomEvent)

        # Should be able to resolve it
        cls = resolve_event_class("unique_custom_xyz123")
        assert cls == UniqueCustomEvent

    def test_resolve_event_class_unknown_raises(self):
        """resolve_event_class should raise for unknown types."""
        with pytest.raises(LookupError, match="Unknown event_type"):
            resolve_event_class("definitely_nonexistent_event_type_xyz_987")


# ============================================
# Basic Append and Retrieve Tests
# ============================================


class TestEventStoreBasicOperations:
    """Test basic append and retrieve operations."""

    def test_append_increases_count(self, event_store, sample_bar_event):
        """Appending events should increase count."""
        assert event_store.count() == 0

        event_store.append(sample_bar_event)
        assert event_store.count() == 1

    def test_get_by_id_success(self, event_store, sample_bar_event):
        """get_by_id should return the correct event."""
        event_store.append(sample_bar_event)

        retrieved = event_store.get_by_id(sample_bar_event.event_id)
        assert retrieved is not None
        assert retrieved.event_id == sample_bar_event.event_id
        assert retrieved.event_type == "bar"

    def test_get_by_id_nonexistent_returns_none(self, event_store):
        """get_by_id for nonexistent ID should return None."""
        result = event_store.get_by_id("nonexistent-id")
        assert result is None

    def test_get_all_returns_all_events(self, event_store, sample_bar_event, sample_action_event):
        """get_all should return all appended events."""
        event_store.append(sample_bar_event)
        event_store.append(sample_action_event)

        all_events = event_store.get_all()
        assert len(all_events) == 2

    def test_get_all_with_limit(self, event_store):
        """get_all with limit should return specified number."""
        for _ in range(5):
            event = BarCloseEvent(source_service="test")
            event_store.append(event)

        limited = event_store.get_all(limit=3)
        assert len(limited) == 3

    def test_clear_removes_all_events(self, event_store, sample_bar_event):
        """clear should remove all events."""
        event_store.append(sample_bar_event)
        assert event_store.count() > 0

        event_store.clear()
        assert event_store.count() == 0


# ============================================
# Idempotency Tests
# ============================================


class TestEventStoreIdempotency:
    """Test duplicate prevention."""

    def test_duplicate_event_id_raises(self, event_store, sample_bar_event):
        """Appending duplicate event_id should raise."""
        event_store.append(sample_bar_event)

        with pytest.raises(ValueError, match="Duplicate event_id"):
            event_store.append(sample_bar_event)

    def test_different_events_same_type_allowed(self, event_store):
        """Multiple events of same type with different IDs should work."""
        event1 = BarCloseEvent(source_service="service1")
        event2 = BarCloseEvent(source_service="service2")

        event_store.append(event1)
        event_store.append(event2)

        assert event_store.count() == 2


# ============================================
# Query by Correlation ID Tests
# ============================================


class TestEventStoreCorrelationQueries:
    """Test querying by correlation_id."""

    def test_get_by_correlation_id_finds_events(self, event_store):
        """get_by_correlation_id should find correlated events."""
        corr_id = str(uuid4())

        event1 = BarCloseEvent(source_service="test", correlation_id=corr_id)
        event2 = BarCloseEvent(source_service="test", correlation_id=corr_id)

        event_store.append(event1)
        event_store.append(event2)

        results = event_store.get_by_correlation_id(corr_id)
        assert len(results) == 2

    def test_get_by_correlation_id_chronological_order(self, event_store):
        """Results should be sorted by occurred_at."""
        corr_id = str(uuid4())
        now = datetime.now(timezone.utc)

        event2 = BarCloseEvent(source_service="test", correlation_id=corr_id)
        event2 = event2.model_copy(update={"occurred_at": now + timedelta(seconds=2)}, deep=True)

        event1 = BarCloseEvent(source_service="test", correlation_id=corr_id)
        event1 = event1.model_copy(update={"occurred_at": now + timedelta(seconds=1)}, deep=True)

        # Append in reverse order
        event_store.append(event2)
        event_store.append(event1)

        results = event_store.get_by_correlation_id(corr_id)

        # Should be sorted chronologically
        assert results[0].event_id == event1.event_id
        assert results[1].event_id == event2.event_id

    def test_get_by_correlation_id_nonexistent_returns_empty(self, event_store):
        """Nonexistent correlation_id should return empty list."""
        results = event_store.get_by_correlation_id("nonexistent")
        assert results == []


# ============================================
# Query by Type Tests
# ============================================


class TestEventStoreTypeQueries:
    """Test querying by event_type."""

    def test_get_by_type_filters_correctly(self, event_store, sample_bar_event, sample_action_event):
        """get_by_type should return only events of specified type."""
        event_store.append(sample_bar_event)
        event_store.append(sample_action_event)

        bars = event_store.get_by_type("bar")
        assert len(bars) == 1
        assert bars[0].event_type == "bar"

        actions = event_store.get_by_type("corporate_action")
        assert len(actions) == 1
        assert actions[0].event_type == "corporate_action"

    def test_get_by_type_with_time_filter(self, event_store):
        """get_by_type should filter by time range."""
        now = datetime.now(timezone.utc)

        old_event = BarCloseEvent(source_service="test")
        old_event = old_event.model_copy(update={"occurred_at": now - timedelta(hours=2)}, deep=True)

        new_event = BarCloseEvent(source_service="test")
        new_event = new_event.model_copy(update={"occurred_at": now}, deep=True)

        event_store.append(old_event)
        event_store.append(new_event)

        # Query with start_time
        cutoff = now - timedelta(hours=1)
        results = event_store.get_by_type("bar_close", start_time=cutoff)
        assert len(results) == 1
        assert results[0].event_id == new_event.event_id

    def test_get_by_type_nonexistent_returns_empty(self, event_store):
        """Nonexistent event_type should return empty list."""
        results = event_store.get_by_type("nonexistent_type")
        assert results == []


# ============================================
# Serialization Tests
# ============================================


class TestEventStoreSerialization:
    """Test event serialization and deserialization."""

    def test_price_bar_roundtrip(self, event_store, sample_bar_event):
        """PriceBarEvent should roundtrip correctly."""
        event_store.append(sample_bar_event)
        retrieved = event_store.get_by_id(sample_bar_event.event_id)

        assert retrieved is not None
        assert retrieved.symbol == "AAPL"
        assert retrieved.open == Decimal("150.00")
        assert retrieved.volume == 1_000_000

    def test_decimal_precision_preserved(self, event_store):
        """Decimal values should maintain precision."""
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
            source="test",
        )

        event_store.append(original)
        retrieved = event_store.get_by_id(original.event_id)

        assert retrieved is not None
        assert retrieved.open == Decimal("123.456789")
        assert retrieved.close == Decimal("123.456789")


# ============================================
# SQLite-Specific Tests
# ============================================


class TestSQLiteEventStoreSpecific:
    """Tests specific to SQLiteEventStore."""

    def test_create_with_file_path(self, tmp_path):
        """SQLiteEventStore should create database file."""
        db_path = tmp_path / "events.db"
        store = SQLiteEventStore(db_path)

        assert db_path.exists()
        store.close()

    def test_create_nested_directory(self, tmp_path):
        """SQLiteEventStore should create nested directories."""
        db_path = tmp_path / "nested" / "deep" / "events.db"
        store = SQLiteEventStore(db_path)

        assert db_path.exists()
        store.close()

    def test_in_memory_database(self):
        """SQLiteEventStore should support in-memory database."""
        store = SQLiteEventStore(":memory:")

        event = BarCloseEvent(source_service="test")
        store.append(event)

        assert store.count() == 1
        store.close()


# ============================================
# InMemoryEventStore-Specific Tests
# ============================================


class TestInMemoryEventStoreSpecific:
    """Tests specific to InMemoryEventStore."""

    def test_events_not_persisted_across_instances(self):
        """InMemoryEventStore should not share state."""
        store1 = InMemoryEventStore()
        event = BarCloseEvent(source_service="test")
        store1.append(event)

        store2 = InMemoryEventStore()
        assert store2.count() == 0

    def test_clear_only_affects_current_instance(self):
        """clear() should only affect the instance it's called on."""
        store1 = InMemoryEventStore()
        store2 = InMemoryEventStore()

        event1 = BarCloseEvent(source_service="test")
        event2 = BarCloseEvent(source_service="test")

        store1.append(event1)
        store2.append(event2)

        store1.clear()

        assert store1.count() == 0
        assert store2.count() == 1


# ============================================
# ParquetEventStore-Specific Tests
# ============================================


class TestParquetEventStoreSpecific:
    """Tests specific to ParquetEventStore."""

    def test_create_with_file_path(self, tmp_path):
        """ParquetEventStore should create parquet file on flush."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path)

        # File doesn't exist until flush
        assert not parquet_path.exists()

        event = BarCloseEvent(source_service="test")
        store.append(event)
        store.flush()

        # File exists after flush
        assert parquet_path.exists()
        store.close()

    def test_create_nested_directory(self, tmp_path):
        """ParquetEventStore should create nested directories."""
        parquet_path = tmp_path / "nested" / "deep" / "events.parquet"
        store = ParquetEventStore(parquet_path)

        event = BarCloseEvent(source_service="test")
        store.append(event)
        store.close()  # close flushes

        assert parquet_path.exists()

    def test_buffered_writes(self, tmp_path):
        """Events should be buffered before writing to Parquet."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, max_buffer_size=10)

        # Add 5 events - should stay in buffer
        for i in range(5):
            event = BarCloseEvent(source_service="test")
            store.append(event)

        assert store.count() == 5
        assert not parquet_path.exists()  # Not flushed yet

        # Add 5 more - should auto-flush at 10
        for i in range(5):
            event = BarCloseEvent(source_service="test")
            store.append(event)

        assert parquet_path.exists()  # Auto-flushed
        store.close()

    def test_auto_flush_on_buffer_full(self, tmp_path):
        """Store should auto-flush when buffer reaches max_buffer_size."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, max_buffer_size=3)

        # Add 2 events
        for _ in range(2):
            event = BarCloseEvent(source_service="test")
            store.append(event)

        assert not parquet_path.exists()

        # Add 3rd event - should trigger auto-flush
        event = BarCloseEvent(source_service="test")
        store.append(event)

        assert parquet_path.exists()
        store.close()

    def test_close_flushes_buffer(self, tmp_path):
        """close() should flush remaining buffered events."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, max_buffer_size=100)

        # Add events (below buffer size)
        for _ in range(5):
            event = BarCloseEvent(source_service="test")
            store.append(event)

        assert not parquet_path.exists()

        # Close should flush
        store.close()
        assert parquet_path.exists()

    def test_get_by_id_from_buffer(self, tmp_path):
        """get_by_id should check buffer before file."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, max_buffer_size=100)

        event = BarCloseEvent(source_service="test")
        store.append(event)

        # Should find in buffer (not yet flushed)
        retrieved = store.get_by_id(event.event_id)
        assert retrieved is not None
        assert retrieved.event_id == event.event_id

        store.close()

    def test_get_by_id_from_file(self, tmp_path):
        """get_by_id should read from file after flush."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, max_buffer_size=100)

        event = BarCloseEvent(source_service="test")
        store.append(event)
        store.flush()

        # Should find in file (after flush)
        retrieved = store.get_by_id(event.event_id)
        assert retrieved is not None
        assert retrieved.event_id == event.event_id

        store.close()

    def test_count_includes_buffer_and_file(self, tmp_path):
        """count() should include both buffered and flushed events."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, max_buffer_size=100)

        # Add 3 events and flush
        for _ in range(3):
            event = BarCloseEvent(source_service="test")
            store.append(event)
        store.flush()

        # Add 2 more to buffer
        for _ in range(2):
            event = BarCloseEvent(source_service="test")
            store.append(event)

        # Count should be 5 (3 in file + 2 in buffer)
        assert store.count() == 5
        store.close()

    def test_get_all_combines_buffer_and_file(self, tmp_path):
        """get_all() should return events from both buffer and file."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, max_buffer_size=100)

        # Add 2 events and flush
        for _ in range(2):
            event = BarCloseEvent(source_service="test")
            store.append(event)
        store.flush()

        # Add 1 more to buffer
        event = BarCloseEvent(source_service="test")
        store.append(event)

        # get_all should return 3 events
        all_events = store.get_all()
        assert len(all_events) == 3
        store.close()

    def test_clear_removes_file_and_buffer(self, tmp_path):
        """clear() should remove both file and buffer."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, max_buffer_size=100)

        # Add events and flush
        for _ in range(3):
            event = BarCloseEvent(source_service="test")
            store.append(event)
        store.flush()

        assert parquet_path.exists()
        assert store.count() == 3

        # Clear should remove file
        store.clear()
        assert not parquet_path.exists()
        assert store.count() == 0

        store.close()

    def test_compression_setting(self, tmp_path):
        """ParquetEventStore should support different compression codecs."""
        parquet_path = tmp_path / "events.parquet"
        store = ParquetEventStore(parquet_path, compression="snappy")

        event = BarCloseEvent(source_service="test")
        store.append(event)
        store.close()

        assert parquet_path.exists()


# ============================================
# Integration Tests
# ============================================


class TestEventStoreIntegration:
    """Integration tests with realistic scenarios."""

    def test_large_volume_append(self, event_store):
        """Store should handle large volumes of events."""
        for _ in range(100):
            event = BarCloseEvent(source_service="test")
            event_store.append(event)

        assert event_store.count() == 100
        all_events = event_store.get_all()
        assert len(all_events) == 100

    def test_mixed_event_types_storage(self, event_store, sample_bar_event, sample_action_event):
        """Store should handle multiple event types."""
        event_store.append(sample_bar_event)
        event_store.append(sample_action_event)

        control_event = BarCloseEvent(source_service="test")
        event_store.append(control_event)

        assert event_store.count() == 3

        # Verify each type can be retrieved
        bars = event_store.get_by_type("bar")
        actions = event_store.get_by_type("corporate_action")
        controls = event_store.get_by_type("bar_close")

        assert len(bars) == 1
        assert len(actions) == 1
        assert len(controls) == 1
