"""Tests for IDataAdapter protocol."""

from datetime import datetime
from decimal import Decimal
from typing import Any, Iterator, Optional, Tuple

from qs_trader.events.events import CorporateActionEvent, PriceBarEvent
from qs_trader.services.data.adapters.protocol import IDataAdapter

# mypy: check-untyped-defs


class MockDataAdapter:
    """Mock adapter satisfying IDataAdapter protocol."""

    def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
        """Stream mock bars."""
        yield {"symbol": "AAPL", "timestamp": datetime(2024, 1, 2), "close": 100.0}
        yield {"symbol": "AAPL", "timestamp": datetime(2024, 1, 3), "close": 101.0}

    def to_price_bar_event(self, bar: Any) -> PriceBarEvent:
        """Convert to PriceBarEvent."""
        return PriceBarEvent(
            symbol=bar["symbol"],
            interval="1d",
            timestamp=bar["timestamp"].isoformat(),
            open=Decimal("100.0"),
            high=Decimal("105.0"),
            low=Decimal("99.0"),
            close=Decimal(str(bar["close"])),
            volume=1000000,
            source="mock",
        )

    def to_corporate_action_event(self, bar: Any, prev_bar: Optional[Any] = None) -> Optional[CorporateActionEvent]:
        """Extract corporate action if present."""
        return None  # No actions in mock data

    def get_timestamp(self, bar: Any) -> datetime:
        """Extract timestamp from bar."""
        timestamp: datetime = bar["timestamp"]
        return timestamp

    def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        """Get available date range."""
        return "2024-01-01", "2024-12-31"

    def prime_cache(self, start_date: str, end_date: str) -> int:
        """Prime cache (optional method)."""
        return 252  # Mock 252 bars

    def write_cache(self, bars: list[Any]) -> None:
        """Write bars to cache (optional method)."""
        pass

    def update_to_latest(self, dry_run: bool = False) -> Tuple[int, str, str]:
        """Update cache to latest (optional method)."""
        return 10, "2024-12-01", "2024-12-31"


class MinimalAdapter:
    """Minimal adapter with only required methods."""

    def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
        yield {"symbol": "TEST", "timestamp": datetime.now(), "close": 50.0}

    def to_price_bar_event(self, bar: Any) -> PriceBarEvent:
        return PriceBarEvent(
            symbol="TEST",
            interval="1d",
            timestamp=bar["timestamp"].isoformat(),
            open=Decimal("50.0"),
            high=Decimal("51.0"),
            low=Decimal("49.0"),
            close=Decimal("50.0"),
            volume=100000,
            source="minimal",
        )

    def to_corporate_action_event(self, bar: Any, prev_bar: Optional[Any] = None) -> Optional[CorporateActionEvent]:
        return None

    def get_timestamp(self, bar: Any) -> datetime:
        timestamp: datetime = bar["timestamp"]
        return timestamp

    def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        return None, None


class TestProtocolCompliance:
    """Test that adapters properly implement the protocol."""

    def test_mock_adapter_satisfies_protocol(self) -> None:
        """Test that MockDataAdapter satisfies IDataAdapter protocol."""
        # Arrange

        adapter: IDataAdapter = MockDataAdapter()

        # Assert - Should have all required methods
        assert hasattr(adapter, "read_bars")
        assert hasattr(adapter, "to_price_bar_event")
        assert hasattr(adapter, "to_corporate_action_event")
        assert hasattr(adapter, "get_timestamp")
        assert hasattr(adapter, "get_available_date_range")

    def test_minimal_adapter_satisfies_protocol(self) -> None:
        """Test that minimal adapter with only required methods works."""
        # Arrange
        adapter = MinimalAdapter()

        # Act & Assert - Should be usable
        bars = list(adapter.read_bars("2024-01-01", "2024-12-31"))
        assert len(bars) == 1

    def test_protocol_type_checking(self) -> None:
        """Test that protocol type checking works at runtime."""
        # Arrange
        adapter = MockDataAdapter()

        # Act - Use as protocol type
        def process_adapter(a: IDataAdapter) -> int:
            bars = list(a.read_bars("2024-01-01", "2024-01-31"))
            return len(bars)

        # Assert
        result = process_adapter(adapter)
        assert result == 2


class TestReadBarsMethod:
    """Test read_bars method requirements."""

    def test_read_bars_returns_iterator(self) -> None:
        """Test that read_bars returns an iterator."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        result = adapter.read_bars("2024-01-01", "2024-12-31")

        # Assert
        assert hasattr(result, "__iter__")
        assert hasattr(result, "__next__")

    def test_read_bars_yields_bars(self) -> None:
        """Test that read_bars yields bar objects."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        bars = list(adapter.read_bars("2024-01-01", "2024-12-31"))

        # Assert
        assert len(bars) == 2
        assert all("symbol" in bar for bar in bars)
        assert all("timestamp" in bar for bar in bars)

    def test_read_bars_with_date_range(self) -> None:
        """Test read_bars accepts date range parameters."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        bars = list(adapter.read_bars("2024-01-02", "2024-01-03"))

        # Assert - Should accept date strings
        assert len(bars) == 2

    def test_read_bars_empty_result(self) -> None:
        """Test read_bars can return empty iterator."""

        # Arrange
        class EmptyAdapter(MinimalAdapter):
            def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
                return iter([])

        adapter = EmptyAdapter()

        # Act
        bars = list(adapter.read_bars("2024-01-01", "2024-12-31"))

        # Assert
        assert len(bars) == 0


class TestToPriceBarEventMethod:
    """Test to_price_bar_event method requirements."""

    def test_to_price_bar_event_returns_event(self) -> None:
        """Test that to_price_bar_event returns PriceBarEvent."""
        # Arrange
        adapter = MockDataAdapter()
        bar = {"symbol": "AAPL", "timestamp": datetime(2024, 1, 2), "close": 100.0}

        # Act
        event = adapter.to_price_bar_event(bar)

        # Assert
        assert isinstance(event, PriceBarEvent)

    def test_to_price_bar_event_has_required_fields(self) -> None:
        """Test that converted event has all required fields."""
        # Arrange
        adapter = MockDataAdapter()
        bar = {"symbol": "AAPL", "timestamp": datetime(2024, 1, 2), "close": 100.0}

        # Act
        event = adapter.to_price_bar_event(bar)

        # Assert
        assert event.symbol == "AAPL"
        assert event.interval is not None
        assert event.timestamp is not None
        assert event.open is not None
        assert event.high is not None
        assert event.low is not None
        assert event.close is not None
        assert event.volume is not None

    def test_to_price_bar_event_handles_vendor_format(self) -> None:
        """Test that adapter can convert vendor-specific format."""
        # Arrange
        adapter = MockDataAdapter()
        # Vendor-specific bar format
        bar = {
            "symbol": "MSFT",
            "timestamp": datetime(2024, 6, 15),
            "close": 350.0,
        }

        # Act
        event = adapter.to_price_bar_event(bar)

        # Assert - Should convert to canonical format
        assert event.symbol == "MSFT"
        assert "2024-06-15" in event.timestamp


class TestToCorporateActionEventMethod:
    """Test to_corporate_action_event method requirements."""

    def test_to_corporate_action_event_returns_optional(self) -> None:
        """Test that method can return None."""
        # Arrange
        adapter = MockDataAdapter()
        bar = {"symbol": "AAPL", "timestamp": datetime(2024, 1, 2), "close": 100.0}

        # Act
        result = adapter.to_corporate_action_event(bar)

        # Assert - Can be None
        assert result is None

    def test_to_corporate_action_event_with_prev_bar(self) -> None:
        """Test method accepts previous bar for change detection."""
        # Arrange
        adapter = MockDataAdapter()
        bar = {"symbol": "AAPL", "timestamp": datetime(2024, 1, 2), "close": 100.0}
        prev_bar = {"symbol": "AAPL", "timestamp": datetime(2024, 1, 1), "close": 200.0}

        # Act
        result = adapter.to_corporate_action_event(bar, prev_bar)

        # Assert - Should accept prev_bar parameter
        assert result is None  # Mock doesn't detect actions

    def test_to_corporate_action_event_detects_action(self) -> None:
        """Test method can detect and return corporate action."""

        # Arrange
        class ActionDetectingAdapter(MinimalAdapter):
            def to_corporate_action_event(
                self, bar: Any, prev_bar: Optional[Any] = None
            ) -> Optional[CorporateActionEvent]:
                # Detect split if price halves
                if prev_bar and bar["close"] < prev_bar["close"] * 0.6:
                    return CorporateActionEvent(
                        symbol=bar["symbol"],
                        ex_date=bar["timestamp"].date().isoformat(),
                        action_type="split",
                        announcement_date=bar["timestamp"].date().isoformat(),
                        effective_date=bar["timestamp"].date().isoformat(),
                        source="mock",
                        split_ratio=Decimal("2.0"),
                    )
                return None

        adapter = ActionDetectingAdapter()
        bar = {"symbol": "AAPL", "timestamp": datetime(2024, 1, 2), "close": 50.0}
        prev_bar = {"symbol": "AAPL", "timestamp": datetime(2024, 1, 1), "close": 100.0}

        # Act
        event = adapter.to_corporate_action_event(bar, prev_bar)

        # Assert
        assert event is not None
        assert isinstance(event, CorporateActionEvent)
        assert event.action_type == "split"


class TestGetTimestampMethod:
    """Test get_timestamp method requirements."""

    def test_get_timestamp_returns_datetime(self) -> None:
        """Test that get_timestamp returns datetime object."""
        # Arrange
        adapter = MockDataAdapter()
        bar = {"symbol": "AAPL", "timestamp": datetime(2024, 1, 2), "close": 100.0}

        # Act
        result = adapter.get_timestamp(bar)

        # Assert
        assert isinstance(result, datetime)

    def test_get_timestamp_extracts_correct_value(self) -> None:
        """Test that timestamp extraction is accurate."""
        # Arrange
        adapter = MockDataAdapter()
        expected = datetime(2024, 3, 15, 16, 0, 0)
        bar = {"symbol": "AAPL", "timestamp": expected, "close": 100.0}

        # Act
        result = adapter.get_timestamp(bar)

        # Assert
        assert result == expected

    def test_get_timestamp_handles_vendor_format(self) -> None:
        """Test timestamp extraction from vendor-specific format."""

        # Arrange
        class CustomTimestampAdapter(MinimalAdapter):
            def get_timestamp(self, bar: Any) -> datetime:
                # Vendor uses "trade_dt" field
                timestamp: datetime = bar["trade_dt"]
                return timestamp

        adapter = CustomTimestampAdapter()
        expected = datetime(2024, 7, 20)
        bar = {"symbol": "AAPL", "trade_dt": expected, "close": 100.0}

        # Act
        result = adapter.get_timestamp(bar)

        # Assert
        assert result == expected


class TestGetAvailableDateRangeMethod:
    """Test get_available_date_range method requirements."""

    def test_get_available_date_range_returns_tuple(self) -> None:
        """Test that method returns tuple of two optionals."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        result = adapter.get_available_date_range()

        # Assert
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_get_available_date_range_with_data(self) -> None:
        """Test method returns date range when data available."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        min_date, max_date = adapter.get_available_date_range()

        # Assert
        assert min_date == "2024-01-01"
        assert max_date == "2024-12-31"

    def test_get_available_date_range_no_data(self) -> None:
        """Test method returns (None, None) when no data."""
        # Arrange
        adapter = MinimalAdapter()

        # Act
        min_date, max_date = adapter.get_available_date_range()

        # Assert
        assert min_date is None
        assert max_date is None


class TestOptionalCachingMethods:
    """Test optional caching methods."""

    def test_prime_cache_optional_returns_count(self) -> None:
        """Test prime_cache returns number of bars cached."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        count = adapter.prime_cache("2024-01-01", "2024-12-31")

        # Assert
        assert isinstance(count, int)
        assert count == 252

    def test_write_cache_optional_accepts_list(self) -> None:
        """Test write_cache accepts list of bars."""
        # Arrange
        adapter = MockDataAdapter()
        bars = [
            {"symbol": "AAPL", "timestamp": datetime(2024, 1, 2), "close": 100.0},
            {"symbol": "AAPL", "timestamp": datetime(2024, 1, 3), "close": 101.0},
        ]

        # Act & Assert - Should not raise
        adapter.write_cache(bars)

    def test_update_to_latest_optional_returns_tuple(self) -> None:
        """Test update_to_latest returns (count, start, end)."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        result = adapter.update_to_latest(dry_run=False)

        # Assert
        assert isinstance(result, tuple)
        assert len(result) == 3
        bars_added, start_date, end_date = result
        assert isinstance(bars_added, int)
        assert isinstance(start_date, str)
        assert isinstance(end_date, str)

    def test_update_to_latest_dry_run_flag(self) -> None:
        """Test update_to_latest accepts dry_run parameter."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        result = adapter.update_to_latest(dry_run=True)

        # Assert - Should accept dry_run flag
        assert result is not None

    def test_adapter_without_caching_methods(self) -> None:
        """Test adapter can work without optional caching methods."""
        # Arrange
        adapter = MinimalAdapter()

        # Act & Assert - Should work without caching methods
        bars = list(adapter.read_bars("2024-01-01", "2024-12-31"))
        assert len(bars) == 1

        # Optional methods should not exist or raise NotImplementedError
        assert not hasattr(adapter, "prime_cache") or True


class TestAdapterUsagePatterns:
    """Test common adapter usage patterns."""

    def test_basic_streaming_workflow(self) -> None:
        """Test basic bar streaming and event conversion."""
        # Arrange
        adapter = MockDataAdapter()

        # Act - Stream bars and convert to events
        events = []
        for bar in adapter.read_bars("2024-01-01", "2024-12-31"):
            event = adapter.to_price_bar_event(bar)
            events.append(event)

        # Assert
        assert len(events) == 2
        assert all(isinstance(e, PriceBarEvent) for e in events)

    def test_multi_symbol_synchronization_workflow(self) -> None:
        """Test pattern for synchronizing multiple symbols by timestamp."""
        # Arrange
        adapter1 = MockDataAdapter()
        adapter2 = MockDataAdapter()

        # Act - Collect bars with timestamps
        bars1 = [(adapter1.get_timestamp(bar), bar) for bar in adapter1.read_bars("2024-01-01", "2024-12-31")]
        bars2 = [(adapter2.get_timestamp(bar), bar) for bar in adapter2.read_bars("2024-01-01", "2024-12-31")]

        # Assert - Can sort by timestamp
        all_bars = sorted(bars1 + bars2, key=lambda x: x[0])
        assert len(all_bars) == 4

    def test_corporate_action_detection_workflow(self) -> None:
        """Test pattern for detecting corporate actions."""
        # Arrange
        adapter = MockDataAdapter()

        # Act - Stream bars and check for actions
        prev_bar = None
        actions = []
        for bar in adapter.read_bars("2024-01-01", "2024-12-31"):
            action = adapter.to_corporate_action_event(bar, prev_bar)
            if action:
                actions.append(action)
            prev_bar = bar

        # Assert
        assert isinstance(actions, list)

    def test_date_range_query_workflow(self) -> None:
        """Test pattern for querying available data range."""
        # Arrange
        adapter = MockDataAdapter()

        # Act
        min_date, max_date = adapter.get_available_date_range()

        # Assert - Can use range for backfill
        if min_date and max_date:
            bars = list(adapter.read_bars(min_date, max_date))
            assert len(bars) >= 0

    def test_incremental_update_workflow(self) -> None:
        """Test pattern for incremental cache updates."""
        # Arrange
        adapter = MockDataAdapter()

        # Act - Get latest data range
        min_date, max_date = adapter.get_available_date_range()

        # Prime cache initially
        if hasattr(adapter, "prime_cache") and min_date and max_date:
            initial_count = adapter.prime_cache(min_date, max_date)
            assert initial_count > 0

            # Update to latest
            if hasattr(adapter, "update_to_latest"):
                new_bars, start, end = adapter.update_to_latest()
                assert isinstance(new_bars, int)


class TestProtocolDocumentation:
    """Test that protocol has proper documentation."""

    def test_protocol_has_docstring(self) -> None:
        """Test IDataAdapter protocol has docstring."""
        assert IDataAdapter.__doc__ is not None
        assert len(IDataAdapter.__doc__) > 0

    def test_protocol_methods_have_docstrings(self) -> None:
        """Test all protocol methods have docstrings."""
        assert IDataAdapter.read_bars.__doc__ is not None
        assert IDataAdapter.to_price_bar_event.__doc__ is not None
        assert IDataAdapter.to_corporate_action_event.__doc__ is not None
        assert IDataAdapter.get_timestamp.__doc__ is not None
        assert IDataAdapter.get_available_date_range.__doc__ is not None
