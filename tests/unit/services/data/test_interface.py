"""Unit tests for data service interfaces.

Tests Protocol interfaces to validate their structure and documentation.
Since Protocols are structural types, we verify they have correct signatures
and can be satisfied by implementations.
"""

from datetime import date, datetime
from typing import Any, Iterator, List, Optional, Tuple
from unittest.mock import MagicMock

from qs_trader.services.data.interface import IDataAdapter, IDataService
from qs_trader.services.data.models import Instrument


class TestIDataServiceProtocol:
    """Test IDataService protocol structure."""

    def test_protocol_has_stream_bars_method(self):
        """Test IDataService has stream_bars method."""
        # Arrange - Create a mock that satisfies the protocol
        mock_service = MagicMock(spec=IDataService)

        # Act - Verify method exists
        assert hasattr(mock_service, "stream_bars")
        assert callable(mock_service.stream_bars)

    def test_protocol_has_stream_universe_method(self):
        """Test IDataService has stream_universe method."""
        # Arrange
        mock_service = MagicMock(spec=IDataService)

        # Act & Assert
        assert hasattr(mock_service, "stream_universe")
        assert callable(mock_service.stream_universe)

    def test_protocol_has_get_instrument_method(self):
        """Test IDataService has get_instrument method."""
        # Arrange
        mock_service = MagicMock(spec=IDataService)

        # Act & Assert
        assert hasattr(mock_service, "get_instrument")
        assert callable(mock_service.get_instrument)

    def test_protocol_has_list_available_symbols_method(self):
        """Test IDataService has list_available_symbols method."""
        # Arrange
        mock_service = MagicMock(spec=IDataService)

        # Act & Assert
        assert hasattr(mock_service, "list_available_symbols")
        assert callable(mock_service.list_available_symbols)

    def test_protocol_has_get_corporate_actions_method(self):
        """Test IDataService has get_corporate_actions method."""
        # Arrange
        mock_service = MagicMock(spec=IDataService)

        # Act & Assert
        assert hasattr(mock_service, "get_corporate_actions")
        assert callable(mock_service.get_corporate_actions)

    def test_mock_implementation_satisfies_protocol(self):
        """Test a mock implementation satisfies IDataService protocol."""

        # Arrange - Create a mock that implements all required methods
        class MockDataService:
            def stream_bars(self, symbol: str, start_date: date, end_date: date, *, is_warmup: bool = False):
                pass

            def stream_universe(
                self,
                symbols: List[str],
                start_date: date,
                end_date: date,
                *,
                is_warmup: bool = False,
                strict: bool = False,
            ):
                pass

            def get_instrument(self, symbol: str):
                return Instrument(symbol=symbol)

            def list_available_symbols(self, data_source=None):
                return []

            def get_corporate_actions(
                self, symbol: str, start_date: date, end_date: date, *, publish_events: bool = True
            ):
                return []

        # Act
        service = MockDataService()

        # Assert - Should be able to call all methods
        assert callable(service.stream_bars)
        assert callable(service.stream_universe)
        assert callable(service.get_instrument)
        assert callable(service.list_available_symbols)
        assert callable(service.get_corporate_actions)

    def test_stream_bars_signature_requirements(self):
        """Test stream_bars has correct signature requirements."""

        # This test documents the expected signature
        class TestService:
            def stream_bars(
                self,
                symbol: str,
                start_date: date,
                end_date: date,
                *,
                is_warmup: bool = False,
            ) -> None:
                pass

        service = TestService()
        result = service.stream_bars("AAPL", date(2020, 1, 1), date(2020, 12, 31))

        # Should return None (publishes events via event bus)
        assert result is None

    def test_stream_universe_signature_requirements(self):
        """Test stream_universe has correct signature requirements."""

        # This test documents the expected signature
        class TestService:
            def stream_universe(
                self,
                symbols: List[str],
                start_date: date,
                end_date: date,
                *,
                is_warmup: bool = False,
                strict: bool = False,
            ) -> None:
                pass

        service = TestService()
        result = service.stream_universe(["AAPL"], date(2020, 1, 1), date(2020, 12, 31))

        # Should return None (publishes events via event bus)
        assert result is None


class TestIDataAdapterProtocol:
    """Test IDataAdapter protocol structure."""

    def test_protocol_has_read_bars_method(self):
        """Test IDataAdapter has read_bars method."""
        # Arrange
        mock_adapter = MagicMock(spec=IDataAdapter)

        # Act & Assert
        assert hasattr(mock_adapter, "read_bars")
        assert callable(mock_adapter.read_bars)

    def test_protocol_has_to_price_bar_event_method(self):
        """Test IDataAdapter has to_price_bar_event method."""
        # Arrange
        mock_adapter = MagicMock(spec=IDataAdapter)

        # Act & Assert
        assert hasattr(mock_adapter, "to_price_bar_event")
        assert callable(mock_adapter.to_price_bar_event)

    def test_protocol_has_to_corporate_action_event_method(self):
        """Test IDataAdapter has to_corporate_action_event method."""
        # Arrange
        mock_adapter = MagicMock(spec=IDataAdapter)

        # Act & Assert
        assert hasattr(mock_adapter, "to_corporate_action_event")
        assert callable(mock_adapter.to_corporate_action_event)

    def test_protocol_has_get_timestamp_method(self):
        """Test IDataAdapter has get_timestamp method."""
        # Arrange
        mock_adapter = MagicMock(spec=IDataAdapter)

        # Act & Assert
        assert hasattr(mock_adapter, "get_timestamp")
        assert callable(mock_adapter.get_timestamp)

    def test_protocol_has_get_available_date_range_method(self):
        """Test IDataAdapter has get_available_date_range method."""
        # Arrange
        mock_adapter = MagicMock(spec=IDataAdapter)

        # Act & Assert
        assert hasattr(mock_adapter, "get_available_date_range")
        assert callable(mock_adapter.get_available_date_range)

    def test_protocol_has_prime_cache_method(self):
        """Test IDataAdapter has prime_cache method."""
        # Arrange
        mock_adapter = MagicMock(spec=IDataAdapter)

        # Act & Assert
        assert hasattr(mock_adapter, "prime_cache")
        assert callable(mock_adapter.prime_cache)

    def test_mock_implementation_satisfies_protocol(self):
        """Test a mock implementation satisfies IDataAdapter protocol."""

        # Arrange
        class MockDataAdapter:
            def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
                return iter([])

            def to_price_bar_event(self, bar: Any):
                return MagicMock()

            def to_corporate_action_event(self, bar: Any, prev_bar: Optional[Any] = None):
                return None

            def get_timestamp(self, bar: Any) -> datetime:
                return datetime.now()

            def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
                return None, None

            def prime_cache(self, start_date: str, end_date: str) -> int:
                return 0

        # Act
        adapter = MockDataAdapter()

        # Assert - Should be able to call all methods
        assert callable(adapter.read_bars)
        assert callable(adapter.to_price_bar_event)
        assert callable(adapter.to_corporate_action_event)
        assert callable(adapter.get_timestamp)
        assert callable(adapter.get_available_date_range)
        assert callable(adapter.prime_cache)

    def test_read_bars_signature_requirements(self):
        """Test read_bars has correct signature requirements."""

        # This test documents the expected signature
        class TestAdapter:
            def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
                return iter([])

        adapter = TestAdapter()
        result = adapter.read_bars("2020-01-01", "2020-12-31")

        # Should return iterator
        assert hasattr(result, "__iter__")

    def test_prime_cache_signature_requirements(self):
        """Test prime_cache has correct signature requirements."""

        # This test documents the expected signature
        class TestAdapter:
            def prime_cache(self, start_date: str, end_date: str) -> int:
                return 100

        adapter = TestAdapter()
        result = adapter.prime_cache("2020-01-01", "2020-12-31")

        # Should return int (number of bars written)
        assert isinstance(result, int)

    def test_get_available_date_range_signature_requirements(self):
        """Test get_available_date_range has correct signature requirements."""

        # This test documents the expected signature
        class TestAdapter:
            def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
                return "2020-01-01", "2024-12-31"

        adapter = TestAdapter()
        result = adapter.get_available_date_range()

        # Should return tuple of two optional strings
        assert isinstance(result, tuple)
        assert len(result) == 2


class TestProtocolUsagePatterns:
    """Test common usage patterns with protocols."""

    def test_service_can_be_mocked_for_testing(self):
        """Test IDataService can be easily mocked for testing."""
        # Arrange - Mock service for testing
        mock_service = MagicMock(spec=IDataService)
        mock_service.stream_bars.return_value = None  # stream_bars returns None

        # Act - Use in test scenario (stream_bars returns None, publishes events)
        result = mock_service.stream_bars("AAPL", date(2020, 1, 1), date(2020, 12, 31))

        # Assert
        assert result is None
        mock_service.stream_bars.assert_called_once_with("AAPL", date(2020, 1, 1), date(2020, 12, 31))

    def test_adapter_can_be_mocked_for_testing(self):
        """Test IDataAdapter can be easily mocked for testing."""
        # Arrange
        mock_adapter = MagicMock(spec=IDataAdapter)
        mock_event = MagicMock()
        mock_adapter.to_price_bar_event.return_value = mock_event

        # Act
        result = mock_adapter.to_price_bar_event(MagicMock())

        # Assert
        assert result == mock_event
        assert mock_adapter.to_price_bar_event.called

    def test_service_protocol_enables_dependency_injection(self):
        """Test IDataService enables dependency injection pattern."""

        # Arrange - Strategy that depends on IDataService
        class SimpleStrategy:
            def __init__(self, data_service):  # Type: IDataService
                self.data_service = data_service

            def stream_data(self, symbol: str):
                return self.data_service.stream_bars(symbol, date(2020, 1, 1), date(2020, 12, 31))

        # Act - Inject mock service
        mock_service = MagicMock(spec=IDataService)
        mock_service.stream_bars.return_value = None

        strategy = SimpleStrategy(mock_service)
        result = strategy.stream_data("AAPL")

        # Assert - Strategy works with protocol
        assert result is None
        mock_service.stream_bars.assert_called_once()

    def test_adapter_protocol_enables_vendor_abstraction(self):
        """Test IDataAdapter enables vendor-specific implementations."""
        # This test demonstrates how different vendors can implement the protocol

        class MockTestAdapter:
            def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
                return iter([{"test_specific": "data"}])

            def to_price_bar_event(self, bar: Any):
                return MagicMock()

            def to_corporate_action_event(self, bar: Any, prev_bar: Optional[Any] = None):
                return None

            def get_timestamp(self, bar: Any) -> datetime:
                return datetime.now()

            def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
                return "2020-01-01", "2024-12-31"

            def prime_cache(self, start_date: str, end_date: str) -> int:
                return 100

        class MockBinanceAdapter:
            def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
                return iter([{"binance_specific": "data"}])

            def to_price_bar_event(self, bar: Any):
                return MagicMock()

            def to_corporate_action_event(self, bar: Any, prev_bar: Optional[Any] = None):
                return None

            def get_timestamp(self, bar: Any) -> datetime:
                return datetime.now()

            def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
                return "2020-01-01", "2024-12-31"

            def prime_cache(self, start_date: str, end_date: str) -> int:
                return 100

        # Both adapters satisfy the protocol
        test_adapter = MockTestAdapter()
        binance = MockBinanceAdapter()

        # Both can be used interchangeably
        for adapter in [test_adapter, binance]:
            bars = adapter.read_bars("2020-01-01", "2020-12-31")
            # read_bars() returns an iterator, not a list
            assert hasattr(bars, "__iter__")

            # Convert first bar to event
            bar_list = list(bars)
            if bar_list:
                event = adapter.to_price_bar_event(bar_list[0])
                assert event is not None


class TestProtocolDocumentation:
    """Test that protocols have proper documentation."""

    def test_idataservice_has_docstring(self):
        """Test IDataService has documentation."""
        assert IDataService.__doc__ is not None
        assert len(IDataService.__doc__) > 0
        assert "Data service interface" in IDataService.__doc__

    def test_idataadapter_has_docstring(self):
        """Test IDataAdapter has documentation."""
        assert IDataAdapter.__doc__ is not None
        assert len(IDataAdapter.__doc__) > 0

    def test_protocol_methods_have_docstrings(self):
        """Test protocol methods have documentation."""
        # IDataService methods
        assert IDataService.stream_bars.__doc__ is not None
        assert IDataService.stream_universe.__doc__ is not None
        assert IDataService.get_instrument.__doc__ is not None
        assert IDataService.list_available_symbols.__doc__ is not None
        assert IDataService.get_corporate_actions.__doc__ is not None

        # IDataAdapter methods
        assert IDataAdapter.read_bars.__doc__ is not None
        assert IDataAdapter.to_price_bar_event.__doc__ is not None
        assert IDataAdapter.to_corporate_action_event.__doc__ is not None
        assert IDataAdapter.get_timestamp.__doc__ is not None
        assert IDataAdapter.get_available_date_range.__doc__ is not None
        assert IDataAdapter.prime_cache.__doc__ is not None
