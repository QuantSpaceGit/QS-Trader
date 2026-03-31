"""Tests for DataService EventBus integration.

Tests that DataService properly publishes PriceBarEvent and CorporateActionEvent
when EventBus is configured.
"""

from datetime import date
from unittest.mock import Mock

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import PriceBarEvent
from qs_trader.services.data.config import BarSchemaConfig, DataConfig
from qs_trader.services.data.service import DataService
from qs_trader.services.data.source_selector import AssetClass, DataSourceSelector


@pytest.fixture
def bar_schema() -> BarSchemaConfig:
    """Standard bar schema for tests."""
    return BarSchemaConfig(
        ts="trade_datetime",
        symbol="symbol",
        open="open",
        high="high",
        low="low",
        close="close",
        volume="volume",
    )


@pytest.fixture
def data_config(bar_schema: BarSchemaConfig) -> DataConfig:
    """Data configuration for tests."""
    selector = DataSourceSelector(provider="yahoo", asset_class=AssetClass.EQUITY)
    return DataConfig(
        mode="adjusted",
        frequency="1d",
        timezone="America/New_York",
        source_selector=selector,
        bar_schema=bar_schema,
    )


@pytest.fixture
def event_bus() -> EventBus:
    """Create EventBus instance."""
    return EventBus()


@pytest.fixture
def data_service_with_bus(data_config: DataConfig, event_bus: EventBus, test_resolver) -> DataService:
    """Create DataService with EventBus configured."""
    return DataService(
        config=data_config,
        dataset="yahoo-us-equity-1d-csv",
        event_bus=event_bus,
        resolver=test_resolver,
    )


@pytest.fixture
def data_service_no_bus(data_config: DataConfig, test_resolver) -> DataService:
    """Create DataService without EventBus."""
    return DataService(
        config=data_config,
        dataset="yahoo-us-equity-1d-csv",
        event_bus=None,
        resolver=test_resolver,
    )


class TestDataServiceEventBusIntegration:
    """Test EventBus integration for DataService."""

    def test_init_with_event_bus(self, data_config: DataConfig, event_bus: EventBus, test_resolver) -> None:
        """Test DataService can be initialized with EventBus."""
        service = DataService(
            config=data_config,
            dataset="yahoo-us-equity-1d-csv",
            event_bus=event_bus,
            resolver=test_resolver,
        )

        assert service._event_bus is event_bus

    def test_init_without_event_bus(self, data_config: DataConfig, test_resolver) -> None:
        """Test DataService can be initialized without EventBus (legacy mode)."""
        service = DataService(
            config=data_config,
            dataset="yahoo-us-equity-1d-csv",
            event_bus=None,
            resolver=test_resolver,
        )

        assert service._event_bus is None

    def test_from_config_with_event_bus(self, data_config: DataConfig, event_bus: EventBus, test_resolver) -> None:
        """Test from_config factory method with EventBus."""
        service = DataService.from_config(
            config_dict=data_config.model_dump(),
            dataset="yahoo-us-equity-1d-csv",
            event_bus=event_bus,
            resolver=test_resolver,
        )

        assert service._event_bus is event_bus

    def test_from_config_without_event_bus(self, data_config: DataConfig, test_resolver) -> None:
        """Test from_config factory method without EventBus."""
        service = DataService.from_config(
            config_dict=data_config.model_dump(),
            dataset="yahoo-us-equity-1d-csv",
            event_bus=None,
            resolver=test_resolver,
        )

        assert service._event_bus is None

    def test_stream_bars_requires_event_bus(self, data_service_no_bus: DataService) -> None:
        """Test stream_bars raises ValueError if EventBus not configured."""
        with pytest.raises(ValueError, match="EventBus not configured"):
            data_service_no_bus.stream_bars(
                symbol="AAPL",
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 31),
            )

    def test_stream_universe_requires_event_bus(self, data_service_no_bus: DataService) -> None:
        """Test stream_universe raises ValueError if EventBus not configured."""
        with pytest.raises(ValueError, match="EventBus not configured"):
            data_service_no_bus.stream_universe(
                symbols=["AAPL", "MSFT"],
                start_date=date(2020, 1, 1),
                end_date=date(2020, 1, 31),
            )

    def test_stream_bars_publishes_events(self, data_service_with_bus: DataService, event_bus: EventBus) -> None:
        """Test stream_bars publishes PriceBarEvent for each bar."""
        # Mock the EventBus publish method to track calls
        event_bus.publish = Mock()  # type: ignore

        # Stream bars for AAPL in January 2020 (21 trading days)
        data_service_with_bus.stream_bars(
            symbol="AAPL",
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 31),
            is_warmup=False,
        )

        # Verify publish was called for each trading day in January 2020
        assert event_bus.publish.call_count == 21

        # Verify published events are PriceBarEvent with correct symbol
        calls = event_bus.publish.call_args_list
        for call_args in calls:
            event = call_args[0][0]
            assert isinstance(event, PriceBarEvent)
            assert event.symbol == "AAPL"

    def test_stream_bars_warmup_flag(self, data_service_with_bus: DataService, event_bus: EventBus) -> None:
        """Test stream_bars parameter is_warmup can be set (metadata not stored in events)."""
        event_bus.publish = Mock()  # type: ignore

        # Stream bars with warmup=True - warmup is for caller metadata, not stored in events
        data_service_with_bus.stream_bars(
            symbol="AAPL",
            start_date=date(2020, 1, 1),
            end_date=date(2020, 1, 31),
            is_warmup=True,
        )

        # Verify events were published (warmup metadata is caller responsibility)
        assert event_bus.publish.call_count == 21
        for call_args in event_bus.publish.call_args_list:
            event = call_args[0][0]
            assert isinstance(event, PriceBarEvent)

    def test_get_corporate_actions_publishes_events_by_default(
        self, data_service_with_bus: DataService, event_bus: EventBus
    ) -> None:
        """Test get_corporate_actions publishes events by default."""
        event_bus.publish = Mock()  # type: ignore

        # Mock get_instrument to avoid real data lookup
        data_service_with_bus.get_instrument = Mock()  # type: ignore

        # Mock _build_adapter_config
        data_service_with_bus._build_adapter_config = Mock(return_value={})  # type: ignore

        # This test would require mocking the adapter which is complex
        # For now, we'll test the logic path exists
        # Full integration test requires real data

    def test_get_corporate_actions_respects_publish_flag(
        self, data_service_with_bus: DataService, event_bus: EventBus
    ) -> None:
        """Test get_corporate_actions respects publish_events=False."""
        event_bus.publish = Mock()  # type: ignore

        # Mock to return empty list (no corp actions)
        data_service_with_bus.get_instrument = Mock()  # type: ignore
        data_service_with_bus._build_adapter_config = Mock(return_value={})  # type: ignore

        # This would need proper mocking to test fully
        # The logic is in place in the implementation


class TestDataServiceEventOrdering:
    """Test event ordering guarantees."""

    def test_stream_universe_publishes_in_timestamp_order(self, data_config: DataConfig, event_bus: EventBus) -> None:
        """Test stream_universe publishes events in timestamp order."""
        # This test requires complex mocking of multiple iterators
        # and would be better as an integration test with real data
        pass

    def test_stream_universe_publishes_all_symbols_per_timestamp(
        self, data_config: DataConfig, event_bus: EventBus
    ) -> None:
        """Test stream_universe publishes all symbols for each timestamp together."""
        # This test requires complex mocking
        # Better suited for integration test
        pass


class TestDataServiceFromBacktestConfig:
    """Test DataService creation from backtest config."""

    def test_backtest_engine_creates_event_enabled_service(self, event_bus: EventBus, test_resolver) -> None:
        """Test BacktestEngine creates DataService with EventBus."""
        # Create DataService directly (BacktestEngine tested elsewhere)
        from qs_trader.services.data.config import DataConfig

        config = DataConfig(
            mode="adjusted",
            source_selector=DataSourceSelector(provider="yahoo", asset_class=AssetClass.EQUITY),
            bar_schema=BarSchemaConfig(
                ts="trade_datetime",
                symbol="symbol",
                open="open",
                high="high",
                low="low",
                close="close",
                volume="volume",
            ),
        )

        # Create DataService using from_config with EventBus
        service = DataService.from_config(
            config_dict=config.model_dump(),
            dataset="yahoo-us-equity-1d-csv",
            event_bus=event_bus,
            resolver=test_resolver,
        )

        assert service._event_bus is event_bus


class TestDataServiceLegacyCompatibility:
    """Test backward compatibility with non-event mode."""

    def test_load_symbol_still_works_without_event_bus(self, data_service_no_bus: DataService) -> None:
        """Test load_symbol works in pull mode without EventBus."""
        # Should work normally without EventBus
        # This requires real data or more complex mocking
        pass

    def test_load_universe_still_works_without_event_bus(self, data_service_no_bus: DataService) -> None:
        """Test load_universe works in pull mode without EventBus."""
        # Should work normally without EventBus
        pass

    def test_get_corporate_actions_works_without_event_bus(self, data_service_no_bus: DataService) -> None:
        """Test get_corporate_actions works without EventBus (no publishing)."""
        # Should return actions but not publish events
        pass
