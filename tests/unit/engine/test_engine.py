"""
Unit tests for qs_trader.engine.engine module.

Tests cover BacktestEngine initialization, configuration loading, and execution.
Note: Tests focus on minimal DataService-only implementation.
"""

from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from qs_trader.engine.config import (
    BacktestConfig,
    DataSelectionConfig,
    DataSourceConfig,
    FeatureConfig,
    RiskPolicyConfig,
    StrategyConfigItem,
)
from qs_trader.engine.engine import BacktestEngine, BacktestResult, _build_clickhouse_manifest
from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_system_config(tmp_path: Path):
    """Provide mock system configuration.

    Uses temporary directories to avoid polluting user-facing folders.
    """
    mock_config = Mock()
    mock_config.data = Mock()
    mock_config.data.sources_config = "config/data_sources.yaml"
    mock_config.data.default_mode = "adjusted"
    mock_config.data.default_timezone = "America/New_York"

    # Output configuration - use temp directory
    output_dir = tmp_path / "output" / "backtests"
    output_dir.mkdir(parents=True, exist_ok=True)
    mock_config.output = Mock()
    mock_config.output.experiments_root = str(output_dir)
    mock_config.output.run_id_format = "%Y%m%d_%H%M%S"
    mock_config.output.event_store = Mock()
    mock_config.output.event_store.backend = "parquet"
    mock_config.output.event_store.filename = "events.{backend}"

    # Custom libraries - use test fixtures
    mock_config.custom_libraries = Mock()
    mock_config.custom_libraries.strategies = "tests/fixtures/strategies"
    mock_config.logging = Mock()
    # Mock the logger config with all required attributes
    mock_logger_config = Mock()
    mock_logger_config.level = "INFO"
    mock_logger_config.format = "json"
    mock_logger_config.timestamp_format = "compact"
    mock_logger_config.enable_file = False  # Disable file logging in tests
    mock_logger_config.file_path = None
    mock_logger_config.file_level = "WARNING"
    mock_logger_config.file_rotation = True
    mock_logger_config.max_file_size_mb = 10
    mock_logger_config.backup_count = 3
    mock_logger_config.console_width = 0
    mock_config.logging.to_logger_config = Mock(return_value=mock_logger_config)
    return mock_config


@pytest.fixture
def sample_backtest_config() -> BacktestConfig:
    """Provide sample backtest configuration for testing."""
    return BacktestConfig(
        backtest_id="test_backtest",
        start_date=datetime(2020, 1, 1),
        end_date=datetime(2020, 12, 31),
        initial_equity=Decimal("100000"),
        replay_speed=0.0,
        data=DataSelectionConfig(
            sources=[
                DataSourceConfig(
                    name="test-source",
                    universe=["AAPL", "MSFT"],
                )
            ]
        ),
        strategies=[
            StrategyConfigItem(
                strategy_id="test_strategy",
                universe=["AAPL"],
                data_sources=["test-source"],
                config={},
            )
        ],
        risk_policy=RiskPolicyConfig(name="naive", config={}),
    )


@pytest.fixture
def mock_event_bus() -> EventBus:
    """Provide mock event bus."""
    return Mock(spec=EventBus)


@pytest.fixture
def mock_data_service():
    """Provide mock data service."""
    service = Mock()
    service.load_symbol = Mock(return_value=iter([]))
    return service


@pytest.fixture
def mock_event_store():
    """Provide mock event store."""
    return Mock(spec=InMemoryEventStore)


# ============================================================================
# BacktestResult Tests
# ============================================================================


class TestBacktestResult:
    """Test suite for BacktestResult dataclass."""

    def test_create_result(self) -> None:
        """Test creating BacktestResult with all fields."""
        # Arrange
        start = date(2020, 1, 1)
        end = date(2020, 12, 31)
        bars = 252
        duration = timedelta(seconds=10)

        # Act
        result = BacktestResult(
            start_date=start,
            end_date=end,
            bars_processed=bars,
            duration=duration,
        )

        # Assert
        assert result.start_date == start
        assert result.end_date == end
        assert result.bars_processed == bars
        assert result.duration == duration

    def test_result_is_dataclass(self) -> None:
        """Test BacktestResult is a dataclass."""
        # Arrange & Act
        result = BacktestResult(
            start_date=date(2020, 1, 1),
            end_date=date(2020, 12, 31),
            bars_processed=100,
            duration=timedelta(seconds=5),
        )

        # Assert
        assert hasattr(result, "__dataclass_fields__")


# ============================================================================
# BacktestEngine Initialization Tests
# ============================================================================


class TestBacktestEngineInit:
    """Test suite for BacktestEngine.__init__."""

    def test_init_with_required_params(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
        mock_data_service,
    ) -> None:
        """Test initializing engine with required parameters."""
        # Arrange & Act
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Assert
        assert engine.config == sample_backtest_config
        assert engine._event_bus == mock_event_bus
        assert engine._data_service == mock_data_service
        assert engine._event_store is None
        assert engine._results_dir is None

    def test_init_with_optional_params(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
        mock_data_service,
        mock_event_store,
        tmp_path: Path,
    ) -> None:
        """Test initializing engine with optional parameters."""
        # Arrange & Act
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
            event_store=mock_event_store,
            results_dir=tmp_path,
        )

        # Assert
        assert engine._event_store == mock_event_store
        assert engine._results_dir == tmp_path

    def test_init_logs_initialization(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
        mock_data_service,
    ) -> None:
        """Test initialization logs engine setup."""
        # Arrange & Act & Assert
        # Should not raise exception
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )
        assert engine is not None


# ============================================================================
# BacktestEngine.from_config Tests
# ============================================================================


class TestBacktestEngineFromConfig:
    """Test suite for BacktestEngine.from_config factory method."""

    @patch("qs_trader.engine.engine.get_system_config")
    @patch("qs_trader.system.log_system.LoggerFactory")
    @patch("qs_trader.engine.engine.DataService")
    @patch("qs_trader.engine.engine.EventBus")
    @patch("qs_trader.engine.engine.SQLiteEventStore")
    def test_from_config_creates_engine(
        self,
        mock_sqlite_store,
        mock_event_bus_class,
        mock_data_service_class,
        mock_logger_factory,
        mock_get_system_config,
        sample_backtest_config: BacktestConfig,
        mock_system_config,
        tmp_path: Path,
    ) -> None:
        """Test from_config creates properly configured engine."""
        # Arrange
        mock_get_system_config.return_value = mock_system_config
        mock_event_bus = Mock()
        mock_event_bus_class.return_value = mock_event_bus
        mock_data_service = Mock()
        mock_data_service_class.from_config.return_value = mock_data_service
        mock_event_store = Mock()
        mock_sqlite_store.return_value = mock_event_store

        # Mock system config output path to use tmp_path
        mock_system_config.output.experiments_root = str(tmp_path / "output")

        # Act
        engine = BacktestEngine.from_config(sample_backtest_config)

        # Assert
        assert isinstance(engine, BacktestEngine)
        assert engine.config == sample_backtest_config
        # Logger configuration happens successfully (verified by log output in test)
        mock_event_bus.attach_store.assert_called_once()

    @patch("qs_trader.engine.engine.get_system_config")
    @patch("qs_trader.system.log_system.LoggerFactory")
    @patch("qs_trader.engine.engine.DataService")
    @patch("qs_trader.engine.engine.EventBus")
    @patch("qs_trader.engine.engine.ParquetEventStore")
    def test_from_config_creates_results_directory(
        self,
        mock_parquet_store,
        mock_event_bus_class,
        mock_data_service_class,
        mock_logger_factory,
        mock_get_system_config,
        sample_backtest_config: BacktestConfig,
        mock_system_config,
        tmp_path: Path,
    ) -> None:
        """Test from_config creates results directory."""
        # Arrange
        mock_get_system_config.return_value = mock_system_config
        mock_event_bus_class.return_value = Mock()
        mock_data_service_class.from_config.return_value = Mock()
        mock_parquet_store.return_value = Mock()

        # Set output dir to tmp_path and use parquet backend
        experiments_root = tmp_path / "experiments"
        mock_system_config.output.experiments_root = str(experiments_root)
        mock_system_config.output.run_id_format = "%Y%m%d_%H%M%S"
        mock_system_config.output.event_store.backend = "parquet"
        mock_system_config.output.event_store.filename = "events.{backend}"

        # Act
        engine = BacktestEngine.from_config(sample_backtest_config)

        # Assert
        assert engine._results_dir is not None
        assert engine._results_dir.exists()

    @patch("qs_trader.engine.engine.get_system_config")
    @patch("qs_trader.system.log_system.LoggerFactory")
    @patch("qs_trader.engine.engine.DataService")
    @patch("qs_trader.engine.engine.EventBus")
    @patch("qs_trader.engine.engine.ParquetEventStore")
    def test_from_config_fallback_to_memory_store_on_error(
        self,
        mock_parquet_store,
        mock_event_bus_class,
        mock_data_service_class,
        mock_logger_factory,
        mock_get_system_config,
        sample_backtest_config: BacktestConfig,
        mock_system_config,
        tmp_path: Path,
    ) -> None:
        """Test from_config falls back to InMemoryEventStore on Parquet error."""
        # Arrange
        mock_get_system_config.return_value = mock_system_config
        mock_event_bus_class.return_value = Mock()
        mock_data_service_class.from_config.return_value = Mock()
        mock_parquet_store.side_effect = Exception("Parquet initialization failed")

        mock_system_config.output.experiments_root = str(tmp_path / "output")

        # Act
        with patch("qs_trader.engine.engine.InMemoryEventStore") as mock_memory_store:
            mock_memory_store.return_value = Mock()
            engine = BacktestEngine.from_config(sample_backtest_config)

        # Assert
        assert engine._event_store is not None
        mock_memory_store.assert_called_once()

    @patch("qs_trader.engine.engine.get_system_config")
    @patch("qs_trader.system.log_system.LoggerFactory")
    @patch("qs_trader.engine.engine.DataService")
    @patch("qs_trader.engine.engine.EventBus")
    @patch("qs_trader.engine.engine.InMemoryEventStore")
    def test_from_config_uses_first_data_source(
        self,
        mock_memory_store,
        mock_event_bus_class,
        mock_data_service_class,
        mock_logger_factory,
        mock_get_system_config,
        mock_system_config,
        tmp_path: Path,
    ) -> None:
        """Test from_config uses data source for DataService initialization."""
        # Arrange
        config = BacktestConfig(
            backtest_id="test_source_config",
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2020, 12, 31),
            initial_equity=Decimal("100000"),
            data=DataSelectionConfig(
                sources=[
                    DataSourceConfig(name="source1", universe=["AAPL", "MSFT"]),
                ]
            ),
            strategies=[
                StrategyConfigItem(
                    strategy_id="test",
                    universe=["AAPL"],
                    data_sources=["source1"],
                    config={},
                )
            ],
            risk_policy=RiskPolicyConfig(name="naive", config={}),
        )

        mock_get_system_config.return_value = mock_system_config
        mock_event_bus_class.return_value = Mock()
        mock_data_service_class.from_config.return_value = Mock()
        mock_memory_store.return_value = Mock()
        mock_system_config.output.experiments_root = str(tmp_path / "output")

        # Act
        BacktestEngine.from_config(config)

        # Assert
        call_kwargs = mock_data_service_class.from_config.call_args[1]
        assert call_kwargs["dataset"] == "source1"


# ============================================================================
# BacktestEngine.run Tests
# ============================================================================


class TestBacktestEngineRun:
    """Test suite for BacktestEngine.run method."""

    def test_run_returns_result(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
        mock_data_service,
    ) -> None:
        """Test run returns BacktestResult."""
        # Arrange
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Act
        result = engine.run()

        # Assert
        assert isinstance(result, BacktestResult)
        # BacktestResult stores datetime, not date
        assert result.start_date == sample_backtest_config.start_date
        assert result.end_date == sample_backtest_config.end_date
        assert isinstance(result.duration, timedelta)

    def test_run_subscribes_to_bar_events(
        self,
        sample_backtest_config: BacktestConfig,
        mock_data_service,
    ) -> None:
        """Test run subscribes to bar events for counting."""
        # Arrange
        mock_event_bus = Mock()
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Act
        engine.run()

        # Assert
        mock_event_bus.subscribe.assert_called()
        call_args = mock_event_bus.subscribe.call_args[0]
        assert call_args[0] == "bar"

    def test_run_loads_symbols_from_first_source(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
    ) -> None:
        """Test run loads symbols from first data source using stream_universe."""
        # Arrange
        mock_data_service = Mock()
        mock_data_service.stream_universe = Mock()
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Act
        engine.run()

        # Assert
        # Should be called once with all symbols from first source
        mock_data_service.stream_universe.assert_called_once()
        call_args = mock_data_service.stream_universe.call_args
        assert set(call_args.kwargs["symbols"]) == {"AAPL", "MSFT"}
        assert call_args.kwargs["is_warmup"] is False
        assert call_args.kwargs["strict"] is False

    def test_run_handles_symbol_load_failure_gracefully(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
    ) -> None:
        """Test run handles data stream failures gracefully."""
        # Arrange
        mock_data_service = Mock()
        mock_data_service.stream_universe = Mock(side_effect=Exception("Stream failed"))
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Act & Assert
        # Should raise RuntimeError wrapping the original exception
        with pytest.raises(RuntimeError, match="Backtest execution failed"):
            engine.run()

    def test_run_tracks_bar_count(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
    ) -> None:
        """Test run tracks number of bars processed."""
        # Arrange
        # Create mock that returns some bars
        mock_bar1 = Mock()
        mock_bar2 = Mock()
        mock_data_service = Mock()
        mock_data_service.load_symbol = Mock(return_value=iter([mock_bar1, mock_bar2]))

        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Act
        result = engine.run()

        # Assert
        assert result.bars_processed >= 0

    def test_run_raises_runtime_error_on_failure(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
        mock_data_service,
    ) -> None:
        """Test run raises RuntimeError on critical failure."""
        # Arrange
        mock_event_bus.subscribe.side_effect = Exception("Critical error")  # type: ignore[attr-defined]
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Act & Assert
        with pytest.raises(RuntimeError) as exc_info:
            engine.run()
        assert "Backtest execution failed" in str(exc_info.value)

    def test_run_calculates_duration(
        self,
        sample_backtest_config: BacktestConfig,
        mock_event_bus: EventBus,
        mock_data_service,
    ) -> None:
        """Test run calculates execution duration."""
        # Arrange
        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Act
        result = engine.run()

        # Assert
        assert isinstance(result.duration, timedelta)
        assert result.duration.total_seconds() >= 0


# ============================================================================
# Integration Tests
# ============================================================================


class TestBacktestEngineIntegration:
    """Integration tests for BacktestEngine with real components."""

    def test_engine_with_real_event_bus(
        self,
        sample_backtest_config: BacktestConfig,
        mock_data_service,
    ) -> None:
        """Test engine works with real EventBus."""
        # Arrange
        event_bus = EventBus()
        event_store = InMemoryEventStore()

        engine = BacktestEngine(
            config=sample_backtest_config,
            event_bus=event_bus,
            data_service=mock_data_service,
            event_store=event_store,
        )

        # Act
        result = engine.run()

        # Assert
        assert isinstance(result, BacktestResult)
        assert result.bars_processed >= 0

    def test_engine_with_multiple_symbols(
        self,
        mock_event_bus: EventBus,
        mock_data_service,
    ) -> None:
        """Test engine handles multiple symbols correctly."""
        # Arrange
        config = BacktestConfig(
            backtest_id="test_multiple_symbols",
            start_date=datetime(2020, 1, 1),
            end_date=datetime(2020, 12, 31),
            initial_equity=Decimal("100000"),
            data=DataSelectionConfig(
                sources=[
                    DataSourceConfig(
                        name="test-source",
                        universe=["AAPL", "MSFT", "GOOGL", "TSLA"],
                    )
                ]
            ),
            strategies=[
                StrategyConfigItem(
                    strategy_id="test",
                    universe=["AAPL", "MSFT"],
                    data_sources=["test-source"],
                    config={},
                )
            ],
            risk_policy=RiskPolicyConfig(name="naive", config={}),
        )

        engine = BacktestEngine(
            config=config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Act
        engine.run()

        # Assert
        # Should call stream_universe once with all 4 symbols
        mock_data_service.stream_universe.assert_called_once()
        call_args = mock_data_service.stream_universe.call_args
        assert set(call_args.kwargs["symbols"]) == {"AAPL", "MSFT", "GOOGL", "TSLA"}


# ============================================================================
# _build_clickhouse_manifest Tests
# ============================================================================


def _make_data_service(*, provider: str, dataset: str = "qs-datamaster-equity-1d") -> Mock:
    """Build a minimal mock DataService for manifest-builder testing."""
    svc = Mock()
    svc.dataset = dataset

    ch_config = {
        "host": "localhost",
        "port": 8123,
        "database": "market",
    }
    source_cfg: dict = {
        "provider": provider,
        "adjusted": True,
        "clickhouse": ch_config,
    }
    svc.resolver.get_source_config.return_value = source_cfg
    return svc


def _make_backtest_config(
    start: datetime | None = None,
    end: datetime | None = None,
    symbols: list[str] | None = None,
) -> BacktestConfig:
    """Build a minimal BacktestConfig for manifest-builder testing."""
    symbols = symbols or ["AAPL", "MSFT"]
    return BacktestConfig(
        backtest_id="test_bt",
        start_date=start or datetime(2023, 1, 1),
        end_date=end or datetime(2023, 12, 31),
        initial_equity=Decimal("100000"),
        replay_speed=0.0,
        data=DataSelectionConfig(
            sources=[
                DataSourceConfig(
                    name="qs-datamaster-equity-1d",
                    universe=symbols,
                )
            ]
        ),
        strategies=[
            StrategyConfigItem(
                strategy_id="test_strategy",
                universe=symbols,
                data_sources=["qs-datamaster-equity-1d"],
                config={},
            )
        ],
        risk_policy=RiskPolicyConfig(name="naive", config={}),
    )


class TestManifestBuilderFunction:
    """Unit tests for the _build_clickhouse_manifest() module-level helper.

    The helper is tested in isolation so that each behaviour (provider gating,
    field mapping, feature-service integration) can be verified without
    spinning up an engine or DataService.
    """

    def test_returns_none_for_yahoo_provider(self) -> None:
        """Non-ClickHouse (Yahoo) data sources must produce no manifest."""
        # Arrange
        data_service = _make_data_service(provider="yahoo")
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is None

    def test_returns_none_for_custom_csv_provider(self) -> None:
        """Custom CSV sources must also produce no manifest."""
        # Arrange
        data_service = _make_data_service(provider="custom")
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is None

    def test_returns_none_when_source_config_missing(self) -> None:
        """KeyError from resolver must be handled gracefully — return None."""
        # Arrange
        data_service = Mock()
        data_service.dataset = "unknown-source"
        data_service.resolver.get_source_config.side_effect = KeyError("unknown-source")
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is None

    def test_returns_manifest_for_qs_datamaster_provider(self) -> None:
        """qs-datamaster sources must produce a non-None ClickHouseInputManifest."""
        from qs_trader.services.reporting.manifest import ClickHouseInputManifest

        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL", "MSFT"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert isinstance(result, ClickHouseInputManifest)

    def test_manifest_source_name_matches_dataset(self) -> None:
        """source_name must equal the data_service dataset name."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster", dataset="qs-datamaster-equity-1d")
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.source_name == "qs-datamaster-equity-1d"

    def test_manifest_database_from_clickhouse_config(self) -> None:
        """database field must come from the clickhouse sub-config."""
        # Arrange
        data_service = Mock()
        data_service.dataset = "qs-datamaster-equity-1d"
        data_service.resolver.get_source_config.return_value = {
            "provider": "qs-datamaster",
            "adjusted": True,
            "clickhouse": {"host": "ch-host", "database": "my_market_db"},
        }
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.database == "my_market_db"

    def test_manifest_bars_table_default(self) -> None:
        """bars_table defaults to the ClickHouse adapter's canonical OHLCV table."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.bars_table == "as_us_equity_ohlc_daily"

    def test_manifest_bars_table_from_source_config(self) -> None:
        """bars_table can be overridden via an explicit key in the source config."""
        # Arrange
        data_service = Mock()
        data_service.dataset = "qs-datamaster-equity-1d"
        data_service.resolver.get_source_config.return_value = {
            "provider": "qs-datamaster",
            "adjusted": True,
            "bars_table": "equity_ohlcv_v2",
            "clickhouse": {"database": "market"},
        }
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.bars_table == "equity_ohlcv_v2"

    def test_manifest_adjustment_mode_total_return_when_adjusted_true(self) -> None:
        """adjusted=True in source config must map to adjustment_mode='total_return'."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")  # adjusted=True
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.adjustment_mode == "total_return"

    def test_manifest_adjustment_mode_split_adjusted_when_not_adjusted(self) -> None:
        """adjusted=False or absent must map to adjustment_mode='split_adjusted'."""
        # Arrange
        data_service = Mock()
        data_service.dataset = "qs-datamaster-equity-1d"
        data_service.resolver.get_source_config.return_value = {
            "provider": "qs-datamaster",
            "adjusted": False,
            "clickhouse": {"database": "market"},
        }
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.adjustment_mode == "split_adjusted"

    def test_manifest_symbols_match_source_symbols(self) -> None:
        """symbols list must exactly match the source_symbols argument."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config()
        symbols = ["AAPL", "MSFT", "GOOGL"]

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=symbols,
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.symbols == symbols

    def test_manifest_dates_extracted_from_config(self) -> None:
        """start_date and end_date must be date objects extracted from config datetimes."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config(
            start=datetime(2022, 3, 15),
            end=datetime(2023, 6, 30),
        )

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.start_date == date(2022, 3, 15)
        assert result.end_date == date(2023, 6, 30)

    def test_manifest_features_and_regime_tables_absent_without_feature_service(self) -> None:
        """When no feature_service is active, features_table and regime_table must be None."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert
        assert result is not None
        assert result.features_table is None
        assert result.regime_table is None
        assert result.feature_set_version is None
        assert result.regime_version is None
        assert result.feature_columns is None

    def test_manifest_feature_tables_from_feature_service_constants(self) -> None:
        """When a FeatureService is active, features_table and regime_table must be populated."""
        from qs_trader.services.features.service import FeatureService

        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config()
        feature_service = Mock()  # Any truthy value counts as "active"
        feature_config = FeatureConfig(feature_version="v2", regime_version="v3", columns=None)

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=feature_service,
            feature_config=feature_config,
        )

        # Assert
        assert result is not None
        assert result.features_table == FeatureService.FEATURES_TABLE
        assert result.regime_table == FeatureService.REGIME_TABLE

    def test_manifest_feature_version_and_regime_version_from_feature_config(self) -> None:
        """feature_set_version and regime_version must match the FeatureConfig values."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config()
        feature_service = Mock()
        feature_config = FeatureConfig(feature_version="v2", regime_version="v3", columns=None)

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=feature_service,
            feature_config=feature_config,
        )

        # Assert
        assert result is not None
        assert result.feature_set_version == "v2"
        assert result.regime_version == "v3"

    def test_manifest_feature_columns_from_feature_config(self) -> None:
        """feature_columns must be taken from FeatureConfig.columns when specified."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config()
        feature_service = Mock()
        requested_cols = ["trend_strength", "momentum_score"]
        feature_config = FeatureConfig(feature_version="v1", regime_version="v1", columns=requested_cols)

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=feature_service,
            feature_config=feature_config,
        )

        # Assert
        assert result is not None
        assert result.feature_columns == requested_cols

    def test_manifest_is_immutable(self) -> None:
        """The produced manifest must be frozen (immutable)."""
        # Arrange
        data_service = _make_data_service(provider="qs-datamaster")
        config = _make_backtest_config()

        # Act
        result = _build_clickhouse_manifest(
            data_service=data_service,
            config=config,
            source_symbols=["AAPL"],
            feature_service=None,
            feature_config=None,
        )

        # Assert — Pydantic frozen models raise on attribute assignment
        assert result is not None
        with pytest.raises(Exception):
            result.source_name = "tampered"  # type: ignore[misc]

    def test_engine_stores_manifest_for_clickhouse_run(
        self,
        mock_event_bus: EventBus,
        mock_data_service,
    ) -> None:
        """BacktestEngine.__init__ must expose _input_manifest when passed."""
        from qs_trader.services.reporting.manifest import ClickHouseInputManifest

        # Arrange
        manifest = ClickHouseInputManifest(
            source_name="qs-datamaster-equity-1d",
            database="market",
            bars_table="as_us_equity_ohlc_daily",
            symbols=["AAPL"],
            start_date=date(2023, 1, 1),
            end_date=date(2023, 12, 31),
        )
        config = _make_backtest_config()

        # Act
        engine = BacktestEngine(
            config=config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
            input_manifest=manifest,
        )

        # Assert
        assert engine._input_manifest is manifest

    def test_engine_input_manifest_defaults_to_none(
        self,
        mock_event_bus: EventBus,
        mock_data_service,
    ) -> None:
        """_input_manifest must default to None for Yahoo/CSV runs."""
        # Arrange
        config = _make_backtest_config()

        # Act
        engine = BacktestEngine(
            config=config,
            event_bus=mock_event_bus,
            data_service=mock_data_service,
        )

        # Assert
        assert engine._input_manifest is None
