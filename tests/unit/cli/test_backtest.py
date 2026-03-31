"""
Unit tests for qs_trader.cli.commands.backtest module.

Tests cover:
- Basic command execution with required --file option
- CLI option overrides (--silent, --replay-speed, --start-date, --end-date)
- Error handling for missing/invalid config files
- Integration with BacktestEngine
"""

from datetime import datetime
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from qs_trader.cli.commands.backtest import backtest_command


@pytest.fixture
def cli_runner():
    """Fixture providing Click CLI test runner."""
    return CliRunner()


@pytest.fixture
def mock_backtest_result():
    """Fixture providing mock BacktestResult."""
    from datetime import timedelta

    result = Mock()
    result.start_date = datetime(2020, 1, 1).date()
    result.end_date = datetime(2020, 12, 31).date()
    result.bars_processed = 252
    result.duration = timedelta(seconds=5.123)
    return result


@pytest.fixture
def mock_engine(mock_backtest_result):
    """Fixture providing mock BacktestEngine."""
    engine = Mock()
    engine.run.return_value = mock_backtest_result
    engine._results_dir = None  # Memory backend by default
    return engine


@pytest.fixture
def valid_config_file(tmp_path):
    """Fixture providing valid backtest config file."""
    config = tmp_path / "test_config.yaml"
    config.write_text(
        """
backtest_id: test_backtest
start_date: 2020-01-01
end_date: 2020-12-31
initial_equity: 100000
replay_speed: 0.0

data:
  sources:
    - name: test-source
      universe: [AAPL]

strategies:
  - strategy_id: test_strategy
    universe: [AAPL]
    data_sources: [test-source]
    config: {}

risk_policy:
  name: naive
  config: {}
"""
    )
    return config


@pytest.fixture
def mock_system_config():
    """Fixture providing mock SystemConfig."""
    config = Mock()
    config.output.event_store.backend = "memory"
    config.output.event_store.filename = "events.{backend}"
    config.output.experiments_root = "experiments"
    config.output.run_id_format = "%Y%m%d_%H%M%S"
    config.output.capture_git_info = False
    config.output.capture_environment = False
    config.output.database.enabled = False
    return config


@pytest.fixture
def mock_experiment_setup(valid_config_file):
    """Fixture providing mock ExperimentResolver and ExperimentMetadata setup."""

    def _setup(mock_resolver_class, mock_metadata_class):
        """Setup experiment mocks with proper return values."""
        # ExperimentResolver class methods
        mock_resolver_class.resolve_config_path.return_value = valid_config_file
        mock_resolver_class.get_experiment_dir.return_value = valid_config_file.parent
        mock_resolver_class.validate_experiment_structure.return_value = None
        mock_resolver_class.generate_run_id.return_value = "20241119_120000"
        mock_resolver_class.create_run_dir.return_value = valid_config_file.parent / "runs" / "20241119_120000"

        # ExperimentMetadata class methods
        mock_metadata_class.compute_config_hash.return_value = "abc123"
        mock_metadata_class.save_config_snapshot.return_value = valid_config_file
        mock_metadata_class.write_run_metadata.return_value = valid_config_file
        mock_metadata_class.create_latest_symlink.return_value = None

    return _setup


# ============================================================================
# Basic Command Tests
# ============================================================================


class TestBacktestCommandBasics:
    """Test basic command functionality."""

    def test_command_requires_file_option(self, cli_runner):
        """Test that CONFIG_PATH argument is required."""
        result = cli_runner.invoke(backtest_command, [])

        assert result.exit_code == 2  # Click error code for missing required argument
        assert "Missing argument" in result.output or "CONFIG_PATH" in result.output

    def test_command_with_nonexistent_file(self, cli_runner):
        """Test command fails gracefully with nonexistent file."""
        result = cli_runner.invoke(backtest_command, ["--file", "nonexistent.yaml"])

        assert result.exit_code == 2
        assert "does not exist" in result.output.lower() or "invalid" in result.output.lower()

    @patch("qs_trader.cli.commands.backtest.ExperimentMetadata")
    @patch("qs_trader.cli.commands.backtest.ExperimentResolver")
    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_successful_backtest_execution(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        mock_resolver_class,
        mock_metadata_class,
        cli_runner,
        valid_config_file,
        mock_engine,
        mock_backtest_result,
        mock_system_config,
        mock_experiment_setup,
    ):
        """Test successful backtest execution with valid config."""
        # Setup mocks
        mock_config = Mock()
        mock_config.backtest_id = "test_backtest"
        mock_config.sanitized_backtest_id = "test_backtest"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = 0.0
        mock_config.display_events = ["bar"]

        # Setup experiment mocks
        mock_experiment_setup(mock_resolver_class, mock_metadata_class)

        mock_load_config.return_value = mock_config
        mock_engine_class.from_config.return_value = mock_engine
        mock_get_sys_config.return_value = mock_system_config

        # Execute command
        result = cli_runner.invoke(backtest_command, [str(valid_config_file)])

        # Assertions
        assert result.exit_code == 0
        assert "✓ Backtest completed successfully!" in result.output
        assert "252" in result.output  # bars_processed
        mock_reload_sys_config.assert_called_once()
        mock_load_config.assert_called_once()
        mock_engine.run.assert_called_once()
        mock_engine.shutdown.assert_called_once()


# ============================================================================
# CLI Override Tests
# ============================================================================


class TestBacktestCommandOverrides:
    """Test CLI option overrides."""

    @patch("qs_trader.cli.commands.backtest.ExperimentMetadata")
    @patch("qs_trader.cli.commands.backtest.ExperimentResolver")
    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_silent_mode_override(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        mock_resolver_class,
        mock_metadata_class,
        cli_runner,
        valid_config_file,
        mock_engine,
        mock_system_config,
        mock_experiment_setup,
    ):
        """Test --silent flag overrides config replay_speed."""
        mock_config = Mock()
        mock_config.backtest_id = "test"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = 0.5  # Will be overridden
        mock_config.display_events = ["bar"]

        mock_experiment_setup(mock_resolver_class, mock_metadata_class)
        mock_load_config.return_value = mock_config
        mock_engine_class.from_config.return_value = mock_engine
        mock_get_sys_config.return_value = mock_system_config

        result = cli_runner.invoke(backtest_command, [str(valid_config_file), "--silent"])

        assert result.exit_code == 0
        assert mock_config.replay_speed == -1.0
        assert mock_config.display_events is None
        assert "Silent mode" in result.output

    @patch("qs_trader.cli.commands.backtest.ExperimentMetadata")
    @patch("qs_trader.cli.commands.backtest.ExperimentResolver")
    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_replay_speed_override(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        mock_resolver_class,
        mock_metadata_class,
        cli_runner,
        valid_config_file,
        mock_engine,
        mock_system_config,
        mock_experiment_setup,
    ):
        """Test --replay-speed option overrides config value."""
        mock_config = Mock()
        mock_config.backtest_id = "test"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = 0.0
        mock_config.display_events = ["bar"]

        mock_experiment_setup(mock_resolver_class, mock_metadata_class)
        mock_load_config.return_value = mock_config
        mock_engine_class.from_config.return_value = mock_engine
        mock_get_sys_config.return_value = mock_system_config

        result = cli_runner.invoke(backtest_command, [str(valid_config_file), "-r", "0.25"])

        assert result.exit_code == 0
        assert mock_config.replay_speed == 0.25
        assert "0.25s per event" in result.output

    @patch("qs_trader.cli.commands.backtest.ExperimentMetadata")
    @patch("qs_trader.cli.commands.backtest.ExperimentResolver")
    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_date_overrides(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        mock_resolver_class,
        mock_metadata_class,
        cli_runner,
        valid_config_file,
        mock_engine,
        mock_system_config,
        mock_experiment_setup,
    ):
        """Test --start-date and --end-date options override config values."""
        mock_config = Mock()
        mock_config.backtest_id = "test"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = 0.0
        mock_config.display_events = ["bar"]

        mock_experiment_setup(mock_resolver_class, mock_metadata_class)
        mock_load_config.return_value = mock_config
        mock_engine_class.from_config.return_value = mock_engine
        mock_get_sys_config.return_value = mock_system_config

        result = cli_runner.invoke(
            backtest_command,
            [
                str(valid_config_file),
                "--start-date",
                "2020-06-01",
                "--end-date",
                "2020-09-30",
            ],
        )

        assert result.exit_code == 0
        assert mock_config.start_date == datetime(2020, 6, 1)
        assert mock_config.end_date == datetime(2020, 9, 30)
        assert "2020-06-01" in result.output
        assert "2020-09-30" in result.output

    @patch("qs_trader.cli.commands.backtest.ExperimentMetadata")
    @patch("qs_trader.cli.commands.backtest.ExperimentResolver")
    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_combined_overrides(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        mock_resolver_class,
        mock_metadata_class,
        cli_runner,
        valid_config_file,
        mock_engine,
        mock_system_config,
        mock_experiment_setup,
    ):
        """Test multiple CLI overrides work together."""
        mock_config = Mock()
        mock_config.backtest_id = "test"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = 0.0
        mock_config.display_events = ["bar"]

        mock_experiment_setup(mock_resolver_class, mock_metadata_class)
        mock_load_config.return_value = mock_config
        mock_engine_class.from_config.return_value = mock_engine
        mock_get_sys_config.return_value = mock_system_config

        result = cli_runner.invoke(
            backtest_command,
            [
                str(valid_config_file),
                "--silent",
                "--start-date",
                "2020-03-01",
                "--end-date",
                "2020-06-30",
            ],
        )

        assert result.exit_code == 0
        assert mock_config.replay_speed == -1.0
        assert mock_config.display_events is None
        assert mock_config.start_date == datetime(2020, 3, 1)
        assert mock_config.end_date == datetime(2020, 6, 30)


# ============================================================================
# Display Output Tests
# ============================================================================


class TestBacktestCommandDisplay:
    """Test command output display."""

    @patch("qs_trader.cli.commands.backtest.ExperimentMetadata")
    @patch("qs_trader.cli.commands.backtest.ExperimentResolver")
    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_memory_backend_display(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        mock_resolver_class,
        mock_metadata_class,
        cli_runner,
        valid_config_file,
        mock_engine,
        mock_system_config,
        mock_experiment_setup,
    ):
        """Test display shows memory backend correctly."""
        mock_config = Mock()
        mock_config.backtest_id = "test"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = -1.0
        mock_config.display_events = None

        mock_experiment_setup(mock_resolver_class, mock_metadata_class)
        mock_load_config.return_value = mock_config
        mock_engine_class.from_config.return_value = mock_engine
        mock_get_sys_config.return_value = mock_system_config

        result = cli_runner.invoke(backtest_command, [str(valid_config_file)])

        assert result.exit_code == 0
        assert "memory (no files created)" in result.output

    @patch("qs_trader.cli.commands.backtest.ExperimentMetadata")
    @patch("qs_trader.cli.commands.backtest.ExperimentResolver")
    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    @patch("qs_trader.cli.commands.backtest.os.path.getsize")
    def test_file_backend_display(
        self,
        mock_getsize,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        mock_resolver_class,
        mock_metadata_class,
        cli_runner,
        valid_config_file,
        mock_engine,
        tmp_path,
        mock_experiment_setup,
    ):
        """Test display shows file backend correctly."""
        mock_config = Mock()
        mock_config.backtest_id = "test"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = -1.0
        mock_config.display_events = None

        # Setup file backend
        results_dir = tmp_path / "results"
        results_dir.mkdir()
        event_file = results_dir / "events.sqlite"
        event_file.write_text("fake event data")

        from datetime import timedelta

        mock_engine.run.return_value = Mock(
            start_date=datetime(2020, 1, 1).date(),
            end_date=datetime(2020, 12, 31).date(),
            bars_processed=252,
            duration=timedelta(seconds=5),
        )
        mock_engine._results_dir = results_dir

        system_config = Mock()
        system_config.output.event_store.backend = "sqlite"
        system_config.output.event_store.filename = "events.{backend}"
        system_config.output.run_id_format = "%Y%m%d_%H%M%S"
        system_config.output.capture_git_info = False
        system_config.output.capture_environment = False
        system_config.output.database.enabled = False

        mock_experiment_setup(mock_resolver_class, mock_metadata_class)
        mock_load_config.return_value = mock_config
        mock_engine_class.from_config.return_value = mock_engine
        mock_get_sys_config.return_value = system_config
        mock_getsize.return_value = 1024 * 1024  # 1 MB

        result = cli_runner.invoke(backtest_command, [str(valid_config_file)])

        assert result.exit_code == 0
        assert "events.sqlite" in result.output
        assert "1.00 MB" in result.output


# ============================================================================
# Error Handling Tests
# ============================================================================


class TestBacktestCommandErrors:
    """Test error handling."""

    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_config_loading_error(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        cli_runner,
        valid_config_file,
    ):
        """Test graceful handling of config loading errors."""
        mock_load_config.side_effect = ValueError("Invalid config format")

        result = cli_runner.invoke(backtest_command, [str(valid_config_file)])

        assert result.exit_code == 1
        assert "✗ Backtest failed:" in result.output
        assert "Invalid config format" in result.output

    @patch("qs_trader.cli.commands.backtest.ExperimentMetadata")
    @patch("qs_trader.cli.commands.backtest.ExperimentResolver")
    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_engine_initialization_error(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        mock_resolver_class,
        mock_metadata_class,
        cli_runner,
        valid_config_file,
        mock_system_config,
        mock_experiment_setup,
    ):
        """Test graceful handling of engine initialization errors."""
        mock_config = Mock()
        mock_config.backtest_id = "test"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = 0.0
        mock_config.display_events = ["bar"]

        mock_experiment_setup(mock_resolver_class, mock_metadata_class)
        mock_load_config.return_value = mock_config
        mock_get_sys_config.return_value = mock_system_config
        mock_engine_class.from_config.side_effect = RuntimeError("Failed to initialize data service")

        result = cli_runner.invoke(backtest_command, [str(valid_config_file)])

        assert result.exit_code == 1
        assert "✗ Backtest failed:" in result.output
        assert "Failed to initialize data service" in result.output

    @patch("qs_trader.cli.commands.backtest.BacktestEngine")
    @patch("qs_trader.cli.commands.backtest.load_backtest_config")
    @patch("qs_trader.cli.commands.backtest.reload_system_config")
    @patch("qs_trader.cli.commands.backtest.get_system_config")
    def test_backtest_execution_error(
        self,
        mock_get_sys_config,
        mock_reload_sys_config,
        mock_load_config,
        mock_engine_class,
        cli_runner,
        valid_config_file,
        mock_engine,
        mock_system_config,
    ):
        """Test graceful handling of backtest execution errors."""
        mock_config = Mock()
        mock_config.backtest_id = "test"
        mock_config.start_date = datetime(2020, 1, 1)
        mock_config.end_date = datetime(2020, 12, 31)
        mock_config.all_symbols = ["AAPL"]
        mock_config.replay_speed = 0.0
        mock_config.display_events = ["bar"]

        mock_load_config.return_value = mock_config
        mock_engine_class.from_config.return_value = mock_engine
        mock_get_sys_config.return_value = mock_system_config
        mock_engine.run.side_effect = RuntimeError("Data loading failed")

        result = cli_runner.invoke(backtest_command, [str(valid_config_file)])

        assert result.exit_code == 1
        assert "✗ Backtest failed:" in result.output
        assert "Data loading failed" in result.output
        mock_engine.shutdown.assert_not_called()  # Shouldn't shutdown if run() failed
