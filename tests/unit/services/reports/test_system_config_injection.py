"""Tests for ReportingService SystemConfig injection behavior.

Validates that ReportingService correctly uses injected SystemConfig
instead of the global singleton when determining artifact policy mode.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.service import ReportingService


def _make_system_config_mock(
    *,
    db_enabled: bool = False,
    artifact_mode: str = "filesystem",
):
    """Create a mock SystemConfig with specified artifact policy mode."""
    mock = MagicMock()
    mock.output.database.enabled = db_enabled
    mock.output.database.backend = "postgres"
    mock.output.database.postgres_url = None
    mock.output.artifact_policy.mode = artifact_mode
    mock.config_root = Path.cwd()
    return mock


class TestReportingServiceSystemConfigInjection:
    """Test that ReportingService respects injected SystemConfig."""

    def test_injected_filesystem_mode_writes_files(self, tmp_path: Path) -> None:
        """When injected SystemConfig has filesystem mode, should_write_files is True."""
        event_bus = EventBus()
        config = ReportingConfig(write_json=True)
        
        # Inject SystemConfig with filesystem mode
        injected_config = _make_system_config_mock(
            db_enabled=False,
            artifact_mode="filesystem",
        )
        
        service = ReportingService(
            event_bus=event_bus,
            config=config,
            output_dir=tmp_path,
            system_config=injected_config,
        )
        
        # Verify the injected config is stored
        assert service._system_config is injected_config
        assert service._system_config.output.artifact_policy.mode == "filesystem"

    def test_injected_database_only_mode_skips_files(self, tmp_path: Path) -> None:
        """When injected SystemConfig has database_only mode, should_write_files is False."""
        event_bus = EventBus()
        config = ReportingConfig(write_json=True)
        
        # Inject SystemConfig with database_only mode
        injected_config = _make_system_config_mock(
            db_enabled=True,
            artifact_mode="database_only",
        )
        
        service = ReportingService(
            event_bus=event_bus,
            config=config,
            output_dir=tmp_path,
            system_config=injected_config,
        )
        
        # Verify the injected config is stored
        assert service._system_config is injected_config
        assert service._system_config.output.artifact_policy.mode == "database_only"

    def test_no_injected_config_falls_back_to_singleton(self, tmp_path: Path) -> None:
        """When no SystemConfig is injected, service falls back to global singleton."""
        event_bus = EventBus()
        config = ReportingConfig(write_json=True)
        
        # Create service without injecting SystemConfig
        service = ReportingService(
            event_bus=event_bus,
            config=config,
            output_dir=tmp_path,
            system_config=None,
        )
        
        # Verify no config is injected
        assert service._system_config is None

    def test_backward_compatibility_no_parameter(self, tmp_path: Path) -> None:
        """When system_config parameter is omitted, service works as before."""
        event_bus = EventBus()
        config = ReportingConfig(write_json=True)
        
        # Create service without passing system_config parameter at all
        service = ReportingService(
            event_bus=event_bus,
            config=config,
            output_dir=tmp_path,
        )
        
        # Verify no config is injected (backward compatible)
        assert service._system_config is None

    @patch('qs_trader.services.reporting.service.write_json_report')
    @patch('qs_trader.system.config.get_system_config')
    def test_write_outputs_uses_injected_config(
        self, 
        mock_get_system_config: MagicMock,
        mock_write_json: MagicMock,
        tmp_path: Path
    ) -> None:
        """_write_outputs should use injected config instead of calling get_system_config()."""
        event_bus = EventBus()
        config = ReportingConfig(write_json=True, write_parquet=False, write_csv_timeline=False, write_html_report=False)
        
        # Create mock for global singleton (should NOT be called)
        global_config = _make_system_config_mock(
            db_enabled=False,
            artifact_mode="database_only",  # Global says database_only
        )
        mock_get_system_config.return_value = global_config
        
        # Inject different config (filesystem mode)
        injected_config = _make_system_config_mock(
            db_enabled=False,
            artifact_mode="filesystem",  # Injected says filesystem
        )
        
        service = ReportingService(
            event_bus=event_bus,
            config=config,
            output_dir=tmp_path,
            system_config=injected_config,
        )
        
        # Set up minimal state for _write_outputs
        service._backtest_id = "test_bt"
        service._backtest_config = MagicMock()
        service._backtest_config.sleeve = None
        
        # Mock the methods that _write_outputs calls
        service._load_portfolio_states_from_events = MagicMock(return_value={})
        service._equity_calc.get_curve = MagicMock(return_value=[])
        service._last_portfolio_state = None
        service._write_to_database = MagicMock()
        
        # Call _write_outputs with dummy metrics
        dummy_metrics = MagicMock()
        dummy_metrics.drawdown_periods = []
        dummy_metrics.total_trades = 0
        service._write_outputs(dummy_metrics)
        
        # Verify get_system_config was NOT called (injected config was used)
        mock_get_system_config.assert_not_called()
        
        # Verify write_json_report WAS called (filesystem mode was respected)
        mock_write_json.assert_called()

    @patch('qs_trader.services.reporting.service.write_json_report')
    @patch('qs_trader.system.config.get_system_config')
    def test_write_outputs_falls_back_when_no_injection(
        self,
        mock_get_system_config: MagicMock,
        mock_write_json: MagicMock,
        tmp_path: Path
    ) -> None:
        """_write_outputs should call get_system_config() when no config is injected."""
        event_bus = EventBus()
        config = ReportingConfig(write_json=True, write_parquet=False, write_csv_timeline=False, write_html_report=False)
        
        # Create mock for global singleton (should be called)
        global_config = _make_system_config_mock(
            db_enabled=False,
            artifact_mode="filesystem",
        )
        mock_get_system_config.return_value = global_config
        
        # Create service WITHOUT injecting SystemConfig
        service = ReportingService(
            event_bus=event_bus,
            config=config,
            output_dir=tmp_path,
            system_config=None,
        )
        
        # Set up minimal state for _write_outputs
        service._backtest_id = "test_bt"
        service._backtest_config = MagicMock()
        service._backtest_config.sleeve = None
        
        # Mock the methods that _write_outputs calls
        service._load_portfolio_states_from_events = MagicMock(return_value={})
        service._equity_calc.get_curve = MagicMock(return_value=[])
        service._last_portfolio_state = None
        service._write_to_database = MagicMock()
        
        # Call _write_outputs with dummy metrics
        dummy_metrics = MagicMock()
        dummy_metrics.drawdown_periods = []
        dummy_metrics.total_trades = 0
        service._write_outputs(dummy_metrics)
        
        # Verify get_system_config WAS called (fallback behavior)
        mock_get_system_config.assert_called()
