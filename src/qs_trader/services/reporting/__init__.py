"""Reporting service for performance metrics and analysis."""

from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.db_writer import DuckDBWriter
from qs_trader.services.reporting.formatters import display_performance_report
from qs_trader.services.reporting.manifest import ClickHouseInputManifest
from qs_trader.services.reporting.service import ReportingService
from qs_trader.services.reporting.writers import (
    write_drawdowns_json,
    write_equity_curve_json,
    write_json_report,
    write_returns_json,
    write_strategy_chart_data,
    write_trades_json,
)

__all__ = [
    "ClickHouseInputManifest",
    "DuckDBWriter",
    "ReportingService",
    "ReportingConfig",
    "display_performance_report",
    "write_json_report",
    "write_equity_curve_json",
    "write_returns_json",
    "write_trades_json",
    "write_drawdowns_json",
    "write_strategy_chart_data",
]
