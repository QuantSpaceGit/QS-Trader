"""Reporting service exports.

The package uses lazy imports so callers can import reporting submodules
without eagerly loading optional runtime dependencies.
"""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from qs_trader.services.reporting.audit_export import AuditExportBuilder
    from qs_trader.services.reporting.config import ReportingConfig
    from qs_trader.services.reporting.manifest import ClickHouseInputManifest
    from qs_trader.services.reporting.postgres_writer import PostgreSQLWriter
    from qs_trader.services.reporting.service import ReportingService


_EXPORT_MAP: dict[str, tuple[str, str]] = {
    "AuditExportBuilder": (
        "qs_trader.services.reporting.audit_export",
        "AuditExportBuilder",
    ),
    "ClickHouseInputManifest": (
        "qs_trader.services.reporting.manifest",
        "ClickHouseInputManifest",
    ),
    "PostgreSQLWriter": (
        "qs_trader.services.reporting.postgres_writer",
        "PostgreSQLWriter",
    ),
    "ReportingService": ("qs_trader.services.reporting.service", "ReportingService"),
    "ReportingConfig": ("qs_trader.services.reporting.config", "ReportingConfig"),
    "display_performance_report": (
        "qs_trader.services.reporting.formatters",
        "display_performance_report",
    ),
    "write_json_report": (
        "qs_trader.services.reporting.writers",
        "write_json_report",
    ),
    "write_equity_curve_json": (
        "qs_trader.services.reporting.writers",
        "write_equity_curve_json",
    ),
    "write_returns_json": (
        "qs_trader.services.reporting.writers",
        "write_returns_json",
    ),
    "write_trades_json": (
        "qs_trader.services.reporting.writers",
        "write_trades_json",
    ),
    "write_drawdowns_json": (
        "qs_trader.services.reporting.writers",
        "write_drawdowns_json",
    ),
    "write_strategy_chart_data": (
        "qs_trader.services.reporting.writers",
        "write_strategy_chart_data",
    ),
}


def __getattr__(name: str) -> Any:
    """Resolve public exports lazily to avoid import-time side effects."""
    try:
        module_name, attr_name = _EXPORT_MAP[name]
    except KeyError as exc:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from exc

    module = import_module(module_name)
    return getattr(module, attr_name)


__all__ = [
    "AuditExportBuilder",
    "ClickHouseInputManifest",
    "PostgreSQLWriter",
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
