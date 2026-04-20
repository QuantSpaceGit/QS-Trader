"""Persistence abstraction for backtest run storage.

Defines the protocol implemented by supported operational persistence writers.
Post-cutover QS-Trader uses PostgreSQL when database persistence is enabled.

The abstraction supports explicit artifact modes:
- filesystem: Create run directories with all artifacts (manifests, configs,
  event stores, time-series JSON, HTML reports)
- database_only: Skip run-directory creation; persist only database-captured
  metadata (for QS-Research service-owned jobs)
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from qs_trader.events.event_store import EventStore
    from qs_trader.libraries.performance.models import (
        DrawdownPeriod,
        EquityCurvePoint,
        FullMetrics,
        ReturnPoint,
        TradeRecord,
    )
    from qs_trader.services.reporting.manifest import ClickHouseInputManifest


class RunPersistenceWriter(Protocol):
    """Protocol for supported backtest run persistence implementations.

    Implementations must persist run summary, time-series data, and metadata
    for downstream API queries and job recovery. Supports upsert semantics:
    re-running the same (experiment_id, run_id) should replace previous data.
    """

    def save_run(
        self,
        experiment_id: str,
        run_id: str,
        metrics: FullMetrics,
        equity_curve: list[EquityCurvePoint],
        returns: list[ReturnPoint],
        trades: list[TradeRecord],
        drawdowns: list[DrawdownPeriod],
        manifest: ClickHouseInputManifest | None = None,
        run_manifest: dict | None = None,
        config_snapshot: dict | None = None,
        effective_execution_spec: dict | None = None,
        event_store: EventStore | None = None,
        artifact_mode: str | None = None,
        job_group_id: str | None = None,
        submission_source: str | None = None,
        split_pct: float | None = None,
        split_role: str | None = None,
    ) -> None:
        """Persist a complete backtest run.

        Args:
            experiment_id: Experiment name (e.g. "buy_hold")
            run_id: Timestamped run identifier
            metrics: Full performance metrics from teardown
            equity_curve: Equity curve time-series points
            returns: Returns time-series points
            trades: Completed trade records
            drawdowns: Drawdown period records
            manifest: Optional ClickHouse input manifest
            run_manifest: Optional run-level manifest (sources, config refs)
            config_snapshot: Optional normalized config snapshot (replaces
                on-disk config_snapshot.yaml for database-only runs)
            effective_execution_spec: Optional immutable runtime provenance
                artifact capturing resolved strategy/risk execution truth
            event_store: Optional in-memory event stream for per-bar audit
                persistence when available
            artifact_mode: Artifact policy ('filesystem' or 'database_only')
            job_group_id: Optional job group identifier for parameter sweeps
            submission_source: Optional source system label
            split_pct: Optional IS fraction for IS/OOS splits
            split_role: Optional role label for IS/OOS splits
        """
        ...

    def close(self) -> None:
        """Release any underlying persistence resources."""
        ...

    def update_audit_export_path(
        self,
        experiment_id: str,
        run_id: str,
        audit_export_path: str,
    ) -> None:
        """Persist the generated audit-export path for an existing run row.

        Args:
            experiment_id: Experiment name.
            run_id: Run identifier.
            audit_export_path: Absolute ZIP path stored in the operational run row.
        """
        ...
