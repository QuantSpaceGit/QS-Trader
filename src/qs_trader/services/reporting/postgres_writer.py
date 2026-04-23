"""PostgreSQL persistence writer for backtest results.

Writes backtest run metadata, performance metrics, equity curves, returns,
trades, and drawdowns to PostgreSQL operational store. This implementation
supports QS-Research database-only execution mode by persisting run manifest
and config snapshot in JSONB columns, eliminating dependency on run-directory
files like config_snapshot.yaml and manifest.json.

Design:
- Transactional: All writes for one run happen in a single transaction
- Upsert: Re-running the same run_id replaces previous data
- Connection: Uses SQLAlchemy connection from environment config
- Schema: Expects schema created by Alembic migration (QS-Research owns DDL)
"""

from __future__ import annotations

import json
import logging
from decimal import Decimal
from typing import TYPE_CHECKING, TypeVar

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from qs_trader.services.reporting.bar_snapshot_collector import collect_run_bar_snapshots
from qs_trader.services.reporting.event_collector import collect_run_events
from qs_trader.services.reporting.lifecycle_event_collector import collect_run_lifecycle_events
from qs_trader.services.reporting.observability_collector import (
    collect_run_observability_bars,
)

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

logger = logging.getLogger(__name__)

RowValueT = TypeVar("RowValueT")


def _to_float(value: Decimal | float | int | None) -> float | None:
    """Convert Decimal to float for PostgreSQL insertion."""
    if value is None:
        return None
    return float(value)


def _to_json(data: dict | str | None) -> str | None:
    """Convert dict to JSON string, pass through existing JSON, or return None.

    Handles datetime/date/Decimal serialization for compatibility with JSONB storage.
    """
    if data is None:
        return None
    if isinstance(data, str):
        return data

    def json_serial(obj):
        """JSON serializer for objects not serializable by default JSON code."""
        from datetime import date, datetime
        from decimal import Decimal

        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        raise TypeError(f"Type {type(obj)} not serializable")

    return json.dumps(data, default=json_serial)


def _deduplicate_timestamp_rows(
    rows: list[dict[str, RowValueT]],
) -> list[dict[str, RowValueT]]:
    """Keep only the last row for each timestamp while preserving series order."""
    deduplicated_rows: dict[object, dict[str, RowValueT]] = {}
    for row in rows:
        deduplicated_rows[row["timestamp"]] = row
    return list(deduplicated_rows.values())


class PostgreSQLWriter:
    """Writes backtest results to PostgreSQL operational store.

    Expects PostgreSQL connection URL from environment variables:
    - RESEARCH_POSTGRES_HOST
    - RESEARCH_POSTGRES_PORT
    - RESEARCH_POSTGRES_DB
    - RESEARCH_POSTGRES_USER
    - RESEARCH_POSTGRES_PASSWORD

    Uses upsert semantics: re-running the same (experiment_id, run_id)
    replaces previous data within a transaction.

    Args:
        connection_url: PostgreSQL connection URL (e.g.
            ``postgresql+psycopg://user:pass@host:port/dbname``)
    """

    def __init__(self, connection_url: str) -> None:
        self._engine: Engine = create_engine(
            connection_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )

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
        """Persist a complete backtest run to PostgreSQL.

        All writes happen in a single transaction. If the same
        (experiment_id, run_id) already exists, it is replaced.

        Args:
            experiment_id: Experiment name (e.g. "buy_hold")
            run_id: Timestamped run identifier
            metrics: Full performance metrics from teardown
            equity_curve: Equity curve time-series points
            returns: Returns time-series points
            trades: Completed trade records
            drawdowns: Drawdown period records
            manifest: Optional ClickHouse input manifest (serialized to JSON)
            run_manifest: Optional run manifest dict (sources, config refs)
            config_snapshot: Optional config snapshot dict (replaces on-disk YAML)
            effective_execution_spec: Optional immutable runtime provenance
                artifact (resolved strategy and risk config)
            event_store: Optional in-memory event stream for per-bar
                audit persistence
            artifact_mode: Artifact policy ('filesystem' or 'database_only')
            job_group_id: Optional job group identifier
            submission_source: Optional source system label
            split_pct: Optional IS fraction for IS/OOS splits
            split_role: Optional role label for IS/OOS splits
        """
        run_event_rows = 0
        bar_snapshot_rows = 0
        lifecycle_event_rows = 0
        observability_event_rows = 0
        with self._engine.begin() as conn:
            self._delete_existing_run(conn, experiment_id, run_id)
            self._insert_run(
                conn,
                experiment_id,
                run_id,
                metrics,
                manifest,
                run_manifest,
                config_snapshot,
                effective_execution_spec,
                artifact_mode=artifact_mode,
                job_group_id=job_group_id,
                submission_source=submission_source,
                split_pct=split_pct,
                split_role=split_role,
            )
            self._insert_equity_curve(conn, experiment_id, run_id, equity_curve)
            self._insert_returns(conn, experiment_id, run_id, returns)
            self._insert_trades(conn, experiment_id, run_id, trades)
            self._insert_drawdowns(conn, experiment_id, run_id, drawdowns)
            bar_snapshot_rows = self._insert_bar_snapshots(conn, experiment_id, run_id, event_store)
            lifecycle_event_rows = self._insert_lifecycle_events(conn, experiment_id, run_id, event_store)
            observability_event_rows = self._insert_observability_bars(conn, experiment_id, run_id, event_store)
            run_event_rows = self._insert_run_events(conn, experiment_id, run_id, event_store)

        logger.info(
            "postgresql_writer.run_saved",
            extra={
                "experiment_id": experiment_id,
                "run_id": run_id,
                "equity_points": len(equity_curve),
                "return_points": len(returns),
                "trades_count": len(trades),
                "drawdown_periods": len(drawdowns),
                "bar_snapshot_rows": bar_snapshot_rows,
                "lifecycle_event_rows": lifecycle_event_rows,
                "observability_event_rows": observability_event_rows,
                "run_event_rows": run_event_rows,
            },
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _delete_existing_run(self, conn: Connection, experiment_id: str, run_id: str) -> None:
        """Remove previous data for this run (upsert semantics).

        ``run_events`` rows are removed automatically via ``ON DELETE CASCADE``
        when the parent ``runs`` row is deleted.
        """
        for table in (
            "run_bar_snapshots",
            "run_observability_bars",
            "run_lifecycle_events",
            "drawdowns",
            "trades",
            "returns",
            "equity_curve",
            "runs",
        ):
            conn.execute(
                text(f"DELETE FROM {table} WHERE experiment_id = :exp AND run_id = :rid"),
                {"exp": experiment_id, "rid": run_id},
            )

    def _insert_bar_snapshots(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        event_store: EventStore | None,
    ) -> int:
        """Insert runtime bar snapshot rows when bar events are available."""
        rows = collect_run_bar_snapshots(experiment_id, run_id, event_store)
        if not rows:
            return 0

        conn.execute(
            text(
                """
                INSERT INTO run_bar_snapshots (
                    experiment_id, run_id, symbol, bar_timestamp,
                    timestamp_local, timezone, source_name,
                    price_currency, price_scale,
                    open_raw, high_raw, low_raw, close_raw,
                    open_adj, high_adj, low_adj, close_adj,
                    volume_raw, volume_adj
                ) VALUES (
                    :experiment_id, :run_id, :symbol, :bar_timestamp,
                    :timestamp_local, :timezone, :source_name,
                    :price_currency, :price_scale,
                    :open_raw, :high_raw, :low_raw, :close_raw,
                    :open_adj, :high_adj, :low_adj, :close_adj,
                    :volume_raw, :volume_adj
                )
                """
            ),
            rows,
        )
        return len(rows)

    def _insert_lifecycle_events(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        event_store: EventStore | None,
    ) -> int:
        """Insert canonical lifecycle-ledger rows when lifecycle events are available."""
        rows = collect_run_lifecycle_events(experiment_id, run_id, event_store)
        if not rows:
            return 0

        conn.execute(
            text(
                """
                INSERT INTO run_lifecycle_events (
                    event_id, schema_version, experiment_id, run_id,
                    strategy_id, symbol, lifecycle_family, lifecycle_type,
                    event_timestamp, correlation_id, causation_id,
                    price_basis, payload_json
                ) VALUES (
                    :event_id, :schema_version, :experiment_id, :run_id,
                    :strategy_id, :symbol, :lifecycle_family, :lifecycle_type,
                    :event_timestamp, :correlation_id, :causation_id,
                    :price_basis, CAST(:payload_json AS jsonb)
                )
                """
            ),
            rows,
        )
        return len(rows)

    def _insert_observability_bars(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        event_store: EventStore | None,
    ) -> int:
        """Insert per-bar observability rows when indicator events exist.

        Sources rows from :func:`collect_run_observability_bars`. Safe to call
        with ``event_store=None`` or with a store that contains no
        :class:`IndicatorEvent` entries (returns 0 without executing SQL).
        """
        rows = collect_run_observability_bars(experiment_id, run_id, event_store)
        if not rows:
            return 0

        conn.execute(
            text(
                """
                INSERT INTO run_observability_bars (
                    experiment_id, run_id, strategy_id, symbol, bar_timestamp,
                    schema_version, indicators_json, runtime_features_json
                ) VALUES (
                    :experiment_id, :run_id, :strategy_id, :symbol, :bar_timestamp,
                    :schema_version, CAST(:indicators_json AS jsonb),
                    CAST(:runtime_features_json AS jsonb)
                )
                """
            ),
            rows,
        )
        return len(rows)

    def _insert_run_events(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        event_store: EventStore | None,
    ) -> int:
        """Insert per-bar event rows when an in-memory event stream is available."""
        rows = collect_run_events(experiment_id, run_id, event_store)
        if not rows:
            return 0

        conn.execute(
            text(
                """
                INSERT INTO run_events (
                    experiment_id, run_id, timestamp, symbol, strategy_id,
                    signal_intention, signal_price, signal_confidence, signal_reason,
                    order_side, order_type, order_qty,
                    fill_qty, fill_price, fill_slippage_bps, commission,
                    trade_id, trade_status, trade_side,
                    trade_entry_price, trade_exit_price, trade_realized_pnl,
                    indicators_json, features_json
                ) VALUES (
                    :experiment_id, :run_id, :timestamp, :symbol, :strategy_id,
                    :signal_intention, :signal_price, :signal_confidence, :signal_reason,
                    :order_side, :order_type, :order_qty,
                    :fill_qty, :fill_price, :fill_slippage_bps, :commission,
                    :trade_id, :trade_status, :trade_side,
                    :trade_entry_price, :trade_exit_price, :trade_realized_pnl,
                    CAST(:indicators_json AS jsonb), CAST(:features_json AS jsonb)
                )
                """
            ),
            rows,
        )
        return len(rows)

    def _insert_run(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        metrics: FullMetrics,
        manifest: ClickHouseInputManifest | None = None,
        run_manifest: dict | None = None,
        config_snapshot: dict | None = None,
        effective_execution_spec: dict | None = None,
        *,
        artifact_mode: str | None = None,
        job_group_id: str | None = None,
        submission_source: str | None = None,
        split_pct: float | None = None,
        split_role: str | None = None,
    ) -> None:
        """Insert run-level summary row."""
        manifest_json = manifest.to_json() if manifest is not None else None
        run_manifest_json = _to_json(run_manifest) if run_manifest is not None else "{}"
        config_snapshot_json = _to_json(config_snapshot) if config_snapshot is not None else "{}"
        effective_execution_spec_json = (
            _to_json(effective_execution_spec) if effective_execution_spec is not None else None
        )

        conn.execute(
            text("""
                INSERT INTO runs (
                    experiment_id, run_id, backtest_id,
                    start_date, end_date, duration_days,
                    initial_equity, final_equity,
                    total_return_pct, cagr,
                    best_day_return_pct, worst_day_return_pct,
                    volatility_annual_pct,
                    max_drawdown_pct, max_drawdown_duration_days,
                    avg_drawdown_pct, current_drawdown_pct,
                    sharpe_ratio, sortino_ratio, calmar_ratio, risk_free_rate,
                    total_trades, winning_trades, losing_trades,
                    win_rate, profit_factor,
                    avg_win, avg_loss, avg_win_pct, avg_loss_pct,
                    largest_win, largest_loss, largest_win_pct, largest_loss_pct,
                    expectancy, max_consecutive_wins, max_consecutive_losses,
                    avg_trade_duration_days,
                    total_commissions, commission_pct_of_pnl,
                    open_trades, realized_pnl, unrealized_pnl,
                    input_manifest_json, run_manifest_json, config_snapshot_json,
                    effective_execution_spec_json,
                    artifact_mode,
                    job_group_id, submission_source,
                    split_pct, split_role
                ) VALUES (
                    :experiment_id, :run_id, :backtest_id,
                    :start_date, :end_date, :duration_days,
                    :initial_equity, :final_equity,
                    :total_return_pct, :cagr,
                    :best_day_return_pct, :worst_day_return_pct,
                    :volatility_annual_pct,
                    :max_drawdown_pct, :max_drawdown_duration_days,
                    :avg_drawdown_pct, :current_drawdown_pct,
                    :sharpe_ratio, :sortino_ratio, :calmar_ratio, :risk_free_rate,
                    :total_trades, :winning_trades, :losing_trades,
                    :win_rate, :profit_factor,
                    :avg_win, :avg_loss, :avg_win_pct, :avg_loss_pct,
                    :largest_win, :largest_loss, :largest_win_pct, :largest_loss_pct,
                    :expectancy, :max_consecutive_wins, :max_consecutive_losses,
                    :avg_trade_duration_days,
                    :total_commissions, :commission_pct_of_pnl,
                    :open_trades, :realized_pnl, :unrealized_pnl,
                    CAST(:input_manifest_json AS jsonb), CAST(:run_manifest_json AS jsonb), CAST(:config_snapshot_json AS jsonb),
                    CAST(:effective_execution_spec_json AS jsonb),
                    :artifact_mode,
                    :job_group_id, :submission_source,
                    :split_pct, :split_role
                )
            """),
            {
                "experiment_id": experiment_id,
                "run_id": run_id,
                "backtest_id": metrics.backtest_id,
                "start_date": metrics.start_date,
                "end_date": metrics.end_date,
                "duration_days": metrics.duration_days,
                "initial_equity": _to_float(metrics.initial_equity),
                "final_equity": _to_float(metrics.final_equity),
                "total_return_pct": _to_float(metrics.total_return_pct),
                "cagr": _to_float(metrics.cagr),
                "best_day_return_pct": _to_float(metrics.best_day_return_pct),
                "worst_day_return_pct": _to_float(metrics.worst_day_return_pct),
                "volatility_annual_pct": _to_float(metrics.volatility_annual_pct),
                "max_drawdown_pct": _to_float(metrics.max_drawdown_pct),
                "max_drawdown_duration_days": metrics.max_drawdown_duration_days,
                "avg_drawdown_pct": _to_float(metrics.avg_drawdown_pct),
                "current_drawdown_pct": _to_float(metrics.current_drawdown_pct),
                "sharpe_ratio": _to_float(metrics.sharpe_ratio),
                "sortino_ratio": _to_float(metrics.sortino_ratio),
                "calmar_ratio": _to_float(metrics.calmar_ratio),
                "risk_free_rate": _to_float(metrics.risk_free_rate),
                "total_trades": metrics.total_trades,
                "winning_trades": metrics.winning_trades,
                "losing_trades": metrics.losing_trades,
                "win_rate": _to_float(metrics.win_rate),
                "profit_factor": _to_float(metrics.profit_factor),
                "avg_win": _to_float(metrics.avg_win),
                "avg_loss": _to_float(metrics.avg_loss),
                "avg_win_pct": _to_float(metrics.avg_win_pct),
                "avg_loss_pct": _to_float(metrics.avg_loss_pct),
                "largest_win": _to_float(metrics.largest_win),
                "largest_loss": _to_float(metrics.largest_loss),
                "largest_win_pct": _to_float(metrics.largest_win_pct),
                "largest_loss_pct": _to_float(metrics.largest_loss_pct),
                "expectancy": _to_float(metrics.expectancy),
                "max_consecutive_wins": metrics.max_consecutive_wins,
                "max_consecutive_losses": metrics.max_consecutive_losses,
                "avg_trade_duration_days": _to_float(metrics.avg_trade_duration_days),
                "total_commissions": _to_float(metrics.total_commissions),
                "commission_pct_of_pnl": _to_float(metrics.commission_pct_of_pnl),
                "open_trades": metrics.open_trades,
                "realized_pnl": _to_float(metrics.realized_pnl),
                "unrealized_pnl": _to_float(metrics.unrealized_pnl),
                "input_manifest_json": manifest_json,
                "run_manifest_json": run_manifest_json,
                "config_snapshot_json": config_snapshot_json,
                "effective_execution_spec_json": effective_execution_spec_json,
                "artifact_mode": artifact_mode or "filesystem",
                "job_group_id": job_group_id,
                "submission_source": submission_source,
                "split_pct": split_pct,
                "split_role": split_role,
            },
        )

    def _insert_equity_curve(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        points: list[EquityCurvePoint],
    ) -> None:
        """Insert equity curve time-series."""
        if not points:
            return

        rows = [
            {
                "experiment_id": experiment_id,
                "run_id": run_id,
                "timestamp": point.timestamp,
                "equity": _to_float(point.equity),
                "cash": _to_float(point.cash),
                "positions_value": _to_float(point.positions_value),
                "num_positions": point.num_positions,
                "gross_exposure": _to_float(point.gross_exposure),
                "net_exposure": _to_float(point.net_exposure),
                "leverage": _to_float(point.leverage),
                "drawdown_pct": _to_float(point.drawdown_pct),
                "underwater": point.underwater,
            }
            for point in points
        ]
        rows = _deduplicate_timestamp_rows(rows)

        conn.execute(
            text("""
                INSERT INTO equity_curve (
                    experiment_id, run_id, timestamp,
                    equity, cash, positions_value, num_positions,
                    gross_exposure, net_exposure, leverage,
                    drawdown_pct, underwater
                ) VALUES (
                    :experiment_id, :run_id, :timestamp,
                    :equity, :cash, :positions_value, :num_positions,
                    :gross_exposure, :net_exposure, :leverage,
                    :drawdown_pct, :underwater
                )
            """),
            rows,
        )

    def _insert_returns(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        points: list[ReturnPoint],
    ) -> None:
        """Insert returns time-series."""
        if not points:
            return

        rows = [
            {
                "experiment_id": experiment_id,
                "run_id": run_id,
                "timestamp": point.timestamp,
                "period_return": _to_float(point.period_return),
                "cumulative_return": _to_float(point.cumulative_return),
                "log_return": _to_float(point.log_return),
            }
            for point in points
        ]
        rows = _deduplicate_timestamp_rows(rows)

        conn.execute(
            text("""
                INSERT INTO returns (
                    experiment_id, run_id, timestamp,
                    period_return, cumulative_return, log_return
                ) VALUES (
                    :experiment_id, :run_id, :timestamp,
                    :period_return, :cumulative_return, :log_return
                )
            """),
            rows,
        )

    def _insert_trades(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        trades: list[TradeRecord],
    ) -> None:
        """Insert trade records."""
        if not trades:
            return

        rows = [
            {
                "experiment_id": experiment_id,
                "run_id": run_id,
                "trade_id": trade.trade_id,
                "strategy_id": trade.strategy_id,
                "symbol": trade.symbol,
                "entry_timestamp": trade.entry_timestamp,
                "exit_timestamp": trade.exit_timestamp,
                "entry_price": _to_float(trade.entry_price),
                "exit_price": _to_float(trade.exit_price),
                "quantity": trade.quantity,
                "side": trade.side,
                "pnl": _to_float(trade.pnl),
                "pnl_pct": _to_float(trade.pnl_pct),
                "commission": _to_float(trade.commission),
                "duration_seconds": trade.duration_seconds,
                "status": trade.status,
            }
            for trade in trades
        ]

        conn.execute(
            text("""
                INSERT INTO trades (
                    experiment_id, run_id, trade_id, strategy_id, symbol,
                    entry_timestamp, exit_timestamp,
                    entry_price, exit_price, quantity, side,
                    pnl, pnl_pct, commission, duration_seconds, status
                ) VALUES (
                    :experiment_id, :run_id, :trade_id, :strategy_id, :symbol,
                    :entry_timestamp, :exit_timestamp,
                    :entry_price, :exit_price, :quantity, :side,
                    :pnl, :pnl_pct, :commission, :duration_seconds, :status
                )
            """),
            rows,
        )

    def _insert_drawdowns(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        drawdowns: list[DrawdownPeriod],
    ) -> None:
        """Insert drawdown periods."""
        if not drawdowns:
            return

        rows = [
            {
                "experiment_id": experiment_id,
                "run_id": run_id,
                "drawdown_id": dd.drawdown_id,
                "start_timestamp": dd.start_timestamp,
                "trough_timestamp": dd.trough_timestamp,
                "end_timestamp": dd.end_timestamp,
                "peak_equity": _to_float(dd.peak_equity),
                "trough_equity": _to_float(dd.trough_equity),
                "depth_pct": _to_float(dd.depth_pct),
                "duration_days": dd.duration_days,
                "recovery_days": dd.recovery_days,
                "recovered": dd.recovered,
            }
            for dd in drawdowns
        ]

        conn.execute(
            text("""
                INSERT INTO drawdowns (
                    experiment_id, run_id, drawdown_id,
                    start_timestamp, trough_timestamp, end_timestamp,
                    peak_equity, trough_equity, depth_pct,
                    duration_days, recovery_days, recovered
                ) VALUES (
                    :experiment_id, :run_id, :drawdown_id,
                    :start_timestamp, :trough_timestamp, :end_timestamp,
                    :peak_equity, :trough_equity, :depth_pct,
                    :duration_days, :recovery_days, :recovered
                )
            """),
            rows,
        )

    def close(self) -> None:
        """Dispose the underlying SQLAlchemy engine."""
        self._engine.dispose()

    def update_audit_export_path(
        self,
        experiment_id: str,
        run_id: str,
        audit_export_path: str,
    ) -> None:
        """Update the persisted audit-export path for an existing run."""
        with self._engine.begin() as conn:
            result = conn.execute(
                text(
                    """
                    UPDATE runs
                    SET audit_export_path = :audit_export_path
                    WHERE experiment_id = :experiment_id
                      AND run_id = :run_id
                    """
                ),
                {
                    "audit_export_path": audit_export_path,
                    "experiment_id": experiment_id,
                    "run_id": run_id,
                },
            )
        if result.rowcount == 0:
            logger.warning(
                "postgresql_writer.audit_export_path_missing_run",
                extra={
                    "experiment_id": experiment_id,
                    "run_id": run_id,
                    "audit_export_path": audit_export_path,
                },
            )
