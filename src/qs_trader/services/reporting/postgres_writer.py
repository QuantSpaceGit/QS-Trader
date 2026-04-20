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
import math
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypeVar

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine

from qs_trader.events.events import (
    FeatureBarEvent,
    FillEvent,
    IndicatorEvent,
    OrderEvent,
    PriceBarEvent,
    SignalEvent,
    TradeEvent,
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


def _normalize_event_timestamp(timestamp: str) -> str:
    """Normalize event timestamps to a consistent ISO format for grouping."""
    return timestamp.replace("Z", "+00:00")


def _parse_event_timestamp(timestamp: str) -> datetime:
    """Parse an event timestamp into a timezone-aware datetime."""
    return datetime.fromisoformat(_normalize_event_timestamp(timestamp))


def _coerce_numeric_json_value(value: Any) -> float | None:
    """Convert supported numeric-like values into finite floats."""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return float(value)
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    return None


def _filter_numeric_payload(payload: dict[str, Any]) -> dict[str, float] | None:
    """Keep only finite numeric values from a flexible event payload."""
    filtered: dict[str, float] = {}
    for key, value in payload.items():
        numeric_value = _coerce_numeric_json_value(value)
        if numeric_value is not None:
            filtered[str(key)] = numeric_value
    return filtered or None


def _join_unique_values(values: list[str]) -> str | None:
    """Join ordered unique strings for deterministic text persistence."""
    ordered: list[str] = []
    seen: set[str] = set()

    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)

    return ",".join(ordered) if ordered else None


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
        for table in ("drawdowns", "trades", "returns", "equity_curve", "runs"):
            conn.execute(
                text(f"DELETE FROM {table} WHERE experiment_id = :exp AND run_id = :rid"),
                {"exp": experiment_id, "rid": run_id},
            )

    @staticmethod
    def _collect_run_events(
        experiment_id: str,
        run_id: str,
        event_store: EventStore | None,
    ) -> list[dict[str, Any]]:
        """Collect per-bar decision-chain rows from the in-memory event store.

        Signal aggregation is intentionally last-wins at the ``(timestamp,
        symbol)`` grain: if multiple ``SignalEvent`` objects share the same bar
        and symbol, the final event encountered in occurred-at order overwrites
        earlier ones. Preserving multiple same-bar signals per strategy would
        require widening the persistence grain (for example by including
        ``strategy_id`` in the primary key), which is outside Phase 1.
        """
        if event_store is None:
            return []

        bar_events: list[PriceBarEvent] = [
            event for event in event_store.get_by_type("bar") if isinstance(event, PriceBarEvent)
        ]
        feature_events: list[FeatureBarEvent] = [
            event for event in event_store.get_by_type("feature_bar") if isinstance(event, FeatureBarEvent)
        ]
        indicator_events: list[IndicatorEvent] = [
            event for event in event_store.get_by_type("indicator") if isinstance(event, IndicatorEvent)
        ]
        signal_events: list[SignalEvent] = [
            event for event in event_store.get_by_type("signal") if isinstance(event, SignalEvent)
        ]
        order_events: list[OrderEvent] = [
            event for event in event_store.get_by_type("order") if isinstance(event, OrderEvent)
        ]
        fill_events: list[FillEvent] = [
            event for event in event_store.get_by_type("fill") if isinstance(event, FillEvent)
        ]
        trade_events: list[TradeEvent] = [
            event for event in event_store.get_by_type("trade") if isinstance(event, TradeEvent)
        ]

        if not any(
            (bar_events, feature_events, indicator_events, signal_events, order_events, fill_events, trade_events)
        ):
            return []

        def event_key(timestamp: str, symbol: str) -> tuple[str, str]:
            return (_normalize_event_timestamp(timestamp), symbol)

        all_keys: set[tuple[str, str]] = set()
        signals_by_key: dict[tuple[str, str], SignalEvent] = {}
        orders_by_key: dict[tuple[str, str], list[OrderEvent]] = defaultdict(list)
        fills_by_key: dict[tuple[str, str], list[FillEvent]] = defaultdict(list)
        trade_by_key: dict[tuple[str, str], TradeEvent] = {}
        indicators_by_key: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
        features_by_key: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
        strategy_ids_by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
        strategy_ids_by_symbol: dict[str, list[str]] = defaultdict(list)
        run_strategy_ids: list[str] = []
        fill_to_trade_id: dict[str, str] = {}

        def record_strategy_id(strategy_id: str | None, key: tuple[str, str] | None = None) -> None:
            if not strategy_id:
                return
            if strategy_id not in run_strategy_ids:
                run_strategy_ids.append(strategy_id)
            if key is not None:
                if strategy_id not in strategy_ids_by_key[key]:
                    strategy_ids_by_key[key].append(strategy_id)
                if strategy_id not in strategy_ids_by_symbol[key[1]]:
                    strategy_ids_by_symbol[key[1]].append(strategy_id)

        for event in bar_events:
            all_keys.add(event_key(event.timestamp, event.symbol))

        for feature_event in feature_events:
            key = event_key(feature_event.timestamp, feature_event.symbol)
            all_keys.add(key)
            numeric_features = _filter_numeric_payload(feature_event.features)
            if numeric_features is not None:
                features_by_key[key].update(numeric_features)

        for indicator_event in indicator_events:
            key = event_key(indicator_event.timestamp, indicator_event.symbol)
            all_keys.add(key)
            record_strategy_id(indicator_event.strategy_id, key)
            numeric_indicators = _filter_numeric_payload(indicator_event.indicators)
            if numeric_indicators is not None:
                indicators_by_key[key].update(numeric_indicators)

        for signal_event in signal_events:
            key = event_key(signal_event.timestamp, signal_event.symbol)
            all_keys.add(key)
            record_strategy_id(signal_event.strategy_id, key)
            # Deliberate last-wins behavior documented in the method docstring.
            signals_by_key[key] = signal_event

        for order_event in order_events:
            key = event_key(order_event.timestamp, order_event.symbol)
            all_keys.add(key)
            record_strategy_id(order_event.source_strategy_id, key)
            orders_by_key[key].append(order_event)

        for fill_event in fill_events:
            key = event_key(fill_event.timestamp, fill_event.symbol)
            all_keys.add(key)
            record_strategy_id(fill_event.strategy_id, key)
            fills_by_key[key].append(fill_event)

        for trade_event_item in trade_events:
            key = event_key(trade_event_item.timestamp, trade_event_item.symbol)
            all_keys.add(key)
            record_strategy_id(trade_event_item.strategy_id, key)
            trade_by_key[key] = trade_event_item
            for fill_id in trade_event_item.fills:
                fill_to_trade_id[fill_id] = trade_event_item.trade_id

        rows: list[dict[str, Any]] = []
        for timestamp_key, symbol in sorted(all_keys):
            key = (timestamp_key, symbol)
            signal = signals_by_key.get(key)
            orders = orders_by_key.get(key, [])
            fills = fills_by_key.get(key, [])
            trade_event: TradeEvent | None = trade_by_key.get(key)

            if trade_event is None:
                for fill in fills:
                    trade_id = fill_to_trade_id.get(fill.fill_id)
                    if trade_id is None:
                        continue
                    for candidate_event in trade_events:
                        if candidate_event.trade_id == trade_id:
                            trade_event = candidate_event
                            break
                    if trade_event is not None:
                        break

            strategy_id = (
                _join_unique_values(strategy_ids_by_key.get(key, []))
                or _join_unique_values(strategy_ids_by_symbol.get(symbol, []))
                or _join_unique_values(run_strategy_ids)
                or "unknown"
            )

            order_qty: Decimal | None = None
            if orders:
                order_qty = sum((order.quantity for order in orders), start=Decimal("0"))

            fill_qty: Decimal | None = None
            fill_price: float | None = None
            fill_slippage_bps: float | None = None
            commission: float | None = None
            if fills:
                fill_qty = sum((fill.filled_quantity for fill in fills), start=Decimal("0"))
                if fill_qty > 0:
                    fill_value = sum(
                        (fill.filled_quantity * fill.fill_price for fill in fills),
                        start=Decimal("0"),
                    )
                    fill_price = float(fill_value / fill_qty)

                total_commission = sum((fill.commission for fill in fills), start=Decimal("0"))
                commission = float(total_commission)

                weighted_slippage_total = Decimal("0")
                weighted_slippage_qty = Decimal("0")
                for fill in fills:
                    if fill.slippage_bps is None:
                        continue
                    weighted_slippage_total += Decimal(fill.slippage_bps) * fill.filled_quantity
                    weighted_slippage_qty += fill.filled_quantity

                if weighted_slippage_qty > 0:
                    fill_slippage_bps = float(weighted_slippage_total / weighted_slippage_qty)

            rows.append(
                {
                    "experiment_id": experiment_id,
                    "run_id": run_id,
                    "timestamp": _parse_event_timestamp(timestamp_key),
                    "symbol": symbol,
                    "strategy_id": strategy_id,
                    "signal_intention": signal.intention if signal is not None else None,
                    "signal_price": _to_float(signal.price) if signal is not None else None,
                    "signal_confidence": _to_float(signal.confidence) if signal is not None else None,
                    "signal_reason": signal.reason if signal is not None else None,
                    "order_side": _join_unique_values([order.side.upper() for order in orders]),
                    "order_type": _join_unique_values([order.order_type.upper() for order in orders]),
                    "order_qty": int(order_qty) if order_qty is not None else None,
                    "fill_qty": int(fill_qty) if fill_qty is not None else None,
                    "fill_price": fill_price,
                    "fill_slippage_bps": fill_slippage_bps,
                    "commission": commission,
                    "trade_id": trade_event.trade_id if trade_event is not None else None,
                    "trade_status": trade_event.status.upper() if trade_event is not None else None,
                    "trade_side": trade_event.side.upper() if trade_event and trade_event.side else None,
                    "trade_entry_price": _to_float(trade_event.entry_price) if trade_event is not None else None,
                    "trade_exit_price": _to_float(trade_event.exit_price) if trade_event is not None else None,
                    "trade_realized_pnl": _to_float(trade_event.realized_pnl) if trade_event is not None else None,
                    "indicators_json": _to_json(indicators_by_key.get(key) or None),
                    "features_json": _to_json(features_by_key.get(key) or None),
                }
            )

        return rows

    def _insert_run_events(
        self,
        conn: Connection,
        experiment_id: str,
        run_id: str,
        event_store: EventStore | None,
    ) -> int:
        """Insert per-bar event rows when an in-memory event stream is available."""
        rows = self._collect_run_events(experiment_id, run_id, event_store)
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
