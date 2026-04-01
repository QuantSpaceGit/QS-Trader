"""DuckDB persistence writer for backtest results.

Writes backtest run metadata, performance metrics, equity curves,
returns, trades, and drawdowns to a DuckDB database file.

This enables downstream API services (e.g. QS-Datamaster /backtest router)
to serve backtest data to dashboards without filesystem coupling.

Design:
- Additive: Does not replace file-based outputs (JSON/Parquet/HTML).
- Run-scoped: Each backtest run is identified by (experiment_id, run_id).
- Upsert: Re-running the same run_id replaces previous data.
- Read-only safe: DuckDB supports concurrent readers; the API opens read-only.
"""

from decimal import Decimal
from pathlib import Path
import json
from typing import Any

import duckdb
import structlog

from qs_trader.libraries.performance.models import (
    DrawdownPeriod,
    EquityCurvePoint,
    FullMetrics,
    ReturnPoint,
    TradeRecord,
)

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Schema DDL
# ---------------------------------------------------------------------------

_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS runs (
    experiment_id   VARCHAR NOT NULL,
    run_id          VARCHAR NOT NULL,
    backtest_id     VARCHAR NOT NULL,
    start_date      VARCHAR NOT NULL,
    end_date        VARCHAR NOT NULL,
    duration_days   INTEGER NOT NULL,
    initial_equity  DOUBLE  NOT NULL,
    final_equity    DOUBLE  NOT NULL,
    total_return_pct    DOUBLE NOT NULL,
    cagr                DOUBLE NOT NULL,
    best_day_return_pct DOUBLE,
    worst_day_return_pct DOUBLE,
    volatility_annual_pct DOUBLE NOT NULL,
    max_drawdown_pct      DOUBLE NOT NULL,
    max_drawdown_duration_days INTEGER NOT NULL,
    avg_drawdown_pct      DOUBLE NOT NULL,
    current_drawdown_pct  DOUBLE NOT NULL,
    sharpe_ratio    DOUBLE  NOT NULL,
    sortino_ratio   DOUBLE,
    calmar_ratio    DOUBLE  NOT NULL,
    risk_free_rate  DOUBLE  NOT NULL,
    total_trades    INTEGER NOT NULL,
    winning_trades  INTEGER NOT NULL,
    losing_trades   INTEGER NOT NULL,
    win_rate        DOUBLE  NOT NULL,
    profit_factor   DOUBLE,
    avg_win         DOUBLE  NOT NULL,
    avg_loss        DOUBLE  NOT NULL,
    avg_win_pct     DOUBLE  NOT NULL,
    avg_loss_pct    DOUBLE  NOT NULL,
    largest_win     DOUBLE  NOT NULL,
    largest_loss    DOUBLE  NOT NULL,
    largest_win_pct DOUBLE  NOT NULL,
    largest_loss_pct DOUBLE NOT NULL,
    expectancy      DOUBLE  NOT NULL,
    max_consecutive_wins   INTEGER NOT NULL,
    max_consecutive_losses INTEGER NOT NULL,
    avg_trade_duration_days DOUBLE,
    total_commissions      DOUBLE NOT NULL,
    commission_pct_of_pnl  DOUBLE NOT NULL,
    created_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (experiment_id, run_id)
);

CREATE TABLE IF NOT EXISTS equity_curve (
    experiment_id   VARCHAR   NOT NULL,
    run_id          VARCHAR   NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    equity          DOUBLE    NOT NULL,
    cash            DOUBLE    NOT NULL,
    positions_value DOUBLE    NOT NULL,
    num_positions   INTEGER   NOT NULL,
    gross_exposure  DOUBLE    NOT NULL,
    net_exposure    DOUBLE    NOT NULL,
    leverage        DOUBLE    NOT NULL,
    drawdown_pct    DOUBLE    NOT NULL,
    underwater      BOOLEAN   NOT NULL,
    PRIMARY KEY (experiment_id, run_id, timestamp)
);

CREATE TABLE IF NOT EXISTS returns (
    experiment_id     VARCHAR   NOT NULL,
    run_id            VARCHAR   NOT NULL,
    timestamp         TIMESTAMP NOT NULL,
    period_return     DOUBLE    NOT NULL,
    cumulative_return DOUBLE    NOT NULL,
    log_return        DOUBLE,
    PRIMARY KEY (experiment_id, run_id, timestamp)
);

CREATE TABLE IF NOT EXISTS trades (
    experiment_id    VARCHAR   NOT NULL,
    run_id           VARCHAR   NOT NULL,
    trade_id         VARCHAR   NOT NULL,
    strategy_id      VARCHAR   NOT NULL,
    symbol           VARCHAR   NOT NULL,
    entry_timestamp  TIMESTAMP NOT NULL,
    exit_timestamp   TIMESTAMP NOT NULL,
    entry_price      DOUBLE    NOT NULL,
    exit_price       DOUBLE    NOT NULL,
    quantity         INTEGER   NOT NULL,
    side             VARCHAR   NOT NULL,
    pnl              DOUBLE    NOT NULL,
    pnl_pct          DOUBLE    NOT NULL,
    commission       DOUBLE    NOT NULL,
    duration_seconds INTEGER   NOT NULL,
    PRIMARY KEY (experiment_id, run_id, trade_id)
);

CREATE TABLE IF NOT EXISTS drawdowns (
    experiment_id    VARCHAR   NOT NULL,
    run_id           VARCHAR   NOT NULL,
    drawdown_id      INTEGER   NOT NULL,
    start_timestamp  TIMESTAMP NOT NULL,
    trough_timestamp TIMESTAMP NOT NULL,
    end_timestamp    TIMESTAMP,
    peak_equity      DOUBLE    NOT NULL,
    trough_equity    DOUBLE    NOT NULL,
    depth_pct        DOUBLE    NOT NULL,
    duration_days    INTEGER   NOT NULL,
    recovery_days    INTEGER,
    recovered        BOOLEAN   NOT NULL,
    PRIMARY KEY (experiment_id, run_id, drawdown_id)
);

CREATE TABLE IF NOT EXISTS bars_with_features (
    experiment_id   VARCHAR   NOT NULL,
    run_id          VARCHAR   NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    symbol          VARCHAR   NOT NULL,
    open            DOUBLE    NOT NULL,
    high            DOUBLE    NOT NULL,
    low             DOUBLE    NOT NULL,
    close           DOUBLE    NOT NULL,
    open_adj        DOUBLE,
    high_adj        DOUBLE,
    low_adj         DOUBLE,
    close_adj       DOUBLE,
    volume          BIGINT    NOT NULL,
    features_json   VARCHAR,
    feature_set_version VARCHAR,
    PRIMARY KEY (experiment_id, run_id, timestamp, symbol)
);
"""


def _to_float(value: Decimal | float | int | None) -> float | None:
    """Convert Decimal to float for DuckDB insertion."""
    if value is None:
        return None
    return float(value)


class DuckDBWriter:
    """Writes backtest results to a DuckDB database file.

    Ensures schema exists on first use, then inserts run data.
    Uses upsert semantics: re-running the same (experiment_id, run_id)
    replaces previous data.

    Args:
        db_path: Path to the DuckDB file. Parent directories are created.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    def save_run(
        self,
        experiment_id: str,
        run_id: str,
        metrics: FullMetrics,
        equity_curve: list[EquityCurvePoint],
        returns: list[ReturnPoint],
        trades: list[TradeRecord],
        drawdowns: list[DrawdownPeriod],
    ) -> None:
        """Persist a complete backtest run to DuckDB.

        All writes happen in a single transaction. If the same
        (experiment_id, run_id) already exists, it is replaced.

        Args:
            experiment_id: Experiment name (e.g. "buy_hold").
            run_id: Timestamped run identifier (e.g. "20260331_120238").
            metrics: Full performance metrics from teardown.
            equity_curve: Equity curve time-series points.
            returns: Returns time-series points.
            trades: Completed trade records.
            drawdowns: Drawdown period records.
        """
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

        con = duckdb.connect(str(self._db_path))
        try:
            con.execute("BEGIN TRANSACTION")
            self._ensure_schema(con)
            self._delete_existing_run(con, experiment_id, run_id)
            self._insert_run(con, experiment_id, run_id, metrics)
            self._insert_equity_curve(con, experiment_id, run_id, equity_curve)
            self._insert_returns(con, experiment_id, run_id, returns)
            self._insert_trades(con, experiment_id, run_id, trades)
            self._insert_drawdowns(con, experiment_id, run_id, drawdowns)
            con.execute("COMMIT")

            logger.info(
                "duckdb_writer.run_saved",
                db_path=str(self._db_path),
                experiment_id=experiment_id,
                run_id=run_id,
                equity_points=len(equity_curve),
                return_points=len(returns),
                trades_count=len(trades),
                drawdown_periods=len(drawdowns),
            )
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _ensure_schema(self, con: duckdb.DuckDBPyConnection) -> None:
        """Create tables if they do not exist."""
        con.execute(_SCHEMA_DDL)

    def _delete_existing_run(self, con: duckdb.DuckDBPyConnection, experiment_id: str, run_id: str) -> None:
        """Remove previous data for this run (upsert semantics)."""
        for table in ("runs", "equity_curve", "returns", "trades", "drawdowns", "bars_with_features"):
            con.execute(
                f"DELETE FROM {table} WHERE experiment_id = $1 AND run_id = $2",  # noqa: S608
                [experiment_id, run_id],
            )

    def _insert_run(
        self, con: duckdb.DuckDBPyConnection, experiment_id: str, run_id: str, metrics: FullMetrics
    ) -> None:
        """Insert run-level summary row."""
        con.execute(
            """
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
                total_commissions, commission_pct_of_pnl
            ) VALUES (
                $1, $2, $3,
                $4, $5, $6,
                $7, $8,
                $9, $10,
                $11, $12,
                $13,
                $14, $15,
                $16, $17,
                $18, $19, $20, $21,
                $22, $23, $24,
                $25, $26,
                $27, $28, $29, $30,
                $31, $32, $33, $34,
                $35, $36, $37,
                $38,
                $39, $40
            )
            """,
            [
                experiment_id,
                run_id,
                metrics.backtest_id,
                metrics.start_date,
                metrics.end_date,
                metrics.duration_days,
                _to_float(metrics.initial_equity),
                _to_float(metrics.final_equity),
                _to_float(metrics.total_return_pct),
                _to_float(metrics.cagr),
                _to_float(metrics.best_day_return_pct),
                _to_float(metrics.worst_day_return_pct),
                _to_float(metrics.volatility_annual_pct),
                _to_float(metrics.max_drawdown_pct),
                metrics.max_drawdown_duration_days,
                _to_float(metrics.avg_drawdown_pct),
                _to_float(metrics.current_drawdown_pct),
                _to_float(metrics.sharpe_ratio),
                _to_float(metrics.sortino_ratio),
                _to_float(metrics.calmar_ratio),
                _to_float(metrics.risk_free_rate),
                metrics.total_trades,
                metrics.winning_trades,
                metrics.losing_trades,
                _to_float(metrics.win_rate),
                _to_float(metrics.profit_factor),
                _to_float(metrics.avg_win),
                _to_float(metrics.avg_loss),
                _to_float(metrics.avg_win_pct),
                _to_float(metrics.avg_loss_pct),
                _to_float(metrics.largest_win),
                _to_float(metrics.largest_loss),
                _to_float(metrics.largest_win_pct),
                _to_float(metrics.largest_loss_pct),
                _to_float(metrics.expectancy),
                metrics.max_consecutive_wins,
                metrics.max_consecutive_losses,
                _to_float(metrics.avg_trade_duration_days),
                _to_float(metrics.total_commissions),
                _to_float(metrics.commission_pct_of_pnl),
            ],
        )

    def _insert_equity_curve(
        self,
        con: duckdb.DuckDBPyConnection,
        experiment_id: str,
        run_id: str,
        points: list[EquityCurvePoint],
    ) -> None:
        """Batch-insert equity curve points.

        Deduplicates by timestamp (last-write-wins), since multiple portfolio
        events can land on the same timestamp within a single bar.
        """
        if not points:
            return
        # Deduplicate: keep the last point at each timestamp (preserves
        # final portfolio state when multiple events share a timestamp).
        deduped: dict = {}
        for p in points:
            deduped[p.timestamp] = p
        rows = [
            (
                experiment_id,
                run_id,
                p.timestamp,
                _to_float(p.equity),
                _to_float(p.cash),
                _to_float(p.positions_value),
                p.num_positions,
                _to_float(p.gross_exposure),
                _to_float(p.net_exposure),
                _to_float(p.leverage),
                _to_float(p.drawdown_pct),
                p.underwater,
            )
            for p in deduped.values()
        ]
        con.executemany(
            """
            INSERT INTO equity_curve (
                experiment_id, run_id, timestamp,
                equity, cash, positions_value, num_positions,
                gross_exposure, net_exposure, leverage,
                drawdown_pct, underwater
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            rows,
        )

    def _insert_returns(
        self,
        con: duckdb.DuckDBPyConnection,
        experiment_id: str,
        run_id: str,
        points: list[ReturnPoint],
    ) -> None:
        """Batch-insert return points.

        Deduplicates by timestamp (last-write-wins) for consistency with
        ``_insert_equity_curve``.
        """
        if not points:
            return
        deduped: dict = {}
        for p in points:
            deduped[p.timestamp] = p
        rows = [
            (
                experiment_id,
                run_id,
                p.timestamp,
                _to_float(p.period_return),
                _to_float(p.cumulative_return),
                _to_float(p.log_return),
            )
            for p in deduped.values()
        ]
        con.executemany(
            """
            INSERT INTO returns (
                experiment_id, run_id, timestamp,
                period_return, cumulative_return, log_return
            ) VALUES ($1, $2, $3, $4, $5, $6)
            """,
            rows,
        )

    def _insert_trades(
        self,
        con: duckdb.DuckDBPyConnection,
        experiment_id: str,
        run_id: str,
        trades: list[TradeRecord],
    ) -> None:
        """Batch-insert trade records."""
        if not trades:
            return
        rows = [
            (
                experiment_id,
                run_id,
                t.trade_id,
                t.strategy_id,
                t.symbol,
                t.entry_timestamp,
                t.exit_timestamp,
                _to_float(t.entry_price),
                _to_float(t.exit_price),
                t.quantity,
                t.side,
                _to_float(t.pnl),
                _to_float(t.pnl_pct),
                _to_float(t.commission),
                t.duration_seconds,
            )
            for t in trades
        ]
        con.executemany(
            """
            INSERT INTO trades (
                experiment_id, run_id, trade_id,
                strategy_id, symbol,
                entry_timestamp, exit_timestamp,
                entry_price, exit_price, quantity, side,
                pnl, pnl_pct, commission, duration_seconds
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
            """,
            rows,
        )

    def _insert_drawdowns(
        self,
        con: duckdb.DuckDBPyConnection,
        experiment_id: str,
        run_id: str,
        drawdowns: list[DrawdownPeriod],
    ) -> None:
        """Batch-insert drawdown period records."""
        if not drawdowns:
            return
        rows = [
            (
                experiment_id,
                run_id,
                d.drawdown_id,
                d.start_timestamp,
                d.trough_timestamp,
                d.end_timestamp,
                _to_float(d.peak_equity),
                _to_float(d.trough_equity),
                _to_float(d.depth_pct),
                d.duration_days,
                d.recovery_days,
                d.recovered,
            )
            for d in drawdowns
        ]
        con.executemany(
            """
            INSERT INTO drawdowns (
                experiment_id, run_id, drawdown_id,
                start_timestamp, trough_timestamp, end_timestamp,
                peak_equity, trough_equity, depth_pct,
                duration_days, recovery_days, recovered
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
            rows,
        )

    def save_bars_with_features(
        self,
        experiment_id: str,
        run_id: str,
        bars_with_features: list[dict[str, Any]],
    ) -> None:
        """Persist OHLCV bars with their precomputed feature snapshots to DuckDB.

        Writes to the ``bars_with_features`` table in the same database file.
        This is called from ReportingService.teardown() when a FeatureService
        is active (ClickHouse integration enabled).

        Args:
            experiment_id: Experiment name (e.g. "sma_crossover").
            run_id: Timestamped run identifier (e.g. "20260401_120000").
            bars_with_features: List of dicts, each containing:
                - timestamp (datetime | str): Bar timestamp (UTC).
                - symbol (str): Ticker symbol.
                - open, high, low, close (float): Split-adjusted OHLC.
                - open_adj, high_adj, low_adj, close_adj (float | None): Total-return adj.
                - volume (int): Adjusted daily volume.
                - features (dict | None): Feature column → value mapping.
                - feature_set_version (str): Feature version string.
        """
        if not bars_with_features:
            return

        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(self._db_path))
        try:
            con.execute("BEGIN TRANSACTION")
            self._ensure_schema(con)
            # Upsert: remove previous bars_with_features for this run
            con.execute(
                "DELETE FROM bars_with_features WHERE experiment_id = $1 AND run_id = $2",  # noqa: S608
                [experiment_id, run_id],
            )
            self._insert_bars_with_features(con, experiment_id, run_id, bars_with_features)
            con.execute("COMMIT")
            logger.info(
                "duckdb_writer.bars_with_features_saved",
                db_path=str(self._db_path),
                experiment_id=experiment_id,
                run_id=run_id,
                bar_count=len(bars_with_features),
            )
        except Exception:
            con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def _insert_bars_with_features(
        self,
        con: duckdb.DuckDBPyConnection,
        experiment_id: str,
        run_id: str,
        bars_with_features: list[dict[str, Any]],
    ) -> None:
        """Batch-insert bars_with_features rows."""
        rows = []
        for b in bars_with_features:
            ts = b["timestamp"]
            features_raw = b.get("features")
            features_json = json.dumps(features_raw, default=str) if features_raw else None
            rows.append((
                experiment_id,
                run_id,
                ts,
                b["symbol"],
                float(b.get("open", 0.0) or 0.0),
                float(b.get("high", 0.0) or 0.0),
                float(b.get("low", 0.0) or 0.0),
                float(b.get("close", 0.0) or 0.0),
                float(b["open_adj"]) if b.get("open_adj") is not None else None,
                float(b["high_adj"]) if b.get("high_adj") is not None else None,
                float(b["low_adj"]) if b.get("low_adj") is not None else None,
                float(b["close_adj"]) if b.get("close_adj") is not None else None,
                int(b.get("volume", 0) or 0),
                features_json,
                b.get("feature_set_version", "v1"),
            ))
        con.executemany(
            """
            INSERT INTO bars_with_features (
                experiment_id, run_id, timestamp, symbol,
                open, high, low, close,
                open_adj, high_adj, low_adj, close_adj,
                volume, features_json, feature_set_version
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8,
                $9, $10, $11, $12, $13, $14, $15
            )
            """,
            rows,
        )

