"""Unit tests for DuckDB reporting writer.

Tests cover:
- Schema creation and table structure
- Run persistence with all time-series data
- Upsert semantics (re-running same run_id replaces data)
- Empty data handling (zero trades, no drawdowns)
- Database file creation in non-existent directory
"""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pytest

from qs_trader.libraries.performance.models import (
    DrawdownPeriod,
    EquityCurvePoint,
    FullMetrics,
    ReturnPoint,
    TradeRecord,
)
from qs_trader.services.reporting.db_writer import DuckDBWriter


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def db_path(tmp_path: Path) -> Path:
    """Temporary DuckDB file path."""
    return tmp_path / "test_runs.duckdb"


@pytest.fixture
def writer(db_path: Path) -> DuckDBWriter:
    """DuckDBWriter instance."""
    return DuckDBWriter(db_path)


@pytest.fixture
def sample_metrics() -> FullMetrics:
    """Minimal FullMetrics for testing."""
    return FullMetrics.model_construct(
        backtest_id="test_buy_hold",
        start_date="2020-08-01",
        end_date="2020-09-01",
        duration_days=31,
        initial_equity=Decimal("100000"),
        final_equity=Decimal("105000"),
        total_return_pct=Decimal("5.00"),
        cagr=Decimal("80.45"),
        best_day_return_pct=Decimal("2.10"),
        worst_day_return_pct=Decimal("-1.30"),
        volatility_annual_pct=Decimal("18.50"),
        max_drawdown_pct=Decimal("3.20"),
        max_drawdown_duration_days=5,
        avg_drawdown_pct=Decimal("1.50"),
        current_drawdown_pct=Decimal("0.00"),
        sharpe_ratio=Decimal("1.85"),
        sortino_ratio=Decimal("2.10"),
        calmar_ratio=Decimal("25.14"),
        risk_free_rate=Decimal("0.00"),
        total_trades=4,
        winning_trades=3,
        losing_trades=1,
        win_rate=Decimal("75.00"),
        profit_factor=Decimal("3.50"),
        avg_win=Decimal("2000"),
        avg_loss=Decimal("-500"),
        avg_win_pct=Decimal("2.00"),
        avg_loss_pct=Decimal("-0.50"),
        largest_win=Decimal("3000"),
        largest_loss=Decimal("-500"),
        largest_win_pct=Decimal("3.00"),
        largest_loss_pct=Decimal("-0.50"),
        expectancy=Decimal("1125"),
        max_consecutive_wins=3,
        max_consecutive_losses=1,
        avg_trade_duration_days=Decimal("5.25"),
        total_commissions=Decimal("8.00"),
        commission_pct_of_pnl=Decimal("0.16"),
        monthly_returns=[],
        quarterly_returns=[],
        annual_returns=[],
        strategy_performance=[],
        drawdown_periods=[],
    )


@pytest.fixture
def sample_equity_curve() -> list[EquityCurvePoint]:
    """Sample equity curve with 3 points."""
    return [
        EquityCurvePoint.model_construct(
            timestamp=datetime(2020, 8, 3, 16, 0, 0, tzinfo=timezone.utc),
            equity=Decimal("100000"),
            cash=Decimal("50000"),
            positions_value=Decimal("50000"),
            num_positions=1,
            gross_exposure=Decimal("50000"),
            net_exposure=Decimal("50000"),
            leverage=Decimal("0.50"),
            drawdown_pct=Decimal("0.00"),
            underwater=False,
        ),
        EquityCurvePoint.model_construct(
            timestamp=datetime(2020, 8, 4, 16, 0, 0, tzinfo=timezone.utc),
            equity=Decimal("101000"),
            cash=Decimal("50000"),
            positions_value=Decimal("51000"),
            num_positions=1,
            gross_exposure=Decimal("51000"),
            net_exposure=Decimal("51000"),
            leverage=Decimal("0.505"),
            drawdown_pct=Decimal("0.00"),
            underwater=False,
        ),
        EquityCurvePoint.model_construct(
            timestamp=datetime(2020, 8, 5, 16, 0, 0, tzinfo=timezone.utc),
            equity=Decimal("102500"),
            cash=Decimal("50000"),
            positions_value=Decimal("52500"),
            num_positions=1,
            gross_exposure=Decimal("52500"),
            net_exposure=Decimal("52500"),
            leverage=Decimal("0.512"),
            drawdown_pct=Decimal("0.00"),
            underwater=False,
        ),
    ]


@pytest.fixture
def sample_returns() -> list[ReturnPoint]:
    """Sample returns with 2 points (compounded cumulative)."""
    return [
        ReturnPoint.model_construct(
            timestamp=datetime(2020, 8, 4, 16, 0, 0, tzinfo=timezone.utc),
            period_return=Decimal("0.01"),
            cumulative_return=Decimal("0.01"),
            log_return=Decimal("0.00995"),
        ),
        ReturnPoint.model_construct(
            timestamp=datetime(2020, 8, 5, 16, 0, 0, tzinfo=timezone.utc),
            period_return=Decimal("0.01485"),
            cumulative_return=Decimal("0.0249985"),  # (1.01)(1.01485) - 1
            log_return=Decimal("0.01474"),
        ),
    ]


@pytest.fixture
def sample_trades() -> list[TradeRecord]:
    """Sample trade records."""
    return [
        TradeRecord.model_construct(
            trade_id="t-001",
            strategy_id="buy_and_hold",
            symbol="AAPL",
            entry_timestamp=datetime(2020, 8, 3, 16, 0, 0, tzinfo=timezone.utc),
            exit_timestamp=datetime(2020, 8, 10, 16, 0, 0, tzinfo=timezone.utc),
            entry_price=Decimal("425.00"),
            exit_price=Decimal("450.00"),
            quantity=100,
            side="long",
            pnl=Decimal("2500"),
            pnl_pct=Decimal("5.88"),
            commission=Decimal("2.00"),
            duration_seconds=604800,
        ),
    ]


@pytest.fixture
def sample_drawdowns() -> list[DrawdownPeriod]:
    """Sample drawdown periods."""
    return [
        DrawdownPeriod.model_construct(
            drawdown_id=1,
            start_timestamp=datetime(2020, 8, 7, 16, 0, 0, tzinfo=timezone.utc),
            trough_timestamp=datetime(2020, 8, 10, 16, 0, 0, tzinfo=timezone.utc),
            end_timestamp=datetime(2020, 8, 12, 16, 0, 0, tzinfo=timezone.utc),
            peak_equity=Decimal("105000"),
            trough_equity=Decimal("101640"),
            depth_pct=Decimal("3.20"),
            duration_days=3,
            recovery_days=2,
            recovered=True,
        ),
    ]


# ============================================================================
# Tests
# ============================================================================


class TestDuckDBWriterSchemaCreation:
    """Tests for schema creation and table structure."""

    def test_save_run_creates_database_file(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """Database file should be created on first save."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="20260331_120000",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        assert db_path.exists()

    def test_save_run_creates_all_tables(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """All five tables should be created."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="20260331_120000",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        con = duckdb.connect(str(db_path), read_only=True)
        tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}
        con.close()

        assert tables == {"runs", "equity_curve", "returns", "trades", "drawdowns"}

    def test_creates_parent_directories(self, tmp_path: Path, sample_metrics: FullMetrics) -> None:
        """Parent directories should be created if they don't exist."""
        deep_path = tmp_path / "a" / "b" / "c" / "test.duckdb"
        writer = DuckDBWriter(deep_path)
        writer.save_run(
            experiment_id="test",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        assert deep_path.exists()


class TestDuckDBWriterRunPersistence:
    """Tests for writing and reading run data."""

    def test_run_summary_persisted(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """Run summary row should contain correct metric values."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="20260331_120000",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT * FROM runs WHERE experiment_id = 'buy_hold'").fetchone()
        con.close()

        assert row is not None
        # Check key fields by name lookup
        con = duckdb.connect(str(db_path), read_only=True)
        result = con.execute(
            "SELECT experiment_id, run_id, backtest_id, total_return_pct, sharpe_ratio, total_trades "
            "FROM runs WHERE experiment_id = 'buy_hold'"
        ).fetchone()
        con.close()

        assert result is not None
        assert result[0] == "buy_hold"
        assert result[1] == "20260331_120000"
        assert result[2] == "test_buy_hold"
        assert abs(result[3] - 5.00) < 0.01
        assert abs(result[4] - 1.85) < 0.01
        assert result[5] == 4

    def test_equity_curve_persisted(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_equity_curve: list[EquityCurvePoint],
    ) -> None:
        """Equity curve points should be written and readable."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=sample_equity_curve,
            returns=[],
            trades=[],
            drawdowns=[],
        )
        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute(
            "SELECT timestamp, equity FROM equity_curve "
            "WHERE experiment_id = 'buy_hold' ORDER BY timestamp"
        ).fetchall()
        con.close()

        assert len(rows) == 3
        assert abs(rows[0][1] - 100000.0) < 0.01
        assert abs(rows[2][1] - 102500.0) < 0.01

    def test_returns_persisted(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_returns: list[ReturnPoint],
    ) -> None:
        """Return points should be written and readable."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=sample_returns,
            trades=[],
            drawdowns=[],
        )
        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute(
            "SELECT period_return, cumulative_return FROM returns "
            "WHERE experiment_id = 'buy_hold' ORDER BY timestamp"
        ).fetchall()
        con.close()

        assert len(rows) == 2
        assert abs(rows[0][0] - 0.01) < 0.0001

    def test_trades_persisted(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_trades: list[TradeRecord],
    ) -> None:
        """Trade records should be written and readable."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=sample_trades,
            drawdowns=[],
        )
        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute(
            "SELECT trade_id, symbol, side, pnl FROM trades "
            "WHERE experiment_id = 'buy_hold'"
        ).fetchall()
        con.close()

        assert len(rows) == 1
        assert rows[0][0] == "t-001"
        assert rows[0][1] == "AAPL"
        assert rows[0][2] == "long"
        assert abs(rows[0][3] - 2500.0) < 0.01

    def test_drawdowns_persisted(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_drawdowns: list[DrawdownPeriod],
    ) -> None:
        """Drawdown period records should be written and readable."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=sample_drawdowns,
        )
        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute(
            "SELECT drawdown_id, depth_pct, recovered FROM drawdowns "
            "WHERE experiment_id = 'buy_hold'"
        ).fetchall()
        con.close()

        assert len(rows) == 1
        assert rows[0][0] == 1
        assert abs(rows[0][1] - 3.20) < 0.01
        assert rows[0][2] is True


class TestDuckDBWriterUpsert:
    """Tests for upsert semantics."""

    def test_rerun_replaces_data(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """Saving the same (experiment_id, run_id) twice should replace data, not duplicate."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        # Save again with updated metrics
        updated = sample_metrics.model_copy(update={"total_return_pct": Decimal("10.00")})
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_001",
            metrics=updated,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )

        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute("SELECT total_return_pct FROM runs WHERE run_id = 'run_001'").fetchall()
        con.close()

        assert len(rows) == 1
        assert abs(rows[0][0] - 10.00) < 0.01

    def test_different_runs_coexist(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """Different run_ids for the same experiment should both be stored."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_002",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )

        con = duckdb.connect(str(db_path), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM runs WHERE experiment_id = 'buy_hold'").fetchone()
        con.close()

        assert count is not None
        assert count[0] == 2

    def test_different_experiments_coexist(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """Different experiment_ids should both be stored."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        writer.save_run(
            experiment_id="sma_crossover",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )

        con = duckdb.connect(str(db_path), read_only=True)
        count = con.execute("SELECT COUNT(*) FROM runs").fetchone()
        con.close()

        assert count is not None
        assert count[0] == 2


class TestDuckDBWriterEmptyData:
    """Tests for edge cases with empty data."""

    def test_empty_time_series(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """Should handle empty equity curve, returns, trades, and drawdowns."""
        writer.save_run(
            experiment_id="empty_test",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )

        con = duckdb.connect(str(db_path), read_only=True)
        run_count = con.execute("SELECT COUNT(*) FROM runs").fetchone()
        eq_count = con.execute("SELECT COUNT(*) FROM equity_curve").fetchone()
        ret_count = con.execute("SELECT COUNT(*) FROM returns").fetchone()
        trade_count = con.execute("SELECT COUNT(*) FROM trades").fetchone()
        dd_count = con.execute("SELECT COUNT(*) FROM drawdowns").fetchone()
        con.close()

        assert run_count is not None and run_count[0] == 1
        assert eq_count is not None and eq_count[0] == 0
        assert ret_count is not None and ret_count[0] == 0
        assert trade_count is not None and trade_count[0] == 0
        assert dd_count is not None and dd_count[0] == 0

    def test_nullable_metric_fields(self, writer: DuckDBWriter, db_path: Path) -> None:
        """Metrics with None values for optional fields should be persisted."""
        metrics = FullMetrics.model_construct(
            backtest_id="nullable_test",
            start_date="2020-01-01",
            end_date="2020-01-31",
            duration_days=30,
            initial_equity=Decimal("100000"),
            final_equity=Decimal("100000"),
            total_return_pct=Decimal("0.00"),
            cagr=Decimal("0.00"),
            best_day_return_pct=None,
            worst_day_return_pct=None,
            volatility_annual_pct=Decimal("0.00"),
            max_drawdown_pct=Decimal("0.00"),
            max_drawdown_duration_days=0,
            avg_drawdown_pct=Decimal("0.00"),
            current_drawdown_pct=Decimal("0.00"),
            sharpe_ratio=Decimal("0.00"),
            sortino_ratio=None,
            calmar_ratio=Decimal("0.00"),
            risk_free_rate=Decimal("0.00"),
            total_trades=0,
            winning_trades=0,
            losing_trades=0,
            win_rate=Decimal("0.00"),
            profit_factor=None,
            avg_win=Decimal("0.00"),
            avg_loss=Decimal("0.00"),
            avg_win_pct=Decimal("0.00"),
            avg_loss_pct=Decimal("0.00"),
            largest_win=Decimal("0.00"),
            largest_loss=Decimal("0.00"),
            largest_win_pct=Decimal("0.00"),
            largest_loss_pct=Decimal("0.00"),
            expectancy=Decimal("0.00"),
            max_consecutive_wins=0,
            max_consecutive_losses=0,
            avg_trade_duration_days=None,
            total_commissions=Decimal("0.00"),
            commission_pct_of_pnl=Decimal("0.00"),
            monthly_returns=[],
            quarterly_returns=[],
            annual_returns=[],
            strategy_performance=[],
            drawdown_periods=[],
        )
        writer.save_run(
            experiment_id="nullable",
            run_id="run_001",
            metrics=metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute(
            "SELECT best_day_return_pct, sortino_ratio, profit_factor, avg_trade_duration_days "
            "FROM runs WHERE experiment_id = 'nullable'"
        ).fetchone()
        con.close()

        assert row is not None
        assert row[0] is None
        assert row[1] is None
        assert row[2] is None
        assert row[3] is None


class TestDuckDBWriterDeduplication:
    """Tests that duplicate timestamps within a run are handled gracefully."""

    def test_duplicate_equity_timestamps_last_wins(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """If the same timestamp appears twice, only the last point is persisted."""
        ts = datetime(2020, 8, 3, 16, 0, 0, tzinfo=timezone.utc)
        points = [
            EquityCurvePoint.model_construct(
                timestamp=ts,
                equity=Decimal("100000"),
                cash=Decimal("100000"),
                positions_value=Decimal("0"),
                num_positions=0,
                gross_exposure=Decimal("0"),
                net_exposure=Decimal("0"),
                leverage=Decimal("0"),
                drawdown_pct=Decimal("0"),
                underwater=False,
            ),
            # Same timestamp, different equity (second event on same bar)
            EquityCurvePoint.model_construct(
                timestamp=ts,
                equity=Decimal("99000"),
                cash=Decimal("50000"),
                positions_value=Decimal("49000"),
                num_positions=1,
                gross_exposure=Decimal("49000"),
                net_exposure=Decimal("49000"),
                leverage=Decimal("0.49"),
                drawdown_pct=Decimal("1.00"),
                underwater=True,
            ),
        ]
        # Must not raise a PK constraint error
        writer.save_run(
            experiment_id="dup_test",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=points,
            returns=[],
            trades=[],
            drawdowns=[],
        )

        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute(
            "SELECT equity, num_positions FROM equity_curve WHERE experiment_id = 'dup_test'"
        ).fetchall()
        con.close()

        # Only one row per timestamp, last write wins (equity=99000)
        assert len(rows) == 1
        assert rows[0][0] == pytest.approx(99000.0)
        assert rows[0][1] == 1

    def test_duplicate_returns_timestamps_last_wins(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """If the same timestamp appears twice in returns, only the last point is persisted."""
        ts = datetime(2020, 8, 4, 16, 0, 0, tzinfo=timezone.utc)
        points = [
            ReturnPoint.model_construct(
                timestamp=ts,
                period_return=Decimal("0.01"),
                cumulative_return=Decimal("0.01"),
                log_return=Decimal("0.00995"),
            ),
            ReturnPoint.model_construct(
                timestamp=ts,
                period_return=Decimal("0.02"),
                cumulative_return=Decimal("0.02"),
                log_return=Decimal("0.0198"),
            ),
        ]
        writer.save_run(
            experiment_id="dup_ret",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=points,
            trades=[],
            drawdowns=[],
        )

        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute(
            "SELECT period_return FROM returns WHERE experiment_id = 'dup_ret'"
        ).fetchall()
        con.close()

        assert len(rows) == 1
        assert rows[0][0] == pytest.approx(0.02)


class TestDuckDBWriterFullRoundTrip:
    """End-to-end test with all data types populated."""

    def test_full_run_with_all_data(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_equity_curve: list[EquityCurvePoint],
        sample_returns: list[ReturnPoint],
        sample_trades: list[TradeRecord],
        sample_drawdowns: list[DrawdownPeriod],
    ) -> None:
        """Full save with all data types should persist everything correctly."""
        writer.save_run(
            experiment_id="buy_hold",
            run_id="20260331_120238",
            metrics=sample_metrics,
            equity_curve=sample_equity_curve,
            returns=sample_returns,
            trades=sample_trades,
            drawdowns=sample_drawdowns,
        )

        con = duckdb.connect(str(db_path), read_only=True)

        run_count = con.execute("SELECT COUNT(*) FROM runs").fetchone()
        eq_count = con.execute("SELECT COUNT(*) FROM equity_curve").fetchone()
        ret_count = con.execute("SELECT COUNT(*) FROM returns").fetchone()
        trade_count = con.execute("SELECT COUNT(*) FROM trades").fetchone()
        dd_count = con.execute("SELECT COUNT(*) FROM drawdowns").fetchone()

        con.close()

        assert run_count is not None and run_count[0] == 1
        assert eq_count is not None and eq_count[0] == 3
        assert ret_count is not None and ret_count[0] == 2
        assert trade_count is not None and trade_count[0] == 1
        assert dd_count is not None and dd_count[0] == 1
