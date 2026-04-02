"""Unit tests for DuckDB reporting writer.

Tests cover:
- Schema creation and table structure
- Run persistence with all time-series data
- Upsert semantics (re-running same run_id replaces data)
- Empty data handling (zero trades, no drawdowns)
- Database file creation in non-existent directory
- Manifest schema column presence
- Manifest NULL default
- Manifest JSON round-trip through DuckDB
- Upsert with manifest update
- Manifest field validation (negative paths)
"""

from datetime import date, datetime, timezone
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
from qs_trader.services.reporting.manifest import ClickHouseInputManifest

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

    def test_run_summary_persisted(self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics) -> None:
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
            "SELECT timestamp, equity FROM equity_curve WHERE experiment_id = 'buy_hold' ORDER BY timestamp"
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
            "SELECT period_return, cumulative_return FROM returns WHERE experiment_id = 'buy_hold' ORDER BY timestamp"
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
        rows = con.execute("SELECT trade_id, symbol, side, pnl FROM trades WHERE experiment_id = 'buy_hold'").fetchall()
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
            "SELECT drawdown_id, depth_pct, recovered FROM drawdowns WHERE experiment_id = 'buy_hold'"
        ).fetchall()
        con.close()

        assert len(rows) == 1
        assert rows[0][0] == 1
        assert abs(rows[0][1] - 3.20) < 0.01
        assert rows[0][2] is True


class TestDuckDBWriterUpsert:
    """Tests for upsert semantics."""

    def test_rerun_replaces_data(self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics) -> None:
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

    def test_different_runs_coexist(self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics) -> None:
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

    def test_empty_time_series(self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics) -> None:
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
        rows = con.execute("SELECT equity, num_positions FROM equity_curve WHERE experiment_id = 'dup_test'").fetchall()
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
        rows = con.execute("SELECT period_return FROM returns WHERE experiment_id = 'dup_ret'").fetchall()
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


# ============================================================================
# Manifest fixtures
# ============================================================================


@pytest.fixture
def sample_manifest() -> ClickHouseInputManifest:
    """A fully populated ClickHouseInputManifest for testing."""
    return ClickHouseInputManifest(
        source_name="qs-datamaster",
        database="market_data",
        bars_table="equity_daily",
        features_table="equity_features_v1",
        regime_table="equity_regime_v1",
        symbols=["AAPL", "MSFT", "GOOGL"],
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        strategy_adjustment_mode="split_adjusted",
        portfolio_adjustment_mode="total_return",
        feature_set_version="v1",
        regime_version="v1",
        feature_columns=["sma_50", "rsi_14", "atr_14"],
    )


# ============================================================================
# Manifest tests
# ============================================================================


class TestDuckDBWriterManifestSchema:
    """Verify the manifest column exists in the ``runs`` schema."""

    def test_runs_table_has_manifest_column(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """The ``input_manifest_json`` VARCHAR column must exist in ``runs``."""
        writer.save_run(
            experiment_id="schema_check",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        con = duckdb.connect(str(db_path), read_only=True)
        columns = {row[0] for row in con.execute("DESCRIBE runs").fetchall()}
        con.close()

        assert "input_manifest_json" in columns

    def test_manifest_column_is_nullable(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """``input_manifest_json`` must allow NULL values (nullable column)."""
        writer.save_run(
            experiment_id="nullable_manifest",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'nullable_manifest'").fetchone()
        con.close()

        assert row is not None
        assert row[0] is None  # NULL — manifest not provided

    def test_ensure_schema_migration_adds_column_to_existing_table(
        self, tmp_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """An existing database without the manifest column must be migrated transparently.

        Simulates a pre-Phase-1 database by creating the ``runs`` table with the
        full original column set (minus ``input_manifest_json``), then verifying
        that a current-version DuckDBWriter adds the column via the ALTER TABLE
        migration without destroying any existing row data.
        """
        db_path = tmp_path / "legacy.duckdb"

        # Full pre-Phase-1 runs DDL — identical to the original schema except
        # ``input_manifest_json`` is intentionally absent.
        legacy_runs_ddl = """
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
        """
        con = duckdb.connect(str(db_path))
        con.execute(legacy_runs_ddl)
        con.execute(
            """
            INSERT INTO runs (
                experiment_id, run_id, backtest_id,
                start_date, end_date, duration_days,
                initial_equity, final_equity,
                total_return_pct, cagr,
                volatility_annual_pct, max_drawdown_pct, max_drawdown_duration_days,
                avg_drawdown_pct, current_drawdown_pct,
                sharpe_ratio, calmar_ratio, risk_free_rate,
                total_trades, winning_trades, losing_trades,
                win_rate, avg_win, avg_loss, avg_win_pct, avg_loss_pct,
                largest_win, largest_loss, largest_win_pct, largest_loss_pct,
                expectancy, max_consecutive_wins, max_consecutive_losses,
                total_commissions, commission_pct_of_pnl
            ) VALUES (
                'legacy_exp', 'r1', 'bt_legacy',
                '2020-01-01', '2020-12-31', 365,
                100000.0, 105000.0,
                5.0, 4.9,
                18.5, 3.2, 5,
                1.5, 0.0,
                1.85, 25.0, 0.0,
                4, 3, 1,
                75.0, 2000.0, -500.0, 2.0, -0.5,
                3000.0, -500.0, 3.0, -0.5,
                1125.0, 3, 1,
                8.0, 0.16
            )
            """
        )
        con.close()

        # A current DuckDBWriter must add the manifest column without destroying data
        writer = DuckDBWriter(db_path)
        writer.save_run(
            experiment_id="new_exp",
            run_id="run_002",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )

        con = duckdb.connect(str(db_path), read_only=True)
        columns = {row[0] for row in con.execute("DESCRIBE runs").fetchall()}
        legacy_row = con.execute("SELECT experiment_id FROM runs WHERE run_id = 'r1'").fetchone()
        con.close()

        assert "input_manifest_json" in columns
        # Legacy row must still exist — no data was destroyed
        assert legacy_row is not None
        assert legacy_row[0] == "legacy_exp"


class TestDuckDBWriterManifestPersistence:
    """Verify manifest JSON is written and read back correctly."""

    def test_save_run_writes_null_when_no_manifest_supplied(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """Calling save_run() without a manifest must write NULL."""
        writer.save_run(
            experiment_id="no_manifest",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            # manifest omitted — relies on default
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'no_manifest'").fetchone()
        con.close()

        assert row is not None
        assert row[0] is None

    def test_save_run_with_manifest_none_explicit(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """Passing manifest=None explicitly must also write NULL."""
        writer.save_run(
            experiment_id="explicit_none",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=None,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'explicit_none'").fetchone()
        con.close()

        assert row is not None
        assert row[0] is None

    def test_save_run_with_manifest_writes_json_string(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_manifest: ClickHouseInputManifest,
    ) -> None:
        """Supplying a manifest must persist a non-NULL JSON string."""
        writer.save_run(
            experiment_id="with_manifest",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=sample_manifest,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'with_manifest'").fetchone()
        con.close()

        assert row is not None
        assert row[0] is not None
        assert isinstance(row[0], str)
        assert len(row[0]) > 0

    def test_manifest_round_trips_through_duckdb(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_manifest: ClickHouseInputManifest,
    ) -> None:
        """A manifest stored in DuckDB must deserialise back to an equal value."""
        writer.save_run(
            experiment_id="roundtrip",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=sample_manifest,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'roundtrip'").fetchone()
        con.close()

        assert row is not None
        raw_json: str = row[0]
        recovered = ClickHouseInputManifest.from_json(raw_json)

        assert recovered == sample_manifest
        assert recovered.source_kind == "clickhouse"
        assert recovered.source_name == "qs-datamaster"
        assert recovered.database == "market_data"
        assert recovered.bars_table == "equity_daily"
        assert recovered.features_table == "equity_features_v1"
        assert recovered.regime_table == "equity_regime_v1"
        assert recovered.symbols == ("AAPL", "MSFT", "GOOGL")
        assert recovered.start_date == date(2023, 1, 1)
        assert recovered.end_date == date(2023, 12, 31)
        assert recovered.adjustment_mode is None
        assert recovered.strategy_adjustment_mode == "split_adjusted"
        assert recovered.portfolio_adjustment_mode == "total_return"
        assert recovered.feature_set_version == "v1"
        assert recovered.regime_version == "v1"
        assert recovered.feature_columns == ("sma_50", "rsi_14", "atr_14")

    def test_manifest_with_minimal_fields_round_trips(
        self, writer: DuckDBWriter, db_path: Path, sample_metrics: FullMetrics
    ) -> None:
        """A manifest with only required fields must also round-trip correctly."""
        manifest = ClickHouseInputManifest(
            source_name="qs-datamaster",
            database="market_data",
            bars_table="equity_daily",
            symbols=["SPY"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 6, 30),
        )

        writer.save_run(
            experiment_id="minimal_manifest",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=manifest,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'minimal_manifest'").fetchone()
        con.close()

        assert row is not None
        recovered = ClickHouseInputManifest.from_json(row[0])

        assert recovered.source_name == "qs-datamaster"
        assert recovered.features_table is None
        assert recovered.regime_table is None
        assert recovered.adjustment_mode is None
        assert recovered.strategy_adjustment_mode is None
        assert recovered.portfolio_adjustment_mode is None
        assert recovered.feature_set_version is None
        assert recovered.regime_version is None
        assert recovered.feature_columns is None

    def test_rerun_upsert_updates_manifest_to_latest_value(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_manifest: ClickHouseInputManifest,
    ) -> None:
        """Re-saving the same (experiment_id, run_id) must replace the manifest with the latest value."""
        # First save: with manifest
        writer.save_run(
            experiment_id="upsert_manifest",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=sample_manifest,
        )
        # Second save: without manifest (NULL)
        writer.save_run(
            experiment_id="upsert_manifest",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=None,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        rows = con.execute("SELECT input_manifest_json FROM runs WHERE run_id = 'run_001'").fetchall()
        con.close()

        assert len(rows) == 1
        assert rows[0][0] is None  # Latest value (NULL) wins

    def test_rerun_upsert_replaces_null_manifest_with_value(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_manifest: ClickHouseInputManifest,
    ) -> None:
        """Re-saving with a manifest after a NULL-manifest run must update to the new value."""
        # First save: without manifest
        writer.save_run(
            experiment_id="upsert_add_manifest",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
        )
        # Second save: with manifest
        writer.save_run(
            experiment_id="upsert_add_manifest",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=sample_manifest,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'upsert_add_manifest'").fetchone()
        con.close()

        assert row is not None
        assert row[0] is not None
        recovered = ClickHouseInputManifest.from_json(row[0])
        assert recovered == sample_manifest

    def test_schema_version_is_1_and_survives_round_trip(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_manifest: ClickHouseInputManifest,
    ) -> None:
        """schema_version=1 must be preserved through JSON serialisation and DuckDB storage."""
        assert sample_manifest.schema_version == 1

        writer.save_run(
            experiment_id="sv_roundtrip",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=sample_manifest,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'sv_roundtrip'").fetchone()
        con.close()

        assert row is not None
        recovered = ClickHouseInputManifest.from_json(row[0])
        assert recovered.schema_version == 1

    def test_existing_run_outputs_unchanged_when_manifest_added(
        self,
        writer: DuckDBWriter,
        db_path: Path,
        sample_metrics: FullMetrics,
        sample_equity_curve: list[EquityCurvePoint],
        sample_trades: list[TradeRecord],
        sample_manifest: ClickHouseInputManifest,
    ) -> None:
        """Adding a manifest to save_run must not affect existing run-output columns."""
        writer.save_run(
            experiment_id="outputs_intact",
            run_id="run_001",
            metrics=sample_metrics,
            equity_curve=sample_equity_curve,
            returns=[],
            trades=sample_trades,
            drawdowns=[],
            manifest=sample_manifest,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        run_row = con.execute(
            "SELECT total_return_pct, sharpe_ratio, total_trades FROM runs WHERE experiment_id = 'outputs_intact'"
        ).fetchone()
        eq_count = con.execute("SELECT COUNT(*) FROM equity_curve WHERE experiment_id = 'outputs_intact'").fetchone()
        trade_count = con.execute("SELECT COUNT(*) FROM trades WHERE experiment_id = 'outputs_intact'").fetchone()
        con.close()

        assert run_row is not None
        assert abs(run_row[0] - 5.00) < 0.01
        assert abs(run_row[1] - 1.85) < 0.01
        assert run_row[2] == 4
        assert eq_count is not None and eq_count[0] == 3
        assert trade_count is not None and trade_count[0] == 1


# ============================================================================
# Manifest validation (negative paths)
# ============================================================================


class TestClickHouseInputManifestValidation:
    """Negative-path tests for ClickHouseInputManifest field validation.

    These guard the persisted contract: invalid manifests must never reach
    DuckDB.  Each test exercises a distinct invariant enforced by a
    ``@field_validator`` or ``@model_validator``.
    """

    # ------------------------------------------------------------------
    # symbols
    # ------------------------------------------------------------------

    def test_rejects_empty_symbols_list(self) -> None:
        """symbols=[] must raise ValidationError (at least one symbol required)."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="at least one ticker symbol"):
            ClickHouseInputManifest(
                source_name="qs-datamaster",
                database="market_data",
                bars_table="equity_daily",
                symbols=[],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

    def test_rejects_omitted_symbols_argument(self) -> None:
        """Omitting symbols entirely must raise ValidationError.

        ``symbols`` has no default; the ``Field(default_factory=list)`` was
        intentionally removed so that callers cannot accidentally produce an
        empty-universe manifest by simply forgetting the argument.
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ClickHouseInputManifest(  # type: ignore[call-arg]
                source_name="qs-datamaster",
                database="market_data",
                bars_table="equity_daily",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
            )

    # ------------------------------------------------------------------
    # start_date / end_date type enforcement
    # ------------------------------------------------------------------

    def test_rejects_non_date_start_date_string(self) -> None:
        """start_date='not-a-date' must raise ValidationError (invalid date string)."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ClickHouseInputManifest(
                source_name="qs-datamaster",
                database="market_data",
                bars_table="equity_daily",
                symbols=["AAPL"],
                start_date="not-a-date",  # type: ignore[arg-type]
                end_date=date(2024, 12, 31),
            )

    def test_rejects_non_date_end_date_string(self) -> None:
        """end_date='not-a-date' must raise ValidationError (invalid date string)."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            ClickHouseInputManifest(
                source_name="qs-datamaster",
                database="market_data",
                bars_table="equity_daily",
                symbols=["AAPL"],
                start_date=date(2024, 1, 1),
                end_date="not-a-date",  # type: ignore[arg-type]
            )

    # ------------------------------------------------------------------
    # date ordering
    # ------------------------------------------------------------------

    def test_rejects_end_date_before_start_date(self) -> None:
        """end_date strictly before start_date must raise ValidationError."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="end_date.*must not be before start_date"):
            ClickHouseInputManifest(
                source_name="qs-datamaster",
                database="market_data",
                bars_table="equity_daily",
                symbols=["AAPL"],
                start_date=date(2024, 6, 1),
                end_date=date(2024, 1, 1),
            )

    def test_accepts_same_start_and_end_date(self) -> None:
        """A single-day run (start_date == end_date) must be accepted."""
        manifest = ClickHouseInputManifest(
            source_name="qs-datamaster",
            database="market_data",
            bars_table="equity_daily",
            symbols=["SPY"],
            start_date=date(2024, 3, 15),
            end_date=date(2024, 3, 15),
        )
        assert manifest.start_date == manifest.end_date

    # ------------------------------------------------------------------
    # ISO-8601 string coercion (positive path)
    # ------------------------------------------------------------------

    def test_accepts_iso8601_strings_for_dates(self) -> None:
        """ISO-8601 date strings in a dict payload must be coerced to date objects.

        The typed constructor signature accepts only ``datetime.date`` objects;
        string coercion is exercised via ``model_validate`` (the path taken by
        JSON payloads and dict-based construction) so the test is both accurate
        and free of static type errors.
        """
        manifest = ClickHouseInputManifest.model_validate(
            {
                "source_name": "qs-datamaster",
                "database": "market_data",
                "bars_table": "equity_daily",
                "symbols": ["SPY"],
                "start_date": "2024-01-01",
                "end_date": "2024-12-31",
            }
        )
        assert manifest.start_date == date(2024, 1, 1)
        assert manifest.end_date == date(2024, 12, 31)

    # ------------------------------------------------------------------
    # extra fields (schema drift protection)
    # ------------------------------------------------------------------

    def test_rejects_unknown_field_at_construction(self) -> None:
        """Unexpected keyword arguments must raise ValidationError (extra='forbid')."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            ClickHouseInputManifest(
                source_name="qs-datamaster",
                database="market_data",
                bars_table="equity_daily",
                symbols=["AAPL"],
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 31),
                typo_field="oops",  # type: ignore[call-arg]
            )

    def test_rejects_unknown_field_in_from_json(self) -> None:
        """from_json must raise ValidationError when the JSON contains an unknown key.

        This catches schema drift between producers and consumers: a manifest
        written by a newer producer (with a field the current model doesn't know
        about) is rejected loudly instead of silently dropping the unknown data.
        """
        from pydantic import ValidationError

        malformed_json = (
            '{"schema_version":1,"source_kind":"clickhouse",'
            '"source_name":"qs-datamaster","database":"market_data",'
            '"bars_table":"equity_daily","features_table":null,'
            '"regime_table":null,"symbols":["AAPL"],'
            '"start_date":"2024-01-01","end_date":"2024-12-31",'
            '"adjustment_mode":null,"feature_set_version":null,'
            '"regime_version":null,"feature_columns":null,'
            '"unknown_future_field":"some_value"}'
        )
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            ClickHouseInputManifest.from_json(malformed_json)

    # ------------------------------------------------------------------
    # schema_version
    # ------------------------------------------------------------------

    def test_schema_version_defaults_to_1(self) -> None:
        """schema_version must default to 1 when not supplied."""
        manifest = ClickHouseInputManifest(
            source_name="qs-datamaster",
            database="market_data",
            bars_table="equity_daily",
            symbols=["AAPL"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )
        assert manifest.schema_version == 1

    def test_schema_version_present_in_serialised_json(self) -> None:
        """schema_version must appear in the compact JSON produced by to_json()."""
        import json

        manifest = ClickHouseInputManifest(
            source_name="qs-datamaster",
            database="market_data",
            bars_table="equity_daily",
            symbols=["AAPL"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )
        parsed = json.loads(manifest.to_json())
        assert "schema_version" in parsed
        assert parsed["schema_version"] == 1

    def test_symbols_and_feature_columns_are_deeply_immutable(self) -> None:
        """Tuple-backed collection fields must not allow in-place mutation."""
        manifest = ClickHouseInputManifest(
            source_name="qs-datamaster",
            database="market_data",
            bars_table="equity_daily",
            symbols=["AAPL"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            feature_columns=["sma_20"],
        )

        with pytest.raises(AttributeError):
            manifest.symbols.append("MSFT")  # type: ignore[attr-defined]

        with pytest.raises(AttributeError):
            manifest.feature_columns.append("rsi_14")  # type: ignore[union-attr]
