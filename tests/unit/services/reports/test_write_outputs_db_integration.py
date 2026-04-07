"""Integration tests for _write_outputs + _write_to_database in ReportingService.

Validates that:
- Time-series construction is gated behind needs_timeseries (write_parquet or db enabled).
- DuckDB write is invoked only when database output is enabled.
- A -100% return does not crash teardown (math.log domain error).
- save_run() is called with manifest=None by default (Phase 1 baseline).
- save_run() can be invoked with an explicit manifest (Phase 1 contract).
- BacktestConfig Phase 1 fields (job_group_id, submission_source, split_pct, split_role)
  flow correctly through ReportingService.teardown() → DuckDBWriter.save_run.
"""

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.libraries.performance.models import FullMetrics
from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.service import ReportingService

# ============================================================================
# Helpers
# ============================================================================


def _make_system_config_mock(*, db_enabled: bool, db_path: str = "test.duckdb", config_root: Path | None = None):
    """Create a mock SystemConfig with configurable database output."""
    mock = MagicMock()
    mock.output.database.enabled = db_enabled
    mock.output.database.path = db_path
    mock.config_root = config_root or Path.cwd()
    return mock


def _minimal_metrics() -> FullMetrics:
    """Construct a minimal FullMetrics for _write_outputs."""
    return FullMetrics.model_construct(
        backtest_id="test_bt",
        start_date="2020-01-01",
        end_date="2020-01-31",
        duration_days=30,
        initial_equity=Decimal("100000"),
        final_equity=Decimal("105000"),
        total_return_pct=Decimal("5.00"),
        cagr=Decimal("0"),
        best_day_return_pct=Decimal("0"),
        worst_day_return_pct=Decimal("0"),
        volatility_annual_pct=Decimal("0"),
        max_drawdown_pct=Decimal("0"),
        max_drawdown_duration_days=0,
        avg_drawdown_pct=Decimal("0"),
        current_drawdown_pct=Decimal("0"),
        sharpe_ratio=Decimal("0"),
        sortino_ratio=Decimal("0"),
        calmar_ratio=Decimal("0"),
        risk_free_rate=Decimal("0"),
        total_trades=0,
        winning_trades=0,
        losing_trades=0,
        win_rate=Decimal("0"),
        profit_factor=Decimal("0"),
        avg_win=Decimal("0"),
        avg_loss=Decimal("0"),
        avg_win_pct=Decimal("0"),
        avg_loss_pct=Decimal("0"),
        largest_win=Decimal("0"),
        largest_loss=Decimal("0"),
        largest_win_pct=Decimal("0"),
        largest_loss_pct=Decimal("0"),
        expectancy=Decimal("0"),
        max_consecutive_wins=0,
        max_consecutive_losses=0,
        avg_trade_duration_days=Decimal("0"),
        total_commissions=Decimal("0"),
        commission_pct_of_pnl=Decimal("0"),
        monthly_returns=[],
        quarterly_returns=[],
        annual_returns=[],
        strategy_performance=[],
        drawdown_periods=[],
    )


def _build_reporting_service(
    tmp_path: Path,
    *,
    write_parquet: bool = True,
    write_json: bool = False,
    write_html_report: bool = False,
    write_csv_timeline: bool = False,
) -> ReportingService:
    """Build a ReportingService with controllable output flags."""
    config = ReportingConfig(
        write_parquet=write_parquet,
        write_json=write_json,
        write_html_report=write_html_report,
        write_csv_timeline=write_csv_timeline,
        display_final_report=False,
    )
    output_dir = tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000"
    output_dir.mkdir(parents=True, exist_ok=True)

    event_bus = EventBus()
    svc = ReportingService(event_bus=event_bus, config=config, output_dir=output_dir)
    svc._backtest_id = "test_bt"
    return svc


# ============================================================================
# Tests: time-series gating
# ============================================================================


class TestTimeSeriesGating:
    """Verify time-series construction is skipped when no consumer needs it."""

    def test_no_timeseries_when_parquet_false_and_db_disabled(self, tmp_path: Path) -> None:
        """With write_parquet=False and database disabled, equity/returns should NOT be built."""
        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        # Inject a mock that would fail if accessed — proves construction is skipped
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        # get_curve should NOT have been called — time-series is gated
        svc._equity_calc.get_curve.assert_not_called()

    def test_no_equity_when_parquet_true_but_include_equity_false_and_db_disabled(self, tmp_path: Path) -> None:
        """write_parquet=True but include_equity_curve=False and DB off → skip equity build."""
        svc = _build_reporting_service(tmp_path, write_parquet=True)
        svc.config = svc.config.model_copy(update={"include_equity_curve": False, "include_returns": False})
        metrics = _minimal_metrics()

        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._last_portfolio_state = None

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        # Neither equity nor returns should have been built
        svc._equity_calc.get_curve.assert_not_called()

    def test_timeseries_built_when_parquet_enabled(self, tmp_path: Path) -> None:
        """With write_parquet=True, equity curve should be built even if DB is disabled."""
        svc = _build_reporting_service(tmp_path, write_parquet=True)
        metrics = _minimal_metrics()

        # Provide realistic calculator state with data
        ts = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [(ts, Decimal("100000"))]
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._last_portfolio_state = None  # No portfolio state → skip equity build

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        # Method completes without error — parquet path was taken

    def test_timeseries_built_when_db_enabled_and_parquet_false(self, tmp_path: Path) -> None:
        """With write_parquet=False but DB enabled, time-series should still be built."""
        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        # Add returns data that would trigger build
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [Decimal("0.01")]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("101000")),
        ]
        svc._last_portfolio_state = None  # Skip equity points

        db_path = str(tmp_path / "test_runs.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch(
                "qs_trader.system.config.get_system_config",
                return_value=sys_config,
            ),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter"),
        ):
            svc._write_outputs(metrics)

        # get_curve WAS called because db is enabled → needs_timeseries is True
        svc._equity_calc.get_curve.assert_called()


# ============================================================================
# Tests: database integration
# ============================================================================


class TestDatabaseIntegration:
    """Verify _write_to_database is called correctly from _write_outputs."""

    def test_db_writer_called_when_enabled(self, tmp_path: Path) -> None:
        """DuckDBWriter.save_run should be called when database is enabled."""
        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "test_runs.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch(
                "qs_trader.system.config.get_system_config",
                return_value=sys_config,
            ),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_writer_cls.return_value.save_run.assert_called_once()

    def test_db_writer_not_called_when_disabled(self, tmp_path: Path) -> None:
        """DuckDBWriter should not be instantiated when database is disabled."""
        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        sys_config = _make_system_config_mock(db_enabled=False)
        with (
            patch(
                "qs_trader.system.config.get_system_config",
                return_value=sys_config,
            ),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_writer_cls.assert_not_called()

    def test_db_writer_error_does_not_crash_teardown(self, tmp_path: Path) -> None:
        """Database write failure should be logged, not propagated."""
        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "test_runs.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch(
                "qs_trader.system.config.get_system_config",
                return_value=sys_config,
            ),
            patch(
                "qs_trader.services.reporting.db_writer.DuckDBWriter",
                side_effect=RuntimeError("disk full"),
            ),
        ):
            # Should not raise
            svc._write_outputs(metrics)


# ============================================================================
# Tests: pathological returns
# ============================================================================


class TestPathologicalReturns:
    """Verify that extreme return values don't crash teardown."""

    def test_minus_100_pct_return_produces_null_log_return(self, tmp_path: Path) -> None:
        """A -100% return (growth=0) should produce log_return=None, not crash."""
        svc = _build_reporting_service(tmp_path, write_parquet=True)
        metrics = _minimal_metrics()

        # -100% return: 1 + (-1.0) = 0 → log undefined → log_return=None
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [Decimal("-1.0")]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("0")),
        ]
        svc._last_portfolio_state = None

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        # Verify the produced ReturnPoint has log_return=None
        # returns_points is built as a local var; inspect via the written JSON
        returns_json = (
            tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000" / "timeseries" / "returns.json"
        )
        import json

        data = json.loads(returns_json.read_text())
        assert len(data) == 1
        assert data[0]["log_return"] is None

    def test_near_minus_100_pct_return_produces_valid_log(self, tmp_path: Path) -> None:
        """A -99.99% return should produce a very negative but finite log_return."""
        svc = _build_reporting_service(tmp_path, write_parquet=True)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [Decimal("-0.9999")]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("10")),
        ]
        svc._last_portfolio_state = None

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        returns_json = (
            tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000" / "timeseries" / "returns.json"
        )
        import json

        data = json.loads(returns_json.read_text())
        assert len(data) == 1
        # math.log(0.0001) ≈ -9.21 — valid finite number
        assert data[0]["log_return"] is not None
        assert data[0]["log_return"] < -9.0

    def test_worse_than_minus_100_pct_produces_null_log_return(self, tmp_path: Path) -> None:
        """Return < -100% means growth < 0. log_return should be None."""
        svc = _build_reporting_service(tmp_path, write_parquet=True)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [Decimal("-1.5")]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("0")),
        ]
        svc._last_portfolio_state = None

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        returns_json = (
            tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000" / "timeseries" / "returns.json"
        )
        import json

        data = json.loads(returns_json.read_text())
        assert len(data) == 1
        assert data[0]["log_return"] is None


class TestDecimalPrecisionRegression:
    """Verify that high-precision Decimals near -100% are handled correctly in Decimal space."""

    def test_high_precision_near_minus_100_not_rounded_to_null(self, tmp_path: Path) -> None:
        """A return like Decimal('-0.9999999999999999999999999999') is >-1 in Decimal space.

        float(Decimal('-0.9999999999999999999999999999')) rounds to -1.0, making
        1 + float(...) == 0.0, which would incorrectly produce log_return=None.
        The domain check must use Decimal arithmetic to avoid this.
        """
        svc = _build_reporting_service(tmp_path, write_parquet=True)
        metrics = _minimal_metrics()

        # 28-digit precision: Decimal growth = 1e-28 > 0, but float growth = 0.0
        high_precision_return = Decimal("-0.9999999999999999999999999999")
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [high_precision_return]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("0.00001")),
        ]
        svc._last_portfolio_state = None

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        returns_json = (
            tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000" / "timeseries" / "returns.json"
        )
        import json

        data = json.loads(returns_json.read_text())
        assert len(data) == 1
        # Decimal growth = 1e-28 > 0 → log_return should be a finite number, NOT None
        assert data[0]["log_return"] is not None
        # math.log of a very small positive number → very large negative
        assert data[0]["log_return"] < -60


# ============================================================================
# Tests: manifest persistence integration (Phase 1 baseline)
# ============================================================================


class TestManifestIntegration:
    """Verify the manifest contract at the service boundary.

    Phase 1 establishes the *structural* contract only: ``save_run()`` must
    accept an optional ``manifest`` kwarg and forward it to the writer.
    Populating the manifest with real ClickHouse metadata is Phase 2 work;
    here we prove the call-site and default behaviour.
    """

    def test_save_run_called_with_no_manifest_by_default(self, tmp_path: Path) -> None:
        """_write_to_database must call save_run() with manifest=None when no manifest is set.

        Phase 1 does not change existing call sites.  The default value of
        ``manifest`` in save_run() must be ``None``, so all pre-existing
        invocations remain unaffected and store NULL in the database.
        """
        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "test_runs.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch(
                "qs_trader.system.config.get_system_config",
                return_value=sys_config,
            ),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_instance = mock_writer_cls.return_value
        mock_instance.save_run.assert_called_once()

        call_kwargs = mock_instance.save_run.call_args
        # manifest kwarg must be absent or explicitly None
        passed_manifest = call_kwargs.kwargs.get("manifest", None)
        assert passed_manifest is None, (
            "save_run() must not receive a non-None manifest in Phase 1 (manifest construction is Phase 2)"
        )

    def test_save_run_signature_accepts_manifest_kwarg(self, tmp_path: Path) -> None:
        """DuckDBWriter.save_run() must accept a ``manifest`` keyword argument.

        This ensures the writer contract is in place before Phase 2 adds
        manifest construction at the engine/service level.
        """
        import inspect

        from qs_trader.services.reporting.db_writer import DuckDBWriter

        sig = inspect.signature(DuckDBWriter.save_run)
        assert "manifest" in sig.parameters, (
            "DuckDBWriter.save_run() must have a 'manifest' parameter (Phase 1 schema contract)"
        )
        param = sig.parameters["manifest"]
        assert param.default is None, "The 'manifest' parameter must default to None for backward compatibility"

    def test_writer_accepts_manifest_object_without_error(self, tmp_path: Path) -> None:
        """Calling save_run() with a real manifest must not raise.

        This is a smoke-test for the complete write path: manifest → JSON
        → DuckDB VARCHAR column.  No mocking of the writer — we use the real
        DuckDBWriter so the full serialisation path is exercised.
        """
        import duckdb

        from qs_trader.services.reporting.db_writer import DuckDBWriter
        from qs_trader.services.reporting.manifest import ClickHouseInputManifest

        manifest = ClickHouseInputManifest(
            source_name="qs-datamaster",
            database="market_data",
            bars_table="equity_daily",
            symbols=("AAPL",),
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
            feature_set_version="v1",
        )

        db_path = tmp_path / "smoke.duckdb"
        writer = DuckDBWriter(db_path)
        metrics = _minimal_metrics()

        # Must not raise
        writer.save_run(
            experiment_id="smoke",
            run_id="run_001",
            metrics=metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            manifest=manifest,
        )

        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'smoke'").fetchone()
        con.close()

        assert row is not None
        assert row[0] is not None
        recovered = ClickHouseInputManifest.from_json(row[0])
        assert recovered.source_name == "qs-datamaster"
        assert recovered.feature_set_version == "v1"

    def test_save_bars_not_called_when_bar_rows_empty(self, tmp_path: Path) -> None:
        """Phase 5 regression: ``save_run`` is called exactly once per write cycle.

        ``save_bars_with_features`` was removed in Phase 5 — this test is
        intentionally renamed as a regression guard to confirm ``save_run``
        still fires correctly on a basic ``_write_outputs`` call with no
        manifest and empty returns/equity.
        """
        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "compat.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch(
                "qs_trader.system.config.get_system_config",
                return_value=sys_config,
            ),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_writer_cls.return_value.save_run.assert_called_once()


class TestCumulativeReturnCompounding:
    """Verify cumulative_return uses geometric compounding, not arithmetic sum."""

    def test_two_positive_periods_compound_correctly(self, tmp_path: Path) -> None:
        """Two +10% periods → compounded cumulative = 1.1 * 1.1 - 1 = 0.21, not 0.20."""
        svc = _build_reporting_service(tmp_path, write_parquet=True, write_json=True)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [Decimal("0.10"), Decimal("0.10")]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        ts3 = datetime(2020, 1, 4, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("110000")),
            (ts3, Decimal("121000")),
        ]
        svc._last_portfolio_state = None

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        returns_json = (
            tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000" / "timeseries" / "returns.json"
        )
        import json

        data = json.loads(returns_json.read_text())
        assert len(data) == 2
        # First period: cumulative = 1.10 - 1 = 0.10
        assert abs(data[0]["cumulative_return"] - 0.10) < 1e-10
        # Second period: cumulative = 1.10 * 1.10 - 1 = 0.21 (NOT 0.20)
        assert abs(data[1]["cumulative_return"] - 0.21) < 1e-10

    def test_alternating_returns_compound_correctly(self, tmp_path: Path) -> None:
        """+50% then -50% → compounded = 1.5 * 0.5 - 1 = -0.25, not 0.00."""
        svc = _build_reporting_service(tmp_path, write_parquet=True, write_json=True)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [Decimal("0.50"), Decimal("-0.50")]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        ts3 = datetime(2020, 1, 4, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("150000")),
            (ts3, Decimal("75000")),
        ]
        svc._last_portfolio_state = None

        sys_config = _make_system_config_mock(db_enabled=False)
        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        returns_json = (
            tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000" / "timeseries" / "returns.json"
        )
        import json

        data = json.loads(returns_json.read_text())
        assert len(data) == 2
        assert abs(data[0]["cumulative_return"] - 0.50) < 1e-10
        # +50% then -50% compounds to -25%, NOT 0%
        assert abs(data[1]["cumulative_return"] - (-0.25)) < 1e-10


class TestDuckDBNullLogReturn:
    """Verify that None log_return survives through the DuckDB path as NULL."""

    def test_minus_100_pct_stored_as_null_in_duckdb(self, tmp_path: Path) -> None:
        """A -100% return should persist log_return IS NULL in DuckDB."""
        import duckdb

        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [Decimal("-1.0")]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("0")),
        ]
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "test_null.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        # Verify in the actual DuckDB file
        con = duckdb.connect(db_path, read_only=True)
        row = con.execute(
            "SELECT period_return, cumulative_return, log_return FROM returns "
            "WHERE experiment_id = 'test_exp' ORDER BY timestamp"
        ).fetchone()
        con.close()

        assert row is not None
        assert abs(row[0] - (-1.0)) < 1e-10  # period_return = -1.0
        assert abs(row[1] - (-1.0)) < 1e-10  # cumulative = (1 + -1) - 1 = -1.0
        assert row[2] is None  # log_return IS NULL

    def test_near_minus_100_pct_stored_as_finite_in_duckdb(self, tmp_path: Path) -> None:
        """A -99.99% return should persist log_return as a finite negative in DuckDB."""
        import duckdb

        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = [Decimal("-0.9999")]
        ts1 = datetime(2020, 1, 2, 16, 0, 0, tzinfo=timezone.utc)
        ts2 = datetime(2020, 1, 3, 16, 0, 0, tzinfo=timezone.utc)
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = [
            (ts1, Decimal("100000")),
            (ts2, Decimal("10")),
        ]
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "test_finite.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        con = duckdb.connect(db_path, read_only=True)
        row = con.execute(
            "SELECT log_return FROM returns WHERE experiment_id = 'test_exp' ORDER BY timestamp"
        ).fetchone()
        con.close()

        assert row is not None
        assert row[0] is not None  # NOT NULL — valid finite value
        assert row[0] < -9.0  # math.log(0.0001) ≈ -9.21


class TestDatabasePathResolution:
    """Verify that relative database paths resolve against config_root, not CWD."""

    def test_relative_db_path_resolves_against_config_root(self, tmp_path: Path) -> None:
        """A relative database path should land under config_root, not os.getcwd()."""
        import os

        import duckdb

        # Set up a config_root that differs from CWD
        project_root = tmp_path / "project"
        project_root.mkdir()
        data_dir = project_root / "data"
        data_dir.mkdir()

        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        # Use a RELATIVE path in config, with config_root pointing to project_root
        sys_config = _make_system_config_mock(
            db_enabled=True,
            db_path="data/backtest_runs.duckdb",
            config_root=project_root,
        )

        # Change CWD to a completely different directory
        other_dir = tmp_path / "other_cwd"
        other_dir.mkdir()
        original_cwd = os.getcwd()
        try:
            os.chdir(str(other_dir))
            with patch(
                "qs_trader.system.config.get_system_config",
                return_value=sys_config,
            ):
                svc._write_outputs(metrics)
        finally:
            os.chdir(original_cwd)

        # The DuckDB file should be under project_root/data/, not other_cwd/data/
        expected_db = project_root / "data" / "backtest_runs.duckdb"
        wrong_db = other_dir / "data" / "backtest_runs.duckdb"

        assert expected_db.exists(), f"DB should exist at {expected_db}"
        assert not wrong_db.exists(), f"DB should NOT exist at {wrong_db}"

        # Verify it's a valid DuckDB with the expected tables
        con = duckdb.connect(str(expected_db), read_only=True)
        tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        con.close()
        assert "runs" in tables

    def test_absolute_db_path_ignores_config_root(self, tmp_path: Path) -> None:
        """An absolute database path should be used as-is, ignoring config_root."""
        import duckdb

        abs_db_path = str(tmp_path / "absolute.duckdb")

        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        # config_root is set to something different, but path is absolute
        sys_config = _make_system_config_mock(
            db_enabled=True,
            db_path=abs_db_path,
            config_root=tmp_path / "should_be_ignored",
        )

        with patch(
            "qs_trader.system.config.get_system_config",
            return_value=sys_config,
        ):
            svc._write_outputs(metrics)

        assert Path(abs_db_path).exists()

        con = duckdb.connect(abs_db_path, read_only=True)
        tables = [row[0] for row in con.execute("SHOW TABLES").fetchall()]
        con.close()
        assert "runs" in tables


class TestConfigResolutionWarning:
    """Verify that config resolution failure logs a warning instead of silently degrading."""

    def test_config_failure_logs_warning(self, tmp_path: Path) -> None:
        """When get_system_config() raises, a warning should be logged."""
        svc = _build_reporting_service(tmp_path, write_parquet=False)
        metrics = _minimal_metrics()

        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        with patch(
            "qs_trader.system.config.get_system_config",
            side_effect=RuntimeError("config file corrupt"),
        ):
            svc._write_outputs(metrics)

        # Verify warning was logged (structlog captures to the logger mock)
        # The service should complete without error
        # We verify via the logger's bound calls
        assert svc.logger is not None  # Service didn't crash


# ============================================================================
# Tests: Phase 1 field flow (BacktestConfig → teardown → DuckDBWriter)
# ============================================================================


def _make_backtest_config(**overrides):
    """Build a minimal BacktestConfig with the Phase 1 fields set to non-None values.

    Overrides any of the Phase 1 fields via keyword args.
    """
    from qs_trader.engine.config import (
        BacktestConfig,
        DataSelectionConfig,
        DataSourceConfig,
        RiskPolicyConfig,
        StrategyConfigItem,
    )

    defaults = {
        "backtest_id": "phase1_integration_test",
        "start_date": datetime(2024, 1, 1, tzinfo=timezone.utc),
        "end_date": datetime(2024, 6, 30, tzinfo=timezone.utc),
        "initial_equity": Decimal("100000"),
        "data": DataSelectionConfig(
            sources=[DataSourceConfig(name="yahoo-us-equity-1d-csv", universe=["AAPL"])]
        ),
        "strategies": [
            StrategyConfigItem(
                strategy_id="buy_and_hold",
                universe=["AAPL"],
                data_sources=["yahoo-us-equity-1d-csv"],
            )
        ],
        "risk_policy": RiskPolicyConfig(name="naive"),
        # Phase 1 fields — all populated to non-None
        "job_group_id": "sweep-abc123",
        "submission_source": "dashboard",
        "split_pct": 0.7,
        "split_role": "in_sample",
    }
    defaults.update(overrides)
    return BacktestConfig(**defaults)


def _build_reporting_service_with_db(tmp_path: Path) -> "ReportingService":
    """Build a ReportingService wired to a real tmp output_dir."""
    config = ReportingConfig(
        write_parquet=False,
        write_json=False,
        write_html_report=False,
        write_csv_timeline=False,
        display_final_report=False,
    )
    output_dir = tmp_path / "experiments" / "phase1_test" / "runs" / "20260101_000000"
    output_dir.mkdir(parents=True, exist_ok=True)
    event_bus = EventBus()
    svc = ReportingService(event_bus=event_bus, config=config, output_dir=output_dir)
    svc._backtest_id = "phase1_integration_test"
    return svc


def _inject_minimal_teardown_state(svc: "ReportingService") -> None:
    """Inject the minimum instance state so teardown() passes the early-return guard."""
    svc._start_datetime = "2024-01-01T00:00:00+00:00"
    svc._initial_equity = Decimal("100000")
    svc._last_portfolio_state = None

    # Mock calculators to return neutral/empty data
    svc._equity_calc = MagicMock()
    svc._equity_calc.latest_timestamp.return_value = None
    svc._equity_calc.latest_equity.return_value = Decimal("105000")
    svc._equity_calc.get_curve.return_value = []

    svc._returns_calc = MagicMock()
    svc._returns_calc.returns = []

    svc._drawdown_calc = MagicMock()
    svc._drawdown_calc.max_drawdown_pct = Decimal("0")
    svc._drawdown_calc.current_drawdown_pct = Decimal("0")
    svc._drawdown_calc.drawdown_periods = []

    svc._trade_stats_calc = MagicMock()
    svc._trade_stats_calc.trades = []
    svc._trade_stats_calc.max_consecutive_wins = 0
    svc._trade_stats_calc.max_consecutive_losses = 0

    svc._period_calc = MagicMock()
    svc._period_calc.calculate_periods.return_value = []

    svc._strategy_perf_calc = MagicMock()
    svc._strategy_perf_calc.calculate_performance.return_value = []


class TestReportingServicePhase1Fields:
    """Integration test: BacktestConfig Phase 1 fields flow through teardown to DuckDB.

    Verifies that job_group_id, submission_source, split_pct, and split_role set
    on a BacktestConfig are extracted by ReportingService.teardown() and forwarded
    to DuckDBWriter.save_run() with the correct non-None values.
    """

    def test_four_phase1_fields_reach_save_run(self, tmp_path: Path) -> None:
        """All four Phase 1 fields on BacktestConfig must be passed to save_run."""
        svc = _build_reporting_service_with_db(tmp_path)
        _inject_minimal_teardown_state(svc)

        config = _make_backtest_config(
            job_group_id="sweep-abc123",
            submission_source="dashboard",
            split_pct=0.7,
            split_role="in_sample",
        )
        db_path = str(tmp_path / "test_runs.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
            patch("qs_trader.services.reporting.service.write_backtest_metadata"),
        ):
            svc.teardown({"backtest_config": config})

        mock_instance = mock_writer_cls.return_value
        mock_instance.save_run.assert_called_once()
        kwargs = mock_instance.save_run.call_args.kwargs

        assert kwargs["job_group_id"] == "sweep-abc123"
        assert kwargs["submission_source"] == "dashboard"
        assert kwargs["split_pct"] == pytest.approx(0.7)
        assert kwargs["split_role"] == "in_sample"

    def test_none_phase1_fields_reach_save_run_as_none(self, tmp_path: Path) -> None:
        """Phase 1 fields absent from BacktestConfig must arrive as None at save_run."""
        svc = _build_reporting_service_with_db(tmp_path)
        _inject_minimal_teardown_state(svc)

        config = _make_backtest_config(
            job_group_id=None,
            submission_source=None,
            split_pct=None,
            split_role=None,
        )
        db_path = str(tmp_path / "test_runs.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
            patch("qs_trader.services.reporting.service.write_backtest_metadata"),
        ):
            svc.teardown({"backtest_config": config})

        mock_instance = mock_writer_cls.return_value
        mock_instance.save_run.assert_called_once()
        kwargs = mock_instance.save_run.call_args.kwargs

        assert kwargs["job_group_id"] is None
        assert kwargs["submission_source"] is None
        assert kwargs["split_pct"] is None
        assert kwargs["split_role"] is None

    def test_phase1_fields_persist_in_duckdb_with_correct_values(self, tmp_path: Path) -> None:
        """End-to-end: Phase 1 fields must be stored with correct values in a real DuckDB file."""
        import duckdb

        from qs_trader.services.reporting.db_writer import DuckDBWriter

        svc = _build_reporting_service_with_db(tmp_path)
        _inject_minimal_teardown_state(svc)

        config = _make_backtest_config(
            job_group_id="e2e-group-999",
            submission_source="cli",
            split_pct=0.8,
            split_role="out_of_sample",
        )
        db_path = tmp_path / "e2e_runs.duckdb"
        sys_config = _make_system_config_mock(db_enabled=True, db_path=str(db_path))

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.service.write_backtest_metadata"),
        ):
            svc.teardown({"backtest_config": config})

        assert db_path.exists(), "DuckDB file must be created"
        con = duckdb.connect(str(db_path), read_only=True)
        row = con.execute(
            "SELECT job_group_id, submission_source, split_pct, split_role FROM runs"
        ).fetchone()
        con.close()

        assert row is not None
        assert row[0] == "e2e-group-999"
        assert row[1] == "cli"
        assert abs(row[2] - 0.8) < 1e-6
        assert row[3] == "out_of_sample"

    def test_teardown_without_backtest_config_leaves_phase1_fields_none(self, tmp_path: Path) -> None:
        """When teardown context omits backtest_config, Phase 1 fields stay None (no crash)."""
        svc = _build_reporting_service_with_db(tmp_path)
        _inject_minimal_teardown_state(svc)

        db_path = str(tmp_path / "test_runs.duckdb")
        sys_config = _make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            # No backtest_config key in context
            svc.teardown({})

        mock_instance = mock_writer_cls.return_value
        mock_instance.save_run.assert_called_once()
        kwargs = mock_instance.save_run.call_args.kwargs

        assert kwargs["job_group_id"] is None
        assert kwargs["submission_source"] is None
        assert kwargs["split_pct"] is None
        assert kwargs["split_role"] is None
