"""Tests for ReportingService feature-gate (feature_enabled flag).

Verifies that bar buffering and feature_bar subscriptions are activated only
when feature_enabled=True, preventing OHLCV deduplication into DuckDB on
ordinary (non-feature) backtest runs.
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import FeatureBarEvent, PriceBarEvent
from qs_trader.services.reporting.manifest import ClickHouseInputManifest
from qs_trader.services.reporting.service import ReportingService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_price_bar_event(symbol: str = "AAPL", timestamp: str = "2024-01-02T21:00:00+00:00") -> PriceBarEvent:
    return PriceBarEvent(
        symbol=symbol,
        asset_class="equity",
        interval="1d",
        timestamp=timestamp,
        open=Decimal("185.00"),
        high=Decimal("186.00"),
        low=Decimal("184.00"),
        close=Decimal("185.50"),
        volume=1_000_000,
        source="test",
    )


def _make_feature_bar_event(symbol: str = "AAPL", timestamp: str = "2024-01-02T21:00:00+00:00") -> FeatureBarEvent:
    return FeatureBarEvent(
        timestamp=timestamp,
        symbol=symbol,
        features={"trend_strength": 0.8, "trend_regime": "bull"},
        feature_set_version="v1",
    )


def _make_reporting_service(feature_enabled: bool) -> ReportingService:
    """Create a minimal ReportingService with a live EventBus."""
    event_bus = EventBus()
    svc = ReportingService(event_bus=event_bus, feature_enabled=feature_enabled)
    return svc


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestReportingServiceFeatureGate:
    """ReportingService bar-buffering gated behind feature_enabled."""

    def test_bar_not_buffered_when_feature_disabled(self) -> None:
        """Publishing a PriceBarEvent should NOT populate _bar_rows when feature_enabled=False."""
        svc = _make_reporting_service(feature_enabled=False)
        event = _make_price_bar_event()
        svc.event_bus.publish(event)
        assert svc._bar_rows == {}

    def test_feature_bar_not_buffered_when_feature_disabled(self) -> None:
        """Publishing a FeatureBarEvent should NOT affect _bar_rows when feature_enabled=False."""
        svc = _make_reporting_service(feature_enabled=False)
        svc.event_bus.publish(_make_price_bar_event())
        svc.event_bus.publish(_make_feature_bar_event())
        assert svc._bar_rows == {}

    def test_bar_buffered_when_feature_enabled(self) -> None:
        """Publishing a PriceBarEvent SHOULD populate _bar_rows when feature_enabled=True."""
        svc = _make_reporting_service(feature_enabled=True)
        event = _make_price_bar_event()
        svc.event_bus.publish(event)
        assert len(svc._bar_rows) == 1
        key = ("AAPL", "2024-01-02T21:00:00+00:00")
        row = svc._bar_rows[key]
        assert row["symbol"] == "AAPL"
        assert row["close"] == pytest.approx(185.50)
        assert row["features"] is None  # not yet merged

    def test_feature_bar_merged_into_buffered_row(self) -> None:
        """FeatureBarEvent should merge feature data into the existing _bar_rows entry."""
        svc = _make_reporting_service(feature_enabled=True)
        svc.event_bus.publish(_make_price_bar_event())
        svc.event_bus.publish(_make_feature_bar_event())
        key = ("AAPL", "2024-01-02T21:00:00+00:00")
        row = svc._bar_rows[key]
        assert row["features"] == {"trend_strength": 0.8, "trend_regime": "bull"}
        assert row["feature_set_version"] == "v1"

    def test_feature_enabled_flag_stored(self) -> None:
        """_feature_enabled attribute should reflect the constructor argument."""
        assert _make_reporting_service(feature_enabled=False)._feature_enabled is False
        assert _make_reporting_service(feature_enabled=True)._feature_enabled is True


# ---------------------------------------------------------------------------
# Manifest capture tests (Phase 2)
# ---------------------------------------------------------------------------


def _sample_manifest() -> ClickHouseInputManifest:
    return ClickHouseInputManifest(
        source_name="qs-datamaster-equity-1d",
        database="market",
        bars_table="as_us_equity_ohlc_daily",
        symbols=["AAPL", "MSFT"],
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        adjustment_mode="total_return",
        feature_set_version="v1",
    )


class TestReportingServiceManifestCapture:
    """ReportingService stores and forwards the ClickHouseInputManifest from setup context."""

    def test_manifest_stored_when_present_in_setup_context(self) -> None:
        """setup() with ``input_manifest`` in context must store the manifest."""
        # Arrange
        svc = _make_reporting_service(feature_enabled=False)
        manifest = _sample_manifest()

        # Act
        svc.setup({"backtest_id": "bt1", "strategy_ids": [], "input_manifest": manifest})

        # Assert
        assert svc._input_manifest is manifest

    def test_manifest_is_none_when_absent_from_setup_context(self) -> None:
        """setup() without ``input_manifest`` key must leave _input_manifest as None."""
        # Arrange
        svc = _make_reporting_service(feature_enabled=False)

        # Act
        svc.setup({"backtest_id": "bt1", "strategy_ids": []})

        # Assert
        assert svc._input_manifest is None

    def test_manifest_set_to_none_when_explicit_none_in_context(self) -> None:
        """Explicit None in context must also result in None (Yahoo/CSV path)."""
        # Arrange
        svc = _make_reporting_service(feature_enabled=False)

        # Act
        svc.setup({"backtest_id": "bt1", "strategy_ids": [], "input_manifest": None})

        # Assert
        assert svc._input_manifest is None

    def test_manifest_cleared_by_reset(self) -> None:
        """reset() must clear a manifest set during a previous setup()."""
        # Arrange
        svc = _make_reporting_service(feature_enabled=False)
        manifest = _sample_manifest()
        svc.setup({"backtest_id": "bt1", "strategy_ids": [], "input_manifest": manifest})
        assert svc._input_manifest is manifest  # precondition

        # Act
        svc.reset()

        # Assert
        assert svc._input_manifest is None

    def test_setup_second_run_replaces_previous_manifest(self) -> None:
        """Calling setup() a second time must replace the previously stored manifest."""
        # Arrange
        svc = _make_reporting_service(feature_enabled=False)
        manifest_a = _sample_manifest()
        manifest_b = ClickHouseInputManifest(
            source_name="qs-datamaster-equity-1d",
            database="market",
            bars_table="as_us_equity_ohlc_daily",
            symbols=["GOOGL"],
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 31),
        )

        svc.setup({"backtest_id": "bt1", "strategy_ids": [], "input_manifest": manifest_a})
        assert svc._input_manifest is manifest_a  # precondition

        # Act — second run with a different manifest
        svc.setup({"backtest_id": "bt2", "strategy_ids": [], "input_manifest": manifest_b})

        # Assert
        assert svc._input_manifest is manifest_b


class TestReportingServiceManifestPersistence:
    """_write_to_database forwards the stored manifest to DuckDBWriter.save_run()."""

    def _build_svc(self, tmp_path: Path) -> ReportingService:
        """Build a ReportingService ready for _write_to_database calls."""

        from qs_trader.events.event_bus import EventBus
        from qs_trader.services.reporting.config import ReportingConfig
        from qs_trader.services.reporting.service import ReportingService

        config = ReportingConfig(
            write_parquet=False,
            write_json=False,
            display_final_report=False,
        )
        output_dir = tmp_path / "experiments" / "exp1" / "runs" / "20260101_120000"
        output_dir.mkdir(parents=True, exist_ok=True)

        svc = ReportingService(event_bus=EventBus(), config=config, output_dir=output_dir)
        svc._backtest_id = "test_bt"
        return svc

    def _make_system_config_mock(self, *, db_enabled: bool, db_path: str) -> MagicMock:
        mock = MagicMock()
        mock.output.database.enabled = db_enabled
        mock.output.database.path = db_path
        mock.config_root = Path.cwd()
        return mock

    def _minimal_metrics(self):
        from decimal import Decimal

        from qs_trader.libraries.performance.models import FullMetrics

        return FullMetrics.model_construct(
            backtest_id="test_bt",
            start_date="2023-01-01",
            end_date="2023-12-31",
            duration_days=365,
            initial_equity=Decimal("100000"),
            final_equity=Decimal("110000"),
            total_return_pct=Decimal("10.00"),
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

    def test_manifest_forwarded_to_save_run_when_set(self, tmp_path) -> None:
        """_write_to_database must pass the stored manifest to DuckDBWriter.save_run()."""
        from unittest.mock import patch

        # Arrange
        svc = self._build_svc(tmp_path)
        manifest = _sample_manifest()
        svc._input_manifest = manifest
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "runs.duckdb")
        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        call_kwargs = mock_writer_cls.return_value.save_run.call_args.kwargs
        assert call_kwargs.get("manifest") is manifest

    def test_manifest_none_passed_when_not_set(self, tmp_path) -> None:
        """When no manifest was stored, save_run() must receive manifest=None."""
        from unittest.mock import patch

        # Arrange
        svc = self._build_svc(tmp_path)
        # _input_manifest defaults to None
        assert svc._input_manifest is None
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "runs.duckdb")
        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        call_kwargs = mock_writer_cls.return_value.save_run.call_args.kwargs
        assert call_kwargs.get("manifest") is None

    def test_manifest_round_trips_through_duckdb(self, tmp_path) -> None:
        """Full integration: manifest set on service must survive a DuckDB write/read cycle."""
        import duckdb

        # Arrange
        svc = self._build_svc(tmp_path)
        manifest = _sample_manifest()
        svc._input_manifest = manifest
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = tmp_path / "round_trip.duckdb"
        db_path_str = str(db_path)
        from unittest.mock import patch

        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path_str)
        sys_config.config_root = tmp_path  # ensure absolute path resolution

        # Use real DuckDBWriter — no mocking
        with patch("qs_trader.system.config.get_system_config", return_value=sys_config):
            svc._write_outputs(metrics)

        # Read back from DuckDB
        con = duckdb.connect(db_path_str, read_only=True)
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'exp1'").fetchone()
        con.close()

        assert row is not None
        assert row[0] is not None
        recovered = ClickHouseInputManifest.from_json(row[0])
        assert recovered.source_name == "qs-datamaster-equity-1d"
        assert recovered.symbols == ["AAPL", "MSFT"]
        assert recovered.adjustment_mode == "total_return"
        assert recovered.feature_set_version == "v1"

    def test_run_persistence_unaffected_by_manifest(self, tmp_path) -> None:
        """Existing run outputs (metrics) must not be disturbed when a manifest is stored."""
        from unittest.mock import patch

        # Arrange
        svc = self._build_svc(tmp_path)
        svc._input_manifest = _sample_manifest()
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "runs.duckdb")
        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path)

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        # save_run must be called exactly once — same as before Phase 2
        mock_writer_cls.return_value.save_run.assert_called_once()
        # The non-manifest kwargs must all still be present
        call_kwargs = mock_writer_cls.return_value.save_run.call_args.kwargs
        assert "experiment_id" in call_kwargs
        assert "run_id" in call_kwargs
        assert "metrics" in call_kwargs


# ---------------------------------------------------------------------------
# Phase 3 — canonical_input_policy enforcement tests
# ---------------------------------------------------------------------------


class TestReportingServiceCanonicalPolicy:
    """_write_to_database respects canonical_input_policy for bar-snapshot gating.

    Policy contract:
      * ``reference`` + canonical run (manifest set) → skip bars_with_features
      * ``snapshot``  + canonical run (manifest set) → write bars_with_features
      * ``reference`` + non-canonical run (no manifest) → write bars_with_features
        (non-ClickHouse runs are unaffected by policy; bar rows are written when present)
      * No bar rows buffered → save_bars_with_features never called (policy-agnostic)
    """

    # ------------------------------------------------------------------
    # Reuse helpers from TestReportingServiceManifestPersistence
    # ------------------------------------------------------------------

    def _build_svc(self, tmp_path: Path) -> "ReportingService":
        from qs_trader.events.event_bus import EventBus
        from qs_trader.services.reporting.config import ReportingConfig
        from qs_trader.services.reporting.service import ReportingService

        config = ReportingConfig(
            write_parquet=False,
            write_json=False,
            display_final_report=False,
        )
        output_dir = tmp_path / "experiments" / "exp1" / "runs" / "20260101_120000"
        output_dir.mkdir(parents=True, exist_ok=True)

        svc = ReportingService(event_bus=EventBus(), config=config, output_dir=output_dir)
        svc._backtest_id = "test_bt"
        return svc

    def _make_system_config_mock(
        self,
        *,
        db_enabled: bool,
        db_path: str,
        canonical_input_policy: str = "reference",
    ) -> MagicMock:
        mock = MagicMock()
        mock.output.database.enabled = db_enabled
        mock.output.database.path = db_path
        mock.output.database.canonical_input_policy = canonical_input_policy
        mock.config_root = Path.cwd()
        return mock

    def _minimal_metrics(self):
        from decimal import Decimal

        from qs_trader.libraries.performance.models import FullMetrics

        return FullMetrics.model_construct(
            backtest_id="test_bt",
            start_date="2023-01-01",
            end_date="2023-12-31",
            duration_days=365,
            initial_equity=Decimal("100000"),
            final_equity=Decimal("110000"),
            total_return_pct=Decimal("10.00"),
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

    def _fake_bar_rows(self) -> dict:
        """Produce a minimal _bar_rows dict to simulate feature-enabled buffering."""
        return {
            ("AAPL", "2023-01-03T21:00:00+00:00"): {
                "timestamp": "2023-01-03T21:00:00+00:00",
                "symbol": "AAPL",
                "open": 130.0,
                "high": 133.0,
                "low": 129.0,
                "close": 131.0,
                "open_adj": None,
                "high_adj": None,
                "low_adj": None,
                "close_adj": None,
                "volume": 50_000,
                "features": {"momentum": 0.4},
                "feature_set_version": "v1",
            }
        }

    # ------------------------------------------------------------------
    # Reference policy + canonical run → bars_with_features SKIPPED
    # ------------------------------------------------------------------

    def test_reference_policy_canonical_run_skips_bars(self, tmp_path: Path) -> None:
        """With policy='reference' and a manifest present, bars_with_features must NOT
        be written even when bar rows were buffered."""
        from unittest.mock import patch

        svc = self._build_svc(tmp_path)
        svc._input_manifest = _sample_manifest()
        svc._bar_rows = self._fake_bar_rows()
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "runs.duckdb")
        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path, canonical_input_policy="reference")

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_writer_cls.return_value.save_bars_with_features.assert_not_called()

    def test_reference_policy_canonical_run_still_writes_run_summary(self, tmp_path: Path) -> None:
        """Policy='reference' must NOT suppress the run summary (save_run still called)."""
        from unittest.mock import patch

        svc = self._build_svc(tmp_path)
        svc._input_manifest = _sample_manifest()
        svc._bar_rows = self._fake_bar_rows()
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "runs.duckdb")
        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path, canonical_input_policy="reference")

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_writer_cls.return_value.save_run.assert_called_once()

    # ------------------------------------------------------------------
    # Snapshot policy + canonical run → bars_with_features WRITTEN
    # ------------------------------------------------------------------

    def test_snapshot_policy_canonical_run_writes_bars(self, tmp_path: Path) -> None:
        """With policy='snapshot', bars_with_features must be written regardless of
        whether a manifest is present."""
        from unittest.mock import patch

        svc = self._build_svc(tmp_path)
        svc._input_manifest = _sample_manifest()
        svc._bar_rows = self._fake_bar_rows()
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "runs.duckdb")
        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path, canonical_input_policy="snapshot")

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_writer_cls.return_value.save_bars_with_features.assert_called_once()

    # ------------------------------------------------------------------
    # Reference policy + non-canonical run → bars_with_features WRITTEN
    # (Yahoo/CSV: no manifest; policy gate does not apply)
    # ------------------------------------------------------------------

    def test_reference_policy_non_canonical_run_writes_bars(self, tmp_path: Path) -> None:
        """Non-canonical runs (no manifest) must still write bar rows to DuckDB
        even under the 'reference' policy — only canonical runs are gated."""
        from unittest.mock import patch

        svc = self._build_svc(tmp_path)
        svc._input_manifest = None  # Yahoo/CSV — no manifest
        svc._bar_rows = self._fake_bar_rows()
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "runs.duckdb")
        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path, canonical_input_policy="reference")

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_writer_cls.return_value.save_bars_with_features.assert_called_once()

    # ------------------------------------------------------------------
    # No bar rows buffered → bars_with_features never called (any policy)
    # ------------------------------------------------------------------

    def test_no_bar_rows_never_calls_save_bars(self, tmp_path: Path) -> None:
        """When no bar rows were buffered, save_bars_with_features must not be called
        regardless of policy or manifest presence."""
        from unittest.mock import patch

        svc = self._build_svc(tmp_path)
        svc._input_manifest = _sample_manifest()
        svc._bar_rows = {}  # nothing buffered
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = str(tmp_path / "runs.duckdb")
        sys_config = self._make_system_config_mock(db_enabled=True, db_path=db_path, canonical_input_policy="snapshot")

        with (
            patch("qs_trader.system.config.get_system_config", return_value=sys_config),
            patch("qs_trader.services.reporting.db_writer.DuckDBWriter") as mock_writer_cls,
        ):
            svc._write_outputs(metrics)

        mock_writer_cls.return_value.save_bars_with_features.assert_not_called()

    # ------------------------------------------------------------------
    # Integration: round-trip with real DuckDB under reference policy
    # ------------------------------------------------------------------

    def test_reference_policy_manifest_round_trips_duckdb(self, tmp_path: Path) -> None:
        """Full integration: under reference policy, manifest is persisted to DuckDB
        and bars_with_features is absent from the written data."""
        from unittest.mock import patch

        import duckdb

        svc = self._build_svc(tmp_path)
        svc._input_manifest = _sample_manifest()
        svc._bar_rows = self._fake_bar_rows()  # would be written under snapshot
        metrics = self._minimal_metrics()
        svc._returns_calc = MagicMock()
        svc._returns_calc.returns = []
        svc._equity_calc = MagicMock()
        svc._equity_calc.get_curve.return_value = []
        svc._last_portfolio_state = None

        db_path = tmp_path / "policy_test.duckdb"
        db_path_str = str(db_path)

        sys_config = self._make_system_config_mock(
            db_enabled=True, db_path=db_path_str, canonical_input_policy="reference"
        )
        sys_config.config_root = tmp_path

        with patch("qs_trader.system.config.get_system_config", return_value=sys_config):
            svc._write_outputs(metrics)

        con = duckdb.connect(db_path_str, read_only=True)

        # Manifest must be persisted
        row = con.execute("SELECT input_manifest_json FROM runs WHERE experiment_id = 'exp1'").fetchone()
        assert row is not None and row[0] is not None

        # bars_with_features table may exist (schema is initialised eagerly) but
        # must contain no rows for this run — the reference policy skips insertion.
        bar_count = con.execute(
            "SELECT COUNT(*) FROM bars_with_features WHERE experiment_id = 'exp1' AND run_id = '20260101_120000'"
        ).fetchone()[0]
        assert bar_count == 0

        con.close()
