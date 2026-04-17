"""Focused PostgreSQL-era reporting tests."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.libraries.performance.models import FullMetrics
from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.postgres_writer import PostgreSQLWriter
from qs_trader.services.reporting.service import ReportingService
from qs_trader.services.reporting.writer_factory import (
    WriterConfigurationError,
    _create_postgres_writer,
    _build_postgres_url_from_env,
)
from qs_trader.system.config import DatabaseOutputConfig


def _make_system_config_mock(
    *,
    db_enabled: bool,
    artifact_mode: str = "filesystem",
    postgres_url: str | None = None,
):
    mock = MagicMock()
    mock.output.database.enabled = db_enabled
    mock.output.database.backend = "postgres"
    mock.output.database.postgres_url = postgres_url
    mock.output.artifact_policy.mode = artifact_mode
    mock.config_root = Path.cwd()
    return mock


def _minimal_metrics() -> FullMetrics:
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


def _build_reporting_service(tmp_path: Path) -> ReportingService:
    config = ReportingConfig(
        write_parquet=False,
        write_json=False,
        write_html_report=False,
        write_csv_timeline=False,
        display_final_report=False,
    )
    output_dir = tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000"
    output_dir.mkdir(parents=True, exist_ok=True)
    service = ReportingService(event_bus=EventBus(), config=config, output_dir=output_dir)
    service._backtest_id = "test_exp"
    service._returns_calc = MagicMock()
    service._returns_calc.returns = []
    service._equity_calc = MagicMock()
    service._equity_calc.latest_timestamp.return_value = datetime(2024, 1, 31, tzinfo=timezone.utc)
    service._equity_calc.latest_equity.return_value = Decimal("105000")
    service._equity_calc.get_curve.return_value = []
    service._last_portfolio_state = None
    return service


def test_database_disabled_skips_persistence_writer(tmp_path: Path) -> None:
    """No writer should be created when database persistence is disabled."""
    service = _build_reporting_service(tmp_path)

    with (
        patch("qs_trader.system.config.get_system_config", return_value=_make_system_config_mock(db_enabled=False)),
        patch("qs_trader.services.reporting.writer_factory.create_persistence_writer") as mock_factory,
    ):
        service._write_outputs(_minimal_metrics())

    mock_factory.assert_not_called()
    assert service.get_database_write_status().state == "disabled"


def test_database_enabled_uses_postgres_writer_with_database_only_metadata(tmp_path: Path) -> None:
    """database_only service runs should persist via the writer factory."""
    service = _build_reporting_service(tmp_path)
    service._job_group_id = "g-001"
    service._submission_source = "research_api"
    mock_writer = MagicMock()

    with (
        patch(
            "qs_trader.system.config.get_system_config",
            return_value=_make_system_config_mock(
                db_enabled=True,
                artifact_mode="database_only",
                postgres_url="postgresql+psycopg://research:secret@localhost:5432/research",
            ),
        ),
        patch(
            "qs_trader.services.reporting.writer_factory.create_persistence_writer",
            return_value=mock_writer,
        ),
    ):
        service._write_outputs(_minimal_metrics())

    mock_writer.save_run.assert_called_once()
    mock_writer.close.assert_called_once()
    kwargs = mock_writer.save_run.call_args.kwargs
    assert kwargs["artifact_mode"] == "database_only"
    assert kwargs["job_group_id"] == "g-001"
    assert kwargs["submission_source"] == "research_api"
    assert service.get_database_write_status().state == "succeeded"


def test_database_failure_is_captured_without_raising(tmp_path: Path) -> None:
    """Writer creation/runtime failures should be recorded, not raised."""
    service = _build_reporting_service(tmp_path)

    with (
        patch(
            "qs_trader.system.config.get_system_config",
            return_value=_make_system_config_mock(db_enabled=True),
        ),
        patch(
            "qs_trader.services.reporting.writer_factory.create_persistence_writer",
            side_effect=WriterConfigurationError("missing postgres url"),
        ),
    ):
        service._write_outputs(_minimal_metrics())

    status = service.get_database_write_status()
    assert status.state == "failed"
    assert status.reason == "missing postgres url"


def test_database_only_mode_skips_filesystem_artifacts(tmp_path: Path) -> None:
    """database_only mode should avoid filesystem outputs even when reporting is enabled."""
    output_dir = tmp_path / "database_only_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    service = ReportingService(
        event_bus=EventBus(),
        config=ReportingConfig(
            write_parquet=True,
            write_json=True,
            write_html_report=True,
            display_final_report=False,
        ),
        output_dir=output_dir,
    )
    service._backtest_id = "database_only_service_run"
    service._start_datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)
    service._initial_equity = Decimal("100000")
    service._returns_calc = MagicMock()
    service._returns_calc.returns = []
    service._equity_calc = MagicMock()
    service._equity_calc.latest_timestamp.return_value = datetime(2024, 1, 31, tzinfo=timezone.utc)
    service._equity_calc.latest_equity.return_value = Decimal("105000")
    service._equity_calc.get_curve.return_value = []
    service._drawdown_calc = MagicMock()
    service._drawdown_calc.max_drawdown_pct = Decimal("0")
    service._drawdown_calc.current_drawdown_pct = Decimal("0")
    service._drawdown_calc.drawdown_periods = []
    service._trade_stats_calc = MagicMock()
    service._trade_stats_calc.trades = []
    service._trade_stats_calc.max_consecutive_wins = 0
    service._trade_stats_calc.max_consecutive_losses = 0
    service._period_calc = MagicMock()
    service._period_calc.calculate_periods.return_value = []
    service._strategy_perf_calc = MagicMock()
    service._strategy_perf_calc.calculate_performance.return_value = []
    mock_writer = MagicMock()

    with (
        patch(
            "qs_trader.system.config.get_system_config",
            return_value=_make_system_config_mock(
                db_enabled=True,
                artifact_mode="database_only",
                postgres_url="postgresql+psycopg://research:secret@localhost:5432/research",
            ),
        ),
        patch(
            "qs_trader.services.reporting.writer_factory.create_persistence_writer",
            return_value=mock_writer,
        ),
        patch("qs_trader.services.reporting.service.write_backtest_metadata"),
    ):
        service.teardown({})

    assert not (output_dir / "performance.json").exists()
    assert not (output_dir / "timeseries").exists()
    assert not (output_dir / "metadata.json").exists()


def test_env_postgres_url_builder_encodes_reserved_characters(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Environment-derived PostgreSQL URLs must percent-encode credentials."""
    monkeypatch.setenv("RESEARCH_POSTGRES_HOST", "localhost")
    monkeypatch.setenv("RESEARCH_POSTGRES_PORT", "5432")
    monkeypatch.setenv("RESEARCH_POSTGRES_DB", "research")
    monkeypatch.setenv("RESEARCH_POSTGRES_USER", "research")
    monkeypatch.setenv("RESEARCH_POSTGRES_PASSWORD", "s3cr/et:with@chars")
    monkeypatch.setenv("RESEARCH_POSTGRES_SSLMODE", "disable")

    assert _build_postgres_url_from_env() == (
        "postgresql+psycopg://research:s3cr%2Fet%3Awith%40chars"
        "@localhost:5432/research?sslmode=disable"
    )


def test_postgres_writer_factory_rejects_unexpanded_placeholders() -> None:
    """Connection URLs must not retain unresolved ${...} placeholders."""
    db_config = DatabaseOutputConfig(
        enabled=True,
        backend="postgres",
        postgres_url=(
            "postgresql+psycopg://research:${RESEARCH_POSTGRES_PASSWORD}"
            "@localhost:5432/research"
        ),
    )

    with pytest.raises(
        WriterConfigurationError,
        match=r"\$\{RESEARCH_POSTGRES_PASSWORD\}",
    ):
        _create_postgres_writer(db_config)


def test_postgresql_writer_batches_bulk_series_inserts() -> None:
    """Time-series/report rows should be sent via one executemany call per table."""
    writer = object.__new__(PostgreSQLWriter)
    timestamp = datetime(2024, 1, 2, tzinfo=timezone.utc)
    next_timestamp = datetime(2024, 1, 3, tzinfo=timezone.utc)
    scenarios = [
        (
            "_insert_equity_curve",
            [
                SimpleNamespace(
                    timestamp=timestamp,
                    equity=Decimal("101000"),
                    cash=Decimal("10000"),
                    positions_value=Decimal("91000"),
                    num_positions=1,
                    gross_exposure=Decimal("0.91"),
                    net_exposure=Decimal("0.91"),
                    leverage=Decimal("1.0"),
                    drawdown_pct=Decimal("0"),
                    underwater=False,
                ),
                SimpleNamespace(
                    timestamp=next_timestamp,
                    equity=Decimal("102000"),
                    cash=Decimal("11000"),
                    positions_value=Decimal("91000"),
                    num_positions=1,
                    gross_exposure=Decimal("0.90"),
                    net_exposure=Decimal("0.90"),
                    leverage=Decimal("1.0"),
                    drawdown_pct=Decimal("0"),
                    underwater=False,
                ),
            ],
        ),
        (
            "_insert_returns",
            [
                SimpleNamespace(
                    timestamp=timestamp,
                    period_return=Decimal("0.01"),
                    cumulative_return=Decimal("0.01"),
                    log_return=Decimal("0.00995"),
                ),
                SimpleNamespace(
                    timestamp=next_timestamp,
                    period_return=Decimal("0.02"),
                    cumulative_return=Decimal("0.03"),
                    log_return=Decimal("0.01980"),
                ),
            ],
        ),
        (
            "_insert_trades",
            [
                SimpleNamespace(
                    trade_id="trade-001",
                    strategy_id="sma_crossover",
                    symbol="AAPL",
                    entry_timestamp=timestamp,
                    exit_timestamp=timestamp,
                    entry_price=Decimal("100.50"),
                    exit_price=Decimal("104.25"),
                    quantity=10,
                    side="long",
                    pnl=Decimal("37.50"),
                    pnl_pct=Decimal("3.73"),
                    commission=Decimal("1.00"),
                    duration_seconds=60,
                    status="closed",
                ),
                SimpleNamespace(
                    trade_id="trade-002",
                    strategy_id="sma_crossover",
                    symbol="MSFT",
                    entry_timestamp=timestamp,
                    exit_timestamp=timestamp,
                    entry_price=Decimal("200.00"),
                    exit_price=Decimal("203.00"),
                    quantity=5,
                    side="long",
                    pnl=Decimal("15.00"),
                    pnl_pct=Decimal("1.50"),
                    commission=Decimal("0.50"),
                    duration_seconds=120,
                    status="open",
                ),
            ],
        ),
        (
            "_insert_drawdowns",
            [
                SimpleNamespace(
                    drawdown_id=1,
                    start_timestamp=timestamp,
                    trough_timestamp=timestamp,
                    end_timestamp=timestamp,
                    peak_equity=Decimal("110000"),
                    trough_equity=Decimal("100000"),
                    depth_pct=Decimal("-9.09"),
                    duration_days=5,
                    recovery_days=2,
                    recovered=True,
                ),
                SimpleNamespace(
                    drawdown_id=2,
                    start_timestamp=timestamp,
                    trough_timestamp=timestamp,
                    end_timestamp=timestamp,
                    peak_equity=Decimal("120000"),
                    trough_equity=Decimal("108000"),
                    depth_pct=Decimal("-10.00"),
                    duration_days=7,
                    recovery_days=3,
                    recovered=True,
                ),
            ],
        ),
    ]

    for method_name, records in scenarios:
        conn = MagicMock()

        getattr(writer, method_name)(conn, "exp", "run", records)

        conn.execute.assert_called_once()
        params = conn.execute.call_args.args[1]
        assert isinstance(params, list)
        assert len(params) == len(records)
        assert {row["experiment_id"] for row in params} == {"exp"}
        assert {row["run_id"] for row in params} == {"run"}


def test_postgresql_writer_deduplicates_equity_curve_timestamps() -> None:
    """Equity curve inserts should keep the last point for duplicate timestamps."""
    writer = object.__new__(PostgreSQLWriter)
    timestamp = datetime(2024, 1, 2, tzinfo=timezone.utc)
    conn = MagicMock()

    getattr(writer, "_insert_equity_curve")(
        conn,
        "exp",
        "run",
        [
            SimpleNamespace(
                timestamp=timestamp,
                equity=Decimal("101000"),
                cash=Decimal("10000"),
                positions_value=Decimal("91000"),
                num_positions=1,
                gross_exposure=Decimal("0.91"),
                net_exposure=Decimal("0.91"),
                leverage=Decimal("1.0"),
                drawdown_pct=Decimal("0"),
                underwater=False,
            ),
            SimpleNamespace(
                timestamp=timestamp,
                equity=Decimal("102000"),
                cash=Decimal("11000"),
                positions_value=Decimal("91000"),
                num_positions=2,
                gross_exposure=Decimal("0.95"),
                net_exposure=Decimal("0.93"),
                leverage=Decimal("1.1"),
                drawdown_pct=Decimal("1.5"),
                underwater=True,
            ),
        ],
    )

    conn.execute.assert_called_once()
    params = conn.execute.call_args.args[1]

    assert isinstance(params, list)
    assert len(params) == 1
    assert params[0]["timestamp"] == timestamp
    assert params[0]["equity"] == 102000.0
    assert params[0]["num_positions"] == 2
    assert params[0]["underwater"] is True


def test_postgresql_writer_deduplicates_return_timestamps() -> None:
    """Return inserts should keep the last point for duplicate timestamps."""
    writer = object.__new__(PostgreSQLWriter)
    timestamp = datetime(2024, 1, 2, tzinfo=timezone.utc)
    conn = MagicMock()

    getattr(writer, "_insert_returns")(
        conn,
        "exp",
        "run",
        [
            SimpleNamespace(
                timestamp=timestamp,
                period_return=Decimal("0.01"),
                cumulative_return=Decimal("0.01"),
                log_return=Decimal("0.00995"),
            ),
            SimpleNamespace(
                timestamp=timestamp,
                period_return=Decimal("0.02"),
                cumulative_return=Decimal("0.03"),
                log_return=Decimal("0.01980"),
            ),
        ],
    )

    conn.execute.assert_called_once()
    params = conn.execute.call_args.args[1]

    assert isinstance(params, list)
    assert len(params) == 1
    assert params[0]["timestamp"] == timestamp
    assert params[0]["period_return"] == 0.02
    assert params[0]["cumulative_return"] == 0.03
    assert params[0]["log_return"] == 0.0198


def test_postgresql_writer_close_disposes_engine() -> None:
    """Writer.close should dispose the owned SQLAlchemy engine."""
    writer = object.__new__(PostgreSQLWriter)
    writer._engine = MagicMock()

    writer.close()

    writer._engine.dispose.assert_called_once()


def test_insert_run_maps_open_trade_fields_to_correct_params() -> None:
    """_insert_run must pass open_trades, realized_pnl, unrealized_pnl to the correct
    parameter slots — a transposition of the two P&L fields would be undetected otherwise."""
    writer = object.__new__(PostgreSQLWriter)
    conn = MagicMock()

    metrics = _minimal_metrics()
    metrics = metrics.model_copy(
        update={
            "open_trades": 2,
            "realized_pnl": Decimal("1000.00"),
            "unrealized_pnl": Decimal("500.00"),
        }
    )

    writer._insert_run(conn, "exp", "run-001", metrics)

    conn.execute.assert_called_once()
    params = conn.execute.call_args.args[1]

    assert params["open_trades"] == 2
    assert params["realized_pnl"] == 1000.0
    assert params["unrealized_pnl"] == 500.0