"""Focused PostgreSQL-era reporting tests."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import (
    FeatureBarEvent,
    FillEvent,
    IndicatorEvent,
    OrderEvent,
    PriceBarEvent,
    SignalEvent,
    TradeEvent,
)
from qs_trader.libraries.performance.models import FullMetrics
from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.event_collector import collect_run_events
from qs_trader.services.reporting.manifest import ClickHouseInputManifest
from qs_trader.services.reporting.postgres_writer import PostgreSQLWriter
from qs_trader.services.reporting.service import ReportingService
from qs_trader.services.reporting.writer_factory import (
    WriterConfigurationError,
    _build_postgres_url_from_env,
    _create_postgres_writer,
)
from qs_trader.system.config import DatabaseOutputConfig


def _make_system_config_mock(
    *,
    db_enabled: bool,
    artifact_mode: str = "filesystem",
    postgres_url: str | None = None,
    config_root: Path | None = None,
):
    mock = MagicMock()
    mock.output.database.enabled = db_enabled
    mock.output.database.backend = "postgres"
    mock.output.database.postgres_url = postgres_url
    mock.output.artifact_policy.mode = artifact_mode
    mock.config_root = config_root or Path.cwd()
    return mock


def _minimal_manifest() -> ClickHouseInputManifest:
    return ClickHouseInputManifest.model_validate(
        {
            "source_name": "qs-datamaster-equity-1d",
            "database": "market",
            "bars_table": "as_us_equity_ohlc_daily",
            "symbols": ("AAPL",),
            "start_date": date(2024, 1, 2),
            "end_date": date(2024, 1, 2),
            "strategy_adjustment_mode": "split_adjusted",
            "portfolio_adjustment_mode": "split_adjusted",
        }
    )


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


def _build_run_event_store() -> InMemoryEventStore:
    store = InMemoryEventStore()
    timestamp = "2024-01-02T00:00:00Z"
    fill_one_id = "550e8400-e29b-41d4-a716-446655440011"
    fill_two_id = "550e8400-e29b-41d4-a716-446655440012"

    store.append(
        PriceBarEvent(
            symbol="AAPL",
            timestamp=timestamp,
            interval="1d",
            open=Decimal("100.00"),
            high=Decimal("101.00"),
            low=Decimal("99.50"),
            close=Decimal("100.75"),
            volume=1000,
            source="unit_test",
        )
    )
    store.append(
        FeatureBarEvent(
            timestamp=timestamp,
            symbol="AAPL",
            features={
                "feat_alpha": Decimal("1.25"),
                "regime": "bull",
                "nan_feature": float("nan"),
            },
        )
    )
    store.append(
        IndicatorEvent(
            strategy_id="sma_crossover",
            symbol="AAPL",
            timestamp=timestamp,
            indicators={
                "SMA(10)": Decimal("101.50"),
                "is_bullish": True,
                "comment": "skip-me",
            },
        )
    )
    store.append(
        SignalEvent(
            signal_id="signal-550e8400-e29b-41d4-a716-446655440001",
            timestamp=timestamp,
            strategy_id="sma_crossover",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("100.50"),
            confidence=Decimal("0.85"),
            reason="golden cross",
        )
    )
    store.append(
        OrderEvent(
            intent_id="signal-550e8400-e29b-41d4-a716-446655440001",
            idempotency_key="order-key-1",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("10"),
            order_type="market",
            source_strategy_id="sma_crossover",
        )
    )
    store.append(
        OrderEvent(
            intent_id="signal-550e8400-e29b-41d4-a716-446655440001",
            idempotency_key="order-key-2",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("5"),
            order_type="limit",
            limit_price=Decimal("100.40"),
            source_strategy_id="sma_crossover",
        )
    )
    store.append(
        FillEvent(
            fill_id=fill_one_id,
            source_order_id="order-001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            filled_quantity=Decimal("10"),
            fill_price=Decimal("100.60"),
            commission=Decimal("1.25"),
            slippage_bps=4,
            strategy_id="sma_crossover",
        )
    )
    store.append(
        FillEvent(
            fill_id=fill_two_id,
            source_order_id="order-002",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            filled_quantity=Decimal("5"),
            fill_price=Decimal("100.80"),
            commission=Decimal("0.75"),
            slippage_bps=2,
            strategy_id="sma_crossover",
        )
    )
    store.append(
        TradeEvent(
            trade_id="T00001",
            timestamp=timestamp,
            strategy_id="sma_crossover",
            symbol="AAPL",
            status="closed",
            side="long",
            fills=[fill_one_id, fill_two_id],
            entry_price=Decimal("100.67"),
            exit_price=Decimal("104.00"),
            current_quantity=Decimal("0"),
            realized_pnl=Decimal("50.00"),
            commission_total=Decimal("2.00"),
            entry_timestamp=timestamp,
            exit_timestamp="2024-01-10T00:00:00Z",
        )
    )

    return store


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
    service._effective_execution_spec = {"schema_version": 1, "captured_from": "qs_trader.reporting"}
    service._event_store = _build_run_event_store()
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
    assert kwargs["effective_execution_spec"] == {
        "schema_version": 1,
        "captured_from": "qs_trader.reporting",
    }
    assert kwargs["event_store"] is service._event_store
    assert service.get_database_write_status().state == "succeeded"


def test_write_metadata_threads_effective_execution_spec(tmp_path: Path) -> None:
    """metadata.json writes should include immutable runtime provenance when available."""
    service = _build_reporting_service(tmp_path)
    service._effective_execution_spec = {
        "schema_version": 1,
        "captured_from": "qs_trader.reporting",
    }
    backtest_config = MagicMock()
    backtest_config.model_dump.return_value = {"backtest_id": "test-backtest"}
    system_config = SimpleNamespace(
        data=SimpleNamespace(
            sources_config="config/data_sources.yaml",
            default_timezone="UTC",
            price_decimals=2,
            validate_on_load=True,
        ),
        output=SimpleNamespace(
            experiments_root="output/backtests",
            run_id_format="%Y%m%d_%H%M%S",
            display_format="tree",
        ),
        logging=SimpleNamespace(
            level="INFO",
            format="console",
            timestamp_format="iso",
            enable_file=False,
            file_path=None,
        ),
    )

    with (
        patch("qs_trader.system.config.get_system_config", return_value=system_config),
        patch("qs_trader.services.reporting.service.write_backtest_metadata") as mock_write,
    ):
        service._write_metadata({"backtest_config": backtest_config})

    mock_write.assert_called_once()
    assert mock_write.call_args.kwargs["effective_execution_spec"] == {
        "schema_version": 1,
        "captured_from": "qs_trader.reporting",
    }


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
        "postgresql+psycopg://research:s3cr%2Fet%3Awith%40chars@localhost:5432/research?sslmode=disable"
    )


def test_postgres_writer_factory_rejects_unexpanded_placeholders(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Connection URLs must not retain unresolved ${...} placeholders."""
    # Unset all RESEARCH_POSTGRES_* vars so the env-var URL builder returns None
    # and the test exercises the db_config.postgres_url validation path.
    for var in (
        "RESEARCH_POSTGRES_HOST",
        "RESEARCH_POSTGRES_PORT",
        "RESEARCH_POSTGRES_DB",
        "RESEARCH_POSTGRES_USER",
        "RESEARCH_POSTGRES_PASSWORD",
        "RESEARCH_POSTGRES_SSLMODE",
    ):
        monkeypatch.delenv(var, raising=False)

    db_config = DatabaseOutputConfig(
        enabled=True,
        backend="postgres",
        postgres_url=("postgresql+psycopg://research:${RESEARCH_POSTGRES_PASSWORD}@localhost:5432/research"),
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


def test_insert_run_serializes_effective_execution_spec_json() -> None:
    """_insert_run should pass the effective execution artifact to the JSONB column."""
    writer = object.__new__(PostgreSQLWriter)
    conn = MagicMock()

    writer._insert_run(
        conn,
        "exp",
        "run-001",
        _minimal_metrics(),
        effective_execution_spec={
            "schema_version": 1,
            "captured_from": "qs_trader.reporting",
            "strategies": [
                {
                    "strategy_id": "sma_crossover",
                    "effective_params": {"fast_period": 10, "slow_period": 50},
                }
            ],
        },
    )

    conn.execute.assert_called_once()
    params = conn.execute.call_args.args[1]

    assert json.loads(params["effective_execution_spec_json"]) == {
        "schema_version": 1,
        "captured_from": "qs_trader.reporting",
        "strategies": [
            {
                "strategy_id": "sma_crossover",
                "effective_params": {"fast_period": 10, "slow_period": 50},
            }
        ],
    }


def test_collect_run_events_aggregates_event_store_payloads() -> None:
    """Run-event collection should merge per-bar signal, order, fill, trade, feature, and indicator data."""
    rows = collect_run_events("exp", "run-001", _build_run_event_store())

    assert len(rows) == 1
    row = rows[0]

    assert row["experiment_id"] == "exp"
    assert row["run_id"] == "run-001"
    assert row["timestamp"] == datetime(2024, 1, 2, tzinfo=timezone.utc)
    assert row["symbol"] == "AAPL"
    assert row["strategy_id"] == "sma_crossover"
    assert row["signal_intention"] == "OPEN_LONG"
    assert row["signal_price"] == 100.5
    assert row["signal_confidence"] == 0.85
    assert row["signal_reason"] == "golden cross"
    assert row["order_side"] == "BUY"
    assert row["order_type"] == "MARKET,LIMIT"
    assert row["order_qty"] == 15
    assert row["fill_qty"] == 15
    assert row["fill_price"] == pytest.approx((10 * 100.60 + 5 * 100.80) / 15)
    assert row["fill_slippage_bps"] == pytest.approx((10 * 4 + 5 * 2) / 15)
    assert row["commission"] == 2.0
    assert row["trade_id"] == "T00001"
    assert row["trade_status"] == "CLOSED"
    assert row["trade_side"] == "LONG"
    assert row["trade_entry_price"] == 100.67
    assert row["trade_exit_price"] == 104.0
    assert row["trade_realized_pnl"] == 50.0
    assert json.loads(row["indicators_json"]) == {
        "SMA(10)": 101.5,
        "is_bullish": 1.0,
    }
    assert json.loads(row["features_json"]) == {"feat_alpha": 1.25}


def test_insert_run_events_batches_parameterized_rows() -> None:
    """run_events inserts should use one executemany-style call with collected row dicts."""
    writer = object.__new__(PostgreSQLWriter)
    conn = MagicMock()

    inserted = writer._insert_run_events(conn, "exp", "run-001", _build_run_event_store())

    assert inserted == 1
    conn.execute.assert_called_once()
    statement, params = conn.execute.call_args.args
    assert "INSERT INTO run_events" in str(statement)
    assert isinstance(params, list)
    assert len(params) == 1
    assert params[0]["experiment_id"] == "exp"
    assert params[0]["run_id"] == "run-001"
    assert json.loads(params[0]["indicators_json"]) == {
        "SMA(10)": 101.5,
        "is_bullish": 1.0,
    }
    assert json.loads(params[0]["features_json"]) == {"feat_alpha": 1.25}


def test_save_run_remains_backward_compatible_without_event_store() -> None:
    """save_run should keep the existing write flow when callers omit event_store."""
    writer = cast(Any, object.__new__(PostgreSQLWriter))
    conn = MagicMock()
    begin_context = MagicMock()
    begin_context.__enter__.return_value = conn
    begin_context.__exit__.return_value = False
    writer._engine = MagicMock()
    writer._engine.begin.return_value = begin_context

    writer._delete_existing_run = MagicMock()
    writer._insert_run = MagicMock()
    writer._insert_equity_curve = MagicMock()
    writer._insert_returns = MagicMock()
    writer._insert_trades = MagicMock()
    writer._insert_drawdowns = MagicMock()
    writer._insert_run_events = MagicMock(return_value=0)

    writer.save_run(
        experiment_id="exp",
        run_id="run-001",
        metrics=_minimal_metrics(),
        equity_curve=[],
        returns=[],
        trades=[],
        drawdowns=[],
    )

    writer._delete_existing_run.assert_called_once_with(conn, "exp", "run-001")
    writer._insert_run.assert_called_once()
    writer._insert_equity_curve.assert_called_once_with(conn, "exp", "run-001", [])
    writer._insert_returns.assert_called_once_with(conn, "exp", "run-001", [])
    writer._insert_trades.assert_called_once_with(conn, "exp", "run-001", [])
    writer._insert_drawdowns.assert_called_once_with(conn, "exp", "run-001", [])
    writer._insert_run_events.assert_called_once_with(conn, "exp", "run-001", None)


def test_postgresql_writer_updates_audit_export_path() -> None:
    """Writer should update runs.audit_export_path with a simple parameterized statement."""
    writer = cast(Any, object.__new__(PostgreSQLWriter))
    conn = MagicMock()
    begin_context = MagicMock()
    begin_context.__enter__.return_value = conn
    begin_context.__exit__.return_value = False
    writer._engine = MagicMock()
    writer._engine.begin.return_value = begin_context

    writer.update_audit_export_path("exp", "run-001", "/tmp/audit.zip")

    conn.execute.assert_called_once()
    statement, params = conn.execute.call_args.args
    assert "UPDATE runs" in str(statement)
    assert params == {
        "audit_export_path": "/tmp/audit.zip",
        "experiment_id": "exp",
        "run_id": "run-001",
    }


def test_postgresql_writer_warns_when_audit_export_path_update_hits_no_rows(caplog) -> None:
    """A zero-row audit-export path update should emit a warning for orphaned ZIP diagnosis."""
    writer = cast(Any, object.__new__(PostgreSQLWriter))
    conn = MagicMock()
    conn.execute.return_value = SimpleNamespace(rowcount=0)
    begin_context = MagicMock()
    begin_context.__enter__.return_value = conn
    begin_context.__exit__.return_value = False
    writer._engine = MagicMock()
    writer._engine.begin.return_value = begin_context

    with caplog.at_level("WARNING"):
        writer.update_audit_export_path("exp", "run-001", "/tmp/audit.zip")

    assert "postgresql_writer.audit_export_path_missing_run" in caplog.text


def test_database_enabled_updates_audit_export_path_after_zip_generation(tmp_path: Path) -> None:
    """Successful audit ZIP generation should persist the path after the run row is saved."""
    service = _build_reporting_service(tmp_path)
    service._event_store = _build_run_event_store()
    service._input_manifest = _minimal_manifest()
    save_writer = MagicMock()
    path_writer = MagicMock()
    audit_zip_path = tmp_path / "audit-exports" / "test_exp" / "20260101_000000.zip"

    with (
        patch(
            "qs_trader.system.config.get_system_config",
            return_value=_make_system_config_mock(
                db_enabled=True,
                postgres_url="postgresql+psycopg://research:secret@localhost:5432/research",
                config_root=tmp_path,
            ),
        ),
        patch(
            "qs_trader.services.reporting.writer_factory.create_persistence_writer",
            side_effect=[save_writer, path_writer],
        ) as mock_factory,
        patch(
            "qs_trader.services.reporting.audit_export.AuditExportBuilder.build",
            return_value=audit_zip_path,
        ) as mock_build,
    ):
        service._write_outputs(_minimal_metrics())

    mock_build.assert_called_once()
    assert mock_factory.call_count == 2
    path_writer.update_audit_export_path.assert_called_once_with(
        experiment_id="test_exp",
        run_id="20260101_000000",
        audit_export_path=str(audit_zip_path),
    )
    path_writer.close.assert_called_once()


def test_audit_export_failure_is_non_fatal_after_database_write(tmp_path: Path) -> None:
    """ClickHouse-side audit export failures must not fail the overall reporting flow."""
    service = _build_reporting_service(tmp_path)
    service._event_store = _build_run_event_store()
    service._input_manifest = _minimal_manifest()
    save_writer = MagicMock()

    with (
        patch(
            "qs_trader.system.config.get_system_config",
            return_value=_make_system_config_mock(
                db_enabled=True,
                postgres_url="postgresql+psycopg://research:secret@localhost:5432/research",
            ),
        ),
        patch(
            "qs_trader.services.reporting.writer_factory.create_persistence_writer",
            return_value=save_writer,
        ) as mock_factory,
        patch(
            "qs_trader.services.reporting.audit_export.AuditExportBuilder.build",
            side_effect=RuntimeError("clickhouse unavailable"),
        ),
    ):
        service._write_outputs(_minimal_metrics())

    assert mock_factory.call_count == 1
    save_writer.save_run.assert_called_once()
    assert service.get_database_write_status().state == "succeeded"
