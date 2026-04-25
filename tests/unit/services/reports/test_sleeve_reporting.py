"""Focused unit tests for sleeve-aware reporting, collectors, and writer parameters."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import IndicatorEvent, PerformanceMetricsEvent, PortfolioStateEvent, RuntimeFeaturesEvent
from qs_trader.events.lifecycle_events import StrategyDecisionEvent
from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.lifecycle_event_collector import collect_run_lifecycle_events
from qs_trader.services.reporting.observability_collector import collect_run_observability_bars
from qs_trader.services.reporting.postgres_writer import PostgreSQLWriter
from qs_trader.services.reporting.service import ReportingService


def _reporting_config(**overrides: object) -> ReportingConfig:
    base_config: dict[str, object] = {
        "emit_metrics_events": True,
        "event_frequency": 1,
        "write_parquet": False,
        "write_json": False,
        "write_html_report": False,
        "write_csv_timeline": False,
        "display_final_report": False,
    }
    base_config.update(overrides)
    return ReportingConfig.model_validate(base_config)


def _reporting_system_config() -> SimpleNamespace:
    return SimpleNamespace(
        output=SimpleNamespace(
            artifact_policy=SimpleNamespace(mode="filesystem"),
            database=SimpleNamespace(enabled=False, backend="postgres", postgres_url=None),
        )
    )


def _portfolio_state() -> PortfolioStateEvent:
    return PortfolioStateEvent(
        portfolio_id="portfolio-001",
        start_datetime="2024-01-01T00:00:00Z",
        snapshot_datetime="2024-01-02T00:00:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=Decimal("100000"),
        cash_balance=Decimal("100000"),
        current_portfolio_equity=Decimal("100000"),
        total_market_value=Decimal("0"),
        total_unrealized_pl=Decimal("0"),
        total_realized_pl=Decimal("0"),
        total_pl=Decimal("0"),
        long_exposure=Decimal("0"),
        short_exposure=Decimal("0"),
        net_exposure=Decimal("0"),
        gross_exposure=Decimal("0"),
        leverage=Decimal("0"),
        total_commissions_paid=Decimal("0"),
        strategies_groups=[],
    )


def _build_reporting_service(
    tmp_path: Path,
    *,
    sleeve: object | None,
    write_html_report: bool = False,
) -> tuple[ReportingService, InMemoryEventStore, EventBus]:
    event_bus = EventBus()
    event_store = InMemoryEventStore()
    event_bus.attach_store(event_store)
    service = ReportingService(
        event_bus=event_bus,
        config=_reporting_config(write_html_report=write_html_report),
        output_dir=tmp_path,
    )
    service.setup(
        {
            "backtest_id": "exp",
            "strategy_ids": ["sma_crossover"],
            "backtest_config": SimpleNamespace(
                run_id="run-001",
                job_group_id=None,
                submission_source=None,
                split_pct=None,
                split_role=None,
                sleeve=sleeve,
            ),
        }
    )
    return service, event_store, event_bus


def test_reporting_service_emits_v2_performance_metrics_for_sleeve_bound_runs(tmp_path: Path) -> None:
    _, event_store, event_bus = _build_reporting_service(
        tmp_path,
        sleeve=SimpleNamespace(sleeve_id="sma_crossover:AAPL", symbol="AAPL"),
    )

    event_bus.publish(_portfolio_state())

    metrics_events = [
        event for event in event_store.get_by_type("performance_metrics") if isinstance(event, PerformanceMetricsEvent)
    ]

    assert len(metrics_events) == 1
    assert metrics_events[0].event_version == 2
    assert metrics_events[0].sleeve_id == "sma_crossover:AAPL"
    assert metrics_events[0].symbol == "AAPL"


def test_reporting_service_keeps_no_sleeve_metrics_on_v1_contract(tmp_path: Path) -> None:
    _, event_store, event_bus = _build_reporting_service(tmp_path, sleeve=None)

    event_bus.publish(_portfolio_state())

    metrics_events = [
        event for event in event_store.get_by_type("performance_metrics") if isinstance(event, PerformanceMetricsEvent)
    ]

    assert len(metrics_events) == 1
    assert metrics_events[0].event_version == 1
    assert metrics_events[0].sleeve_id is None
    assert metrics_events[0].symbol is None
    assert "sleeve_id" not in metrics_events[0].model_dump(exclude_none=True)
    assert "symbol" not in metrics_events[0].model_dump(exclude_none=True)


def test_reporting_service_skips_html_report_generation_for_sleeve_bound_runs(tmp_path: Path) -> None:
    service, _, event_bus = _build_reporting_service(
        tmp_path,
        sleeve=SimpleNamespace(sleeve_id="sma_crossover:AAPL", symbol="AAPL"),
        write_html_report=True,
    )

    event_bus.publish(_portfolio_state())

    with (
        patch("qs_trader.system.config.get_system_config", return_value=_reporting_system_config()),
        patch("qs_trader.services.reporting.html_reporter.HTMLReportGenerator.generate") as generate_html,
    ):
        service.teardown({})

    generate_html.assert_not_called()
    assert (tmp_path / "performance.json").exists()
    assert not (tmp_path / "report.html").exists()


def test_lifecycle_rows_include_sleeve_id_column_when_present() -> None:
    event_store = InMemoryEventStore()
    event_store.append(
        StrategyDecisionEvent(
            experiment_id="exp",
            run_id="run-001",
            sleeve_id="sma_crossover:AAPL",
            occurred_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
            decision_id="550e8400-e29b-41d4-a716-446655440401",
            strategy_id="sma_crossover",
            symbol="AAPL",
            bar_timestamp="2024-01-02T00:00:00Z",
            decision_type="open_long",
            decision_price=Decimal("100.50"),
            decision_basis="adjusted_ohlc_adj_columns",
            confidence=Decimal("0.90"),
            source_service="strategy_service",
            correlation_id="550e8400-e29b-41d4-a716-446655440402",
        )
    )

    rows = collect_run_lifecycle_events("exp", "run-001", event_store)

    assert len(rows) == 1
    assert rows[0]["sleeve_id"] == "sma_crossover:AAPL"


def test_observability_rows_are_disambiguated_by_sleeve_id() -> None:
    event_store = InMemoryEventStore()
    event_store.append(
        IndicatorEvent(
            strategy_id="sma_crossover",
            symbol="AAPL",
            sleeve_id="sma_crossover:AAPL-main",
            timestamp="2024-01-02T00:00:00Z",
            indicators={"sma_fast": Decimal("100.25")},
        )
    )
    event_store.append(
        RuntimeFeaturesEvent(
            strategy_id="sma_crossover",
            symbol="AAPL",
            sleeve_id="sma_crossover:AAPL-main",
            timestamp="2024-01-02T00:00:00Z",
            runtime_features={"momentum_score": Decimal("0.12")},
        )
    )
    event_store.append(
        IndicatorEvent(
            strategy_id="sma_crossover",
            symbol="AAPL",
            sleeve_id="sma_crossover:AAPL-alt",
            timestamp="2024-01-02T00:00:00Z",
            indicators={"sma_fast": Decimal("101.25")},
        )
    )

    rows = collect_run_observability_bars("exp", "run-001", event_store)

    assert len(rows) == 2
    by_sleeve = {row["sleeve_id"]: row for row in rows}
    assert set(by_sleeve) == {"sma_crossover:AAPL-alt", "sma_crossover:AAPL-main"}
    assert json.loads(by_sleeve["sma_crossover:AAPL-main"]["runtime_features_json"]) == {"momentum_score": "0.12"}
    assert json.loads(by_sleeve["sma_crossover:AAPL-alt"]["indicators_json"]) == {"sma_fast": "101.25"}


def test_postgresql_writer_passes_sleeve_id_for_lifecycle_rows() -> None:
    writer = object.__new__(PostgreSQLWriter)
    conn = MagicMock()
    event_store = InMemoryEventStore()
    event_store.append(
        StrategyDecisionEvent(
            experiment_id="exp",
            run_id="run-001",
            sleeve_id="sma_crossover:AAPL",
            occurred_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
            decision_id="550e8400-e29b-41d4-a716-446655440403",
            strategy_id="sma_crossover",
            symbol="AAPL",
            bar_timestamp="2024-01-02T00:00:00Z",
            decision_type="open_long",
            decision_price=Decimal("100.50"),
            decision_basis="adjusted_ohlc_adj_columns",
            confidence=Decimal("0.90"),
            source_service="strategy_service",
            correlation_id="550e8400-e29b-41d4-a716-446655440404",
        )
    )

    inserted = writer._insert_lifecycle_events(conn, "exp", "run-001", event_store)

    assert inserted == 1
    _, params = conn.execute.call_args.args
    assert params[0]["sleeve_id"] == "sma_crossover:AAPL"


def test_postgresql_writer_passes_sleeve_id_for_observability_rows() -> None:
    writer = object.__new__(PostgreSQLWriter)
    conn = MagicMock()
    event_store = InMemoryEventStore()
    event_store.append(
        IndicatorEvent(
            strategy_id="sma_crossover",
            symbol="AAPL",
            sleeve_id="sma_crossover:AAPL",
            timestamp="2024-01-02T00:00:00Z",
            indicators={"sma_fast": Decimal("100.25")},
        )
    )

    inserted = writer._insert_observability_bars(conn, "exp", "run-001", event_store)

    assert inserted == 1
    _, params = conn.execute.call_args.args
    assert params[0]["sleeve_id"] == "sma_crossover:AAPL"
