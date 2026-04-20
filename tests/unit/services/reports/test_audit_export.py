"""Unit tests for Phase 2 audit export generation."""

from __future__ import annotations

import csv
import io
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from zipfile import ZipFile

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
from qs_trader.services.reporting.audit_export import AuditExportBuilder, ResolvedAuditBar
from qs_trader.services.reporting.manifest import AdjustmentMode, ClickHouseInputManifest, UnsafeManifestIdentifierError


def _minimal_metrics() -> FullMetrics:
    return FullMetrics.model_construct(
        backtest_id="audit_test",
        start_date="2024-01-01",
        end_date="2024-01-03",
        duration_days=2,
        initial_equity=Decimal("100000"),
        final_equity=Decimal("100500"),
        total_return_pct=Decimal("0.50"),
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
        total_trades=1,
        winning_trades=1,
        losing_trades=0,
        win_rate=Decimal("100"),
        profit_factor=Decimal("0"),
        avg_win=Decimal("50"),
        avg_loss=Decimal("0"),
        avg_win_pct=Decimal("0"),
        avg_loss_pct=Decimal("0"),
        largest_win=Decimal("50"),
        largest_loss=Decimal("0"),
        largest_win_pct=Decimal("0"),
        largest_loss_pct=Decimal("0"),
        expectancy=Decimal("50"),
        max_consecutive_wins=1,
        max_consecutive_losses=0,
        avg_trade_duration_days=Decimal("1"),
        total_commissions=Decimal("2"),
        commission_pct_of_pnl=Decimal("4"),
        monthly_returns=[],
        quarterly_returns=[],
        annual_returns=[],
        strategy_performance=[],
        drawdown_periods=[],
        open_trades=0,
        realized_pnl=Decimal("50"),
        unrealized_pnl=Decimal("0"),
    )


def _minimal_manifest(
    *,
    strategy_mode: AdjustmentMode = "split_adjusted",
    portfolio_mode: AdjustmentMode = "split_adjusted",
) -> ClickHouseInputManifest:
    return ClickHouseInputManifest(
        source_name="qs-datamaster-equity-1d",
        database="market",
        bars_table="as_us_equity_ohlc_daily",
        symbols=("AAPL",),
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 3),
        strategy_adjustment_mode=strategy_mode,
        portfolio_adjustment_mode=portfolio_mode,
    )


def _system_config(tmp_path: Path, *, config_root: Path | None = None) -> SimpleNamespace:
    experiments_root = tmp_path / "experiments"
    experiments_root.mkdir(parents=True, exist_ok=True)
    return SimpleNamespace(
        data=SimpleNamespace(sources_config="config/data_sources.yaml"),
        output=SimpleNamespace(experiments_root=str(experiments_root)),
        config_root=config_root,
    )


def _build_event_store() -> InMemoryEventStore:
    store = InMemoryEventStore()
    first_timestamp = "2024-01-02T00:00:00Z"
    second_timestamp = "2024-01-03T00:00:00Z"
    fill_one_id = "550e8400-e29b-41d4-a716-446655440011"
    fill_two_id = "550e8400-e29b-41d4-a716-446655440012"

    for timestamp, close_price in ((first_timestamp, Decimal("100.75")), (second_timestamp, Decimal("101.25"))):
        store.append(
            PriceBarEvent(
                symbol="AAPL",
                timestamp=timestamp,
                interval="1d",
                open=Decimal("100.00"),
                high=Decimal("101.00"),
                low=Decimal("99.50"),
                close=close_price,
                volume=1000,
                source="unit_test",
            )
        )

    store.append(
        FeatureBarEvent(
            timestamp=first_timestamp,
            symbol="AAPL",
            features={"alpha": Decimal("1.25"), "regime": "bull"},
        )
    )
    store.append(
        IndicatorEvent(
            strategy_id="sma_crossover",
            symbol="AAPL",
            timestamp=first_timestamp,
            indicators={"SMA(10)": Decimal("101.50"), "is_bullish": True, "comment": "skip-me"},
        )
    )
    store.append(
        SignalEvent(
            signal_id="signal-550e8400-e29b-41d4-a716-446655440001",
            timestamp=first_timestamp,
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
            timestamp=first_timestamp,
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
            timestamp=first_timestamp,
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
            timestamp=first_timestamp,
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
            timestamp=first_timestamp,
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
            timestamp=first_timestamp,
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
            entry_timestamp=first_timestamp,
            exit_timestamp="2024-01-10T00:00:00Z",
        )
    )

    return store


def test_resolve_ohlcv_uses_portfolio_adjustment_mode_contract() -> None:
    """OHLC export should follow the manifest's portfolio basis, not the strategy basis."""
    price_event = PriceBarEvent(
        symbol="AAPL",
        timestamp="2024-01-02T00:00:00Z",
        interval="1d",
        open=Decimal("100.00"),
        high=Decimal("101.00"),
        low=Decimal("99.00"),
        close=Decimal("100.50"),
        open_adj=Decimal("10.00"),
        high_adj=Decimal("10.10"),
        low_adj=Decimal("9.90"),
        close_adj=Decimal("10.05"),
        volume=1234,
        source="unit_test",
    )

    split_manifest = _minimal_manifest(strategy_mode="total_return", portfolio_mode="split_adjusted")
    total_return_manifest = _minimal_manifest(strategy_mode="split_adjusted", portfolio_mode="total_return")

    assert AuditExportBuilder.resolve_ohlcv_from_bar_event(price_event, split_manifest) == (
        100.0,
        101.0,
        99.0,
        100.5,
        1234,
    )
    assert AuditExportBuilder.resolve_ohlcv_from_bar_event(price_event, total_return_manifest) == (
        10.0,
        10.1,
        9.9,
        10.05,
        1234,
    )


def test_build_writes_symbol_and_summary_csvs(tmp_path: Path) -> None:
    """The builder should write a ZIP with one symbol CSV plus summary.csv."""
    export_root = tmp_path / "audit-exports"
    builder = AuditExportBuilder(_system_config(tmp_path), export_root=export_root)
    manifest = _minimal_manifest()
    bars = [
        ResolvedAuditBar(
            symbol="AAPL",
            trade_date=date(2024, 1, 2),
            timestamp=datetime(2024, 1, 2, tzinfo=timezone.utc),
            open=100.0,
            high=101.0,
            low=99.5,
            close=100.75,
            volume=1000,
        ),
        ResolvedAuditBar(
            symbol="AAPL",
            trade_date=date(2024, 1, 3),
            timestamp=datetime(2024, 1, 3, tzinfo=timezone.utc),
            open=100.5,
            high=101.5,
            low=100.0,
            close=101.25,
            volume=1100,
        ),
    ]

    with patch.object(AuditExportBuilder, "_load_symbol_bars", return_value=bars):
        zip_path = builder.build(
            experiment_id="exp-001",
            run_id="run-001",
            metrics=_minimal_metrics(),
            event_store=_build_event_store(),
            manifest=manifest,
            config_snapshot={"strategies": [{"strategy_id": "sma_crossover", "config": {"fast_period": 10}}]},
            effective_execution_spec={"risk_policy": {"name": "naive"}},
        )

    assert zip_path == (export_root / "exp-001" / "run-001.zip").resolve()
    assert zip_path.exists()

    with ZipFile(zip_path) as archive:
        assert sorted(archive.namelist()) == [
            "exp-001_run-001_audit/AAPL.csv",
            "exp-001_run-001_audit/summary.csv",
        ]

        symbol_rows = list(csv.DictReader(io.StringIO(archive.read("exp-001_run-001_audit/AAPL.csv").decode("utf-8"))))
        assert len(symbol_rows) == 2
        assert "ind_SMA(10)" in symbol_rows[0]
        assert "ind_is_bullish" in symbol_rows[0]
        assert "feat_alpha" in symbol_rows[0]
        assert symbol_rows[0]["signal_intention"] == "OPEN_LONG"
        assert symbol_rows[0]["order_qty"] == "15"
        assert float(symbol_rows[0]["fill_price"]) == 100.66666666666667
        assert symbol_rows[0]["ind_SMA(10)"] == "101.5"
        assert symbol_rows[0]["feat_alpha"] == "1.25"
        assert symbol_rows[1]["signal_intention"] == ""
        assert symbol_rows[1]["ind_SMA(10)"] == ""

        summary_rows = list(
            csv.DictReader(io.StringIO(archive.read("exp-001_run-001_audit/summary.csv").decode("utf-8")))
        )
        assert len(summary_rows) == 1
        summary = summary_rows[0]
        assert summary["experiment_id"] == "exp-001"
        assert summary["run_id"] == "run-001"
        assert "exported_at" in summary
        assert "created_at" not in summary
        assert summary["universe"] == "AAPL"
        assert summary["bars_table"] == "as_us_equity_ohlc_daily"
        assert summary["price_basis_mode"] == "split_adjusted"
        assert summary["config__strategies__0__config__fast_period"] == "10"
        assert summary["exec_spec__risk_policy__name"] == "naive"


def test_load_symbol_bars_rejects_unsafe_manifest_identifiers(tmp_path: Path) -> None:
    """Unsafe manifest identifiers should be rejected before touching adapter construction."""
    builder = AuditExportBuilder(_system_config(tmp_path))
    manifest = _minimal_manifest().model_copy(update={"database": "market;drop"})

    try:
        builder._load_symbol_bars(manifest, "AAPL")
    except UnsafeManifestIdentifierError as exc:
        assert "database" in str(exc)
    else:  # pragma: no cover - explicit failure branch for readability
        raise AssertionError("Expected unsafe manifest identifier to be rejected")


def test_resolve_export_base_dir_uses_documented_fallbacks(tmp_path: Path) -> None:
    """Export path resolution should follow container → sibling repo → local fallback."""
    trader_root = tmp_path / "QS-Trader"
    trader_root.mkdir(parents=True, exist_ok=True)
    research_root = tmp_path / "QS-Research"
    research_root.mkdir(parents=True, exist_ok=True)

    builder = AuditExportBuilder(_system_config(tmp_path, config_root=trader_root))
    container_root = tmp_path / "container" / "audit-exports"
    container_root.parent.mkdir(parents=True, exist_ok=True)

    with patch.object(AuditExportBuilder, "CONTAINER_EXPORT_ROOT", container_root):
        assert builder._resolve_export_base_dir() == container_root

    with patch.object(AuditExportBuilder, "CONTAINER_EXPORT_ROOT", tmp_path / "missing" / "audit-exports"):
        assert builder._resolve_export_base_dir() == (research_root / "data" / "audit-exports")

    no_repo_builder = AuditExportBuilder(_system_config(tmp_path), export_root=None)
    with patch.object(AuditExportBuilder, "CONTAINER_EXPORT_ROOT", tmp_path / "missing" / "audit-exports"):
        assert no_repo_builder._resolve_export_base_dir() == (tmp_path / "experiments" / "audit-exports")
