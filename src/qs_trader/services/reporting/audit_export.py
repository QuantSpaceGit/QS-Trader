"""Audit export ZIP builder for backtest runs.

Builds one CSV per symbol plus a single ``summary.csv`` from canonical
ClickHouse bars and the in-memory decision-chain events collected during the
run teardown phase.

The export path follows the requirement-doc contract with a safe fallback chain:

1. ``/app/data/audit-exports`` when running inside the container layout.
2. Sibling ``../QS-Research/data/audit-exports`` when the standard repo layout
   is available from ``SystemConfig.config_root``.
3. ``{output.experiments_root}/audit-exports`` as a local/test fallback.

OHLC basis rule:
    The audit export resolves the final CSV OHLC columns from the manifest's
    single run-level ``price_basis`` contract. Dividend cash-flow accounting is
    intentionally handled outside of this OHLC selection.
"""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from zipfile import ZIP_DEFLATED, ZipFile

import structlog

from qs_trader.events.event_store import EventStore
from qs_trader.events.events import PriceBarEvent
from qs_trader.events.price_basis import PriceBasis
from qs_trader.libraries.performance.models import FullMetrics
from qs_trader.services.data.adapters.builtin.clickhouse import ClickhouseDataAdapter
from qs_trader.services.data.adapters.resolver import DataSourceResolver
from qs_trader.services.data.models import Instrument
from qs_trader.services.reporting.event_collector import collect_run_events
from qs_trader.services.reporting.manifest import ClickHouseInputManifest, assert_safe_manifest_identifier


def _normalize_csv_value(value: Any) -> Any:
    """Normalize Python values into CSV-friendly scalars."""
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, tuple, dict)):
        return json.dumps(value, sort_keys=True)
    return value


def _decode_numeric_json(raw: str | None) -> dict[str, float]:
    """Decode a persisted JSON map, returning an empty dict on null input."""
    if not raw:
        return {}

    decoded = json.loads(raw)
    if not isinstance(decoded, dict):
        return {}

    result: dict[str, float] = {}
    for key, value in decoded.items():
        if isinstance(value, (int, float)):
            result[str(key)] = float(value)
    return result


@dataclass(frozen=True, slots=True)
class ResolvedAuditBar:
    """Canonical bar payload used for audit CSV generation."""

    symbol: str
    trade_date: date
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int


class AuditExportBuilder:
    """Build ZIP audit exports for a completed backtest run."""

    CONTAINER_EXPORT_ROOT = Path("/app/data/audit-exports")

    SYMBOL_FIXED_COLUMNS = [
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "signal_intention",
        "signal_price",
        "signal_confidence",
        "signal_reason",
        "order_side",
        "order_type",
        "order_qty",
        "fill_qty",
        "fill_price",
        "fill_slippage_bps",
        "commission",
        "trade_id",
        "trade_status",
        "trade_side",
        "trade_entry_price",
        "trade_exit_price",
        "trade_realized_pnl",
    ]

    SUMMARY_FIXED_COLUMNS = [
        "experiment_id",
        "run_id",
        "backtest_id",
        "start_date",
        "end_date",
        "duration_days",
        "exported_at",
        "initial_equity",
        "final_equity",
        "total_return_pct",
        "cagr",
        "volatility_annual_pct",
        "max_drawdown_pct",
        "max_drawdown_duration_days",
        "avg_drawdown_pct",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "risk_free_rate",
        "total_trades",
        "winning_trades",
        "losing_trades",
        "win_rate",
        "profit_factor",
        "avg_win",
        "avg_loss",
        "expectancy",
        "total_commissions",
        "open_trades",
        "realized_pnl",
        "unrealized_pnl",
        "universe",
        "source_name",
        "database",
        "features_database",
        "bars_table",
        "features_table",
        "regime_table",
        "feature_set_version",
        "regime_version",
        "price_basis",
    ]

    def __init__(self, system_config: Any, *, export_root: Path | None = None) -> None:
        """Initialize the builder.

        Args:
            system_config: Active QS-Trader system configuration (or test double).
            export_root: Optional explicit export root override, primarily for tests.
        """
        self._system_config = system_config
        self._export_root = export_root
        self._logger = structlog.get_logger(self.__class__.__name__)

    @classmethod
    def resolve_price_basis(cls, manifest: ClickHouseInputManifest) -> PriceBasis:
        """Resolve the deterministic OHLC basis for the export."""
        return manifest.price_basis

    @classmethod
    def resolve_ohlcv_from_bar_event(
        cls,
        price_event: PriceBarEvent,
        manifest: ClickHouseInputManifest,
    ) -> tuple[float, float, float, float, int]:
        """Resolve CSV OHLCV values from a bar event and manifest contract.

        Args:
            price_event: ClickHouse-backed bar event with base and adjusted prices.
            manifest: Run input manifest describing the required basis.

        Returns:
            Tuple of ``(open, high, low, close, volume)`` for CSV export.

        Raises:
            ValueError: If adjusted output is requested but adjusted prices are absent.
        """
        price_basis = cls.resolve_price_basis(manifest)
        if price_basis == PriceBasis.ADJUSTED:
            adjusted_open = price_event.open_adj
            adjusted_high = price_event.high_adj
            adjusted_low = price_event.low_adj
            adjusted_close = price_event.close_adj
            if any(value is None for value in (adjusted_open, adjusted_high, adjusted_low, adjusted_close)):
                raise ValueError(
                    "Manifest requested adjusted audit pricing but the resolved ClickHouse bar is missing adjusted OHLC values."
                )
            bar_open, bar_high, bar_low, bar_close = (
                cast(Decimal, adjusted_open),
                cast(Decimal, adjusted_high),
                cast(Decimal, adjusted_low),
                cast(Decimal, adjusted_close),
            )
        else:
            bar_open, bar_high, bar_low, bar_close = (
                price_event.open,
                price_event.high,
                price_event.low,
                price_event.close,
            )

        return (
            float(bar_open),
            float(bar_high),
            float(bar_low),
            float(bar_close),
            price_event.volume,
        )

    def build(
        self,
        *,
        experiment_id: str,
        run_id: str,
        metrics: FullMetrics,
        event_store: EventStore,
        manifest: ClickHouseInputManifest,
        config_snapshot: dict[str, Any] | None = None,
        effective_execution_spec: dict[str, Any] | None = None,
    ) -> Path:
        """Build the audit export ZIP for a completed run.

        Args:
            experiment_id: Experiment identifier.
            run_id: Run identifier.
            metrics: Final backtest metrics.
            event_store: In-memory event stream used for decision-chain collection.
            manifest: ClickHouse input manifest for bar re-resolution.
            config_snapshot: Optional normalized backtest config snapshot.
            effective_execution_spec: Optional immutable execution provenance.

        Returns:
            Absolute path to the generated ZIP archive.
        """
        collected_rows = collect_run_events(experiment_id, run_id, event_store)
        event_rows_by_symbol = self._index_run_event_rows(collected_rows)

        archive_root = f"{experiment_id}_{run_id}_audit"
        zip_path = self._resolve_zip_path(experiment_id, run_id)
        zip_path.parent.mkdir(parents=True, exist_ok=True)

        with ZipFile(zip_path, mode="w", compression=ZIP_DEFLATED) as archive:
            for symbol in manifest.symbols:
                bars = self._load_symbol_bars(manifest, symbol)
                rows, fieldnames = self._build_symbol_rows(
                    bars=bars,
                    event_rows_by_timestamp=event_rows_by_symbol.get(symbol, {}),
                )
                archive.writestr(
                    f"{archive_root}/{symbol}.csv",
                    self._render_csv(rows, fieldnames),
                )

            summary_row = self._build_summary_row(
                experiment_id=experiment_id,
                run_id=run_id,
                metrics=metrics,
                manifest=manifest,
                config_snapshot=config_snapshot,
                effective_execution_spec=effective_execution_spec,
            )
            summary_fieldnames = list(self.SUMMARY_FIXED_COLUMNS)
            summary_fieldnames.extend(
                sorted(key for key in summary_row.keys() if key not in set(self.SUMMARY_FIXED_COLUMNS))
            )
            archive.writestr(
                f"{archive_root}/summary.csv",
                self._render_csv([summary_row], summary_fieldnames),
            )

        resolved_path = zip_path.resolve()
        self._logger.info(
            "audit_export.created",
            experiment_id=experiment_id,
            run_id=run_id,
            path=str(resolved_path),
            symbols=list(manifest.symbols),
        )
        return resolved_path

    def _index_run_event_rows(
        self,
        rows: list[dict[str, Any]],
    ) -> dict[str, dict[datetime, dict[str, Any]]]:
        """Index collected run-event rows by symbol and timestamp."""
        indexed: dict[str, dict[datetime, dict[str, Any]]] = {}
        for row in rows:
            symbol = str(row["symbol"])
            timestamp = row["timestamp"]
            payload = dict(row)
            payload["indicators"] = _decode_numeric_json(payload.pop("indicators_json", None))
            payload["features"] = _decode_numeric_json(payload.pop("features_json", None))
            indexed.setdefault(symbol, {})[timestamp] = payload
        return indexed

    def _build_symbol_rows(
        self,
        *,
        bars: list[ResolvedAuditBar],
        event_rows_by_timestamp: dict[datetime, dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """Build flat per-bar CSV rows for a single symbol."""
        indicator_names = sorted(
            {
                indicator_name
                for payload in event_rows_by_timestamp.values()
                for indicator_name in payload.get("indicators", {}).keys()
            }
        )
        feature_names = sorted(
            {
                feature_name
                for payload in event_rows_by_timestamp.values()
                for feature_name in payload.get("features", {}).keys()
            }
        )

        fieldnames = list(self.SYMBOL_FIXED_COLUMNS)
        fieldnames.extend(f"ind_{name}" for name in indicator_names)
        fieldnames.extend(f"feat_{name}" for name in feature_names)

        rows: list[dict[str, Any]] = []
        for bar in bars:
            payload = event_rows_by_timestamp.get(bar.timestamp, {})
            indicators = payload.get("indicators", {})
            features = payload.get("features", {})

            row: dict[str, Any] = {
                "date": bar.trade_date.isoformat(),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": bar.volume,
                "signal_intention": payload.get("signal_intention"),
                "signal_price": payload.get("signal_price"),
                "signal_confidence": payload.get("signal_confidence"),
                "signal_reason": payload.get("signal_reason"),
                "order_side": payload.get("order_side"),
                "order_type": payload.get("order_type"),
                "order_qty": payload.get("order_qty"),
                "fill_qty": payload.get("fill_qty"),
                "fill_price": payload.get("fill_price"),
                "fill_slippage_bps": payload.get("fill_slippage_bps"),
                "commission": payload.get("commission"),
                "trade_id": payload.get("trade_id"),
                "trade_status": payload.get("trade_status"),
                "trade_side": payload.get("trade_side"),
                "trade_entry_price": payload.get("trade_entry_price"),
                "trade_exit_price": payload.get("trade_exit_price"),
                "trade_realized_pnl": payload.get("trade_realized_pnl"),
            }
            for indicator_name in indicator_names:
                row[f"ind_{indicator_name}"] = indicators.get(indicator_name)
            for feature_name in feature_names:
                row[f"feat_{feature_name}"] = features.get(feature_name)
            rows.append(row)

        return rows, fieldnames

    def _build_summary_row(
        self,
        *,
        experiment_id: str,
        run_id: str,
        metrics: FullMetrics,
        manifest: ClickHouseInputManifest,
        config_snapshot: dict[str, Any] | None,
        effective_execution_spec: dict[str, Any] | None,
    ) -> dict[str, Any]:
        """Build the single-row summary payload for ``summary.csv``."""
        summary: dict[str, Any] = {
            "experiment_id": experiment_id,
            "run_id": run_id,
            "backtest_id": metrics.backtest_id,
            "start_date": metrics.start_date,
            "end_date": metrics.end_date,
            "duration_days": metrics.duration_days,
            "exported_at": datetime.now(timezone.utc).isoformat(),
            "initial_equity": metrics.initial_equity,
            "final_equity": metrics.final_equity,
            "total_return_pct": metrics.total_return_pct,
            "cagr": metrics.cagr,
            "volatility_annual_pct": metrics.volatility_annual_pct,
            "max_drawdown_pct": metrics.max_drawdown_pct,
            "max_drawdown_duration_days": metrics.max_drawdown_duration_days,
            "avg_drawdown_pct": metrics.avg_drawdown_pct,
            "sharpe_ratio": metrics.sharpe_ratio,
            "sortino_ratio": metrics.sortino_ratio,
            "calmar_ratio": metrics.calmar_ratio,
            "risk_free_rate": metrics.risk_free_rate,
            "total_trades": metrics.total_trades,
            "winning_trades": metrics.winning_trades,
            "losing_trades": metrics.losing_trades,
            "win_rate": metrics.win_rate,
            "profit_factor": metrics.profit_factor,
            "avg_win": metrics.avg_win,
            "avg_loss": metrics.avg_loss,
            "expectancy": metrics.expectancy,
            "total_commissions": metrics.total_commissions,
            "open_trades": metrics.open_trades,
            "realized_pnl": metrics.realized_pnl,
            "unrealized_pnl": metrics.unrealized_pnl,
            "universe": ",".join(manifest.symbols),
            "source_name": manifest.source_name,
            "database": manifest.database,
            "features_database": manifest.features_database,
            "bars_table": manifest.bars_table,
            "features_table": manifest.features_table,
            "regime_table": manifest.regime_table,
            "feature_set_version": manifest.feature_set_version,
            "regime_version": manifest.regime_version,
            "price_basis": str(manifest.price_basis),
        }

        if manifest.feature_columns is not None:
            summary["feature_columns"] = ",".join(manifest.feature_columns)

        if config_snapshot is not None:
            summary.update(self._flatten_value("config", config_snapshot))
        if effective_execution_spec is not None:
            summary.update(self._flatten_value("exec_spec", effective_execution_spec))

        return summary

    def _flatten_value(self, prefix: str, value: Any) -> dict[str, Any]:
        """Flatten nested dict/list structures into a flat mapping."""
        if hasattr(value, "model_dump"):
            value = value.model_dump(mode="json")
        elif hasattr(value, "dict"):
            value = value.dict()

        flattened: dict[str, Any] = {}
        if isinstance(value, dict):
            for key in sorted(value.keys(), key=str):
                child_prefix = f"{prefix}__{key}"
                flattened.update(self._flatten_value(child_prefix, value[key]))
            return flattened

        if isinstance(value, (list, tuple)):
            if not value:
                flattened[prefix] = "[]"
                return flattened
            for index, item in enumerate(value):
                child_prefix = f"{prefix}__{index}"
                flattened.update(self._flatten_value(child_prefix, item))
            return flattened

        flattened[prefix] = value
        return flattened

    def _render_csv(self, rows: list[dict[str, Any]], fieldnames: list[str]) -> str:
        """Render rows to a UTF-8 CSV string."""
        buffer = io.StringIO(newline="")
        writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _normalize_csv_value(row.get(key)) for key in fieldnames})
        return buffer.getvalue()

    def _resolve_zip_path(self, experiment_id: str, run_id: str) -> Path:
        """Resolve the final ZIP path for the export."""
        return self._resolve_export_base_dir() / experiment_id / f"{run_id}.zip"

    def _resolve_export_base_dir(self) -> Path:
        """Resolve the base output directory using the documented fallback chain."""
        if self._export_root is not None:
            return self._export_root.expanduser().resolve()

        if self.CONTAINER_EXPORT_ROOT.parent.exists():
            return self.CONTAINER_EXPORT_ROOT

        config_root_value = getattr(self._system_config, "config_root", None)
        if config_root_value is not None:
            config_root = Path(config_root_value).expanduser().resolve()
            research_root = config_root.parent / "QS-Research"
            if research_root.exists():
                return research_root / "data" / "audit-exports"
        else:
            config_root = None

        experiments_root = Path(str(self._system_config.output.experiments_root)).expanduser()
        if not experiments_root.is_absolute() and config_root is not None:
            experiments_root = config_root / experiments_root

        return experiments_root.resolve() / "audit-exports"

    def _load_symbol_bars(
        self,
        manifest: ClickHouseInputManifest,
        symbol: str,
    ) -> list[ResolvedAuditBar]:
        """Load canonical ClickHouse bars for a symbol using the manifest contract."""
        assert_safe_manifest_identifier(manifest.database, "database")
        assert_safe_manifest_identifier(manifest.bars_table, "bars_table")

        resolver = DataSourceResolver(system_sources_config=self._system_config.data.sources_config)
        source_config = resolver.get_source_config(manifest.source_name)
        if source_config.get("adapter") != "clickhouse":
            raise ValueError(
                f"Audit export requires a ClickHouse-backed datasource, got adapter={source_config.get('adapter')!r} "
                f"for source {manifest.source_name!r}."
            )

        adapter_config = dict(source_config)
        clickhouse_config = dict(adapter_config.get("clickhouse", {}) or {})
        clickhouse_config["database"] = manifest.database
        adapter_config["clickhouse"] = clickhouse_config
        adapter_config["bars_table"] = manifest.bars_table

        adapter = ClickhouseDataAdapter(
            config=adapter_config,
            instrument=Instrument(symbol=symbol),
            dataset_name=manifest.source_name,
        )

        bars: list[ResolvedAuditBar] = []
        for raw_bar in adapter.read_bars(manifest.start_date.isoformat(), manifest.end_date.isoformat()):
            price_event = adapter.to_price_bar_event(raw_bar)
            bar_open, bar_high, bar_low, bar_close, volume = self.resolve_ohlcv_from_bar_event(price_event, manifest)
            bars.append(
                ResolvedAuditBar(
                    symbol=symbol,
                    trade_date=raw_bar.trade_date,
                    timestamp=datetime.fromisoformat(price_event.timestamp.replace("Z", "+00:00")),
                    open=bar_open,
                    high=bar_high,
                    low=bar_low,
                    close=bar_close,
                    volume=volume,
                )
            )

        return bars
