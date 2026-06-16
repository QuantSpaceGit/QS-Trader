"""Candidate scan runner.

Iterates over a universe of instruments, resolves them via InstrumentResolver,
evaluates candidate rules, computes forward returns / MFE / MAE, and persists
results to CSV.

Supports both the new structured context/decision contract and the legacy
tuple-return contract through a compatibility adapter.
"""

from __future__ import annotations

import csv
import inspect
import json
import math
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Callable

import structlog

from qs_trader.services.scan.calculations import compute_scan_metrics
from qs_trader.services.scan.models import (
    ScanDecision,
    ScanRuleContext,
    canonicalize_parameters,
    decision_from_tuple,
    hash_parameters,
)

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Scan result schema (additive — existing columns preserved)
# ---------------------------------------------------------------------------

_SCAN_RESULT_COLUMNS = [
    "date",
    "secid",
    "display_symbol",
    "ticker_at_date",
    "runtime_symbol",
    "strategy_id",
    "candidate_status",
    "reason_code",
    "score",
    "gates_json",
    "features_json",
    "diagnostics_json",
    "forward_return_5d",
    "forward_return_10d",
    "forward_return_20d",
    "mfe_20d",
    "mae_20d",
]


@dataclass
class ScanResult:
    """Single scan result row."""

    date: str
    secid: int | None
    display_symbol: str
    ticker_at_date: str
    runtime_symbol: str
    strategy_id: str
    candidate_status: str
    reason_code: str
    score: float | None = None
    gates: dict[str, Any] = field(default_factory=dict)
    features: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    forward_return_5d: float | None = None
    forward_return_10d: float | None = None
    forward_return_20d: float | None = None
    mfe_20d: float | None = None
    mae_20d: float | None = None


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


@dataclass
class ScanSummary:
    """Summary of a completed scan run."""

    total_instruments: int = 0
    instruments_processed: int = 0
    instruments_failed: int = 0
    total_rows: int = 0
    failures: list[str] = field(default_factory=list)


class ScanRunner:
    """Execute a candidate scan over a universe of instruments.

    Args:
        instrument_resolver: Resolves tickers to secids.
        data_loader: Callable(secid) -> dict with "closes", "highs", "lows",
            "dates", "opens", "volumes".
        candidate_rule: Callable returning ScanDecision or legacy tuple.
        strategy_id: Strategy identifier for attribution.
        horizons: Forward return horizons in bars.
        data_source: Data source identifier (e.g. "qs-datamaster").
        price_basis: Price basis name (e.g. "adjusted_ohlc_adj_columns").
        parameters: Optional rule parameter snapshot.
    """

    def __init__(
        self,
        instrument_resolver: Any,
        data_loader: Callable[[int], dict[str, Any]],
        candidate_rule: Callable[..., Any],
        strategy_id: str,
        horizons: list[int] | None = None,
        data_source: str = "",
        price_basis: str = "",
        parameters: dict[str, Any] | None = None,
        rule_import_path: str = "",
        database: str = "",
        bars_table: str = "",
        source_columns: dict[str, str] | None = None,
    ):
        self._resolver = instrument_resolver
        self._data_loader = data_loader
        self._candidate_rule = candidate_rule
        self._strategy_id = strategy_id
        self._horizons = horizons or [5, 10, 20]
        self._data_source = data_source
        self._price_basis = price_basis
        self._parameters = parameters or {}
        self._parameter_hash = hash_parameters(self._parameters)
        self._canonical_params = canonicalize_parameters(self._parameters)
        self._rule_import_path = rule_import_path
        self._database = database
        self._bars_table = bars_table
        self._source_columns = source_columns

    def run(
        self,
        tickers: list[str] | None = None,
        date_range: tuple[date, date] | None = None,
        output_dir: Path | None = None,
        filename: str = "candidate_scan_results.csv",
        ticker_policy: str = "anchor_first_in_range",
        secids: list[int] | None = None,
    ) -> tuple[list[ScanResult], ScanSummary]:
        """Execute the scan over the given tickers or secids and date range.

        Args:
            tickers: List of ticker symbols to scan.
            date_range: (start_date, end_date) for the scan.
            output_dir: Directory to write results.
            filename: Output CSV filename.
            ticker_policy: Resolution policy for InstrumentResolver.
            secids: List of secids to scan directly (bypasses ticker resolution).

        Returns:
            (results, summary) tuple.
        """
        if tickers is None:
            tickers = []
        if date_range is None:
            raise ValueError("date_range is required")
        if output_dir is None:
            raise ValueError("output_dir is required")

        results: list[ScanResult] = []
        summary = ScanSummary()

        # Resolve instruments: secids directly, tickers via resolver
        resolved = self._resolve_instruments(tickers, secids, date_range, ticker_policy, summary)
        summary.total_instruments = len(resolved)

        for ticker, instrument in resolved.items():
            try:
                rows = self._scan_instrument(ticker, instrument)
                results.extend(rows)
                summary.instruments_processed += 1
            except Exception as e:
                summary.instruments_failed += 1
                summary.failures.append(f"{ticker}: {e}")
                logger.error(
                    "scan.instrument_failed",
                    ticker=ticker,
                    error=str(e),
                )

        # Persist results
        output_dir.mkdir(parents=True, exist_ok=True)
        self._persist_results(results, output_dir / filename)
        summary.total_rows = len(results)

        # Write scan manifest
        self._write_manifest(
            output_dir=output_dir,
            resolved=resolved,
            summary=summary,
            date_range=date_range,
            tickers=tickers,
            secids=secids,
            ticker_policy=ticker_policy,
            filename=filename,
        )

        logger.info(
            "scan.completed",
            processed=summary.instruments_processed,
            failed=summary.instruments_failed,
            total_rows=summary.total_rows,
        )

        return results, summary

    def _resolve_instruments(
        self,
        tickers: list[str],
        secids: list[int] | None,
        date_range: tuple[date, date],
        policy: str,
        summary: ScanSummary,
    ) -> dict[str, Any]:
        """Resolve tickers and/or secids to instruments."""
        resolved: dict[str, Any] = {}

        # Resolve secids directly
        if secids:
            if self._resolver is None:
                raise RuntimeError(
                    "InstrumentResolver is required for secid resolution. "
                    "Provide --data-source or configure an InstrumentResolver."
                )
            for secid in secids:
                try:
                    instrument = self._resolver.resolve_by_secid(secid, date_range)
                    resolved[str(secid)] = instrument
                except Exception as e:
                    summary.instruments_failed += 1
                    summary.failures.append(f"secid:{secid}: {e}")
                    logger.error(
                        "scan.secid_resolution_failed",
                        secid=secid,
                        error=str(e),
                    )

        # Resolve tickers via batch resolution
        if tickers:
            if self._resolver is None:
                raise RuntimeError(
                    "InstrumentResolver is required for candidate scan. "
                    "Provide --data-source or configure an InstrumentResolver."
                )
            try:
                ticker_resolved = self._resolver.resolve_batch(tickers, date_range, policy)
                resolved.update(ticker_resolved)
            except Exception as e:
                summary.instruments_failed += len(tickers)
                for ticker in tickers:
                    summary.failures.append(f"{ticker}: {e}")
                logger.error(
                    "scan.ticker_batch_resolution_failed",
                    tickers=tickers,
                    error=str(e),
                )

        return resolved

    def _scan_instrument(
        self,
        ticker: str,
        instrument: Any,
    ) -> list[ScanResult]:
        """Scan a single instrument across its date range."""
        secid = getattr(instrument, "secid", None)
        display_symbol = getattr(instrument, "display_symbol", ticker)
        ticker_at_date = getattr(instrument, "ticker_at_date", ticker)
        identity_source = getattr(instrument, "identity_source", "ticker")

        # Load price data
        data = self._data_loader(secid if secid is not None else ticker)
        opens = data.get("opens", [])
        highs = data.get("highs", [])
        lows = data.get("lows", [])
        closes = data.get("closes", [])
        volumes = data.get("volumes", [])
        dates = data.get("dates", [])

        # Identify feature columns (keys that are not core OHLCV/date fields)
        _core_keys = {"closes", "highs", "lows", "dates", "opens", "volumes"}
        feature_keys = [k for k in data if k not in _core_keys]

        results = []
        for i, bar_date in enumerate(dates):
            date_str = str(bar_date)

            # Extract per-bar feature values from parallel arrays
            bar_features: dict[str, Any] = {}
            for fk in feature_keys:
                col = data[fk]
                if isinstance(col, list) and i < len(col):
                    bar_features[fk] = col[i]

            # Build ScanRuleContext
            context = ScanRuleContext(
                secid=secid,
                display_symbol=display_symbol,
                ticker_at_date=ticker_at_date,
                identity_source=identity_source,
                runtime_symbol=ticker,
                date=date_str,
                bar_index=i,
                dates=dates,
                open=opens,
                high=highs,
                low=lows,
                close=closes,
                volume=volumes,
                features=bar_features,
                feature_columns=feature_keys,
                data_source=self._data_source,
                price_basis=self._price_basis,
                parameters=self._parameters,
                parameter_hash=self._parameter_hash,
            )

            # Evaluate candidate rule — prefer new context contract,
            # fall back to legacy tuple-return through adapter.
            # Also detect legacy 3-arg call shape: (secid, date_str, features).
            sig = inspect.signature(self._candidate_rule)
            param_count = len([
                p for p in sig.parameters.values()
                if p.default is inspect.Parameter.empty
                and p.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
            ])

            if param_count == 3:
                # Legacy 3-arg call shape: (secid, date_str, features)
                raw_result = self._candidate_rule(
                    secid or 0,
                    date_str,
                    bar_features,
                )
            else:
                # New context call shape
                raw_result = self._candidate_rule(context)

            # Convert to ScanDecision
            if isinstance(raw_result, ScanDecision):
                decision = raw_result
            elif isinstance(raw_result, tuple):
                decision = decision_from_tuple(raw_result)
            elif isinstance(raw_result, dict):
                decision = ScanDecision(**raw_result)
            else:
                raise TypeError(
                    f"Candidate rule must return ScanDecision, dict, or tuple, "
                    f"got {type(raw_result).__name__}."
                )

            # Compute scan metrics (unchanged)
            metrics = compute_scan_metrics(closes, highs, lows, i, self._horizons)

            result = ScanResult(
                date=date_str,
                secid=secid,
                display_symbol=display_symbol,
                ticker_at_date=ticker_at_date,
                runtime_symbol=ticker,
                strategy_id=self._strategy_id,
                candidate_status=decision.candidate_status,
                reason_code=decision.reason_code,
                score=decision.score,
                gates=decision.gates,
                features=decision.features,
                diagnostics=decision.diagnostics,
                forward_return_5d=metrics.get("forward_return_5d"),
                forward_return_10d=metrics.get("forward_return_10d"),
                forward_return_20d=metrics.get("forward_return_20d"),
                mfe_20d=metrics.get("mfe_20d"),
                mae_20d=metrics.get("mae_20d"),
            )
            results.append(result)

        return results

    @staticmethod
    def _persist_results(
        results: list[ScanResult],
        path: Path,
    ) -> Path | None:
        """Persist scan results to CSV."""
        if not results:
            logger.debug("no_scan_results_to_persist")
            return None

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_SCAN_RESULT_COLUMNS, extrasaction="ignore")
            writer.writeheader()
            for r in results:
                row = {
                    "date": r.date,
                    "secid": r.secid if r.secid is not None else "",
                    "display_symbol": r.display_symbol,
                    "ticker_at_date": r.ticker_at_date,
                    "runtime_symbol": r.runtime_symbol,
                    "strategy_id": r.strategy_id,
                    "candidate_status": r.candidate_status,
                    "reason_code": r.reason_code,
                    "score": r.score if r.score is not None else "",
                    "gates_json": json.dumps(r.gates) if r.gates else "",
                    "features_json": json.dumps(r.features) if r.features else "",
                    "diagnostics_json": json.dumps(r.diagnostics) if r.diagnostics else "",
                    "forward_return_5d": _fmt_float(r.forward_return_5d),
                    "forward_return_10d": _fmt_float(r.forward_return_10d),
                    "forward_return_20d": _fmt_float(r.forward_return_20d),
                    "mfe_20d": _fmt_float(r.mfe_20d),
                    "mae_20d": _fmt_float(r.mae_20d),
                }
                writer.writerow(row)

        logger.info("scan_results_persisted", path=str(path), count=len(results))
        return path

    def _write_manifest(
        self,
        output_dir: Path,
        resolved: dict[str, Any],
        summary: ScanSummary,
        date_range: tuple[date, date],
        tickers: list[str],
        secids: list[int] | None,
        ticker_policy: str,
        filename: str,
    ) -> Path | None:
        """Write scan manifest JSON to the output directory."""
        manifest = self._build_manifest(
            resolved=resolved,
            summary=summary,
            date_range=date_range,
            tickers=tickers,
            secids=secids,
            ticker_policy=ticker_policy,
            filename=filename,
        )

        manifest_path = output_dir / "scan_manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=2, default=str)

        logger.info("scan_manifest_written", path=str(manifest_path))
        return manifest_path

    def _build_manifest(
        self,
        resolved: dict[str, Any],
        summary: ScanSummary,
        date_range: tuple[date, date],
        tickers: list[str],
        secids: list[int] | None,
        ticker_policy: str,
        filename: str,
    ) -> dict[str, Any]:
        """Build a scan manifest dictionary."""
        # Build resolved instruments list
        resolved_instruments = []
        for key, instrument in resolved.items():
            instrument_entry: dict[str, Any] = {
                "key": key,
                "secid": getattr(instrument, "secid", None),
                "display_symbol": getattr(instrument, "display_symbol", key),
                "ticker_at_date": getattr(instrument, "ticker_at_date", key),
                "identity_source": getattr(instrument, "identity_source", "ticker"),
            }

            # Capture ticker history if available
            ticker_history = getattr(instrument, "ticker_history", None)
            if ticker_history:
                instrument_entry["ticker_history"] = ticker_history

            # Capture resolution policy
            instrument_entry["resolution_policy"] = ticker_policy

            # Capture coverage dates from ResolvedInstrument attributes
            first_date = getattr(instrument, "first_date", None)
            last_date = getattr(instrument, "last_date", None)
            requested_start = getattr(instrument, "requested_start_date", None)
            requested_end = getattr(instrument, "requested_end_date", None)
            if first_date or last_date:
                instrument_entry["coverage_dates"] = {
                    "first_date": str(first_date) if first_date else None,
                    "last_date": str(last_date) if last_date else None,
                }
            if requested_start or requested_end:
                instrument_entry["requested_range"] = {
                    "start_date": str(requested_start) if requested_start else None,
                    "end_date": str(requested_end) if requested_end else None,
                }

            # Capture ambiguity and candidate mappings from ResolvedInstrument
            ambiguous = getattr(instrument, "ambiguous", None)
            if ambiguous is not None:
                instrument_entry["ambiguous"] = bool(ambiguous)

            candidates = getattr(instrument, "candidates", None)
            if candidates:
                instrument_entry["candidates"] = [
                    {
                        "secid": getattr(c, "secid", None),
                        "display_symbol": getattr(c, "display_symbol", None),
                        "overlap_start": str(getattr(c, "overlap_start", "")) if getattr(c, "overlap_start", None) else None,
                        "overlap_end": str(getattr(c, "overlap_end", "")) if getattr(c, "overlap_end", None) else None,
                    }
                    for c in candidates
                ]

            resolved_instruments.append(instrument_entry)

        return {
            "schema_version": "1.0",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "rule": {
                "strategy_id": self._strategy_id,
                "rule_import_path": self._rule_import_path,
                "parameter_snapshot": self._parameters,
                "parameter_hash": self._parameter_hash,
            },
            "data": {
                "data_source": self._data_source,
                "database": self._database,
                "bars_table": self._bars_table,
                "source_columns": self._source_columns or {},
                "price_basis": self._price_basis,
            },
            "date_range": {
                "start_date": str(date_range[0]),
                "end_date": str(date_range[1]),
            },
            "universe": {
                "requested_tickers": tickers,
                "requested_secids": secids or [],
                "ticker_policy": ticker_policy,
            },
            "resolved_instruments": resolved_instruments,
            "output_files": {
                "results_csv": filename,
                "manifest_json": "scan_manifest.json",
            },
            "summary": {
                "total_instruments": summary.total_instruments,
                "instruments_processed": summary.instruments_processed,
                "instruments_failed": summary.instruments_failed,
                "total_rows": summary.total_rows,
                "failures": summary.failures,
            },
        }


def _fmt_float(value: float | None) -> str:
    """Format a float for CSV, handling NaN."""
    if value is None:
        return ""
    if math.isnan(value):
        return ""
    return str(value)


class _MinimalInstrument:
    """Fallback instrument when resolution is unavailable."""

    def __init__(
        self,
        secid: int | None,
        display_symbol: str,
        ticker_at_date: str,
        runtime_symbol: str,
    ):
        self.secid = secid
        self.display_symbol = display_symbol
        self.ticker_at_date = ticker_at_date
        self.runtime_symbol = runtime_symbol
