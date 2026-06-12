"""Candidate scan runner.

Iterates over a universe of instruments, resolves them via InstrumentResolver,
evaluates candidate rules, computes forward returns / MFE / MAE, and persists
results to CSV.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable

import structlog

from qs_trader.services.scan.calculations import compute_scan_metrics

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Scan result schema
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
        data_loader: Callable(secid) -> dict with "closes", "highs", "lows", "dates".
        candidate_rule: Callable returning (status, reason_code, score, gates, features).
        strategy_id: Strategy identifier for attribution.
        horizons: Forward return horizons in bars.
    """

    def __init__(
        self,
        instrument_resolver: Any,
        data_loader: Callable[[int], dict[str, Any]],
        candidate_rule: Callable[
            [int, str, dict[str, Any]],
            tuple[str, str, float | None, dict[str, Any], dict[str, Any]],
        ],
        strategy_id: str,
        horizons: list[int] | None = None,
    ):
        self._resolver = instrument_resolver
        self._data_loader = data_loader
        self._candidate_rule = candidate_rule
        self._strategy_id = strategy_id
        self._horizons = horizons or [5, 10, 20]

    def run(
        self,
        tickers: list[str],
        date_range: tuple[date, date],
        output_dir: Path,
        filename: str = "candidate_scan_results.csv",
        ticker_policy: str = "anchor_first_in_range",
    ) -> tuple[list[ScanResult], ScanSummary]:
        """Execute the scan over the given tickers and date range.

        Args:
            tickers: List of ticker symbols to scan.
            date_range: (start_date, end_date) for the scan.
            output_dir: Directory to write results.
            filename: Output CSV filename.
            ticker_policy: Resolution policy for InstrumentResolver.

        Returns:
            (results, summary) tuple.
        """
        results: list[ScanResult] = []
        summary = ScanSummary(total_instruments=len(tickers))

        # Batch resolve tickers
        resolved = self._resolve_batch(tickers, date_range, ticker_policy, summary)

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

        logger.info(
            "scan.completed",
            processed=summary.instruments_processed,
            failed=summary.instruments_failed,
            total_rows=summary.total_rows,
        )

        return results, summary

    def _resolve_batch(
        self,
        tickers: list[str],
        date_range: tuple[date, date],
        policy: str,
        summary: ScanSummary,
    ) -> dict[str, Any]:
        """Resolve tickers to instruments via InstrumentResolver."""
        if self._resolver is None:
            # Fallback: create minimal resolved instruments without secid
            return {
                t: _MinimalInstrument(
                    secid=None,
                    display_symbol=t,
                    ticker_at_date=t,
                    runtime_symbol=t,
                )
                for t in tickers
            }

        try:
            return self._resolver.resolve_batch(tickers, date_range, policy)
        except Exception as e:
            logger.error("scan.batch_resolution_failed", error=str(e))
            # Fall back to unresolved
            return {
                t: _MinimalInstrument(
                    secid=None,
                    display_symbol=t,
                    ticker_at_date=t,
                    runtime_symbol=t,
                )
                for t in tickers
            }

    def _scan_instrument(
        self,
        ticker: str,
        instrument: Any,
    ) -> list[ScanResult]:
        """Scan a single instrument across its date range."""
        secid = getattr(instrument, "secid", None)
        display_symbol = getattr(instrument, "display_symbol", ticker)
        ticker_at_date = getattr(instrument, "ticker_at_date", ticker)

        # Load price data
        data = self._data_loader(secid if secid is not None else ticker)
        closes = data.get("closes", [])
        highs = data.get("highs", [])
        lows = data.get("lows", [])
        dates = data.get("dates", [])

        results = []
        for i, bar_date in enumerate(dates):
            date_str = str(bar_date)

            # Evaluate candidate rule
            features: dict[str, Any] = {}
            status, reason_code, score, gates, features = self._candidate_rule(
                secid or 0,
                date_str,
                features,
            )

            # Compute scan metrics
            metrics = compute_scan_metrics(closes, highs, lows, i, self._horizons)

            result = ScanResult(
                date=date_str,
                secid=secid,
                display_symbol=display_symbol,
                ticker_at_date=ticker_at_date,
                runtime_symbol=ticker,
                strategy_id=self._strategy_id,
                candidate_status=status,
                reason_code=reason_code,
                score=score,
                gates=gates,
                features=features,
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
        import json

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
                    "forward_return_5d": _fmt_float(r.forward_return_5d),
                    "forward_return_10d": _fmt_float(r.forward_return_10d),
                    "forward_return_20d": _fmt_float(r.forward_return_20d),
                    "mfe_20d": _fmt_float(r.mfe_20d),
                    "mae_20d": _fmt_float(r.mae_20d),
                }
                writer.writerow(row)

        logger.info("scan_results_persisted", path=str(path), count=len(results))
        return path


def _fmt_float(value: float | None) -> str:
    """Format a float for CSV, handling NaN."""
    if value is None:
        return ""
    import math

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
