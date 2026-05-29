"""Benchmark overlay helpers for the Phase 2A.3 OOS validation framework.

The benchmark overlay is a single synthetic buy-and-hold child run executed
over a validation plan's full range on a user-declared instrument.  The same
engine machinery (cost model, calendar, performance metrics) used for fold
child runs is reused so the benchmark equity curve is directly comparable.

Public API:

* :class:`BenchmarkDataUnavailableError` — typed exception raised by the
  pre-flight data-availability check.  Carries the instrument string and is
  caught by the CLI to emit the ``benchmark_data_unavailable:<instrument>``
  reason code (exit 3).
* :func:`derive_benchmark_child_config` — pure function returning a new
  :class:`~qs_trader.engine.config.BacktestConfig` for the benchmark child
  run.  Does not mutate the input config.
* :func:`benchmark_full_range` — resolve the full validation date range for
  static / walk-forward plans.
* :func:`check_benchmark_data_availability` — pre-flight check that the
  declared benchmark instrument has price coverage spanning the full range
  in the data source declared by the base config.
"""

from __future__ import annotations

from datetime import date, datetime, time
from typing import TYPE_CHECKING, Callable, Iterable

import structlog

from qs_trader.validation.plan import (
    BenchmarkSpec,
    DateRange,
    StaticSplitSpec,
    ValidationPlan,
    WalkForwardSplitsSpec,
)

if TYPE_CHECKING:
    from qs_trader.engine.config import BacktestConfig

logger = structlog.get_logger(__name__)

__all__ = [
    "BENCHMARK_FOLD_ID",
    "BENCHMARK_ROLE",
    "BenchmarkDataUnavailableError",
    "benchmark_full_range",
    "check_benchmark_data_availability",
    "derive_benchmark_child_config",
]

# Fixed identifiers for the benchmark child run.  Used by the runner so the
# benchmark ChildRunRef is structurally distinguishable from fold refs in the
# summary writer and the CLI.
BENCHMARK_FOLD_ID = "benchmark"
BENCHMARK_ROLE = "benchmark"


class BenchmarkDataUnavailableError(Exception):
    """Raised when the benchmark instrument lacks coverage for the full range.

    Attributes:
        instrument: The declared benchmark instrument that failed the check.
        full_range: The validation full range that was checked.
        detail: Optional human-readable detail (e.g. partial-coverage span).
    """

    def __init__(self, instrument: str, full_range: DateRange, detail: str | None = None) -> None:
        message = (
            f"Benchmark data unavailable for instrument {instrument!r} over "
            f"{full_range.start_date} \u2192 {full_range.end_date}"
        )
        if detail:
            message = f"{message}: {detail}"
        super().__init__(message)
        self.instrument = instrument
        self.full_range = full_range
        self.detail = detail


def benchmark_full_range(plan: ValidationPlan) -> DateRange:
    """Return the full validation date range for a plan.

    For ``static_is_oos`` plans this is the union ``[in_sample.start_date,
    out_of_sample.end_date]``.  For ``walk_forward`` plans it is
    ``splits.total_range`` verbatim.
    """
    splits = plan.splits
    if isinstance(splits, StaticSplitSpec):
        return DateRange(
            start_date=splits.in_sample.start_date,
            end_date=splits.out_of_sample.end_date,
        )
    if isinstance(splits, WalkForwardSplitsSpec):
        return splits.total_range
    raise TypeError(f"Unsupported splits type for benchmark range: {type(splits).__name__}")


def derive_benchmark_child_config(
    plan: ValidationPlan,
    full_range: DateRange,
    base_config: "BacktestConfig",
) -> "BacktestConfig":
    """Build a benchmark child :class:`BacktestConfig` from the plan + base config.

    The returned config reuses the base config's data source name, calendar
    settings, starting equity, risk policy, price basis, and reporting block,
    but overrides:

    * ``data.sources[0].universe`` → ``[plan.benchmark.instrument]``
    * ``strategies`` → single :class:`StrategyConfigItem` for the buy-and-hold
      strategy declared on the plan (registry id = ``plan.benchmark.strategy``)
      configured for the single benchmark instrument with
      ``reinvest_dividends`` propagated from the plan.
    * ``start_date`` / ``end_date`` → ``full_range`` (00:00 UTC midnight).
    * ``backtest_id`` → ``"<original_id>__benchmark"`` so on-disk artifacts are
      labelled.

    The input ``base_config`` is not mutated.  Multi-source backtest configs
    are not supported in Phase 2A.3: the engine itself rejects them at
    ``BacktestConfig`` validation time, so we only need to handle the
    single-source case here.

    Raises:
        ValueError: If ``plan.benchmark`` is ``None``.
    """
    from qs_trader.engine.config import (  # noqa: PLC0415
        BacktestConfig,
        DataSelectionConfig,
        DataSourceConfig,
        StrategyConfigItem,
    )

    if plan.benchmark is None:
        raise ValueError("derive_benchmark_child_config requires plan.benchmark to be declared")

    bench: BenchmarkSpec = plan.benchmark
    if not base_config.data.sources:
        raise ValueError("Base BacktestConfig has no data sources; cannot derive benchmark child config")

    primary_source = base_config.data.sources[0]
    data_override = DataSelectionConfig(
        sources=[DataSourceConfig(name=primary_source.name, universe=[bench.instrument])]
    )

    strategy_override = StrategyConfigItem(
        strategy_id=bench.strategy,
        universe=[bench.instrument],
        data_sources=[primary_source.name],
        config={"reinvest_dividends": bench.reinvest_dividends},
    )

    start_dt = datetime.combine(full_range.start_date, time.min)
    end_dt = datetime.combine(full_range.end_date, time.min)

    update: dict = {
        "backtest_id": f"{base_config.backtest_id}__benchmark",
        "start_date": start_dt,
        "end_date": end_dt,
        "data": data_override,
        "strategies": [strategy_override],
        # Benchmark child must not inherit sleeve-bound constraints; the
        # benchmark universe is a different single symbol.
        "sleeve": None,
        # Clear IS/OOS split metadata — the benchmark spans the full range.
        "split_pct": None,
        "split_role": None,
    }
    return BacktestConfig.model_validate({**base_config.model_dump(mode="python"), **update})


# ---------------------------------------------------------------------------
# Pre-flight data-availability check
# ---------------------------------------------------------------------------


# Loader protocol: given (source_name, instrument, start_date, end_date) return
# an iterable of bar timestamps (anything supporting ``__iter__``).  Empty
# iterable / iterator => no data.  The loader is injectable so tests can stub
# it without touching the data adapter layer.
BenchmarkBarLoader = Callable[[str, str, date, date], Iterable[date]]


def _default_benchmark_loader(
    source_name: str,
    instrument: str,
    start_date: date,
    end_date: date,
) -> Iterable[date]:
    """Default loader: resolve via :class:`DataSourceResolver` and read bars.

    Returns an iterable of bar timestamps (``datetime`` or ``date`` objects).
    On any adapter failure returns an empty list so the pre-flight check
    surfaces ``benchmark_data_unavailable``.  The check function downstream
    will compare min/max bar dates against the requested range.
    """
    try:
        from qs_trader.services.data.adapters.resolver import DataSourceResolver  # noqa: PLC0415
        from qs_trader.services.data.models import Instrument  # noqa: PLC0415
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("benchmark.loader.import_failed", error=str(exc))
        return []

    try:
        resolver = DataSourceResolver()
        adapter = resolver.resolve_by_dataset(source_name, Instrument(symbol=instrument))
    except Exception as exc:
        logger.warning(
            "benchmark.loader.resolve_failed",
            source=source_name,
            instrument=instrument,
            error=str(exc),
        )
        return []

    # Prefer a ``read_bars`` / ``load`` API on the adapter; otherwise return
    # empty.  Concrete adapters expose different surface areas; this loader is
    # the documented default fallback and tests should stub it.
    for method_name in ("read_bars", "load_bars", "load"):
        method = getattr(adapter, method_name, None)
        if callable(method):
            try:
                bars = method(start_date, end_date)
            except TypeError:
                try:
                    bars = method(instrument, start_date, end_date)
                except Exception as exc:
                    logger.warning("benchmark.loader.read_failed", error=str(exc))
                    return []
            except Exception as exc:
                logger.warning("benchmark.loader.read_failed", error=str(exc))
                return []
            # Extract timestamps if records carry a ``timestamp`` / ``date`` field.
            stamps: list[date] = []
            for bar in bars or []:
                ts = getattr(bar, "timestamp", None) or getattr(bar, "date", None) or bar
                if isinstance(ts, datetime):
                    stamps.append(ts.date())
                elif isinstance(ts, date):
                    stamps.append(ts)
            return stamps
    return []


def check_benchmark_data_availability(
    plan: ValidationPlan,
    full_range: DateRange,
    base_config: "BacktestConfig",
    *,
    loader: BenchmarkBarLoader | None = None,
) -> None:
    """Pre-flight check: confirm benchmark data spans the full range.

    Raises :class:`BenchmarkDataUnavailableError` when:

    * ``plan.benchmark`` is declared and the loader returns no bars at all, or
    * the loaded bars do not span the requested ``full_range`` (the earliest
      bar is after ``full_range.start_date`` or the latest bar is before
      ``full_range.end_date``).

    When ``plan.benchmark`` is ``None`` the function is a no-op.

    The default loader resolves the adapter via the QS-Trader data layer
    (:class:`~qs_trader.services.data.adapters.resolver.DataSourceResolver`).
    Tests inject a custom ``loader`` callable to avoid touching the adapter
    layer.
    """
    if plan.benchmark is None:
        return
    if not base_config.data.sources:
        raise BenchmarkDataUnavailableError(
            plan.benchmark.instrument,
            full_range,
            detail="base config has no data sources",
        )

    source_name = base_config.data.sources[0].name
    instrument = plan.benchmark.instrument
    bar_loader = loader or _default_benchmark_loader

    bars = list(bar_loader(source_name, instrument, full_range.start_date, full_range.end_date))
    if not bars:
        raise BenchmarkDataUnavailableError(
            instrument,
            full_range,
            detail=f"no bars returned from data source {source_name!r}",
        )
    min_bar = min(bars)
    max_bar = max(bars)
    if min_bar > full_range.start_date or max_bar < full_range.end_date:
        raise BenchmarkDataUnavailableError(
            instrument,
            full_range,
            detail=(
                f"partial coverage from {source_name!r}: bars span {min_bar} \u2192 {max_bar}, "
                f"required {full_range.start_date} \u2192 {full_range.end_date}"
            ),
        )
