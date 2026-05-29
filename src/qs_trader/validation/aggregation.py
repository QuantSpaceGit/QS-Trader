"""Metric aggregation for the QS-Trader OOS validation framework.

Builds IS/OOS comparison rows with decay deltas from per-fold metric dicts.
Full-period comparison (``full`` field) is deferred to Phase 1.4 and always
``None`` in this module.
"""

from __future__ import annotations

import statistics
from dataclasses import dataclass
from typing import TYPE_CHECKING

import structlog

from qs_trader.validation.plan import MetricsCatalog

if TYPE_CHECKING:
    from qs_trader.validation.decision import ValidationDecision

logger = structlog.get_logger(__name__)

_DECAY_EPSILON: float = 1e-6

__all__ = [
    "FoldAggregates",
    "MetricComparison",
    "MetricsAggregator",
    "WalkForwardAggregator",
]


@dataclass(frozen=True)
class MetricComparison:
    """IS/OOS comparison for a single metric, with decay delta.

    The ``full`` field is always ``None`` in Phase 1; full-period computation
    is deferred to Phase 1.4 and requires a third engine fold.

    Attributes:
        is_val: In-sample metric value (field name ``is`` in JSON output, but
                ``is`` is a Python keyword, so the Python attribute is ``is_val``).
        oos: Out-of-sample metric value.
        full: Full-period metric value. Always ``None`` in Phase 1 (not yet
              computed).
        decay: IS→OOS decay: ``(is_val - oos) / max(abs(is_val), ε)``. ``None``
               when either ``is_val`` or ``oos`` is ``None``.
    """

    # NOTE: serialization to the JSON key "is" (per §4.3 summary.json schema) must be
    # handled explicitly at the summary writer layer (Phase 1.4), because dataclasses.asdict()
    # will produce the key "is_val". See Phase 1.4 task for the required adapter.
    is_val: float | None
    oos: float | None
    full: float | None
    decay: float | None


def _compute_decay(is_val: float | None, oos_val: float | None) -> float | None:
    """Compute IS→OOS decay with epsilon guard (R3).

    Formula: ``(is_val - oos) / max(abs(is_val), ε)`` where ε = 1e-6.
    Returns ``None`` when either side is ``None``.
    """
    if is_val is None or oos_val is None:
        return None
    return (is_val - oos_val) / max(abs(is_val), _DECAY_EPSILON)


class MetricsAggregator:
    """Aggregates per-fold metric dicts into IS/OOS comparison rows.

    The ``full`` field of each :class:`MetricComparison` is always ``None``;
    full-period computation requires a third engine fold and is deferred to
    Phase 1.4.

    Usage::

        aggregator = MetricsAggregator()
        comparison = aggregator.aggregate(is_metrics, oos_metrics, catalog)
    """

    def aggregate(
        self,
        is_metrics: dict[str, float],
        oos_metrics: dict[str, float],
        metric_catalog: MetricsCatalog,
    ) -> dict[str, MetricComparison]:
        """Build a comparison dict from IS and OOS metric dicts.

        For each metric in the catalog (required + recommended), extract values
        from ``is_metrics`` and ``oos_metrics``, compute decay, and produce a
        :class:`MetricComparison`.  Metrics absent from a dict produce ``None``
        for that side.  Metrics in the dicts but not in the catalog are silently
        ignored.  The ``full`` field is always ``None`` (deferred to Phase 1.4).

        Args:
            is_metrics: Per-metric float values for the in-sample fold.
            oos_metrics: Per-metric float values for the out-of-sample fold.
            metric_catalog: Catalog specifying which metrics to aggregate.

        Returns:
            Dict mapping metric name → :class:`MetricComparison`.
        """
        result: dict[str, MetricComparison] = {}
        seen: set[str] = set()

        for name in (*metric_catalog.required, *metric_catalog.recommended):
            if name in seen:
                continue
            seen.add(name)

            is_val = is_metrics.get(name)
            oos_val = oos_metrics.get(name)
            decay = _compute_decay(is_val, oos_val)

            result[name] = MetricComparison(
                is_val=is_val,
                oos=oos_val,
                full=None,
                decay=decay,
            )
            logger.debug(
                "metric_aggregated",
                metric=name,
                is_val=is_val,
                oos=oos_val,
                decay=decay,
            )

        return result


# ---------------------------------------------------------------------------
# Walk-forward cross-fold aggregation (Phase 2A.4)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FoldAggregates:
    """Cross-fold aggregate statistics for a single OOS metric.

    Attributes:
        metric: The metric name that was aggregated (e.g. ``'sharpe_ratio'``).
        median: Median of OOS metric values across folds with a successful OOS
                run.  ``None`` when no successful OOS runs were available.
        iqr: Interquartile range (Q3 − Q1) of the OOS values.  ``0.0`` when
             exactly one fold contributed a value.  ``None`` when no successful
             OOS runs were available.
        min: Minimum OOS metric value.  ``None`` when no successful OOS runs.
        max: Maximum OOS metric value.  ``None`` when no successful OOS runs.
        count_pass_folds: Number of folds whose per-fold decision outcome was
                          ``'Pass'``.
        count_total_folds: Total folds attempted (includes failed/invalid folds).
    """

    metric: str
    median: float | None
    iqr: float | None
    min: float | None
    max: float | None
    count_pass_folds: int
    count_total_folds: int


def _iqr(sorted_vals: list[float]) -> float:
    """Compute Q3 − Q1.  Returns ``0.0`` for a single-element list."""
    n = len(sorted_vals)
    if n <= 1:
        return 0.0
    q1, _, q3 = statistics.quantiles(sorted_vals, n=4)
    return q3 - q1


class WalkForwardAggregator:
    """Aggregates per-fold metric comparisons across a walk-forward run.

    The aggregator collects OOS values for a single configurable metric from
    each fold's :class:`MetricComparison` dict.  Folds that did not produce a
    value for the metric (e.g. the OOS run failed) contribute to
    ``count_total_folds`` but not to the statistical aggregates.

    Usage::

        agg = WalkForwardAggregator()
        fa = agg.aggregate(fold_comparisons, fold_decisions, metric="sharpe_ratio")
    """

    def aggregate(
        self,
        fold_comparisons: list[dict[str, MetricComparison]],
        fold_decisions: list[ValidationDecision],
        metric: str = "sharpe_ratio",
    ) -> FoldAggregates:
        """Compute cross-fold aggregate statistics.

        Args:
            fold_comparisons: One comparison dict per fold (IS/OOS pair), as
                produced by :class:`MetricsAggregator`.  The list must be the
                same length as ``fold_decisions``.
            fold_decisions: Per-fold :class:`~qs_trader.validation.decision.ValidationDecision`
                objects.  Each decision's ``outcome`` is checked against
                ``'Pass'`` to determine ``count_pass_folds``.
            metric: The metric key to aggregate across OOS values (default
                    ``'sharpe_ratio'``).

        Returns:
            A :class:`FoldAggregates` instance.  When no OOS value is available
            for the metric across any fold, ``median``, ``iqr``, ``min``, and
            ``max`` are all ``None``.
        """
        count_total = len(fold_comparisons)
        count_pass = sum(1 for d in fold_decisions if d.outcome == "Pass")

        oos_values: list[float] = []
        for comp in fold_comparisons:
            mc = comp.get(metric)
            if mc is not None and mc.oos is not None:
                oos_values.append(mc.oos)

        if not oos_values:
            return FoldAggregates(
                metric=metric,
                median=None,
                iqr=None,
                min=None,
                max=None,
                count_pass_folds=count_pass,
                count_total_folds=count_total,
            )

        sorted_vals = sorted(oos_values)
        n = len(sorted_vals)
        mid = n // 2
        if n % 2 == 0:
            median = (sorted_vals[mid - 1] + sorted_vals[mid]) / 2.0
        else:
            median = sorted_vals[mid]

        return FoldAggregates(
            metric=metric,
            median=median,
            iqr=_iqr(sorted_vals),
            min=sorted_vals[0],
            max=sorted_vals[-1],
            count_pass_folds=count_pass,
            count_total_folds=count_total,
        )
