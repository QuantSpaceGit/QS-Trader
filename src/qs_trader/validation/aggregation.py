"""Metric aggregation for the QS-Trader OOS validation framework.

Builds IS/OOS comparison rows with decay deltas from per-fold metric dicts.
Full-period comparison (``full`` field) is deferred to Phase 1.4 and always
``None`` in this module.
"""

from __future__ import annotations

from dataclasses import dataclass

import structlog

from qs_trader.validation.plan import MetricsCatalog

logger = structlog.get_logger(__name__)

_DECAY_EPSILON: float = 1e-6

__all__ = [
    "MetricComparison",
    "MetricsAggregator",
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
