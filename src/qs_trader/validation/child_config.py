"""Pure derivation of a child BacktestConfig from a ValidationPlan split."""

from __future__ import annotations

from datetime import datetime, time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qs_trader.engine.config import BacktestConfig

from qs_trader.validation.plan import ValidationPlan
from qs_trader.validation.splits.base import ValidationSplit


def derive_child_config(
    plan: ValidationPlan,
    split: ValidationSplit,
    base_config: BacktestConfig,
) -> BacktestConfig:
    """Return a copy of ``base_config`` with ``start_date``/``end_date`` overridden from the split.

    The returned :class:`~qs_trader.engine.config.BacktestConfig` is a new model
    instance (``model_copy``) with:

    - ``start_date`` set to ``split.test_range.start_date`` (converted to ``datetime`` at midnight).
    - ``end_date`` set to ``split.test_range.end_date`` (converted to ``datetime`` at midnight).
    - All other fields preserved unchanged from ``base_config``.

    Note on ``validation_context``:
        :class:`~qs_trader.engine.config.BacktestConfig` has no free-form extras
        field, so the split role, fold index, and validation ID are **not** injected
        here.  The ``ValidationRunner`` (Phase 1.2) is responsible for writing these
        into ``RunMetadata.metrics['validation_context']``.

    Args:
        plan: The loaded and validated :class:`~qs_trader.validation.plan.ValidationPlan`.
              Included for forward-compatibility with the Phase 1.2 runner signature;
              unused in Phase 1.1.
        split: The :class:`~qs_trader.validation.splits.base.ValidationSplit` whose
               ``test_range.start_date``/``test_range.end_date`` define the child run's
               date window.  The :class:`~qs_trader.validation.plan.DateRange` invariant
               guarantees ``end_date > start_date`` before this function is called.
        base_config: The base :class:`~qs_trader.engine.config.BacktestConfig` to
                     derive from.  Not mutated.

    Returns:
        A new :class:`~qs_trader.engine.config.BacktestConfig` instance with overridden
        date fields.
    """
    start_dt = datetime.combine(split.test_range.start_date, time.min)
    end_dt = datetime.combine(split.test_range.end_date, time.min)

    return base_config.model_copy(update={
        "start_date": start_dt,
        "end_date": end_dt,
        "backtest_id": plan.strategy_experiment,
    })
