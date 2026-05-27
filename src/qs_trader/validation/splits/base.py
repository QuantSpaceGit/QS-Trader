"""Base types for the split-generation protocol."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Literal, Protocol

from qs_trader.validation.plan import DateRange

if TYPE_CHECKING:
    from qs_trader.validation.plan import ValidationPlan


@dataclass(frozen=True)
class ValidationSplit:
    """A single period fold within a validation run.

    Attributes:
        fold_index: Zero-based index of this fold in the ordered split list.
        role: Semantic role of the split period.  One of:
              ``'is'`` (in-sample), ``'oos'`` (out-of-sample),
              ``'holdout'``, or ``'warmup_only'``.
        test_range: Inclusive date range of the evaluation (test) period.
                    ``start_date`` and ``end_date`` are guaranteed strictly ordered
                    by :class:`~qs_trader.validation.plan.DateRange`.
        train_range: Inclusive date range of the training period, or ``None``
                     when the training window is implicit (e.g. the corresponding
                     IS fold for an OOS split).
        embargo: Gap between the training end and the test start to prevent
                 look-ahead contamination.  Defaults to ``timedelta(0)``.
    """

    fold_index: int
    role: Literal["is", "oos", "holdout", "warmup_only"]
    test_range: DateRange
    train_range: DateRange | None = None
    embargo: timedelta = timedelta(0)


class SplitGenerator(Protocol):
    """Protocol for split generator implementations.

    Phase 1 provides :class:`~qs_trader.validation.splits.static.StaticSplitGenerator`.
    Walk-forward and other generators plug in via this protocol in Phase 2+.
    """

    def generate(self, plan: "ValidationPlan") -> list[ValidationSplit]:
        """Generate the ordered list of validation splits for the given plan.

        Args:
            plan: The loaded and validated :class:`~qs_trader.validation.plan.ValidationPlan`.

        Returns:
            Ordered list of :class:`ValidationSplit` objects.
        """
        ...
