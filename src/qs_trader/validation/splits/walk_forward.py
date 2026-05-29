"""Walk-forward split generator (anchored and rolling modes)."""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from qs_trader.validation.plan import DateRange, WalkForwardSplitsSpec, parse_duration
from qs_trader.validation.splits.base import ValidationSplit

if TYPE_CHECKING:
    from qs_trader.validation.plan import ValidationPlan


class WalkForwardSplitGenerator:
    """Generate walk-forward validation splits from a ``walk_forward`` plan.

    Supports two styles:

    - **anchored**: train start is fixed; train end and test window advance by
      ``step`` each fold.
    - **rolling**: train start, train end, and test window all advance by
      ``step`` each fold (fixed train duration).

    Fold generation stops when ``test_range.end > total_range.end``.

    Each fold produces a pair of :class:`~qs_trader.validation.splits.base.ValidationSplit`:

    - ``role='train'`` — the training window for that fold.
    - ``role='oos'``   — the out-of-sample test window, with ``train_range`` set
      for traceability.

    If ``min_fold_bars`` is configured and a test window spans fewer calendar
    days, both splits for that fold are returned with ``status='invalid'`` and
    ``reason='insufficient_history_for_fold:<fold_index>'``.  No exception is
    raised; the caller (runner) handles invalid folds.
    """

    def generate(self, plan: "ValidationPlan") -> list[ValidationSplit]:
        """Generate ordered walk-forward splits for the given plan.

        Args:
            plan: A validated :class:`~qs_trader.validation.plan.ValidationPlan`
                  with ``mode='walk_forward'``.

        Returns:
            Ordered list of :class:`~qs_trader.validation.splits.base.ValidationSplit`
            objects: alternating ``train`` and ``oos`` splits per fold.

        Raises:
            ValueError: If ``plan.mode`` is not ``'walk_forward'``.
        """
        if plan.mode != "walk_forward":
            raise ValueError(f"WalkForwardSplitGenerator only supports mode='walk_forward'; got '{plan.mode}'")

        assert isinstance(plan.splits, WalkForwardSplitsSpec)
        spec: WalkForwardSplitsSpec = plan.splits

        total_range = spec.total_range
        train_dur = parse_duration(spec.train)
        test_dur = parse_duration(spec.test)
        step_dur = parse_duration(spec.step)
        embargo_dur = parse_duration(spec.embargo)

        splits: list[ValidationSplit] = []

        # Initialise first fold's train window
        fold_train_start = total_range.start_date
        fold_train_end = total_range.start_date + train_dur - timedelta(days=1)

        fold_index = 0
        while True:
            # Compute test window
            test_start = fold_train_end + embargo_dur + timedelta(days=1)
            test_end = test_start + test_dur - timedelta(days=1)

            # Stop when test window exceeds total range
            if test_end > total_range.end_date:
                break

            # Determine status / reason for min_fold_bars enforcement
            fold_status: str | None = None
            fold_reason: str | None = None
            if spec.min_fold_bars is not None:
                test_days = (test_end - test_start).days + 1
                if test_days < spec.min_fold_bars:
                    fold_status = "invalid"
                    fold_reason = f"insufficient_history_for_fold:{fold_index}"

            train_range = DateRange(start_date=fold_train_start, end_date=fold_train_end)
            test_range = DateRange(start_date=test_start, end_date=test_end)
            # Actual calendar-day embargo gap (days between train_end and test_start exclusive)
            embargo_delta = timedelta(days=(test_start - fold_train_end).days - 1)

            splits.append(
                ValidationSplit(
                    fold_index=fold_index,
                    role="train",
                    test_range=train_range,
                    train_range=None,
                    embargo=timedelta(0),
                    status=fold_status,
                    reason=fold_reason,
                )
            )
            splits.append(
                ValidationSplit(
                    fold_index=fold_index,
                    role="oos",
                    test_range=test_range,
                    train_range=train_range,
                    embargo=embargo_delta,
                    status=fold_status,
                    reason=fold_reason,
                )
            )

            fold_index += 1

            # Advance windows for next fold
            if spec.style == "rolling":
                fold_train_start = fold_train_start + step_dur
                fold_train_end = fold_train_end + step_dur
            else:  # anchored: train start stays fixed, train end expands by step
                fold_train_end = fold_train_end + step_dur

        return splits
