"""Static IS/OOS split generator."""

from __future__ import annotations

from typing import TYPE_CHECKING

from qs_trader.validation.plan import StaticSplitSpec
from qs_trader.validation.splits.base import ValidationSplit

if TYPE_CHECKING:
    from qs_trader.validation.plan import ValidationPlan


class StaticSplitGenerator:
    """Generates exactly two splits from a ``static_is_oos`` validation plan.

    Produces:

    - **fold 0** — ``role='is'``, test range = ``plan.splits.in_sample``
    - **fold 1** — ``role='oos'``, test range = ``plan.splits.out_of_sample``

    Neither fold carries an explicit ``train_start``/``train_end``; the IS fold
    is self-describing, and the OOS fold's training window is understood by
    convention to be the IS fold's test range.
    """

    def generate(self, plan: "ValidationPlan") -> list[ValidationSplit]:
        """Generate ``[is, oos]`` splits from a ``static_is_oos`` plan.

        Args:
            plan: A validated :class:`~qs_trader.validation.plan.ValidationPlan`
                  with ``mode='static_is_oos'``.

        Returns:
            A list of exactly two :class:`~qs_trader.validation.splits.base.ValidationSplit`
            objects: the IS fold followed by the OOS fold.

        Raises:
            ValueError: If ``plan.mode`` is not ``'static_is_oos'``.
        """
        if plan.mode != "static_is_oos":
            raise ValueError(f"StaticSplitGenerator only supports mode='static_is_oos'; got '{plan.mode}'")

        assert isinstance(plan.splits, StaticSplitSpec)
        is_split = ValidationSplit(
            fold_index=0,
            role="is",
            test_range=plan.splits.in_sample,
        )

        oos_split = ValidationSplit(
            fold_index=1,
            role="oos",
            test_range=plan.splits.out_of_sample,
        )

        return [is_split, oos_split]
