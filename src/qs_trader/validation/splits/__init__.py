"""Split generation protocol and implementations."""

from __future__ import annotations

from typing import TYPE_CHECKING

from qs_trader.validation.splits.base import SplitGenerator, ValidationSplit
from qs_trader.validation.splits.static import StaticSplitGenerator
from qs_trader.validation.splits.walk_forward import WalkForwardSplitGenerator

if TYPE_CHECKING:
    from qs_trader.validation.plan import ValidationPlan


def get_split_generator(plan: "ValidationPlan") -> SplitGenerator:
    """Return the appropriate split generator for the given plan mode.

    Args:
        plan: A loaded :class:`~qs_trader.validation.plan.ValidationPlan`.

    Returns:
        :class:`StaticSplitGenerator` for ``mode='static_is_oos'``,
        :class:`WalkForwardSplitGenerator` for ``mode='walk_forward'``.

    Raises:
        ValueError: If ``plan.mode`` is not a known mode.
    """
    if plan.mode == "static_is_oos":
        return StaticSplitGenerator()
    if plan.mode == "walk_forward":
        return WalkForwardSplitGenerator()
    raise ValueError(f"Unknown plan mode: {plan.mode!r}")


__all__ = [
    "SplitGenerator",
    "ValidationSplit",
    "StaticSplitGenerator",
    "WalkForwardSplitGenerator",
    "get_split_generator",
]
