"""Split generation protocol and implementations."""

from qs_trader.validation.splits.base import SplitGenerator, ValidationSplit
from qs_trader.validation.splits.static import StaticSplitGenerator

__all__ = [
    "SplitGenerator",
    "ValidationSplit",
    "StaticSplitGenerator",
]
