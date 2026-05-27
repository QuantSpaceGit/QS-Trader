"""QS-Trader Out-of-Sample Validation Framework.

Public API for the ``qs_trader.validation`` package.
"""

from qs_trader.validation.child_config import derive_child_config
from qs_trader.validation.plan import (
    BenchmarkRef,
    DateRange,
    DecisionRulesSpec,
    ExecutionSpec,
    HoldoutSpec,
    KNOWN_FAIL_RULES,
    KNOWN_REVIEW_RULES,
    MetricsCatalog,
    ReportingSpec,
    StaticSplitSpec,
    ValidationPlan,
    compute_plan_sha256,
    load_validation_plan,
)
from qs_trader.validation.splits.base import SplitGenerator, ValidationSplit
from qs_trader.validation.splits.static import StaticSplitGenerator

__all__ = [
    "BenchmarkRef",
    "DateRange",
    "DecisionRulesSpec",
    "ExecutionSpec",
    "HoldoutSpec",
    "KNOWN_FAIL_RULES",
    "KNOWN_REVIEW_RULES",
    "MetricsCatalog",
    "ReportingSpec",
    "StaticSplitSpec",
    "ValidationPlan",
    "compute_plan_sha256",
    "load_validation_plan",
    "SplitGenerator",
    "ValidationSplit",
    "StaticSplitGenerator",
    "derive_child_config",
]
