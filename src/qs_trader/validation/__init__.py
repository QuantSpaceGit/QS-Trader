"""QS-Trader Out-of-Sample Validation Framework.

Public API for the ``qs_trader.validation`` package.
"""

from qs_trader.validation.aggregation import MetricComparison, MetricsAggregator
from qs_trader.validation.child_config import derive_child_config
from qs_trader.validation.decision import DecisionEngine, DecisionRule, RuleResult, ValidationDecision
from qs_trader.validation.plan import (
    KNOWN_FAIL_RULES,
    KNOWN_REVIEW_RULES,
    BenchmarkRef,
    BenchmarkSpec,
    DateRange,
    DecisionRulesSpec,
    ExecutionSpec,
    HoldoutSpec,
    MetricsCatalog,
    ReportingSpec,
    StaticSplitSpec,
    ValidationPlan,
    compute_plan_sha256,
    load_validation_plan,
)
from qs_trader.validation.runner import ChildRunRef, SequentialValidationRunner
from qs_trader.validation.splits.base import SplitGenerator, ValidationSplit
from qs_trader.validation.splits.static import StaticSplitGenerator

__all__ = [
    "BenchmarkRef",
    "BenchmarkSpec",
    "ChildRunRef",
    "DateRange",
    "DecisionEngine",
    "DecisionRule",
    "DecisionRulesSpec",
    "ExecutionSpec",
    "HoldoutSpec",
    "KNOWN_FAIL_RULES",
    "KNOWN_REVIEW_RULES",
    "MetricComparison",
    "MetricsCatalog",
    "MetricsAggregator",
    "ReportingSpec",
    "RuleResult",
    "SequentialValidationRunner",
    "StaticSplitSpec",
    "ValidationDecision",
    "ValidationPlan",
    "compute_plan_sha256",
    "load_validation_plan",
    "SplitGenerator",
    "ValidationSplit",
    "StaticSplitGenerator",
    "derive_child_config",
]
