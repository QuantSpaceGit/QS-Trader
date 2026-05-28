"""Declarative decision engine for the QS-Trader OOS validation framework.

Evaluates a :class:`~qs_trader.validation.plan.DecisionRulesSpec` against
aggregated metric comparisons to produce a :class:`ValidationDecision` with
one of ``Pass``, ``Fail``, ``ReviewRequired``, or ``Invalid`` outcomes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, Protocol, runtime_checkable

import structlog

from qs_trader.validation.aggregation import MetricComparison
from qs_trader.validation.plan import DecisionRulesSpec, MetricsCatalog

if TYPE_CHECKING:
    from qs_trader.validation.runner import ChildRunRef

logger = structlog.get_logger(__name__)

__all__ = [
    "DecisionEngine",
    "DecisionRule",
    "RuleResult",
    "ValidationDecision",
]


# ---------------------------------------------------------------------------
# Public data models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RuleResult:
    """Outcome of evaluating a single decision rule.

    Attributes:
        rule: Rule key string (e.g. ``'oos_sharpe_min'``).
        threshold: The configured threshold value (float for numeric rules,
                   ``True`` for the boolean ``require_positive_oos_total_return``
                   rule).
        actual: The observed value used for comparison. ``float`` for numeric
                rules, ``bool`` for the boolean rule, ``None`` when the metric
                was unavailable.
        passed: ``True`` when the rule criterion is satisfied.
    """

    rule: str
    threshold: float | bool
    actual: float | bool | None
    passed: bool


@dataclass(frozen=True)
class ValidationDecision:
    """Outcome of the full declarative rule evaluation pass.

    Attributes:
        outcome: One of ``Pass``, ``Fail``, ``ReviewRequired``, or ``Invalid``.
        reason_codes: Machine-readable snake_case reason strings explaining a
                      non-Pass outcome. Empty for ``Pass``.
        rule_results: Per-rule evaluation results. Empty for ``Invalid``.
    """

    outcome: Literal["Pass", "Fail", "ReviewRequired", "Invalid"]
    reason_codes: list[str]
    rule_results: list[RuleResult]


# ---------------------------------------------------------------------------
# DecisionRule Protocol (extensibility seam — Phase 4+)
# ---------------------------------------------------------------------------


@runtime_checkable
class DecisionRule(Protocol):
    """Protocol for custom decision rules (Phase 4+ extensibility seam).

    Built-in rules are handled declaratively by :class:`DecisionEngine`.
    Third-party plugin rules implement this Protocol and are registered in
    Phase 4.
    """

    rule_key: str

    def evaluate(self, actual: float | None, threshold: float | bool) -> bool:
        """Return ``True`` when the rule criterion is satisfied."""
        ...


# ---------------------------------------------------------------------------
# Internal rule infrastructure
# ---------------------------------------------------------------------------

_NULL_COMPARISON: MetricComparison = MetricComparison(is_val=None, oos=None, full=None, decay=None)

# Maps rule_key → (metric_catalog_key, MetricComparison field name)
_RULE_METRIC_MAP: dict[str, tuple[str, str]] = {
    "oos_sharpe_min": ("sharpe_ratio", "oos"),
    "oos_max_drawdown_max": ("max_drawdown", "oos"),
    "is_to_oos_sharpe_decay_max": ("sharpe_ratio", "decay"),
    "min_oos_trades": ("num_trades", "oos"),
    "require_positive_oos_total_return": ("total_return", "oos"),
    "is_to_oos_sharpe_decay_warn": ("sharpe_ratio", "decay"),
}

# Human-readable metric label for Invalid reason codes
_RULE_METRIC_LABEL: dict[str, str] = {
    "oos_sharpe_min": "sharpe_ratio",
    "oos_max_drawdown_max": "max_drawdown",
    "is_to_oos_sharpe_decay_max": "sharpe_ratio",
    "min_oos_trades": "num_trades",
    "require_positive_oos_total_return": "total_return",
    "is_to_oos_sharpe_decay_warn": "sharpe_ratio",
}


def _get_raw(rule_key: str, comparison: dict[str, MetricComparison]) -> float | None:
    """Extract the raw float metric value for a rule from the comparison dict."""
    metric_key, field = _RULE_METRIC_MAP[rule_key]
    mc = comparison.get(metric_key, _NULL_COMPARISON)
    return getattr(mc, field)  # type: ignore[no-any-return]


def _compute_actual(rule_key: str, raw: float | None) -> float | bool | None:
    """Compute the ``actual`` value for a :class:`RuleResult`.

    For ``require_positive_oos_total_return`` the actual is a bool (``raw > 0``).
    For all other rules the actual is the raw float value.
    """
    if raw is None:
        return None
    if rule_key == "require_positive_oos_total_return":
        return raw > 0
    return raw


def _evaluate_pass(rule_key: str, raw: float | None, threshold: float | bool) -> bool:
    """Return ``True`` when the rule criterion is met.

    Returns ``False`` when ``raw`` is ``None`` (should only occur when called
    explicitly; normal flow guards against this via the Invalid check).
    """
    if raw is None:
        return False
    if rule_key == "require_positive_oos_total_return":
        return raw > 0
    numeric: float = float(threshold)
    match rule_key:
        case "oos_sharpe_min":
            return raw >= numeric
        case "oos_max_drawdown_max":
            return raw <= numeric
        case "is_to_oos_sharpe_decay_max":
            return raw <= numeric
        case "min_oos_trades":
            return raw >= numeric
        case "is_to_oos_sharpe_decay_warn":
            return raw <= numeric
        case _:
            raise ValueError(f"Unknown rule key: {rule_key!r}")


def _build_rule_result(
    rule_key: str,
    threshold: float | bool,
    comparison: dict[str, MetricComparison],
) -> RuleResult:
    """Build a :class:`RuleResult` for a single rule evaluation."""
    raw = _get_raw(rule_key, comparison)
    actual = _compute_actual(rule_key, raw)
    passed = _evaluate_pass(rule_key, raw, threshold)
    return RuleResult(rule=rule_key, threshold=threshold, actual=actual, passed=passed)


def _get_enabled_fail_rules(
    rules_spec: DecisionRulesSpec,
) -> list[tuple[str, float | bool]]:
    """Return ``(rule_key, threshold)`` pairs for all enabled (non-None) fail rules."""
    pairs: list[tuple[str, float | bool]] = []
    if rules_spec.oos_sharpe_min is not None:
        pairs.append(("oos_sharpe_min", rules_spec.oos_sharpe_min))
    if rules_spec.oos_max_drawdown_max is not None:
        pairs.append(("oos_max_drawdown_max", rules_spec.oos_max_drawdown_max))
    if rules_spec.is_to_oos_sharpe_decay_max is not None:
        pairs.append(("is_to_oos_sharpe_decay_max", rules_spec.is_to_oos_sharpe_decay_max))
    if rules_spec.min_oos_trades is not None:
        pairs.append(("min_oos_trades", float(rules_spec.min_oos_trades)))
    if rules_spec.require_positive_oos_total_return is not None:
        pairs.append(("require_positive_oos_total_return", rules_spec.require_positive_oos_total_return))
    return pairs


# ---------------------------------------------------------------------------
# DecisionEngine
# ---------------------------------------------------------------------------


class DecisionEngine:
    """Evaluates declarative decision rules against aggregated metric comparisons.

    The engine must be constructed with a :class:`~qs_trader.validation.plan.MetricsCatalog`
    so that catalog-level required-metric coverage is always enforced.
    Pass ``MetricsCatalog(required=(), recommended=())`` to opt out of coverage
    checks (useful in focused unit tests that exercise individual rules only).

    Outcome priority (highest to lowest):

    1. ``Invalid`` — any fold failed OR a required catalog metric is missing/``None``
       OR a required metric for an enabled fail rule is ``None``.
    2. ``Fail`` — any enabled fail rule evaluates to ``passed=False``.
    3. ``ReviewRequired`` — all fail rules pass but an ``on_review_required``
       rule evaluates to ``passed=False``.
    4. ``Pass`` — all rules satisfied.
    """

    def __init__(self, metric_catalog: MetricsCatalog) -> None:
        """Construct the engine with a catalog that enforces required-metric coverage.

        Args:
            metric_catalog: Catalog specifying required and recommended metrics.
                All *required* metrics are checked for ``None`` IS or OOS values
                before any fail rule runs.  Pass
                ``MetricsCatalog(required=(), recommended=())`` to skip coverage
                enforcement (e.g., in rule-focused unit tests).
        """
        self._metric_catalog = metric_catalog

    def evaluate(
        self,
        comparison: dict[str, MetricComparison],
        rules_spec: DecisionRulesSpec,
        child_refs: list[ChildRunRef],
    ) -> ValidationDecision:
        """Evaluate all rules and return a :class:`ValidationDecision`.

        Args:
            comparison: Dict from :class:`~.aggregation.MetricsAggregator`.
            rules_spec: Declarative rule thresholds from
                        :class:`~qs_trader.validation.plan.DecisionRulesSpec`.
            child_refs: Fold execution records from
                        :class:`~qs_trader.validation.runner.ChildRunRef`.

        Returns:
            :class:`ValidationDecision` with outcome and reason codes.
        """
        reason_codes: list[str] = []

        # ------------------------------------------------------------------
        # Step 1: child fold failures → Invalid
        # ------------------------------------------------------------------
        if any(ref.status == "failed" for ref in child_refs):
            reason_codes.append("child_fold_failed")

        # ------------------------------------------------------------------
        # Step 2a: missing required catalog metrics → Invalid
        # ------------------------------------------------------------------
        seen_missing: set[str] = set()
        for metric_name in self._metric_catalog.required:
            mc = comparison.get(metric_name)
            if mc is None or mc.is_val is None or mc.oos is None:
                code = f"missing_metric:{metric_name}"
                if code not in seen_missing:
                    seen_missing.add(code)
                    reason_codes.append(code)

        # ------------------------------------------------------------------
        # Step 2b: missing metrics for enabled fail rules → Invalid
        # ------------------------------------------------------------------
        enabled_fail_rules = _get_enabled_fail_rules(rules_spec)
        for rule_key, _ in enabled_fail_rules:
            raw = _get_raw(rule_key, comparison)
            if raw is None:
                metric_label = _RULE_METRIC_LABEL[rule_key]
                missing_code = f"missing_metric:{metric_label}"
                if missing_code not in seen_missing:
                    seen_missing.add(missing_code)
                    reason_codes.append(missing_code)

        if reason_codes:
            logger.debug("validation_invalid", reason_codes=reason_codes)
            return ValidationDecision(
                outcome="Invalid",
                reason_codes=reason_codes,
                rule_results=[],
            )

        # ------------------------------------------------------------------
        # Step 3: evaluate fail rules
        # ------------------------------------------------------------------
        rule_results: list[RuleResult] = []
        fail_reason_codes: list[str] = []
        for rule_key, threshold in enabled_fail_rules:
            rr = _build_rule_result(rule_key, threshold, comparison)
            rule_results.append(rr)
            if not rr.passed:
                fail_reason_codes.append(f"{rule_key}_fail")

        if fail_reason_codes:
            logger.debug("validation_fail", reason_codes=fail_reason_codes)
            return ValidationDecision(
                outcome="Fail",
                reason_codes=fail_reason_codes,
                rule_results=rule_results,
            )

        # ------------------------------------------------------------------
        # Step 4: evaluate review rules (on_review_required)
        # ------------------------------------------------------------------
        review_reason_codes: list[str] = []
        for review_rule in rules_spec.on_review_required:
            raw = _get_raw(review_rule.rule, comparison)
            if raw is None:
                # Missing metric for review rule: skip (don't trigger ReviewRequired)
                rr = RuleResult(
                    rule=review_rule.rule,
                    threshold=review_rule.threshold,
                    actual=None,
                    passed=True,
                )
            else:
                rr = _build_rule_result(review_rule.rule, review_rule.threshold, comparison)
            rule_results.append(rr)
            if not rr.passed:
                review_reason_codes.append(f"review_required:{review_rule.rule}")

        if review_reason_codes:
            logger.debug("validation_review_required", reason_codes=review_reason_codes)
            return ValidationDecision(
                outcome="ReviewRequired",
                reason_codes=review_reason_codes,
                rule_results=rule_results,
            )

        # ------------------------------------------------------------------
        # Step 5: all pass
        # ------------------------------------------------------------------
        logger.debug("validation_pass")
        return ValidationDecision(
            outcome="Pass",
            reason_codes=[],
            rule_results=rule_results,
        )
