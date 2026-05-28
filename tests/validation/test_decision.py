"""Tests for qs_trader.validation.decision (T6.1–T6.4)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from qs_trader.validation.aggregation import MetricComparison
from qs_trader.validation.decision import DecisionEngine, DecisionRule, RuleResult, ValidationDecision
from qs_trader.validation.plan import DecisionRulesSpec, MetricsCatalog, OnReviewRequiredRule
from qs_trader.validation.runner import ChildRunRef

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _child_ref(*, status: str = "success") -> ChildRunRef:
    return ChildRunRef(
        fold_id="f0__is",
        run_id="val_test__f0__is",
        experiment_id="test_exp",
        role="is",
        run_dir=Path("/tmp/test_fold"),
        status=status,
        error=None if status == "success" else "boom",
    )


def _success_refs() -> list[ChildRunRef]:
    return [_child_ref(status="success"), _child_ref(status="success")]


def _all_pass_comparison() -> dict[str, MetricComparison]:
    """Comparison where all standard rules pass at reasonable thresholds."""
    return {
        "sharpe_ratio": MetricComparison(is_val=1.5, oos=1.2, full=None, decay=0.2),
        "total_return": MetricComparison(is_val=0.6, oos=0.3, full=None, decay=0.5),
        "max_drawdown": MetricComparison(is_val=0.15, oos=0.20, full=None, decay=None),
        "num_trades": MetricComparison(is_val=150.0, oos=100.0, full=None, decay=0.33),
        "cagr": MetricComparison(is_val=0.21, oos=0.10, full=None, decay=0.52),
        "volatility": MetricComparison(is_val=0.14, oos=0.16, full=None, decay=None),
    }


def _all_fail_rules_spec() -> DecisionRulesSpec:
    return DecisionRulesSpec(
        oos_sharpe_min=0.8,
        oos_max_drawdown_max=0.25,
        is_to_oos_sharpe_decay_max=0.5,
        min_oos_trades=30,
        require_positive_oos_total_return=True,
    )


def _engine() -> DecisionEngine:
    """Engine with empty catalog — no coverage enforcement; for per-rule unit tests."""
    return DecisionEngine(MetricsCatalog(required=(), recommended=()))


# ---------------------------------------------------------------------------
# T6.1 — Per-rule: pass case, fail case, boundary
# ---------------------------------------------------------------------------


class TestOosSharpeMin:
    """Rule: comparison["sharpe_ratio"].oos >= threshold → Pass."""

    RULE_SPEC = DecisionRulesSpec(oos_sharpe_min=0.8)

    def _comparison(self, sharpe_oos: float) -> dict[str, MetricComparison]:
        return {"sharpe_ratio": MetricComparison(is_val=1.2, oos=sharpe_oos, full=None, decay=0.3)}

    def test_pass(self) -> None:
        decision = _engine().evaluate(self._comparison(0.9), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"
        assert len(decision.reason_codes) == 0

    def test_fail(self) -> None:
        decision = _engine().evaluate(self._comparison(0.7), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"
        assert "oos_sharpe_min_fail" in decision.reason_codes

    def test_boundary_exactly_at_threshold_passes(self) -> None:
        """oos == threshold → PASS (≥ semantics)."""
        decision = _engine().evaluate(self._comparison(0.8), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"

    def test_just_below_threshold_fails(self) -> None:
        decision = _engine().evaluate(self._comparison(0.799), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"

    def test_rule_result_contains_correct_values(self) -> None:
        decision = _engine().evaluate(self._comparison(0.9), self.RULE_SPEC, _success_refs())
        rr = decision.rule_results[0]
        assert rr.rule == "oos_sharpe_min"
        assert rr.threshold == pytest.approx(0.8)
        assert rr.actual == pytest.approx(0.9)
        assert rr.passed is True


class TestOosMaxDrawdownMax:
    """Rule: comparison["max_drawdown"].oos <= threshold → Pass."""

    RULE_SPEC = DecisionRulesSpec(oos_max_drawdown_max=0.25)

    def _comparison(self, dd_oos: float) -> dict[str, MetricComparison]:
        return {"max_drawdown": MetricComparison(is_val=0.18, oos=dd_oos, full=None, decay=None)}

    def test_pass(self) -> None:
        decision = _engine().evaluate(self._comparison(0.20), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"

    def test_fail(self) -> None:
        decision = _engine().evaluate(self._comparison(0.30), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"
        assert "oos_max_drawdown_max_fail" in decision.reason_codes

    def test_boundary_exactly_at_threshold_passes(self) -> None:
        """oos == threshold → PASS (≤ semantics)."""
        decision = _engine().evaluate(self._comparison(0.25), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"

    def test_just_above_threshold_fails(self) -> None:
        decision = _engine().evaluate(self._comparison(0.251), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"


class TestIsToOosSharpeDecayMax:
    """Rule: comparison["sharpe_ratio"].decay <= threshold → Pass."""

    RULE_SPEC = DecisionRulesSpec(is_to_oos_sharpe_decay_max=0.5)

    def _comparison(self, decay: float) -> dict[str, MetricComparison]:
        return {"sharpe_ratio": MetricComparison(is_val=1.2, oos=0.9, full=None, decay=decay)}

    def test_pass(self) -> None:
        decision = _engine().evaluate(self._comparison(0.3), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"

    def test_fail(self) -> None:
        decision = _engine().evaluate(self._comparison(0.7), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"
        assert "is_to_oos_sharpe_decay_max_fail" in decision.reason_codes

    def test_boundary_exactly_at_threshold_passes(self) -> None:
        """decay == threshold → PASS (≤ semantics)."""
        decision = _engine().evaluate(self._comparison(0.5), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"

    def test_just_above_threshold_fails(self) -> None:
        decision = _engine().evaluate(self._comparison(0.501), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"


class TestMinOosTrades:
    """Rule: comparison["num_trades"].oos >= threshold → Pass."""

    RULE_SPEC = DecisionRulesSpec(min_oos_trades=30)

    def _comparison(self, num_trades: float) -> dict[str, MetricComparison]:
        return {"num_trades": MetricComparison(is_val=150.0, oos=num_trades, full=None, decay=None)}

    def test_pass(self) -> None:
        decision = _engine().evaluate(self._comparison(50.0), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"

    def test_fail(self) -> None:
        decision = _engine().evaluate(self._comparison(20.0), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"
        assert "min_oos_trades_fail" in decision.reason_codes

    def test_boundary_exactly_at_threshold_passes(self) -> None:
        """num_trades == threshold → PASS (≥ semantics)."""
        decision = _engine().evaluate(self._comparison(30.0), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"

    def test_just_below_threshold_fails(self) -> None:
        decision = _engine().evaluate(self._comparison(29.0), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"

    def test_threshold_stored_as_float_in_rule_result(self) -> None:
        """min_oos_trades spec is int but threshold in RuleResult is float."""
        decision = _engine().evaluate(self._comparison(50.0), self.RULE_SPEC, _success_refs())
        rr = decision.rule_results[0]
        assert rr.threshold == pytest.approx(30.0)


class TestRequirePositiveOosTotalReturn:
    """Rule: comparison["total_return"].oos > 0 → Pass (bool rule, threshold=True)."""

    RULE_SPEC = DecisionRulesSpec(require_positive_oos_total_return=True)

    def _comparison(self, total_return_oos: float) -> dict[str, MetricComparison]:
        return {"total_return": MetricComparison(is_val=0.6, oos=total_return_oos, full=None, decay=None)}

    def test_pass_when_positive(self) -> None:
        decision = _engine().evaluate(self._comparison(0.3), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"

    def test_fail_when_zero(self) -> None:
        """oos == 0 → FAIL (strict > 0 semantics)."""
        decision = _engine().evaluate(self._comparison(0.0), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"
        assert "require_positive_oos_total_return_fail" in decision.reason_codes

    def test_fail_when_negative(self) -> None:
        decision = _engine().evaluate(self._comparison(-0.05), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Fail"
        assert "require_positive_oos_total_return_fail" in decision.reason_codes

    def test_rule_result_actual_is_bool_true(self) -> None:
        """actual in RuleResult is a bool for this rule, not the raw float."""
        decision = _engine().evaluate(self._comparison(0.3), self.RULE_SPEC, _success_refs())
        rr = decision.rule_results[0]
        assert rr.actual is True
        assert isinstance(rr.actual, bool)

    def test_rule_result_actual_is_bool_false(self) -> None:
        decision = _engine().evaluate(self._comparison(-0.1), self.RULE_SPEC, _success_refs())
        rr = decision.rule_results[0]
        assert rr.actual is False
        assert isinstance(rr.actual, bool)

    def test_rule_result_threshold_is_true(self) -> None:
        decision = _engine().evaluate(self._comparison(0.3), self.RULE_SPEC, _success_refs())
        rr = decision.rule_results[0]
        assert rr.threshold is True

    def test_small_positive_passes(self) -> None:
        """Any positive value > 0 passes."""
        decision = _engine().evaluate(self._comparison(1e-9), self.RULE_SPEC, _success_refs())
        assert decision.outcome == "Pass"


# ---------------------------------------------------------------------------
# T6.2 — on_review_required downgrade semantics
# ---------------------------------------------------------------------------


class TestReviewRequiredDowngrade:
    """All fail-rules pass but review rule triggers → ReviewRequired."""

    def _review_spec(self, decay_warn_threshold: float) -> DecisionRulesSpec:
        return DecisionRulesSpec(
            oos_sharpe_min=0.5,  # will pass (oos=0.9 > 0.5)
            on_review_required=(
                OnReviewRequiredRule(
                    rule="is_to_oos_sharpe_decay_warn",
                    threshold=decay_warn_threshold,
                ),
            ),
        )

    def _comparison(self, sharpe_oos: float, decay: float) -> dict[str, MetricComparison]:
        return {"sharpe_ratio": MetricComparison(is_val=1.5, oos=sharpe_oos, full=None, decay=decay)}

    def test_review_required_when_decay_exceeds_warn_threshold(self) -> None:
        spec = self._review_spec(0.3)
        # decay=0.4 > warn_threshold=0.3 → review triggers
        comparison = self._comparison(sharpe_oos=0.9, decay=0.4)
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "ReviewRequired"
        assert "review_required:is_to_oos_sharpe_decay_warn" in decision.reason_codes

    def test_pass_when_decay_below_warn_threshold(self) -> None:
        spec = self._review_spec(0.3)
        # decay=0.2 < warn_threshold=0.3 → no review trigger
        comparison = self._comparison(sharpe_oos=0.9, decay=0.2)
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "Pass"
        assert len(decision.reason_codes) == 0

    def test_review_rule_at_boundary_passes(self) -> None:
        """decay == threshold → PASS (≤ semantics)."""
        spec = self._review_spec(0.4)
        comparison = self._comparison(sharpe_oos=0.9, decay=0.4)
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "Pass"

    def test_fail_rules_fail_takes_priority_over_review(self) -> None:
        """If a fail rule fails AND a review rule fires → Fail (not ReviewRequired)."""
        spec = DecisionRulesSpec(
            oos_sharpe_min=1.0,  # will fail (oos=0.9 < 1.0)
            on_review_required=(OnReviewRequiredRule(rule="is_to_oos_sharpe_decay_warn", threshold=0.1),),
        )
        comparison = self._comparison(sharpe_oos=0.9, decay=0.4)
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "Fail"
        assert "oos_sharpe_min_fail" in decision.reason_codes

    def test_review_result_included_in_rule_results(self) -> None:
        spec = self._review_spec(0.3)
        comparison = self._comparison(sharpe_oos=0.9, decay=0.4)
        decision = _engine().evaluate(comparison, spec, _success_refs())
        review_rules = [rr for rr in decision.rule_results if rr.rule == "is_to_oos_sharpe_decay_warn"]
        assert len(review_rules) == 1
        assert review_rules[0].passed is False

    def test_review_result_included_as_pass_when_missing_metric(self) -> None:
        """Review rule with None metric → passed=True, included but not triggering."""
        spec = DecisionRulesSpec(
            on_review_required=(OnReviewRequiredRule(rule="is_to_oos_sharpe_decay_warn", threshold=0.3),)
        )
        # sharpe_ratio not in comparison → decay=None for review rule
        decision = _engine().evaluate({}, spec, _success_refs())
        assert decision.outcome == "Pass"
        review_rules = [rr for rr in decision.rule_results if rr.rule == "is_to_oos_sharpe_decay_warn"]
        assert len(review_rules) == 1
        assert review_rules[0].passed is True
        assert review_rules[0].actual is None


# ---------------------------------------------------------------------------
# T6.3 — Invalid outcomes
# ---------------------------------------------------------------------------


class TestInvalidOutcome:
    def test_invalid_when_child_ref_has_failed_status(self) -> None:
        spec = _all_fail_rules_spec()
        refs = [_child_ref(status="failed"), _child_ref(status="success")]
        decision = _engine().evaluate(_all_pass_comparison(), spec, refs)
        assert decision.outcome == "Invalid"
        assert "child_fold_failed" in decision.reason_codes

    def test_invalid_when_all_refs_failed(self) -> None:
        spec = _all_fail_rules_spec()
        refs = [_child_ref(status="failed"), _child_ref(status="failed")]
        decision = _engine().evaluate(_all_pass_comparison(), spec, refs)
        assert decision.outcome == "Invalid"
        assert "child_fold_failed" in decision.reason_codes

    def test_invalid_when_sharpe_oos_is_none(self) -> None:
        spec = DecisionRulesSpec(oos_sharpe_min=0.8)
        comparison = {"sharpe_ratio": MetricComparison(is_val=1.2, oos=None, full=None, decay=None)}
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:sharpe_ratio" in decision.reason_codes

    def test_invalid_when_metric_key_absent_from_comparison(self) -> None:
        """sharpe_ratio key missing entirely from comparison dict."""
        spec = DecisionRulesSpec(oos_sharpe_min=0.8)
        decision = _engine().evaluate({}, spec, _success_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:sharpe_ratio" in decision.reason_codes

    def test_invalid_when_max_drawdown_missing(self) -> None:
        spec = DecisionRulesSpec(oos_max_drawdown_max=0.25)
        decision = _engine().evaluate({}, spec, _success_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:max_drawdown" in decision.reason_codes

    def test_invalid_when_num_trades_missing(self) -> None:
        spec = DecisionRulesSpec(min_oos_trades=30)
        decision = _engine().evaluate({}, spec, _success_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:num_trades" in decision.reason_codes

    def test_invalid_when_total_return_missing(self) -> None:
        spec = DecisionRulesSpec(require_positive_oos_total_return=True)
        decision = _engine().evaluate({}, spec, _success_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:total_return" in decision.reason_codes

    def test_invalid_when_sharpe_decay_is_none(self) -> None:
        """is_to_oos_sharpe_decay_max enabled but decay=None → Invalid."""
        spec = DecisionRulesSpec(is_to_oos_sharpe_decay_max=0.5)
        comparison = {"sharpe_ratio": MetricComparison(is_val=None, oos=1.2, full=None, decay=None)}
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:sharpe_ratio" in decision.reason_codes

    def test_invalid_child_failure_and_missing_metric(self) -> None:
        """Both child failure and missing metric → Invalid with both reason codes."""
        spec = DecisionRulesSpec(oos_sharpe_min=0.8)
        refs = [_child_ref(status="failed")]
        decision = _engine().evaluate({}, spec, refs)
        assert decision.outcome == "Invalid"
        assert "child_fold_failed" in decision.reason_codes
        assert "missing_metric:sharpe_ratio" in decision.reason_codes

    def test_missing_metric_deduplicated_for_rules_sharing_same_metric(self) -> None:
        """oos_sharpe_min and is_to_oos_sharpe_decay_max both use sharpe_ratio.

        The reason code 'missing_metric:sharpe_ratio' should appear exactly once.
        """
        spec = DecisionRulesSpec(
            oos_sharpe_min=0.8,
            is_to_oos_sharpe_decay_max=0.5,
        )
        decision = _engine().evaluate({}, spec, _success_refs())
        assert decision.outcome == "Invalid"
        assert decision.reason_codes.count("missing_metric:sharpe_ratio") == 1

    def test_invalid_rule_results_are_empty(self) -> None:
        spec = DecisionRulesSpec(oos_sharpe_min=0.8)
        refs = [_child_ref(status="failed")]
        decision = _engine().evaluate({}, spec, refs)
        assert decision.rule_results == []

    def test_invalid_when_catalog_required_metric_cagr_missing(self) -> None:
        """catalog.required includes cagr; absent from comparison → Invalid."""
        catalog = MetricsCatalog(
            required=("sharpe_ratio", "total_return", "max_drawdown", "num_trades", "cagr", "volatility"),
            recommended=(),
        )
        # all rule-input metrics present, but cagr absent
        comparison = _all_pass_comparison().copy()
        del comparison["cagr"]
        decision = DecisionEngine(catalog).evaluate(comparison, _all_fail_rules_spec(), _success_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:cagr" in decision.reason_codes

    def test_invalid_when_catalog_required_metric_volatility_missing(self) -> None:
        """catalog.required includes volatility; absent from comparison → Invalid."""
        catalog = MetricsCatalog(
            required=("sharpe_ratio", "total_return", "max_drawdown", "num_trades", "cagr", "volatility"),
            recommended=(),
        )
        comparison = _all_pass_comparison().copy()
        del comparison["volatility"]
        decision = DecisionEngine(catalog).evaluate(comparison, _all_fail_rules_spec(), _success_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:volatility" in decision.reason_codes

    def test_no_invalid_when_all_catalog_metrics_present(self) -> None:
        """All catalog required metrics present → Invalid not triggered by catalog check."""
        catalog = MetricsCatalog(
            required=("sharpe_ratio", "total_return", "max_drawdown", "num_trades", "cagr", "volatility"),
            recommended=(),
        )
        decision = DecisionEngine(catalog).evaluate(_all_pass_comparison(), _all_fail_rules_spec(), _success_refs())
        assert decision.outcome == "Pass"

    def test_catalog_missing_metric_deduped_with_rule_missing_metric(self) -> None:
        """sharpe_ratio missing from both catalog required and a fail rule → code appears once."""
        catalog = MetricsCatalog(required=("sharpe_ratio",), recommended=())
        spec = DecisionRulesSpec(oos_sharpe_min=0.8)
        decision = DecisionEngine(catalog).evaluate({}, spec, _success_refs())
        assert decision.outcome == "Invalid"
        assert decision.reason_codes.count("missing_metric:sharpe_ratio") == 1

    def test_invalid_does_not_evaluate_fail_rules(self) -> None:
        """On Invalid, rule_results is empty regardless of comparison state."""
        spec = _all_fail_rules_spec()
        refs = [_child_ref(status="failed")]
        decision = _engine().evaluate(_all_pass_comparison(), spec, refs)
        assert decision.rule_results == []


# ---------------------------------------------------------------------------
# T6.4 — Reason codes are machine-readable + stable
# ---------------------------------------------------------------------------


class TestReasonCodeStability:
    """Snapshot test: reason codes match the exact expected strings."""

    def test_oos_sharpe_min_fail_code(self) -> None:
        spec = DecisionRulesSpec(oos_sharpe_min=1.0)
        c = {"sharpe_ratio": MetricComparison(is_val=1.5, oos=0.7, full=None, decay=0.5)}
        d = _engine().evaluate(c, spec, _success_refs())
        assert d.reason_codes == ["oos_sharpe_min_fail"]

    def test_oos_max_drawdown_max_fail_code(self) -> None:
        spec = DecisionRulesSpec(oos_max_drawdown_max=0.20)
        c = {"max_drawdown": MetricComparison(is_val=0.15, oos=0.30, full=None, decay=None)}
        d = _engine().evaluate(c, spec, _success_refs())
        assert d.reason_codes == ["oos_max_drawdown_max_fail"]

    def test_is_to_oos_sharpe_decay_max_fail_code(self) -> None:
        spec = DecisionRulesSpec(is_to_oos_sharpe_decay_max=0.5)
        c = {"sharpe_ratio": MetricComparison(is_val=1.5, oos=0.5, full=None, decay=0.67)}
        d = _engine().evaluate(c, spec, _success_refs())
        assert d.reason_codes == ["is_to_oos_sharpe_decay_max_fail"]

    def test_min_oos_trades_fail_code(self) -> None:
        spec = DecisionRulesSpec(min_oos_trades=30)
        c = {"num_trades": MetricComparison(is_val=100.0, oos=10.0, full=None, decay=None)}
        d = _engine().evaluate(c, spec, _success_refs())
        assert d.reason_codes == ["min_oos_trades_fail"]

    def test_require_positive_oos_total_return_fail_code(self) -> None:
        spec = DecisionRulesSpec(require_positive_oos_total_return=True)
        c = {"total_return": MetricComparison(is_val=0.5, oos=-0.1, full=None, decay=None)}
        d = _engine().evaluate(c, spec, _success_refs())
        assert d.reason_codes == ["require_positive_oos_total_return_fail"]

    def test_child_fold_failed_code(self) -> None:
        refs = [_child_ref(status="failed")]
        d = _engine().evaluate({}, DecisionRulesSpec(), refs)
        assert "child_fold_failed" in d.reason_codes

    def test_missing_metric_code_format(self) -> None:
        spec = DecisionRulesSpec(oos_sharpe_min=0.8)
        d = _engine().evaluate({}, spec, _success_refs())
        assert "missing_metric:sharpe_ratio" in d.reason_codes

    def test_review_required_code_format(self) -> None:
        spec = DecisionRulesSpec(
            on_review_required=(OnReviewRequiredRule(rule="is_to_oos_sharpe_decay_warn", threshold=0.1),)
        )
        c = {"sharpe_ratio": MetricComparison(is_val=1.5, oos=1.2, full=None, decay=0.2)}
        d = _engine().evaluate(c, spec, _success_refs())
        assert "review_required:is_to_oos_sharpe_decay_warn" in d.reason_codes

    def test_all_fail_codes_are_snake_case(self) -> None:
        """Verify all reason codes match snake_case pattern."""
        import re

        pattern = re.compile(r"^[a-z][a-z0-9_:]*$")
        spec = _all_fail_rules_spec()
        comparison: dict[str, MetricComparison] = {}  # all missing
        d = _engine().evaluate(comparison, spec, _success_refs())
        for code in d.reason_codes:
            assert pattern.match(code), f"Reason code {code!r} is not snake_case"


# ---------------------------------------------------------------------------
# Decision matrix — one scenario per outcome
# ---------------------------------------------------------------------------


class TestDecisionMatrix:
    """Full matrix: one scenario per outcome (Pass, Fail, ReviewRequired, Invalid)."""

    SPEC = _all_fail_rules_spec()

    def test_pass_scenario(self) -> None:
        comparison = _all_pass_comparison()
        decision = _engine().evaluate(comparison, self.SPEC, _success_refs())
        assert decision.outcome == "Pass"
        assert decision.reason_codes == []

    def test_fail_scenario_oos_sharpe_below_min(self) -> None:
        """OOS Sharpe below minimum → Fail."""
        comparison = {
            "sharpe_ratio": MetricComparison(is_val=1.5, oos=0.5, full=None, decay=0.67),
            "total_return": MetricComparison(is_val=0.6, oos=0.3, full=None, decay=0.5),
            "max_drawdown": MetricComparison(is_val=0.15, oos=0.20, full=None, decay=None),
            "num_trades": MetricComparison(is_val=150.0, oos=100.0, full=None, decay=0.33),
        }
        decision = _engine().evaluate(comparison, self.SPEC, _success_refs())
        assert decision.outcome == "Fail"
        assert "oos_sharpe_min_fail" in decision.reason_codes

    def test_review_required_scenario(self) -> None:
        """All fail-rules pass but review warn fires → ReviewRequired."""
        spec = DecisionRulesSpec(
            oos_sharpe_min=0.5,  # passes (oos=0.9)
            on_review_required=(OnReviewRequiredRule(rule="is_to_oos_sharpe_decay_warn", threshold=0.1),),
        )
        # decay=0.4 > warn_threshold=0.1
        comparison = {"sharpe_ratio": MetricComparison(is_val=1.5, oos=0.9, full=None, decay=0.4)}
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "ReviewRequired"
        assert "review_required:is_to_oos_sharpe_decay_warn" in decision.reason_codes

    def test_invalid_scenario_child_failure(self) -> None:
        refs = [_child_ref(status="failed")]
        decision = _engine().evaluate(_all_pass_comparison(), self.SPEC, refs)
        assert decision.outcome == "Invalid"

    def test_invalid_scenario_missing_metric(self) -> None:
        comparison: dict[str, MetricComparison] = {}  # all metrics missing
        decision = _engine().evaluate(comparison, self.SPEC, _success_refs())
        assert decision.outcome == "Invalid"


# ---------------------------------------------------------------------------
# General engine behavior
# ---------------------------------------------------------------------------


class TestEngineGeneralBehavior:
    def test_pass_reason_codes_empty(self) -> None:
        decision = _engine().evaluate(_all_pass_comparison(), _all_fail_rules_spec(), _success_refs())
        assert decision.outcome == "Pass"
        assert decision.reason_codes == []

    def test_all_rules_disabled_passes_with_empty_results(self) -> None:
        """No rules enabled → Pass with empty rule_results."""
        decision = _engine().evaluate(_all_pass_comparison(), DecisionRulesSpec(), _success_refs())
        assert decision.outcome == "Pass"
        assert decision.rule_results == []
        assert decision.reason_codes == []

    def test_rule_results_contain_all_enabled_rules(self) -> None:
        spec = _all_fail_rules_spec()
        comparison = _all_pass_comparison()
        decision = _engine().evaluate(comparison, spec, _success_refs())
        rule_keys = [rr.rule for rr in decision.rule_results]
        assert "oos_sharpe_min" in rule_keys
        assert "oos_max_drawdown_max" in rule_keys
        assert "is_to_oos_sharpe_decay_max" in rule_keys
        assert "min_oos_trades" in rule_keys
        assert "require_positive_oos_total_return" in rule_keys

    def test_multiple_rules_fail_all_reason_codes_present(self) -> None:
        """Multiple fail rules → all reason codes included."""
        spec = DecisionRulesSpec(
            oos_sharpe_min=1.0,  # fail: oos=0.5
            min_oos_trades=100,  # fail: oos=10
        )
        comparison = {
            "sharpe_ratio": MetricComparison(is_val=1.5, oos=0.5, full=None, decay=0.67),
            "num_trades": MetricComparison(is_val=100.0, oos=10.0, full=None, decay=None),
        }
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "Fail"
        assert "oos_sharpe_min_fail" in decision.reason_codes
        assert "min_oos_trades_fail" in decision.reason_codes

    def test_empty_child_refs_no_invalid(self) -> None:
        """No child refs → no fold failure path, proceeds to rule evaluation."""
        spec = DecisionRulesSpec(oos_sharpe_min=0.5)
        comparison = {"sharpe_ratio": MetricComparison(is_val=1.5, oos=1.2, full=None, decay=0.2)}
        decision = _engine().evaluate(comparison, spec, [])
        assert decision.outcome == "Pass"

    def test_disabled_rule_not_in_rule_results(self) -> None:
        """Rules that are None in spec are not evaluated or included."""
        spec = DecisionRulesSpec(oos_sharpe_min=0.8)  # only one rule enabled
        comparison = _all_pass_comparison()
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert len(decision.rule_results) == 1
        assert decision.rule_results[0].rule == "oos_sharpe_min"

    def test_review_rules_not_evaluated_when_fail_rules_fail(self) -> None:
        """On Fail outcome, review rules are not in rule_results."""
        spec = DecisionRulesSpec(
            oos_sharpe_min=1.0,  # will fail
            on_review_required=(OnReviewRequiredRule(rule="is_to_oos_sharpe_decay_warn", threshold=0.1),),
        )
        comparison = {"sharpe_ratio": MetricComparison(is_val=1.5, oos=0.9, full=None, decay=0.4)}
        decision = _engine().evaluate(comparison, spec, _success_refs())
        assert decision.outcome == "Fail"
        review_rules = [rr for rr in decision.rule_results if rr.rule == "is_to_oos_sharpe_decay_warn"]
        assert len(review_rules) == 0


# ---------------------------------------------------------------------------
# ValidationDecision and RuleResult immutability
# ---------------------------------------------------------------------------


class TestModelImmutability:
    def test_rule_result_frozen(self) -> None:
        rr = RuleResult(rule="oos_sharpe_min", threshold=0.8, actual=1.2, passed=True)
        with pytest.raises(FrozenInstanceError):
            rr.rule = "other"  # type: ignore[misc]

    def test_validation_decision_frozen(self) -> None:
        d = ValidationDecision(outcome="Pass", reason_codes=[], rule_results=[])
        with pytest.raises(FrozenInstanceError):
            d.outcome = "Fail"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# DecisionRule Protocol compliance
# ---------------------------------------------------------------------------


class TestDecisionRuleProtocol:
    def test_protocol_is_importable(self) -> None:
        """DecisionRule is a Protocol — verify it can be imported and used as type hint."""
        assert DecisionRule is not None

    def test_protocol_is_runtime_checkable(self) -> None:
        """DecisionRule uses @runtime_checkable so Phase 4 plugin registration
        can use isinstance(obj, DecisionRule) without TypeErrors.
        """

        class _ConcreteRule:
            rule_key = "test_rule"

            def evaluate(self, actual: float | None, threshold: float | bool) -> bool:
                return True

        assert isinstance(_ConcreteRule(), DecisionRule)
