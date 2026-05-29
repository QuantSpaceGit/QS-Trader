"""Tests for WalkForwardDecisionInput and DecisionEngine.evaluate_walk_forward (Phase 2A.4)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest
from pydantic import ValidationError

from qs_trader.validation.decision import DecisionEngine, ValidationDecision, WalkForwardDecisionInput
from qs_trader.validation.plan import DecisionRulesSpec, MetricsCatalog, ValidationPlan
from qs_trader.validation.runner import ChildRunRef

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_catalog() -> MetricsCatalog:
    return MetricsCatalog(required=(), recommended=())


def _no_failed_refs() -> list[ChildRunRef]:
    return [
        ChildRunRef(
            fold_id="f0__is",
            run_id="val__f0__is",
            experiment_id="exp",
            role="is",
            run_dir=Path("/tmp"),
            status="success",
            error=None,
        ),
        ChildRunRef(
            fold_id="f0__oos",
            run_id="val__f0__oos",
            experiment_id="exp",
            role="oos",
            run_dir=Path("/tmp"),
            status="success",
            error=None,
        ),
    ]


def _with_failed_ref() -> list[ChildRunRef]:
    return [
        ChildRunRef(
            fold_id="f0__is",
            run_id="val__f0__is",
            experiment_id="exp",
            role="is",
            run_dir=Path("/tmp"),
            status="success",
            error=None,
        ),
        ChildRunRef(
            fold_id="f0__oos",
            run_id="val__f0__oos",
            experiment_id="exp",
            role="oos",
            run_dir=Path("/tmp"),
            status="failed",
            error="oops",
        ),
    ]


def _engine() -> DecisionEngine:
    return DecisionEngine(_empty_catalog())


def _wf(
    *,
    count_pass: int = 5,
    count_total: int = 7,
    median_sharpe: float | None = 1.5,
    worst_dd: float | None = 0.15,
) -> WalkForwardDecisionInput:
    return WalkForwardDecisionInput(
        count_pass_folds=count_pass,
        count_total_folds=count_total,
        median_oos_sharpe=median_sharpe,
        worst_oos_max_drawdown=worst_dd,
    )


def _rules(
    *,
    min_pass_folds_fraction: float | None = None,
    median_oos_sharpe_min: float | None = None,
    worst_oos_max_drawdown_max: float | None = None,
) -> DecisionRulesSpec:
    return DecisionRulesSpec(
        min_pass_folds_fraction=min_pass_folds_fraction,
        median_oos_sharpe_min=median_oos_sharpe_min,
        worst_oos_max_drawdown_max=worst_oos_max_drawdown_max,
    )


# ---------------------------------------------------------------------------
# child_fold_failed → Invalid
# ---------------------------------------------------------------------------


class TestChildFoldFailed:
    def test_child_fold_failed_produces_invalid(self) -> None:
        rules = _rules(median_oos_sharpe_min=1.0)
        decision = _engine().evaluate_walk_forward(_wf(), rules, _with_failed_ref())
        assert decision.outcome == "Invalid"
        assert "child_fold_failed" in decision.reason_codes

    def test_no_child_failure_does_not_produce_invalid(self) -> None:
        rules = _rules(median_oos_sharpe_min=1.0)
        decision = _engine().evaluate_walk_forward(_wf(), rules, _no_failed_refs())
        assert decision.outcome != "Invalid"


# ---------------------------------------------------------------------------
# min_pass_folds_fraction
# ---------------------------------------------------------------------------


class TestMinPassFoldsFraction:
    def test_at_boundary_passes(self) -> None:
        """6/7 >= 6/7 → Pass."""
        rules = _rules(min_pass_folds_fraction=6 / 7)
        wf = _wf(count_pass=6, count_total=7)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Pass"

    def test_just_below_threshold_fails(self) -> None:
        """5/7 < 6/7 → Fail."""
        rules = _rules(min_pass_folds_fraction=6 / 7)
        wf = _wf(count_pass=5, count_total=7)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Fail"
        assert "min_pass_folds_fraction_fail" in decision.reason_codes

    def test_zero_pass_folds_fails(self) -> None:
        """0/7 < threshold → Fail."""
        rules = _rules(min_pass_folds_fraction=0.5)
        wf = _wf(count_pass=0, count_total=7)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Fail"
        assert "min_pass_folds_fraction_fail" in decision.reason_codes

    def test_zero_total_folds_is_invalid(self) -> None:
        """count_total=0 → can't compute fraction → Invalid (missing_metric)."""
        rules = _rules(min_pass_folds_fraction=0.5)
        wf = _wf(count_pass=0, count_total=0)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:pass_folds_fraction" in decision.reason_codes

    def test_all_pass(self) -> None:
        rules = _rules(min_pass_folds_fraction=1.0)
        wf = _wf(count_pass=5, count_total=5)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Pass"


# ---------------------------------------------------------------------------
# median_oos_sharpe_min
# ---------------------------------------------------------------------------


class TestMedianOosSharpeMin:
    def test_above_threshold_passes(self) -> None:
        rules = _rules(median_oos_sharpe_min=1.0)
        wf = _wf(median_sharpe=1.5)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Pass"

    def test_at_threshold_passes(self) -> None:
        rules = _rules(median_oos_sharpe_min=1.5)
        wf = _wf(median_sharpe=1.5)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Pass"

    def test_below_threshold_fails(self) -> None:
        rules = _rules(median_oos_sharpe_min=2.0)
        wf = _wf(median_sharpe=1.5)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Fail"
        assert "median_oos_sharpe_min_fail" in decision.reason_codes

    def test_none_median_produces_invalid(self) -> None:
        rules = _rules(median_oos_sharpe_min=1.0)
        wf = _wf(median_sharpe=None)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:sharpe_ratio" in decision.reason_codes

    def test_rule_not_enabled_no_sharpe_check(self) -> None:
        """When median_oos_sharpe_min is None the rule is not evaluated."""
        rules = _rules()  # all rules disabled
        wf = _wf(median_sharpe=None)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Pass"


# ---------------------------------------------------------------------------
# worst_oos_max_drawdown_max
# ---------------------------------------------------------------------------


class TestWorstOosMaxDrawdownMax:
    def test_below_threshold_passes(self) -> None:
        """Worst drawdown 0.10 <= 0.25: positive-loss convention, Pass.

        The engine stores max_drawdown as a positive fraction (0.10 = 10% loss).
        The user sets ``worst_oos_max_drawdown_max=0.25`` to allow up to 25%.
        worst_dd=0.10 (worst fold had 10% drawdown) <= 0.25 → Pass.
        """
        rules = _rules(worst_oos_max_drawdown_max=0.25)
        wf = _wf(worst_dd=0.10)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Pass"

    def test_above_threshold_fails(self) -> None:
        """Worst drawdown 0.10 > 0.05: worst fold exceeds allowed threshold → Fail."""
        rules = _rules(worst_oos_max_drawdown_max=0.05)
        wf = _wf(worst_dd=0.10)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Fail"
        assert "worst_oos_max_drawdown_max_fail" in decision.reason_codes

    def test_at_threshold_passes(self) -> None:
        """Worst drawdown exactly at threshold (0.20 <= 0.20) → Pass."""
        rules = _rules(worst_oos_max_drawdown_max=0.20)
        wf = _wf(worst_dd=0.20)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Pass"

    def test_none_worst_produces_invalid(self) -> None:
        rules = _rules(worst_oos_max_drawdown_max=0.20)
        wf = _wf(worst_dd=None)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Invalid"
        assert "missing_metric:max_drawdown" in decision.reason_codes


# ---------------------------------------------------------------------------
# Reason codes are exact
# ---------------------------------------------------------------------------


class TestReasonCodes:
    def test_all_fail_reason_codes_present(self) -> None:
        rules = _rules(
            min_pass_folds_fraction=0.9,
            median_oos_sharpe_min=2.0,
            worst_oos_max_drawdown_max=0.10,
        )
        wf = _wf(count_pass=1, count_total=7, median_sharpe=0.5, worst_dd=0.30)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Fail"
        assert "min_pass_folds_fraction_fail" in decision.reason_codes
        assert "median_oos_sharpe_min_fail" in decision.reason_codes
        assert "worst_oos_max_drawdown_max_fail" in decision.reason_codes

    def test_rule_results_populated_on_fail(self) -> None:
        rules = _rules(median_oos_sharpe_min=2.0)
        wf = _wf(median_sharpe=0.5)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert len(decision.rule_results) == 1
        rr = decision.rule_results[0]
        assert rr.rule == "median_oos_sharpe_min"
        assert rr.threshold == pytest.approx(2.0)
        assert rr.actual == pytest.approx(0.5)
        assert rr.passed is False

    def test_rule_results_empty_on_invalid(self) -> None:
        rules = _rules(median_oos_sharpe_min=1.0)
        wf = _wf(median_sharpe=None)
        decision = _engine().evaluate_walk_forward(wf, rules, _no_failed_refs())
        assert decision.outcome == "Invalid"
        assert decision.rule_results == []


# ---------------------------------------------------------------------------
# static_is_oos plan with WF fields → ValidationError
# ---------------------------------------------------------------------------


class TestStaticIsOosRejectsWFFields:
    def _base_static_kwargs(self) -> dict:
        return {
            "validation_id": "test_wf_field_rejection",
            "strategy_experiment": "test_strategy",
            "base_config": FIXTURES_DIR / "base_config.yaml",
            "mode": "static_is_oos",
            "splits": {
                "in_sample": {"start_date": date(2018, 1, 2), "end_date": date(2021, 12, 31)},
                "out_of_sample": {"start_date": date(2022, 1, 3), "end_date": date(2024, 12, 31)},
            },
        }

    def test_min_pass_folds_fraction_rejected_for_static_mode(self) -> None:
        kwargs = self._base_static_kwargs()
        kwargs["decision"] = {"min_pass_folds_fraction": 0.7}
        with pytest.raises(ValidationError, match="min_pass_folds_fraction"):
            ValidationPlan(**kwargs)

    def test_median_oos_sharpe_min_rejected_for_static_mode(self) -> None:
        kwargs = self._base_static_kwargs()
        kwargs["decision"] = {"median_oos_sharpe_min": 1.0}
        with pytest.raises(ValidationError, match="median_oos_sharpe_min"):
            ValidationPlan(**kwargs)

    def test_worst_oos_max_drawdown_max_rejected_for_static_mode(self) -> None:
        kwargs = self._base_static_kwargs()
        kwargs["decision"] = {"worst_oos_max_drawdown_max": 0.20}
        with pytest.raises(ValidationError, match="worst_oos_max_drawdown_max"):
            ValidationPlan(**kwargs)

    def test_multiple_wf_fields_rejected_together(self) -> None:
        kwargs = self._base_static_kwargs()
        kwargs["decision"] = {
            "min_pass_folds_fraction": 0.7,
            "median_oos_sharpe_min": 1.0,
        }
        with pytest.raises(ValidationError):
            ValidationPlan(**kwargs)

    def test_walk_forward_plan_accepts_wf_fields(self) -> None:
        """WF fields are valid on a walk_forward plan."""
        plan = ValidationPlan(
            validation_id="wf_ok",
            strategy_experiment="test_strategy",
            base_config=FIXTURES_DIR / "base_config.yaml",
            mode="walk_forward",
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            },
            decision={
                "min_pass_folds_fraction": 0.7,
                "median_oos_sharpe_min": 0.5,
                "worst_oos_max_drawdown_max": 0.30,
            },
        )
        assert plan.decision.min_pass_folds_fraction == pytest.approx(0.7)
        assert plan.decision.median_oos_sharpe_min == pytest.approx(0.5)
        assert plan.decision.worst_oos_max_drawdown_max == pytest.approx(0.30)


# ---------------------------------------------------------------------------
# ValidationDecision dataclass (immutability sanity)
# ---------------------------------------------------------------------------


class TestValidationDecisionImmutable:
    def test_frozen(self) -> None:
        d = ValidationDecision(outcome="Pass", reason_codes=[], rule_results=[])
        with pytest.raises(Exception):
            d.outcome = "Fail"  # type: ignore[misc]
