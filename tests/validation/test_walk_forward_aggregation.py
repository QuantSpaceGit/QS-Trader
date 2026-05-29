"""Tests for WalkForwardAggregator and FoldAggregates (Phase 2A.4)."""

from __future__ import annotations

import pytest

from qs_trader.validation.aggregation import FoldAggregates, MetricComparison, WalkForwardAggregator
from qs_trader.validation.decision import ValidationDecision

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mc(oos: float | None, is_val: float | None = None) -> MetricComparison:
    """Construct a MetricComparison with the given OOS and IS values."""
    return MetricComparison(is_val=is_val, oos=oos, full=None, decay=None)


def _pass() -> ValidationDecision:
    return ValidationDecision(outcome="Pass", reason_codes=[], rule_results=[])


def _fail() -> ValidationDecision:
    return ValidationDecision(outcome="Fail", reason_codes=["some_rule_fail"], rule_results=[])


def _invalid() -> ValidationDecision:
    return ValidationDecision(outcome="Invalid", reason_codes=["child_fold_failed"], rule_results=[])


# ---------------------------------------------------------------------------
# Basic aggregation
# ---------------------------------------------------------------------------


class TestWalkForwardAggregatorBasic:
    def test_n1_fold(self) -> None:
        """Single fold: median == value, iqr == 0.0, min == max == value."""
        comps = [{"sharpe_ratio": _mc(oos=1.5)}]
        decs = [_pass()]
        fa = WalkForwardAggregator().aggregate(comps, decs, metric="sharpe_ratio")
        assert fa.metric == "sharpe_ratio"
        assert fa.median == pytest.approx(1.5)
        assert fa.iqr == pytest.approx(0.0)
        assert fa.min == pytest.approx(1.5)
        assert fa.max == pytest.approx(1.5)
        assert fa.count_pass_folds == 1
        assert fa.count_total_folds == 1

    def test_n3_folds_odd(self) -> None:
        """N=3 folds: median is the middle value."""
        comps = [
            {"sharpe_ratio": _mc(oos=1.0)},
            {"sharpe_ratio": _mc(oos=2.0)},
            {"sharpe_ratio": _mc(oos=3.0)},
        ]
        decs = [_pass(), _pass(), _fail()]
        fa = WalkForwardAggregator().aggregate(comps, decs, metric="sharpe_ratio")
        assert fa.median == pytest.approx(2.0)
        assert fa.min == pytest.approx(1.0)
        assert fa.max == pytest.approx(3.0)
        assert fa.count_pass_folds == 2
        assert fa.count_total_folds == 3

    def test_n5_folds_odd(self) -> None:
        """N=5 folds: median, IQR, min, max."""
        values = [0.5, 1.0, 1.5, 2.0, 2.5]
        comps = [{"sharpe_ratio": _mc(oos=v)} for v in values]
        decs = [_pass()] * 4 + [_fail()]
        fa = WalkForwardAggregator().aggregate(comps, decs, metric="sharpe_ratio")
        assert fa.median == pytest.approx(1.5)
        assert fa.iqr == pytest.approx(1.5)
        assert fa.min == pytest.approx(0.5)
        assert fa.max == pytest.approx(2.5)
        assert fa.count_pass_folds == 4
        assert fa.count_total_folds == 5

    def test_n4_folds_even(self) -> None:
        """N=4 folds: median is average of two middle values."""
        values = [1.0, 2.0, 3.0, 4.0]
        comps = [{"sharpe_ratio": _mc(oos=v)} for v in values]
        decs = [_pass()] * 4
        fa = WalkForwardAggregator().aggregate(comps, decs, metric="sharpe_ratio")
        # median of [1,2,3,4] = (2+3)/2 = 2.5
        assert fa.median == pytest.approx(2.5)
        assert fa.min == pytest.approx(1.0)
        assert fa.max == pytest.approx(4.0)
        assert fa.count_pass_folds == 4
        assert fa.count_total_folds == 4

    def test_all_failed_folds_no_oos_values(self) -> None:
        """All folds failed: median/iqr/min/max are None; pass count is 0."""
        comps = [
            {"sharpe_ratio": _mc(oos=None)},
            {"sharpe_ratio": _mc(oos=None)},
        ]
        decs = [_invalid(), _invalid()]
        fa = WalkForwardAggregator().aggregate(comps, decs, metric="sharpe_ratio")
        assert fa.median is None
        assert fa.iqr is None
        assert fa.min is None
        assert fa.max is None
        assert fa.count_pass_folds == 0
        assert fa.count_total_folds == 2

    def test_empty_fold_list(self) -> None:
        """Zero folds: all aggregates are None, counts are 0."""
        fa = WalkForwardAggregator().aggregate([], [], metric="sharpe_ratio")
        assert fa.median is None
        assert fa.iqr is None
        assert fa.min is None
        assert fa.max is None
        assert fa.count_pass_folds == 0
        assert fa.count_total_folds == 0


# ---------------------------------------------------------------------------
# count_pass_folds accuracy
# ---------------------------------------------------------------------------


class TestCountPassFolds:
    def test_count_pass_when_subset_pass(self) -> None:
        """count_pass_folds counts only 'Pass' outcome decisions."""
        comps = [{"sharpe_ratio": _mc(oos=1.0)}] * 5
        decs = [_pass(), _pass(), _fail(), _invalid(), _pass()]
        fa = WalkForwardAggregator().aggregate(comps, decs)
        assert fa.count_pass_folds == 3
        assert fa.count_total_folds == 5

    def test_count_pass_zero_when_all_fail(self) -> None:
        comps = [{"sharpe_ratio": _mc(oos=0.5)}] * 3
        decs = [_fail(), _fail(), _fail()]
        fa = WalkForwardAggregator().aggregate(comps, decs)
        assert fa.count_pass_folds == 0

    def test_oos_missing_but_pass_decision(self) -> None:
        """A fold can have a Pass decision but no OOS value for the aggregated metric.

        count_pass_folds still reflects the outcome; only the median computation
        skips that fold.
        """
        comps = [
            {"sharpe_ratio": _mc(oos=None)},  # no OOS value
            {"sharpe_ratio": _mc(oos=2.0)},
        ]
        decs = [_pass(), _pass()]
        fa = WalkForwardAggregator().aggregate(comps, decs)
        assert fa.count_pass_folds == 2
        assert fa.count_total_folds == 2
        # Only second fold contributes to median
        assert fa.median == pytest.approx(2.0)
        assert fa.iqr == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# Non-default metric
# ---------------------------------------------------------------------------


class TestNonDefaultMetric:
    def test_total_return_metric(self) -> None:
        """Aggregating a non-default metric key works correctly."""
        comps = [
            {"total_return": _mc(oos=0.15)},
            {"total_return": _mc(oos=0.25)},
            {"total_return": _mc(oos=0.20)},
        ]
        decs = [_pass()] * 3
        fa = WalkForwardAggregator().aggregate(comps, decs, metric="total_return")
        assert fa.metric == "total_return"
        assert fa.median == pytest.approx(0.20)
        assert fa.min == pytest.approx(0.15)
        assert fa.max == pytest.approx(0.25)

    def test_metric_key_missing_from_comparison_dict(self) -> None:
        """Folds that don't include the requested metric contribute count_total only."""
        comps = [
            {"sharpe_ratio": _mc(oos=1.0)},  # no 'total_return' key
            {"sharpe_ratio": _mc(oos=1.5)},
        ]
        decs = [_pass()] * 2
        fa = WalkForwardAggregator().aggregate(comps, decs, metric="total_return")
        assert fa.median is None
        assert fa.count_total_folds == 2

    def test_max_drawdown_metric(self) -> None:
        """max_drawdown aggregation: worst drawdown is the max (largest positive fraction).

        The engine stores max_drawdown using positive-loss convention (0.25 = 25% loss).
        The largest value is the worst fold; ``fa.max`` feeds ``worst_oos_max_drawdown``
        in the decision engine.
        """
        comps = [
            {"max_drawdown": _mc(oos=0.10)},
            {"max_drawdown": _mc(oos=0.30)},
            {"max_drawdown": _mc(oos=0.20)},
        ]
        decs = [_pass()] * 3
        fa = WalkForwardAggregator().aggregate(comps, decs, metric="max_drawdown")
        # sorted: [0.10, 0.20, 0.30]; median = 0.20
        assert fa.median == pytest.approx(0.20)
        # max = 0.30 is the worst (deepest) fold drawdown
        assert fa.max == pytest.approx(0.30)
        assert fa.min == pytest.approx(0.10)


# ---------------------------------------------------------------------------
# FoldAggregates dataclass immutability
# ---------------------------------------------------------------------------


class TestFoldAggregatesImmutability:
    def test_frozen(self) -> None:
        fa = FoldAggregates(
            metric="sharpe_ratio",
            median=1.0,
            iqr=0.5,
            min=0.5,
            max=1.5,
            count_pass_folds=3,
            count_total_folds=3,
        )
        with pytest.raises(Exception):
            fa.median = 9.9  # type: ignore[misc]

    def test_fields(self) -> None:
        fa = FoldAggregates(
            metric="sharpe_ratio",
            median=1.2,
            iqr=0.4,
            min=1.0,
            max=1.4,
            count_pass_folds=5,
            count_total_folds=7,
        )
        assert fa.metric == "sharpe_ratio"
        assert fa.count_pass_folds == 5
        assert fa.count_total_folds == 7
