"""Tests for qs_trader.validation.aggregation (T5, T5.1, T5.2)."""

from __future__ import annotations

import math

import pytest

from qs_trader.validation.aggregation import MetricComparison, MetricsAggregator
from qs_trader.validation.plan import MetricsCatalog

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EMPTY_CATALOG = MetricsCatalog(required=(), recommended=())
_STANDARD_CATALOG = MetricsCatalog(
    required=("sharpe_ratio", "total_return", "max_drawdown", "num_trades", "cagr", "volatility"),
    recommended=(),
)


def _aggregator() -> MetricsAggregator:
    return MetricsAggregator()


# ---------------------------------------------------------------------------
# T5.1 — IS/OOS metric tables for the required catalog
# ---------------------------------------------------------------------------


class TestAggregateRequiredMetrics:
    """All 6 required metrics present in both IS and OOS."""

    IS = {
        "sharpe_ratio": 1.21,
        "total_return": 0.62,
        "max_drawdown": 0.18,
        "num_trades": 142.0,
        "cagr": 0.21,
        "volatility": 0.14,
    }
    OOS = {
        "sharpe_ratio": 0.94,
        "total_return": 0.31,
        "max_drawdown": 0.22,
        "num_trades": 88.0,
        "cagr": 0.10,
        "volatility": 0.16,
    }

    def test_returns_all_catalog_metrics(self) -> None:
        result = _aggregator().aggregate(self.IS, self.OOS, _STANDARD_CATALOG)
        assert set(result.keys()) == {
            "sharpe_ratio",
            "total_return",
            "max_drawdown",
            "num_trades",
            "cagr",
            "volatility",
        }

    def test_is_values(self) -> None:
        result = _aggregator().aggregate(self.IS, self.OOS, _STANDARD_CATALOG)
        assert result["sharpe_ratio"].is_val == pytest.approx(1.21)
        assert result["total_return"].is_val == pytest.approx(0.62)
        assert result["max_drawdown"].is_val == pytest.approx(0.18)
        assert result["num_trades"].is_val == pytest.approx(142.0)
        assert result["cagr"].is_val == pytest.approx(0.21)
        assert result["volatility"].is_val == pytest.approx(0.14)

    def test_oos_values(self) -> None:
        result = _aggregator().aggregate(self.IS, self.OOS, _STANDARD_CATALOG)
        assert result["sharpe_ratio"].oos == pytest.approx(0.94)
        assert result["total_return"].oos == pytest.approx(0.31)
        assert result["max_drawdown"].oos == pytest.approx(0.22)
        assert result["num_trades"].oos == pytest.approx(88.0)
        assert result["cagr"].oos == pytest.approx(0.10)
        assert result["volatility"].oos == pytest.approx(0.16)

    def test_full_always_none_phase1(self) -> None:
        """full is always None in Phase 1."""
        result = _aggregator().aggregate(self.IS, self.OOS, _STANDARD_CATALOG)
        for mc in result.values():
            assert mc.full is None

    def test_decay_sharpe(self) -> None:
        result = _aggregator().aggregate(self.IS, self.OOS, _STANDARD_CATALOG)
        expected = (1.21 - 0.94) / max(abs(1.21), 1e-6)
        assert result["sharpe_ratio"].decay == pytest.approx(expected)

    def test_decay_total_return(self) -> None:
        result = _aggregator().aggregate(self.IS, self.OOS, _STANDARD_CATALOG)
        expected = (0.62 - 0.31) / max(abs(0.62), 1e-6)
        assert result["total_return"].decay == pytest.approx(expected)

    def test_all_six_metrics_have_non_none_decay(self) -> None:
        result = _aggregator().aggregate(self.IS, self.OOS, _STANDARD_CATALOG)
        for name in ("sharpe_ratio", "total_return", "max_drawdown", "num_trades", "cagr", "volatility"):
            assert result[name].decay is not None, f"decay should not be None for {name}"


# ---------------------------------------------------------------------------
# T5.2 — Decay formula with epsilon guard (R3)
# ---------------------------------------------------------------------------


class TestDecayFormula:
    def test_normal_case(self) -> None:
        is_val = 1.5
        oos_val = 0.9
        result = _aggregator().aggregate(
            {"sharpe_ratio": is_val},
            {"sharpe_ratio": oos_val},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        expected = (is_val - oos_val) / max(abs(is_val), 1e-6)
        assert result["sharpe_ratio"].decay == pytest.approx(expected)

    def test_near_zero_is_epsilon_guard(self) -> None:
        """When IS ≈ 0, epsilon guard prevents divide-by-zero."""
        is_val = 1e-10  # extremely small but non-zero
        oos_val = 0.5
        result = _aggregator().aggregate(
            {"metric": is_val},
            {"metric": oos_val},
            MetricsCatalog(required=("metric",), recommended=()),
        )
        # With epsilon guard: denominator = 1e-6 (since abs(1e-10) < 1e-6)
        expected = (is_val - oos_val) / max(abs(is_val), 1e-6)
        assert result["metric"].decay == pytest.approx(expected)
        assert not math.isnan(result["metric"].decay)  # type: ignore[arg-type]
        assert not math.isinf(result["metric"].decay)  # type: ignore[arg-type]

    def test_exactly_zero_is_uses_epsilon(self) -> None:
        """When IS = 0, decay uses epsilon as denominator."""
        result = _aggregator().aggregate(
            {"metric": 0.0},
            {"metric": 0.5},
            MetricsCatalog(required=("metric",), recommended=()),
        )
        expected = (0.0 - 0.5) / 1e-6
        assert result["metric"].decay == pytest.approx(expected)

    def test_negative_is_value(self) -> None:
        """Negative IS value: abs() ensures correct denominator."""
        is_val = -1.0
        oos_val = -0.5
        result = _aggregator().aggregate(
            {"sharpe_ratio": is_val},
            {"sharpe_ratio": oos_val},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        expected = (is_val - oos_val) / max(abs(is_val), 1e-6)
        assert result["sharpe_ratio"].decay == pytest.approx(expected)

    def test_positive_decay_when_is_better_than_oos(self) -> None:
        """IS > OOS → positive decay."""
        result = _aggregator().aggregate(
            {"metric": 2.0},
            {"metric": 1.0},
            MetricsCatalog(required=("metric",), recommended=()),
        )
        assert result["metric"].decay is not None
        assert result["metric"].decay > 0

    def test_negative_decay_when_oos_better_than_is(self) -> None:
        """OOS > IS → negative decay."""
        result = _aggregator().aggregate(
            {"metric": 1.0},
            {"metric": 2.0},
            MetricsCatalog(required=("metric",), recommended=()),
        )
        assert result["metric"].decay is not None
        assert result["metric"].decay < 0

    def test_decay_none_when_is_missing(self) -> None:
        result = _aggregator().aggregate(
            {},
            {"sharpe_ratio": 0.94},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        assert result["sharpe_ratio"].decay is None

    def test_decay_none_when_oos_missing(self) -> None:
        result = _aggregator().aggregate(
            {"sharpe_ratio": 1.21},
            {},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        assert result["sharpe_ratio"].decay is None

    def test_decay_none_when_both_missing(self) -> None:
        result = _aggregator().aggregate(
            {},
            {},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        assert result["sharpe_ratio"].decay is None


# ---------------------------------------------------------------------------
# Missing metric edge cases
# ---------------------------------------------------------------------------


class TestMissingMetrics:
    def test_metric_missing_from_is_dict(self) -> None:
        result = _aggregator().aggregate(
            {},
            {"sharpe_ratio": 0.94},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        mc = result["sharpe_ratio"]
        assert mc.is_val is None
        assert mc.oos == pytest.approx(0.94)
        assert mc.full is None
        assert mc.decay is None

    def test_metric_missing_from_oos_dict(self) -> None:
        result = _aggregator().aggregate(
            {"sharpe_ratio": 1.21},
            {},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        mc = result["sharpe_ratio"]
        assert mc.is_val == pytest.approx(1.21)
        assert mc.oos is None
        assert mc.full is None
        assert mc.decay is None

    def test_metric_missing_from_both_dicts(self) -> None:
        result = _aggregator().aggregate(
            {},
            {},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        mc = result["sharpe_ratio"]
        assert mc.is_val is None
        assert mc.oos is None
        assert mc.full is None
        assert mc.decay is None

    def test_partial_missing_across_metrics(self) -> None:
        """Some metrics missing from one side, others from both."""
        result = _aggregator().aggregate(
            {"sharpe_ratio": 1.0},
            {"max_drawdown": 0.2},
            MetricsCatalog(
                required=("sharpe_ratio", "max_drawdown"),
                recommended=(),
            ),
        )
        assert result["sharpe_ratio"].is_val == pytest.approx(1.0)
        assert result["sharpe_ratio"].oos is None
        assert result["max_drawdown"].is_val is None
        assert result["max_drawdown"].oos == pytest.approx(0.2)


# ---------------------------------------------------------------------------
# Catalog edge cases
# ---------------------------------------------------------------------------


class TestCatalogEdgeCases:
    def test_empty_catalog_returns_empty_dict(self) -> None:
        result = _aggregator().aggregate(
            {"sharpe_ratio": 1.0},
            {"sharpe_ratio": 0.8},
            _EMPTY_CATALOG,
        )
        assert result == {}

    def test_extra_metrics_in_dicts_ignored(self) -> None:
        """Metrics present in dicts but absent from catalog are not in result."""
        result = _aggregator().aggregate(
            {"sharpe_ratio": 1.0, "unknown_metric": 99.0},
            {"sharpe_ratio": 0.8, "another_unknown": 42.0},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        assert set(result.keys()) == {"sharpe_ratio"}
        assert "unknown_metric" not in result
        assert "another_unknown" not in result

    def test_recommended_metrics_included(self) -> None:
        """Both required and recommended metrics are aggregated."""
        result = _aggregator().aggregate(
            {"sharpe_ratio": 1.0, "win_rate": 0.55},
            {"sharpe_ratio": 0.8, "win_rate": 0.50},
            MetricsCatalog(
                required=("sharpe_ratio",),
                recommended=("win_rate",),
            ),
        )
        assert "sharpe_ratio" in result
        assert "win_rate" in result
        assert result["win_rate"].is_val == pytest.approx(0.55)
        assert result["win_rate"].oos == pytest.approx(0.50)

    def test_duplicate_metric_in_required_and_recommended_deduplicated(self) -> None:
        """Duplicate metrics across required/recommended are deduplicated."""
        result = _aggregator().aggregate(
            {"sharpe_ratio": 1.0},
            {"sharpe_ratio": 0.8},
            MetricsCatalog(
                required=("sharpe_ratio",),
                recommended=("sharpe_ratio",),
            ),
        )
        assert list(result.keys()).count("sharpe_ratio") == 1

    def test_only_required_metrics(self) -> None:
        catalog = MetricsCatalog(
            required=("total_return", "sharpe_ratio"),
            recommended=(),
        )
        result = _aggregator().aggregate(
            {"total_return": 0.5, "sharpe_ratio": 1.0},
            {"total_return": 0.3, "sharpe_ratio": 0.8},
            catalog,
        )
        assert set(result.keys()) == {"total_return", "sharpe_ratio"}

    def test_only_recommended_metrics(self) -> None:
        catalog = MetricsCatalog(required=(), recommended=("win_rate",))
        result = _aggregator().aggregate(
            {"win_rate": 0.55},
            {"win_rate": 0.50},
            catalog,
        )
        assert "win_rate" in result
        assert result["win_rate"].is_val == pytest.approx(0.55)


# ---------------------------------------------------------------------------
# MetricComparison immutability
# ---------------------------------------------------------------------------


class TestMetricComparisonImmutability:
    def test_frozen_dataclass(self) -> None:
        from dataclasses import FrozenInstanceError

        mc = MetricComparison(is_val=1.0, oos=0.8, full=None, decay=0.2)
        with pytest.raises(FrozenInstanceError):
            mc.is_val = 2.0  # type: ignore[misc]

    def test_full_is_none_in_phase1(self) -> None:
        """Explicitly assert full=None contract for Phase 1."""
        result = _aggregator().aggregate(
            {"sharpe_ratio": 1.0},
            {"sharpe_ratio": 0.8},
            MetricsCatalog(required=("sharpe_ratio",), recommended=()),
        )
        assert result["sharpe_ratio"].full is None
