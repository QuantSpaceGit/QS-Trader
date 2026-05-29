"""Contract test: verify the QS-Research reference validation plan loads end-to-end.

This test resolves the sibling repository's reference plan
(QS-Research/experiments/buy_hold/validations/buy_hold_oos_2024.yaml) and runs
the same chain the real CLI executes before launching backtests:

  load_validation_plan → load_backtest_config(plan.base_config) → compute_plan_sha256

If the plan YAML, base config, or their schemas drift, this test fails so the
regression is caught before the reference document becomes stale again.

The test is skipped when the QS-Research sibling repository is not present (e.g.
in CI environments that only clone QS-Trader).
"""

from __future__ import annotations

import pathlib

import pytest

# ---------------------------------------------------------------------------
# Locate the reference plan — sibling repo, skip gracefully if absent
# ---------------------------------------------------------------------------

_QS_RESEARCH = pathlib.Path(__file__).resolve().parents[3] / "QS-Research"
_REFERENCE_PLAN = _QS_RESEARCH / "experiments" / "buy_hold" / "validations" / "buy_hold_oos_2024.yaml"

_SKIP = not _REFERENCE_PLAN.exists()
_SKIP_REASON = f"QS-Research sibling repo not found at {_QS_RESEARCH}"


@pytest.mark.skipif(_SKIP, reason=_SKIP_REASON)
class TestReferencePlanContract:
    """End-to-end contract checks for the shipped reference validation plan."""

    def test_plan_loads(self) -> None:
        """load_validation_plan must succeed and return the correct validation_id."""
        from qs_trader.validation.plan import load_validation_plan

        plan = load_validation_plan(_REFERENCE_PLAN)
        assert plan.validation_id == "buy_hold_oos_2024"
        assert plan.mode == "static_is_oos"

    def test_splits_are_non_overlapping(self) -> None:
        """IS end date must be strictly before OOS start date."""
        from qs_trader.validation.plan import load_validation_plan

        plan = load_validation_plan(_REFERENCE_PLAN)
        assert plan.splits.in_sample.end_date < plan.splits.out_of_sample.start_date

    def test_base_config_is_backtest_config(self) -> None:
        """plan.base_config must resolve to a valid BacktestConfig, not a system config."""
        from qs_trader.engine.config import load_backtest_config
        from qs_trader.validation.plan import load_validation_plan

        plan = load_validation_plan(_REFERENCE_PLAN)
        cfg = load_backtest_config(plan.base_config)
        # If this raises ConfigLoadError / ValidationError the base_config is wrong
        assert cfg.backtest_id == "buy_hold"

    def test_plan_sha256_computes(self) -> None:
        """compute_plan_sha256 must succeed — verifies plan + base_config are hash-stable."""
        from qs_trader.validation.plan import compute_plan_sha256, load_validation_plan

        plan = load_validation_plan(_REFERENCE_PLAN)
        sha = compute_plan_sha256(plan, plan.base_config)
        assert len(sha) == 64  # SHA-256 hex digest

    def test_static_is_oos_plan_hash_is_stable(self) -> None:
        """Regression guard: Phase 1 reference plan sha256 prefix must not change silently.

        This plan and base_config are stable artifacts; any change to hash logic must be
        caught here and require deliberate reviewer sign-off.
        """
        from qs_trader.validation.plan import compute_plan_sha256, load_validation_plan

        plan = load_validation_plan(_REFERENCE_PLAN)
        sha = compute_plan_sha256(plan, plan.base_config)
        assert sha.startswith("428e27b2"), (
            f"Static IS/OOS plan hash changed unexpectedly: {sha[:12]}. "
            "If this is intentional, update this pin and get reviewer sign-off."
        )

    def test_holdout_is_declared(self) -> None:
        """Reference plan must declare a holdout block to demonstrate the feature."""
        from qs_trader.validation.plan import load_validation_plan

        plan = load_validation_plan(_REFERENCE_PLAN)
        assert plan.holdout is not None
        assert plan.holdout.start_date < plan.holdout.end_date

    def test_at_least_one_rule_defined(self) -> None:
        """Reference plan must define at least one decision rule threshold."""
        from qs_trader.validation.plan import load_validation_plan

        plan = load_validation_plan(_REFERENCE_PLAN)
        decision = plan.decision
        # At minimum, oos_sharpe_min or oos_max_drawdown_max must be set
        assert decision.oos_sharpe_min is not None or decision.oos_max_drawdown_max is not None
