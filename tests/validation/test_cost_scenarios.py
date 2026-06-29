"""Tests for Phase 2A.2 — Cost scenarios.

Covers:
- ``apply_scenario_overrides`` pure-function semantics (T2.1).
- Plan-load rejection of unknown override keys / invalid scenario names /
  duplicates (T2.2 + name regex).
- Hash stability when ``cost_scenarios`` is absent (back-compat with the
  ``36919c93`` Phase 1 static plan pin and the ``description`` exclusion
  pattern).
- ``SequentialValidationRunner`` (fold × scenario) matrix with
  ``fail_fast`` / ``continue`` (T2.3) and the on-disk
  ``scenarios/<name>/folds/...`` layout (T2.5).
- ``summary.json`` ``cost_scenarios`` block emitted when declared and absent
  otherwise (T2.4).
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml
from pydantic import ValidationError

from qs_trader.engine.config import (
    BacktestConfig,
    DataSelectionConfig,
    DataSourceConfig,
    RiskPolicyConfig,
    StrategyConfigItem,
)
from qs_trader.validation.cost_scenarios import apply_scenario_overrides, validate_override_path
from qs_trader.validation.plan import (
    CostScenarioSpec,
    DateRange,
    ExecutionSpec,
    StaticSplitSpec,
    ValidationPlan,
    _plan_to_canonical_dict,
    load_validation_plan,
)
from qs_trader.validation.runner import ChildRunFailedError, SequentialValidationRunner
from qs_trader.validation.splits.base import ValidationSplit

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_base_config() -> BacktestConfig:
    return BacktestConfig(
        backtest_id="cost_scenarios_test",
        start_date=datetime(2020, 1, 2),
        end_date=datetime(2020, 6, 30),
        initial_equity=Decimal("100000"),
        data=DataSelectionConfig(sources=[DataSourceConfig(name="yahoo-us-equity-1d-csv", universe=["AAPL"])]),
        strategies=[
            StrategyConfigItem(
                strategy_id="buy_and_hold",
                universe=["AAPL"],
                data_sources=["yahoo-us-equity-1d-csv"],
                config={},
            )
        ],
        risk_policy=RiskPolicyConfig(name="naive", config={}),
    )


def _make_splits() -> list[ValidationSplit]:
    return [
        ValidationSplit(
            fold_index=0,
            role="is",
            test_range=DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 6, 30)),
        ),
        ValidationSplit(
            fold_index=1,
            role="oos",
            test_range=DateRange(start_date=date(2020, 7, 1), end_date=date(2020, 12, 30)),
        ),
    ]


def _make_plan(
    *,
    cost_scenarios: list[CostScenarioSpec] | None,
    on_child_failure: str = "continue",
) -> ValidationPlan:
    return ValidationPlan(
        validation_id="cost_scenarios_test",
        strategy_experiment="test_strategy",
        base_config=FIXTURES_DIR / "runner_base_config.yaml",
        mode="static_is_oos",
        splits=StaticSplitSpec(
            in_sample=DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 6, 30)),
            out_of_sample=DateRange(start_date=date(2020, 7, 1), end_date=date(2020, 12, 30)),
        ),
        execution=ExecutionSpec(on_child_failure=on_child_failure),  # type: ignore[arg-type]
        cost_scenarios=cost_scenarios,
    )


def _make_mock_engine(bars: int = 100) -> MagicMock:
    from datetime import timedelta as td

    mock_result = Mock()
    mock_result.bars_processed = bars
    mock_result.duration = td(seconds=1)

    mock_engine = MagicMock()
    mock_engine.run.return_value = mock_result
    mock_engine.__enter__ = Mock(return_value=mock_engine)
    mock_engine.__exit__ = Mock(return_value=False)
    return mock_engine


def _make_failing_engine() -> MagicMock:
    mock_engine = MagicMock()
    mock_engine.run.side_effect = RuntimeError("simulated fold failure")
    mock_engine.__enter__ = Mock(return_value=mock_engine)
    mock_engine.__exit__ = Mock(return_value=False)
    return mock_engine


# ---------------------------------------------------------------------------
# T2.1 — apply_scenario_overrides
# ---------------------------------------------------------------------------


class TestApplyScenarioOverrides:
    def test_empty_overrides_returns_input(self) -> None:
        cfg = _make_base_config()
        out = apply_scenario_overrides(cfg, {})
        # Identity preserved when no overrides (BacktestConfig is immutable).
        assert out is cfg

    def test_scalar_override(self) -> None:
        cfg = _make_base_config()
        out = apply_scenario_overrides(cfg, {"replay_speed": 5.0})
        assert out.replay_speed == 5.0
        # Other fields preserved
        assert out.initial_equity == cfg.initial_equity
        assert out.backtest_id == cfg.backtest_id

    def test_input_not_mutated(self) -> None:
        cfg = _make_base_config()
        original_speed = cfg.replay_speed
        _ = apply_scenario_overrides(cfg, {"replay_speed": 7.0})
        assert cfg.replay_speed == original_speed

    def test_returns_backtest_config_type(self) -> None:
        cfg = _make_base_config()
        out = apply_scenario_overrides(cfg, {"replay_speed": 1.0})
        assert isinstance(out, BacktestConfig)

    def test_nested_dotted_override_creates_subconfig(self) -> None:
        """``feature_config.feature_version`` should populate the nested model."""
        cfg = _make_base_config()
        out = apply_scenario_overrides(
            cfg,
            {
                "feature_config.feature_version": "v9",
                "feature_config.regime_version": "v9",
            },
        )
        assert out.feature_config is not None
        assert out.feature_config.feature_version == "v9"
        assert out.feature_config.regime_version == "v9"

    def test_multiple_independent_paths_merge(self) -> None:
        cfg = _make_base_config()
        out = apply_scenario_overrides(
            cfg,
            {
                "replay_speed": 2.0,
                "feature_config.feature_version": "v2",
            },
        )
        assert out.replay_speed == 2.0
        assert out.feature_config is not None
        assert out.feature_config.feature_version == "v2"


# ---------------------------------------------------------------------------
# T2.2 — Override-path schema validation
# ---------------------------------------------------------------------------


class TestValidateOverridePath:
    def test_known_top_level_path(self) -> None:
        validate_override_path(BacktestConfig, "replay_speed")  # no raise

    def test_known_nested_path(self) -> None:
        validate_override_path(BacktestConfig, "feature_config.feature_version")  # no raise

    def test_unknown_top_level_path_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown_override_key:bogus_field"):
            validate_override_path(BacktestConfig, "bogus_field")

    def test_unknown_nested_path_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown_override_key:feature_config.no_such"):
            validate_override_path(BacktestConfig, "feature_config.no_such")

    def test_descend_below_leaf_rejected(self) -> None:
        with pytest.raises(ValueError, match="unknown_override_key:replay_speed.too_deep"):
            validate_override_path(BacktestConfig, "replay_speed.too_deep")


class TestPlanLoadValidatesOverridePaths:
    def test_unknown_override_path_raises_validation_error(self) -> None:
        with pytest.raises(ValidationError) as exc_info:
            _make_plan(
                cost_scenarios=[
                    CostScenarioSpec(name="bad", overrides={"not_a_real_field": 1}),
                ],
            )
        assert "unknown_override_key:not_a_real_field" in str(exc_info.value)

    def test_invalid_scenario_name_rejected(self) -> None:
        with pytest.raises(ValidationError, match=r"must match \^\[A-Za-z0-9_-\]\+\$"):
            CostScenarioSpec(name="bad/name", overrides={})

    def test_duplicate_scenario_names_rejected(self) -> None:
        with pytest.raises(ValidationError, match="Duplicate cost-scenario name"):
            _make_plan(
                cost_scenarios=[
                    CostScenarioSpec(name="base", overrides={}),
                    CostScenarioSpec(name="base", overrides={"replay_speed": 1.0}),
                ],
            )

    def test_cost_scenarios_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            CostScenarioSpec(name="x", overrides={}, bogus_extra=True)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# Hash stability — adding cost_scenarios=None must not change canonical dict
# ---------------------------------------------------------------------------


class TestCanonicalDictStability:
    def test_cost_scenarios_none_excluded_from_canonical_dict(self) -> None:
        plan = _make_plan(cost_scenarios=None)
        d = _plan_to_canonical_dict(plan)
        assert "cost_scenarios" not in d

    def test_cost_scenarios_declared_included_in_canonical_dict(self) -> None:
        plan = _make_plan(
            cost_scenarios=[CostScenarioSpec(name="base", overrides={})],
        )
        d = _plan_to_canonical_dict(plan)
        assert "cost_scenarios" in d
        assert d["cost_scenarios"] == [{"name": "base", "overrides": {}}]

    def test_static_reference_plan_hash_pin_unchanged(self) -> None:
        """The Phase 1 reference plan must still hash to the 36919c93 prefix."""
        ref_plan = (
            Path(__file__).resolve().parents[3]
            / "QS-Research"
            / ("experiments/buy_hold/validations/buy_hold_oos_2024.yaml")
        )
        if not ref_plan.exists():
            pytest.skip("QS-Research sibling repo not present")
        from qs_trader.validation.plan import compute_plan_sha256  # noqa: PLC0415

        plan = load_validation_plan(ref_plan)
        sha = compute_plan_sha256(plan, plan.base_config)
        assert sha.startswith("36919c93"), f"Static plan hash drifted: {sha[:12]}"


# ---------------------------------------------------------------------------
# T2.3 + T2.5 — Runner (fold × scenario) matrix and on-disk layout
# ---------------------------------------------------------------------------


class TestRunnerCostScenarioMatrix:
    def _run(self, plan: ValidationPlan, tmp_path: Path, engine: MagicMock) -> Any:
        base_config = _make_base_config()
        validations_dir = tmp_path / "validations" / plan.validation_id
        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = engine
            runner = SequentialValidationRunner(
                plan=plan,
                splits=_make_splits(),
                base_config=base_config,
                validations_dir=validations_dir,
            )
            return runner.run(), validations_dir

    def test_two_scenarios_run_all_folds(self, tmp_path: Path) -> None:
        plan = _make_plan(
            cost_scenarios=[
                CostScenarioSpec(name="base", overrides={}),
                CostScenarioSpec(name="high", overrides={"replay_speed": 0.0}),
            ],
        )
        refs, vdir = self._run(plan, tmp_path, _make_mock_engine())
        assert len(refs) == 4  # 2 scenarios × 2 folds
        assert (vdir / "scenarios" / "base" / "folds" / "f0__is").is_dir()
        assert (vdir / "scenarios" / "base" / "folds" / "f1__oos").is_dir()
        assert (vdir / "scenarios" / "high" / "folds" / "f0__is").is_dir()
        assert (vdir / "scenarios" / "high" / "folds" / "f1__oos").is_dir()
        # The Phase 1 `folds/` directory must NOT be created when scenarios are
        # declared.
        assert not (vdir / "folds").exists()
        # Each ref carries its scenario tag.
        scenarios = {(r.scenario, r.fold_id) for r in refs}
        assert scenarios == {
            ("base", "f0__is"),
            ("base", "f1__oos"),
            ("high", "f0__is"),
            ("high", "f1__oos"),
        }

    def test_failure_in_one_scenario_does_not_abort_others_under_continue(self, tmp_path: Path) -> None:
        plan = _make_plan(
            cost_scenarios=[
                CostScenarioSpec(name="bad", overrides={}),
                CostScenarioSpec(name="good", overrides={}),
            ],
            on_child_failure="continue",
        )
        refs, vdir = self._run(plan, tmp_path, _make_failing_engine())
        assert len(refs) == 4
        # All four refs failed (engine always raises), but both scenarios ran
        # to completion — failure isolation across scenarios is the contract.
        assert all(r.status == "failed" for r in refs)
        seen_scenarios = {r.scenario for r in refs}
        assert seen_scenarios == {"bad", "good"}

    def test_fail_fast_aborts_on_first_scenario_failure(self, tmp_path: Path) -> None:
        plan = _make_plan(
            cost_scenarios=[
                CostScenarioSpec(name="bad", overrides={}),
                CostScenarioSpec(name="never_reached", overrides={}),
            ],
            on_child_failure="fail_fast",
        )
        with pytest.raises(ChildRunFailedError) as exc_info:
            self._run(plan, tmp_path, _make_failing_engine())
        partial = exc_info.value.partial_refs
        # Only the first scenario's first fold should have been recorded.
        assert len(partial) == 1
        assert partial[0].scenario == "bad"

    def test_no_cost_scenarios_layout_unchanged(self, tmp_path: Path) -> None:
        plan = _make_plan(cost_scenarios=None)
        refs, vdir = self._run(plan, tmp_path, _make_mock_engine())
        assert (vdir / "folds" / "f0__is").is_dir()
        assert (vdir / "folds" / "f1__oos").is_dir()
        assert not (vdir / "scenarios").exists()
        assert all(r.scenario is None for r in refs)


# ---------------------------------------------------------------------------
# T2.4 — summary.json cost_scenarios block
# ---------------------------------------------------------------------------


class TestSummaryCostScenariosBlock:
    def _write_perf(self, run_dir: Path, sharpe: float = 1.2) -> None:
        run_dir.mkdir(parents=True, exist_ok=True)
        perf = {
            "total_return": 0.1,
            "cagr": 0.1,
            "sharpe_ratio": sharpe,
            "max_drawdown": 0.05,
            "volatility": 0.1,
            "num_trades": 40,
        }
        (run_dir / "performance.json").write_text(json.dumps(perf))

    def test_block_emitted_when_scenarios_declared(self, tmp_path: Path) -> None:
        """Run the CLI end-to-end with mocked engine + perf JSON to assert the
        cost_scenarios summary block."""
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        # Materialize plan YAML and base config in tmp_path.
        base_cfg_path = tmp_path / "base.yaml"
        base_cfg_path.write_text((FIXTURES_DIR / "runner_base_config.yaml").read_text())
        plan_yaml = tmp_path / "plan_with_scenarios.yaml"
        plan_yaml.write_text(
            yaml.safe_dump(
                {
                    "validation_id": "cs_test",
                    "strategy_experiment": "test_strategy",
                    "base_config": str(base_cfg_path),
                    "mode": "static_is_oos",
                    "splits": {
                        "in_sample": {"start_date": "2020-01-02", "end_date": "2020-06-30"},
                        "out_of_sample": {"start_date": "2020-07-01", "end_date": "2020-12-30"},
                    },
                    "decision": {"rules": {"oos_sharpe_min": 0.5}},
                    "cost_scenarios": [
                        {"name": "base", "overrides": {}},
                        {"name": "high", "overrides": {"replay_speed": 0.0}},
                    ],
                }
            )
        )
        out_dir = plan_yaml.parent / "cs_test"

        # Mocked engine + writer for per-fold performance.json.
        def fake_from_config(child_config: Any, **kwargs: Any) -> MagicMock:
            results_dir = Path(kwargs["results_dir"])
            self._write_perf(results_dir, sharpe=1.5)
            return _make_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = fake_from_config
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])
        assert result.exit_code in (0, 1, 2, 3), result.output

        summary = json.loads((out_dir / "summary.json").read_text())
        assert "cost_scenarios" in summary
        names = [s["name"] for s in summary["cost_scenarios"]]
        assert names == ["base", "high"]
        for entry in summary["cost_scenarios"]:
            assert "decision" in entry
            assert "reason_codes" in entry
            assert "folds" in entry
            assert {f["fold_id"] for f in entry["folds"]} == {"f0__is", "f1__oos"}

    def test_block_absent_when_scenarios_not_declared(self, tmp_path: Path) -> None:
        """The cost_scenarios key must be entirely omitted when not declared."""
        from qs_trader.validation.aggregation import MetricsAggregator
        from qs_trader.validation.decision import DecisionEngine
        from qs_trader.validation.reporting.summary import SummaryWriter

        plan = _make_plan(cost_scenarios=None)
        writer = SummaryWriter()
        out_dir = tmp_path / "novalid"
        comparison = MetricsAggregator().aggregate({}, {}, plan.metrics)
        decision = DecisionEngine(plan.metrics).evaluate(comparison, plan.decision, [])
        writer.write_summary(
            validation_id=plan.validation_id,
            plan=plan,
            plan_sha256="0" * 64,
            base_config_sha256="1" * 64,
            outcome=decision.outcome,
            reason_codes=list(decision.reason_codes),
            folds=[],
            comparison=comparison,
            decision=decision,
            audit={},
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:00:00+00:00",
            out_dir=out_dir,
        )
        summary = json.loads((out_dir / "summary.json").read_text())
        assert "cost_scenarios" not in summary


# ---------------------------------------------------------------------------
# CLI dry-run cost-scenarios expansion
# ---------------------------------------------------------------------------


class TestCliDryRunCostScenarios:
    def test_dry_run_prints_scenarios(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        base_cfg_path = tmp_path / "base.yaml"
        base_cfg_path.write_text((FIXTURES_DIR / "runner_base_config.yaml").read_text())
        plan_yaml = tmp_path / "plan.yaml"
        plan_yaml.write_text(
            yaml.safe_dump(
                {
                    "validation_id": "dr",
                    "strategy_experiment": "exp",
                    "base_config": str(base_cfg_path),
                    "mode": "static_is_oos",
                    "splits": {
                        "in_sample": {"start_date": "2020-01-02", "end_date": "2020-06-30"},
                        "out_of_sample": {"start_date": "2020-07-01", "end_date": "2020-12-30"},
                    },
                    "cost_scenarios": [
                        {"name": "base", "overrides": {}},
                        {"name": "high", "overrides": {"replay_speed": 0.0}},
                    ],
                }
            )
        )
        result = CliRunner().invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "Cost scenarios:" in result.output
        assert "base" in result.output
        assert "high" in result.output
        assert "folds=" in result.output

    def test_dry_run_table_aligned(self, tmp_path: Path) -> None:
        """The scenario name column is padded so ``folds=`` lines up regardless
        of name length (Phase 2A.2 INFO I4)."""
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        base_cfg_path = tmp_path / "base.yaml"
        base_cfg_path.write_text((FIXTURES_DIR / "runner_base_config.yaml").read_text())
        plan_yaml = tmp_path / "plan.yaml"
        plan_yaml.write_text(
            yaml.safe_dump(
                {
                    "validation_id": "dr_align",
                    "strategy_experiment": "exp",
                    "base_config": str(base_cfg_path),
                    "mode": "static_is_oos",
                    "splits": {
                        "in_sample": {"start_date": "2020-01-02", "end_date": "2020-06-30"},
                        "out_of_sample": {"start_date": "2020-07-01", "end_date": "2020-12-30"},
                    },
                    "cost_scenarios": [
                        {"name": "lo", "overrides": {}},
                        {"name": "high_friction_long", "overrides": {"replay_speed": 0.0}},
                    ],
                }
            )
        )
        result = CliRunner().invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert result.exit_code == 0, result.output
        # Locate the two scenario lines and assert the ``folds=`` column lines up.
        lines = [ln for ln in result.output.splitlines() if "folds=" in ln]
        assert len(lines) == 2, lines
        folds_columns = [ln.index("folds=") for ln in lines]
        assert folds_columns[0] == folds_columns[1], (
            f"Scenario rows not column-aligned: {folds_columns} in lines {lines}"
        )


# ---------------------------------------------------------------------------
# I2 — validate_override_path empty/malformed path uses unknown_override_key prefix
# ---------------------------------------------------------------------------


class TestValidateOverridePathPrefixForEmptyPath:
    def test_empty_path_uses_unknown_override_key_prefix(self) -> None:
        with pytest.raises(ValueError, match=r"^unknown_override_key:"):
            validate_override_path(BacktestConfig, "")

    def test_leading_dot_path_uses_unknown_override_key_prefix(self) -> None:
        with pytest.raises(ValueError, match=r"^unknown_override_key:\.foo"):
            validate_override_path(BacktestConfig, ".foo")

    def test_trailing_dot_path_uses_unknown_override_key_prefix(self) -> None:
        with pytest.raises(ValueError, match=r"^unknown_override_key:foo\."):
            validate_override_path(BacktestConfig, "foo.")


# ---------------------------------------------------------------------------
# B1 — Top-level outcome / exit code aggregate across scenarios; reason code emitted
# ---------------------------------------------------------------------------


def _write_cost_scenario_plan(
    tmp_path: Path,
    *,
    validation_id: str,
    on_child_failure: str = "continue",
    oos_sharpe_min: float = 0.5,
    scenarios: list[dict[str, Any]] | None = None,
) -> tuple[Path, Path]:
    """Materialise a plan YAML + base config and return ``(plan_path, out_dir)``."""
    base_cfg_path = tmp_path / "base.yaml"
    base_cfg_path.write_text((FIXTURES_DIR / "runner_base_config.yaml").read_text())
    plan_yaml = tmp_path / f"{validation_id}.yaml"
    plan_yaml.write_text(
        yaml.safe_dump(
            {
                "validation_id": validation_id,
                "strategy_experiment": "test_strategy",
                "base_config": str(base_cfg_path),
                "mode": "static_is_oos",
                "splits": {
                    "in_sample": {"start_date": "2020-01-02", "end_date": "2020-06-30"},
                    "out_of_sample": {"start_date": "2020-07-01", "end_date": "2020-12-30"},
                },
                "execution": {"on_child_failure": on_child_failure},
                "decision": {"rules": {"oos_sharpe_min": oos_sharpe_min}},
                "cost_scenarios": scenarios
                or [
                    {"name": "base", "overrides": {}},
                    {"name": "high", "overrides": {"replay_speed": 0.0}},
                ],
            }
        )
    )
    return plan_yaml, plan_yaml.parent / validation_id


def _make_perf_writer(scenario_sharpes: dict[str, float], default_sharpe: float = 2.0) -> Any:
    """Return a ``BacktestEngine.from_config`` side-effect that writes a
    ``performance.json`` whose sharpe depends on which scenario directory the
    fold landed in.
    """

    def _side_effect(child_config: Any, **kwargs: Any) -> MagicMock:
        results_dir = Path(kwargs["results_dir"])
        scenario_name = None
        # Path looks like .../scenarios/<name>/folds/<fold_id>
        parts = results_dir.parts
        if "scenarios" in parts:
            idx = parts.index("scenarios")
            if idx + 1 < len(parts):
                scenario_name = parts[idx + 1]
        sharpe = scenario_sharpes.get(scenario_name or "", default_sharpe)
        results_dir.mkdir(parents=True, exist_ok=True)
        (results_dir / "performance.json").write_text(
            json.dumps(
                {
                    "total_return": 0.1,
                    "cagr": 0.1,
                    "sharpe_ratio": sharpe,
                    "max_drawdown": 0.05,
                    "volatility": 0.1,
                    "num_trades": 40,
                }
            )
        )
        return _make_mock_engine()

    return _side_effect


class TestCrossScenarioOutcomeAggregation:
    """Phase 2A.2 BLOCKER B1: top-level outcome / exit code / reason codes must
    aggregate across all declared cost scenarios."""

    def test_non_base_fail_drives_top_level_fail_and_emits_reason_code(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml, out_dir = _write_cost_scenario_plan(tmp_path, validation_id="b1_fail", oos_sharpe_min=1.0)
        # base passes (sharpe=2.0), high fails (sharpe=0.1).
        side_effect = _make_perf_writer({"base": 2.0, "high": 0.1})

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code == 1, result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] == "Fail"
        assert "cost_scenario_failed:high" in summary["reason_codes"]
        assert "cost_scenario_failed:base" not in summary["reason_codes"]
        # Per-scenario block still records the individual outcomes.
        outcomes = {s["name"]: s["decision"] for s in summary["cost_scenarios"]}
        assert outcomes == {"base": "Pass", "high": "Fail"}

    def test_all_pass_no_failure_reason_code(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml, out_dir = _write_cost_scenario_plan(tmp_path, validation_id="b1_pass", oos_sharpe_min=0.5)
        side_effect = _make_perf_writer({"base": 2.0, "high": 1.5})

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code == 0, result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] == "Pass"
        assert not any(rc.startswith("cost_scenario_failed:") for rc in summary["reason_codes"])

    def test_non_base_review_required_propagates_without_fail(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        # Use the ``is_to_oos_sharpe_decay_warn`` review rule (the only review
        # rule currently in the catalog) to surface the ``high`` scenario as
        # ReviewRequired without ever becoming Fail.  Decay is measured as
        # IS-Sharpe minus OOS-Sharpe; the base scenario keeps decay low while
        # the high scenario shows a large drop.
        plan_yaml = tmp_path / "b1_review.yaml"
        base_cfg_path = tmp_path / "base.yaml"
        base_cfg_path.write_text((FIXTURES_DIR / "runner_base_config.yaml").read_text())
        plan_yaml.write_text(
            yaml.safe_dump(
                {
                    "validation_id": "b1_review",
                    "strategy_experiment": "test_strategy",
                    "base_config": str(base_cfg_path),
                    "mode": "static_is_oos",
                    "splits": {
                        "in_sample": {"start_date": "2020-01-02", "end_date": "2020-06-30"},
                        "out_of_sample": {"start_date": "2020-07-01", "end_date": "2020-12-30"},
                    },
                    "execution": {"on_child_failure": "continue"},
                    "decision": {
                        "rules": {
                            "on_review_required": [
                                {"rule": "is_to_oos_sharpe_decay_warn", "threshold": 0.5},
                            ],
                        }
                    },
                    "cost_scenarios": [
                        {"name": "base", "overrides": {}},
                        {"name": "high", "overrides": {"replay_speed": 0.0}},
                    ],
                }
            )
        )
        out_dir = plan_yaml.parent / "b1_review"

        def side_effect(child_config: Any, **kwargs: Any) -> MagicMock:
            results_dir = Path(kwargs["results_dir"])
            parts = results_dir.parts
            scenario = parts[parts.index("scenarios") + 1] if "scenarios" in parts else ""
            role = results_dir.name.split("__")[-1] if "__" in results_dir.name else ""
            # base scenario: IS=2.0, OOS=1.9 (decay 0.1 → review passes).
            # high scenario: IS=2.0, OOS=0.5 (decay 1.5 → review breached).
            if scenario == "base":
                sharpe = 2.0 if role == "is" else 1.9
            else:
                sharpe = 2.0 if role == "is" else 0.5
            results_dir.mkdir(parents=True, exist_ok=True)
            (results_dir / "performance.json").write_text(
                json.dumps(
                    {
                        "total_return": 0.1,
                        "cagr": 0.1,
                        "sharpe_ratio": sharpe,
                        "max_drawdown": 0.05,
                        "volatility": 0.1,
                        "num_trades": 100,
                    }
                )
            )
            return _make_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code == 2, result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] == "ReviewRequired"
        assert "cost_scenario_failed:high" in summary["reason_codes"]
        outcomes = {s["name"]: s["decision"] for s in summary["cost_scenarios"]}
        assert outcomes["base"] == "Pass"
        assert outcomes["high"] == "ReviewRequired"

    def test_fail_fast_aborts_mid_scenario_and_reports_failure(self, tmp_path: Path) -> None:
        """Under ``fail_fast`` the runner aborts at the first failing fold.
        The aggregated top-level outcome must still reflect the failure and
        emit ``cost_scenario_failed:<name>`` for the aborted scenario."""
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml, out_dir = _write_cost_scenario_plan(
            tmp_path,
            validation_id="b1_failfast",
            on_child_failure="fail_fast",
            oos_sharpe_min=1.0,
            scenarios=[
                {"name": "base", "overrides": {}},
                {"name": "high", "overrides": {}},
            ],
        )

        # Make ``base`` succeed (sharpe=2.0) and ``high`` raise from the engine.
        def side_effect(child_config: Any, **kwargs: Any) -> MagicMock:
            results_dir = Path(kwargs["results_dir"])
            parts = results_dir.parts
            scenario = parts[parts.index("scenarios") + 1] if "scenarios" in parts else ""
            if scenario == "high":
                raise RuntimeError("simulated high-scenario fold failure")
            results_dir.mkdir(parents=True, exist_ok=True)
            (results_dir / "performance.json").write_text(
                json.dumps(
                    {
                        "total_return": 0.1,
                        "cagr": 0.1,
                        "sharpe_ratio": 2.0,
                        "max_drawdown": 0.05,
                        "volatility": 0.1,
                        "num_trades": 100,
                    }
                )
            )
            return _make_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        # Outcome should be a non-Pass (the engine failure surfaces the
        # ``high`` scenario as Invalid; aggregated top-level reflects that).
        assert result.exit_code in (1, 3), result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] in ("Fail", "Invalid")
        assert "cost_scenario_failed:high" in summary["reason_codes"]
        # The ``base`` scenario should have been recorded as Pass before
        # the abort.
        outcomes = {s["name"]: s["decision"] for s in summary["cost_scenarios"]}
        assert outcomes["base"] == "Pass"
        assert outcomes["high"] != "Pass"


# ---------------------------------------------------------------------------
# B1 — Helper unit test for severity ordering
# ---------------------------------------------------------------------------


class TestAggregateScenarioOutcomes:
    def test_empty_returns_pass(self) -> None:
        from qs_trader.validation.cli import _aggregate_scenario_outcomes

        assert _aggregate_scenario_outcomes([]) == "Pass"

    def test_fail_beats_review_required(self) -> None:
        from qs_trader.validation.cli import _aggregate_scenario_outcomes

        assert _aggregate_scenario_outcomes(["Pass", "ReviewRequired", "Fail"]) == "Fail"

    def test_review_required_beats_invalid(self) -> None:
        from qs_trader.validation.cli import _aggregate_scenario_outcomes

        assert _aggregate_scenario_outcomes(["Invalid", "ReviewRequired", "Pass"]) == "ReviewRequired"

    def test_invalid_beats_pass(self) -> None:
        from qs_trader.validation.cli import _aggregate_scenario_outcomes

        assert _aggregate_scenario_outcomes(["Pass", "Invalid", "Pass"]) == "Invalid"

    def test_all_pass(self) -> None:
        from qs_trader.validation.cli import _aggregate_scenario_outcomes

        assert _aggregate_scenario_outcomes(["Pass", "Pass"]) == "Pass"


# ---------------------------------------------------------------------------
# I5 — Lone-base scenario suppresses redundant cost_scenario_failed:base
# ---------------------------------------------------------------------------


class TestLoneBaseScenarioSuppressesRedundantPrefix:
    """When the plan declares exactly one scenario named ``base`` the
    ``cost_scenario_failed:base`` marker is suppressed because the per-fold
    reason codes already convey the full story (Phase 2A.2 INFO I5)."""

    def test_lone_base_failing_run_omits_cost_scenario_failed_base(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml, out_dir = _write_cost_scenario_plan(
            tmp_path,
            validation_id="i5_lone_base_fail",
            oos_sharpe_min=1.0,
            scenarios=[{"name": "base", "overrides": {}}],
        )
        # base sharpe well below threshold → Fail.
        side_effect = _make_perf_writer({"base": 0.1})

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code == 1, result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] == "Fail"
        # Redundant prefix must be suppressed.
        assert "cost_scenario_failed:base" not in summary["reason_codes"]
        # Underlying per-fold reason codes from the decision engine must
        # still propagate (the user needs *some* signal explaining the Fail).
        assert summary["reason_codes"], "reason_codes must not be empty on a Fail"
        # Per-scenario block still records the base outcome.
        outcomes = {s["name"]: s["decision"] for s in summary["cost_scenarios"]}
        assert outcomes == {"base": "Fail"}

    def test_multi_scenario_base_failure_still_emits_prefix(self, tmp_path: Path) -> None:
        """When the user declares multiple scenarios — even if one is ``base``
        — the per-scenario prefix is the user's explicit signal that they
        want per-scenario tracking and must be emitted for every failing
        scenario including ``base``."""
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml, out_dir = _write_cost_scenario_plan(
            tmp_path,
            validation_id="i5_multi_base_fail",
            oos_sharpe_min=1.0,
            scenarios=[
                {"name": "base", "overrides": {}},
                {"name": "high", "overrides": {"replay_speed": 0.0}},
            ],
        )
        # base fails, high passes.
        side_effect = _make_perf_writer({"base": 0.1, "high": 2.0})

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code == 1, result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] == "Fail"
        assert "cost_scenario_failed:base" in summary["reason_codes"]
        assert "cost_scenario_failed:high" not in summary["reason_codes"]

    def test_multi_scenario_only_high_fails_omits_base_prefix(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml, out_dir = _write_cost_scenario_plan(
            tmp_path,
            validation_id="i5_multi_high_fail",
            oos_sharpe_min=1.0,
            scenarios=[
                {"name": "base", "overrides": {}},
                {"name": "high", "overrides": {"replay_speed": 0.0}},
            ],
        )
        side_effect = _make_perf_writer({"base": 2.0, "high": 0.1})

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code == 1, result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] == "Fail"
        assert "cost_scenario_failed:high" in summary["reason_codes"]
        assert "cost_scenario_failed:base" not in summary["reason_codes"]


# ---------------------------------------------------------------------------
# I6 — Unreached scenarios under fail_fast do not contaminate aggregation
# ---------------------------------------------------------------------------


def _make_per_scenario_sharpe_or_fail(
    scenario_sharpes: dict[str, float],
    failing_scenarios: set[str],
) -> Any:
    """Side effect: for ``failing_scenarios`` raise from the engine; otherwise
    write a ``performance.json`` with the per-scenario sharpe."""

    def _side_effect(child_config: Any, **kwargs: Any) -> MagicMock:
        results_dir = Path(kwargs["results_dir"])
        parts = results_dir.parts
        scenario = parts[parts.index("scenarios") + 1] if "scenarios" in parts else ""
        if scenario in failing_scenarios:
            raise RuntimeError(f"simulated {scenario}-scenario fold failure")
        sharpe = scenario_sharpes.get(scenario, 2.0)
        results_dir.mkdir(parents=True, exist_ok=True)
        (results_dir / "performance.json").write_text(
            json.dumps(
                {
                    "total_return": 0.1,
                    "cagr": 0.1,
                    "sharpe_ratio": sharpe,
                    "max_drawdown": 0.05,
                    "volatility": 0.1,
                    "num_trades": 100,
                }
            )
        )
        return _make_mock_engine()

    return _side_effect


class TestFailFastUnreachedScenariosOmitted:
    """Under ``fail_fast`` the runner aborts at the first failing fold.
    Scenarios that never ran (no ChildRunRef emitted) must not contribute
    to either the aggregated top-level decision or the top-level reason
    codes (Phase 2A.2 INFO I6).  The predicate is ``bool(grouped[name])``."""

    def test_fail_fast_unreached_scenario_does_not_appear_in_reason_codes(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml, out_dir = _write_cost_scenario_plan(
            tmp_path,
            validation_id="i6_failfast",
            on_child_failure="fail_fast",
            oos_sharpe_min=1.0,
            scenarios=[
                {"name": "base", "overrides": {}},
                {"name": "bad1", "overrides": {}},
                {"name": "bad2", "overrides": {}},
            ],
        )
        # base passes (sharpe 2.0).  bad1 raises → fail_fast aborts before
        # bad2 ever runs.  bad2 must NOT appear in reason codes.
        side_effect = _make_per_scenario_sharpe_or_fail(
            scenario_sharpes={"base": 2.0},
            failing_scenarios={"bad1"},
        )
        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code in (1, 3), result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] in ("Fail", "Invalid")
        # bad1 ran and failed → prefix emitted.
        assert "cost_scenario_failed:bad1" in summary["reason_codes"]
        # bad2 never ran → must NOT appear anywhere in top-level codes.
        assert "cost_scenario_failed:bad2" not in summary["reason_codes"]
        assert not any("bad2" in rc for rc in summary["reason_codes"])
        # Nor should any missing_metric:* code from bad2 contaminate the
        # top-level codes (bad2 produced no metrics, but it never ran so
        # the decision engine was never invoked for it).
        bad2_names = {s["name"] for s in summary["cost_scenarios"]}
        assert "bad2" not in bad2_names, "Unreached scenarios must be omitted from the cost_scenarios block"
        # base + bad1 should appear.
        assert bad2_names == {"base", "bad1"}

    def test_continue_runs_all_scenarios_and_emits_each_failure(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml, out_dir = _write_cost_scenario_plan(
            tmp_path,
            validation_id="i6_continue",
            on_child_failure="continue",
            oos_sharpe_min=1.0,
            scenarios=[
                {"name": "base", "overrides": {}},
                {"name": "bad1", "overrides": {}},
                {"name": "bad2", "overrides": {}},
            ],
        )
        # base passes; bad1 and bad2 both fail (engine raises for both).
        side_effect = _make_per_scenario_sharpe_or_fail(
            scenario_sharpes={"base": 2.0},
            failing_scenarios={"bad1", "bad2"},
        )
        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = side_effect
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code in (1, 3), result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert "cost_scenario_failed:bad1" in summary["reason_codes"]
        assert "cost_scenario_failed:bad2" in summary["reason_codes"]
        names = {s["name"] for s in summary["cost_scenarios"]}
        assert names == {"base", "bad1", "bad2"}
