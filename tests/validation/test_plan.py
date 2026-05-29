"""Unit tests for qs_trader.validation.plan module."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from qs_trader.validation.plan import (
    KNOWN_REVIEW_RULES,
    BenchmarkRef,
    DateRange,
    DecisionRulesSpec,
    ExecutionSpec,
    HoldoutSpec,
    MetricsCatalog,
    OnReviewRequiredRule,
    ReportingSpec,
    StaticSplitSpec,
    ValidationPlan,
    compute_plan_sha256,
    load_validation_plan,
)

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_plan_dict(**overrides: Any) -> dict[str, Any]:
    """Return a minimal valid raw dict for constructing a ValidationPlan."""
    base: dict[str, Any] = {
        "validation_id": "test_plan",
        "strategy_experiment": "test_strategy",
        "base_config": FIXTURES_DIR / "base_config.yaml",
        "mode": "static_is_oos",
        "splits": {
            "in_sample": {"start_date": date(2018, 1, 2), "end_date": date(2021, 12, 31)},
            "out_of_sample": {"start_date": date(2022, 1, 3), "end_date": date(2024, 12, 31)},
        },
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# TestDateRange
# ---------------------------------------------------------------------------


class TestDateRange:
    """Tests for the DateRange model."""

    def test_valid_range(self) -> None:
        """Accept a valid date range where end is strictly after start."""
        dr = DateRange(start_date=date(2020, 1, 1), end_date=date(2020, 12, 31))
        assert dr.start_date == date(2020, 1, 1)
        assert dr.end_date == date(2020, 12, 31)

    def test_end_before_start_rejected(self) -> None:
        """Reject when end_date is before start_date."""
        with pytest.raises(ValidationError, match="strictly after"):
            DateRange(start_date=date(2021, 1, 1), end_date=date(2020, 12, 31))

    def test_end_equal_to_start_rejected(self) -> None:
        """Reject when end_date equals start_date."""
        with pytest.raises(ValidationError, match="strictly after"):
            DateRange(start_date=date(2020, 6, 1), end_date=date(2020, 6, 1))

    def test_iso_string_coercion(self) -> None:
        """Accept ISO date strings via Pydantic coercion."""
        dr = DateRange(start_date="2020-01-01", end_date="2020-12-31")  # type: ignore[arg-type]
        assert dr.start_date == date(2020, 1, 1)
        assert dr.end_date == date(2020, 12, 31)

    def test_minimum_one_day_range(self) -> None:
        """A range of exactly one day (end = start + 1) is valid."""
        dr = DateRange(start_date=date(2020, 1, 1), end_date=date(2020, 1, 2))
        assert dr.end_date > dr.start_date


# ---------------------------------------------------------------------------
# TestStaticSplitSpec
# ---------------------------------------------------------------------------


class TestStaticSplitSpec:
    """Tests for the StaticSplitSpec model."""

    def test_valid_non_overlapping_spec(self) -> None:
        """Accept non-overlapping IS/OOS ranges."""
        spec = StaticSplitSpec(
            in_sample=DateRange(start_date=date(2018, 1, 1), end_date=date(2021, 12, 31)),
            out_of_sample=DateRange(start_date=date(2022, 1, 1), end_date=date(2024, 12, 31)),
        )
        assert spec.in_sample.start_date == date(2018, 1, 1)
        assert spec.out_of_sample.end_date == date(2024, 12, 31)

    def test_oos_starts_before_is_ends_rejected(self) -> None:
        """Reject when OOS start_date is before IS end_date (overlap)."""
        with pytest.raises(ValidationError, match="strictly after"):
            StaticSplitSpec(
                in_sample=DateRange(start_date=date(2018, 1, 1), end_date=date(2022, 6, 30)),
                out_of_sample=DateRange(start_date=date(2022, 1, 1), end_date=date(2024, 12, 31)),
            )

    def test_oos_starts_on_is_end_date_rejected(self) -> None:
        """Reject touching boundaries (OOS start == IS end)."""
        with pytest.raises(ValidationError, match="strictly after"):
            StaticSplitSpec(
                in_sample=DateRange(start_date=date(2018, 1, 1), end_date=date(2021, 12, 31)),
                out_of_sample=DateRange(start_date=date(2021, 12, 31), end_date=date(2024, 12, 31)),
            )

    def test_oos_starts_one_day_after_is_ends_accepted(self) -> None:
        """Accept when OOS start is exactly one day after IS end."""
        spec = StaticSplitSpec(
            in_sample=DateRange(start_date=date(2018, 1, 1), end_date=date(2021, 12, 31)),
            out_of_sample=DateRange(start_date=date(2022, 1, 1), end_date=date(2024, 12, 31)),
        )
        assert spec is not None


# ---------------------------------------------------------------------------
# TestValidationPlan
# ---------------------------------------------------------------------------


class TestValidationPlan:
    """Tests for the ValidationPlan root model."""

    def test_minimal_valid_plan(self) -> None:
        """Create a minimal valid ValidationPlan with required fields only."""
        plan = ValidationPlan(**_minimal_plan_dict())
        assert plan.validation_id == "test_plan"
        assert plan.mode == "static_is_oos"
        assert plan.strategy_experiment == "test_strategy"

    def test_defaults_applied(self) -> None:
        """Optional fields get expected default values."""
        plan = ValidationPlan(**_minimal_plan_dict())
        assert plan.execution.on_child_failure == "fail_fast"
        assert plan.reporting.html is True
        assert plan.reporting.console_summary is True
        assert plan.holdout is None
        assert plan.benchmark is None
        assert plan.decision.oos_sharpe_min is None
        assert plan.decision.on_review_required == ()
        assert "total_return" in plan.metrics.required

    def test_frozen_prevents_attribute_assignment(self) -> None:
        """Frozen model raises on direct attribute assignment."""
        plan = ValidationPlan(**_minimal_plan_dict())
        with pytest.raises((TypeError, ValidationError)):
            plan.validation_id = "mutated"

    def test_unknown_on_review_required_rule_rejected(self) -> None:
        """Reject unknown rule names in decision.on_review_required."""
        d = _minimal_plan_dict(
            decision=DecisionRulesSpec(
                on_review_required=(OnReviewRequiredRule(rule="nonexistent_rule", threshold=0.5),)
            )
        )
        with pytest.raises(ValidationError, match="Unknown rule"):
            ValidationPlan(**d)

    def test_known_on_review_required_rules_accepted(self) -> None:
        """Every known review rule name is accepted in decision.on_review_required."""
        for rule_name in sorted(KNOWN_REVIEW_RULES):
            d = _minimal_plan_dict(
                decision=DecisionRulesSpec(on_review_required=(OnReviewRequiredRule(rule=rule_name, threshold=0.5),))
            )
            plan = ValidationPlan(**d)
            assert plan.decision.on_review_required[0].rule == rule_name

    def test_unknown_mode_rejected(self) -> None:
        """Reject mode values that are not in the supported Literal set."""
        d = _minimal_plan_dict(mode="bad_mode")
        with pytest.raises(ValidationError):
            ValidationPlan(**d)

    def test_with_holdout_and_benchmark(self) -> None:
        """Create a plan with optional holdout and benchmark fields."""
        d = _minimal_plan_dict(
            holdout={"start_date": date(2025, 1, 2), "end_date": date(2025, 12, 31)},
            benchmark={"instrument": "SPY", "strategy": "buy_and_hold", "reinvest_dividends": True},
        )
        plan = ValidationPlan(**d)
        assert plan.holdout is not None
        assert plan.holdout.start_date == date(2025, 1, 2)
        assert plan.benchmark is not None
        assert plan.benchmark.instrument == "SPY"
        assert plan.benchmark.strategy == "buy_and_hold"
        assert plan.benchmark.reinvest_dividends is True

    def test_with_decision_thresholds(self) -> None:
        """Decision rules are stored correctly when provided."""
        d = _minimal_plan_dict(
            decision={
                "oos_sharpe_min": 0.8,
                "oos_max_drawdown_max": 0.25,
                "min_oos_trades": 30,
            }
        )
        plan = ValidationPlan(**d)
        assert plan.decision.oos_sharpe_min == 0.8
        assert plan.decision.oos_max_drawdown_max == 0.25
        assert plan.decision.min_oos_trades == 30
        assert plan.decision.is_to_oos_sharpe_decay_max is None

    def test_execution_continue_on_failure(self) -> None:
        """Execution.on_child_failure can be set to 'continue'."""
        d = _minimal_plan_dict(execution={"on_child_failure": "continue"})
        plan = ValidationPlan(**d)
        assert plan.execution.on_child_failure == "continue"

    def test_reporting_disabled(self) -> None:
        """Reporting fields can be disabled."""
        d = _minimal_plan_dict(reporting={"html": False, "console_summary": False})
        plan = ValidationPlan(**d)
        assert plan.reporting.html is False
        assert plan.reporting.console_summary is False


# ---------------------------------------------------------------------------
# TestLoadValidationPlan
# ---------------------------------------------------------------------------


class TestLoadValidationPlan:
    """Tests for the load_validation_plan loader function."""

    def test_load_from_file(self) -> None:
        """Load a ValidationPlan from a YAML file path."""
        plan = load_validation_plan(FIXTURES_DIR / "plan.yaml")
        assert plan.validation_id == "test_plan"
        assert plan.strategy_experiment == "test_strategy"
        assert plan.mode == "static_is_oos"

    def test_base_config_resolved_to_absolute(self) -> None:
        """The base_config field is resolved to an absolute path."""
        plan = load_validation_plan(FIXTURES_DIR / "plan.yaml")
        assert plan.base_config.is_absolute()
        assert plan.base_config.exists()

    def test_base_config_points_to_fixture(self) -> None:
        """The resolved base_config path points to the expected fixture."""
        plan = load_validation_plan(FIXTURES_DIR / "plan.yaml")
        assert plan.base_config == (FIXTURES_DIR / "base_config.yaml").resolve()

    def test_load_from_directory(self, tmp_path: Path) -> None:
        """Load a plan from a directory that contains <dir_name>.yaml."""
        plan_dir = tmp_path / "my_plan"
        plan_dir.mkdir()
        src_plan = FIXTURES_DIR / "plan.yaml"
        src_cfg = FIXTURES_DIR / "base_config.yaml"
        (plan_dir / "my_plan.yaml").write_text(src_plan.read_text())
        (plan_dir / "base_config.yaml").write_bytes(src_cfg.read_bytes())

        plan = load_validation_plan(plan_dir)
        assert plan.validation_id == "test_plan"
        assert plan.base_config.is_absolute()

    def test_directory_missing_canonical_yaml_raises(self, tmp_path: Path) -> None:
        """Raise ValueError when a directory lacks the canonical YAML file."""
        plan_dir = tmp_path / "my_plan"
        plan_dir.mkdir()
        with pytest.raises(ValueError, match="must contain"):
            load_validation_plan(plan_dir)

    def test_file_not_found_raises_value_error(self) -> None:
        """Raise ValueError for a missing plan file."""
        with pytest.raises(ValueError, match="not found"):
            load_validation_plan(Path("/nonexistent/path/plan.yaml"))

    def test_invalid_yaml_raises_value_error(self, tmp_path: Path) -> None:
        """Raise ValueError for a YAML file with a syntax error."""
        bad_yaml = tmp_path / "plan.yaml"
        bad_yaml.write_text("key: [unclosed bracket\n")
        with pytest.raises(ValueError, match="Invalid YAML"):
            load_validation_plan(bad_yaml)

    def test_missing_required_field_raises_value_error(self, tmp_path: Path) -> None:
        """Raise ValueError when required fields are absent."""
        plan_yaml = tmp_path / "plan.yaml"
        plan_yaml.write_text("validation_id: x\n")
        with pytest.raises(ValueError, match="validation failed"):
            load_validation_plan(plan_yaml)

    def test_normalizes_decision_rules_block(self, tmp_path: Path) -> None:
        """Loader flattens decision.rules sub-key and normalizes on_review_required."""
        cfg_path = FIXTURES_DIR / "base_config.yaml"
        plan_data: dict[str, Any] = {
            "validation_id": "rule_test",
            "strategy_experiment": "test",
            "base_config": str(cfg_path),
            "mode": "static_is_oos",
            "splits": {
                "in_sample": {"start_date": "2018-01-02", "end_date": "2021-12-31"},
                "out_of_sample": {"start_date": "2022-01-03", "end_date": "2024-12-31"},
            },
            "decision": {
                "rules": {
                    "oos_sharpe_min": 0.8,
                    "min_oos_trades": 30,
                },
                "on_review_required": [
                    {"is_to_oos_sharpe_decay_warn": 0.5},
                ],
            },
        }
        plan_yaml = tmp_path / "plan.yaml"
        with plan_yaml.open("w") as f:
            yaml.dump(plan_data, f)

        plan = load_validation_plan(plan_yaml)
        assert plan.decision.oos_sharpe_min == 0.8
        assert plan.decision.min_oos_trades == 30
        assert len(plan.decision.on_review_required) == 1
        assert plan.decision.on_review_required[0].rule == "is_to_oos_sharpe_decay_warn"
        assert plan.decision.on_review_required[0].threshold == 0.5

    def test_absolute_base_config_path_accepted(self, tmp_path: Path) -> None:
        """An absolute base_config path in YAML is used as-is."""
        cfg_path = (FIXTURES_DIR / "base_config.yaml").resolve()
        plan_data: dict[str, Any] = {
            "validation_id": "abs_test",
            "strategy_experiment": "test",
            "base_config": str(cfg_path),
            "mode": "static_is_oos",
            "splits": {
                "in_sample": {"start_date": "2018-01-02", "end_date": "2021-12-31"},
                "out_of_sample": {"start_date": "2022-01-03", "end_date": "2024-12-31"},
            },
        }
        plan_yaml = tmp_path / "plan.yaml"
        with plan_yaml.open("w") as f:
            yaml.dump(plan_data, f)

        plan = load_validation_plan(plan_yaml)
        assert plan.base_config == cfg_path


# ---------------------------------------------------------------------------
# TestComputePlanSha256
# ---------------------------------------------------------------------------


class TestComputePlanSha256:
    """Tests for the compute_plan_sha256 function."""

    def test_returns_64_char_hex_string(self) -> None:
        """Result is a 64-character lowercase hex string."""
        plan = load_validation_plan(FIXTURES_DIR / "plan.yaml")
        sha = compute_plan_sha256(plan, plan.base_config)
        assert isinstance(sha, str)
        assert len(sha) == 64
        assert all(c in "0123456789abcdef" for c in sha)

    def test_deterministic_same_inputs(self) -> None:
        """Two calls with identical inputs return the same hash."""
        plan = load_validation_plan(FIXTURES_DIR / "plan.yaml")
        sha1 = compute_plan_sha256(plan, plan.base_config)
        sha2 = compute_plan_sha256(plan, plan.base_config)
        assert sha1 == sha2

    def test_different_validation_ids_produce_different_hashes(self, tmp_path: Path) -> None:
        """Different validation_id values produce different hashes."""
        cfg_path = FIXTURES_DIR / "base_config.yaml"

        def _write_plan(vid: str) -> Path:
            d: dict[str, Any] = {
                "validation_id": vid,
                "strategy_experiment": "test",
                "base_config": str(cfg_path),
                "mode": "static_is_oos",
                "splits": {
                    "in_sample": {"start_date": "2018-01-02", "end_date": "2021-12-31"},
                    "out_of_sample": {"start_date": "2022-01-03", "end_date": "2024-12-31"},
                },
            }
            p = tmp_path / f"{vid}.yaml"
            with p.open("w") as f:
                yaml.dump(d, f)
            return p

        plan_a = load_validation_plan(_write_plan("plan_a"))
        plan_b = load_validation_plan(_write_plan("plan_b"))

        sha_a = compute_plan_sha256(plan_a, plan_a.base_config)
        sha_b = compute_plan_sha256(plan_b, plan_b.base_config)
        assert sha_a != sha_b

    def test_different_split_dates_produce_different_hashes(self, tmp_path: Path) -> None:
        """Different split date configurations produce different hashes."""
        cfg_path = FIXTURES_DIR / "base_config.yaml"

        def _write_plan(oos_start: str, oos_end: str) -> Path:
            d: dict[str, Any] = {
                "validation_id": "same_id",
                "strategy_experiment": "test",
                "base_config": str(cfg_path),
                "mode": "static_is_oos",
                "splits": {
                    "in_sample": {"start_date": "2018-01-02", "end_date": "2021-12-31"},
                    "out_of_sample": {"start_date": oos_start, "end_date": oos_end},
                },
            }
            p = tmp_path / f"plan_{oos_start}.yaml"
            with p.open("w") as f:
                yaml.dump(d, f)
            return p

        plan_a = load_validation_plan(_write_plan("2022-01-03", "2024-12-31"))
        plan_b = load_validation_plan(_write_plan("2022-01-03", "2023-06-30"))

        sha_a = compute_plan_sha256(plan_a, plan_a.base_config)
        sha_b = compute_plan_sha256(plan_b, plan_b.base_config)
        assert sha_a != sha_b

    def test_hash_changes_when_base_config_changes(self, tmp_path: Path) -> None:
        """W2: Different base configs (different initial_equity) produce different hashes."""
        base_a = tmp_path / "base_a.yaml"
        base_b = tmp_path / "base_b.yaml"

        base_template = (FIXTURES_DIR / "base_config.yaml").read_text()
        base_a.write_text(base_template)
        base_b.write_text(base_template.replace("initial_equity: 100000", "initial_equity: 200000"))

        def _write_plan(base_cfg: Path) -> Path:
            d: dict[str, Any] = {
                "validation_id": "hash_test",
                "strategy_experiment": "test",
                "base_config": str(base_cfg),
                "mode": "static_is_oos",
                "splits": {
                    "in_sample": {"start_date": "2018-01-02", "end_date": "2021-12-31"},
                    "out_of_sample": {"start_date": "2022-01-03", "end_date": "2024-12-31"},
                },
            }
            p = tmp_path / f"plan_{base_cfg.stem}.yaml"
            with p.open("w") as f:
                yaml.dump(d, f)
            return p

        plan_a = load_validation_plan(_write_plan(base_a))
        plan_b = load_validation_plan(_write_plan(base_b))

        sha_a = compute_plan_sha256(plan_a, plan_a.base_config)
        sha_b = compute_plan_sha256(plan_b, plan_b.base_config)
        assert sha_a != sha_b

    def test_hash_stable_across_equivalent_yaml_reformulations(self, tmp_path: Path) -> None:
        """W3: Same plan in two YAML files with different key ordering produces identical hash."""
        cfg_path = FIXTURES_DIR / "base_config.yaml"

        # Write plan with keys in one order
        plan_yaml_a = tmp_path / "plan_order_a.yaml"
        plan_yaml_a.write_text(
            f"validation_id: stable_test\n"
            f"strategy_experiment: test_strategy\n"
            f"base_config: {cfg_path}\n"
            f"mode: static_is_oos\n"
            f"splits:\n"
            f"  in_sample:\n"
            f"    start_date: '2018-01-02'\n"
            f"    end_date: '2021-12-31'\n"
            f"  out_of_sample:\n"
            f"    start_date: '2022-01-03'\n"
            f"    end_date: '2024-12-31'\n"
        )

        # Write plan with keys in reversed / different order and extra whitespace
        plan_yaml_b = tmp_path / "plan_order_b.yaml"
        plan_yaml_b.write_text(
            f"mode: static_is_oos\n"
            f"strategy_experiment: test_strategy\n"
            f"validation_id: stable_test\n"
            f"base_config: {cfg_path}\n"
            f"splits:\n"
            f"  out_of_sample:\n"
            f"    end_date: '2024-12-31'\n"
            f"    start_date: '2022-01-03'\n"
            f"  in_sample:\n"
            f"    end_date: '2021-12-31'\n"
            f"    start_date: '2018-01-02'\n"
        )

        plan_a = load_validation_plan(plan_yaml_a)
        plan_b = load_validation_plan(plan_yaml_b)

        sha_a = compute_plan_sha256(plan_a, plan_a.base_config)
        sha_b = compute_plan_sha256(plan_b, plan_b.base_config)
        assert sha_a == sha_b


# ---------------------------------------------------------------------------
# TestHoldoutSpec
# ---------------------------------------------------------------------------


class TestHoldoutSpec:
    """Tests for the HoldoutSpec model (I1: date-order validation)."""

    def test_valid_holdout(self) -> None:
        """Accept a valid holdout where end is strictly after start."""
        h = HoldoutSpec(start_date=date(2025, 1, 1), end_date=date(2025, 12, 31))
        assert h.start_date == date(2025, 1, 1)

    def test_end_before_start_rejected(self) -> None:
        """I1: Reject holdout when end_date is before start_date."""
        with pytest.raises(ValidationError, match="strictly after"):
            HoldoutSpec(start_date=date(2025, 12, 31), end_date=date(2025, 1, 1))

    def test_end_equal_to_start_rejected(self) -> None:
        """I1: Reject holdout when end_date equals start_date."""
        with pytest.raises(ValidationError, match="strictly after"):
            HoldoutSpec(start_date=date(2025, 6, 1), end_date=date(2025, 6, 1))


# ---------------------------------------------------------------------------
# TestFrozenNestedModels
# ---------------------------------------------------------------------------


class TestFrozenNestedModels:
    """W1: Nested models must be immutable after construction."""

    def test_date_range_frozen(self) -> None:
        """DateRange raises on attribute assignment after construction."""
        dr = DateRange(start_date=date(2020, 1, 1), end_date=date(2020, 12, 31))
        with pytest.raises((TypeError, ValidationError)):
            dr.start_date = date(2021, 1, 1)

    def test_static_split_spec_nested_date_range_frozen(self) -> None:
        """StaticSplitSpec.in_sample raises on attribute assignment."""
        spec = StaticSplitSpec(
            in_sample=DateRange(start_date=date(2018, 1, 1), end_date=date(2021, 12, 31)),
            out_of_sample=DateRange(start_date=date(2022, 1, 1), end_date=date(2024, 12, 31)),
        )
        with pytest.raises((TypeError, ValidationError)):
            spec.in_sample.start_date = date(2019, 1, 1)

    def test_plan_nested_splits_frozen(self) -> None:
        """plan.splits raises on attribute assignment (nested mutation blocked)."""
        plan = ValidationPlan(**_minimal_plan_dict())
        assert isinstance(plan.splits, StaticSplitSpec)
        with pytest.raises((TypeError, ValidationError)):
            plan.splits.in_sample.start_date = date(2019, 1, 1)

    def test_holdout_spec_frozen(self) -> None:
        """HoldoutSpec raises on attribute assignment after construction."""
        h = HoldoutSpec(start_date=date(2025, 1, 1), end_date=date(2025, 12, 31))
        with pytest.raises((TypeError, ValidationError)):
            h.start_date = date(2026, 1, 1)

    def test_benchmark_ref_frozen(self) -> None:
        """BenchmarkSpec raises on attribute assignment after construction.

        ``BenchmarkRef`` is retained as a backward-compatible alias for the
        Phase 2A.3 ``BenchmarkSpec`` model (instrument / strategy /
        reinvest_dividends).
        """
        b = BenchmarkRef(instrument="SPY")
        with pytest.raises((TypeError, ValidationError)):
            b.instrument = "QQQ"

    def test_decision_rules_spec_frozen(self) -> None:
        """DecisionRulesSpec raises on attribute assignment after construction."""
        d = DecisionRulesSpec(oos_sharpe_min=0.8)
        with pytest.raises((TypeError, ValidationError)):
            d.oos_sharpe_min = 0.5

    def test_metrics_catalog_frozen(self) -> None:
        """MetricsCatalog raises on attribute assignment after construction."""
        m = MetricsCatalog()
        with pytest.raises((TypeError, ValidationError)):
            m.required = []  # type: ignore[assignment]

    def test_execution_spec_frozen(self) -> None:
        """ExecutionSpec raises on attribute assignment after construction."""
        e = ExecutionSpec()
        with pytest.raises((TypeError, ValidationError)):
            e.on_child_failure = "continue"

    def test_reporting_spec_frozen(self) -> None:
        """ReportingSpec raises on attribute assignment after construction."""
        r = ReportingSpec()
        with pytest.raises((TypeError, ValidationError)):
            r.html = False


# ---------------------------------------------------------------------------
# TestDecisionRulesSchemaStrict
# ---------------------------------------------------------------------------


class TestDecisionRulesSchemaStrict:
    """B1: Decision rules schema rejects unknown keys and enforces rule catalogs."""

    def test_unknown_rules_key_rejected(self, tmp_path: Path) -> None:
        """Unknown key under decision.rules raises ValueError via Pydantic extra='forbid'."""
        cfg_path = (FIXTURES_DIR / "base_config.yaml").resolve()
        plan_data: dict[str, Any] = {
            "validation_id": "unknown_key_test",
            "strategy_experiment": "test",
            "base_config": str(cfg_path),
            "mode": "static_is_oos",
            "splits": {
                "in_sample": {"start_date": "2018-01-02", "end_date": "2021-12-31"},
                "out_of_sample": {"start_date": "2022-01-03", "end_date": "2024-12-31"},
            },
            "decision": {
                "rules": {
                    "oos_sharpe_typo": 0.8,
                },
            },
        }
        plan_yaml = tmp_path / "plan.yaml"
        with plan_yaml.open("w") as f:
            yaml.dump(plan_data, f)
        with pytest.raises(ValueError, match="validation failed"):
            load_validation_plan(plan_yaml)

    def test_fail_rule_in_on_review_required_rejected(self) -> None:
        """A pass/fail rule name in on_review_required is rejected (wrong catalog)."""
        d = _minimal_plan_dict(
            decision=DecisionRulesSpec(on_review_required=(OnReviewRequiredRule(rule="oos_sharpe_min", threshold=0.5),))
        )
        with pytest.raises(ValidationError, match="Unknown rule"):
            ValidationPlan(**d)

    def test_review_rule_accepted_in_on_review_required(self) -> None:
        """is_to_oos_sharpe_decay_warn is accepted in on_review_required."""
        d = _minimal_plan_dict(
            decision=DecisionRulesSpec(
                on_review_required=(OnReviewRequiredRule(rule="is_to_oos_sharpe_decay_warn", threshold=0.3),)
            )
        )
        plan = ValidationPlan(**d)
        assert plan.decision.on_review_required[0].rule == "is_to_oos_sharpe_decay_warn"

    def test_malformed_decision_rules_rejected(self, tmp_path: Path) -> None:
        """decision.rules as a non-mapping (e.g. a list) raises ValueError before model construction."""
        cfg_path = (FIXTURES_DIR / "base_config.yaml").resolve()
        plan_data: dict[str, Any] = {
            "validation_id": "malformed_rules_test",
            "strategy_experiment": "test",
            "base_config": str(cfg_path),
            "mode": "static_is_oos",
            "splits": {
                "in_sample": {"start_date": "2018-01-02", "end_date": "2021-12-31"},
                "out_of_sample": {"start_date": "2022-01-03", "end_date": "2024-12-31"},
            },
            "decision": {
                "rules": ["oos_sharpe_min"],
            },
        }
        plan_yaml = tmp_path / "plan.yaml"
        with plan_yaml.open("w") as f:
            yaml.dump(plan_data, f)
        with pytest.raises(ValueError, match="decision.rules must be a mapping"):
            load_validation_plan(plan_yaml)


# ---------------------------------------------------------------------------
# TestDeepImmutability
# ---------------------------------------------------------------------------


class TestDeepImmutability:
    """B3: Tuple fields in frozen models prevent in-place mutation."""

    def test_metrics_required_is_tuple(self) -> None:
        """MetricsCatalog.required is a tuple, not a list."""
        m = MetricsCatalog()
        assert isinstance(m.required, tuple)

    def test_metrics_recommended_is_tuple(self) -> None:
        """MetricsCatalog.recommended is a tuple, not a list."""
        m = MetricsCatalog()
        assert isinstance(m.recommended, tuple)

    def test_on_review_required_is_tuple(self) -> None:
        """DecisionRulesSpec.on_review_required is a tuple, not a list."""
        d = DecisionRulesSpec()
        assert isinstance(d.on_review_required, tuple)

    def test_metrics_required_append_raises(self) -> None:
        """Tuple field .required cannot be appended to (AttributeError)."""
        m = MetricsCatalog()
        with pytest.raises(AttributeError):
            m.required.append("extra_metric")  # type: ignore[attr-defined]

    def test_on_review_required_append_raises(self) -> None:
        """Tuple field .on_review_required cannot be appended to (AttributeError)."""
        d = DecisionRulesSpec()
        with pytest.raises(AttributeError):
            d.on_review_required.append(  # type: ignore[attr-defined]
                OnReviewRequiredRule(rule="is_to_oos_sharpe_decay_warn", threshold=0.3)
            )
