"""Tests for Phase 2A.1 — Walk-forward split generator.

Covers T1.1–T1.9: parse_duration, WalkForwardSplitsSpec, WalkForwardSplitGenerator
(anchored + rolling, embargo, min_fold_bars), plan-hash extension, CLI dry-run,
and the synthetic fixture.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from qs_trader.validation.cli import validate_command
from qs_trader.validation.plan import (
    DateRange,
    StaticSplitSpec,
    ValidationPlan,
    WalkForwardSplitsSpec,
    compute_plan_sha256,
    load_validation_plan,
    parse_duration,
)
from qs_trader.validation.splits import WalkForwardSplitGenerator, get_split_generator
from qs_trader.validation.splits.base import ValidationSplit
from qs_trader.validation.splits.static import StaticSplitGenerator

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_wf_plan(**overrides: Any) -> ValidationPlan:
    """Minimal valid walk_forward ValidationPlan."""
    base: dict[str, Any] = {
        "validation_id": "wf_test",
        "strategy_experiment": "test_strategy",
        "base_config": FIXTURES_DIR / "base_config.yaml",
        "mode": "walk_forward",
        "splits": {
            "style": "rolling",
            "train": "2y",
            "test": "1y",
            "step": "1y",
            "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
        },
    }
    base.update(overrides)
    return ValidationPlan(**base)


def _make_static_plan(**overrides: Any) -> ValidationPlan:
    """Minimal valid static_is_oos ValidationPlan."""
    base: dict[str, Any] = {
        "validation_id": "static_test",
        "strategy_experiment": "test_strategy",
        "base_config": FIXTURES_DIR / "base_config.yaml",
        "mode": "static_is_oos",
        "splits": {
            "in_sample": {"start_date": date(2018, 1, 2), "end_date": date(2021, 12, 31)},
            "out_of_sample": {"start_date": date(2022, 1, 3), "end_date": date(2024, 12, 31)},
        },
    }
    base.update(overrides)
    return ValidationPlan(**base)


# ---------------------------------------------------------------------------
# T1.1 — parse_duration
# ---------------------------------------------------------------------------


class TestParseDuration:
    """T1.1: Duration-string parser."""

    def test_years_returns_relativedelta(self) -> None:
        from dateutil.relativedelta import relativedelta

        result = parse_duration("3y")
        assert isinstance(result, relativedelta)
        assert result == relativedelta(years=3)

    def test_months_returns_relativedelta(self) -> None:
        from dateutil.relativedelta import relativedelta

        result = parse_duration("6mo")
        assert isinstance(result, relativedelta)
        assert result == relativedelta(months=6)

    def test_days_returns_timedelta(self) -> None:
        result = parse_duration("30d")
        assert isinstance(result, timedelta)
        assert result == timedelta(days=30)

    def test_zero_days(self) -> None:
        assert parse_duration("0d") == timedelta(0)

    def test_one_year(self) -> None:
        from dateutil.relativedelta import relativedelta

        assert parse_duration("1y") == relativedelta(years=1)

    def test_one_month(self) -> None:
        from dateutil.relativedelta import relativedelta

        assert parse_duration("1mo") == relativedelta(months=1)

    def test_large_number(self) -> None:
        result = parse_duration("365d")
        assert result == timedelta(days=365)

    def test_invalid_unit_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid duration string"):
            parse_duration("3w")

    def test_invalid_format_no_number_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid duration string"):
            parse_duration("y")

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid duration string"):
            parse_duration("")

    def test_float_not_accepted(self) -> None:
        with pytest.raises(ValueError, match="Invalid duration string"):
            parse_duration("1.5y")

    def test_whitespace_stripped(self) -> None:
        from dateutil.relativedelta import relativedelta

        assert parse_duration(" 2y ") == relativedelta(years=2)

    def test_invalid_suffix_m_not_accepted(self) -> None:
        """'m' alone is not a valid unit; must use 'mo' for months."""
        with pytest.raises(ValueError, match="Invalid duration string"):
            parse_duration("3m")

    def test_combined_units_rejected(self) -> None:
        """Combined-unit strings like '1y2mo' must be rejected by the anchored regex."""
        with pytest.raises(ValueError, match="Invalid duration"):
            parse_duration("1y2mo")

    def test_date_arithmetic_year(self) -> None:
        """relativedelta(years=1) + date gives correct date."""
        dur = parse_duration("1y")
        assert date(2010, 1, 1) + dur == date(2011, 1, 1)

    def test_date_arithmetic_month(self) -> None:
        """relativedelta(months=6) + date gives correct date."""
        dur = parse_duration("6mo")
        assert date(2010, 1, 1) + dur == date(2010, 7, 1)

    def test_date_arithmetic_day(self) -> None:
        """timedelta(days=90) + date gives correct date."""
        dur = parse_duration("90d")
        assert date(2010, 1, 1) + dur == date(2010, 4, 1)


# ---------------------------------------------------------------------------
# T1.2/T1.3 — WalkForwardSplitsSpec validation
# ---------------------------------------------------------------------------


class TestWalkForwardSplitsSpec:
    """Validate WalkForwardSplitsSpec field constraints."""

    def test_valid_spec_constructs(self) -> None:
        spec = WalkForwardSplitsSpec(
            style="rolling",
            train="2y",
            test="1y",
            step="1y",
            total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
        )
        assert spec.style == "rolling"

    def test_anchored_style_accepted(self) -> None:
        spec = WalkForwardSplitsSpec(
            style="anchored",
            train="3y",
            test="1y",
            step="1y",
            total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2018, 12, 31)),
        )
        assert spec.style == "anchored"

    def test_step_less_than_test_rejected(self) -> None:
        with pytest.raises(ValidationError, match="step.*must be >= test"):
            WalkForwardSplitsSpec(
                style="rolling",
                train="2y",
                test="1y",
                step="6mo",  # 6mo < 1y → invalid
                total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
            )

    def test_step_equals_test_accepted(self) -> None:
        spec = WalkForwardSplitsSpec(
            style="rolling",
            train="2y",
            test="1y",
            step="1y",  # equal → valid
            total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
        )
        assert spec.step == "1y"

    def test_step_greater_than_test_accepted(self) -> None:
        spec = WalkForwardSplitsSpec(
            style="rolling",
            train="2y",
            test="6mo",
            step="1y",  # 1y > 6mo → valid
            total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
        )
        assert spec.step == "1y"

    def test_invalid_train_duration_rejected(self) -> None:
        with pytest.raises(ValidationError, match="Invalid duration"):
            WalkForwardSplitsSpec(
                style="rolling",
                train="bad",
                test="1y",
                step="1y",
                total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
            )

    def test_min_fold_bars_optional(self) -> None:
        spec = WalkForwardSplitsSpec(
            style="rolling",
            train="2y",
            test="1y",
            step="1y",
            total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
        )
        assert spec.min_fold_bars is None

    def test_min_fold_bars_set(self) -> None:
        spec = WalkForwardSplitsSpec(
            style="rolling",
            train="2y",
            test="1y",
            step="1y",
            total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
            min_fold_bars=200,
        )
        assert spec.min_fold_bars == 200

    def test_default_embargo_is_zero(self) -> None:
        spec = WalkForwardSplitsSpec(
            style="rolling",
            train="2y",
            test="1y",
            step="1y",
            total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
        )
        assert spec.embargo == "0d"

    # ------------------------------------------------------------------
    # BLOCKER regression: zero/negative duration rejection
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("train", "0d"),
            ("train", "0y"),
            ("train", "0mo"),
            ("test", "0d"),
            ("step", "0d"),
        ],
    )
    def test_zero_duration_for_required_field_rejected(self, field: str, value: str) -> None:
        """train/test/step must be strictly positive. Zero is rejected at load time."""
        kwargs: dict[str, Any] = {
            "style": "rolling",
            "train": "2y",
            "test": "1y",
            "step": "1y",
            "total_range": DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
        }
        kwargs[field] = value
        with pytest.raises(ValidationError, match=f"'{field}' must be a strictly positive duration"):
            WalkForwardSplitsSpec(**kwargs)

    def test_zero_embargo_accepted(self) -> None:
        """embargo may be zero (the documented default)."""
        spec = WalkForwardSplitsSpec(
            style="rolling",
            train="2y",
            test="1y",
            step="1y",
            embargo="0d",
            total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
        )
        assert spec.embargo == "0d"

    @pytest.mark.parametrize("value", [0, -1, -10])
    def test_non_positive_min_fold_bars_rejected(self, value: int) -> None:
        """min_fold_bars must be strictly positive when set; 0 and negatives are rejected."""
        with pytest.raises(ValidationError, match="min_fold_bars must be strictly positive"):
            WalkForwardSplitsSpec(
                style="rolling",
                train="2y",
                test="1y",
                step="1y",
                total_range=DateRange(start_date=date(2010, 1, 1), end_date=date(2016, 12, 31)),
                min_fold_bars=value,
            )


# ---------------------------------------------------------------------------
# T1.7 — Mode/splits cross-contamination rejection
# ---------------------------------------------------------------------------


class TestModeFieldCrossContamination:
    """T1.7: static_is_oos rejects WF fields; walk_forward rejects static fields."""

    def test_static_plan_rejects_wf_fields(self) -> None:
        with pytest.raises((ValidationError, ValueError), match="Walk-forward fields"):
            ValidationPlan(
                validation_id="x",
                strategy_experiment="x",
                base_config=FIXTURES_DIR / "base_config.yaml",
                mode="static_is_oos",
                splits={
                    "in_sample": {"start_date": date(2018, 1, 2), "end_date": date(2021, 12, 31)},
                    "out_of_sample": {"start_date": date(2022, 1, 3), "end_date": date(2024, 12, 31)},
                    "style": "rolling",  # WF field — must be rejected
                },
            )

    def test_wf_plan_rejects_static_fields(self) -> None:
        with pytest.raises((ValidationError, ValueError), match="Static fields"):
            ValidationPlan(
                validation_id="x",
                strategy_experiment="x",
                base_config=FIXTURES_DIR / "base_config.yaml",
                mode="walk_forward",
                splits={
                    "style": "rolling",
                    "train": "2y",
                    "test": "1y",
                    "step": "1y",
                    "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
                    "in_sample": {"start_date": date(2018, 1, 2), "end_date": date(2021, 12, 31)},
                },
            )

    def test_splits_is_static_spec_for_static_plan(self) -> None:
        plan = _make_static_plan()
        assert isinstance(plan.splits, StaticSplitSpec)

    def test_splits_is_wf_spec_for_wf_plan(self) -> None:
        plan = _make_wf_plan()
        assert isinstance(plan.splits, WalkForwardSplitsSpec)


# ---------------------------------------------------------------------------
# T1.7 — get_split_generator dispatch
# ---------------------------------------------------------------------------


class TestGetSplitGenerator:
    """T1.7: Dispatch function returns correct generator."""

    def test_static_plan_returns_static_generator(self) -> None:
        plan = _make_static_plan()
        gen = get_split_generator(plan)
        assert isinstance(gen, StaticSplitGenerator)

    def test_wf_plan_returns_wf_generator(self) -> None:
        plan = _make_wf_plan()
        gen = get_split_generator(plan)
        assert isinstance(gen, WalkForwardSplitGenerator)


# ---------------------------------------------------------------------------
# T1.2 — WalkForwardSplitGenerator anchored mode
# ---------------------------------------------------------------------------


class TestWalkForwardGeneratorAnchored:
    """T1.2: Anchored walk-forward generation."""

    def _make_anchored_plan(self, **overrides: Any) -> ValidationPlan:
        splits: dict[str, Any] = {
            "style": "anchored",
            "train": "2y",
            "test": "1y",
            "step": "1y",
            "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2015, 12, 31)},
        }
        return ValidationPlan(
            validation_id="anc_test",
            strategy_experiment="x",
            base_config=FIXTURES_DIR / "base_config.yaml",
            mode="walk_forward",
            splits={**splits, **overrides},
        )

    def test_generates_multiple_folds(self) -> None:
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos = [s for s in splits if s.role == "oos"]
        assert len(oos) >= 2

    def test_train_start_stays_fixed(self) -> None:
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        train_splits = [s for s in splits if s.role == "train"]
        first_train_start = train_splits[0].test_range.start_date
        for ts in train_splits:
            assert ts.test_range.start_date == first_train_start

    def test_train_end_expands_by_step(self) -> None:
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        train_splits = [s for s in splits if s.role == "train"]
        # Each fold's train end is 1y ahead of the previous
        for i in range(1, len(train_splits)):
            prev_end = train_splits[i - 1].test_range.end_date
            curr_end = train_splits[i].test_range.end_date
            from dateutil.relativedelta import relativedelta

            assert curr_end == prev_end + relativedelta(years=1)

    def test_test_windows_advance_by_step(self) -> None:
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos_splits = [s for s in splits if s.role == "oos"]
        from dateutil.relativedelta import relativedelta

        for i in range(1, len(oos_splits)):
            prev_start = oos_splits[i - 1].test_range.start_date
            curr_start = oos_splits[i].test_range.start_date
            assert curr_start == prev_start + relativedelta(years=1)

    def test_fold_index_is_sequential(self) -> None:
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos_splits = [s for s in splits if s.role == "oos"]
        for i, sp in enumerate(oos_splits):
            assert sp.fold_index == i

    def test_oos_train_range_set(self) -> None:
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos_splits = [s for s in splits if s.role == "oos"]
        for sp in oos_splits:
            assert sp.train_range is not None

    def test_oos_train_range_matches_train_split(self) -> None:
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        train_splits = [s for s in splits if s.role == "train"]
        oos_splits = [s for s in splits if s.role == "oos"]
        for ts, os in zip(train_splits, oos_splits):
            assert os.train_range == ts.test_range

    def test_no_test_beyond_total_range(self) -> None:
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos_splits = [s for s in splits if s.role == "oos"]
        for sp in oos_splits:
            assert sp.test_range.end_date <= plan.splits.total_range.end_date

    def test_fold_pair_structure(self) -> None:
        """Each fold has exactly one train and one oos split."""
        plan = self._make_anchored_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos_indices = {s.fold_index for s in splits if s.role == "oos"}
        train_indices = {s.fold_index for s in splits if s.role == "train"}
        assert oos_indices == train_indices


# ---------------------------------------------------------------------------
# T1.3 — WalkForwardSplitGenerator rolling mode
# ---------------------------------------------------------------------------


class TestWalkForwardGeneratorRolling:
    """T1.3: Rolling walk-forward generation."""

    def test_produces_at_least_3_oos_folds(self) -> None:
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos = [s for s in splits if s.role == "oos"]
        assert len(oos) >= 3

    def test_rolling_5_folds_for_fixture_params(self) -> None:
        """2y train, 1y test, 1y step, 2010-01-01..2016-12-31 → 5 folds."""
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos = [s for s in splits if s.role == "oos"]
        assert len(oos) == 5

    def test_train_start_advances_each_fold(self) -> None:
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        train_splits = [s for s in splits if s.role == "train"]
        from dateutil.relativedelta import relativedelta

        for i in range(1, len(train_splits)):
            prev_start = train_splits[i - 1].test_range.start_date
            curr_start = train_splits[i].test_range.start_date
            assert curr_start == prev_start + relativedelta(years=1)

    def test_train_duration_constant_across_folds(self) -> None:
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        train_splits = [s for s in splits if s.role == "train"]
        durations = {(s.test_range.end_date - s.test_range.start_date).days for s in train_splits}
        # All durations equal (within ±1 day due to leap years)
        assert len(durations) <= 2

    def test_oos_windows_are_non_overlapping(self) -> None:
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        oos_splits = [s for s in splits if s.role == "oos"]
        for i in range(1, len(oos_splits)):
            prev_end = oos_splits[i - 1].test_range.end_date
            curr_start = oos_splits[i].test_range.start_date
            assert curr_start > prev_end

    def test_test_start_after_train_end(self) -> None:
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        paired = zip(
            [s for s in splits if s.role == "train"],
            [s for s in splits if s.role == "oos"],
        )
        for ts, os in paired:
            assert os.test_range.start_date > ts.test_range.end_date

    def test_fold_0_train_start_is_total_range_start(self) -> None:
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        fold0_train = next(s for s in splits if s.role == "train" and s.fold_index == 0)
        assert fold0_train.test_range.start_date == plan.splits.total_range.start_date

    def test_results_are_validation_splits(self) -> None:
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        for s in splits:
            assert isinstance(s, ValidationSplit)

    def test_wf_generator_rejects_static_plan(self) -> None:
        plan = _make_static_plan()
        with pytest.raises(ValueError, match="mode='walk_forward'"):
            WalkForwardSplitGenerator().generate(plan)


# ---------------------------------------------------------------------------
# T1.4 — Embargo
# ---------------------------------------------------------------------------


class TestEmbargo:
    """T1.4: Embargo gap is applied correctly."""

    def test_zero_embargo_adjacent_windows(self) -> None:
        """Zero embargo: test_start == train_end + 1 day."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "embargo": "0d",
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)
        paired = list(
            zip(
                [s for s in splits if s.role == "train"],
                [s for s in splits if s.role == "oos"],
            )
        )
        for ts, os in paired:
            assert os.test_range.start_date == ts.test_range.end_date + timedelta(days=1)

    def test_nonzero_embargo_gap(self) -> None:
        """5-day embargo: test_start == train_end + 6 days."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "embargo": "5d",
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)
        paired = list(
            zip(
                [s for s in splits if s.role == "train"],
                [s for s in splits if s.role == "oos"],
            )
        )
        for ts, os in paired:
            assert os.test_range.start_date == ts.test_range.end_date + timedelta(days=6)

    def test_test_start_always_after_train_end(self) -> None:
        """Enforce no overlap: test_start > train_end for any embargo."""
        for embargo in ("0d", "5d", "30d"):
            plan = _make_wf_plan(
                splits={
                    "style": "rolling",
                    "train": "2y",
                    "test": "1y",
                    "step": "1y",
                    "embargo": embargo,
                    "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2018, 12, 31)},
                }
            )
            splits = WalkForwardSplitGenerator().generate(plan)
            paired = zip(
                [s for s in splits if s.role == "train"],
                [s for s in splits if s.role == "oos"],
            )
            for ts, os in paired:
                assert os.test_range.start_date > ts.test_range.end_date

    def test_embargo_stored_on_oos_split(self) -> None:
        """OOS split carries the embargo timedelta."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "embargo": "5d",
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)
        oos_splits = [s for s in splits if s.role == "oos"]
        for sp in oos_splits:
            assert sp.embargo == timedelta(days=5)


# ---------------------------------------------------------------------------
# T1.5 — min_fold_bars enforcement
# ---------------------------------------------------------------------------


class TestMinFoldBars:
    """T1.5: min_fold_bars marks insufficient folds as invalid."""

    def test_no_invalid_when_bars_sufficient(self) -> None:
        """1y test ≈ 365 days; min_fold_bars=200 → all valid."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "min_fold_bars": 200,
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)
        invalid = [s for s in splits if s.status == "invalid"]
        assert len(invalid) == 0

    def test_all_invalid_when_bars_too_high(self) -> None:
        """1y test ≈ 365 days; min_fold_bars=500 → all folds invalid."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "min_fold_bars": 500,
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)
        invalid_oos = [s for s in splits if s.role == "oos" and s.status == "invalid"]
        oos = [s for s in splits if s.role == "oos"]
        assert len(invalid_oos) == len(oos)

    def test_reason_code_includes_fold_index(self) -> None:
        """Reason code is 'insufficient_history_for_fold:<n>'."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "min_fold_bars": 500,
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)
        invalid_oos = [s for s in splits if s.role == "oos" and s.status == "invalid"]
        for sp in invalid_oos:
            assert sp.reason is not None
            assert sp.reason.startswith("insufficient_history_for_fold:")
            fold_n = int(sp.reason.split(":")[-1])
            assert fold_n == sp.fold_index

    def test_invalid_fold_still_returned_not_raised(self) -> None:
        """Generator returns invalid folds rather than raising."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "min_fold_bars": 500,
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)  # must not raise
        assert len(splits) > 0

    def test_no_min_fold_bars_status_is_none(self) -> None:
        """Without min_fold_bars, all splits have status=None."""
        plan = _make_wf_plan()
        splits = WalkForwardSplitGenerator().generate(plan)
        for sp in splits:
            assert sp.status is None
            assert sp.reason is None


# ---------------------------------------------------------------------------
# T1.6 — compute_plan_sha256 extension
# ---------------------------------------------------------------------------


class TestComputePlanSha256WalkForward:
    """T1.6: Hash covers walk-forward fields; static hash unchanged."""

    def _wf_plan_with_splits(self, **split_overrides: Any) -> ValidationPlan:
        base_splits: dict[str, Any] = {
            "style": "rolling",
            "train": "2y",
            "test": "1y",
            "step": "1y",
            "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
        }
        base_splits.update(split_overrides)
        return ValidationPlan(
            validation_id="hash_test",
            strategy_experiment="x",
            base_config=FIXTURES_DIR / "base_config.yaml",
            mode="walk_forward",
            splits=base_splits,
        )

    def test_changing_train_changes_hash(self) -> None:
        plan_2y = self._wf_plan_with_splits(train="2y")
        plan_3y = self._wf_plan_with_splits(train="3y")
        h1 = compute_plan_sha256(plan_2y, FIXTURES_DIR / "base_config.yaml")
        h2 = compute_plan_sha256(plan_3y, FIXTURES_DIR / "base_config.yaml")
        assert h1 != h2

    def test_changing_step_changes_hash(self) -> None:
        plan_1y = self._wf_plan_with_splits(step="1y")
        plan_2y = self._wf_plan_with_splits(step="2y", test="1y")
        h1 = compute_plan_sha256(plan_1y, FIXTURES_DIR / "base_config.yaml")
        h2 = compute_plan_sha256(plan_2y, FIXTURES_DIR / "base_config.yaml")
        assert h1 != h2

    def test_changing_style_changes_hash(self) -> None:
        plan_rolling = self._wf_plan_with_splits(style="rolling")
        plan_anchored = self._wf_plan_with_splits(style="anchored")
        h1 = compute_plan_sha256(plan_rolling, FIXTURES_DIR / "base_config.yaml")
        h2 = compute_plan_sha256(plan_anchored, FIXTURES_DIR / "base_config.yaml")
        assert h1 != h2

    def test_changing_total_range_changes_hash(self) -> None:
        plan_a = self._wf_plan_with_splits(total_range={"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)})
        plan_b = self._wf_plan_with_splits(total_range={"start_date": date(2010, 1, 1), "end_date": date(2018, 12, 31)})
        h1 = compute_plan_sha256(plan_a, FIXTURES_DIR / "base_config.yaml")
        h2 = compute_plan_sha256(plan_b, FIXTURES_DIR / "base_config.yaml")
        assert h1 != h2

    def test_static_plan_hash_stable(self) -> None:
        """static_is_oos plan hash is consistent across calls (backward compat)."""
        plan = _make_static_plan()
        h1 = compute_plan_sha256(plan, FIXTURES_DIR / "base_config.yaml")
        h2 = compute_plan_sha256(plan, FIXTURES_DIR / "base_config.yaml")
        assert h1 == h2

    def test_static_and_wf_hashes_differ(self) -> None:
        """Different modes produce different hashes."""
        static_plan = _make_static_plan()
        wf_plan = _make_wf_plan()
        h_static = compute_plan_sha256(static_plan, FIXTURES_DIR / "base_config.yaml")
        h_wf = compute_plan_sha256(wf_plan, FIXTURES_DIR / "base_config.yaml")
        assert h_static != h_wf

    def test_hash_is_hex_string_64_chars(self) -> None:
        plan = _make_wf_plan()
        h = compute_plan_sha256(plan, FIXTURES_DIR / "base_config.yaml")
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_description_does_not_affect_hash(self) -> None:
        """description is non-execution metadata and must not change the plan hash.

        Plans that predated the description field must hash identically to plans
        that carry a description value — and to plans that omit it entirely.
        """
        plan_no_desc = _make_static_plan()
        plan_with_desc = _make_static_plan(description="Human-readable label")
        h1 = compute_plan_sha256(plan_no_desc, FIXTURES_DIR / "base_config.yaml")
        h2 = compute_plan_sha256(plan_with_desc, FIXTURES_DIR / "base_config.yaml")
        assert h1 == h2, f"description field must be excluded from the canonical hash; got h1={h1[:12]} h2={h2[:12]}"

    def test_canonical_dict_excludes_description(self) -> None:
        """_plan_to_canonical_dict must not contain a 'description' key."""
        from qs_trader.validation.plan import _plan_to_canonical_dict

        plan_with_desc = _make_static_plan(description="Some label")
        d = _plan_to_canonical_dict(plan_with_desc)
        assert "description" not in d, "description must be excluded from the canonical dict to preserve hash stability"


# ---------------------------------------------------------------------------
# T1.8 — CLI dry-run for walk_forward mode
# ---------------------------------------------------------------------------


class TestCliDryRunWalkForward:
    """T1.8: CLI dry-run prints fold list for walk_forward mode."""

    def _make_wf_plan_yaml(self, tmp_path: Path) -> Path:
        base_cfg = tmp_path / "base_config.yaml"
        base_cfg.write_text((FIXTURES_DIR / "base_config.yaml").read_text())
        plan_yaml = tmp_path / "wf_plan.yaml"
        plan_yaml.write_text(
            f"""
validation_id: wf_cli_test
strategy_experiment: test_exp
base_config: {base_cfg}
mode: walk_forward

splits:
  style: rolling
  train: 2y
  test: 1y
  step: 1y
  total_range:
    start_date: "2010-01-01"
    end_date: "2016-12-31"
"""
        )
        return plan_yaml

    def test_dry_run_exits_0(self, tmp_path: Path) -> None:
        plan_yaml = self._make_wf_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert result.exit_code == 0, result.output

    def test_dry_run_prints_validation_id(self, tmp_path: Path) -> None:
        plan_yaml = self._make_wf_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert "wf_cli_test" in result.output

    def test_dry_run_prints_splits_header(self, tmp_path: Path) -> None:
        plan_yaml = self._make_wf_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert "Splits:" in result.output

    def test_dry_run_prints_train_folds(self, tmp_path: Path) -> None:
        plan_yaml = self._make_wf_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert "role=train" in result.output

    def test_dry_run_prints_oos_folds(self, tmp_path: Path) -> None:
        plan_yaml = self._make_wf_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert "role=oos" in result.output

    def test_dry_run_prints_fold_indices(self, tmp_path: Path) -> None:
        plan_yaml = self._make_wf_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert "fold=0" in result.output
        assert "fold=1" in result.output

    def test_dry_run_shows_date_arrows(self, tmp_path: Path) -> None:
        plan_yaml = self._make_wf_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert "→" in result.output

    def test_dry_run_5_oos_folds_listed(self, tmp_path: Path) -> None:
        """2y/1y/1y over 2010-2016 produces 5 OOS folds."""
        plan_yaml = self._make_wf_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        oos_lines = [line for line in result.output.splitlines() if "role=oos" in line]
        assert len(oos_lines) == 5


# ---------------------------------------------------------------------------
# T1.9 — Synthetic fixture
# ---------------------------------------------------------------------------


class TestWalkForwardFixture:
    """T1.9: Synthetic multi-year fixture loads and produces ≥3 OOS folds."""

    def test_fixture_file_exists(self) -> None:
        assert (FIXTURES_DIR / "walk_forward_plan.yaml").exists()

    def test_fixture_loads_as_walk_forward_plan(self) -> None:
        plan = load_validation_plan(FIXTURES_DIR / "walk_forward_plan.yaml")
        assert plan.mode == "walk_forward"

    def test_fixture_splits_is_wf_spec(self) -> None:
        plan = load_validation_plan(FIXTURES_DIR / "walk_forward_plan.yaml")
        assert isinstance(plan.splits, WalkForwardSplitsSpec)

    def test_fixture_produces_at_least_3_oos_folds(self) -> None:
        plan = load_validation_plan(FIXTURES_DIR / "walk_forward_plan.yaml")
        splits = WalkForwardSplitGenerator().generate(plan)
        oos = [s for s in splits if s.role == "oos"]
        assert len(oos) >= 3

    def test_fixture_rolling_style(self) -> None:
        plan = load_validation_plan(FIXTURES_DIR / "walk_forward_plan.yaml")
        assert plan.splits.style == "rolling"

    def test_fixture_all_splits_valid(self) -> None:
        """No min_fold_bars configured → all splits should have status=None."""
        plan = load_validation_plan(FIXTURES_DIR / "walk_forward_plan.yaml")
        splits = WalkForwardSplitGenerator().generate(plan)
        for sp in splits:
            assert sp.status is None


# ---------------------------------------------------------------------------
# Additional edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Edge cases for walk-forward generation."""

    def test_empty_result_when_total_range_too_short(self) -> None:
        """If train alone exceeds total_range, no folds are generated."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "5y",
                "test": "1y",
                "step": "1y",
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2013, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)
        assert splits == []

    def test_exactly_one_fold_when_range_is_exact(self) -> None:
        """Range fits exactly one fold."""
        plan = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2012, 12, 31)},
            }
        )
        splits = WalkForwardSplitGenerator().generate(plan)
        oos = [s for s in splits if s.role == "oos"]
        assert len(oos) == 1

    def test_anchored_same_first_fold_as_rolling(self) -> None:
        """Fold 0 is identical for both styles."""
        roll = _make_wf_plan(
            splits={
                "style": "rolling",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        anch = _make_wf_plan(
            splits={
                "style": "anchored",
                "train": "2y",
                "test": "1y",
                "step": "1y",
                "total_range": {"start_date": date(2010, 1, 1), "end_date": date(2016, 12, 31)},
            }
        )
        roll_splits = WalkForwardSplitGenerator().generate(roll)
        anch_splits = WalkForwardSplitGenerator().generate(anch)

        roll_fold0_oos = next(s for s in roll_splits if s.role == "oos" and s.fold_index == 0)
        anch_fold0_oos = next(s for s in anch_splits if s.role == "oos" and s.fold_index == 0)
        assert roll_fold0_oos.test_range == anch_fold0_oos.test_range
        assert roll_fold0_oos.train_range == anch_fold0_oos.train_range


# ---------------------------------------------------------------------------
# BLOCKER-3 regression: ValidationPlan extra="forbid"
# ---------------------------------------------------------------------------


class TestValidationPlanExtraForbid:
    """ValidationPlan must reject unknown root-level fields (extra='forbid').

    Spec §5.1: static_is_oos plans cannot silently accept Phase 2 fields
    such as cost_scenarios that are not defined in the Phase 1 schema.
    """

    def test_static_plan_with_cost_scenarios_raises(self) -> None:
        """extra='forbid': cost_scenarios on a static_is_oos plan is rejected."""
        with pytest.raises(ValidationError, match="cost_scenarios"):
            ValidationPlan(
                validation_id="bad_static",
                strategy_experiment="test",
                base_config=FIXTURES_DIR / "base_config.yaml",
                mode="static_is_oos",
                splits={
                    "in_sample": {"start_date": date(2018, 1, 2), "end_date": date(2021, 12, 31)},
                    "out_of_sample": {"start_date": date(2022, 1, 3), "end_date": date(2024, 12, 31)},
                },
                cost_scenarios=[{"name": "base", "slippage_bps": 5}],  # type: ignore[call-arg]
            )

    def test_static_plan_with_unknown_field_raises(self) -> None:
        """extra='forbid': arbitrary unknown keys on a static_is_oos plan are rejected."""
        with pytest.raises(ValidationError):
            ValidationPlan(
                validation_id="bad_static",
                strategy_experiment="test",
                base_config=FIXTURES_DIR / "base_config.yaml",
                mode="static_is_oos",
                splits={
                    "in_sample": {"start_date": date(2018, 1, 2), "end_date": date(2021, 12, 31)},
                    "out_of_sample": {"start_date": date(2022, 1, 3), "end_date": date(2024, 12, 31)},
                },
                unknown_phase2_field="some_value",  # type: ignore[call-arg]
            )

    def test_walk_forward_plan_loads_normally(self) -> None:
        """walk_forward plans are not affected by the extra='forbid' guard."""
        plan = _make_wf_plan()
        assert plan.mode == "walk_forward"

    def test_description_field_accepted_on_static_plan(self) -> None:
        """description is an explicit Optional field; it must be accepted and stored."""
        plan = _make_static_plan(description="Human-readable label for this plan.")
        assert plan.description == "Human-readable label for this plan."

    def test_description_defaults_to_none(self) -> None:
        """description defaults to None when omitted."""
        plan = _make_static_plan()
        assert plan.description is None
