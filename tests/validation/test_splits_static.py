"""Unit tests for qs_trader.validation.splits.static module."""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pytest

from qs_trader.validation.plan import (
    DateRange,
    StaticSplitSpec,
    ValidationPlan,
)
from qs_trader.validation.splits.base import ValidationSplit
from qs_trader.validation.splits.static import StaticSplitGenerator

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_plan(**overrides: Any) -> ValidationPlan:
    """Build a minimal ValidationPlan for split-generation tests."""
    base: dict[str, Any] = {
        "validation_id": "test",
        "strategy_experiment": "test",
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
# TestStaticSplitGenerator
# ---------------------------------------------------------------------------


class TestStaticSplitGenerator:
    """Tests for StaticSplitGenerator."""

    def test_generates_exactly_two_splits(self) -> None:
        """Generator produces a list of exactly two splits."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        assert len(splits) == 2

    def test_first_split_has_is_role(self) -> None:
        """First split has role='is'."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        assert splits[0].role == "is"

    def test_second_split_has_oos_role(self) -> None:
        """Second split has role='oos'."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        assert splits[1].role == "oos"

    def test_fold_index_zero_for_is(self) -> None:
        """IS split has fold_index=0."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        assert splits[0].fold_index == 0

    def test_fold_index_one_for_oos(self) -> None:
        """OOS split has fold_index=1."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        assert splits[1].fold_index == 1

    def test_is_split_dates_match_in_sample(self) -> None:
        """IS split test range matches plan.splits.in_sample."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        is_split = splits[0]
        assert is_split.test_range.start_date == date(2018, 1, 2)
        assert is_split.test_range.end_date == date(2021, 12, 31)

    def test_oos_split_dates_match_out_of_sample(self) -> None:
        """OOS split test range matches plan.splits.out_of_sample."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        oos_split = splits[1]
        assert oos_split.test_range.start_date == date(2022, 1, 3)
        assert oos_split.test_range.end_date == date(2024, 12, 31)

    def test_no_train_range_for_either_split(self) -> None:
        """Static splits carry no explicit train_range."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        for split in splits:
            assert split.train_range is None

    def test_default_embargo_is_zero(self) -> None:
        """Default embargo is timedelta(0) for both splits."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        for split in splits:
            assert split.embargo == timedelta(0)

    def test_splits_are_frozen_dataclasses(self) -> None:
        """ValidationSplit instances are immutable (frozen=True)."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        with pytest.raises((AttributeError, TypeError)):
            splits[0].fold_index = 99  # type: ignore[misc]
    def test_returns_list_of_validation_split_instances(self) -> None:
        """Each element in the result is a ValidationSplit."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        for split in splits:
            assert isinstance(split, ValidationSplit)

    def test_is_split_precedes_oos_split(self) -> None:
        """IS split test period ends strictly before OOS split test period begins."""
        plan = _make_plan()
        splits = StaticSplitGenerator().generate(plan)
        assert splits[0].test_range.end_date < splits[1].test_range.start_date

    def test_wrong_mode_raises_value_error(self) -> None:
        """Raise ValueError when the plan mode is not 'static_is_oos'."""
        # Bypass Pydantic validation to construct a plan with an unsupported mode.
        plan = ValidationPlan.model_construct(
            validation_id="x",
            strategy_experiment="x",
            base_config=FIXTURES_DIR / "base_config.yaml",
            mode="walk_forward",
            splits=StaticSplitSpec(
                in_sample=DateRange(start_date=date(2018, 1, 1), end_date=date(2021, 12, 31)),
                out_of_sample=DateRange(start_date=date(2022, 1, 1), end_date=date(2024, 12, 31)),
            ),
        )
        with pytest.raises(ValueError, match="static_is_oos"):
            StaticSplitGenerator().generate(plan)

    def test_generator_is_reusable(self) -> None:
        """The same StaticSplitGenerator instance can be called multiple times."""
        generator = StaticSplitGenerator()
        plan = _make_plan()
        splits1 = generator.generate(plan)
        splits2 = generator.generate(plan)
        assert splits1 == splits2

    def test_different_date_ranges_reflected_in_splits(self) -> None:
        """Splits correctly reflect different date ranges from the plan."""
        plan = ValidationPlan(
            validation_id="alt",
            strategy_experiment="test",
            base_config=FIXTURES_DIR / "base_config.yaml",
            mode="static_is_oos",
            splits=StaticSplitSpec(
                in_sample=DateRange(start_date=date(2010, 1, 1), end_date=date(2015, 12, 31)),
                out_of_sample=DateRange(start_date=date(2016, 1, 1), end_date=date(2019, 12, 31)),
            ),
        )
        splits = StaticSplitGenerator().generate(plan)
        assert splits[0].test_range.start_date == date(2010, 1, 1)
        assert splits[0].test_range.end_date == date(2015, 12, 31)
        assert splits[1].test_range.start_date == date(2016, 1, 1)
        assert splits[1].test_range.end_date == date(2019, 12, 31)
