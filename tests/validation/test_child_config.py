"""Unit tests for qs_trader.validation.child_config module."""

from __future__ import annotations

from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from qs_trader.engine.config import BacktestConfig
from qs_trader.validation.child_config import derive_child_config
from qs_trader.validation.plan import DateRange, StaticSplitSpec, ValidationPlan
from qs_trader.validation.splits.base import ValidationSplit

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_plan() -> ValidationPlan:
    """Build a minimal ValidationPlan for child-config tests."""
    return ValidationPlan(
        validation_id="test",
        strategy_experiment="test",
        base_config=FIXTURES_DIR / "base_config.yaml",
        mode="static_is_oos",
        splits=StaticSplitSpec(
            in_sample=DateRange(start_date=date(2018, 1, 2), end_date=date(2021, 12, 31)),
            out_of_sample=DateRange(start_date=date(2022, 1, 3), end_date=date(2024, 12, 31)),
        ),
    )


def _make_base_config(**overrides: Any) -> BacktestConfig:
    """Build a minimal BacktestConfig for testing."""
    base: dict[str, Any] = {
        "backtest_id": "base_strategy",
        "start_date": datetime(2020, 1, 1),
        "end_date": datetime(2023, 12, 31),
        "initial_equity": Decimal("100000"),
        "data": {
            "sources": [
                {"name": "yahoo-us-equity-1d-csv", "universe": ["AAPL", "MSFT"]}
            ]
        },
        "strategies": [
            {
                "strategy_id": "buy_and_hold",
                "universe": ["AAPL", "MSFT"],
                "data_sources": ["yahoo-us-equity-1d-csv"],
                "config": {},
            }
        ],
        "risk_policy": {"name": "naive", "config": {"max_pct_position_size": 0.30}},
    }
    base.update(overrides)
    return BacktestConfig(**base)


def _make_is_split() -> ValidationSplit:
    """Build a typical IS ValidationSplit."""
    return ValidationSplit(
        fold_index=0,
        role="is",
        test_range=DateRange(start_date=date(2018, 1, 2), end_date=date(2021, 12, 31)),
    )


def _make_oos_split() -> ValidationSplit:
    """Build a typical OOS ValidationSplit."""
    return ValidationSplit(
        fold_index=1,
        role="oos",
        test_range=DateRange(start_date=date(2022, 1, 3), end_date=date(2024, 12, 31)),
    )


# ---------------------------------------------------------------------------
# TestDeriveChildConfig
# ---------------------------------------------------------------------------


class TestDeriveChildConfig:
    """Tests for derive_child_config."""

    def test_start_date_overridden_from_split(self) -> None:
        """start_date in child config reflects split.test_range.start_date."""
        plan = _make_plan()
        base = _make_base_config()
        split = _make_is_split()

        child = derive_child_config(plan, split, base)

        expected = datetime.combine(split.test_range.start_date, time.min)
        assert child.start_date == expected

    def test_end_date_overridden_from_split(self) -> None:
        """end_date in child config reflects split.test_range.end_date."""
        plan = _make_plan()
        base = _make_base_config()
        split = _make_is_split()

        child = derive_child_config(plan, split, base)

        expected = datetime.combine(split.test_range.end_date, time.min)
        assert child.end_date == expected

    def test_other_fields_are_preserved(self) -> None:
        """All fields except start_date and end_date are unchanged."""
        plan = _make_plan()
        base = _make_base_config()
        split = _make_is_split()

        child = derive_child_config(plan, split, base)

        assert child.backtest_id == base.backtest_id
        assert child.initial_equity == base.initial_equity
        assert child.data == base.data
        assert child.strategies == base.strategies
        assert child.risk_policy == base.risk_policy
        assert child.price_basis == base.price_basis

    def test_base_config_is_not_mutated(self) -> None:
        """The original base_config is not changed by derivation."""
        plan = _make_plan()
        base = _make_base_config()
        original_start = base.start_date
        original_end = base.end_date
        split = _make_is_split()

        derive_child_config(plan, split, base)

        assert base.start_date == original_start
        assert base.end_date == original_end

    def test_returns_new_instance(self) -> None:
        """The returned config is a different object from the original."""
        plan = _make_plan()
        base = _make_base_config()
        split = _make_is_split()

        child = derive_child_config(plan, split, base)

        assert child is not base

    def test_child_is_valid_backtest_config(self) -> None:
        """The derived child config is a valid, fully-constructed BacktestConfig."""
        plan = _make_plan()
        base = _make_base_config()
        split = _make_is_split()

        child = derive_child_config(plan, split, base)

        assert isinstance(child, BacktestConfig)

    def test_oos_split_dates_applied_correctly(self) -> None:
        """Derive child config from an OOS split with the correct date range."""
        plan = _make_plan()
        base = _make_base_config()
        split = _make_oos_split()

        child = derive_child_config(plan, split, base)

        assert child.start_date == datetime(2022, 1, 3, 0, 0, 0)
        assert child.end_date == datetime(2024, 12, 31, 0, 0, 0)

    def test_is_split_dates_applied_correctly(self) -> None:
        """Derive child config from an IS split with the correct date range."""
        plan = _make_plan()
        base = _make_base_config()
        split = _make_is_split()

        child = derive_child_config(plan, split, base)

        assert child.start_date == datetime(2018, 1, 2, 0, 0, 0)
        assert child.end_date == datetime(2021, 12, 31, 0, 0, 0)

    def test_midnight_time_component(self) -> None:
        """Converted datetimes have midnight (00:00:00) time component."""
        plan = _make_plan()
        base = _make_base_config()
        split = _make_is_split()

        child = derive_child_config(plan, split, base)

        assert child.start_date.hour == 0
        assert child.start_date.minute == 0
        assert child.start_date.second == 0
        assert child.end_date.hour == 0
        assert child.end_date.minute == 0
        assert child.end_date.second == 0

    def test_derived_config_satisfies_date_ordering_constraint(self) -> None:
        """The derived config satisfies BacktestConfig's end_date > start_date constraint."""
        plan = _make_plan()
        base = _make_base_config()
        split = ValidationSplit(
            fold_index=0,
            role="is",
            test_range=DateRange(start_date=date(2018, 1, 2), end_date=date(2021, 12, 31)),
        )

        child = derive_child_config(plan, split, base)

        assert child.end_date > child.start_date

    def test_optional_fields_preserved(self) -> None:
        """Optional fields (run_id, reporting, etc.) are carried over unchanged."""
        plan = _make_plan()
        base = _make_base_config(
            run_id="original_run",
            reporting={
                "write_json": True,
                "display_final_report": False,
            },
        )
        split = _make_is_split()

        child = derive_child_config(plan, split, base)

        assert child.run_id == "original_run"
        assert child.reporting is not None
        assert child.reporting.display_final_report is False
