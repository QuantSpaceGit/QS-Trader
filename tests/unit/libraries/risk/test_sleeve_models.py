"""Focused unit tests for Phase 2 sleeve primitives and budget behavior."""

from decimal import Decimal

import pytest

from qs_trader.libraries.risk.models import (
    ConcentrationLimit,
    LeverageLimit,
    RiskConfig,
    ShortingPolicy,
    SizingConfig,
    StrategyBudget,
    SleeveBudget,
    SleeveId,
)


@pytest.fixture
def base_risk_config() -> RiskConfig:
    """Provide a baseline risk config that can be sleeve-bound."""
    return RiskConfig(
        budgets=[
            StrategyBudget(strategy_id="sma_crossover", capital_weight=0.30),
            StrategyBudget(strategy_id="default", capital_weight=0.70),
        ],
        sizing={
            "sma_crossover": SizingConfig(
                model="fixed_fraction",
                fraction=Decimal("0.10"),
                min_quantity=1,
                lot_size=1,
            ),
            "default": SizingConfig(
                model="fixed_fraction",
                fraction=Decimal("0.10"),
                min_quantity=1,
                lot_size=1,
            ),
        },
        concentration=ConcentrationLimit(max_position_pct=1.0),
        leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
        shorting=ShortingPolicy(allow_short_positions=False),
        cash_buffer_pct=0.05,
    )


def test_sleeve_id_round_trips_between_structured_and_serialized_forms() -> None:
    sleeve_id = SleeveId(strategy_id="sma_crossover", sleeve_key="AAPL")

    assert sleeve_id.serialized == "sma_crossover:AAPL"
    assert str(sleeve_id) == "sma_crossover:AAPL"
    assert SleeveId.parse("sma_crossover:AAPL") == sleeve_id


@pytest.mark.parametrize("bad_value", ["", "missing-delimiter"])
def test_sleeve_id_parse_rejects_invalid_serialized_values(bad_value: str) -> None:
    with pytest.raises(ValueError):
        SleeveId.parse(bad_value)


def test_sleeve_budget_requires_positive_allocated_equity() -> None:
    with pytest.raises(ValueError, match="allocated_equity must be positive"):
        SleeveBudget(
            sleeve_id=SleeveId(strategy_id="sma_crossover", sleeve_key="AAPL"),
            allocated_equity=Decimal("0"),
            symbols=("AAPL",),
        )


def test_sleeve_budget_matches_only_owned_strategy_and_symbol() -> None:
    sleeve_budget = SleeveBudget(
        sleeve_id=SleeveId(strategy_id="sma_crossover", sleeve_key="AAPL"),
        allocated_equity=Decimal("25000"),
        symbols=("AAPL",),
    )

    assert sleeve_budget.matches("sma_crossover", "AAPL") is True
    assert sleeve_budget.matches("sma_crossover", "MSFT") is False
    assert sleeve_budget.matches("mean_reversion", "AAPL") is False


def test_risk_config_uses_frozen_sleeve_equity_for_matching_strategy_symbol(base_risk_config: RiskConfig) -> None:
    sleeve_budget = SleeveBudget(
        sleeve_id=SleeveId(strategy_id="sma_crossover", sleeve_key="AAPL"),
        allocated_equity=Decimal("25000"),
        symbols=("AAPL",),
    )
    config = RiskConfig(
        budgets=base_risk_config.budgets,
        sizing=base_risk_config.sizing,
        concentration=base_risk_config.concentration,
        leverage=base_risk_config.leverage,
        shorting=base_risk_config.shorting,
        cash_buffer_pct=base_risk_config.cash_buffer_pct,
        sleeve_budget=sleeve_budget,
    )

    allocated = config.get_allocated_capital("sma_crossover", Decimal("100000"), "AAPL")

    assert allocated == Decimal("25000")


def test_risk_config_falls_back_to_strategy_budget_when_symbol_is_not_sleeve_bound(base_risk_config: RiskConfig) -> None:
    sleeve_budget = SleeveBudget(
        sleeve_id=SleeveId(strategy_id="sma_crossover", sleeve_key="AAPL"),
        allocated_equity=Decimal("25000"),
        symbols=("AAPL",),
    )
    config = RiskConfig(
        budgets=base_risk_config.budgets,
        sizing=base_risk_config.sizing,
        concentration=base_risk_config.concentration,
        leverage=base_risk_config.leverage,
        shorting=base_risk_config.shorting,
        cash_buffer_pct=base_risk_config.cash_buffer_pct,
        sleeve_budget=sleeve_budget,
    )

    allocated = config.get_allocated_capital("sma_crossover", Decimal("100000"), "MSFT")

    assert allocated == Decimal("30000")
