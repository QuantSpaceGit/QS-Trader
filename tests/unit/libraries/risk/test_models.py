"""Unit tests for risk policy models.

Tests data models in libraries/risk/models.py, particularly budget allocation logic.

Coverage focus:
- RiskConfig.get_allocated_capital: Strategy-specific and default budget fallback
- Budget allocation with multiple strategies
- KeyError handling for unlisted strategies without default
"""

from decimal import Decimal

import pytest

from qs_trader.libraries.risk.models import (
    ConcentrationLimit,
    LeverageLimit,
    RiskConfig,
    ShortingPolicy,
    SizingConfig,
    StrategyBudget,
)


class TestRiskConfigGetAllocatedCapital:
    """Test suite for RiskConfig.get_allocated_capital method."""

    def test_strategy_specific_budget_returns_correct_allocation(self):
        """Test that strategy-specific budget allocation is used when available."""
        # Arrange
        config = RiskConfig(
            budgets=[
                StrategyBudget(strategy_id="sma_crossover", capital_weight=0.30),
                StrategyBudget(strategy_id="momentum", capital_weight=0.40),
            ],
            sizing={
                "sma_crossover": SizingConfig(
                    model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1
                ),
                "momentum": SizingConfig(model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1),
            },
            concentration=ConcentrationLimit(max_position_pct=1.0),
            leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
            shorting=ShortingPolicy(allow_short_positions=False),
            cash_buffer_pct=0.05,
        )
        equity = Decimal("100000")

        # Act
        allocated = config.get_allocated_capital("sma_crossover", equity)

        # Assert
        assert allocated == Decimal("30000")  # 30% of $100k

    def test_default_budget_fallback_when_strategy_not_listed(self):
        """Test that 'default' budget is used when strategy not explicitly listed."""
        # Arrange
        config = RiskConfig(
            budgets=[
                StrategyBudget(strategy_id="sma_crossover", capital_weight=0.30),
                StrategyBudget(strategy_id="default", capital_weight=0.20),  # Fallback
            ],
            sizing={
                "sma_crossover": SizingConfig(
                    model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1
                ),
                "default": SizingConfig(model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1),
            },
            concentration=ConcentrationLimit(max_position_pct=1.0),
            leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
            shorting=ShortingPolicy(allow_short_positions=False),
            cash_buffer_pct=0.05,
        )
        equity = Decimal("100000")

        # Act - request unlisted strategy "momentum"
        allocated = config.get_allocated_capital("momentum", equity)

        # Assert
        assert allocated == Decimal("20000")  # Uses default 20% allocation

    def test_multiple_strategies_use_correct_allocations(self):
        """Test that multiple strategies each get their correct allocation."""
        # Arrange
        config = RiskConfig(
            budgets=[
                StrategyBudget(strategy_id="strategy_a", capital_weight=0.25),
                StrategyBudget(strategy_id="strategy_b", capital_weight=0.35),
                StrategyBudget(strategy_id="strategy_c", capital_weight=0.30),
            ],
            sizing={
                "strategy_a": SizingConfig(
                    model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1
                ),
                "strategy_b": SizingConfig(
                    model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1
                ),
                "strategy_c": SizingConfig(
                    model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1
                ),
            },
            concentration=ConcentrationLimit(max_position_pct=1.0),
            leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
            shorting=ShortingPolicy(allow_short_positions=False),
            cash_buffer_pct=0.05,
        )
        equity = Decimal("100000")

        # Act
        alloc_a = config.get_allocated_capital("strategy_a", equity)
        alloc_b = config.get_allocated_capital("strategy_b", equity)
        alloc_c = config.get_allocated_capital("strategy_c", equity)

        # Assert
        assert alloc_a == Decimal("25000")  # 25%
        assert alloc_b == Decimal("35000")  # 35%
        assert alloc_c == Decimal("30000")  # 30%

    def test_no_matching_strategy_and_no_default_raises_key_error(self):
        """Test that KeyError raised when strategy not found and no default budget."""
        # Arrange
        config = RiskConfig(
            budgets=[
                StrategyBudget(strategy_id="sma_crossover", capital_weight=0.30),
            ],
            sizing={
                "sma_crossover": SizingConfig(
                    model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1
                ),
            },
            concentration=ConcentrationLimit(max_position_pct=1.0),
            leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
            shorting=ShortingPolicy(allow_short_positions=False),
            cash_buffer_pct=0.05,
        )
        equity = Decimal("100000")

        # Act & Assert
        with pytest.raises(KeyError, match="not found in budgets and no 'default' fallback defined"):
            config.get_allocated_capital("unlisted_strategy", equity)

    def test_default_only_budget_used_for_all_strategies(self):
        """Test that 'default' budget can be the only budget and applies to all strategies."""
        # Arrange
        config = RiskConfig(
            budgets=[
                StrategyBudget(strategy_id="default", capital_weight=0.90),
            ],
            sizing={
                "default": SizingConfig(model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1),
            },
            concentration=ConcentrationLimit(max_position_pct=1.0),
            leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
            shorting=ShortingPolicy(allow_short_positions=False),
            cash_buffer_pct=0.05,
        )
        equity = Decimal("100000")

        # Act
        alloc_any = config.get_allocated_capital("any_strategy", equity)

        # Assert
        assert alloc_any == Decimal("90000")  # 90% for any strategy

    def test_allocation_scales_with_equity(self):
        """Test that allocation correctly scales with changing equity."""
        # Arrange
        config = RiskConfig(
            budgets=[
                StrategyBudget(strategy_id="test", capital_weight=0.50),
            ],
            sizing={
                "test": SizingConfig(model="fixed_fraction", fraction=Decimal("0.02"), min_quantity=1, lot_size=1),
            },
            concentration=ConcentrationLimit(max_position_pct=1.0),
            leverage=LeverageLimit(max_gross=1.0, max_net=1.0),
            shorting=ShortingPolicy(allow_short_positions=False),
            cash_buffer_pct=0.05,
        )

        # Act
        alloc_100k = config.get_allocated_capital("test", Decimal("100000"))
        alloc_200k = config.get_allocated_capital("test", Decimal("200000"))
        alloc_50k = config.get_allocated_capital("test", Decimal("50000"))

        # Assert
        assert alloc_100k == Decimal("50000")  # 50% of 100k
        assert alloc_200k == Decimal("100000")  # 50% of 200k
        assert alloc_50k == Decimal("25000")  # 50% of 50k
