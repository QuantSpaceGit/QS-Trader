"""Risk management configuration models.

Immutable data structures for risk policies, sizing, and limits.
These models are used to configure risk management behavior.

Design Principles:
- Immutable (frozen dataclasses)
- Pure data (no business logic)
- Comprehensive validation in __post_init__
- Type-safe (full type hints)

Thread Safety:
- All models are immutable and thread-safe
"""

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal


@dataclass(frozen=True)
class StrategyBudget:
    """Capital allocation for a strategy.

    Defines how much of the portfolio equity is allocated to a strategy.

    Attributes:
        strategy_id: Unique identifier for the strategy
        capital_weight: Fraction of equity allocated [0, 1] (e.g., 0.3 = 30%)

    Example:
        >>> budget = StrategyBudget(strategy_id="sma_crossover", capital_weight=0.30)
        >>> # Allocates 30% of portfolio equity to sma_crossover strategy
    """

    strategy_id: str
    capital_weight: float

    def __post_init__(self) -> None:
        """Validate budget fields."""
        if not self.strategy_id:
            raise ValueError("strategy_id cannot be empty")

        if not 0.0 <= self.capital_weight <= 1.0:
            raise ValueError(f"capital_weight must be in [0, 1], got {self.capital_weight}")


@dataclass(frozen=True)
class SizingConfig:
    """Position sizing configuration.

    Defines how to calculate order quantities from signals.

    Attributes:
        model: Sizing model ("fixed_fraction" or "equal_weight")
        fraction: For fixed_fraction: % of allocated capital per position (e.g., 0.02 = 2%)
        min_quantity: Minimum order quantity (default: 1)
        lot_size: Minimum trading unit (default: 1 share)

    Examples:
        >>> # Fixed fraction: 2% of allocated capital per position
        >>> config = SizingConfig(
        ...     model="fixed_fraction",
        ...     fraction=Decimal("0.02"),
        ...     min_quantity=1,
        ...     lot_size=1
        ... )

        >>> # Equal weight across positions
        >>> config2 = SizingConfig(
        ...     model="equal_weight",
        ...     fraction=Decimal("1.0"),  # Not used for equal_weight
        ...     min_quantity=1,
        ...     lot_size=100  # Options contracts
        ... )
    """

    model: Literal["fixed_fraction", "equal_weight"]
    fraction: Decimal
    min_quantity: int = 1
    lot_size: int = 1

    def __post_init__(self) -> None:
        """Validate sizing config."""
        if self.model not in ("fixed_fraction", "equal_weight"):
            raise ValueError(f"model must be 'fixed_fraction' or 'equal_weight', got {self.model}")

        if self.fraction < 0 or self.fraction > 1:
            raise ValueError(f"fraction must be in [0, 1], got {self.fraction}")

        if self.min_quantity < 1:
            raise ValueError(f"min_quantity must be >= 1, got {self.min_quantity}")

        if self.lot_size < 1:
            raise ValueError(f"lot_size must be >= 1, got {self.lot_size}")


@dataclass(frozen=True)
class ConcentrationLimit:
    """Concentration limit per symbol.

    Prevents over-concentration in a single security.

    Attributes:
        max_position_pct: Maximum position size as % of equity (e.g., 0.10 = 10%)

    Example:
        >>> limit = ConcentrationLimit(max_position_pct=0.10)
        >>> # No single position can exceed 10% of portfolio equity
    """

    max_position_pct: float

    def __post_init__(self) -> None:
        """Validate concentration limit."""
        if not 0.0 < self.max_position_pct <= 1.0:
            raise ValueError(f"max_position_pct must be in (0, 1], got {self.max_position_pct}")


@dataclass(frozen=True)
class LeverageLimit:
    """Portfolio leverage limits.

    Controls total portfolio exposure.

    Attributes:
        max_gross: Maximum gross leverage (e.g., 2.0 = 200% gross exposure)
        max_net: Maximum net leverage (e.g., 1.0 = 100% net exposure)

    Example:
        >>> limit = LeverageLimit(max_gross=2.0, max_net=1.0)
        >>> # Allows 2x gross leverage, 1x net leverage
        >>> # Good for long-short portfolios
    """

    max_gross: float
    max_net: float

    def __post_init__(self) -> None:
        """Validate leverage limits."""
        if self.max_gross <= 0.0:
            raise ValueError(f"max_gross must be positive, got {self.max_gross}")

        if self.max_net <= 0.0:
            raise ValueError(f"max_net must be positive, got {self.max_net}")

        # Note: max_net can be > max_gross for long-short portfolios


@dataclass(frozen=True)
class ShortingPolicy:
    """Shorting policy configuration.

    Controls whether short positions are allowed.

    Attributes:
        allow_short_positions: If False, reject OPEN_SHORT signals

    Example:
        >>> policy = ShortingPolicy(allow_short_positions=False)
        >>> # Long-only portfolio
    """

    allow_short_positions: bool = False


@dataclass(frozen=True)
class RiskConfig:
    """Complete risk management configuration.

    Top-level configuration for all risk management parameters.
    Loaded from YAML policy files.

    Attributes:
        budgets: List of strategy capital allocations
        sizing: Sizing config per strategy (strategy_id -> SizingConfig)
        concentration: Concentration limit configuration
        leverage: Leverage limit configuration
        shorting: Shorting policy configuration
        cash_buffer_pct: Reserve cash as % of equity (default: 2%)

    Example:
        >>> config = RiskConfig(
        ...     budgets=[
        ...         StrategyBudget("sma_crossover", 0.50),
        ...         StrategyBudget("momentum", 0.30),
        ...     ],
        ...     sizing={
        ...         "sma_crossover": SizingConfig("fixed_fraction", Decimal("0.02")),
        ...         "momentum": SizingConfig("fixed_fraction", Decimal("0.03")),
        ...     },
        ...     concentration=ConcentrationLimit(max_position_pct=0.10),
        ...     leverage=LeverageLimit(max_gross=2.0, max_net=1.0),
        ...     shorting=ShortingPolicy(allow_short_positions=False),
        ...     cash_buffer_pct=0.02
        ... )
    """

    budgets: list[StrategyBudget]
    sizing: dict[str, SizingConfig]
    concentration: ConcentrationLimit
    leverage: LeverageLimit
    shorting: ShortingPolicy
    cash_buffer_pct: float = 0.02

    def __post_init__(self) -> None:
        """Validate risk config."""
        # Check budgets sum to <= 1.0
        total_weight = sum(b.capital_weight for b in self.budgets)
        if total_weight > 1.0:
            raise ValueError(f"Budget weights sum to {total_weight:.2%}, must be <= 100%")

        # Check all budgets have sizing config
        strategy_ids = {b.strategy_id for b in self.budgets}
        sizing_ids = set(self.sizing.keys())
        missing = strategy_ids - sizing_ids
        if missing:
            raise ValueError(f"Strategies {missing} have budgets but no sizing config")

        # Validate cash buffer
        if not 0.0 <= self.cash_buffer_pct <= 0.5:
            raise ValueError(f"cash_buffer_pct must be in [0, 0.5], got {self.cash_buffer_pct}")

    def get_allocated_capital(self, strategy_id: str, equity: Decimal) -> Decimal:
        """Calculate allocated capital for a strategy.

        Args:
            strategy_id: Strategy identifier
            equity: Current portfolio equity

        Returns:
            Allocated capital for the strategy

        Raises:
            KeyError: If strategy_id not found in budgets and no 'default' fallback

        Example:
            >>> config = RiskConfig(...)  # With sma_crossover at 30% weight
            >>> allocated = config.get_allocated_capital("sma_crossover", Decimal("100000"))
            >>> allocated
            Decimal('30000')
        """
        # Try strategy-specific budget first
        for budget in self.budgets:
            if budget.strategy_id == strategy_id:
                return equity * Decimal(str(budget.capital_weight))

        # Fall back to "default" budget if exists
        for budget in self.budgets:
            if budget.strategy_id == "default":
                return equity * Decimal(str(budget.capital_weight))

        # No budget found for strategy and no default
        raise KeyError(f"Strategy '{strategy_id}' not found in budgets and no 'default' fallback defined")

    def get_sizing_config(self, strategy_id: str) -> SizingConfig:
        """Get sizing configuration for a strategy.

        Args:
            strategy_id: Strategy identifier

        Returns:
            Sizing configuration for the strategy

        Raises:
            KeyError: If strategy_id not found in sizing config

        Example:
            >>> config = RiskConfig(...)
            >>> sizing = config.get_sizing_config("sma_crossover")
            >>> sizing.fraction
            Decimal('0.02')
        """
        if strategy_id not in self.sizing:
            raise KeyError(f"Strategy '{strategy_id}' not found in sizing config")

        return self.sizing[strategy_id]
