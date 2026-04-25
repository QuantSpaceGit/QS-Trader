"""
Risk Management Library.

Pure function-based risk tools for position sizing and limit checking.
All tools are stateless, composable, and easy to test.

Architecture:
- tools/sizing.py: Position sizing functions
- tools/limits.py: Risk limit checking functions
- models.py: Configuration dataclasses
- loaders.py: Policy loading from YAML

Usage:
    >>> from qs_trader.libraries.risk import load_policy
    >>> from qs_trader.libraries.risk.tools import sizing, limits
    >>>
    >>> # Load risk policy
    >>> config = load_policy("naive")
    >>>
    >>> # Calculate position size
    >>> quantity = sizing.calculate_fixed_fraction_size(
    ...     allocated_capital=Decimal("10000"),
    ...     signal_strength=0.8,
    ...     current_price=Decimal("150"),
    ...     fraction=Decimal("0.02")
    ... )
    >>>
    >>> # Check limits
    >>> violation = limits.check_concentration_limit(
    ...     order=order,
    ...     current_positions=positions,
    ...     equity=Decimal("100000"),
    ...     current_price=Decimal("150"),
    ...     max_position_pct=0.10
    ... )
"""

from qs_trader.libraries.risk.loaders import list_builtin_policies, list_custom_policies, load_policy
from qs_trader.libraries.risk.models import (
    ConcentrationLimit,
    LeverageLimit,
    RiskConfig,
    SizingConfig,
    SleeveBudget,
    SleeveId,
    StrategyBudget,
)

__all__ = [
    # Policy loading
    "load_policy",
    "list_builtin_policies",
    "list_custom_policies",
    # Configuration models
    "RiskConfig",
    "StrategyBudget",
    "SleeveId",
    "SleeveBudget",
    "SizingConfig",
    "ConcentrationLimit",
    "LeverageLimit",
]
