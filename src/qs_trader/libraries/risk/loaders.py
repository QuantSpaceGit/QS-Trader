"""Risk policy loader.

Loads risk management policies from YAML files and converts them to
RiskConfig dataclasses.

Search Order:
1. Built-in policies: src/qs_trader/libraries/risk/builtin/{name}.yaml
2. Custom policies: {custom_libraries.risk_policies}/{name}.yaml (from qs_trader.yaml)

Design Principles:
- Clear error messages for missing/invalid policies
- Validation at load time (fail fast)
- Support for both built-in and custom policies
- Custom path loaded from system configuration (not hardcoded)

Thread Safety:
- All functions are pure and thread-safe
"""

from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from qs_trader.libraries.risk.models import (
    ConcentrationLimit,
    LeverageLimit,
    RiskConfig,
    ShortingPolicy,
    SizingConfig,
    StrategyBudget,
)
from qs_trader.system.config import SystemConfig


def load_policy(name: str, custom_path: str | None = None) -> RiskConfig:
    """Load risk policy from YAML file.

    Search order:
    1. Test fixtures: tests/fixtures/risk_policies/{name}.yaml (for tests only)
    2. Built-in policies: src/qs_trader/libraries/risk/builtin/{name}.yaml
    3. Custom policies: {custom_path}/{name}.yaml OR system config custom_libraries.risk_policies

    Args:
        name: Policy name (without .yaml extension)
        custom_path: Optional custom search path for policies (overrides system config)

    Returns:
        Parsed and validated RiskConfig

    Raises:
        FileNotFoundError: If policy file not found in any search location
        ValueError: If policy YAML is invalid or missing required fields
        yaml.YAMLError: If YAML parsing fails

    Examples:
        >>> # Load built-in policy
        >>> config = load_policy("naive")

        >>> # Load custom policy (uses system config path)
        >>> config = load_policy("my_policy")

        >>> # Load test fixture (in tests)
        >>> config = load_policy("test_naive")

        >>> # Load custom policy with explicit path
        >>> config = load_policy("my_policy", custom_path="custom/risk")

        >>> # Access configuration
        >>> print(config.concentration.max_position_pct)
        0.10
    """
    # Determine test fixtures path (relative to project root)
    # Path from this file: src/qs_trader/libraries/risk/loaders.py
    # Need to go up 4 levels to reach project root
    module_dir = Path(__file__).parent
    project_root = (
        module_dir.parent.parent.parent.parent
    )  # src/qs_trader/libraries/risk -> src -> qs_trader -> libraries -> risk
    test_fixtures_dir = project_root / "tests" / "fixtures" / "risk_policies"
    test_fixtures_path = test_fixtures_dir / f"{name}.yaml"

    # Determine builtin path (relative to this module)
    builtin_dir = module_dir / "builtin"
    builtin_path = builtin_dir / f"{name}.yaml"

    # Determine custom path
    custom_policy_path = None
    if custom_path:
        # Explicit override provided
        custom_dir = Path(custom_path)
        custom_policy_path = custom_dir / f"{name}.yaml"
    else:
        # Load from system configuration
        system_config = SystemConfig.load()
        # Only check custom directory if path is configured (not None)
        if system_config.custom_libraries.risk_policies is not None:
            custom_dir = Path(system_config.custom_libraries.risk_policies)
            custom_policy_path = custom_dir / f"{name}.yaml"

    # Search in priority order: test fixtures -> builtin -> custom
    if test_fixtures_path.exists():
        policy_path = test_fixtures_path
    elif builtin_path.exists():
        policy_path = builtin_path
    elif custom_policy_path and custom_policy_path.exists():
        policy_path = custom_policy_path
    else:
        custom_path_msg = (
            f"  3. Custom: {custom_policy_path}\n"
            if custom_policy_path
            else "  3. Custom: (not configured - set custom_libraries.risk_policies in qs_trader.yaml)\n"
        )
        raise FileNotFoundError(
            f"Policy '{name}' not found. Searched:\n"
            f"  1. Test fixtures: {test_fixtures_path}\n"
            f"  2. Built-in: {builtin_path}\n"
            f"{custom_path_msg}"
            f"\nAvailable built-in policies: {list_builtin_policies()}\n"
            f"Available custom policies: {list_custom_policies(custom_path)}"
        )

    # Load YAML
    try:
        with open(policy_path, "r") as f:
            raw_policy = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML from {policy_path}: {e}")

    # Parse to RiskConfig
    try:
        config = _parse_policy(raw_policy, policy_path)
    except Exception as e:
        raise ValueError(f"Failed to parse policy from {policy_path}: {e}")

    return config


def list_builtin_policies() -> list[str]:
    """List available built-in policy names.

    Returns:
        List of policy names (without .yaml extension)

    Example:
        >>> policies = list_builtin_policies()
        >>> print(policies)
        ['naive', 'aggressive', 'conservative']
    """
    module_dir = Path(__file__).parent
    builtin_dir = module_dir / "builtin"

    if not builtin_dir.exists():
        return []

    policies = []
    for yaml_file in builtin_dir.glob("*.yaml"):
        policies.append(yaml_file.stem)

    return sorted(policies)


def list_custom_policies(custom_path: str | None = None) -> list[str]:
    """List available custom policy names.

    Args:
        custom_path: Optional custom search path for policies (overrides system config)

    Returns:
        List of policy names (without .yaml extension)

    Example:
        >>> # List from system config path
        >>> policies = list_custom_policies()
        >>> print(policies)
        ['my_policy', 'backtest_policy']

        >>> # List from explicit path
        >>> policies = list_custom_policies("custom/risk")
    """
    if custom_path:
        custom_dir = Path(custom_path)
    else:
        # Load from system configuration
        system_config = SystemConfig.load()
        # Return empty list if custom path not configured
        if system_config.custom_libraries.risk_policies is None:
            return []
        custom_dir = Path(system_config.custom_libraries.risk_policies)

    if not custom_dir.exists():
        return []

    policies = []
    for yaml_file in custom_dir.glob("*.yaml"):
        policies.append(yaml_file.stem)

    return sorted(policies)


def _parse_policy(raw_policy: dict[str, Any], source_path: Path) -> RiskConfig:
    """Parse raw YAML policy dict into RiskConfig.

    Args:
        raw_policy: Raw policy dictionary from YAML
        source_path: Path to policy file (for error messages)

    Returns:
        Validated RiskConfig

    Raises:
        ValueError: If policy structure is invalid
    """
    if "portfolio_risk_policy" not in raw_policy:
        raise ValueError(f"Policy file {source_path} must have 'portfolio_risk_policy' root key")

    policy = raw_policy["portfolio_risk_policy"]

    # Parse budgets
    budgets = _parse_budgets(policy, source_path)

    # Parse sizing configs
    sizing = _parse_sizing(policy, budgets, source_path)

    # Parse concentration limit
    concentration = _parse_concentration(policy, source_path)

    # Parse leverage limit
    leverage = _parse_leverage(policy, source_path)

    # Parse shorting policy
    shorting = _parse_shorting(policy, source_path)

    # Parse cash buffer
    cash_buffer_pct = _parse_cash_buffer(policy)

    # Construct RiskConfig (will validate in __post_init__)
    return RiskConfig(
        budgets=budgets,
        sizing=sizing,
        concentration=concentration,
        leverage=leverage,
        shorting=shorting,
        cash_buffer_pct=cash_buffer_pct,
    )


def _parse_budgets(policy: dict[str, Any], source_path: Path) -> list[StrategyBudget]:
    """Parse strategy budgets from policy.

    Looks for explicit 'budgets' section in the policy YAML. If not found,
    creates a single 'default' budget at 95% capital weight.

    Expected YAML structure:
        budgets:
          - strategy_id: "sma_crossover"
            capital_weight: 0.30
          - strategy_id: "momentum"
            capital_weight: 0.30
          - strategy_id: "default"
            capital_weight: 0.35

    Raises:
        ValueError: If budget weights sum to > 1.0
    """
    if "budgets" not in policy or not policy["budgets"]:
        # No explicit budgets or empty list - create default budget
        return [StrategyBudget(strategy_id="default", capital_weight=0.95)]

    budget_defs = policy["budgets"]
    if not isinstance(budget_defs, list):
        raise ValueError(f"Policy {source_path}: 'budgets' must be a list")

    budgets = []
    total_weight = 0.0

    for budget_def in budget_defs:
        if not isinstance(budget_def, dict):
            raise ValueError(f"Policy {source_path}: each budget must be a dict with strategy_id and capital_weight")

        strategy_id = budget_def.get("strategy_id")
        capital_weight = budget_def.get("capital_weight")

        if strategy_id is None:
            raise ValueError(f"Policy {source_path}: budget missing 'strategy_id'")
        if capital_weight is None:
            raise ValueError(f"Policy {source_path}: budget for '{strategy_id}' missing 'capital_weight'")

        # Validate weight is numeric and reasonable
        try:
            weight_float = float(capital_weight)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Policy {source_path}: capital_weight for '{strategy_id}' must be numeric: {e}")

        if not 0.0 <= weight_float <= 1.0:
            raise ValueError(
                f"Policy {source_path}: capital_weight for '{strategy_id}' must be in [0, 1], got {weight_float}"
            )

        total_weight += weight_float
        budgets.append(StrategyBudget(strategy_id=strategy_id, capital_weight=weight_float))

    # Validate total doesn't exceed 1.0
    if total_weight > 1.0:
        raise ValueError(
            f"Policy {source_path}: budget weights sum to {total_weight:.2f}, must be ≤ 1.0. "
            f"This leaves {(1.0 - total_weight) * 100:.1f}% unallocated."
        )

    return budgets


def _parse_sizing(policy: dict[str, Any], budgets: list[StrategyBudget], source_path: Path) -> dict[str, SizingConfig]:
    """Parse sizing configuration from policy."""
    if "sizing" not in policy:
        raise ValueError(f"Policy {source_path} missing 'sizing' section")

    sizing_section = policy["sizing"]
    algorithm = sizing_section.get("algorithm", "fixed_equity_pct")

    # Parse based on algorithm
    if algorithm == "fixed_equity_pct":
        fraction = sizing_section.get("fixed_equity_pct", 0.02)
        sizing_config = SizingConfig(
            model="fixed_fraction",
            fraction=Decimal(str(fraction)),
            min_quantity=1,
            lot_size=1,
        )
    elif algorithm == "equal_weighting":
        sizing_config = SizingConfig(
            model="equal_weight",
            fraction=Decimal("1.0"),  # Not used for equal_weight
            min_quantity=1,
            lot_size=1,
        )
    else:
        raise ValueError(f"Unsupported sizing algorithm: {algorithm}. Use 'fixed_equity_pct' or 'equal_weighting'")

    # Apply to all strategies (MVP: single config for all)
    sizing_dict = {}
    for budget in budgets:
        sizing_dict[budget.strategy_id] = sizing_config

    return sizing_dict


def _parse_concentration(policy: dict[str, Any], source_path: Path) -> ConcentrationLimit:
    """Parse concentration limit from policy."""
    if "limits" not in policy or "concentration" not in policy["limits"]:
        # Default: 10% max per position
        return ConcentrationLimit(max_position_pct=0.10)

    concentration = policy["limits"]["concentration"]
    max_position_pct = concentration.get("max_position_size_pct", 0.10)

    return ConcentrationLimit(max_position_pct=max_position_pct)


def _parse_leverage(policy: dict[str, Any], source_path: Path) -> LeverageLimit:
    """Parse leverage limit from policy."""
    if "limits" not in policy or "leverage" not in policy["limits"]:
        # Default: 1.0x gross, 1.0x net (no leverage)
        return LeverageLimit(max_gross=1.0, max_net=1.0)

    leverage = policy["limits"]["leverage"]
    max_gross = leverage.get("max_gross_leverage", 1.0)
    max_net = leverage.get("max_net_leverage", 1.0)

    return LeverageLimit(max_gross=max_gross, max_net=max_net)


def _parse_shorting(policy: dict[str, Any], source_path: Path) -> ShortingPolicy:
    """Parse shorting policy from policy."""
    if "limits" not in policy or "shorting" not in policy["limits"]:
        # Default: no shorting allowed (long-only)
        return ShortingPolicy(allow_short_positions=False)

    shorting = policy["limits"]["shorting"]
    allow_short = shorting.get("allow_short_positions", False)

    return ShortingPolicy(allow_short_positions=allow_short)


def _parse_cash_buffer(policy: dict[str, Any]) -> float:
    """Parse cash buffer percentage from policy."""
    if "cash_management" not in policy:
        return 0.02  # Default: 2% cash buffer

    cash_mgmt = policy["cash_management"]
    buffer: float = cash_mgmt.get("cash_buffer_pct", 0.02)
    return buffer
