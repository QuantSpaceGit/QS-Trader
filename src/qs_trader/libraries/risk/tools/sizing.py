"""Position sizing tools for risk management.

Pure functions for calculating order quantities from trading signals.
All functions are stateless and thread-safe.

Design Principles:
- Pure functions (no side effects, no global state)
- Decimal precision for financial calculations
- Clear separation of concerns (sizing logic only, no risk limits)
- Comprehensive error handling with detailed messages

Supported Models:
- Fixed Fraction: Allocate fixed % of capital per position

Thread Safety:
- All functions are pure and thread-safe
- No shared mutable state
"""

from decimal import Decimal
from typing import Literal


def calculate_fixed_fraction_size(
    *,
    allocated_capital: Decimal,
    signal_strength: float,
    current_price: Decimal,
    fraction: Decimal,
    lot_size: int = 1,
    min_quantity: int = 0,
) -> int:
    """Calculate position size using fixed-fraction capital allocation.

    Formula:
        target_notional = fraction * allocated_capital * |signal_strength|
        raw_quantity = target_notional / current_price
        quantity = floor(raw_quantity / lot_size) * lot_size

    Signal strength scales the position size:
    - strength = 0.0 → 0% of max position (no order)
    - strength = 0.5 → 50% of max position
    - strength = 1.0 → 100% of max position (full fraction)

    Args:
        allocated_capital: Capital allocated to this strategy (Decimal for precision)
        signal_strength: Signal confidence [-1, 1], where sign indicates direction
        current_price: Current market price for the symbol (Decimal for precision)
        fraction: Position sizing fraction [0, 1] (e.g., 0.02 = 2% of capital)
        lot_size: Minimum trading unit (default: 1 share). Must be positive.
        min_quantity: Minimum order quantity (default: 0). Must be non-negative.

    Returns:
        Order quantity in shares (always >= 0, never negative)
        Returns 0 if:
        - signal_strength is zero
        - calculated quantity < min_quantity
        - calculated quantity < lot_size
        - current_price is zero

    Raises:
        ValueError: If allocated_capital, current_price, or fraction is negative
        ValueError: If lot_size is not positive (must be >= 1)
        ValueError: If min_quantity is negative
        ValueError: If fraction not in [0, 1] range

    Examples:
        >>> # Full strength signal: 2% of $10k = $200 / $150 = 1.33 shares → 1 share
        >>> calculate_fixed_fraction_size(
        ...     allocated_capital=Decimal("10000"),
        ...     signal_strength=1.0,
        ...     current_price=Decimal("150"),
        ...     fraction=Decimal("0.02"),
        ...     lot_size=1
        ... )
        1

        >>> # Half strength signal: 1% of $10k = $100 / $150 = 0.67 shares → 0 shares
        >>> calculate_fixed_fraction_size(
        ...     allocated_capital=Decimal("10000"),
        ...     signal_strength=0.5,
        ...     current_price=Decimal("150"),
        ...     fraction=Decimal("0.02")
        ... )
        0

        >>> # Options with lot size = 100 contracts
        >>> calculate_fixed_fraction_size(
        ...     allocated_capital=Decimal("100000"),
        ...     signal_strength=1.0,
        ...     current_price=Decimal("450"),
        ...     fraction=Decimal("0.10"),
        ...     lot_size=100
        ... )
        2200  # 10% of $100k = $10k / $450 = 22.2 shares → 2200 shares (22 contracts)

        >>> # Negative signal strength (short signal) - absolute value used
        >>> calculate_fixed_fraction_size(
        ...     allocated_capital=Decimal("10000"),
        ...     signal_strength=-0.8,
        ...     current_price=Decimal("150"),
        ...     fraction=Decimal("0.02")
        ... )
        1  # abs(-0.8) = 0.8, same calculation as positive
    """
    # Input validation
    if allocated_capital < 0:
        raise ValueError(f"allocated_capital must be non-negative, got {allocated_capital}")

    if current_price < 0:
        raise ValueError(f"current_price must be non-negative, got {current_price}")

    if fraction < 0 or fraction > 1:
        raise ValueError(f"fraction must be in [0, 1], got {fraction}")

    if lot_size < 1:
        raise ValueError(f"lot_size must be positive (>= 1), got {lot_size}")

    if min_quantity < 0:
        raise ValueError(f"min_quantity must be non-negative, got {min_quantity}")

    # Early exit: zero strength or zero price
    if signal_strength == 0 or current_price == 0:
        return 0

    # Step 1: Calculate target notional using absolute strength
    # Use absolute value so direction doesn't affect size calculation
    abs_strength = abs(signal_strength)
    abs_strength_decimal = Decimal(str(abs_strength))
    target_notional = fraction * allocated_capital * abs_strength_decimal

    # Step 2: Convert notional to raw quantity
    raw_quantity = target_notional / current_price

    # Step 3: Round down to lot size
    # Formula: floor(raw_quantity / lot_size) * lot_size
    quantity = int(raw_quantity / lot_size) * lot_size

    # Step 4: Enforce minimum quantity
    if quantity < min_quantity:
        return 0

    return quantity


def calculate_equal_weight_size(
    *,
    allocated_capital: Decimal,
    num_positions: int,
    current_price: Decimal,
    lot_size: int = 1,
    min_quantity: int = 0,
) -> int:
    """Calculate position size for equal-weight allocation.

    Divides capital equally among all positions. Useful for simple
    diversification strategies.

    Formula:
        notional_per_position = allocated_capital / num_positions
        quantity = floor(notional_per_position / current_price / lot_size) * lot_size

    Args:
        allocated_capital: Total capital to allocate across positions
        num_positions: Number of positions to create
        current_price: Current market price for this symbol
        lot_size: Minimum trading unit (default: 1 share)
        min_quantity: Minimum order quantity (default: 0)

    Returns:
        Order quantity in shares (always >= 0)
        Returns 0 if calculated quantity < min_quantity

    Raises:
        ValueError: If allocated_capital or current_price is negative
        ValueError: If num_positions < 1
        ValueError: If lot_size < 1 or min_quantity < 0

    Example:
        >>> # $10k across 5 positions = $2k each / $150 = 13.33 shares → 13 shares
        >>> calculate_equal_weight_size(
        ...     allocated_capital=Decimal("10000"),
        ...     num_positions=5,
        ...     current_price=Decimal("150"),
        ...     lot_size=1
        ... )
        13
    """
    # Input validation
    if allocated_capital < 0:
        raise ValueError(f"allocated_capital must be non-negative, got {allocated_capital}")

    if current_price < 0:
        raise ValueError(f"current_price must be non-negative, got {current_price}")

    if num_positions < 1:
        raise ValueError(f"num_positions must be positive, got {num_positions}")

    if lot_size < 1:
        raise ValueError(f"lot_size must be positive (>= 1), got {lot_size}")

    if min_quantity < 0:
        raise ValueError(f"min_quantity must be non-negative, got {min_quantity}")

    # Early exit: zero price
    if current_price == 0:
        return 0

    # Step 1: Calculate notional per position
    notional_per_position = allocated_capital / num_positions

    # Step 2: Convert to quantity
    raw_quantity = notional_per_position / current_price

    # Step 3: Round down to lot size
    quantity = int(raw_quantity / lot_size) * lot_size

    # Step 4: Enforce minimum quantity
    if quantity < min_quantity:
        return 0

    return quantity


def validate_sizing_inputs(
    *,
    model: Literal["fixed_fraction", "equal_weight"],
    allocated_capital: Decimal,
    current_price: Decimal,
    fraction: Decimal | None = None,
    num_positions: int | None = None,
) -> None:
    """Validate common sizing inputs before calculation.

    Utility function to check inputs are valid before calling sizing functions.
    Raises descriptive errors for invalid inputs.

    Args:
        model: Sizing model to use
        allocated_capital: Capital allocated to this strategy
        current_price: Current market price
        fraction: Required for "fixed_fraction" model
        num_positions: Required for "equal_weight" model

    Raises:
        ValueError: If any input is invalid with detailed message
    """
    if model not in ("fixed_fraction", "equal_weight"):
        raise ValueError(f"model must be 'fixed_fraction' or 'equal_weight', got {model}")

    if allocated_capital < 0:
        raise ValueError(f"allocated_capital must be non-negative, got {allocated_capital}")

    if current_price < 0:
        raise ValueError(f"current_price must be non-negative, got {current_price}")

    if model == "fixed_fraction":
        if fraction is None:
            raise ValueError("fraction is required for fixed_fraction model")
        if fraction < 0 or fraction > 1:
            raise ValueError(f"fraction must be in [0, 1], got {fraction}")

    if model == "equal_weight":
        if num_positions is None:
            raise ValueError("num_positions is required for equal_weight model")
        if num_positions < 1:
            raise ValueError(f"num_positions must be positive, got {num_positions}")
