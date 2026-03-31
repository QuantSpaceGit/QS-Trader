"""Risk limit checking tools.

Pure functions for validating proposed orders against risk limits.
All functions are stateless and thread-safe.

Design Principles:
- Pure functions (no side effects, no global state)
- Decimal precision for financial calculations
- Clear violation messages for audit trail
- Comprehensive error handling

Supported Limits:
- Concentration: Per-symbol exposure as % of equity
- Leverage: Portfolio gross/net exposure as % of equity

Thread Safety:
- All functions are pure and thread-safe
- No shared mutable state
"""

from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class Position:
    """Position snapshot for risk calculations.

    Immutable representation of a current position.

    Attributes:
        symbol: Instrument identifier
        quantity: Signed quantity (positive = long, negative = short)
        market_value: Current market value (quantity * price)
    """

    symbol: str
    quantity: int
    market_value: Decimal


@dataclass(frozen=True)
class ProposedOrder:
    """Proposed order for limit checking.

    Minimal order representation needed for risk calculations.

    Attributes:
        symbol: Instrument identifier
        side: Order side - "buy"/"BUY" or "sell"/"SELL" (case-insensitive)
        quantity: Order quantity (always positive)
    """

    symbol: str
    side: str  # "buy"/"BUY" or "sell"/"SELL" (case-insensitive)
    quantity: int


@dataclass(frozen=True)
class LimitViolation:
    """Details about a limit violation.

    Immutable record of why an order was rejected due to limit breach.
    Used in audit trails and rejection reasons.

    Attributes:
        limit_type: "concentration" or "leverage"
        symbol: Symbol that violated (or "PORTFOLIO" for leverage)
        proposed_exposure: Proposed exposure in USD
        proposed_pct: Proposed exposure as % of equity
        limit_pct: Configured limit as % of equity
        message: Human-readable violation message
    """

    limit_type: str
    symbol: str
    proposed_exposure: Decimal
    proposed_pct: float
    limit_pct: float
    message: str


def check_concentration_limit(
    *,
    order: ProposedOrder,
    current_positions: list[Position],
    equity: Decimal,
    current_price: Decimal,
    max_position_pct: float,
) -> LimitViolation | None:
    """Check if order would violate concentration limit for its symbol.

    Concentration limit restricts per-symbol exposure as a percentage of equity.
    This prevents over-concentration in a single security.

    Calculation:
    1. Find current position in the symbol (if any)
    2. Calculate proposed position after order execution
    3. Calculate proposed exposure: |proposed_qty| * current_price
    4. Calculate proposed percentage: proposed_exposure / equity
    5. Compare against max_position_pct

    Args:
        order: Proposed order to check
        current_positions: List of current positions
        equity: Current portfolio equity
        current_price: Current market price for order.symbol
        max_position_pct: Maximum position size as % of equity (e.g., 0.10 = 10%)

    Returns:
        LimitViolation if limit exceeded, None if within limit

    Raises:
        ValueError: If equity is negative
        ValueError: If current_price is negative
        ValueError: If max_position_pct not in (0, 1]

    Examples:
        >>> order = ProposedOrder(symbol="AAPL", side="BUY", quantity=100)
        >>> positions = [Position(symbol="AAPL", quantity=50, market_value=Decimal("7500"))]
        >>> violation = check_concentration_limit(
        ...     order=order,
        ...     current_positions=positions,
        ...     equity=Decimal("100000"),
        ...     current_price=Decimal("150"),
        ...     max_position_pct=0.10
        ... )
        >>> if violation:
        ...     print(violation.message)
        Concentration limit exceeded for AAPL: 22.50% > 10.00%

        >>> # No violation: proposed position within limit
        >>> order2 = ProposedOrder(symbol="AAPL", side="BUY", quantity=10)
        >>> violation2 = check_concentration_limit(
        ...     order=order2,
        ...     current_positions=positions,
        ...     equity=Decimal("100000"),
        ...     current_price=Decimal("150"),
        ...     max_position_pct=0.10
        ... )
        >>> violation2 is None
        True
    """
    # Input validation
    if equity < 0:
        raise ValueError(f"equity must be non-negative, got {equity}")

    if current_price < 0:
        raise ValueError(f"current_price must be non-negative, got {current_price}")

    if not (0 < max_position_pct <= 1):
        raise ValueError(f"max_position_pct must be in (0, 1], got {max_position_pct}")

    # Aggregate current position across all strategies for this symbol
    # Multiple strategies may hold the same symbol, need to sum quantities
    current_qty = 0
    for pos in current_positions:
        if pos.symbol == order.symbol:
            current_qty += pos.quantity  # Accumulate, don't overwrite

    # Calculate proposed position after order execution
    # Normalize to uppercase for comparison (accepts "buy"/"BUY", "sell"/"SELL")
    side_upper = order.side.upper()
    if side_upper == "BUY":
        proposed_qty = current_qty + order.quantity
    elif side_upper == "SELL":
        proposed_qty = current_qty - order.quantity
    else:
        raise ValueError(f"Invalid order side: {order.side}. Must be 'buy'/'BUY' or 'sell'/'SELL'")

    # Calculate proposed exposure (absolute value for long or short)
    proposed_exposure = abs(proposed_qty) * current_price

    # Calculate proposed percentage of equity
    if equity == 0:
        # If equity is 0, any position is a violation
        if proposed_qty != 0:
            return LimitViolation(
                limit_type="concentration",
                symbol=order.symbol,
                proposed_exposure=proposed_exposure,
                proposed_pct=float("inf"),
                limit_pct=max_position_pct,
                message=(f"Concentration limit exceeded for {order.symbol}: proposed position with zero equity"),
            )
        return None

    proposed_pct = float(proposed_exposure / equity)

    # Check limit
    if proposed_pct > max_position_pct:
        return LimitViolation(
            limit_type="concentration",
            symbol=order.symbol,
            proposed_exposure=proposed_exposure,
            proposed_pct=proposed_pct,
            limit_pct=max_position_pct,
            message=(f"Concentration limit exceeded for {order.symbol}: {proposed_pct:.2%} > {max_position_pct:.2%}"),
        )

    return None


def check_leverage_limits(
    *,
    order: ProposedOrder,
    current_positions: list[Position],
    equity: Decimal,
    current_price: Decimal,
    max_gross_leverage: float,
    max_net_leverage: float,
) -> LimitViolation | None:
    """Check if order would violate leverage limits (gross or net).

    Leverage limits restrict portfolio-wide exposure:
    - Gross leverage: sum of absolute exposures (long + short)
    - Net leverage: net exposure (long - short)

    Calculation:
    1. Calculate current gross and net exposure
    2. Simulate order execution (add/remove position)
    3. Calculate proposed gross and net exposure
    4. Check both against configured limits

    Args:
        order: Proposed order to check
        current_positions: List of current positions
        equity: Current portfolio equity
        current_price: Current market price for order.symbol
        max_gross_leverage: Maximum gross leverage (e.g., 2.0 = 200%)
        max_net_leverage: Maximum net leverage (e.g., 1.0 = 100%)

    Returns:
        LimitViolation if any limit exceeded, None if within limits
        If both gross and net exceeded, returns gross violation (more severe)

    Raises:
        ValueError: If equity or current_price is negative
        ValueError: If max_gross_leverage or max_net_leverage <= 0

    Examples:
        >>> order = ProposedOrder(symbol="AAPL", side="BUY", quantity=1000)
        >>> positions = [
        ...     Position(symbol="SPY", quantity=500, market_value=Decimal("225000")),
        ...     Position(symbol="AAPL", quantity=100, market_value=Decimal("15000")),
        ... ]
        >>> violation = check_leverage_limits(
        ...     order=order,
        ...     current_positions=positions,
        ...     equity=Decimal("100000"),
        ...     current_price=Decimal("150"),
        ...     max_gross_leverage=2.0,
        ...     max_net_leverage=1.0
        ... )
        >>> if violation:
        ...     print(violation.message)
        Gross leverage limit exceeded: 3.90 > 2.00
    """
    # Input validation
    if equity < 0:
        raise ValueError(f"equity must be non-negative, got {equity}")

    if current_price < 0:
        raise ValueError(f"current_price must be non-negative, got {current_price}")

    if max_gross_leverage <= 0:
        raise ValueError(f"max_gross_leverage must be positive, got {max_gross_leverage}")

    if max_net_leverage <= 0:
        raise ValueError(f"max_net_leverage must be positive, got {max_net_leverage}")

    # Step 1: Calculate current exposures (excluding the order's symbol)
    # Aggregate quantities/exposures across strategies for the same symbol
    current_gross = Decimal("0")
    current_net = Decimal("0")
    current_qty_in_symbol = 0

    for pos in current_positions:
        if pos.symbol == order.symbol:
            # Accumulate quantity across all strategies holding this symbol
            current_qty_in_symbol += pos.quantity
        else:
            # Add to current exposures (for other symbols)
            current_gross += abs(pos.market_value)
            current_net += pos.market_value  # signed (long=+, short=-)

    # Step 2: Calculate proposed position in order's symbol
    # Normalize to uppercase for comparison (accepts "buy"/"BUY", "sell"/"SELL")
    side_upper = order.side.upper()
    if side_upper == "BUY":
        proposed_qty = current_qty_in_symbol + order.quantity
    elif side_upper == "SELL":
        proposed_qty = current_qty_in_symbol - order.quantity
    else:
        raise ValueError(f"Invalid order side: {order.side}. Must be 'buy'/'BUY' or 'sell'/'SELL'")

    # Calculate proposed exposure for order's symbol
    proposed_symbol_exposure = Decimal(str(proposed_qty)) * current_price

    # Step 3: Calculate proposed portfolio exposures
    proposed_gross = current_gross + abs(proposed_symbol_exposure)
    proposed_net = current_net + proposed_symbol_exposure

    # Step 4: Calculate proposed leverage ratios
    if equity == 0:
        # If equity is 0, any position is a violation
        if proposed_gross > 0:
            return LimitViolation(
                limit_type="leverage",
                symbol="PORTFOLIO",
                proposed_exposure=proposed_gross,
                proposed_pct=float("inf"),
                limit_pct=max_gross_leverage,
                message="Leverage limit exceeded: proposed position with zero equity",
            )
        return None

    proposed_gross_leverage = float(proposed_gross / equity)
    proposed_net_leverage = float(abs(proposed_net) / equity)

    # Step 5: Check limits (gross first, then net)
    if proposed_gross_leverage > max_gross_leverage:
        return LimitViolation(
            limit_type="leverage",
            symbol="PORTFOLIO",
            proposed_exposure=proposed_gross,
            proposed_pct=proposed_gross_leverage,
            limit_pct=max_gross_leverage,
            message=(f"Gross leverage limit exceeded: {proposed_gross_leverage:.2f} > {max_gross_leverage:.2f}"),
        )

    if proposed_net_leverage > max_net_leverage:
        return LimitViolation(
            limit_type="leverage",
            symbol="PORTFOLIO",
            proposed_exposure=abs(proposed_net),
            proposed_pct=proposed_net_leverage,
            limit_pct=max_net_leverage,
            message=(f"Net leverage limit exceeded: {proposed_net_leverage:.2f} > {max_net_leverage:.2f}"),
        )

    return None


def check_all_limits(
    *,
    order: ProposedOrder,
    current_positions: list[Position],
    equity: Decimal,
    current_price: Decimal,
    max_position_pct: float | None = None,
    max_gross_leverage: float | None = None,
    max_net_leverage: float | None = None,
) -> list[LimitViolation]:
    """Check all configured limits for an order.

    Convenience function that checks both concentration and leverage limits.
    Returns all violations found (empty list if no violations).

    Args:
        order: Proposed order to check
        current_positions: List of current positions
        equity: Current portfolio equity
        current_price: Current market price for order.symbol
        max_position_pct: Max concentration (None = skip check)
        max_gross_leverage: Max gross leverage (None = skip check)
        max_net_leverage: Max net leverage (None = skip check)

    Returns:
        List of all violations (empty if order passes all limits)

    Raises:
        ValueError: If both max_gross_leverage and max_net_leverage provided but
                   max_net_leverage is specified without max_gross_leverage

    Examples:
        >>> order = ProposedOrder(symbol="AAPL", side="BUY", quantity=100)
        >>> violations = check_all_limits(
        ...     order=order,
        ...     current_positions=[],
        ...     equity=Decimal("100000"),
        ...     current_price=Decimal("150"),
        ...     max_position_pct=0.10,
        ...     max_gross_leverage=2.0,
        ...     max_net_leverage=1.0
        ... )
        >>> for v in violations:
        ...     print(f"REJECT: {v.message}")
    """
    violations: list[LimitViolation] = []

    # Check concentration limit if configured
    if max_position_pct is not None:
        violation = check_concentration_limit(
            order=order,
            current_positions=current_positions,
            equity=equity,
            current_price=current_price,
            max_position_pct=max_position_pct,
        )
        if violation:
            violations.append(violation)

    # Check leverage limits if configured
    if max_gross_leverage is not None and max_net_leverage is not None:
        violation = check_leverage_limits(
            order=order,
            current_positions=current_positions,
            equity=equity,
            current_price=current_price,
            max_gross_leverage=max_gross_leverage,
            max_net_leverage=max_net_leverage,
        )
        if violation:
            violations.append(violation)
    elif max_gross_leverage is not None or max_net_leverage is not None:
        # Both must be provided together
        raise ValueError("max_gross_leverage and max_net_leverage must both be provided or both be None")

    return violations
