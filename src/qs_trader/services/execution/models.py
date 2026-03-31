"""Order and Fill models for execution service.

This module defines the core data models for order execution:
- Order: Mutable order with state tracking
- Fill: Immutable execution result
- Enums: OrderState, OrderSide, OrderType, TimeInForce
- FillDecision: Internal decision data for fill evaluation
"""

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Literal
from uuid import uuid4


class OrderState(str, Enum):
    """Order lifecycle states.

    State Transitions:
        PENDING → SUBMITTED → PARTIAL → FILLED
        PENDING → CANCELLED
        SUBMITTED → CANCELLED
        PARTIAL → CANCELLED
        PARTIAL → EXPIRED
        Any → REJECTED
    """

    PENDING = "pending"  # Order created, not yet submitted
    SUBMITTED = "submitted"  # Order submitted to market
    PARTIAL = "partial"  # Partially filled
    FILLED = "filled"  # Completely filled
    CANCELLED = "cancelled"  # Cancelled by user or system
    REJECTED = "rejected"  # Rejected by market
    EXPIRED = "expired"  # Expired (time/bars limit reached)


class OrderSide(str, Enum):
    """Order side (buy or sell)."""

    BUY = "buy"
    SELL = "sell"


class OrderType(str, Enum):
    """Order type.

    MARKET: Execute at next available price (next bar open)
    LIMIT: Execute only at limit price or better
    STOP: Trigger at stop price, then execute as market
    MARKET_ON_CLOSE: Execute at current bar's close price
    """

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    MARKET_ON_CLOSE = "moc"


class TimeInForce(str, Enum):
    """Time-in-force instruction.

    DAY: Good for current trading day only
    GTC: Good till cancelled
    IOC: Immediate or cancel (fill available quantity immediately)
    FOK: Fill or kill (fill entire quantity or cancel)
    """

    DAY = "day"
    GTC = "gtc"
    IOC = "ioc"
    FOK = "fok"


@dataclass
class Order:
    """Mutable order with state tracking.

    Tracks order lifecycle from creation through fills to completion.
    State changes are recorded with timestamps.

    Attributes:
        order_id: Unique identifier (auto-generated UUID)
        symbol: Ticker symbol
        side: Buy or sell
        quantity: Total shares requested (positive)
        order_type: Market, limit, stop, or MOC
        state: Current order state
        created_at: Order creation timestamp
        time_in_force: Time-in-force instruction
        limit_price: For limit orders (must touch or better)
        stop_price: For stop orders (trigger price)
        filled_quantity: Shares filled so far
        avg_fill_price: Weighted average fill price
        last_updated: Last state change timestamp
        bars_queued: Bars order has been queued (for market orders)

    Examples:
        >>> # Market order
        >>> order = Order(
        ...     symbol="AAPL",
        ...     side=OrderSide.BUY,
        ...     quantity=Decimal("100"),
        ...     order_type=OrderType.MARKET,
        ...     created_at=datetime.now()
        ... )

        >>> # Limit order
        >>> order = Order(
        ...     symbol="AAPL",
        ...     side=OrderSide.SELL,
        ...     quantity=Decimal("100"),
        ...     order_type=OrderType.LIMIT,
        ...     limit_price=Decimal("150.00"),
        ...     created_at=datetime.now()
        ... )
    """

    symbol: str
    side: OrderSide
    quantity: Decimal
    order_type: OrderType
    created_at: datetime

    # Auto-generated or defaulted fields
    order_id: str = field(default_factory=lambda: str(uuid4()))
    state: OrderState = OrderState.PENDING
    time_in_force: TimeInForce = TimeInForce.DAY
    limit_price: Decimal | None = None
    stop_price: Decimal | None = None
    filled_quantity: Decimal = field(default_factory=lambda: Decimal("0"))
    avg_fill_price: Decimal = field(default_factory=lambda: Decimal("0"))
    last_updated: datetime | None = None
    bars_queued: int = 0
    submitted_date: datetime | None = None  # Date order was submitted (for DAY expiry)
    strategy_id: str | None = None  # Strategy attribution for position tracking

    def __post_init__(self) -> None:
        """Validate order on creation."""
        # Validate quantity
        if self.quantity <= 0:
            raise ValueError(f"Order quantity must be positive, got {self.quantity}")

        # Validate limit price for limit orders
        if self.order_type == OrderType.LIMIT and self.limit_price is None:
            raise ValueError("Limit orders require limit_price")

        if self.limit_price is not None and self.limit_price <= 0:
            raise ValueError(f"Limit price must be positive, got {self.limit_price}")

        # Validate stop price for stop orders
        if self.order_type == OrderType.STOP and self.stop_price is None:
            raise ValueError("Stop orders require stop_price")

        if self.stop_price is not None and self.stop_price <= 0:
            raise ValueError(f"Stop price must be positive, got {self.stop_price}")

        # Set last_updated to created_at if not set
        if self.last_updated is None:
            self.last_updated = self.created_at

        # Validate filled_quantity
        if self.filled_quantity < 0:
            raise ValueError(f"Filled quantity cannot be negative, got {self.filled_quantity}")

        if self.filled_quantity > self.quantity:
            raise ValueError(f"Filled quantity {self.filled_quantity} exceeds order quantity {self.quantity}")

    @classmethod
    def market_order(
        cls,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_id: str | None = None,
        time_in_force: TimeInForce = TimeInForce.DAY,
        created_at: datetime | None = None,
    ) -> "Order":
        """Create a market order.

        Args:
            symbol: Ticker symbol
            side: Buy or sell
            quantity: Shares to trade
            order_id: Optional order ID (auto-generated if None)
            time_in_force: Time-in-force instruction
            created_at: Creation timestamp (defaults to now)

        Returns:
            Market order instance

        Example:
            >>> order = Order.market_order(
            ...     symbol="AAPL",
            ...     side=OrderSide.BUY,
            ...     quantity=Decimal("100")
            ... )
        """
        return cls(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.MARKET,
            created_at=created_at or datetime.now(),
            state=OrderState.PENDING,
            time_in_force=time_in_force,
            order_id=order_id or f"{symbol}_{datetime.now().isoformat()}",
        )

    @classmethod
    def limit_order(
        cls,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        limit_price: Decimal,
        order_id: str | None = None,
        time_in_force: TimeInForce = TimeInForce.DAY,
        created_at: datetime | None = None,
    ) -> "Order":
        """Create a limit order.

        Args:
            symbol: Ticker symbol
            side: Buy or sell
            quantity: Shares to trade
            limit_price: Maximum buy price or minimum sell price
            order_id: Optional order ID (auto-generated if None)
            time_in_force: Time-in-force instruction
            created_at: Creation timestamp (defaults to now)

        Returns:
            Limit order instance

        Example:
            >>> order = Order.limit_order(
            ...     symbol="AAPL",
            ...     side=OrderSide.BUY,
            ...     quantity=Decimal("100"),
            ...     limit_price=Decimal("150.00")
            ... )
        """
        return cls(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.LIMIT,
            created_at=created_at or datetime.now(),
            state=OrderState.PENDING,
            time_in_force=time_in_force,
            limit_price=limit_price,
            order_id=order_id or f"{symbol}_{datetime.now().isoformat()}",
        )

    @classmethod
    def stop_order(
        cls,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        stop_price: Decimal,
        order_id: str | None = None,
        time_in_force: TimeInForce = TimeInForce.DAY,
        created_at: datetime | None = None,
    ) -> "Order":
        """Create a stop order.

        Args:
            symbol: Ticker symbol
            side: Buy or sell
            quantity: Shares to trade
            stop_price: Trigger price
            order_id: Optional order ID (auto-generated if None)
            time_in_force: Time-in-force instruction
            created_at: Creation timestamp (defaults to now)

        Returns:
            Stop order instance

        Example:
            >>> order = Order.stop_order(
            ...     symbol="AAPL",
            ...     side=OrderSide.SELL,
            ...     quantity=Decimal("100"),
            ...     stop_price=Decimal("145.00")
            ... )
        """
        return cls(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.STOP,
            created_at=created_at or datetime.now(),
            state=OrderState.PENDING,
            time_in_force=time_in_force,
            stop_price=stop_price,
            order_id=order_id or f"{symbol}_{datetime.now().isoformat()}",
        )

    @classmethod
    def moc_order(
        cls,
        symbol: str,
        side: OrderSide,
        quantity: Decimal,
        order_id: str | None = None,
        time_in_force: TimeInForce = TimeInForce.DAY,
        created_at: datetime | None = None,
    ) -> "Order":
        """Create a market-on-close order.

        Args:
            symbol: Ticker symbol
            side: Buy or sell
            quantity: Shares to trade
            order_id: Optional order ID (auto-generated if None)
            time_in_force: Time-in-force instruction
            created_at: Creation timestamp (defaults to now)

        Returns:
            MOC order instance

        Example:
            >>> order = Order.moc_order(
            ...     symbol="AAPL",
            ...     side=OrderSide.BUY,
            ...     quantity=Decimal("100")
            ... )
        """
        return cls(
            symbol=symbol,
            side=side,
            quantity=quantity,
            order_type=OrderType.MARKET_ON_CLOSE,
            created_at=created_at or datetime.now(),
            state=OrderState.PENDING,
            time_in_force=time_in_force,
            order_id=order_id or f"{symbol}_{datetime.now().isoformat()}",
        )

    @property
    def remaining_quantity(self) -> Decimal:
        """Unfilled quantity."""
        return self.quantity - self.filled_quantity

    @property
    def is_complete(self) -> bool:
        """Check if order is in terminal state."""
        return self.state in (OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED, OrderState.EXPIRED)

    @property
    def is_active(self) -> bool:
        """Check if order can still be filled."""
        return self.state in (OrderState.PENDING, OrderState.SUBMITTED, OrderState.PARTIAL)

    def update_fill(self, fill_qty: Decimal, fill_price: Decimal, timestamp: datetime) -> None:
        """Update order state after a fill.

        Calculates weighted average fill price and updates state.
        Automatically transitions state based on remaining quantity.

        Args:
            fill_qty: Quantity filled in this execution
            fill_price: Price of this fill
            timestamp: When fill occurred

        Raises:
            ValueError: If fill_qty exceeds remaining quantity
            ValueError: If order is already complete
            ValueError: If fill_qty or fill_price invalid

        Example:
            >>> order.update_fill(
            ...     fill_qty=Decimal("50"),
            ...     fill_price=Decimal("150.00"),
            ...     timestamp=datetime.now()
            ... )
        """
        if self.is_complete:
            raise ValueError(f"Cannot fill order in state: {self.state}")

        if fill_qty <= 0:
            raise ValueError(f"Fill quantity must be positive, got {fill_qty}")

        if fill_price <= 0:
            raise ValueError(f"Fill price must be positive, got {fill_price}")

        if fill_qty > self.remaining_quantity:
            raise ValueError(f"Fill quantity {fill_qty} exceeds remaining {self.remaining_quantity}")

        # Update weighted average fill price
        if self.filled_quantity == 0:
            self.avg_fill_price = fill_price
        else:
            total_filled_value = self.avg_fill_price * self.filled_quantity
            new_filled_value = fill_price * fill_qty
            self.filled_quantity += fill_qty
            self.avg_fill_price = (total_filled_value + new_filled_value) / self.filled_quantity

        # Update filled quantity (do this after avg price calculation if filled_quantity was 0)
        if self.filled_quantity < fill_qty:  # First fill
            self.filled_quantity = fill_qty

        # Update state
        if self.filled_quantity >= self.quantity:
            self.state = OrderState.FILLED
        else:
            self.state = OrderState.PARTIAL

        self.last_updated = timestamp

    def cancel(self, timestamp: datetime) -> None:
        """Cancel the order.

        Args:
            timestamp: When cancellation occurred

        Raises:
            ValueError: If order already filled or rejected

        Example:
            >>> order.cancel(datetime.now())
        """
        if self.state == OrderState.FILLED:
            raise ValueError("Cannot cancel filled order")

        if self.state == OrderState.REJECTED:
            raise ValueError("Cannot cancel rejected order")

        self.state = OrderState.CANCELLED
        self.last_updated = timestamp

    def reject(self, timestamp: datetime) -> None:
        """Reject the order.

        Args:
            timestamp: When rejection occurred

        Example:
            >>> order.reject(datetime.now())
        """
        self.state = OrderState.REJECTED
        self.last_updated = timestamp

    def expire(self, timestamp: datetime) -> None:
        """Expire the order.

        Args:
            timestamp: When expiration occurred

        Raises:
            ValueError: If order already filled

        Example:
            >>> order.expire(datetime.now())
        """
        if self.state == OrderState.FILLED:
            raise ValueError("Cannot expire filled order")

        self.state = OrderState.EXPIRED
        self.last_updated = timestamp

    def submit(self, timestamp: datetime) -> None:
        """Mark order as submitted to market.

        Args:
            timestamp: When submission occurred

        Raises:
            ValueError: If order not in pending state

        Example:
            >>> order.submit(datetime.now())
        """
        if self.state != OrderState.PENDING:
            raise ValueError(f"Cannot submit order in state: {self.state}")

        self.state = OrderState.SUBMITTED
        self.last_updated = timestamp
        self.submitted_date = timestamp  # Track submission date for DAY expiry


@dataclass(frozen=True)
class Fill:
    """Immutable execution result.

    Maps exactly to IPortfolioService.apply_fill() signature.
    Once created, cannot be modified.

    Attributes:
        fill_id: Unique identifier (auto-generated UUID)
        order_id: Originating order ID
        timestamp: When fill occurred
        symbol: Ticker symbol
        side: Buy or sell (as string for Portfolio compatibility)
        quantity: Shares filled (positive)
        price: Execution price per share (includes slippage)
        commission: Total commission charged (calculated by ExecutionService)
        slippage_bps: Slippage applied in basis points (for audit trail)

    Note:
        Commission and slippage are calculated by ExecutionService based on
        system configuration. Portfolio records these values but does not
        recalculate them.

    Example:
        >>> fill = Fill(
        ...     order_id="order_123",
        ...     timestamp=datetime.now(),
        ...     symbol="AAPL",
        ...     side="buy",
        ...     quantity=Decimal("100"),
        ...     price=Decimal("150.00"),
        ...     commission=Decimal("1.00"),
        ...     slippage_bps=5
        ... )
    """

    order_id: str
    timestamp: datetime
    symbol: str
    side: Literal["buy", "sell"]  # String for Portfolio compatibility
    quantity: Decimal
    price: Decimal
    commission: Decimal = Decimal("0")
    slippage_bps: int = 0  # Basis points of slippage applied
    fill_id: str = field(default_factory=lambda: str(uuid4()))
    strategy_id: str | None = None  # Strategy attribution for position tracking

    def __post_init__(self) -> None:
        """Validate fill on creation."""
        if self.quantity <= 0:
            raise ValueError(f"Fill quantity must be positive, got {self.quantity}")

        if self.price <= 0:
            raise ValueError(f"Fill price must be positive, got {self.price}")

        if self.commission < 0:
            raise ValueError(f"Commission cannot be negative, got {self.commission}")

        if self.side not in ("buy", "sell"):
            raise ValueError(f"Side must be 'buy' or 'sell', got {self.side}")

    @property
    def gross_value(self) -> Decimal:
        """Total value before commission."""
        return self.quantity * self.price

    @property
    def net_value(self) -> Decimal:
        """Total value including commission."""
        return self.gross_value + self.commission


@dataclass
class FillDecision:
    """Internal decision data for fill evaluation.

    Used by FillPolicy to communicate fill decisions to ExecutionService.
    Not part of public API.

    Attributes:
        should_fill: Whether order should fill
        fill_price: Price to fill at (if should_fill=True)
        fill_quantity: Quantity to fill (supports partial fills)
        slippage_bps: Slippage applied in basis points (for audit trail)
        reason: Human-readable explanation
        queue_for_next_bar: If True, re-evaluate on next bar
        should_expire: If True, order should be expired
        should_cancel: If True, order should be cancelled (for IOC/FOK)

    Example:
        >>> # Order can fill
        >>> decision = FillDecision(
        ...     should_fill=True,
        ...     fill_price=Decimal("150.00"),
        ...     fill_quantity=Decimal("100"),
        ...     slippage_bps=5,
        ...     reason="Market order at next bar open"
        ... )

        >>> # Order needs to wait
        >>> decision = FillDecision(
        ...     should_fill=False,
        ...     reason="Market order queued for 1 bar",
        ...     queue_for_next_bar=True
        ... )

        >>> # Order should expire
        >>> decision = FillDecision(
        ...     should_fill=False,
        ...     reason="Order exceeded queue bars limit",
        ...     should_expire=True
        ... )
    """

    should_fill: bool
    reason: str
    fill_price: Decimal = field(default_factory=lambda: Decimal("0"))
    fill_quantity: Decimal = field(default_factory=lambda: Decimal("0"))
    slippage_bps: int = 0  # Basis points of slippage applied
    queue_for_next_bar: bool = False
    should_expire: bool = False
    should_cancel: bool = False

    def __post_init__(self) -> None:
        """Validate fill decision."""
        if self.should_fill:
            if self.fill_price <= 0:
                raise ValueError(f"Fill price must be positive when should_fill=True, got {self.fill_price}")

            if self.fill_quantity <= 0:
                raise ValueError(f"Fill quantity must be positive when should_fill=True, got {self.fill_quantity}")
