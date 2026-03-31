"""Execution service interface (Protocol).

Defines the contract that all execution service implementations must satisfy.
Enables dependency injection and makes the service independently testable.
"""

from typing import Protocol

from qs_trader.services.data.models import Bar
from qs_trader.services.execution.models import Fill, Order


class IExecutionService(Protocol):
    """Execution service interface for order simulation.

    Simulates realistic order execution in backtesting environment.
    Returns Fill objects without modifying Portfolio state.

    Core responsibilities:
    - Accept and validate orders
    - Simulate realistic fills based on bar data
    - Calculate commissions and slippage
    - Handle partial fills based on volume
    - Track order state transitions
    - Support multiple order types

    NOT responsible for:
    - Applying fills to portfolio (Portfolio does this)
    - Making trading decisions (Strategy does this)
    - Data loading (DataService does this)

    Example:
        >>> execution: IExecutionService = ExecutionService(config)
        >>> order = execution.submit_order(
        ...     symbol="AAPL",
        ...     side="buy",
        ...     quantity=Decimal("100"),
        ...     order_type=OrderType.MARKET
        ... )
        >>>
        >>> # Later, when bar arrives
        >>> fills = execution.on_bar(bar)
        >>> for fill in fills:
        ...     portfolio.apply_fill(
        ...         fill_id=fill.fill_id,
        ...         timestamp=fill.timestamp,
        ...         symbol=fill.symbol,
        ...         side=fill.side,
        ...         quantity=fill.quantity,
        ...         price=fill.price,
        ...         commission=fill.commission
        ...     )
    """

    def submit_order(self, order: Order) -> str:
        """Submit a new order for execution.

        Order is validated and tracked. Returns order_id for reference.

        Args:
            order: Order object with all parameters

        Returns:
            order_id: Unique identifier for tracking

        Raises:
            ValueError: If order validation fails
            ValueError: If duplicate order_id

        Example:
            >>> order = Order(
            ...     symbol="AAPL",
            ...     side=OrderSide.BUY,
            ...     quantity=Decimal("100"),
            ...     order_type=OrderType.MARKET,
            ...     created_at=datetime.now()
            ... )
            >>> order_id = execution.submit_order(order)
        """
        ...

    def on_bar(self, bar: Bar) -> list[Fill]:
        """Process bar and generate fills for eligible orders.

        Fill rules by order type:

        Market Orders:
            - Fill at NEXT bar's open price
            - Queue for 1 bar, then fill
            - Apply slippage

        Limit Orders:
            - Buy: Fill if bar.low <= limit_price
            - Sell: Fill if bar.high >= limit_price
            - Conservative: Fill at min(limit, close) for buy, max(limit, close) for sell
            - Require price to "touch" limit

        Stop Orders:
            - Buy stop: Trigger if bar.high >= stop_price
            - Sell stop: Trigger if bar.low <= stop_price
            - Once triggered, fill at max(stop, close) + slippage for buy
            - Fill at min(stop, close) - slippage for sell

        MOC (Market-on-Close):
            - Fill at current bar's close price
            - Apply slippage
            - Execute at end of trading day

        Partial Fills:
            - Respect max_participation_rate (e.g., 10% of bar volume)
            - If order size > max_volume, create partial fill
            - Keep order active for next bar

        Args:
            bar: Price/volume data for single symbol

        Returns:
            List of Fill objects (empty if no fills)

        Example:
            >>> bar = Bar(
            ...     trade_datetime=datetime(2020, 1, 2, 9, 30),
            ...     open=150.0,
            ...     high=151.0,
            ...     low=149.5,
            ...     close=150.5,
            ...     volume=1000000
            ... )
            >>> fills = execution.on_bar(bar)
        """
        ...

    def cancel_order(self, order_id: str) -> bool:
        """Cancel pending order.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if cancelled, False if not found or already complete

        Example:
            >>> execution.cancel_order(order.order_id)
        """
        ...

    def get_order(self, order_id: str) -> Order | None:
        """Retrieve order by ID.

        Args:
            order_id: Order identifier

        Returns:
            Order object or None if not found
        """
        ...

    def get_pending_orders(self, symbol: str | None = None) -> list[Order]:
        """Get all pending/partially filled orders.

        Args:
            symbol: Filter by symbol (optional)

        Returns:
            List of active orders

        Example:
            >>> pending = execution.get_pending_orders(symbol="AAPL")
            >>> print(f"Active AAPL orders: {len(pending)}")
        """
        ...

    def get_filled_orders(self, symbol: str | None = None) -> list[Order]:
        """Get all filled orders.

        Args:
            symbol: Filter by symbol (optional)

        Returns:
            List of filled orders

        Example:
            >>> filled = execution.get_filled_orders(symbol="AAPL")
            >>> print(f"Filled AAPL orders: {len(filled)}")
        """
        ...
