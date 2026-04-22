"""Execution service implementation.

Simulates realistic order execution for backtesting.
"""

from datetime import datetime, timezone
from typing import Any, Optional

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import FillEvent, OrderEvent, PriceBarEvent
from qs_trader.events.lifecycle_context import LifecycleRunContext
from qs_trader.events.lifecycle_events import FillLifecycleEvent, OrderLifecycleEvent
from qs_trader.events.price_basis import PriceBasis
from qs_trader.services.data.models import Bar
from qs_trader.services.execution.commission import CommissionCalculator
from qs_trader.services.execution.config import ExecutionConfig
from qs_trader.services.execution.fill_policy import FillPolicy
from qs_trader.services.execution.models import Fill, Order, OrderSide, OrderState, OrderType, TimeInForce
from qs_trader.system import LoggerFactory

logger = LoggerFactory.get_logger()


class ExecutionService:
    """Execution service for order simulation.

    Accepts orders, evaluates them against bar data, and generates fills.
    Does NOT modify portfolio - returns Fill objects for external application.

    Attributes:
        config: Execution configuration
        fill_policy: Fill evaluation logic
        commission_calculator: Commission calculation
        _orders: All orders (by order_id)
        _pending_orders_by_symbol: Active orders grouped by symbol

    Example:
        >>> config = ExecutionConfig()
        >>> execution = ExecutionService(config)
        >>>
        >>> # Submit order
        >>> order = Order(
        ...     symbol="AAPL",
        ...     side=OrderSide.BUY,
        ...     quantity=Decimal("100"),
        ...     order_type=OrderType.MARKET,
        ...     created_at=datetime.now()
        ... )
        >>> order_id = execution.submit_order(order)
        >>>
        >>> # Process bar
        >>> bar = Bar(...)
        >>> fills = execution.on_bar(bar)
        >>>
        >>> # Apply fills to portfolio
        >>> for fill in fills:
        ...     portfolio.apply_fill(**fill.__dict__)
    """

    def __init__(
        self,
        config: ExecutionConfig,
        event_bus: Optional[EventBus] = None,
        price_basis: PriceBasis = PriceBasis.ADJUSTED,
        lifecycle_context: LifecycleRunContext | None = None,
    ) -> None:
        """Initialize execution service.

        Args:
            config: Execution configuration
            event_bus: Optional event bus for Phase 5 event-driven mode
            price_basis: Price basis to use for fills.
                - 'raw' uses the base OHLC values from the bar event.
                - 'adjusted' prefers adjusted ClickHouse OHLC values when available.
        """
        self.config = config
        self.fill_policy = FillPolicy(config)
        self.commission_calculator = CommissionCalculator(config.commission)
        self._event_bus = event_bus
        self._price_basis = price_basis
        self._lifecycle_context: LifecycleRunContext | None = None

        # Order tracking
        self._orders: dict[str, Order] = {}
        self._pending_orders_by_symbol: dict[str, list[str]] = {}

        # Event tracking for causation chain
        self._order_events: dict[str, "OrderEvent"] = {}  # order_id → OrderEvent
        self._latest_order_lifecycle_event_ids: dict[str, str] = {}  # order_id → latest OrderLifecycleEvent.event_id

        # Subscribe to events if event bus provided
        if self._event_bus:
            self._event_bus.subscribe("bar", self.on_bar_event)  # type: ignore[arg-type]
            self._event_bus.subscribe("order", self.on_order)  # type: ignore[arg-type]

        if lifecycle_context is not None:
            self.enable_lifecycle_tracking(lifecycle_context)

    def enable_lifecycle_tracking(self, lifecycle_context: LifecycleRunContext) -> None:
        """Enable canonical lifecycle emission for this execution service."""
        self._lifecycle_context = lifecycle_context

    @staticmethod
    def _isoformat_utc(value: datetime) -> str:
        """Serialize datetimes to RFC3339 with Z suffix."""
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.isoformat().replace("+00:00", "Z")

    @staticmethod
    def _ensure_utc(value: datetime) -> datetime:
        """Normalize datetimes to timezone-aware UTC instances."""
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    @staticmethod
    def _parse_order_type(value: str) -> OrderType:
        """Map wire order types onto the execution-service order model."""
        normalized = value.lower()
        mapping = {
            "market": OrderType.MARKET,
            "limit": OrderType.LIMIT,
            "stop": OrderType.STOP,
            "moc": OrderType.MARKET_ON_CLOSE,
            "market_on_close": OrderType.MARKET_ON_CLOSE,
        }
        try:
            return mapping[normalized]
        except KeyError as exc:
            raise ValueError(f"Unsupported order type: {value}") from exc

    @staticmethod
    def _parse_time_in_force(value: str) -> TimeInForce:
        """Map wire time-in-force values onto the execution-service model."""
        normalized = value.lower()
        mapping = {
            "day": TimeInForce.DAY,
            "gtc": TimeInForce.GTC,
            "ioc": TimeInForce.IOC,
            "fok": TimeInForce.FOK,
        }
        try:
            return mapping[normalized]
        except KeyError as exc:
            raise ValueError(f"Unsupported time_in_force: {value}") from exc

    def _emit_order_lifecycle_event(
        self,
        *,
        order: Order,
        occurred_at: datetime,
        order_state: str,
        causation_id: str | None,
        rejection_reason: str | None = None,
        expiry_reason: str | None = None,
        cancellation_reason: str | None = None,
    ) -> OrderLifecycleEvent | None:
        """Emit a canonical order lifecycle transition when lifecycle mode is enabled."""
        if self._event_bus is None or self._lifecycle_context is None:
            return None

        order_event = self._order_events.get(order.order_id)
        strategy_id = order.strategy_id or (order_event.source_strategy_id if order_event else None) or "manual"
        lifecycle_event = OrderLifecycleEvent(
            experiment_id=self._lifecycle_context.experiment_id,
            run_id=self._lifecycle_context.run_id,
            occurred_at=self._ensure_utc(occurred_at),
            order_id=order.order_id,
            intent_id=order_event.intent_id if order_event is not None else None,
            strategy_id=strategy_id,
            symbol=order.symbol,
            order_state=order_state,
            side=order.side.value,
            quantity=order.quantity,
            filled_quantity=order.filled_quantity,
            order_type=order_event.order_type if order_event is not None else order.order_type.value,
            limit_price=order.limit_price,
            stop_price=order.stop_price,
            time_in_force=(order_event.time_in_force if order_event is not None else order.time_in_force.value.upper()),
            price_basis=self._lifecycle_context.execution_price_basis,
            idempotency_key=order_event.idempotency_key if order_event is not None else order.order_id,
            rejection_reason=rejection_reason,
            expiry_reason=expiry_reason,
            cancellation_reason=cancellation_reason,
            source_service="execution_service",
            correlation_id=order_event.correlation_id if order_event is not None else None,
            causation_id=causation_id,
        )
        self._event_bus.publish(lifecycle_event)
        self._latest_order_lifecycle_event_ids[order.order_id] = lifecycle_event.event_id
        return lifecycle_event

    def _emit_fill_lifecycle_event(
        self,
        *,
        fill: Fill,
        order: Order,
        causation_id: str | None,
    ) -> FillLifecycleEvent | None:
        """Emit a canonical fill lifecycle event when lifecycle mode is enabled."""
        if self._event_bus is None or self._lifecycle_context is None:
            return None

        order_event = self._order_events.get(order.order_id)
        gross_value = fill.quantity * fill.price
        net_value = -(gross_value + fill.commission) if fill.side == "buy" else gross_value - fill.commission

        lifecycle_event = FillLifecycleEvent(
            experiment_id=self._lifecycle_context.experiment_id,
            run_id=self._lifecycle_context.run_id,
            occurred_at=self._ensure_utc(fill.timestamp),
            fill_id=fill.fill_id,
            order_id=order.order_id,
            intent_id=order_event.intent_id if order_event is not None else None,
            strategy_id=fill.strategy_id
            or order.strategy_id
            or (order_event.source_strategy_id if order_event else None)
            or "manual",
            symbol=fill.symbol,
            side=fill.side,
            filled_quantity=fill.quantity,
            fill_price=fill.price,
            price_basis=self._lifecycle_context.execution_price_basis,
            commission=fill.commission,
            slippage_bps=fill.slippage_bps,
            gross_value=gross_value,
            net_value=net_value,
            order_received_at=order_event.timestamp if order_event is not None else None,
            source_service="execution_service",
            correlation_id=order_event.correlation_id if order_event is not None else None,
            causation_id=causation_id,
        )
        self._event_bus.publish(lifecycle_event)
        return lifecycle_event

    def _publish_fill_event(self, fill: Fill) -> None:
        """Publish the legacy fill event for downstream portfolio/reporting consumers."""
        if self._event_bus is None:
            return

        timestamp_str = self._isoformat_utc(fill.timestamp)
        order_event = self._order_events.get(fill.order_id)

        fill_event = FillEvent(
            fill_id=fill.fill_id,
            source_order_id=fill.order_id,
            timestamp=timestamp_str,
            symbol=fill.symbol,
            side=fill.side,
            filled_quantity=fill.quantity,
            fill_price=fill.price,
            commission=fill.commission,
            slippage_bps=fill.slippage_bps,
            strategy_id=fill.strategy_id,
            source_service="execution_service",
            correlation_id=order_event.correlation_id if order_event else None,
            causation_id=fill.order_id,
        )
        self._event_bus.publish(fill_event)

    def submit_order(self, order: Order) -> str:
        """Submit a new order for execution.

        Order is validated (by Order.__post_init__) and tracked.

        Args:
            order: Order object with all parameters

        Returns:
            order_id: Unique identifier for tracking

        Raises:
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
        # Check for duplicate
        if order.order_id in self._orders:
            error_msg = f"Order ID already exists: {order.order_id}"
            logger.error(
                "execution.order.duplicate",
                order_id=order.order_id,
                symbol=order.symbol,
                error=error_msg,
            )
            raise ValueError(error_msg)

        # Mark as submitted
        order.submit(order.created_at)

        # Track order
        self._orders[order.order_id] = order

        # Log submission
        logger.debug(
            "execution.order.submitted",
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side.value,
            quantity=str(order.quantity),
            order_type=order.order_type.value,
            limit_price=str(order.limit_price) if order.limit_price else None,
            stop_price=str(order.stop_price) if order.stop_price else None,
        )

        # Add to pending list for this symbol
        if order.symbol not in self._pending_orders_by_symbol:
            self._pending_orders_by_symbol[order.symbol] = []
        self._pending_orders_by_symbol[order.symbol].append(order.order_id)

        return order.order_id

    def on_bar(self, bar: Bar) -> list[Fill]:
        """Process bar and generate fills for eligible orders.

        Evaluates all pending orders for this symbol against bar data.
        Generates Fill objects for successful executions.

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
        # Extract symbol from bar (assuming it's available via some mechanism)
        # For now, we need to iterate through pending orders for all symbols
        # and check if they match. In practice, the caller would provide symbol.

        fills: list[Fill] = []

        # Process all symbols (caller should filter by symbol before calling)
        for symbol in list(self._pending_orders_by_symbol.keys()):
            order_ids = self._pending_orders_by_symbol.get(symbol, [])

            for order_id in list(order_ids):
                order = self._orders.get(order_id)

                # Defensive: handle missing order
                if order is None:
                    logger.warning(
                        "execution.process_limit_orders.order_not_found",
                        order_id=order_id,
                        symbol=symbol,
                    )
                    continue

                # Fill decision for this order-bar pair
                decision = self.fill_policy.evaluate_order(order, bar)

                # Handle expiry
                if decision.should_expire:
                    order.expire(bar.trade_datetime)
                    self._remove_from_pending(order)
                    logger.info(
                        "execution.order.expired",
                        order_id=order.order_id,
                        symbol=order.symbol,
                        reason=decision.reason,
                    )
                    continue

                # Queue market orders (before processing fill)
                if decision.queue_for_next_bar:
                    order.bars_queued += 1

                # Generate fill if should_fill (BEFORE cancellation)
                if decision.should_fill:
                    # Calculate commission (pass price for percentage-based models)
                    commission = self.commission_calculator.calculate(decision.fill_quantity, decision.fill_price)

                    # Create fill
                    fill = Fill(
                        order_id=order.order_id,
                        timestamp=bar.trade_datetime,
                        symbol=order.symbol,
                        side=order.side.value,  # Convert enum to string
                        quantity=decision.fill_quantity,
                        price=decision.fill_price,
                        commission=commission,
                        slippage_bps=decision.slippage_bps,
                        strategy_id=order.strategy_id,  # Preserve strategy attribution
                    )
                    fills.append(fill)

                    logger.info(
                        "execution.fill.generated",
                        order_id=order.order_id,
                        fill_id=fill.fill_id,
                        symbol=fill.symbol,
                        side=fill.side,
                        quantity=fill.quantity,
                        price=fill.price,
                        commission=fill.commission,
                    )

                    # Update order state
                    order.update_fill(decision.fill_quantity, decision.fill_price, bar.trade_datetime)

                    # Remove from pending if complete
                    if order.is_complete:
                        self._remove_from_pending(order)
                        logger.debug(
                            "execution.order.completed",
                            order_id=order.order_id,
                            symbol=order.symbol,
                            filled_quantity=order.filled_quantity,
                        )

                # Handle cancellation AFTER fill (IOC/FOK partial fills)
                if decision.should_cancel and not order.is_complete:
                    order.cancel(bar.trade_datetime)
                    self._remove_from_pending(order)
                    logger.warning(
                        "execution.order.cancelled",
                        order_id=order.order_id,
                        symbol=order.symbol,
                        reason=decision.reason or "policy_decision",
                        filled_quantity=order.filled_quantity,
                    )

        return fills

    def cancel_order(self, order_id: str) -> bool:
        """Cancel pending order.

        Args:
            order_id: ID of order to cancel

        Returns:
            True if cancelled, False if not found or already complete

        Example:
            >>> execution.cancel_order(order.order_id)
        """
        order = self._orders.get(order_id)

        if order is None:
            logger.warning(
                "execution.cancel.not_found",
                order_id=order_id,
            )
            return False

        if order.is_complete:
            logger.warning(
                "execution.cancel.already_complete",
                order_id=order_id,
                symbol=order.symbol,
                state=order.state.value,
            )
            return False

        # Cancel order
        order.cancel(datetime.now())

        # Remove from pending
        self._remove_from_pending(order)

        self._emit_order_lifecycle_event(
            order=order,
            occurred_at=(order.last_updated or datetime.now(timezone.utc)),
            order_state="cancelled",
            causation_id=self._latest_order_lifecycle_event_ids.get(order.order_id),
            cancellation_reason="manual_cancel",
        )

        logger.info(
            "execution.order.cancelled_manual",
            order_id=order_id,
            symbol=order.symbol,
            filled_quantity=order.filled_quantity,
        )

        return True

    def get_order(self, order_id: str) -> Order | None:
        """Retrieve order by ID.

        Args:
            order_id: Order identifier

        Returns:
            Order object or None if not found
        """
        return self._orders.get(order_id)

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
        if symbol is not None:
            order_ids = self._pending_orders_by_symbol.get(symbol, [])
            return [self._orders[oid] for oid in order_ids if self._orders[oid].is_active]
        else:
            # Return all pending orders across all symbols
            return [order for order in self._orders.values() if order.is_active]

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
        if symbol is not None:
            return [
                order for order in self._orders.values() if order.symbol == symbol and order.state == OrderState.FILLED
            ]
        else:
            return [order for order in self._orders.values() if order.state == OrderState.FILLED]

    def _remove_from_pending(self, order: Order) -> None:
        """Remove order from pending tracking.

        Args:
            order: Order to remove
        """
        if order.symbol in self._pending_orders_by_symbol:
            order_ids = self._pending_orders_by_symbol[order.symbol]
            if order.order_id in order_ids:
                order_ids.remove(order.order_id)

            # Clean up empty lists
            if not order_ids:
                del self._pending_orders_by_symbol[order.symbol]

    # ==================== Phase 5: Event Handlers ====================

    def on_bar_event(self, event: PriceBarEvent) -> None:
        """
        Handle bar event to process fills (Phase 5).

        Converts PriceBarEvent to Bar and processes fills for the symbol.

        Args:
            event: Price bar event
        """
        from datetime import datetime

        trade_datetime = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))

        if self._price_basis == PriceBasis.ADJUSTED:
            open_price = float(event.open_adj if event.open_adj is not None else event.open)
            high_price = float(event.high_adj if event.high_adj is not None else event.high)
            low_price = float(event.low_adj if event.low_adj is not None else event.low)
            close_price = float(event.close_adj if event.close_adj is not None else event.close)
        else:
            open_price = float(event.open)
            high_price = float(event.high)
            low_price = float(event.low)
            close_price = float(event.close)

        bar = Bar(
            trade_datetime=trade_datetime,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=event.volume,
        )

        # Process bar ONLY for this symbol's pending orders
        symbol = event.symbol
        if symbol in self._pending_orders_by_symbol:
            order_ids = self._pending_orders_by_symbol.get(symbol, [])

            for order_id in list(order_ids):
                order = self._orders.get(order_id)

                # Defensive: handle missing order
                if order is None:
                    logger.error(
                        "execution.on_bar.missing_order",
                        order_id=order_id,
                        symbol=symbol,
                    )
                    continue

                # Skip if order not active
                if not order.is_active:
                    self._remove_from_pending(order)
                    continue

                # Evaluate order against bar
                decision = self.fill_policy.evaluate_order(order, bar)

                # Handle expiry
                if decision.should_expire:
                    order.expire(bar.trade_datetime)
                    self._remove_from_pending(order)
                    self._emit_order_lifecycle_event(
                        order=order,
                        occurred_at=bar.trade_datetime,
                        order_state="expired",
                        causation_id=self._latest_order_lifecycle_event_ids.get(order.order_id),
                        expiry_reason=decision.reason,
                    )
                    logger.info(
                        "execution.order.expired",
                        order_id=order.order_id,
                        symbol=order.symbol,
                        reason=decision.reason,
                    )
                    continue

                # Queue market orders (before processing fill)
                if decision.queue_for_next_bar:
                    order.bars_queued += 1

                # Generate fill if should_fill (BEFORE cancellation)
                if decision.should_fill:
                    # Calculate commission (pass price for percentage-based models)
                    commission = self.commission_calculator.calculate(decision.fill_quantity, decision.fill_price)

                    # Create fill
                    fill = Fill(
                        order_id=order.order_id,
                        timestamp=bar.trade_datetime,
                        symbol=order.symbol,
                        side=order.side.value,  # Convert enum to string
                        quantity=decision.fill_quantity,
                        price=decision.fill_price,
                        commission=commission,
                        slippage_bps=decision.slippage_bps,
                        strategy_id=order.strategy_id,  # Preserve strategy attribution
                    )

                    logger.info(
                        "execution.fill.generated",
                        order_id=order.order_id,
                        fill_id=fill.fill_id,
                        symbol=fill.symbol,
                        side=fill.side,
                        quantity=fill.quantity,
                        price=fill.price,
                        commission=fill.commission,
                    )

                    fill_lifecycle_event = self._emit_fill_lifecycle_event(
                        fill=fill,
                        order=order,
                        causation_id=self._latest_order_lifecycle_event_ids.get(order.order_id),
                    )

                    # Update order state
                    order.update_fill(decision.fill_quantity, decision.fill_price, bar.trade_datetime)

                    self._emit_order_lifecycle_event(
                        order=order,
                        occurred_at=bar.trade_datetime,
                        order_state="filled" if order.is_complete else "partially_filled",
                        causation_id=fill_lifecycle_event.event_id if fill_lifecycle_event is not None else None,
                    )

                    self._publish_fill_event(fill)

                    # Remove from pending if complete
                    if order.is_complete:
                        self._remove_from_pending(order)
                        logger.debug(
                            "execution.order.completed",
                            order_id=order.order_id,
                            symbol=order.symbol,
                            filled_quantity=order.filled_quantity,
                        )

                # Handle cancellation AFTER fill (IOC/FOK partial fills)
                if decision.should_cancel and not order.is_complete:
                    order.cancel(bar.trade_datetime)
                    self._remove_from_pending(order)
                    self._emit_order_lifecycle_event(
                        order=order,
                        occurred_at=bar.trade_datetime,
                        order_state="cancelled",
                        causation_id=self._latest_order_lifecycle_event_ids.get(order.order_id),
                        cancellation_reason=decision.reason or "policy_decision",
                    )
                    logger.warning(
                        "execution.order.cancelled",
                        order_id=order.order_id,
                        symbol=order.symbol,
                        reason=decision.reason or "policy_decision",
                        filled_quantity=order.filled_quantity,
                    )

    def on_order(self, event: OrderEvent) -> None:
        """
        Handle order event - create and submit order (Phase 5).

        Args:
            event: Order event from ManagerService
        """
        from datetime import datetime

        # Parse timestamp string to datetime
        created_at = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))

        # Create order from event - use OrderEvent.event_id as order_id for causality tracking
        order = Order(
            order_id=str(event.event_id),  # Use OrderEvent's UUID for causality chain
            symbol=event.symbol,
            side=OrderSide.BUY if event.side == "buy" else OrderSide.SELL,
            quantity=event.quantity,
            order_type=self._parse_order_type(event.order_type),
            time_in_force=self._parse_time_in_force(event.time_in_force),
            limit_price=event.limit_price,
            stop_price=event.stop_price,
            created_at=created_at,
            strategy_id=event.source_strategy_id,  # Preserve strategy attribution
        )

        # Store OrderEvent for causation chain propagation
        self._order_events[order.order_id] = event
        if event.causation_id is not None:
            self._latest_order_lifecycle_event_ids[order.order_id] = event.causation_id

        # Submit order
        try:
            order_id = self.submit_order(order)
            self._emit_order_lifecycle_event(
                order=order,
                occurred_at=created_at,
                order_state="submitted",
                causation_id=event.causation_id or event.event_id,
            )
            logger.debug(
                "execution.order_from_event",
                order_id=order_id,
                source_strategy_id=event.source_strategy_id,
                symbol=event.symbol,
                side=event.side,
                quantity=str(event.quantity),
                intent_id=event.intent_id,
            )
        except ValueError as e:
            self._emit_order_lifecycle_event(
                order=order,
                occurred_at=created_at,
                order_state="rejected",
                causation_id=event.causation_id or event.event_id,
                rejection_reason=str(e),
            )
            logger.error(
                "execution.order_submission_failed",
                source_strategy_id=event.source_strategy_id,
                symbol=event.symbol,
                error=str(e),
            )

    @classmethod
    def from_config(cls, config_dict: dict[str, Any], event_bus: EventBus) -> "ExecutionService":
        """
        Factory method to create service from configuration.

        Args:
            config_dict: Execution configuration dictionary
            event_bus: Event bus for communication

        Returns:
            Configured ExecutionService instance
        """
        # For Phase 5, we use a simplified ExecutionConfig
        # Fill policy is specified in backtest config, but ExecutionConfig
        # uses other parameters. We'll create a default config.
        execution_config = ExecutionConfig()

        return cls(config=execution_config, event_bus=event_bus)
