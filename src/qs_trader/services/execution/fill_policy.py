"""Fill policy for order execution simulation.

Implements conservative fill logic based on bar data and order types.
"""

from decimal import Decimal

from qs_trader.services.data.models import Bar
from qs_trader.services.execution.config import ExecutionConfig
from qs_trader.services.execution.models import FillDecision, Order, OrderType, TimeInForce
from qs_trader.services.execution.slippage import ISlippageCalculator, SlippageCalculatorFactory, SlippageModel


class FillPolicy:
    """Fill policy implementing conservative execution simulation.

    Evaluates orders against bar data and determines if/how they should fill.
    Uses conservative assumptions to avoid overly optimistic backtests.

    Conservative Rules:
    - Market: Fill at next bar's open (queued for 1 bar)
    - Limit: Fill at min(limit, close) for buy, max(limit, close) for sell
    - Stop: Fill at max(stop, close) + slippage for buy, min(stop, close) - slippage for sell
    - MOC: Fill at current bar's close + slippage

    Attributes:
        config: Execution configuration with slippage, participation limits
        slippage_calculator: Calculator for slippage adjustments
    """

    def __init__(self, config: ExecutionConfig) -> None:
        """Initialize fill policy.

        Args:
            config: Execution configuration
        """
        self.config = config

        # Create slippage calculator from config
        self.slippage_calculator: ISlippageCalculator = SlippageCalculatorFactory.create(
            SlippageModel(config.slippage.model), **config.slippage.params
        )

    def evaluate_order(self, order: Order, bar: Bar) -> FillDecision:
        """Evaluate order against bar data.

        Routes to appropriate evaluation method based on order type.
        Applies volume participation limits for all order types.
        Checks expiry conditions (queue bars, day boundary, TIF).

        Args:
            order: Order to evaluate
            bar: Bar data with OHLCV

        Returns:
            FillDecision with fill instructions

        Raises:
            ValueError: If order type not supported
        """
        # Skip if order not active
        if not order.is_active:
            return FillDecision(
                should_fill=False, reason=f"Order in terminal state: {order.state}", queue_for_next_bar=False
            )

        # Check if order has exceeded queue bars limit
        if order.bars_queued >= self.config.queue_bars:
            return FillDecision(
                should_fill=False,
                reason=f"Order expired: queued {order.bars_queued}/{self.config.queue_bars} bars",
                queue_for_next_bar=False,
                should_expire=True,
            )

        # Check if DAY order has crossed day boundary
        if order.time_in_force == TimeInForce.DAY and order.submitted_date is not None:
            order_date = order.submitted_date.date()
            bar_date = bar.trade_datetime.date()
            if bar_date > order_date:
                return FillDecision(
                    should_fill=False,
                    reason=f"DAY order expired at end of trading day ({order_date})",
                    queue_for_next_bar=False,
                    should_expire=True,
                )

        # Route to appropriate handler based on order type
        if order.order_type == OrderType.MARKET:
            decision = self._evaluate_market(order, bar)
        elif order.order_type == OrderType.LIMIT:
            decision = self._evaluate_limit(order, bar)
        elif order.order_type == OrderType.STOP:
            decision = self._evaluate_stop(order, bar)
        elif order.order_type == OrderType.MARKET_ON_CLOSE:
            decision = self._evaluate_moc(order, bar)
        else:
            raise ValueError(f"Unsupported order type: {order.order_type}")

        # Apply time-in-force logic
        return self._apply_tif_logic(order, decision, bar)

    def _evaluate_market(self, order: Order, bar: Bar) -> FillDecision:
        """Evaluate market order.

        Market orders fill at next bar's open after queueing.
        IOC and FOK orders skip queueing and fill immediately.

        Args:
            order: Market order
            bar: Current bar

        Returns:
            FillDecision
        """
        # IOC and FOK orders skip queueing - must fill immediately
        skip_queue = order.time_in_force in (TimeInForce.IOC, TimeInForce.FOK)

        # Market orders must be queued for N bars (unless IOC/FOK)
        if not skip_queue and order.bars_queued < self.config.market_order_queue_bars:
            return FillDecision(
                should_fill=False,
                reason=f"Market order queued ({order.bars_queued}/{self.config.market_order_queue_bars} bars)",
                queue_for_next_bar=True,
            )

        # Calculate fillable quantity based on volume participation
        fill_quantity = self._calculate_fillable_quantity(order, bar.volume)

        # Cannot fill if zero quantity
        if fill_quantity == 0:
            return FillDecision(
                should_fill=False, reason="Market order cannot fill (zero volume bar)", queue_for_next_bar=True
            )

        # Fill at bar open with slippage
        base_price = Decimal(str(bar.open))
        fill_price = self._apply_slippage(order, bar, fill_quantity, base_price)

        # Get slippage BPS from config (for market orders, use market_order_bps if available)
        slippage_bps = 0
        if hasattr(self.config.slippage, "params") and "bps" in self.config.slippage.params:
            slippage_bps = int(self.config.slippage.params["bps"])

        return FillDecision(
            should_fill=True,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
            slippage_bps=slippage_bps,
            reason="Market order filled at next bar open",
            queue_for_next_bar=(fill_quantity < order.remaining_quantity),
        )

    def _evaluate_limit(self, order: Order, bar: Bar) -> FillDecision:
        """Evaluate limit order.

        Buy limit: Fill if bar.low <= limit_price
        Sell limit: Fill if bar.high >= limit_price
        Fill at min(limit, close) for buy, max(limit, close) for sell.

        Args:
            order: Limit order
            bar: Current bar

        Returns:
            FillDecision
        """
        if order.limit_price is None:
            raise ValueError("Limit order must have limit_price")

        # Convert bar prices to Decimal
        bar_low = Decimal(str(bar.low))
        bar_high = Decimal(str(bar.high))
        bar_close = Decimal(str(bar.close))

        # Check if limit price touched
        if order.side.value == "buy":
            if bar_low > order.limit_price:
                return FillDecision(
                    should_fill=False, reason="Buy limit not touched (bar.low > limit)", queue_for_next_bar=True
                )
            # Fill at min(limit, close) - conservative
            fill_price = min(order.limit_price, bar_close)
        else:  # sell
            if bar_high < order.limit_price:
                return FillDecision(
                    should_fill=False, reason="Sell limit not touched (bar.high < limit)", queue_for_next_bar=True
                )
            # Fill at max(limit, close) - conservative
            fill_price = max(order.limit_price, bar_close)

        # Calculate fillable quantity
        fill_quantity = self._calculate_fillable_quantity(order, bar.volume)

        # Cannot fill if zero quantity
        if fill_quantity == 0:
            return FillDecision(
                should_fill=False,
                reason=f"{order.side.value.capitalize()} limit touched but cannot fill (zero volume)",
                queue_for_next_bar=True,
            )

        # Limit orders typically have 0 slippage as they only fill at limit price or better
        slippage_bps = 0

        return FillDecision(
            should_fill=True,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
            slippage_bps=slippage_bps,
            reason=f"{order.side.value.capitalize()} limit touched and filled",
            queue_for_next_bar=(fill_quantity < order.remaining_quantity),
        )

    def _evaluate_stop(self, order: Order, bar: Bar) -> FillDecision:
        """Evaluate stop order.

        Buy stop: Trigger if bar.high >= stop_price
        Sell stop: Trigger if bar.low <= stop_price
        Fill at max(stop, close) + slippage for buy, min(stop, close) - slippage for sell.

        Args:
            order: Stop order
            bar: Current bar

        Returns:
            FillDecision
        """
        if order.stop_price is None:
            raise ValueError("Stop order must have stop_price")

        # Convert bar prices to Decimal
        bar_low = Decimal(str(bar.low))
        bar_high = Decimal(str(bar.high))
        bar_close = Decimal(str(bar.close))

        # Check if stop triggered and calculate base price
        if order.side.value == "buy":
            if bar_high < order.stop_price:
                return FillDecision(
                    should_fill=False, reason="Buy stop not triggered (bar.high < stop)", queue_for_next_bar=True
                )
            # Fill at max(stop, close) + slippage
            base_price = max(order.stop_price, bar_close)
        else:  # sell
            if bar_low > order.stop_price:
                return FillDecision(
                    should_fill=False, reason="Sell stop not triggered (bar.low > stop)", queue_for_next_bar=True
                )
            # Fill at min(stop, close) - slippage
            base_price = min(order.stop_price, bar_close)

        # Calculate fillable quantity
        fill_quantity = self._calculate_fillable_quantity(order, bar.volume)

        # Cannot fill if zero quantity
        if fill_quantity == 0:
            return FillDecision(
                should_fill=False,
                reason=f"{order.side.value.capitalize()} stop triggered but cannot fill (zero volume)",
                queue_for_next_bar=True,
            )

        # Apply slippage to base price
        fill_price = self._apply_slippage(order, bar, fill_quantity, base_price)

        # Get slippage BPS from config (for stop orders)
        slippage_bps = 0
        if hasattr(self.config.slippage, "params") and "bps" in self.config.slippage.params:
            slippage_bps = int(self.config.slippage.params["bps"])

        return FillDecision(
            should_fill=True,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
            slippage_bps=slippage_bps,
            reason=f"{order.side.value.capitalize()} stop triggered and filled",
            queue_for_next_bar=(fill_quantity < order.remaining_quantity),
        )

    def _evaluate_moc(self, order: Order, bar: Bar) -> FillDecision:
        """Evaluate market-on-close order.

        Fills at current bar's close price with slippage.

        Args:
            order: MOC order
            bar: Current bar

        Returns:
            FillDecision
        """
        # Calculate fillable quantity
        fill_quantity = self._calculate_fillable_quantity(order, bar.volume)

        # Cannot fill if zero quantity
        if fill_quantity == 0:
            return FillDecision(
                should_fill=False, reason="MOC order cannot fill (zero volume bar)", queue_for_next_bar=True
            )

        # Fill at close with slippage
        base_price = Decimal(str(bar.close))
        fill_price = self._apply_slippage(order, bar, fill_quantity, base_price)

        # Get slippage BPS from config (for MOC orders)
        slippage_bps = 0
        if hasattr(self.config.slippage, "params") and "bps" in self.config.slippage.params:
            slippage_bps = int(self.config.slippage.params["bps"])

        return FillDecision(
            should_fill=True,
            fill_price=fill_price,
            fill_quantity=fill_quantity,
            slippage_bps=slippage_bps,
            reason="MOC order filled at close",
            queue_for_next_bar=(fill_quantity < order.remaining_quantity),
        )

    def _apply_slippage(self, order: Order, bar: Bar, fill_quantity: Decimal, price: Decimal) -> Decimal:
        """Apply slippage to price using configured slippage calculator.

        Args:
            order: Order being filled
            bar: Current bar data
            fill_quantity: Quantity being filled
            price: Base price

        Returns:
            Price with slippage applied
        """
        return self.slippage_calculator.calculate(order, bar, fill_quantity, price)

    def _calculate_fillable_quantity(self, order: Order, bar_volume: int) -> Decimal:
        """Calculate how much can fill this bar based on volume participation.

        Args:
            order: Order to fill
            bar_volume: Bar's total volume

        Returns:
            Quantity that can fill (may be partial)
        """
        # Handle zero/negative volume bars
        if bar_volume <= 0:
            return Decimal("0")

        # Max fillable based on participation limit
        max_fillable = Decimal(str(bar_volume)) * self.config.max_participation_rate

        # Return minimum of remaining quantity and max fillable
        return min(order.remaining_quantity, max_fillable)

    def _apply_tif_logic(self, order: Order, decision: FillDecision, bar: Bar) -> FillDecision:
        """Apply time-in-force logic to fill decision.

        Handles IOC (Immediate or Cancel) and FOK (Fill or Kill) orders.
        DAY and GTC orders are handled by expiry checks in evaluate_order().

        Args:
            order: Order being evaluated
            decision: Fill decision from order type handler
            bar: Current bar data

        Returns:
            Modified fill decision with TIF logic applied

        TIF Logic:
            - DAY: Handled by day boundary check (expires at end of day)
            - GTC: No special handling (persists until filled or cancelled)
            - IOC: Fill available quantity immediately, cancel rest
            - FOK: Fill complete quantity immediately or cancel entirely
        """
        # DAY and GTC: no special handling needed (handled by expiry checks)
        if order.time_in_force in (TimeInForce.DAY, TimeInForce.GTC):
            return decision

        # IOC: Immediate or Cancel
        # Fill whatever is available immediately, cancel the rest
        if order.time_in_force == TimeInForce.IOC:
            if decision.should_fill:
                # Partial fill is OK for IOC
                # If less than full quantity filled, mark for cancellation
                if decision.fill_quantity < order.remaining_quantity:
                    return FillDecision(
                        should_fill=True,
                        fill_price=decision.fill_price,
                        fill_quantity=decision.fill_quantity,
                        reason=f"{decision.reason} (IOC: partial fill, cancelling rest)",
                        queue_for_next_bar=False,
                        should_cancel=True,  # Cancel after this fill
                    )
                else:
                    # Full fill
                    return decision
            else:
                # Cannot fill immediately, cancel
                return FillDecision(
                    should_fill=False,
                    reason=f"IOC order cannot fill immediately: {decision.reason}",
                    queue_for_next_bar=False,
                    should_cancel=True,
                )

        # FOK: Fill or Kill
        # Must fill complete quantity immediately or cancel entirely
        if order.time_in_force == TimeInForce.FOK:
            if decision.should_fill and decision.fill_quantity >= order.remaining_quantity:
                # Can fill complete quantity
                return decision
            else:
                # Cannot fill complete quantity, cancel
                reason = (
                    f"FOK order cannot fill complete quantity ({decision.fill_quantity} < {order.remaining_quantity})"
                    if decision.should_fill
                    else f"FOK order cannot fill: {decision.reason}"
                )
                return FillDecision(
                    should_fill=False,
                    reason=reason,
                    queue_for_next_bar=False,
                    should_cancel=True,
                )

        # Should not reach here
        return decision
