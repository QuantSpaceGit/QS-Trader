"""Lot tracker for FIFO/LIFO position accounting.

Manages lots for both long and short positions:
- Long positions: FIFO (First In, First Out)
- Short positions: LIFO (Last In, First Out)

Week 1: Foundation with data structures
Week 2: Complete implementation with lot matching
"""

from collections import deque
from decimal import Decimal

from qs_trader.services.portfolio.models import Lot, LotSide


class LotTracker:
    """
    Tracker for lot-based position accounting.

    Maintains FIFO queue for long positions and LIFO stack for shorts.
    Handles partial lot closes and position transitions.

    Week 1 Status: Foundation (data structures defined)
    Week 2 Status: Complete implementation (lot matching logic)

    Example:
        >>> tracker = LotTracker()
        >>> tracker.add_lot(lot)  # Add lot to appropriate queue/stack
        >>> closed_lots = tracker.match_close(...)  # Match for close (Week 2)
    """

    def __init__(self) -> None:
        """Initialize lot tracker."""
        # FIFO queue for long positions (close oldest first)
        self._long_lots: deque[Lot] = deque()

        # LIFO stack for short positions (close newest first)
        self._short_lots: list[Lot] = []

    def add_lot(self, lot: Lot) -> None:
        """
        Add lot to appropriate queue/stack.

        Args:
            lot: Lot to add

        Raises:
            ValueError: If lot quantity is zero
        """
        if lot.quantity == 0:
            raise ValueError("Cannot add lot with zero quantity")

        if lot.side == LotSide.LONG:
            # Add to end of FIFO queue
            self._long_lots.append(lot)
        elif lot.side == LotSide.SHORT:
            # Add to end of LIFO stack
            self._short_lots.append(lot)
        else:
            raise ValueError(f"Invalid lot side: {lot.side}")

    def get_lots(self, side: LotSide) -> list[Lot]:
        """
        Get all lots for given side.

        Args:
            side: Long or short

        Returns:
            List of lots (ordered for FIFO/LIFO)
        """
        if side == LotSide.LONG:
            return list(self._long_lots)
        elif side == LotSide.SHORT:
            return list(self._short_lots)
        else:
            raise ValueError(f"Invalid lot side: {side}")

    def get_total_quantity(self, side: LotSide) -> Decimal:
        """
        Get total quantity across all lots for given side.

        Args:
            side: Long or short

        Returns:
            Total quantity (positive for long, negative for short)
        """
        lots = self.get_lots(side)
        return sum((lot.quantity for lot in lots), start=Decimal("0"))

    def has_position(self, side: LotSide) -> bool:
        """
        Check if any lots exist for given side.

        Args:
            side: Long or short

        Returns:
            True if lots exist
        """
        if side == LotSide.LONG:
            return len(self._long_lots) > 0
        elif side == LotSide.SHORT:
            return len(self._short_lots) > 0
        else:
            raise ValueError(f"Invalid lot side: {side}")

    def clear(self, side: LotSide | None = None) -> None:
        """
        Clear lots for given side (or both if None).

        Args:
            side: Long, short, or None for both
        """
        if side is None or side == LotSide.LONG:
            self._long_lots.clear()
        if side is None or side == LotSide.SHORT:
            self._short_lots.clear()

    # ==================== Week 2 Implementation ====================

    def match_close_long(self, quantity: Decimal) -> list[tuple[Lot, Decimal]]:
        """
        Match quantity against long lots using FIFO (First In, First Out).

        Closes oldest lots first. Handles partial lot closes by creating
        a new lot with remaining quantity.

        Args:
            quantity: Quantity to close (positive)

        Returns:
            List of (lot, quantity_closed) tuples in match order

        Raises:
            ValueError: If quantity is zero/negative or insufficient quantity

        Example:
            >>> # Close 150 shares from [100@$150, 100@$155]
            >>> matches = tracker.match_close_long(Decimal("150"))
            >>> # Returns: [(Lot(100@$150), 100), (Lot(100@$155), 50)]
            >>> # Leaves: [Lot(50@$155)]
        """
        if quantity <= 0:
            raise ValueError(f"Quantity must be positive, got {quantity}")

        # Check sufficient quantity available
        total_available = self.get_total_quantity(LotSide.LONG)
        if quantity > total_available:
            raise ValueError(f"Insufficient long quantity: need {quantity}, have {total_available}")

        matches: list[tuple[Lot, Decimal]] = []
        remaining_to_close = quantity

        # Match FIFO: pop from front of deque
        while remaining_to_close > 0 and len(self._long_lots) > 0:
            lot = self._long_lots[0]  # Peek at oldest lot

            if lot.quantity <= remaining_to_close:
                # Full lot close - remove entirely
                self._long_lots.popleft()
                matches.append((lot, lot.quantity))
                remaining_to_close -= lot.quantity
            else:
                # Partial lot close - split lot
                qty_to_close = remaining_to_close
                qty_remaining = lot.quantity - qty_to_close

                # Remove old lot
                self._long_lots.popleft()

                # Add match for closed portion
                matches.append((lot, qty_to_close))

                # Calculate proportional commission for remaining portion
                # The closed portion's commission will be calculated by PortfolioService
                # using: lot.entry_commission * (qty_to_close / lot.quantity)
                # The remaining lot must retain the unused commission
                remaining_commission = lot.entry_commission * (qty_remaining / lot.quantity)

                # Create new lot with remaining quantity
                new_lot = Lot(
                    lot_id=f"{lot.lot_id}_remaining",
                    symbol=lot.symbol,
                    side=lot.side,
                    quantity=qty_remaining,
                    entry_price=lot.entry_price,
                    entry_timestamp=lot.entry_timestamp,
                    entry_fill_id=lot.entry_fill_id,
                    entry_commission=remaining_commission,  # Preserve unused commission
                    realized_pnl=Decimal("0"),
                )
                self._long_lots.appendleft(new_lot)  # Put back at front

                remaining_to_close = Decimal("0")

        return matches

    def match_close_short(self, quantity: Decimal) -> list[tuple[Lot, Decimal]]:
        """
        Match quantity against short lots using LIFO (Last In, First Out).

        Closes newest lots first. Handles partial lot closes by creating
        a new lot with remaining quantity.

        Args:
            quantity: Quantity to close (positive value, will match negative lots)

        Returns:
            List of (lot, quantity_closed) tuples in match order

        Raises:
            ValueError: If quantity is zero/negative or insufficient quantity

        Example:
            >>> # Close 150 shares from [-100@$150, -100@$155]
            >>> matches = tracker.match_close_short(Decimal("150"))
            >>> # Returns: [(Lot(-100@$155), 100), (Lot(-100@$150), 50)]
            >>> # Leaves: [Lot(-50@$150)]
        """
        if quantity <= 0:
            raise ValueError(f"Quantity must be positive, got {quantity}")

        # Check sufficient quantity available (shorts are negative)
        total_available = abs(self.get_total_quantity(LotSide.SHORT))
        if quantity > total_available:
            raise ValueError(f"Insufficient short quantity: need {quantity}, have {total_available}")

        matches: list[tuple[Lot, Decimal]] = []
        remaining_to_close = quantity

        # Match LIFO: pop from back of list (newest first)
        while remaining_to_close > 0 and len(self._short_lots) > 0:
            lot = self._short_lots[-1]  # Peek at newest lot
            lot_qty = abs(lot.quantity)  # Convert to positive for comparison

            if lot_qty <= remaining_to_close:
                # Full lot close - remove entirely
                self._short_lots.pop()
                matches.append((lot, lot_qty))
                remaining_to_close -= lot_qty
            else:
                # Partial lot close - split lot
                qty_to_close = remaining_to_close
                qty_remaining = lot_qty - qty_to_close

                # Remove old lot
                self._short_lots.pop()

                # Add match for closed portion
                matches.append((lot, qty_to_close))

                # Calculate proportional commission for remaining portion
                # The closed portion's commission will be calculated by PortfolioService
                # using: lot.entry_commission * (qty_to_close / lot_qty)
                # The remaining lot must retain the unused commission
                remaining_commission = lot.entry_commission * (qty_remaining / lot_qty)

                # Create new lot with remaining quantity (still negative)
                new_lot = Lot(
                    lot_id=f"{lot.lot_id}_remaining",
                    symbol=lot.symbol,
                    side=lot.side,
                    quantity=-qty_remaining,  # Keep negative for shorts
                    entry_price=lot.entry_price,
                    entry_timestamp=lot.entry_timestamp,
                    entry_fill_id=lot.entry_fill_id,
                    entry_commission=remaining_commission,  # Preserve unused commission
                    realized_pnl=Decimal("0"),
                )
                self._short_lots.append(new_lot)  # Put back at end

                remaining_to_close = Decimal("0")

        return matches

    def remove_lot(self, lot_id: str, side: LotSide) -> Lot | None:
        """
        Remove specific lot by ID.

        Args:
            lot_id: Lot ID to remove
            side: Side to search

        Returns:
            Removed lot or None if not found
        """
        if side == LotSide.LONG:
            for i, lot in enumerate(self._long_lots):
                if lot.lot_id == lot_id:
                    removed = self._long_lots[i]
                    del self._long_lots[i]
                    return removed
        elif side == LotSide.SHORT:
            for i, lot in enumerate(self._short_lots):
                if lot.lot_id == lot_id:
                    return self._short_lots.pop(i)

        return None
