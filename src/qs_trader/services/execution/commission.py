"""Commission calculation utilities.

Calculates trading commissions based on configuration.
"""

from decimal import Decimal

from qs_trader.services.execution.config import CommissionConfig


class CommissionCalculator:
    """Calculates commissions for fills.

    Supports three commission models:
    1. Per-share: max(qty * per_share, minimum)
    2. Per-trade: flat_fee OR percentage * notional
    3. Tiered: Volume-based brackets with different per-share rates

    Commission cap is applied after model calculation and minimum.

    Attributes:
        config: Commission configuration
    """

    def __init__(self, config: CommissionConfig) -> None:
        """Initialize commission calculator.

        Args:
            config: Commission configuration
        """
        self.config = config

    def calculate(self, quantity: Decimal, price: Decimal | None = None) -> Decimal:
        """Calculate commission for a fill.

        The calculation depends on the configured model:
        - Per-share: max(qty * per_share, minimum)
        - Per-trade flat: max(flat_fee, minimum)
        - Per-trade percentage: max(qty * price * percentage, minimum)
        - Tiered: Calculate per tier, then max(total, minimum)

        If a cap is configured, the final result is min(commission, cap).

        Args:
            quantity: Number of shares filled
            price: Fill price per share (required for per-trade percentage model)

        Returns:
            Commission amount

        Raises:
            ValueError: If quantity is negative
            ValueError: If price is required but not provided
            ValueError: If price is negative

        Examples:
            Per-share model:
            >>> config = CommissionConfig(per_share=Decimal("0.005"), minimum=Decimal("1.00"))
            >>> calc = CommissionCalculator(config)
            >>> calc.calculate(Decimal("100"))
            Decimal('1.00')
            >>> calc.calculate(Decimal("500"))
            Decimal('2.50')

            Per-trade flat fee:
            >>> config = CommissionConfig(flat_fee=Decimal("5.00"), minimum=Decimal("1.00"))
            >>> calc = CommissionCalculator(config)
            >>> calc.calculate(Decimal("100"))
            Decimal('5.00')

            Per-trade percentage:
            >>> config = CommissionConfig(percentage=Decimal("0.001"), minimum=Decimal("1.00"))
            >>> calc = CommissionCalculator(config)
            >>> calc.calculate(Decimal("100"), Decimal("50.00"))
            Decimal('5.00')

            Tiered model:
            >>> config = CommissionConfig(
            ...     tiers=[(Decimal("1000"), Decimal("0.01")),
            ...            (Decimal("inf"), Decimal("0.005"))],
            ...     minimum=Decimal("1.00")
            ... )
            >>> calc = CommissionCalculator(config)
            >>> calc.calculate(Decimal("500"))  # 500 * 0.01
            Decimal('5.00')
            >>> calc.calculate(Decimal("1500"))  # 1000 * 0.01 + 500 * 0.005
            Decimal('12.50')

            With commission cap:
            >>> config = CommissionConfig(per_share=Decimal("0.01"), cap=Decimal("50.00"))
            >>> calc = CommissionCalculator(config)
            >>> calc.calculate(Decimal("10000"))  # Would be $100, capped at $50
            Decimal('50.00')
        """
        if quantity < 0:
            raise ValueError(f"Quantity cannot be negative, got {quantity}")
        if price is not None and price < 0:
            raise ValueError(f"Price cannot be negative, got {price}")

        # Calculate base commission based on model
        if self.config.per_share is not None:
            commission = self._calculate_per_share(quantity)
        elif self.config.flat_fee is not None:
            commission = self._calculate_flat_fee()
        elif self.config.percentage is not None:
            commission = self._calculate_percentage(quantity, price)
        elif self.config.tiers is not None:
            commission = self._calculate_tiered(quantity)
        else:
            raise ValueError("No commission model configured")

        # Apply minimum
        commission = max(commission, self.config.minimum)

        # Apply cap if configured
        if self.config.cap is not None:
            commission = min(commission, self.config.cap)

        return commission

    def _calculate_per_share(self, quantity: Decimal) -> Decimal:
        """Calculate per-share commission.

        Args:
            quantity: Number of shares

        Returns:
            Commission amount
        """
        assert self.config.per_share is not None
        return quantity * self.config.per_share

    def _calculate_flat_fee(self) -> Decimal:
        """Calculate flat fee commission.

        Returns:
            Commission amount
        """
        assert self.config.flat_fee is not None
        return self.config.flat_fee

    def _calculate_percentage(self, quantity: Decimal, price: Decimal | None) -> Decimal:
        """Calculate percentage-based commission.

        Args:
            quantity: Number of shares
            price: Price per share

        Returns:
            Commission amount

        Raises:
            ValueError: If price is not provided
        """
        assert self.config.percentage is not None
        if price is None:
            raise ValueError("Price is required for percentage-based commission")

        notional = quantity * price
        return notional * self.config.percentage

    def _calculate_tiered(self, quantity: Decimal) -> Decimal:
        """Calculate tiered commission based on volume brackets.

        Args:
            quantity: Number of shares

        Returns:
            Commission amount

        Example:
            Tiers: [(1000, 0.01), (5000, 0.005), (inf, 0.003)]
            Quantity: 3000
            - First 1000: 1000 * 0.01 = $10.00
            - Next 2000: 2000 * 0.005 = $10.00
            - Total: $20.00
        """
        assert self.config.tiers is not None

        commission = Decimal("0")
        remaining_qty = quantity
        prev_max_qty = Decimal("0")

        for max_qty, rate in self.config.tiers:
            # Calculate quantity in this tier
            tier_max = max_qty - prev_max_qty
            qty_in_tier = min(remaining_qty, tier_max)

            # Add commission for this tier
            commission += qty_in_tier * rate

            # Update remaining quantity
            remaining_qty -= qty_in_tier

            # If no more quantity, done
            if remaining_qty <= 0:
                break

            prev_max_qty = max_qty

        return commission
