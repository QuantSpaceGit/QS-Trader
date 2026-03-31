"""Slippage models for realistic fill simulation.

Provides multiple slippage calculation strategies to model market impact
and execution costs in backtesting.
"""

from abc import ABC, abstractmethod
from decimal import Decimal
from enum import Enum

from qs_trader.services.data.models import Bar
from qs_trader.services.execution.models import Order, OrderSide


class SlippageModel(str, Enum):
    """Slippage model types."""

    FIXED_BPS = "fixed_bps"
    VOLUME_BASED = "volume_based"
    SPREAD_BASED = "spread_based"
    TIME_OF_DAY = "time_of_day"


class ISlippageCalculator(ABC):
    """Interface for slippage calculation strategies."""

    @abstractmethod
    def calculate(self, order: Order, bar: Bar, fill_quantity: Decimal, base_price: Decimal) -> Decimal:
        """Calculate slippage-adjusted fill price.

        Args:
            order: Order being filled
            bar: Current bar data
            fill_quantity: Quantity being filled
            base_price: Base price before slippage

        Returns:
            Slippage-adjusted price (base_price ± slippage)
        """
        ...


class FixedBpsSlippage(ISlippageCalculator):
    """Fixed basis points slippage model.

    Applies constant slippage regardless of order size or market conditions.
    Simple and fast, suitable for basic backtesting.

    Buy orders pay more: price * (1 + bps/10000)
    Sell orders receive less: price * (1 - bps/10000)

    Attributes:
        bps: Slippage in basis points (5 = 0.05%)
    """

    def __init__(self, bps: Decimal) -> None:
        """Initialize fixed BPS slippage calculator.

        Args:
            bps: Slippage in basis points (e.g., 5 for 0.05%)

        Raises:
            ValueError: If bps is negative
        """
        if bps < 0:
            raise ValueError(f"BPS cannot be negative, got {bps}")
        self.bps = bps

    def calculate(self, order: Order, bar: Bar, fill_quantity: Decimal, base_price: Decimal) -> Decimal:
        """Calculate fixed BPS slippage.

        Args:
            order: Order being filled
            bar: Current bar data (not used in fixed model)
            fill_quantity: Quantity being filled (not used in fixed model)
            base_price: Base price before slippage

        Returns:
            Slippage-adjusted price
        """
        if self.bps == 0:
            return base_price

        multiplier = Decimal("1") + (self.bps / Decimal("10000"))

        if order.side == OrderSide.BUY:
            # Buy pays more
            return base_price * multiplier
        else:
            # Sell receives less: price * (2 - multiplier)
            return base_price * (Decimal("2") - multiplier)


class VolumeBasedSlippage(ISlippageCalculator):
    """Volume-based slippage model.

    Slippage increases with order size relative to bar volume.
    Models market impact: larger orders move the price more.

    Slippage = base_bps + (fill_qty / bar_volume) * impact_factor

    Attributes:
        base_bps: Base slippage in basis points
        impact_factor: Additional slippage per 100% of volume (in bps)
    """

    def __init__(self, base_bps: Decimal, impact_factor: Decimal) -> None:
        """Initialize volume-based slippage calculator.

        Args:
            base_bps: Base slippage in basis points
            impact_factor: Additional bps per 100% of bar volume

        Raises:
            ValueError: If base_bps or impact_factor is negative

        Example:
            >>> # Base 5bps + 10bps per 100% of volume
            >>> calc = VolumeBasedSlippage(Decimal("5"), Decimal("10"))
            >>> # Filling 10% of volume: 5 + 0.1 * 10 = 6 bps
            >>> # Filling 50% of volume: 5 + 0.5 * 10 = 10 bps
        """
        if base_bps < 0:
            raise ValueError(f"Base BPS cannot be negative, got {base_bps}")
        if impact_factor < 0:
            raise ValueError(f"Impact factor cannot be negative, got {impact_factor}")

        self.base_bps = base_bps
        self.impact_factor = impact_factor

    def calculate(self, order: Order, bar: Bar, fill_quantity: Decimal, base_price: Decimal) -> Decimal:
        """Calculate volume-based slippage.

        Args:
            order: Order being filled
            bar: Current bar data (provides volume)
            fill_quantity: Quantity being filled
            base_price: Base price before slippage

        Returns:
            Slippage-adjusted price
        """
        # Calculate participation rate
        if bar.volume == 0:
            # No volume data, use base slippage
            participation_rate = Decimal("0")
        else:
            participation_rate = fill_quantity / bar.volume

        # Calculate total slippage
        total_bps = self.base_bps + (participation_rate * self.impact_factor)

        # Apply slippage
        multiplier = Decimal("1") + (total_bps / Decimal("10000"))

        if order.side == OrderSide.BUY:
            return base_price * multiplier
        else:
            return base_price * (Decimal("2") - multiplier)


class SpreadBasedSlippage(ISlippageCalculator):
    """Spread-based slippage model.

    Uses bid-ask spread from bar data to estimate slippage.
    More realistic for markets with observable spreads.

    If spread data available: use half-spread as slippage
    If spread data missing: fall back to fixed BPS

    Attributes:
        fallback_bps: Fallback slippage when spread data unavailable
        spread_fraction: Fraction of spread to use (default 0.5 = half-spread)
    """

    def __init__(
        self,
        fallback_bps: Decimal,
        spread_fraction: Decimal = Decimal("0.5"),
    ) -> None:
        """Initialize spread-based slippage calculator.

        Args:
            fallback_bps: Fallback slippage in basis points
            spread_fraction: Fraction of spread to use (0.5 = half-spread)

        Raises:
            ValueError: If fallback_bps is negative
            ValueError: If spread_fraction not in [0, 1]
        """
        if fallback_bps < 0:
            raise ValueError(f"Fallback BPS cannot be negative, got {fallback_bps}")
        if not (0 <= spread_fraction <= 1):
            raise ValueError(f"Spread fraction must be in [0, 1], got {spread_fraction}")

        self.fallback_bps = fallback_bps
        self.spread_fraction = spread_fraction

    def calculate(self, order: Order, bar: Bar, fill_quantity: Decimal, base_price: Decimal) -> Decimal:
        """Calculate spread-based slippage.

        Args:
            order: Order being filled
            bar: Current bar data (provides high-low as spread proxy)
            fill_quantity: Quantity being filled (not used)
            base_price: Base price before slippage

        Returns:
            Slippage-adjusted price
        """
        # Use high-low range as spread proxy
        # Real implementation could use actual bid-ask if available
        spread = Decimal(str(bar.high)) - Decimal(str(bar.low))

        if spread == 0:
            # No spread data, use fallback
            multiplier = Decimal("1") + (self.fallback_bps / Decimal("10000"))
        else:
            # Calculate slippage as fraction of spread
            # Convert to BPS: (spread_fraction * spread / price) * 10000
            spread_bps = self.spread_fraction * spread / base_price * Decimal("10000")
            multiplier = Decimal("1") + (spread_bps / Decimal("10000"))

        if order.side == OrderSide.BUY:
            return base_price * multiplier
        else:
            return base_price * (Decimal("2") - multiplier)


class TimeOfDaySlippage(ISlippageCalculator):
    """Time-of-day slippage model.

    Adjusts slippage based on time of day:
    - Market open: Higher slippage (wider spreads, more volatility)
    - Mid-day: Lower slippage (normal conditions)
    - Market close: Higher slippage (closing auctions, urgency)

    Attributes:
        base_bps: Base slippage during normal hours
        open_multiplier: Multiplier for market open (e.g., 2.0 = 2x slippage)
        close_multiplier: Multiplier for market close (e.g., 1.5 = 1.5x slippage)
        open_minutes: Minutes after open to apply open_multiplier (default 30)
        close_minutes: Minutes before close to apply close_multiplier (default 30)
    """

    def __init__(
        self,
        base_bps: Decimal,
        open_multiplier: Decimal = Decimal("2.0"),
        close_multiplier: Decimal = Decimal("1.5"),
        open_minutes: int = 30,
        close_minutes: int = 30,
    ) -> None:
        """Initialize time-of-day slippage calculator.

        Args:
            base_bps: Base slippage in basis points
            open_multiplier: Slippage multiplier at market open
            close_multiplier: Slippage multiplier at market close
            open_minutes: Minutes after open for elevated slippage
            close_minutes: Minutes before close for elevated slippage

        Raises:
            ValueError: If base_bps is negative
            ValueError: If multipliers are less than 1.0
            ValueError: If minutes are negative
        """
        if base_bps < 0:
            raise ValueError(f"Base BPS cannot be negative, got {base_bps}")
        if open_multiplier < 1:
            raise ValueError(f"Open multiplier must be >= 1.0, got {open_multiplier}")
        if close_multiplier < 1:
            raise ValueError(f"Close multiplier must be >= 1.0, got {close_multiplier}")
        if open_minutes < 0:
            raise ValueError(f"Open minutes cannot be negative, got {open_minutes}")
        if close_minutes < 0:
            raise ValueError(f"Close minutes cannot be negative, got {close_minutes}")

        self.base_bps = base_bps
        self.open_multiplier = open_multiplier
        self.close_multiplier = close_multiplier
        self.open_minutes = open_minutes
        self.close_minutes = close_minutes

    def calculate(self, order: Order, bar: Bar, fill_quantity: Decimal, base_price: Decimal) -> Decimal:
        """Calculate time-of-day adjusted slippage.

        Args:
            order: Order being filled
            bar: Current bar data (provides timestamp)
            fill_quantity: Quantity being filled (not used)
            base_price: Base price before slippage

        Returns:
            Slippage-adjusted price

        Note:
            Currently uses a simplified model. In production, would check
            actual market hours and time relative to open/close.
        """
        # Simplified: use hour to determine time of day
        # Real implementation would check market hours calendar
        hour = bar.trade_datetime.hour
        minute = bar.trade_datetime.minute

        # Market hours: 9:30 AM - 4:00 PM ET (simplified as 9-16)
        # Apply open multiplier for first open_minutes (9:30-10:00)
        # Apply close multiplier for last close_minutes (3:30-4:00)

        multiplier = Decimal("1.0")

        # Market open (9:30-10:00): elevated slippage
        if hour == 9 and minute >= 30:
            # Within open period
            minutes_since_open = minute - 30
            if minutes_since_open <= self.open_minutes:
                multiplier = self.open_multiplier
        elif hour == 10 and minute < self.open_minutes:
            # Just after 10:00, still in open period
            minutes_since_open = 30 + minute
            if minutes_since_open <= self.open_minutes:
                multiplier = self.open_multiplier

        # Market close (3:30-4:00): elevated slippage
        elif hour == 15 and minute >= (60 - self.close_minutes):
            # Within close period
            multiplier = self.close_multiplier
        elif hour == 16 and minute == 0:
            # Exactly at close
            multiplier = self.close_multiplier

        # Apply multiplied slippage
        adjusted_bps = self.base_bps * multiplier
        price_multiplier = Decimal("1") + (adjusted_bps / Decimal("10000"))

        if order.side == OrderSide.BUY:
            return base_price * price_multiplier
        else:
            return base_price * (Decimal("2") - price_multiplier)


class SlippageCalculatorFactory:
    """Factory for creating slippage calculators from configuration."""

    @staticmethod
    def create(
        model: SlippageModel,
        **kwargs: Decimal | int,
    ) -> ISlippageCalculator:
        """Create slippage calculator from model type and parameters.

        Args:
            model: Slippage model type
            **kwargs: Model-specific parameters

        Returns:
            Configured slippage calculator

        Raises:
            ValueError: If model is unknown
            ValueError: If required parameters are missing

        Examples:
            Fixed BPS:
            >>> calc = SlippageCalculatorFactory.create(
            ...     SlippageModel.FIXED_BPS,
            ...     bps=Decimal("5")
            ... )

            Volume-based:
            >>> calc = SlippageCalculatorFactory.create(
            ...     SlippageModel.VOLUME_BASED,
            ...     base_bps=Decimal("5"),
            ...     impact_factor=Decimal("10")
            ... )

            Spread-based:
            >>> calc = SlippageCalculatorFactory.create(
            ...     SlippageModel.SPREAD_BASED,
            ...     fallback_bps=Decimal("5"),
            ...     spread_fraction=Decimal("0.5")
            ... )

            Time-of-day:
            >>> calc = SlippageCalculatorFactory.create(
            ...     SlippageModel.TIME_OF_DAY,
            ...     base_bps=Decimal("5"),
            ...     open_multiplier=Decimal("2.0"),
            ...     close_multiplier=Decimal("1.5")
            ... )
        """
        if model == SlippageModel.FIXED_BPS:
            if "bps" not in kwargs:
                raise ValueError("Fixed BPS model requires 'bps' parameter")
            return FixedBpsSlippage(bps=Decimal(str(kwargs["bps"])))

        elif model == SlippageModel.VOLUME_BASED:
            if "base_bps" not in kwargs or "impact_factor" not in kwargs:
                raise ValueError("Volume-based model requires 'base_bps' and 'impact_factor'")
            return VolumeBasedSlippage(
                base_bps=Decimal(str(kwargs["base_bps"])),
                impact_factor=Decimal(str(kwargs["impact_factor"])),
            )

        elif model == SlippageModel.SPREAD_BASED:
            if "fallback_bps" not in kwargs:
                raise ValueError("Spread-based model requires 'fallback_bps' parameter")
            spread_fraction = kwargs.get("spread_fraction", Decimal("0.5"))
            return SpreadBasedSlippage(
                fallback_bps=Decimal(str(kwargs["fallback_bps"])),
                spread_fraction=Decimal(str(spread_fraction)),
            )

        elif model == SlippageModel.TIME_OF_DAY:
            if "base_bps" not in kwargs:
                raise ValueError("Time-of-day model requires 'base_bps' parameter")
            return TimeOfDaySlippage(
                base_bps=Decimal(str(kwargs["base_bps"])),
                open_multiplier=Decimal(str(kwargs.get("open_multiplier", "2.0"))),
                close_multiplier=Decimal(str(kwargs.get("close_multiplier", "1.5"))),
                open_minutes=int(kwargs.get("open_minutes", 30)),
                close_minutes=int(kwargs.get("close_minutes", 30)),
            )

        else:
            raise ValueError(f"Unknown slippage model: {model}")
