"""Portfolio service interface (Protocol).

Defines the contract that all portfolio service implementations must satisfy.
Enables dependency injection and makes the service independently testable.
"""

from datetime import datetime
from decimal import Decimal
from typing import Literal, Protocol

from qs_trader.services.portfolio.models import LedgerEntry, LedgerEntryType, PortfolioState, Position


class IPortfolioService(Protocol):
    """
    Portfolio service interface for position and cash management.

    Implements lot-based accounting with complete audit trail.
    Deterministic replay capability for backtesting.

    Core responsibilities:
    - Process fills (buy/sell) with lot accounting
    - Track cash balance
    - Calculate realized and unrealized P&L
    - Process corporate actions (splits, dividends)
    - Accrue fees and interest
    - Maintain complete ledger
    - Provide portfolio state snapshots

    Example:
        >>> portfolio: IPortfolioService = PortfolioService(config)
        >>> portfolio.apply_fill(
        ...     fill_id="fill_001",
        ...     timestamp=datetime.now(),
        ...     symbol="AAPL",
        ...     side="buy",
        ...     quantity=Decimal("100"),
        ...     price=Decimal("150.00")
        ... )
        >>> print(f"Equity: ${portfolio.get_equity()}")
    """

    # ==================== Fill Processing ====================

    def apply_fill(
        self,
        fill_id: str,
        timestamp: datetime,
        symbol: str,
        side: Literal["buy", "sell"],
        quantity: Decimal,
        price: Decimal,
        commission: Decimal = Decimal("0"),
    ) -> None:
        """
        Apply fill to portfolio.

        Processing:
        1. Validate inputs (quantity > 0, price > 0, commission >= 0)
        2. Determine if opening or closing position
        3. For closes: Match lots using FIFO (long) or LIFO (short)
        4. Calculate realized P&L for closed lots
        5. Update cash balance
        6. Record in ledger

        Args:
            fill_id: Unique identifier for this fill
            timestamp: When fill occurred
            symbol: Ticker symbol
            side: "buy" or "sell"
            quantity: Number of shares (positive)
            price: Price per share
            commission: Commission paid (separate from cost basis)

        Raises:
            ValueError: If quantity/price invalid
            ValueError: If attempting to close more than available
            ValueError: If duplicate fill_id

        Example:
            >>> # Open long position
            >>> portfolio.apply_fill(
            ...     fill_id="fill_001",
            ...     timestamp=datetime(2020, 1, 2, 9, 30),
            ...     symbol="AAPL",
            ...     side="buy",
            ...     quantity=Decimal("100"),
            ...     price=Decimal("150.00"),
            ...     commission=Decimal("1.00")
            ... )
        """
        ...

    # ==================== Market Data ====================

    def update_prices(self, prices: dict[str, Decimal]) -> None:
        """
        Update mark-to-market prices (intraday).

        Updates position market values and unrealized P&L without creating
        ledger entries. Use this for intraday price updates during backtesting.

        For end-of-day valuation with fee accruals, use mark_to_market() instead.

        Args:
            prices: Dict mapping symbol → current price

        Example:
            >>> # Intraday price update
            >>> portfolio.update_prices({"AAPL": Decimal("150.25")})
            >>> print(portfolio.get_unrealized_pnl())
        """
        ...

    def mark_to_market(self, timestamp: datetime) -> None:
        """
        Perform end-of-day mark-to-market valuation.

        This is the comprehensive EOD process that:
        1. Updates all position values with current prices
        2. Calculates unrealized P&L
        3. Accrues borrow fees on short positions
        4. Accrues margin interest on negative cash
        5. Creates ledger entries for all fees/interest

        Should be called once per trading day at market close.

        Args:
            timestamp: Time of mark (typically end of day, e.g., 16:00)

        Example:
            >>> # End of day processing
            >>> portfolio.mark_to_market(datetime(2020, 1, 2, 16, 0))
        """
        ...

    # ==================== Corporate Actions ====================

    def process_split(
        self,
        symbol: str,
        split_date: datetime,
        ratio: Decimal,
    ) -> None:
        """
        Process stock split or reverse split.

        Adjusts all lots for this symbol:
        - quantity = quantity * ratio
        - entry_price = entry_price / ratio
        - Total value preserved

        Works for both long and short positions.

        Args:
            symbol: Symbol splitting
            split_date: Date of split
            ratio: Split ratio (4.0 for 4-for-1, 0.25 for 1-for-4 reverse)

        Raises:
            ValueError: If ratio is zero or negative

        Example:
            >>> # 4-for-1 split
            >>> portfolio.process_split(
            ...     symbol="AAPL",
            ...     split_date=datetime(2020, 8, 31),
            ...     ratio=Decimal("4.0")
            ... )
        """
        ...

    def process_dividend(
        self,
        symbol: str,
        ex_date: datetime,
        amount_per_share: Decimal,
    ) -> None:
        """
        Process cash dividend.

        For long positions: Cash increases (income)
        For short positions: Cash decreases (expense)

        Args:
            symbol: Symbol paying dividend
            ex_date: Ex-dividend date
            amount_per_share: Dividend per share

        Raises:
            ValueError: If amount is negative

        Example:
            >>> # Process dividend
            >>> portfolio.process_dividend(
            ...     symbol="AAPL",
            ...     ex_date=datetime(2020, 2, 7),
            ...     amount_per_share=Decimal("0.82")
            ... )
        """
        ...

    # ==================== Queries ====================

    def get_position(self, symbol: str) -> Position | None:
        """
        Get current position for symbol.

        Returns None if no position (or position is flat and history disabled).

        Args:
            symbol: Ticker symbol

        Returns:
            Position or None

        Example:
            >>> position = portfolio.get_position("AAPL")
            >>> if position:
            ...     print(f"Quantity: {position.quantity}")
        """
        ...

    def get_positions(self) -> dict[str, Position]:
        """
        Get all current positions.

        Returns:
            Dict mapping symbol → Position (includes flat if keep_history=True)

        Example:
            >>> positions = portfolio.get_positions()
            >>> for symbol, position in positions.items():
            ...     print(f"{symbol}: {position.quantity} shares")
        """
        ...

    def get_cash(self) -> Decimal:
        """
        Get current cash balance.

        Can be negative (margin loan).

        Returns:
            Current cash

        Example:
            >>> cash = portfolio.get_cash()
            >>> print(f"Cash: ${cash}")
        """
        ...

    def get_equity(self) -> Decimal:
        """
        Calculate total portfolio equity.

        Equity = Cash + Sum(position market values)

        Returns:
            Total equity

        Example:
            >>> equity = portfolio.get_equity()
            >>> print(f"Total Equity: ${equity}")
        """
        ...

    def get_state(self) -> PortfolioState:
        """
        Get complete portfolio state snapshot.

        Returns immutable snapshot with all metrics:
        - Cash and positions
        - Equity and valuations
        - Realized/unrealized P&L
        - Exposures (long, short, net, gross)
        - Leverage
        - Cumulative fees/interest/dividends

        Returns:
            Immutable state for reporting, risk, etc.

        Example:
            >>> state = portfolio.get_state()
            >>> print(f"Equity: ${state.equity}")
            >>> print(f"Leverage: {state.leverage}x")
            >>> print(f"Total P&L: ${state.total_pnl}")
        """
        ...

    def get_ledger(
        self,
        since: datetime | None = None,
        entry_types: list[LedgerEntryType] | None = None,
    ) -> list[LedgerEntry]:
        """
        Get ledger entries.

        Args:
            since: Only return entries after this time (optional)
            entry_types: Filter by entry type (optional)

        Returns:
            List of ledger entries in chronological order

        Example:
            >>> # Get all fills
            >>> fills = portfolio.get_ledger(
            ...     entry_types=[LedgerEntryType.FILL]
            ... )
            >>>
            >>> # Get entries since start of day
            >>> today = datetime(2020, 1, 2, 0, 0)
            >>> entries = portfolio.get_ledger(since=today)
        """
        ...

    def get_realized_pnl(
        self,
        symbol: str | None = None,
        since: datetime | None = None,
    ) -> Decimal:
        """
        Get realized P&L.

        Args:
            symbol: Specific symbol or None for total
            since: Since this timestamp or None for all time

        Returns:
            Total realized P&L

        Example:
            >>> # Total realized P&L
            >>> total_pnl = portfolio.get_realized_pnl()
            >>>
            >>> # Realized P&L for AAPL only
            >>> aapl_pnl = portfolio.get_realized_pnl(symbol="AAPL")
        """
        ...

    def get_unrealized_pnl(
        self,
        symbol: str | None = None,
    ) -> Decimal:
        """
        Get unrealized P&L.

        Args:
            symbol: Specific symbol or None for total

        Returns:
            Current unrealized P&L

        Example:
            >>> # Total unrealized P&L
            >>> unrealized = portfolio.get_unrealized_pnl()
            >>>
            >>> # Unrealized P&L for AAPL
            >>> aapl_unrealized = portfolio.get_unrealized_pnl(symbol="AAPL")
        """
        ...
