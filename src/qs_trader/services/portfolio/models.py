"""Data models for portfolio service.

Defines all core entities for portfolio accounting:
- Lot: Individual position building block
- Position: Aggregate position view
- LedgerEntry: Audit trail record
- PortfolioState: Complete portfolio snapshot
- PortfolioConfig: Service configuration
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator


class LotSide(str, Enum):
    """Side of lot position."""

    LONG = "long"
    SHORT = "short"


class Lot(BaseModel):
    """
    Individual lot representing a single trade.

    Used for FIFO/LIFO accounting. Each fill creates one or more lots.
    Multiple lots can exist for same symbol.

    Attributes:
        lot_id: Unique identifier
        symbol: Ticker symbol
        side: Long or short
        quantity: Shares in lot (positive for long, negative for short)
        entry_price: Price per share when opened
        entry_timestamp: When lot was created
        entry_fill_id: Fill that created this lot
        entry_commission: Commission allocated to this lot (not in cost basis)
        realized_pnl: Accumulated P&L as lot closes (partial or full)

    Example:
        >>> lot = Lot(
        ...     lot_id="lot_001",
        ...     symbol="AAPL",
        ...     side=LotSide.LONG,
        ...     quantity=Decimal("100"),
        ...     entry_price=Decimal("150.00"),
        ...     entry_timestamp=datetime.now(),
        ...     entry_fill_id="fill_001"
        ... )
    """

    lot_id: str = Field(default_factory=lambda: str(uuid4()))
    symbol: str
    side: LotSide
    quantity: Decimal  # Positive for long, negative for short
    entry_price: Decimal  # Price per share when opened
    entry_timestamp: datetime
    entry_fill_id: str  # Reference to fill that created this lot

    # Fees allocated to this lot (NOT in cost basis)
    entry_commission: Decimal = Decimal("0")

    # For P&L calculation (updated as lot closes)
    realized_pnl: Decimal = Decimal("0")

    @field_validator("quantity")
    @classmethod
    def validate_quantity(cls, v: Decimal) -> Decimal:
        """Validate quantity is non-zero."""
        if v == 0:
            raise ValueError("Lot quantity cannot be zero")
        return v

    @field_validator("entry_price")
    @classmethod
    def validate_entry_price(cls, v: Decimal) -> Decimal:
        """Validate entry price is positive."""
        if v <= 0:
            raise ValueError(f"Entry price must be positive, got {v}")
        return v

    model_config = ConfigDict(frozen=True)  # Immutable after creation


class Position(BaseModel):
    """
    Aggregate position for a symbol.

    Derived from lots - represents total position across all lots.

    Attributes:
        symbol: Ticker symbol
        quantity: Total shares (positive=long, negative=short, zero=flat)
        lots: All open lots for this position
        total_cost: Sum of (lot.quantity * lot.entry_price)
        avg_price: Average entry price (total_cost / quantity)
        current_price: Last known market price
        market_value: Current position value (quantity * current_price)
        unrealized_pnl: Current unrealized P&L (market_value - total_cost)
        strategy_id: Strategy that owns this position
        realized_pl: Realized P&L for this symbol (lifetime, symbol-scoped)
        dividends_received: Cumulative dividends received (longs)
        dividends_paid: Cumulative dividends paid (shorts)
        commission_paid: Total commission paid on current open position
        last_updated: Last update timestamp

    Example:
        >>> position = Position(
        ...     symbol="AAPL",
        ...     quantity=Decimal("100"),
        ...     lots=[lot1, lot2],
        ...     total_cost=Decimal("15000"),
        ...     avg_price=Decimal("150.00"),
        ...     current_price=Decimal("155.00"),
        ...     strategy_id="momentum_strategy",
        ...     last_updated=datetime.now()
        ... )
    """

    symbol: str
    quantity: Decimal  # Total shares (positive=long, negative=short)
    lots: list[Lot] = Field(default_factory=list)

    # Aggregate values
    total_cost: Decimal = Decimal("0")  # Sum of (lot.quantity * lot.entry_price)
    avg_price: Decimal = Decimal("0")  # total_cost / quantity

    # Current valuation
    current_price: Decimal | None = None
    market_value: Decimal = Decimal("0")  # quantity * current_price
    unrealized_pnl: Decimal = Decimal("0")  # market_value - total_cost

    # Strategy attribution
    strategy_id: str | None = None  # Strategy that owns this position

    # P&L tracking
    realized_pl: Decimal = Decimal("0")  # Lifetime realized P&L for this symbol

    # Dividend tracking
    dividends_received: Decimal = Decimal("0")  # Cumulative dividends from longs
    dividends_paid: Decimal = Decimal("0")  # Cumulative dividends paid on shorts

    # Commission tracking
    commission_paid: Decimal = Decimal("0")  # Total commission on current open position

    # Metadata
    last_updated: datetime = Field(default_factory=datetime.now)

    @property
    def side(self) -> Literal["long", "short", "flat"]:
        """Position side based on quantity."""
        if self.quantity > 0:
            return "long"
        elif self.quantity < 0:
            return "short"
        else:
            return "flat"

    def update_market_value(self, price: Decimal) -> None:
        """
        Update position with new market price.

        Recalculates market_value and unrealized_pnl.

        Args:
            price: New market price per share
        """
        self.current_price = price
        self.market_value = self.quantity * price
        self.unrealized_pnl = self.market_value - self.total_cost
        self.last_updated = datetime.now()

    model_config = ConfigDict(arbitrary_types_allowed=True)  # NOT frozen - mutable


class LedgerEntryType(str, Enum):
    """Type of ledger entry."""

    FILL = "fill"
    DIVIDEND = "dividend"
    SPLIT = "split"
    BORROW_FEE = "borrow_fee"
    MARGIN_INTEREST = "margin_interest"
    COMMISSION = "commission"
    MARK_TO_MARKET = "mark_to_market"


class LedgerEntry(BaseModel):
    """
    Single entry in portfolio ledger.

    Records all economic events affecting portfolio.
    Complete audit trail for compliance, debugging, replay.

    Attributes:
        entry_id: Unique identifier
        timestamp: When event occurred
        entry_type: Type of economic event
        symbol: Ticker (if applicable)
        quantity: Shares affected (if applicable)
        price: Price per share (if applicable)
        cash_flow: Net cash impact (positive=in, negative=out)
        commission: Commission paid (tracked separately)
        realized_pnl: Realized P&L from this event (if applicable)
        fill_id: Reference to fill (if applicable)
        lot_ids: Lots affected by this entry
        description: Human-readable description
        metadata: Additional context

    Example:
        >>> entry = LedgerEntry(
        ...     entry_id="entry_001",
        ...     timestamp=datetime.now(),
        ...     entry_type=LedgerEntryType.FILL,
        ...     symbol="AAPL",
        ...     quantity=Decimal("100"),
        ...     price=Decimal("150.00"),
        ...     cash_flow=Decimal("-15001.00"),
        ...     commission=Decimal("1.00"),
        ...     description="Buy 100 AAPL @ $150.00"
        ... )
    """

    entry_id: str = Field(default_factory=lambda: str(uuid4()))
    timestamp: datetime
    entry_type: LedgerEntryType

    # Transaction details (if applicable)
    symbol: str | None = None
    quantity: Decimal | None = None
    price: Decimal | None = None

    # Cash flow (positive = cash in, negative = cash out)
    cash_flow: Decimal

    # Fees (tracked separately from cost basis)
    commission: Decimal = Decimal("0")

    # P&L (if applicable)
    realized_pnl: Decimal | None = None

    # References
    fill_id: str | None = None
    lot_ids: list[str] = Field(default_factory=list)

    # Metadata
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(frozen=True)  # Immutable after creation


class PortfolioState(BaseModel):
    """
    Immutable snapshot of portfolio at a point in time.

    Used for:
    - Risk evaluation
    - Reporting
    - Historical analysis

    Attributes:
        timestamp: Snapshot time
        cash: Current cash balance
        positions: All positions (symbol → Position)
        equity: Total portfolio value (cash + market_value)
        market_value: Sum of all position values
        realized_pnl: Total realized P&L from inception
        unrealized_pnl: Current unrealized P&L across all positions
        total_pnl: realized + unrealized
        long_exposure: Sum of long position values
        short_exposure: Sum of short position values (absolute)
        net_exposure: long - short
        gross_exposure: long + short
        leverage: gross_exposure / equity (if equity > 0)
        total_commissions: Cumulative commissions paid
        total_borrow_fees: Cumulative borrow fees on shorts
        total_margin_interest: Cumulative margin interest on negative cash
        total_dividends_received: Cumulative dividends from longs
        total_dividends_paid: Cumulative dividends paid on shorts

    Example:
        >>> state = portfolio.get_state()
        >>> print(f"Equity: ${state.equity}")
        >>> print(f"Leverage: {state.leverage}x")
    """

    timestamp: datetime

    # Cash
    cash: Decimal

    # Positions
    positions: dict[tuple[str, str], Position]  # (strategy_id, symbol) → Position

    # Valuations
    equity: Decimal  # Total portfolio value
    market_value: Decimal  # Sum of all position values

    # P&L
    realized_pnl: Decimal  # Total realized (from inception)
    unrealized_pnl: Decimal  # Current unrealized
    total_pnl: Decimal  # realized + unrealized

    # Exposures
    long_exposure: Decimal  # Sum of long position values
    short_exposure: Decimal  # Sum of short position values (absolute)
    net_exposure: Decimal  # long - short
    gross_exposure: Decimal  # long + short

    # Leverage
    leverage: Decimal  # gross_exposure / equity (if equity > 0)

    # Fees & Interest (cumulative)
    total_commissions: Decimal
    total_borrow_fees: Decimal
    total_margin_interest: Decimal
    total_dividends_received: Decimal
    total_dividends_paid: Decimal

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)  # Immutable snapshot


class PortfolioConfig(BaseModel):
    """
    Configuration for portfolio service.

    Controls initial capital, fee rates, accounting policies, and price field selection.

    Attributes:
        portfolio_id: Unique portfolio identifier
        start_datetime: Portfolio inception timestamp (UTC)
        initial_cash: Starting capital
        reporting_currency: ISO 4217 currency code (e.g., USD, EUR)
        default_commission_per_share: Default per-share commission
        default_commission_pct: Default commission as % of notional
        default_borrow_rate_apr: Default annual borrow rate for shorts
        borrow_rate_by_symbol: Symbol-specific borrow rates
        margin_rate_apr: Annual interest rate on negative cash
        day_count_convention: Days in year for interest (360 or 365)
        lot_method_long: Lot matching for longs ("fifo" only in Phase 2)
        lot_method_short: Lot matching for shorts ("lifo" only in Phase 2)
        keep_position_history: Keep flat positions for reporting
        max_ledger_entries: Max ledger size (0 = unlimited)
        adjustment_mode: Adjustment mode for valuation and mark-to-market.
            'split_adjusted' = use close field (process dividend cash-ins),
            'total_return' = use close_adj field (skip dividend cash-ins).
            Default: 'split_adjusted'

    Example:
        >>> config = PortfolioConfig(
        ...     initial_cash=Decimal("100000"),
        ...     default_borrow_rate_apr=Decimal("0.05"),
        ...     margin_rate_apr=Decimal("0.07")
        ... )
    """

    # Portfolio metadata
    portfolio_id: str = Field(default_factory=lambda: str(uuid4()))
    start_datetime: datetime = Field(default_factory=lambda: datetime.now())
    reporting_currency: str = "USD"

    # Starting capital
    initial_cash: Decimal = Decimal("100000.00")

    # Commission rates (defaults, can be overridden per fill)
    default_commission_per_share: Decimal = Decimal("0.00")
    default_commission_pct: Decimal = Decimal("0.00")  # As decimal (0.001 = 0.1%)

    # Borrow fees for short selling (annual rates)
    default_borrow_rate_apr: Decimal = Decimal("0.05")  # 5% annual
    borrow_rate_by_symbol: dict[str, Decimal] = Field(default_factory=dict)

    # Margin interest on negative cash (annual rate)
    margin_rate_apr: Decimal = Decimal("0.07")  # 7% annual

    # Day count convention for interest calculations
    day_count_convention: int = 360  # 360 or 365

    # Lot matching methods
    lot_method_long: str = "fifo"  # "fifo" only for Phase 2
    lot_method_short: str = "lifo"  # "lifo" only for Phase 2

    # Position history
    keep_position_history: bool = True  # Keep positions at zero for reporting

    # Ledger settings
    max_ledger_entries: int = 0  # 0 = unlimited

    # Adjustment mode selection (Group 2 configuration)
    adjustment_mode: str = "split_adjusted"  # "split_adjusted" or "total_return"

    @field_validator("adjustment_mode")
    @classmethod
    def validate_adjustment_mode(cls, v: str) -> str:
        """Validate adjustment mode is supported."""
        allowed = {"split_adjusted", "total_return"}
        if v not in allowed:
            raise ValueError(f"adjustment_mode must be one of {allowed}, got: {v}")
        return v

    @field_validator("initial_cash")
    @classmethod
    def validate_initial_cash(cls, v: Decimal) -> Decimal:
        """Validate initial cash is positive."""
        if v <= 0:
            raise ValueError(f"Initial cash must be positive, got {v}")
        return v

    @field_validator("day_count_convention")
    @classmethod
    def validate_day_count(cls, v: int) -> int:
        """Validate day count convention."""
        if v not in (360, 365):
            raise ValueError(f"Day count must be 360 or 365, got {v}")
        return v

    @field_validator("lot_method_long")
    @classmethod
    def validate_lot_method_long(cls, v: str) -> str:
        """Validate lot method for longs."""
        if v != "fifo":
            raise ValueError(f"Phase 2 only supports 'fifo' for longs, got '{v}'")
        return v

    @field_validator("lot_method_short")
    @classmethod
    def validate_lot_method_short(cls, v: str) -> str:
        """Validate lot method for shorts."""
        if v != "lifo":
            raise ValueError(f"Phase 2 only supports 'lifo' for shorts, got '{v}'")
        return v

    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)  # Immutable after creation


class Ledger:
    """
    Portfolio ledger for complete audit trail.

    Records all economic events in chronological order.
    Supports querying by time, type, symbol.

    Example:
        >>> ledger = Ledger()
        >>> ledger.add_entry(entry)
        >>> fills = ledger.get_entries(entry_type=LedgerEntryType.FILL)
    """

    def __init__(self, max_entries: int = 0):
        """
        Initialize ledger.

        Args:
            max_entries: Maximum entries to keep (0 = unlimited)
        """
        self._entries: list[LedgerEntry] = []
        self._max_entries = max_entries

    def add_entry(self, entry: LedgerEntry) -> None:
        """
        Add entry to ledger.

        Entries are stored in chronological order.
        If max_entries exceeded, oldest entries are removed.

        Args:
            entry: Ledger entry to add

        Raises:
            ValueError: If entry_id already exists
        """
        # Check for duplicate entry_id
        if any(e.entry_id == entry.entry_id for e in self._entries):
            raise ValueError(f"Entry ID {entry.entry_id} already exists in ledger")

        # Add entry
        self._entries.append(entry)

        # Enforce max size
        if self._max_entries > 0 and len(self._entries) > self._max_entries:
            self._entries = self._entries[-self._max_entries :]

    def get_entries(
        self,
        since: datetime | None = None,
        entry_type: LedgerEntryType | None = None,
        symbol: str | None = None,
    ) -> list[LedgerEntry]:
        """
        Query ledger entries with filters.

        Args:
            since: Only return entries after this time
            entry_type: Filter by entry type
            symbol: Filter by symbol

        Returns:
            List of entries matching filters (chronological order)
        """
        entries = self._entries

        # Filter by time
        if since is not None:
            entries = [e for e in entries if e.timestamp >= since]

        # Filter by type
        if entry_type is not None:
            entries = [e for e in entries if e.entry_type == entry_type]

        # Filter by symbol
        if symbol is not None:
            entries = [e for e in entries if e.symbol == symbol]

        return entries

    def get_entry_count(self) -> int:
        """Get total number of entries."""
        return len(self._entries)

    def clear(self) -> None:
        """Clear all entries (useful for testing)."""
        self._entries.clear()
