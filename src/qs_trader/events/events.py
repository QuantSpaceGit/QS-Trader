"""
Event system for QS-Trader - Pydantic models validated against JSON Schema contracts.

Architecture:
    JSON Schema (*.v1.json) ← Source of truth (cross-language contract)
         ↓
    Pydantic Event ← Python implementation with automatic validation
         ↓
    Event Bus

All events inherit from Event base class which validates against JSON Schema.
Control events (barriers, lifecycle) don't require schemas.

Design Principles:
- Envelope and payload validated separately
- Schemas cached and pre-compiled for performance
- UTC timezone-aware timestamps (RFC3339 with Z)
- Field types aligned to wire contract (strings for decimals)
- event_type matches schema base name
"""

import json
from datetime import datetime, timezone
from decimal import Decimal
from functools import lru_cache
from importlib import resources
from typing import Any, ClassVar, Optional
from uuid import uuid4

import jsonschema
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import BaseModel, Field, field_serializer, field_validator, model_validator

# ============================================
# Constants and Enums
# ============================================

# Reserved envelope field names (excluded from payload validation)
RESERVED_ENVELOPE_KEYS = {
    "event_id",
    "event_type",
    "event_version",
    "occurred_at",
    "correlation_id",
    "causation_id",
    "source_service",
}

# Schema package path (single source of truth for imports)
SCHEMA_PACKAGE = "qs_trader.contracts.schemas"

# JavaScript-safe integer limit (2^53 - 1)
JS_SAFE_INTEGER_MAX = 9007199254740991


# ============================================
# Schema Loading & Caching
# ============================================


@lru_cache(maxsize=128)
def load_and_compile_schema(schema_name: str) -> Draft202012Validator:
    """
    Load and compile JSON Schema validator with caching.

    Uses importlib.resources for package-safe loading (works with wheels).

    Args:
        schema_name: Schema filename (e.g., "bar.v1.json")

    Returns:
        Pre-compiled validator with format checker

    Raises:
        FileNotFoundError: If schema file doesn't exist
    """
    try:
        schema_file = resources.files(SCHEMA_PACKAGE).joinpath(schema_name)
        with schema_file.open("r", encoding="utf-8") as f:
            schema = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(f"Schema not found: {schema_name} in package {SCHEMA_PACKAGE}")

    # Pre-compile validator with format checker for uuid, date-time, etc.
    return Draft202012Validator(schema, format_checker=FormatChecker())


@lru_cache(maxsize=8)
def load_envelope_schema() -> Draft202012Validator:
    """Load and compile envelope schema validator."""
    return load_and_compile_schema("envelope.v1.json")


# ============================================
# Base Event Classes
# ============================================


class BaseEvent(BaseModel):
    """
    Base for all events - provides envelope fields only.
    All events (including control/lifecycle) validate envelope.
    """

    # Envelope fields (validated against envelope.v1.json)
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    event_type: str = "base"
    event_version: int = Field(default=1, description="Schema major version")
    occurred_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc), description="UTC timestamp RFC3339"
    )
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    source_service: str = "unknown"

    model_config = {"frozen": True, "extra": "forbid"}

    @field_validator("occurred_at", mode="before")
    @classmethod
    def ensure_utc(cls, v: Any) -> datetime:
        """Ensure timestamp is UTC timezone-aware."""
        if isinstance(v, str):
            # Parse ISO string
            dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        elif isinstance(v, datetime):
            dt = v
        else:
            raise ValueError(f"Cannot parse datetime from {type(v)}: {v}")

        # Convert to UTC if needed
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        elif dt.tzinfo != timezone.utc:
            dt = dt.astimezone(timezone.utc)

        return dt

    @field_serializer("occurred_at")
    def _serialize_occurred_at(self, v: datetime) -> str:
        """Serialize datetime to RFC3339 with Z suffix."""
        return v.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    @model_validator(mode="after")
    def _validate_envelope(self) -> "BaseEvent":
        """
        Validate envelope fields against envelope.v1.json.

        All events (ValidatedEvent and ControlEvent) inherit this validation.

        Drops None values from optional fields before validation to avoid
        schema validation errors when fields are nullable.
        """
        envelope_validator = load_envelope_schema()

        # Build envelope data, excluding None values for optional fields
        envelope_data = {
            "event_id": self.event_id,
            "event_type": self.event_type,
            "event_version": self.event_version,
            "occurred_at": self.occurred_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source_service": self.source_service,
        }

        # Only include optional fields if they have values
        if self.correlation_id is not None:
            envelope_data["correlation_id"] = self.correlation_id
        if self.causation_id is not None:
            envelope_data["causation_id"] = self.causation_id

        try:
            envelope_validator.validate(envelope_data)
        except jsonschema.ValidationError as e:
            raise ValueError(
                f"{self.__class__.__name__} envelope validation failed (envelope.v1.json): {e.message}\n"
                f"Path: {list(e.path)}\n"
                f"Schema path: {list(e.schema_path)}"
            )

        return self


class ValidatedEvent(BaseEvent):
    """
    Base for domain events that require JSON Schema validation.

    Validates payload against domain-specific schema (e.g., bar.v1.json).
    Envelope validation inherited from BaseEvent.
    """

    # Class variable: override in subclasses (must match event_type)
    SCHEMA_BASE: ClassVar[Optional[str]] = None

    @model_validator(mode="after")
    def _validate_payload(self) -> "ValidatedEvent":
        """
        Validate payload fields against {SCHEMA_BASE}.v{event_version}.json.

        Envelope validation already done by BaseEvent._validate_envelope().

        Raises:
            ValueError: If validation fails, with full error context
        """
        # Skip if no schema specified (shouldn't happen for ValidatedEvent)
        if self.SCHEMA_BASE is None:
            raise ValueError(f"{self.__class__.__name__} must specify SCHEMA_BASE")

        # Extract contract name from SCHEMA_BASE (handles both "bar" and "data/bar")
        schema_base_name = self.SCHEMA_BASE.split("/")[-1]

        # Verify event_type matches contract name (not full path)
        if self.event_type != schema_base_name:
            raise ValueError(
                f"{self.__class__.__name__}: event_type '{self.event_type}' must equal contract name '{schema_base_name}' "
                f"(from SCHEMA_BASE '{self.SCHEMA_BASE}')"
            )

        # Serialize to dict for validation
        data = self.model_dump()

        # Extract payload (everything except envelope fields)
        payload_data = {k: v for k, v in data.items() if k not in RESERVED_ENVELOPE_KEYS}

        schema_file = f"{self.SCHEMA_BASE}.v{self.event_version}.json"

        try:
            payload_validator = load_and_compile_schema(schema_file)
            payload_validator.validate(payload_data)
        except FileNotFoundError as e:
            raise ValueError(f"{self.__class__.__name__}: Schema not found: {schema_file}") from e
        except jsonschema.ValidationError as e:
            raise ValueError(
                f"{self.__class__.__name__} payload validation failed against {schema_file}: {e.message}\n"
                f"Path: {list(e.path)}\n"
                f"Schema path: {list(e.schema_path)}\n"
                f"Failed value: {e.instance}"
            )

        return self


class ControlEvent(BaseEvent):
    """
    Base for control/lifecycle events that don't require payload validation.

    Only validates envelope, skips payload schema validation.
    Use for: barriers, lifecycle events, coordination signals.
    """

    pass


# ============================================
# data Events
# ============================================


class PriceBarEvent(ValidatedEvent):
    """
    Price bar event - validates against data/bar.v{version}.json.

    Wire format uses strings for decimals to avoid floating point issues.
    Pydantic auto-converts to Decimal for Python domain use.

    Price adjustment modes:
    - open/high/low/close: Split-adjusted only (CumulativeVolumeFactor)
      Shows real market prices with dividend drops on ex-dates.
      Use for: Trading signals, execution, realistic price action.

    - open_adj/high_adj/low_adj/close_adj: Total-return adjusted (CumulativePriceFactor)
      Smooths dividend drops, assumes reinvestment at ex-date.
      Use for: Performance analysis, long-term backtests, benchmarking.
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "data/bar"
    event_type: str = "bar"  # Must match SCHEMA_BASE

    # Domain fields (wire uses strings, Pydantic converts)
    symbol: str
    asset_class: str = "equity"
    interval: str = "1D"
    timestamp: str  # ISO8601 string on wire (UTC RFC3339)
    timestamp_local: Optional[str] = None  # RFC3339 with offset (e.g., 09:30:00-05:00)
    timezone: Optional[str] = None  # IANA timezone (e.g., America/New_York)

    # Split-adjusted prices (backward-adjusted using CumulativeVolumeFactor)
    open: Decimal  # String on wire, Decimal in Python
    high: Decimal
    low: Decimal
    close: Decimal

    # Total-return adjusted prices (backward-adjusted using CumulativePriceFactor)
    # Optional - only populated by adapters that support dividend adjustment
    open_adj: Optional[Decimal] = None
    high_adj: Optional[Decimal] = None
    low_adj: Optional[Decimal] = None
    close_adj: Optional[Decimal] = None

    volume: int
    price_currency: str = "USD"
    price_scale: int = 2
    source: str
    trace_id: Optional[str] = None

    @field_serializer("open", "high", "low", "close", "open_adj", "high_adj", "low_adj", "close_adj")
    def _serialize_decimal(self, v: Optional[Decimal]) -> Optional[str]:
        """Serialize Decimal to string for wire format with fixed precision."""
        if v is None:
            return None
        # Format with fixed precision based on price_scale (default 2 decimals)
        return f"{v:.{self.price_scale}f}"

    @field_serializer("volume")
    def _serialize_volume(self, v: Optional[int]) -> Optional[int | str]:
        """
        Serialize volume as int if within JavaScript safe integer range,
        otherwise as string to prevent precision loss in JS consumers.

        JavaScript safe integer: 2^53 - 1 = 9,007,199,254,740,991
        """
        if v is None:
            return None
        if v > JS_SAFE_INTEGER_MAX:
            return str(v)
        return v


class CorporateActionEvent(ValidatedEvent):
    """
    Corporate action event - validates against data/corporate_action.v{version}.json.
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "data/corporate_action"
    event_type: str = "corporate_action"

    # Required domain fields (per schema)
    symbol: str
    asset_class: str = "equity"  # Default to equity
    action_type: str  # "split" | "dividend" | "merger" | etc.
    announcement_date: str  # ISO8601 date (YYYY-MM-DD)
    ex_date: str  # ISO8601 date
    effective_date: str  # ISO8601 date
    source: str  # Data source

    # Optional fields
    record_date: Optional[str] = None
    payment_date: Optional[str] = None
    split_from: Optional[int] = None
    split_to: Optional[int] = None
    split_ratio: Optional[Decimal] = None
    dividend_amount: Optional[Decimal] = None
    dividend_currency: Optional[str] = None
    dividend_type: Optional[str] = None
    price_adjustment_factor: Optional[Decimal] = None
    volume_adjustment_factor: Optional[Decimal] = None
    new_symbol: Optional[str] = None
    source_reference: Optional[str] = None
    notes: Optional[str] = None
    trace_id: Optional[str] = None

    @field_serializer(
        "split_ratio",
        "dividend_amount",
        "price_adjustment_factor",
        "volume_adjustment_factor",
    )
    def _serialize_decimal(self, v: Optional[Decimal]) -> Optional[str]:
        """Serialize Decimal to string for wire format."""
        return format(v, "f") if v is not None else None


# ============================================
# Strategy Events
# ============================================


class SignalEvent(ValidatedEvent):
    """
    Trading signal event - validates against strategy/signal.v{version}.json.

    Wire format uses strings for decimals and enums to avoid floating point issues
    and ensure cross-language compatibility. Pydantic auto-converts to proper types
    for Python domain use.

    The intention field accepts either SignalIntention enum or string on input,
    but serializes to string for wire format.

    Attributes:
        signal_id: Unique signal identifier (links to OrderEvent.intent_id for audit trail)
        timestamp: Signal generation timestamp (ISO8601 UTC)
        strategy_id: Strategy that generated this signal (source attribution)
        symbol: Instrument identifier
        intention: Trading action (OPEN_LONG, CLOSE_LONG, OPEN_SHORT, CLOSE_SHORT)
        price: Price at signal generation
        confidence: Signal confidence [0.0, 1.0]
        reason: Human-readable explanation (optional)
        metadata: Strategy-specific data (optional)
        stop_loss: Stop loss price (optional)
        take_profit: Take profit price (optional)

    Example:
        >>> from decimal import Decimal
        >>> signal = SignalEvent(
        ...     signal_id="signal-550e8400-e29b-41d4-a716-446655440001",
        ...     timestamp="2024-03-15T14:35:22Z",
        ...     strategy_id="sma_crossover",
        ...     symbol="AAPL",
        ...     intention="OPEN_LONG",
        ...     price=Decimal("145.75"),
        ...     confidence=Decimal("0.85")
        ... )
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "strategy/signal"
    event_type: str = "signal"  # Must match SCHEMA_BASE

    # Required domain fields
    signal_id: str  # Unique identifier for audit trail (links to OrderEvent.intent_id)
    timestamp: str  # ISO8601 string on wire (UTC RFC3339 with Z suffix)
    strategy_id: str  # Strategy source attribution
    symbol: str
    intention: str  # OPEN_LONG | CLOSE_LONG | OPEN_SHORT | CLOSE_SHORT (string on wire)
    price: Decimal  # String on wire, Decimal in Python
    confidence: Decimal  # String on wire, Decimal in Python (0.0 - 1.0)

    # Optional fields
    reason: Optional[str] = None
    metadata: Optional[dict[str, Any]] = None
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None

    @field_validator("intention", mode="before")
    @classmethod
    def _validate_intention(cls, v: Any) -> str:
        """Convert SignalIntention enum to string, or validate string value."""
        # Import here to avoid circular dependency
        from qs_trader.services.strategy.models import SignalIntention

        if isinstance(v, SignalIntention):
            return v.value
        if isinstance(v, str):
            # Validate that it's a valid intention value
            try:
                SignalIntention(v)
                return v
            except ValueError:
                valid_values = [e.value for e in SignalIntention]
                raise ValueError(f"Invalid intention value: {v}. Must be one of: {valid_values}")
        raise ValueError(f"intention must be SignalIntention or str, got {type(v)}")

    @field_serializer("price", "confidence", "stop_loss", "take_profit")
    def _serialize_decimal(self, v: Optional[Decimal]) -> Optional[str]:
        """Serialize Decimal to string for wire format."""
        return format(v, "f") if v is not None else None


class IndicatorEvent(ValidatedEvent):
    """
    Technical indicator values event - validates against strategy/indicator.v{version}.json.

    Emitted by strategies to log indicator calculations for debugging and analysis.
    Includes indicator type classification and adjustment metadata for proper reporting.
    Opt-in via strategy config (log_indicators: true).

    Attributes:
        strategy_id: Strategy that calculated these indicators
        symbol: Security symbol for which indicators were calculated
        timestamp: Bar timestamp for which indicators were calculated (ISO8601 UTC)
        indicators: Dictionary of indicator name to value (flexible structure)
        indicator_types: Dictionary of indicator name to IndicatorType (PRICE/NORMALIZED/etc)
        adjustment_factor: Cumulative price factor used during calculation (Decimal as string)
        metadata: Optional metadata for additional context (e.g., signal direction)

    Example:
        >>> indicator_event = IndicatorEvent(
        ...     strategy_id="sma_crossover",
        ...     symbol="AAPL",
        ...     timestamp="2024-03-15T14:30:00Z",
        ...     indicators={
        ...         "sma_fast": 150.25,
        ...         "sma_slow": 148.50,
        ...         "rsi": 65.4,
        ...         "crossover": 1.0
        ...     },
        ...     metadata={"signal": "bullish"}
        ... )
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "strategy/indicator"
    event_type: str = "indicator"  # Must match SCHEMA_BASE

    # Required domain fields (event_id and occurred_at inherited from BaseEvent)
    # Indicators are already on correct scale (backward-adjusted from data service)
    # No adjustment metadata needed - everything is transparent
    strategy_id: str  # Strategy source attribution
    symbol: str  # Security identifier
    timestamp: str  # Bar timestamp for which indicators were calculated (ISO8601 UTC)
    indicators: dict[str, Any]  # Flexible structure: name -> value (any JSON-serializable type)

    # Optional fields
    metadata: Optional[dict[str, Any]] = None  # Additional context


# ============================================
# Manager Events
# ============================================


class OrderEvent(ValidatedEvent):
    """
    Order event - validates against manager/order.v{version}.json.

    Emitted by ManagerService when an order is created and sent to execution.
    Represents order placement details before execution.

    Wire format uses strings for decimals to avoid floating point issues
    and ensure cross-language compatibility. Pydantic auto-converts to proper
    types for Python domain use.

    Attributes:
        intent_id: Links back to SignalEvent.signal_id (audit trail: signal → order)
        idempotency_key: Replay protection key (prevents duplicate orders)
        timestamp: Order creation timestamp (ISO8601 UTC)
        symbol: Instrument identifier
        side: "buy" or "sell"
        quantity: Shares/units to order (always positive)
        order_type: "market", "limit", "stop", or "stop_limit"
        limit_price: Limit price (required for limit/stop_limit orders)
        stop_price: Stop trigger price (required for stop/stop_limit orders)
        time_in_force: "GTC", "DAY", "IOC", or "FOK" (defaults to GTC)
        source_strategy_id: Strategy attribution (from SignalEvent.strategy_id)
        stop_loss: Stop loss price (optional)
        take_profit: Take profit price (optional)

    Example:
        >>> from decimal import Decimal
        >>> order = OrderEvent(
        ...     intent_id="signal-550e8400-e29b-41d4-a716-446655440001",
        ...     idempotency_key="sma_crossover-signal-550e8400-e29b-41d4-a716-446655440001-2024-03-15T14:30:15.123Z",
        ...     timestamp="2024-03-15T14:30:15.123Z",
        ...     symbol="AAPL",
        ...     side="buy",
        ...     quantity=Decimal("100"),
        ...     order_type="limit",
        ...     limit_price=Decimal("145.50"),
        ...     time_in_force="GTC",
        ...     source_strategy_id="sma_crossover"
        ... )
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "manager/order"
    event_type: str = "order"  # Must match SCHEMA_BASE

    # Required domain fields
    intent_id: str  # Links to SignalEvent.signal_id (audit trail)
    idempotency_key: str  # Replay protection
    timestamp: str  # ISO8601 string (UTC RFC3339 with Z suffix)
    symbol: str
    side: str  # "buy" or "sell" (validated by schema)
    quantity: Decimal  # String on wire, Decimal in Python
    order_type: str  # "market", "limit", "stop", "stop_limit" (validated by schema)

    # Optional fields
    limit_price: Optional[Decimal] = None  # String on wire, Decimal in Python
    stop_price: Optional[Decimal] = None  # String on wire, Decimal in Python
    time_in_force: str = "GTC"  # "GTC", "DAY", "IOC", "FOK" (validated by schema)
    source_strategy_id: Optional[str] = None  # From SignalEvent.strategy_id
    stop_loss: Optional[Decimal] = None  # String on wire, Decimal in Python
    take_profit: Optional[Decimal] = None  # String on wire, Decimal in Python

    @field_serializer("quantity", "limit_price", "stop_price", "stop_loss", "take_profit")
    def _serialize_decimal(self, v: Optional[Decimal]) -> Optional[str]:
        """Serialize Decimal to string for wire format."""
        return format(v, "f") if v is not None else None


# ============================================
# Execution Events
# ============================================


class FillEvent(ValidatedEvent):
    """
    Order fill event - validates against execution/fill.v{version}.json.

    Emitted by ExecutionService when an order is filled. Contains execution
    details only - portfolio-level position state is tracked separately.

    Wire format uses strings for decimals to avoid floating point issues
    and ensure cross-language compatibility. Pydantic auto-converts to proper
    types for Python domain use.

    Attributes:
        fill_id: Unique fill identifier (UUID)
        source_order_id: Originating order ID (required for audit trail)
        timestamp: Fill execution timestamp (ISO8601 UTC)
        symbol: Instrument identifier
        side: "buy" or "sell"
        filled_quantity: Shares/units filled (always positive)
        fill_price: Execution price per share/unit (includes slippage)
        order_received_at: When execution service received the order (optional)
        strategy_id: Strategy attribution (optional, for multi-strategy)
        commission: Transaction cost (optional, defaults to 0)
        slippage_bps: Slippage in basis points (optional)
        gross_value: filled_quantity * fill_price (optional)
        net_value: Cash impact including commission (optional)

    Example:
        >>> from decimal import Decimal
        >>> fill = FillEvent(
        ...     fill_id="f7e6d5c4-b3a2-1098-7654-321fedcba098",
        ...     source_order_id="order-123",
        ...     timestamp="2024-03-15T14:35:24.123Z",
        ...     symbol="AAPL",
        ...     side="buy",
        ...     filled_quantity=Decimal("100"),
        ...     fill_price=Decimal("145.75"),
        ...     commission=Decimal("1.00")
        ... )
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "execution/fill"
    event_type: str = "fill"  # Must match SCHEMA_BASE

    # Required domain fields
    fill_id: str  # UUID string
    source_order_id: str
    timestamp: str  # ISO8601 string (UTC RFC3339 with Z suffix)
    symbol: str
    side: str  # "buy" or "sell" (validated by schema)
    filled_quantity: Decimal  # String on wire, Decimal in Python
    fill_price: Decimal  # String on wire, Decimal in Python

    # Optional fields
    order_received_at: Optional[str] = None  # ISO8601 string
    strategy_id: Optional[str] = None
    commission: Decimal = Decimal("0")  # String on wire, Decimal in Python
    slippage_bps: Optional[int] = None
    gross_value: Optional[Decimal] = None  # String on wire, Decimal in Python
    net_value: Optional[Decimal] = None  # String on wire, Decimal in Python

    @field_serializer("filled_quantity", "fill_price", "commission", "gross_value", "net_value")
    def _serialize_decimal(self, v: Optional[Decimal]) -> Optional[str]:
        """Serialize Decimal to string for wire format."""
        return format(v, "f") if v is not None else None


# ============================================
# Portfolio Events
# ============================================


class PortfolioPosition(BaseModel):
    """
    Individual position within a portfolio snapshot.

    Nested model used within PortfolioStateEvent.
    Represents a single symbol position (open, closed, or flat).

    Attributes:
        symbol: Instrument identifier
        side: Position side ("long", "short", or "flat")
        open_quantity: Current quantity (positive=long, negative=short, zero=flat)
        average_fill_price: Average entry price per share/unit
        commission_paid: Total commission on current open position
        cost_basis: Total cost including commission
        market_price: Current market price per share/unit
        gross_market_value: Current market value (can be negative for shorts)
        unrealized_pl: Unrealized P&L from current open position
        realized_pl: Lifetime realized P&L for this symbol
        dividends_received: Cumulative dividends received (longs)
        dividends_paid: Cumulative dividends paid (shorts)
        total_position_value: Total value including dividends
        sector: Optional sector classification
        country: Optional country of listing
        asset_class: Optional asset class
        currency: ISO 4217 currency code
        last_updated: Last modification timestamp
    """

    symbol: str
    side: str  # "long", "short", or "flat"
    open_quantity: int
    average_fill_price: Decimal
    commission_paid: Decimal
    cost_basis: Decimal
    market_price: Decimal
    gross_market_value: Decimal
    unrealized_pl: Decimal
    realized_pl: Decimal
    dividends_received: Decimal
    dividends_paid: Decimal
    total_position_value: Decimal
    sector: Optional[str] = None
    country: Optional[str] = None
    asset_class: Optional[str] = None
    currency: str
    last_updated: str  # ISO8601 string

    model_config = {"frozen": True}

    @field_serializer(
        "average_fill_price",
        "commission_paid",
        "cost_basis",
        "market_price",
        "gross_market_value",
        "unrealized_pl",
        "realized_pl",
        "dividends_received",
        "dividends_paid",
        "total_position_value",
    )
    def _serialize_decimal(self, v: Decimal) -> str:
        """Serialize Decimal to string for wire format."""
        return format(v, "f")


class StrategyGroup(BaseModel):
    """
    Strategy group containing positions owned by a single strategy.

    Nested model used within PortfolioStateEvent.

    Attributes:
        strategy_id: Unique identifier for the strategy
        positions: List of positions owned by this strategy
    """

    strategy_id: str
    positions: list[PortfolioPosition]

    model_config = {"frozen": True}


# ============================================
# Control Events (No Payload Validation)
# ============================================


class ValuationTriggerEvent(ControlEvent):
    """Barrier event - triggers portfolio valuation."""

    event_type: str = "valuation_trigger"


class PortfolioStateEvent(ValidatedEvent):
    """
    Portfolio state snapshot event - validates against portfolio/portfolio_state.v{version}.json.

    Published by PortfolioService at regular intervals. Contains complete portfolio
    state including positions grouped by strategy, P&L, exposures, and fees.

    This is a static point-in-time snapshot published for:
    - Risk monitoring (ManagerService uses this for real-time risk checks)
    - Performance tracking
    - Portfolio reporting
    - Strategy attribution

    Wire format uses strings for decimals to avoid floating point issues
    and ensure cross-language compatibility. Pydantic auto-converts to proper
    types for Python domain use.

    Attributes:
        portfolio_id: Unique portfolio identifier
        start_datetime: Backtest/simulation start timestamp (ISO8601 UTC)
        snapshot_datetime: Snapshot generation timestamp (ISO8601 UTC)
        reporting_currency: ISO 4217 currency code for all values
        initial_portfolio_equity: Starting capital for performance tracking
        cash_balance: Current cash (can be negative with margin)
        current_portfolio_equity: Current equity (cash + total_market_value)
        total_market_value: Sum of all position values
        total_unrealized_pl: Total unrealized P&L from open positions
        total_realized_pl: Lifetime realized P&L
        total_pl: Total P&L (realized + unrealized)
        long_exposure: Sum of long position values
        short_exposure: Sum of short position values (absolute)
        net_exposure: Long - short exposure
        gross_exposure: Long + short exposure (absolute)
        leverage: Gross exposure / equity ratio
        total_commissions_paid: Cumulative commissions from inception
        total_dividends_received: Cumulative dividends from longs
        total_dividends_paid: Cumulative dividends paid on shorts
        total_borrow_fees: Cumulative short borrow fees
        total_margin_interest: Cumulative margin interest on negative cash
        strategies_groups: Positions grouped by strategy
        currency_conversion_rates: Currency conversion rates to reporting currency
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "portfolio/portfolio_state"
    event_type: str = "portfolio_state"

    # Required domain fields
    portfolio_id: str
    start_datetime: str  # ISO8601 string (UTC RFC3339 with Z suffix)
    snapshot_datetime: str  # ISO8601 string (UTC RFC3339 with Z suffix)
    reporting_currency: str  # ISO 4217 code (e.g., "USD")
    initial_portfolio_equity: Decimal  # String on wire, Decimal in Python
    cash_balance: Decimal  # String on wire, Decimal in Python
    current_portfolio_equity: Decimal  # String on wire, Decimal in Python
    total_market_value: Decimal  # String on wire, Decimal in Python
    total_unrealized_pl: Decimal  # String on wire, Decimal in Python
    total_realized_pl: Decimal  # String on wire, Decimal in Python
    total_pl: Decimal  # String on wire, Decimal in Python
    long_exposure: Decimal  # String on wire, Decimal in Python
    short_exposure: Decimal  # String on wire, Decimal in Python
    net_exposure: Decimal  # String on wire, Decimal in Python
    gross_exposure: Decimal  # String on wire, Decimal in Python
    leverage: Decimal  # String on wire, Decimal in Python
    strategies_groups: list[StrategyGroup]

    # Optional fields
    total_commissions_paid: Decimal = Decimal("0")  # String on wire, Decimal in Python
    total_dividends_received: Decimal = Decimal("0")  # String on wire, Decimal in Python
    total_dividends_paid: Decimal = Decimal("0")  # String on wire, Decimal in Python
    total_borrow_fees: Decimal = Decimal("0")  # String on wire, Decimal in Python
    total_margin_interest: Decimal = Decimal("0")  # String on wire, Decimal in Python
    currency_conversion_rates: dict[str, Decimal] = Field(default_factory=dict)

    @field_serializer(
        "initial_portfolio_equity",
        "cash_balance",
        "current_portfolio_equity",
        "total_market_value",
        "total_unrealized_pl",
        "total_realized_pl",
        "total_pl",
        "long_exposure",
        "short_exposure",
        "net_exposure",
        "gross_exposure",
        "leverage",
        "total_commissions_paid",
        "total_dividends_received",
        "total_dividends_paid",
        "total_borrow_fees",
        "total_margin_interest",
    )
    def _serialize_decimal(self, v: Decimal) -> str:
        """Serialize Decimal to string for wire format."""
        return format(v, "f")

    @field_serializer("currency_conversion_rates")
    def _serialize_currency_rates(self, v: dict[str, Decimal]) -> dict[str, str]:
        """Serialize currency conversion rates (Decimal values to strings)."""
        return {k: format(val, "f") for k, val in v.items()}


class TradeEvent(ValidatedEvent):
    """
    Trade event - aggregates all fills belonging to a single trade.

    Emitted by PortfolioService when a trade opens or closes. Provides a complete
    view of a trade lifecycle including all fills (entry and exits), P&L, and timing.

    A "trade" is defined as opening a position from flat and closing back to flat
    for a specific (strategy_id, symbol) pair.

    Wire format uses strings for decimals to avoid floating point issues
    and ensure cross-language compatibility. Pydantic auto-converts to proper
    types for Python domain use.

    Attributes:
        trade_id: Unique trade identifier (e.g., T00001, T00002)
        timestamp: Timestamp of last fill in this trade update (ISO8601 UTC)
        strategy_id: Strategy that generated this trade
        symbol: Instrument identifier
        status: "open" (position active) or "closed" (back to flat)
        side: "long" (buy-first) or "short" (sell-first)
        fills: List of fill_ids that comprise this trade (references FillEvent.fill_id)
        entry_price: Average entry price per share/unit (or None)
        current_quantity: Current position quantity (zero when closed)
        realized_pnl: Realized P&L when closed (or None for open trades)
        commission_total: Total commission across all fills
        entry_timestamp: Timestamp of first fill (trade entry, optional)
        exit_timestamp: Timestamp of closing fill (optional, only when status="closed")

    Example:
        >>> from decimal import Decimal
        >>> trade = TradeEvent(
        ...     trade_id="T00001",
        ...     timestamp="2024-03-15T14:35:24.123Z",
        ...     strategy_id="momentum",
        ...     symbol="AAPL",
        ...     status="open",
        ...     side="long",
        ...     fills=["f7e6d5c4-b3a2-1098-7654-321fedcba098"],
        ...     entry_price=Decimal("145.75"),
        ...     current_quantity=Decimal("100"),
        ...     commission_total=Decimal("1.00"),
        ...     entry_timestamp="2024-03-15T14:35:24.123Z"
        ... )
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "portfolio/trade"
    event_type: str = "trade"

    # Required domain fields
    trade_id: str  # Pattern: T00001, T00002, etc.
    timestamp: str  # ISO8601 string (UTC RFC3339 with Z suffix)
    strategy_id: str
    symbol: str
    status: str  # "open" or "closed" (validated by schema)
    fills: list[str]  # List of fill_id (UUID strings)

    # Optional fields
    side: Optional[str] = None  # "long" or "short"
    entry_price: Optional[Decimal] = None  # String on wire, Decimal in Python
    exit_price: Optional[Decimal] = None  # String on wire, Decimal in Python
    current_quantity: Optional[Decimal] = None  # String on wire, Decimal in Python
    realized_pnl: Optional[Decimal] = None  # String on wire, Decimal in Python
    commission_total: Decimal = Decimal("0")  # String on wire, Decimal in Python
    entry_timestamp: Optional[str] = None  # ISO8601 string
    exit_timestamp: Optional[str] = None  # ISO8601 string

    @field_serializer("entry_price", "exit_price", "current_quantity", "realized_pnl", "commission_total")
    def _serialize_decimal(self, v: Optional[Decimal]) -> Optional[str]:
        """Serialize Decimal to string for wire format."""
        return format(v, "f") if v is not None else None


class StrategyMetrics(BaseModel):
    """Per-strategy performance metrics."""

    strategy_id: str
    equity_allocated: Decimal
    positions_value: Decimal
    num_positions: int
    return_pct: Decimal
    total_trades: int = 0
    winning_trades: int = 0


class PeriodMetricsSnapshot(BaseModel):
    """Period metrics snapshot for PerformanceMetricsEvent."""

    period: str
    period_type: str  # "monthly", "quarterly", "annual"
    start_date: str
    end_date: str
    return_pct: Decimal
    num_trades: int
    winning_trades: int
    losing_trades: int

    @field_serializer("return_pct")
    def _serialize_decimal(self, v: Decimal) -> str:
        """Serialize Decimal to string for wire format."""
        return format(v, "f")


class PerformanceMetricsEvent(ValidatedEvent):
    """
    Performance metrics snapshot event - validates against reporting/performance_metrics.v{version}.json.

    Published by ReportingService after each PortfolioStateEvent. Contains basic metrics
    for real-time monitoring during backtest execution.

    Wire format uses strings for decimals to avoid floating point issues
    and ensure cross-language compatibility. Pydantic auto-converts to proper
    types for Python domain use.

    Attributes:
        timestamp: Snapshot timestamp (matches PortfolioStateEvent.snapshot_datetime)
        equity: Current portfolio equity (cash + positions_value)
        cash: Current cash balance (can be negative with margin)
        positions_value: Total market value of all positions
        total_return_pct: Total return from initial equity
        max_drawdown_pct: Maximum peak-to-trough equity decline from inception
        current_drawdown_pct: Current drawdown from peak (zero if at peak)
        num_positions: Number of currently open positions
        gross_exposure: Gross exposure (|long| + |short|)
        net_exposure: Net exposure (long - short)
        leverage: Leverage ratio (gross_exposure / equity)
        total_trades: Total completed round-trip trades
        winning_trades: Number of profitable trades
        losing_trades: Number of unprofitable trades
        total_commissions: Cumulative commissions paid
        strategy_metrics: Optional per-strategy performance breakdown
    """

    SCHEMA_BASE: ClassVar[Optional[str]] = "reporting/performance_metrics"
    event_type: str = "performance_metrics"

    # Required domain fields
    timestamp: str  # ISO8601 string (UTC RFC3339 with Z suffix)
    equity: Decimal
    cash: Decimal
    positions_value: Decimal
    total_return_pct: Decimal
    max_drawdown_pct: Decimal
    current_drawdown_pct: Decimal
    num_positions: int
    gross_exposure: Decimal
    net_exposure: Decimal
    leverage: Decimal

    # Optional fields - trade statistics
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    total_commissions: Decimal = Decimal("0")

    # Optional fields - risk-adjusted metrics (calculated incrementally)
    cagr: Decimal = Decimal("0")
    volatility: Decimal = Decimal("0")
    sharpe_ratio: Decimal = Decimal("0")
    sortino_ratio: Decimal | None = Decimal("0")  # None if no downside risk (infinite Sortino)
    calmar_ratio: Decimal = Decimal("0")
    win_rate: Decimal = Decimal("0")
    profit_factor: Decimal | None = None
    expectancy: Decimal = Decimal("0")

    # Optional fields - per-strategy breakdown
    strategy_metrics: list[StrategyMetrics] = Field(default_factory=list)

    # Optional fields - period breakdowns
    monthly_returns: list[PeriodMetricsSnapshot] = Field(default_factory=list)
    quarterly_returns: list[PeriodMetricsSnapshot] = Field(default_factory=list)
    annual_returns: list[PeriodMetricsSnapshot] = Field(default_factory=list)

    @field_serializer(
        "equity",
        "cash",
        "positions_value",
        "total_return_pct",
        "max_drawdown_pct",
        "current_drawdown_pct",
        "gross_exposure",
        "net_exposure",
        "leverage",
        "total_commissions",
        "cagr",
        "volatility",
        "sharpe_ratio",
        "sortino_ratio",
        "calmar_ratio",
        "win_rate",
        "expectancy",
    )
    def _serialize_decimal(self, v: Decimal | None) -> str | None:
        """Serialize Decimal to string for wire format."""
        return format(v, "f") if v is not None else None

    @field_serializer("profit_factor")
    def _serialize_profit_factor(self, v: Decimal | None) -> str | None:
        """Serialize profit factor (can be None if no losing trades)."""
        return format(v, "f") if v is not None else None

    @field_serializer("strategy_metrics")
    def _serialize_strategy_metrics(self, v: list[StrategyMetrics]) -> list[dict]:
        """Serialize strategy metrics list."""
        return [
            {
                "strategy_id": m.strategy_id,
                "equity_allocated": format(m.equity_allocated, "f"),
                "positions_value": format(m.positions_value, "f"),
                "num_positions": m.num_positions,
                "return_pct": format(m.return_pct, "f"),
                "total_trades": m.total_trades,
                "winning_trades": m.winning_trades,
            }
            for m in v
        ]

    @field_serializer("monthly_returns", "quarterly_returns", "annual_returns")
    def _serialize_period_metrics(self, v: list[PeriodMetricsSnapshot]) -> list[dict]:
        """Serialize period metrics list."""
        return [
            {
                "period": p.period,
                "period_type": p.period_type,
                "start_date": p.start_date,
                "end_date": p.end_date,
                "return_pct": format(p.return_pct, "f"),
                "num_trades": p.num_trades,
                "winning_trades": p.winning_trades,
                "losing_trades": p.losing_trades,
            }
            for p in v
        ]


class RiskEvaluationTriggerEvent(ControlEvent):
    """Barrier event - triggers risk evaluation."""

    event_type: str = "risk_evaluation_trigger"


class BarCloseEvent(ControlEvent):
    """Barrier event - marks end of bar processing."""

    event_type: str = "bar_close"


class BacktestStartedEvent(ControlEvent):
    """Lifecycle event - backtest started."""

    event_type: str = "backtest_started"
    config: dict[str, Any] = Field(default_factory=dict)


class BacktestEndedEvent(ControlEvent):
    """Lifecycle event - backtest ended."""

    event_type: str = "backtest_ended"
    success: bool = True
    error_message: str = ""
    stats: dict[str, Any] = Field(default_factory=dict)
