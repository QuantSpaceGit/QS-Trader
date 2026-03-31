"""
DataService Contract - Published Events and Data Models.

This module defines the PUBLIC API of DataService. All data structures
published by DataService are defined here. This is a CONTRACT: breaking
changes require major version bump.

CONTRACT: DataService v1.0.0

Published Data Models:
- Bar: OHLCV price bar (single adjustment mode)
- PriceSeries: Time series of bars (single adjustment mode)
- Instrument: Tradable instrument specification
- CorporateAction: Corporate action (split, dividend)

Future (planned but not implemented):
- Quote: Level 1 quote (top of book) - Phase 2
- NewsEvent: News with sentiment - Phase 3
- EconomicEvent: Economic calendar event - Phase 3

Published By: DataService
Consumed By: All services requiring market data

Design Principles:
- Immutability: All models frozen=True (events are facts)
- Validation: Pydantic strict validation
- Backward Compatibility: New fields must be Optional
- Documentation: Clear purpose and usage examples
"""

from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any, ClassVar, Dict, Literal, Optional

from pydantic import BaseModel, Field, model_validator

# ============================================
# Contract Version
# ============================================

CONTRACT_VERSION = "1.0.0"

# ============================================
# Instrument Models (Contract)
# ============================================


class InstrumentType(Enum):
    """
    CONTRACT: Asset class classification.

    Used for instrument categorization and routing to appropriate adapters.
    """

    EQUITY = "equity"
    CRYPTO = "crypto"
    FUTURE = "future"
    FOREX = "forex"
    SIGNAL = "signal"


class DataSource(Enum):
    """
    CONTRACT: Logical data source identifier.

    Maps to physical adapters via data_sources.yaml configuration.
    Allows environment-specific configuration (dev/prod).
    """

    CSV_FILE = "csv_file"


class Instrument(BaseModel):
    """
    CONTRACT: Tradable instrument specification.

    Minimal instrument representation by symbol. Dataset configuration
    (provider, asset type, etc.) is specified separately via dataset name.

    Published By: DataService (instrument resolution)
    Used By: All services requiring instrument identification

    Design Philosophy:
        User specifies: "Give me bars for AAPL from dataset yahoo-us-equity-1d-csv"
        Not: "Give me bars for AAPL (equity, from Yahoo)" - that duplicates dataset config

    Attributes:
        symbol: Ticker symbol (e.g., "AAPL", "BTCUSD", "ES_Z24")
        frequency: Optional override for dataset default frequency
        metadata: Custom attributes (exchange, contract month, etc.)

    Examples:
        >>> # Basic instrument
        >>> instrument = Instrument(symbol="AAPL")
        >>>
        >>> # With custom frequency override
        >>> instrument = Instrument(symbol="BTCUSD", frequency="1m")
        >>>
        >>> # With metadata
        >>> instrument = Instrument(
        ...     symbol="ES_Z24",
        ...     metadata={"contract_month": "2024-12", "exchange": "CME"}
        ... )

    Notes:
        - Dataset specified separately when resolving to adapter
        - Symbol is the primary identifier
        - Metadata for custom attributes without schema pollution
    """

    symbol: str = Field(..., description="Ticker symbol")
    frequency: Optional[str] = Field(default=None, description="Override dataset default frequency")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Custom attributes")

    model_config = {"frozen": True}

    def __repr__(self) -> str:
        """Human-readable representation."""
        freq = f"@{self.frequency}" if self.frequency else ""
        meta = f" {self.metadata}" if self.metadata else ""
        return f"Instrument({self.symbol}{freq}{meta})"


# ============================================
# Price Data Models (Contract)
# ============================================


class Bar(BaseModel):
    """
    CONTRACT: Canonical OHLCV price bar.

    Vendor-agnostic representation of a single bar in one adjustment mode.
    All vendor-specific data must be transformed to this format by adapters.

    Published By: DataService (in PriceBarEvent)
    Used By: Strategy, Execution, Portfolio, Analytics

    Adjustment Modes:
    - unadjusted: Raw prices as traded (for execution/fills)
    - adjusted: Split-adjusted only (for indicators/signals)
    - total_return: Split + dividend adjusted (for performance)

    Attributes:
        trade_datetime: Trading datetime (bar timestamp)
        open: Opening price (must be positive)
        high: High price (must be positive, >= low)
        low: Low price (must be positive, <= high)
        close: Closing price (must be positive)
        volume: Trading volume (must be non-negative)
        dividend: Split-adjusted dividend per share (if any, on ex-date)

    Validation:
        - All prices > 0
        - High >= Low (strictly enforced)
        - Volume >= 0
        - Dividend >= 0 (if present)

    Examples:
        >>> # Basic bar (no dividend)
        >>> bar = Bar(
        ...     trade_datetime=datetime(2024, 1, 2, 16, 0),
        ...     open=150.0,
        ...     high=151.0,
        ...     low=149.5,
        ...     close=150.5,
        ...     volume=1000000
        ... )
        >>>
        >>> # Bar with dividend
        >>> bar = Bar(
        ...     trade_datetime=datetime(2024, 2, 7, 16, 0),
        ...     open=150.0,
        ...     high=150.5,
        ...     low=149.8,
        ...     close=150.2,
        ...     volume=2000000,
        ...     dividend=Decimal("0.77")  # Ex-dividend date
        ... )

    Notes:
        - Immutable after creation (frozen=True)
        - Dividend is split-adjusted to current share basis
        - Strictly validates OHLC relationships
    """

    trade_datetime: datetime = Field(..., description="Trade datetime")
    open: float = Field(..., gt=0, description="Open price")
    high: float = Field(..., gt=0, description="High price")
    low: float = Field(..., gt=0, description="Low price")
    close: float = Field(..., gt=0, description="Close price")
    volume: int = Field(..., ge=0, description="Volume")
    dividend: Optional[Decimal] = Field(default=None, ge=0, description="Split-adjusted dividend per share (if any)")

    model_config = {"frozen": True}

    @model_validator(mode="after")
    def validate_ohlc(self) -> "Bar":
        """
        Validate OHLC relationships.

        Enforces: High >= Low (strict)

        Raises:
            ValueError: If High < Low
        """
        if self.high < self.low:
            raise ValueError(f"[{self.trade_datetime}] OHLC violation: High ({self.high}) < Low ({self.low})")
        return self


class AdjustmentMode(str, Enum):
    """
    CONTRACT: Price adjustment modes.

    Defines how prices are adjusted for corporate actions:
    - UNADJUSTED: Raw prices, no adjustments (execution, fills)
    - ADJUSTED: Split-adjusted backward (indicators, signals)
    - TOTAL_RETURN: Forward compounding with dividend reinvestment (performance)
    """

    UNADJUSTED = "unadjusted"
    ADJUSTED = "adjusted"
    TOTAL_RETURN = "total_return"


class PriceSeries(BaseModel):
    """
    CONTRACT: Time series of bars for a specific adjustment mode.

    Vendor-agnostic price series used by backtester. All bars share
    the same adjustment mode and symbol.

    Published By: DataService (via DataLoader)
    Used By: BacktestEngine, Analytics

    Attributes:
        mode: Adjustment mode (unadjusted|adjusted|total_return)
        symbol: Ticker symbol
        bars: List of canonical bars (chronologically ordered)

    Validation:
        - Mode must be valid (one of VALID_MODES)
        - All bars should be in chronological order (not enforced, flexibility)

    Examples:
        >>> # Adjusted price series
        >>> series = PriceSeries(
        ...     mode="adjusted",
        ...     symbol="AAPL",
        ...     bars=[bar1, bar2, bar3]
        ... )

    Notes:
        - Immutable after creation (frozen=True)
        - Bars should be chronologically ordered (oldest first)
        - Consumers may need to sort if order not guaranteed
    """

    VALID_MODES: ClassVar[set[str]] = {"unadjusted", "adjusted", "total_return"}

    mode: str = Field(..., description="Adjustment mode")
    symbol: str = Field(..., description="Ticker symbol")
    bars: list[Bar] = Field(..., description="List of canonical bars")

    model_config = {"frozen": True}

    @model_validator(mode="after")
    def validate_mode(self) -> "PriceSeries":
        """Validate mode is valid."""
        if self.mode not in self.VALID_MODES:
            raise ValueError(f"Invalid mode '{self.mode}'. Must be one of {self.VALID_MODES}")
        return self


# ============================================
# Corporate Action Models (Contract)
# ============================================


class CorporateActionType(str, Enum):
    """
    CONTRACT: Types of corporate actions.

    Defines the kinds of corporate actions that can affect positions:
    - DIVIDEND: Cash or stock dividend payment
    - SPLIT: Stock split (forward or reverse)
    """

    DIVIDEND = "dividend"
    SPLIT = "split"


class CorporateAction(BaseModel):
    """
    CONTRACT: Corporate action (split, dividend).

    Represents a corporate action that affects positions and cash.
    Published when detected from data feed.

    Published By: DataService (in CorporateActionEvent)
    Used By: PortfolioService (position/cash adjustments), Analytics

    Attributes:
        symbol: Ticker symbol
        action_type: Type of action (dividend or split)
        effective_date: When action takes effect
        dividend_amount: Amount per share for dividends
        dividend_type: Cash or stock dividend
        ex_date: Ex-dividend date (for dividends)
        split_ratio: Split ratio for splits (e.g., 4.0 for 4-for-1)

    Examples:
        >>> # Cash dividend
        >>> action = CorporateAction(
        ...     symbol="AAPL",
        ...     action_type=CorporateActionType.DIVIDEND,
        ...     effective_date=datetime(2024, 2, 7),
        ...     ex_date=datetime(2024, 2, 7),
        ...     dividend_amount=Decimal("0.77"),
        ...     dividend_type="cash"
        ... )
        >>>
        >>> # Stock split (4-for-1)
        >>> action = CorporateAction(
        ...     symbol="AAPL",
        ...     action_type=CorporateActionType.SPLIT,
        ...     effective_date=datetime(2024, 8, 31),
        ...     split_ratio=Decimal("4.0")
        ... )

    Notes:
        - Immutable after creation (frozen=True)
        - Dividend fields populated only for dividends
        - Split fields populated only for splits
    """

    symbol: str = Field(..., description="Ticker symbol")
    action_type: CorporateActionType = Field(..., description="Type of action")
    effective_date: datetime = Field(..., description="When action takes effect")

    # Dividend fields (populated only for dividends)
    dividend_amount: Optional[Decimal] = Field(default=None, ge=0, description="Dividend per share")
    dividend_type: Optional[Literal["cash", "stock"]] = Field(default=None, description="Dividend type")
    ex_date: Optional[datetime] = Field(default=None, description="Ex-dividend date")

    # Split fields (populated only for splits)
    split_ratio: Optional[Decimal] = Field(default=None, gt=0, description="Split ratio")

    model_config = {"frozen": True}


# ============================================
# Future: Level 1 Market Data
# ============================================
# NOTE: Not implemented yet, reserved for future use


class Quote(BaseModel):
    """
    CONTRACT: Level 1 quote (top of book).

    FUTURE: Phase 2 - Real-time market data

    Published By: DataService (in QuoteEvent)
    Used By: Execution (realistic fills), Strategy (intraday signals)

    Attributes:
        symbol: Ticker symbol
        timestamp: Quote timestamp
        bid_price: Best bid price
        bid_size: Best bid size
        ask_price: Best ask price
        ask_size: Best ask size
        last_price: Last trade price
        last_size: Last trade size
    """

    symbol: str
    timestamp: datetime
    bid_price: Decimal
    bid_size: int
    ask_price: Decimal
    ask_size: int
    last_price: Optional[Decimal] = None
    last_size: Optional[int] = None

    model_config = {"frozen": True}


# ============================================
# Future: News & Sentiment
# ============================================
# NOTE: Not implemented yet, reserved for future use


class NewsSentiment(str, Enum):
    """CONTRACT: News sentiment classification."""

    POSITIVE = "positive"
    NEUTRAL = "neutral"
    NEGATIVE = "negative"


class NewsEvent(BaseModel):
    """
    CONTRACT: News event with sentiment.

    FUTURE: Phase 3 - Alternative data

    Published By: DataService (in NewsPublishedEvent)
    Used By: Strategy (sentiment-based signals)

    Attributes:
        symbol: Ticker symbol (None for market-wide news)
        timestamp: News timestamp
        headline: News headline
        source: News source
        sentiment: Sentiment classification
        sentiment_score: Sentiment score [-1, 1]
        relevance_score: Relevance score [0, 1]
    """

    symbol: Optional[str] = None
    timestamp: datetime
    headline: str
    source: str
    sentiment: NewsSentiment
    sentiment_score: float = Field(ge=-1.0, le=1.0)
    relevance_score: float = Field(ge=0.0, le=1.0)

    model_config = {"frozen": True}


# ============================================
# Future: Economic Calendar
# ============================================
# NOTE: Not implemented yet, reserved for future use


class EconomicEventImportance(str, Enum):
    """CONTRACT: Economic event importance."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class EconomicEvent(BaseModel):
    """
    CONTRACT: Economic calendar event.

    FUTURE: Phase 3 - Macro data

    Published By: DataService (in EconomicEventPublishedEvent)
    Used By: Strategy (macro positioning), Risk (volatility adjustments)

    Attributes:
        event_id: Unique event identifier
        release_datetime: When data is released
        indicator_name: Indicator name (e.g., "CPI", "NFP")
        country: Country code
        importance: Event importance level
        previous_value: Previous reading
        forecast_value: Consensus forecast
        actual_value: Actual released value
    """

    event_id: str
    release_datetime: datetime
    indicator_name: str
    country: str
    importance: EconomicEventImportance
    previous_value: Optional[Decimal] = None
    forecast_value: Optional[Decimal] = None
    actual_value: Optional[Decimal] = None

    model_config = {"frozen": True}


# ============================================
# public API
# ============================================
__all__ = [
    # Version
    "CONTRACT_VERSION",
    # Data Contract
    "Bar",
    "PriceSeries",
    "AdjustmentMode",
    "Instrument",
    "InstrumentType",
    "DataSource",
    "CorporateAction",
    "CorporateActionType",
]
