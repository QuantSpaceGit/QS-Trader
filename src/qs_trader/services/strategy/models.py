"""
StrategyService Contract - Published Events and Data Models.

This module defines the PUBLIC API of StrategyService. All data structures
published by StrategyService are defined here. This is a CONTRACT: breaking
changes require major version bump.

CONTRACT: StrategyService v1.0.0

Published Data Models:
- Signal: Trading signal with confidence level
- SignalIntention: OPEN_LONG/CLOSE_LONG/OPEN_SHORT/CLOSE_SHORT intention enum
- SignalConfidence: Confidence level [0.0, 1.0]

Published By: StrategyService
Consumed By: RiskService, ExecutionService, Analytics

Design Principles:
- Immutability: All models frozen=True (signals are facts)
- Validation: Pydantic strict validation
- Backward Compatibility: New fields must be Optional
- Documentation: Clear purpose and usage examples
"""

from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator

# ============================================
# Contract Version
# ============================================

CONTRACT_VERSION = "1.0.0"

# ============================================
# Signal Models (Contract)
# ============================================


class SignalIntention(str, Enum):
    """
    CONTRACT: Signal intention enumeration.

    Defines the trading action that the strategy intends to take.
    More expressive than simple BUY/SELL as it clarifies the intent
    to open new positions or close existing ones.

    Values:
        OPEN_LONG: Initiate or add to a long position (bullish)
        CLOSE_LONG: Close or reduce a long position (exit long)
        OPEN_SHORT: Initiate or add to a short position (bearish)
        CLOSE_SHORT: Close or reduce a short position (cover short)

    Examples:
        >>> # Opening a new long position
        >>> intention = SignalIntention.OPEN_LONG
        >>>
        >>> # Closing an existing short position
        >>> intention = SignalIntention.CLOSE_SHORT
        >>>
        >>> # Can also create from string
        >>> intention = SignalIntention("OPEN_LONG")

    Notes:
        - RiskService interprets intentions in context of current positions
        - Multiple signals can be aggregated (e.g., partial exits)
        - Clear distinction between opening and closing helps with position management
    """

    OPEN_LONG = "OPEN_LONG"
    CLOSE_LONG = "CLOSE_LONG"
    OPEN_SHORT = "OPEN_SHORT"
    CLOSE_SHORT = "CLOSE_SHORT"


class Signal(BaseModel):
    """
    CONTRACT: Trading signal with confidence level.

    Represents a strategy's trading intent WITHOUT position sizing.
    The strategy declares WHAT to trade and HOW CONFIDENT it is,
    while RiskService determines HOW MUCH to trade.

    Published By: StrategyService (emitted by BaseStrategy implementations)
    Consumed By: RiskService (for sizing), ExecutionService (for orders), Analytics

    Attributes:
        timestamp: Signal generation time (bar timestamp or now())
        strategy_id: Unique strategy identifier (from config.name)
        symbol: Instrument symbol to trade
        intention: Trading intention (OPEN_LONG/CLOSE_LONG/OPEN_SHORT/CLOSE_SHORT)
        confidence: Signal strength [0.0, 1.0] where:
            - 0.0 = weakest signal (rarely used)
            - 0.5 = moderate confidence
            - 1.0 = maximum confidence
        reason: Optional human-readable explanation
        metadata: Optional additional context (technical values, etc.)

    Validation:
        - timestamp must be valid datetime
        - strategy_id cannot be empty
        - symbol cannot be empty
        - confidence must be in [0.0, 1.0]
        - reason max length 500 chars

    Examples:
        >>> # Strong OPEN_LONG signal from Bollinger Bands
        >>> signal = Signal(
        ...     timestamp=datetime(2024, 1, 2, 16, 0),
        ...     strategy_id="bb_breakout",
        ...     symbol="AAPL",
        ...     intention=SignalIntention.OPEN_LONG,
        ...     confidence=0.85,
        ...     reason="Oversold: %B=-0.25",
        ...     metadata={
        ...         "percent_b": -0.25,
        ...         "bandwidth": 0.045,
        ...         "price": 149.50,
        ...         "lower_band": 150.20,
        ...         "upper_band": 154.80,
        ...     }
        ... )
        >>>
        >>> # Moderate OPEN_SHORT signal from RSI
        >>> signal = Signal(
        ...     timestamp=datetime(2024, 1, 3, 16, 0),
        ...     strategy_id="rsi_reversal",
        ...     symbol="TSLA",
        ...     intention=SignalIntention.OPEN_SHORT,
        ...     confidence=0.65,
        ...     reason="Overbought: RSI=72",
        ...     metadata={"rsi": 72.0, "price": 245.30}
        ... )

    Notes:
        - Immutable after creation (frozen=True)
        - Confidence is strategy's self-assessment, not a prediction
        - RiskService may further adjust sizing based on portfolio risk
        - Metadata keys should be lowercase with underscores
        - Metadata values should be JSON-serializable (float, int, str, bool)
    """

    timestamp: datetime = Field(..., description="Signal generation timestamp")
    strategy_id: str = Field(..., min_length=1, description="Strategy identifier")
    symbol: str = Field(..., min_length=1, description="Instrument symbol")
    intention: SignalIntention = Field(..., description="Trading intention")
    confidence: float = Field(..., ge=0.0, le=1.0, description="Signal confidence [0.0, 1.0]")
    reason: Optional[str] = Field(default=None, max_length=500, description="Human-readable explanation")
    metadata: Optional[Dict[str, Any]] = Field(default=None, description="Additional signal context")

    model_config = {"frozen": True}

    @field_validator("strategy_id", "symbol")
    @classmethod
    def validate_not_empty(cls, v: str) -> str:
        """Ensure strategy_id and symbol are not empty or whitespace."""
        if not v or not v.strip():
            raise ValueError("Field cannot be empty or whitespace")
        return v.strip()

    @field_validator("metadata")
    @classmethod
    def validate_metadata(cls, v: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Validate metadata values are JSON-serializable."""
        if v is None:
            return v

        allowed_types = (str, int, float, bool, type(None))
        for key, value in v.items():
            if not isinstance(value, allowed_types):
                raise ValueError(
                    f"Metadata value for '{key}' must be JSON-serializable "
                    f"(str, int, float, bool, None), got {type(value).__name__}"
                )
        return v


# ============================================
# Helper Functions
# ============================================


def create_signal(
    timestamp: datetime,
    strategy_id: str,
    symbol: str,
    intention: SignalIntention | str,
    confidence: float,
    reason: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Signal:
    """
    Convenience factory for creating Signal instances.

    Args:
        timestamp: Signal generation time
        strategy_id: Strategy identifier
        symbol: Instrument symbol
        intention: Trading intention (SignalIntention enum or "OPEN_LONG"/"CLOSE_LONG"/etc.)
        confidence: Signal confidence [0.0, 1.0]
        reason: Optional explanation
        metadata: Optional additional context

    Returns:
        Validated Signal instance

    Example:
        >>> signal = create_signal(
        ...     timestamp=datetime.now(),
        ...     strategy_id="my_strategy",
        ...     symbol="AAPL",
        ...     intention="OPEN_LONG",
        ...     confidence=0.75,
        ...     reason="Strong uptrend"
        ... )
    """
    if isinstance(intention, str):
        intention = SignalIntention(intention)

    return Signal(
        timestamp=timestamp,
        strategy_id=strategy_id,
        symbol=symbol,
        intention=intention,
        confidence=confidence,
        reason=reason,
        metadata=metadata,
    )


# ============================================
# Public API
# ============================================

__all__ = [
    "CONTRACT_VERSION",
    "Signal",
    "SignalIntention",
    "create_signal",
]
