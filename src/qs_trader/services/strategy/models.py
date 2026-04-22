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

from pydantic import BaseModel, Field, field_validator, model_validator

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


class LifecycleIntentType(str, Enum):
    """Explicit lifecycle classification for strategy-emitted signals.

    ``SignalIntention`` still carries *directional* semantics (open/close long/short).
    ``LifecycleIntentType`` is the explicit opt-in the Manager uses to decide
    whether a same-side action is a regular open/close or a scale-in/scale-out.
    """

    OPEN = "open"
    CLOSE = "close"
    SCALE_IN = "scale_in"
    SCALE_OUT = "scale_out"


def normalize_signal_intention(intention: SignalIntention | str) -> SignalIntention:
    """Normalize a string or enum into ``SignalIntention``."""
    if isinstance(intention, SignalIntention):
        return intention
    return SignalIntention(str(intention))


def normalize_lifecycle_intent_type(
    intention: SignalIntention | str,
    intent_type: LifecycleIntentType | str | None = None,
) -> LifecycleIntentType:
    """Validate and normalize an explicit lifecycle intent type.

    Open-direction signals may opt into ``scale_in``; close-direction signals
    may opt into ``scale_out``. When omitted, the legacy default mapping remains
    ``open`` for ``OPEN_*`` signals and ``close`` for ``CLOSE_*`` signals.
    """

    normalized_intention = normalize_signal_intention(intention)
    is_open_signal = normalized_intention in {SignalIntention.OPEN_LONG, SignalIntention.OPEN_SHORT}

    if intent_type is None:
        return LifecycleIntentType.OPEN if is_open_signal else LifecycleIntentType.CLOSE

    normalized_intent_type = (
        intent_type
        if isinstance(intent_type, LifecycleIntentType)
        else LifecycleIntentType(str(intent_type).strip().lower())
    )
    allowed_intent_types = (
        {LifecycleIntentType.OPEN, LifecycleIntentType.SCALE_IN}
        if is_open_signal
        else {LifecycleIntentType.CLOSE, LifecycleIntentType.SCALE_OUT}
    )
    if normalized_intent_type not in allowed_intent_types:
        allowed_values = ", ".join(sorted(intent_type.value for intent_type in allowed_intent_types))
        raise ValueError(f"{normalized_intention.value} signals only support lifecycle intent types: {allowed_values}")
    return normalized_intent_type


def decision_type_from_signal(
    intention: SignalIntention | str,
    intent_type: LifecycleIntentType | str | None = None,
) -> str:
    """Map the legacy signal contract onto the canonical strategy-decision type."""

    normalized_intention = normalize_signal_intention(intention)
    normalized_intent_type = normalize_lifecycle_intent_type(normalized_intention, intent_type)
    if normalized_intent_type == LifecycleIntentType.SCALE_IN:
        return "scale_in"
    if normalized_intent_type == LifecycleIntentType.SCALE_OUT:
        return "scale_out"

    mapping = {
        SignalIntention.OPEN_LONG: "open_long",
        SignalIntention.CLOSE_LONG: "close_long",
        SignalIntention.OPEN_SHORT: "open_short",
        SignalIntention.CLOSE_SHORT: "close_short",
    }
    return mapping[normalized_intention]


class PositionState(str, Enum):
    """Declarative strategy-side view of lifecycle-backed position state."""

    FLAT = "flat"
    PENDING_OPEN_LONG = "pending_open_long"
    OPEN_LONG = "open_long"
    PENDING_CLOSE_LONG = "pending_close_long"
    PENDING_OPEN_SHORT = "pending_open_short"
    OPEN_SHORT = "open_short"
    PENDING_CLOSE_SHORT = "pending_close_short"


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
        intention: Directional trading intention (OPEN_LONG/CLOSE_LONG/OPEN_SHORT/CLOSE_SHORT)
        intent_type: Optional explicit lifecycle classification. Use ``scale_in``
            to add to an existing same-side position or ``scale_out`` to reduce one.
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
    intent_type: Optional[LifecycleIntentType] = Field(
        default=None,
        description="Optional explicit lifecycle intent type (open/close/scale_in/scale_out)",
    )
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

    @model_validator(mode="after")
    def validate_intention_and_intent_type(self) -> "Signal":
        """Ensure explicit lifecycle overrides match the directional intention."""
        normalize_lifecycle_intent_type(self.intention, self.intent_type)
        return self


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
    intent_type: LifecycleIntentType | str | None = None,
) -> Signal:
    """
    Convenience factory for creating Signal instances.

    Args:
        timestamp: Signal generation time
        strategy_id: Strategy identifier
        symbol: Instrument symbol
        intention: Trading intention (SignalIntention enum or "OPEN_LONG"/"CLOSE_LONG"/etc.)
        intent_type: Optional explicit lifecycle intent type.
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
    normalized_intent_type = None if intent_type is None else normalize_lifecycle_intent_type(intention, intent_type)

    return Signal(
        timestamp=timestamp,
        strategy_id=strategy_id,
        symbol=symbol,
        intention=intention,
        intent_type=normalized_intent_type,
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
    "LifecycleIntentType",
    "PositionState",
    "decision_type_from_signal",
    "create_signal",
    "normalize_lifecycle_intent_type",
    "normalize_signal_intention",
]
