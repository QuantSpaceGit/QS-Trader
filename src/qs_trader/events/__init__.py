"""
Event infrastructure for QS-Trader event-driven architecture.

This module provides the event system for loose coupling between services:
- Event classes: Immutable Pydantic events validated against JSON Schema contracts
  - BaseEvent: Provides envelope fields only
  - ValidatedEvent: Domain events with payload validation
  - ControlEvent: Barriers/lifecycle (no payload validation)
- EventBus: Publish/subscribe infrastructure for event distribution
- EventStore: Persistent event storage for audit trail and replay

Events enable:
- Services to communicate without direct dependencies
- Multiple consumers per event (one-to-many)
- Audit trail and replay capability
- Deterministic backtesting
- Causality tracking via correlation_id and causation_id

SIMPLIFIED event system focused on data events and control events.
Other events (signals, orders, fills, portfolio) to be added as services are rebuilt.
"""

from qs_trader.events.event_bus import EventBus, IEventBus
from qs_trader.events.events import (
    BacktestEndedEvent,
    BacktestStartedEvent,
    BarCloseEvent,
    BaseEvent,
    ControlEvent,
    CorporateActionEvent,
    FillEvent,
    IndicatorEvent,
    OrderEvent,
    PortfolioPosition,
    PortfolioStateEvent,
    PriceBarEvent,
    RiskEvaluationTriggerEvent,
    SignalEvent,
    StrategyGroup,
    ValidatedEvent,
    ValuationTriggerEvent,
)
from qs_trader.events.lifecycle_context import CANONICAL_PRICE_BASIS, NO_PRICE_BASIS, LifecycleRunContext
from qs_trader.events.lifecycle_events import (
    FillLifecycleEvent,
    LifecycleBaseEvent,
    LifecycleValidatedEvent,
    OrderIntentEvent,
    OrderLifecycleEvent,
    PortfolioLifecycleEvent,
    PositionLifecycleEvent,
    StrategyDecisionEvent,
    TradeLifecycleEvent,
)

__all__ = [
    # Base classes
    "BaseEvent",
    "ValidatedEvent",
    "LifecycleBaseEvent",
    "LifecycleValidatedEvent",
    "ControlEvent",
    # Market Data
    "PriceBarEvent",
    "CorporateActionEvent",
    # Trading Events
    "SignalEvent",
    "IndicatorEvent",
    "OrderEvent",
    "FillEvent",
    "StrategyDecisionEvent",
    "OrderIntentEvent",
    "OrderLifecycleEvent",
    "FillLifecycleEvent",
    "TradeLifecycleEvent",
    "PositionLifecycleEvent",
    "PortfolioLifecycleEvent",
    # Portfolio Events
    "PortfolioStateEvent",
    "PortfolioPosition",
    "StrategyGroup",
    # Barrier Events
    "RiskEvaluationTriggerEvent",
    "ValuationTriggerEvent",
    "BarCloseEvent",
    # Backtest Lifecycle
    "BacktestStartedEvent",
    "BacktestEndedEvent",
    # Lifecycle Runtime Context
    "LifecycleRunContext",
    "CANONICAL_PRICE_BASIS",
    "NO_PRICE_BASIS",
    # EventBus
    "IEventBus",
    "EventBus",
]
