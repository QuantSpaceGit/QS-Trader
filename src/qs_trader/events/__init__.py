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

__all__ = [
    # Base classes
    "BaseEvent",
    "ValidatedEvent",
    "ControlEvent",
    # Market Data
    "PriceBarEvent",
    "CorporateActionEvent",
    # Trading Events
    "SignalEvent",
    "IndicatorEvent",
    "OrderEvent",
    "FillEvent",
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
    # EventBus
    "IEventBus",
    "EventBus",
]
