"""
EventBus implementation for QS-Trader event-driven architecture.

Provides synchronous, deterministic publish/subscribe infrastructure
optimized for backtesting. Events are dispatched in priority order with
error isolation and complete history tracking.

Key Features:
- Synchronous execution (no async/threading)
- Deterministic ordering (priority-based)
- Error isolation (one handler failure doesn't stop others)
- Event history for replay and debugging
- Memory-bounded history (configurable)
"""

from collections import defaultdict, deque
from datetime import datetime
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ContextManager,
    Dict,
    List,
    Optional,
    Protocol,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

# Event type registry for ergonomic subscribe
from qs_trader.events.events import BaseEvent
from qs_trader.system import LoggerFactory

EventT = TypeVar("EventT", bound=BaseEvent)

logger = LoggerFactory.get_logger()

if TYPE_CHECKING:  # pragma: no cover - only for type checkers
    from qs_trader.events.event_store import EventStore


class IEventBus(Protocol):
    """
    Event bus interface for publish/subscribe messaging.

    Enables loose coupling between services by allowing them to communicate
    via events rather than direct method calls. Services publish events when
    something interesting happens, and other services subscribe to receive
    those events.

    Benefits:
    - Services don't need to know about each other
    - One event can have multiple consumers
    - Easy to add new services without modifying existing ones
    - Complete audit trail of all events
    - Enables replay for debugging and testing

    Usage:
        >>> bus = EventBus()
        >>>
        >>> # Service subscribes to events
        >>> def handle_fill(event: FillEvent):
        ...     portfolio.apply_fill(event)
        >>> bus.subscribe("fill", handle_fill, priority=10)
        >>>
        >>> # Service publishes events
        >>> fill_event = FillEvent(...)
        >>> bus.publish(fill_event)  # Handler called synchronously
    """

    def publish(self, event: BaseEvent) -> None:
        """
        Publish event to all subscribers.

        Calls all registered handlers for this event type synchronously
        in priority order. If a handler raises an exception, it is logged
        but other handlers continue to be called.

        Args:
            event: Event to publish

        Example:
            >>> bus.publish(PriceBarEvent(symbol="AAPL", bar=bar))
        """
        ...

    @overload
    def subscribe(
        self,
        event_type: str,
        handler: Callable[[BaseEvent], None],
        priority: int = 0,
    ) -> "SubscriptionToken": ...

    @overload
    def subscribe(
        self,
        event_type: Type[EventT],
        handler: Callable[[EventT], None],
        priority: int = 0,
    ) -> "SubscriptionToken": ...

    def subscribe(
        self,
        event_type: Union[str, Type[BaseEvent]],
        handler: Callable[[Any], None],
        priority: int = 0,
    ) -> "SubscriptionToken":
        """
        Subscribe to event type.

        Registers a handler to be called when events of the specified type
        are published. Handlers with higher priority are called first.

        Args:
            event_type: Type of event to subscribe to (e.g., 'fill', 'price_bar')
                       or event class type for type-safe subscription
            handler: Callback function to handle event
            priority: Handler priority (higher = called first, default=0)

        Returns:
            SubscriptionToken for context-managed unsubscription

        Example:
            >>> # String-based subscription
            >>> bus.subscribe("fill", handle_fill, priority=10)
            >>>
            >>> # Type-safe subscription
            >>> bus.subscribe(FillEvent, handle_fill, priority=10)
            >>>
            >>> # Context manager for automatic cleanup
            >>> with bus.subscribe(FillEvent, handle_fill):
            ...     # Handler active in this block
            ...     pass
            >>> # Handler automatically unsubscribed
        """
        ...

    def unsubscribe(
        self,
        event_type: str,
        handler: Callable[[BaseEvent], None],
    ) -> None:
        """
        Unsubscribe from event type.

        Removes a previously registered handler. If handler was not
        subscribed, this is a no-op.

        Args:
            event_type: Type of event to unsubscribe from
            handler: Handler to remove

        Example:
            >>> bus.unsubscribe("fill", my_handler)
        """
        ...

    def get_history(
        self,
        event_type: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[BaseEvent]:
        """
        Get event history with optional filters.

        Useful for debugging, replay, and analysis. Returns events in
        chronological order.

        Args:
            event_type: Filter by event type (None = all types)
            since: Filter by timestamp (None = all time)
            limit: Max events to return (None = no limit)

        Returns:
            List of events matching filters

        Example:
            >>> # Get all fills in last hour
            >>> fills = bus.get_history(
            ...     event_type="fill",
            ...     since=datetime.now() - timedelta(hours=1)
            ... )
        """
        ...

    def clear_history(self) -> None:
        """
        Clear event history.

        Useful for starting a new backtest with clean state.

        Example:
            >>> bus.clear_history()  # Start fresh
        """
        ...


class SubscriptionToken(ContextManager):
    """Token for context-managed subscription removal."""

    def __init__(self, bus: "EventBus", event_type: str, handler: Callable):
        self.bus = bus
        self.event_type = event_type
        self.handler = handler
        self._active = True

    def unsubscribe(self):
        if self._active:
            self.bus.unsubscribe(self.event_type, self.handler)
            self._active = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.unsubscribe()


class EventBus:
    """
    Synchronous event bus for deterministic backtesting.

    Features:
    - Synchronous execution: publish() blocks until all handlers complete
    - Deterministic ordering: Handlers called in priority order (highest first)
    - Error isolation: One handler failure doesn't stop others
    - Event history: All events stored for replay/debugging
    - Memory bounded: History capped to prevent memory issues

    Thread Safety: NOT thread-safe (backtesting is single-threaded)

    Performance: Optimized for in-memory, single-threaded backtesting.
    Suitable for millions of events.

    Example:
        >>> # Setup
        >>> bus = EventBus(max_history=100_000)
        >>>
        >>> # Subscribe with priority
        >>> bus.subscribe("fill", portfolio.handle_fill, priority=100)
        >>> bus.subscribe("fill", analytics.record_fill, priority=50)
        >>>
        >>> # Publish (handlers called in priority order)
        >>> bus.publish(FillEvent(...))
        >>>
        >>> # Query history
        >>> recent_fills = bus.get_history(event_type="fill", limit=10)
    """

    def __init__(
        self,
        max_history: int = 100_000,
        event_store: Optional["EventStore"] = None,
        display_events: Optional[list[str]] = None,
    ):
        """
        Initialize event bus.

        Args:
            max_history: Maximum events to keep in history (0 = unlimited).
                        When limit reached, oldest events are discarded.
                        Default 100k events ≈ 40 years of daily backtesting.
            event_store: Optional persistence backend. When provided every
                         published event is appended to the store before
                         handlers run. Store failures are logged but do not
                         interrupt handler execution.
            display_events: Optional list of event types to display in console
                          (e.g., ["bar", "signal", "fill"]). Use ["*"] for all events.
                          Use None or [] to disable event display.
                          Controlled by display_events in portfolio config.
        """
        # Store handlers by event type: {event_type: [(priority, handler), ...]}
        self._subscribers: Dict[str, List[Tuple[int, Callable]]] = defaultdict(list)
        self._handler_cache: Dict[str, List[Tuple[int, Callable]]] = {}
        self._event_history: deque[BaseEvent] = deque(maxlen=max_history if max_history > 0 else None)
        self._max_history = max_history
        self._on_publish: Optional[Callable[[BaseEvent], BaseEvent]] = None
        self._on_error: Optional[Callable[[BaseEvent, Callable, Exception], None]] = None
        self._event_store: Optional["EventStore"] = event_store
        self._display_events = display_events or []
        logger.debug("event_bus.initialized", max_history=max_history, display_events=self._display_events)

    def _should_display_event(self, event: BaseEvent) -> bool:
        """Check if event should be displayed in console based on configuration."""
        if not self._display_events:
            return False

        # Check for wildcard (display all events)
        if "*" in self._display_events:
            return True

        # Check if specific event type is in display list
        return event.event_type in self._display_events

    def _log_event(self, event: BaseEvent) -> None:
        """
        Log event for display with Rich formatting.

        Uses qs_trader.events.{event_type} logger name to trigger Rich formatting
        in the console renderer. All event fields are passed as structured log context.
        """
        # Get event-specific logger (triggers Rich formatting in renderer)
        event_logger = LoggerFactory.get_logger(f"qs_trader.events.{event.event_type}")

        # Extract event data as dict for structured logging
        event_data = event.model_dump() if hasattr(event, "model_dump") else {}

        # Note: Keep timestamp field as-is - it contains backtest simulation time
        # not real-world creation time. Structlog will add its own timestamp
        # as "timestamp" but event fields take precedence in event_dict.

        # Log with special marker event name
        event_logger.info("event.display", **event_data)

    def publish(self, event: BaseEvent) -> None:
        """
        Publish event to all subscribers.

        Processing order:
        1. Add event to history
        2. Get handlers for this event type
        3. Sort handlers by priority (highest first)
        4. Call each handler synchronously
        5. If handler raises exception, log it but continue

        This ensures:
        - Deterministic execution (same events → same order)
        - Error isolation (one failure doesn't cascade)
        - Complete audit trail (all events in history)

        Args:
            event: Event to publish
        """
        import time

        start = time.perf_counter()
        # Pre-publish hook
        if self._on_publish:
            event = self._on_publish(event)
        if self._event_store is not None:
            try:
                self._event_store.append(event)
            except Exception as exc:
                logger.error(
                    "event_bus.store_error",
                    event_type=event.event_type,
                    event_id=event.event_id,
                    error=str(exc),
                )
        self._event_history.append(event)

        # Display event if configured
        if self._should_display_event(event):
            self._log_event(event)

        # Handler cache for performance
        sorted_handlers = self._handler_cache.get(event.event_type)
        if sorted_handlers is None:
            handlers = self._subscribers.get(event.event_type, [])
            sorted_handlers = sorted(handlers, key=lambda x: x[0], reverse=True)
            self._handler_cache[event.event_type] = sorted_handlers
        errors = []
        for priority, handler in sorted_handlers:
            try:
                handler(event)
            except Exception as e:
                errors.append((handler, e))
                logger.error(
                    "event_bus.handler_error",
                    event_type=event.event_type,
                    event_id=event.event_id,
                    handler=getattr(handler, "__name__", str(handler)),
                    error=str(e),
                )
                if self._on_error:
                    self._on_error(event, handler, e)
        duration = time.perf_counter() - start
        logger.debug(
            "event_bus.published",
            event_type=event.event_type,
            event_id=event.event_id,
            version=getattr(event, "event_version", None),
            subscriber_count=len(sorted_handlers),
            duration=duration,
            errors=len(errors),
        )

    @overload
    def subscribe(
        self, event_type: str, handler: Callable[[BaseEvent], None], priority: int = 0
    ) -> SubscriptionToken: ...
    @overload
    def subscribe(
        self, event_type: Type[EventT], handler: Callable[[EventT], None], priority: int = 0
    ) -> SubscriptionToken: ...

    def subscribe(
        self, event_type: Union[str, Type[BaseEvent]], handler: Callable[[Any], None], priority: int = 0
    ) -> SubscriptionToken:
        """
        Subscribe to event type (by string or class).
        Returns a SubscriptionToken for context-managed unsubscription.
        """
        if isinstance(event_type, str):
            event_type_str: str = event_type
        else:
            tmp_type = getattr(event_type, "event_type", None)
            if tmp_type is None or not isinstance(tmp_type, str):
                raise ValueError(f"Event class {event_type} missing event_type")
            event_type_str: str = tmp_type  # type: ignore
        self._subscribers[event_type_str].append((priority, handler))
        self._handler_cache.pop(event_type_str, None)  # Invalidate cache
        logger.debug(
            "event_bus.subscribed",
            event_type=event_type_str,
            handler=getattr(handler, "__name__", str(handler)),
            priority=priority,
            total_handlers=len(self._subscribers[event_type_str]),
        )
        return SubscriptionToken(self, event_type_str, handler)

    def unsubscribe(self, event_type: str, handler: Callable[[BaseEvent], None]) -> None:
        """
        Unsubscribe from event type.

        Removes handler from subscriber list. If handler was not subscribed,
        this is a no-op (idempotent).

        Args:
            event_type: Type of event to unsubscribe from
            handler: Handler to remove
        """
        if event_type not in self._subscribers:
            return
        original_count = len(self._subscribers[event_type])
        self._subscribers[event_type] = [(p, h) for p, h in self._subscribers[event_type] if h != handler]
        removed_count = original_count - len(self._subscribers[event_type])
        self._handler_cache.pop(event_type, None)  # Invalidate cache
        if removed_count > 0:
            logger.debug(
                "event_bus.unsubscribed",
                event_type=event_type,
                handler=getattr(handler, "__name__", str(handler)),
                removed_count=removed_count,
            )

    def get_history(
        self,
        event_type: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[BaseEvent]:
        """
        Get event history with optional filters.

        Filters are applied in order:
        1. Filter by event type (if specified)
        2. Filter by timestamp (if specified)
        3. Limit results (if specified)

        Args:
            event_type: Filter by event type (None = all types)
            since: Filter by timestamp (None = all time)
            limit: Max events to return (None = no limit)

        Returns:
            List of events in chronological order
        """
        events = list(self._event_history)
        if event_type is not None:
            events = [e for e in events if e.event_type == event_type]
        if since is not None:
            events = [e for e in events if e.occurred_at >= since]
        if limit is not None:
            events = events[-limit:]
        return events

    def clear_history(self) -> None:
        """
        Clear event history.

        Useful for starting a new backtest run with clean state.
        Does not affect subscriptions.
        """
        self._event_history.clear()
        logger.debug("event_bus.history_cleared")

    def get_subscriber_count(self, event_type: str) -> int:
        """
        Get number of subscribers for event type.

        Useful for debugging and monitoring.

        Args:
            event_type: Event type to check

        Returns:
            Number of registered handlers for this event type
        """
        return len(self._subscribers.get(event_type, []))

    def inspect_subscribers(self, event_type: str) -> List[str]:
        """List handler names for diagnostics."""
        return [getattr(h, "__name__", str(h)) for _, h in self._subscribers.get(event_type, [])]

    def set_middleware(
        self,
        on_publish: Optional[Callable[[BaseEvent], BaseEvent]] = None,
        on_error: Optional[Callable[[BaseEvent, Callable, Exception], None]] = None,
    ) -> None:
        """Set bus middleware hooks."""
        self._on_publish = on_publish
        self._on_error = on_error
        logger.debug(
            "event_bus.middleware_set",
            on_publish=getattr(on_publish, "__name__", None) if on_publish else None,
            on_error=getattr(on_error, "__name__", None) if on_error else None,
        )

    def attach_store(self, event_store: "EventStore") -> None:
        """
        Attach persistence backend to capture every published event.

        Args:
            event_store: EventStore implementation (e.g., InMemoryEventStore, SQLiteEventStore)
        """
        self._event_store = event_store
        logger.debug(
            "event_bus.store_attached",
            backend=event_store.__class__.__name__,
        )

    def detach_store(self) -> None:
        """Detach persistence backend."""
        if self._event_store is not None:
            logger.debug(
                "event_bus.store_detached",
                backend=self._event_store.__class__.__name__,
            )
        self._event_store = None

    def get_all_event_types(self) -> list[str]:
        """
        Get list of all event types with subscribers.

        Useful for debugging and monitoring.

        Returns:
            List of event types that have at least one subscriber
        """
        return list(self._subscribers.keys())
