"""
Strategy Context Implementation.

Provides runtime context to strategies during execution. Strategies use
this to emit signals and query market state (positions, prices, bars).

Architecture:
- Context is strategy's interface to the outside world
- Strategies NEVER directly access services or event bus
- All communication flows through Context

Philosophy:
- Strategies declare INTENT (signals), not orders
- Strategies ask for STATE (positions, prices), don't manage it
- Strategies are STATELESS regarding portfolio (no position tracking)
"""

import uuid
from collections import deque
from datetime import datetime, timezone
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Optional

import structlog

from qs_trader.events.event_bus import IEventBus
from qs_trader.events.events import FillEvent, IndicatorEvent, PriceBarEvent, SignalEvent
from qs_trader.events.lifecycle_context import LifecycleRunContext
from qs_trader.events.lifecycle_events import StrategyDecisionEvent
from qs_trader.events.price_basis import BarView, PriceBasis
from qs_trader.services.manager.lifecycle_intent_projection import LifecycleIntentProjection
from qs_trader.services.strategy.models import (
    LifecycleIntentType,
    PositionState,
    SignalIntention,
    decision_type_from_signal,
    normalize_lifecycle_intent_type,
)

if TYPE_CHECKING:
    from qs_trader.services.features.service import FeatureService

logger = structlog.get_logger()


class Context:
    """
    Strategy execution context.

    Public strategy-facing responsibilities:
    - Emit declarative trading signals.
    - Expose explicit-basis price and bar queries.
    - Provide a strategy-safe position-state view derived from fills.

    The Context is created per strategy instance and owns a rolling in-memory
    bar cache per symbol. Strategies never reach through to services or the
    event bus directly; everything flows through this boundary.
    """

    def __init__(
        self,
        strategy_id: str,
        event_bus: IEventBus,
        max_bars: int = 500,
        config: Optional[dict[str, Any]] = None,
        feature_service: Optional["FeatureService"] = None,
        lifecycle_context: Optional[LifecycleRunContext] = None,
        lifecycle_projection: LifecycleIntentProjection | None = None,
    ):
        """
        Initialize context for a strategy.

        Args:
            strategy_id: Unique strategy identifier (from config.name)
            event_bus: Event bus for publishing signals
            max_bars: Maximum bars to cache per symbol (default 500)
            config: Optional strategy configuration dict for feature flags
        """
        self._strategy_id = strategy_id
        self._event_bus = event_bus
        self._signal_count = 0  # Track emitted signals for logging
        self._max_bars = max_bars
        self._config = config or {}
        # Bar cache for indicator calculations: {symbol: deque[PriceBarEvent]}
        # Uses deque with maxlen for automatic windowing
        self._bar_cache: dict[str, deque[PriceBarEvent]] = {}
        self._position_quantities: dict[str, Decimal] = {}

        # Optional FeatureService for consuming precomputed features from ClickHouse
        self._feature_service = feature_service
        self._lifecycle_context = lifecycle_context
        self._lifecycle_projection = lifecycle_projection

        # Indicator tracking for automatic event emission: {indicator_name: value}
        # Reset at start of each bar, emitted at end if log_indicators: true
        self._indicators: dict[str, Any] = {}

        logger.debug(
            "strategy.context.initialized",
            strategy_id=strategy_id,
            max_bars=max_bars,
            log_indicators=self._config.get("log_indicators", False),
            has_feature_service=feature_service is not None,
        )

    def emit_signal(
        self,
        timestamp: str,
        symbol: str,
        intention: SignalIntention | str,
        price: Decimal | float | str,
        confidence: Decimal | float | str,
        reason: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
        stop_loss: Optional[Decimal | float | str] = None,
        take_profit: Optional[Decimal | float | str] = None,
        intent_type: LifecycleIntentType | str | None = None,
    ) -> SignalEvent:
        """
        Emit a trading signal to the event bus.

        This is the PRIMARY way strategies communicate their trading intent.
        Signals are published to the event bus where other services can consume them.

        Args:
            timestamp: Signal generation time (ISO8601 UTC string, usually bar.timestamp)
            symbol: Instrument symbol to trade
            intention: Trading intention (OPEN_LONG/CLOSE_LONG/OPEN_SHORT/CLOSE_SHORT)
            intent_type: Optional explicit lifecycle classification.
                Use ``scale_in`` to add to an existing same-side position and
                ``scale_out`` to reduce one without changing direction.
            price: Price at which signal generated (typically current market price)
            confidence: Signal strength [0.0, 1.0]
            reason: Optional human-readable explanation
            metadata: Optional additional context (indicator values, etc.)
            stop_loss: Optional stop loss price
            take_profit: Optional take profit price

        Returns:
            The created SignalEvent instance (for testing/logging)

        Example:
            >>> context.emit_signal(
            ...     timestamp="2024-01-02T16:00:00Z",
            ...     symbol="AAPL",
            ...     intention=SignalIntention.OPEN_LONG,
            ...     price=Decimal("150.25"),
            ...     confidence=0.85,
            ...     reason="SMA crossover: fast > slow",
            ...     metadata={"fast_sma": 150.0, "slow_sma": 148.0},
            ...     stop_loss=Decimal("145.00"),
            ...     take_profit=Decimal("160.00")
            ... )

        Notes:
            - Signal is automatically tagged with strategy_id
            - Signal is published to event bus immediately
            - Signal validates against signal.v1.json schema
            - RiskService decides whether to act on the signal
            - Multiple signals can be emitted per bar (if needed)
        """
        # Convert to Decimal if needed
        if not isinstance(price, Decimal):
            price = Decimal(str(price))
        if not isinstance(confidence, Decimal):
            confidence = Decimal(str(confidence))
        if stop_loss is not None and not isinstance(stop_loss, Decimal):
            stop_loss = Decimal(str(stop_loss))
        if take_profit is not None and not isinstance(take_profit, Decimal):
            take_profit = Decimal(str(take_profit))
        normalized_intent_type = normalize_lifecycle_intent_type(intention, intent_type)

        # Generate unique signal_id using UUID
        decision_event: StrategyDecisionEvent | None = None
        if self._lifecycle_context is not None:
            decision_id = str(uuid.uuid4())
            signal_id = decision_id
            correlation_id = decision_id
            occurred_at_dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if occurred_at_dt.tzinfo is None:
                occurred_at_dt = occurred_at_dt.replace(tzinfo=timezone.utc)
            else:
                occurred_at_dt = occurred_at_dt.astimezone(timezone.utc)

            decision_event = StrategyDecisionEvent(
                experiment_id=self._lifecycle_context.experiment_id,
                run_id=self._lifecycle_context.run_id,
                occurred_at=occurred_at_dt,
                decision_id=decision_id,
                strategy_id=self._strategy_id,
                symbol=symbol,
                bar_timestamp=timestamp,
                decision_type=self._map_intention_to_decision_type(intention, normalized_intent_type),
                decision_price=price,
                decision_basis=self._lifecycle_context.decision_basis,
                confidence=confidence,
                indicator_context=self.get_tracked_indicators() or None,
                reason=reason,
                metadata=metadata,
                source_service="strategy_service",
                correlation_id=correlation_id,
            )
            self._event_bus.publish(decision_event)
        else:
            # Format: {strategy_id}-{uuid} for traceability and uniqueness across runs
            signal_id = f"{self._strategy_id}-{uuid.uuid4()}"

            # Generate correlation_id for this trading workflow
            # This will be propagated through Order → Fill → Trade for full traceability
            # Must be a plain UUID string (no prefix) to match envelope schema pattern
            correlation_id = str(uuid.uuid4())

        # Create SignalEvent (validates against schema)
        signal = SignalEvent(
            signal_id=signal_id,
            timestamp=timestamp,
            strategy_id=self._strategy_id,
            symbol=symbol,
            intention=intention,
            intent_type=normalized_intent_type.value,
            price=price,
            confidence=confidence,
            reason=reason,
            metadata=metadata,
            stop_loss=stop_loss,
            take_profit=take_profit,
            source_service="strategy_service",
            correlation_id=correlation_id,
            causation_id=decision_event.event_id if decision_event is not None else None,
        )

        # Publish to event bus
        self._event_bus.publish(signal)

        # Track for logging
        self._signal_count += 1

        logger.info(
            "strategy.signal.emitted",
            strategy_id=self._strategy_id,
            symbol=symbol,
            intention=signal.intention,
            price=str(signal.price),
            confidence=str(signal.confidence),
            reason=signal.reason,
            total_signals=self._signal_count,
        )

        return signal

    @staticmethod
    def _map_intention_to_decision_type(
        intention: SignalIntention | str,
        intent_type: LifecycleIntentType | str | None = None,
    ) -> str:
        """Map the signal contract onto the canonical lifecycle decision type."""
        return decision_type_from_signal(intention, intent_type)

    def track_indicators(
        self,
        indicators: dict[str, Any],
        display_names: dict[str, str] | None = None,
        placements: dict[str, str] | None = None,
        colors: dict[str, str] | None = None,
    ) -> None:
        """
        Track indicator values for automatic event emission with optional display names and visualization metadata.

        Strategies call this to register calculated indicators.
        If log_indicators: true in strategy config, StrategyService will automatically
        emit an IndicatorEvent at the end of on_bar() processing.

        Indicators are already on the correct scale (backward-adjusted from data service).
        No type classification or adjustment logic needed.

        Args:
            indicators: Dictionary of indicator key to value
                Values can be any JSON-serializable type (float, int, bool, str)
            display_names: Optional mapping from indicator keys to display names
                Display names are used in CSV exports and include parameters
                If not provided, uses the indicator keys as-is
            placements: Optional mapping from indicator keys to placement type
                Values: "overlay" (same chart as price), "subplot" (separate chart), "volume"
                If not provided, defaults to "subplot"
            colors: Optional mapping from indicator keys to hex color codes
                Format: "#RRGGBB" (e.g., "#667eea")
                If not provided, uses default colors

        Example:
            ```python
            def on_bar(self, event: PriceBarEvent, context: Context) -> None:
                bars = context.get_bars(event.symbol, n=50)
                if bars is None or len(bars) < 50:
                    return

                # Calculate indicators on backward-adjusted prices
                fast_sma = sum(b.close for b in bars[-10:]) / 10
                slow_sma = sum(b.close for b in bars[-50:]) / 50
                rsi = calculate_rsi(bars, 14)
                atr = calculate_atr(bars, 14)
                crossover = fast_sma > slow_sma

                # Track indicators with display names and visualization metadata
                context.track_indicators(
                    indicators={
                        "fast_sma": float(fast_sma),
                        "slow_sma": float(slow_sma),
                        "rsi": float(rsi),
                        "atr": float(atr),
                        "golden_cross": crossover,
                    },
                    display_names={
                        "fast_sma": "SMA(10)",
                        "slow_sma": "SMA(50)",
                        "rsi": "RSI(14)",
                        "atr": "ATR(14)",
                        "golden_cross": "Golden Cross",
                    },
                    placements={
                        "fast_sma": "overlay",
                        "slow_sma": "overlay",
                        "rsi": "subplot",
                        "atr": "subplot",
                        "golden_cross": "subplot",
                    },
                    colors={
                        "fast_sma": "#667eea",
                        "slow_sma": "#764ba2",
                        "rsi": "#fa709a",
                        "atr": "#f093fb",
                    }
                )

                # Rest of trading logic...
                if crossover:
                    context.emit_signal(...)
            ```

        Note:
            - Accumulates indicators within a bar (can call multiple times)
            - Indicators cleared by StrategyService at start of each bar
            - Only emitted if log_indicators: true in strategy config
            - Display names appear in CSV exports as ticker column
            - Placement and colors used for HTML report chart rendering
        """
        # Store indicator values with display names
        for key, value in indicators.items():
            # Convert bool to float for JSON serialization (True→1.0, False→0.0)
            if isinstance(value, bool):
                value = 1.0 if value else 0.0

            # Use display name if provided, otherwise use key
            display_name = display_names.get(key, key) if display_names else key
            self._indicators[display_name] = value

        # Store visualization metadata if provided
        if placements:
            if not hasattr(self, "_indicator_placements"):
                self._indicator_placements: dict[str, str] = {}
            for key, placement in placements.items():
                display_name = display_names.get(key, key) if display_names else key
                self._indicator_placements[display_name] = placement

        if colors:
            if not hasattr(self, "_indicator_colors"):
                self._indicator_colors: dict[str, str] = {}
            for key, color in colors.items():
                display_name = display_names.get(key, key) if display_names else key
                self._indicator_colors[display_name] = color

    def emit_indicator_event(
        self,
        symbol: str,
        timestamp: str,
        indicators: dict[str, Any],
        metadata: Optional[dict[str, Any]] = None,
    ) -> "IndicatorEvent":
        """
        Emit indicator event.

        Called by StrategyService when log_indicators: true.

        Args:
            symbol: Security symbol
            timestamp: Bar timestamp (ISO8601 UTC)
            indicators: Dictionary of indicator name to value
            metadata: Optional metadata (e.g., signal strength, regime)

        Returns:
            IndicatorEvent published to event bus

        Note:
            - Usually called automatically by StrategyService
            - Strategies should use track_indicators() instead
        """
        # Merge visualization metadata into metadata dict
        if metadata is None:
            metadata = {}

        # Add placements if tracked
        if hasattr(self, "_indicator_placements") and self._indicator_placements:
            metadata["placements"] = self._indicator_placements.copy()

        # Add colors if tracked
        if hasattr(self, "_indicator_colors") and self._indicator_colors:
            metadata["colors"] = self._indicator_colors.copy()

        # Create IndicatorEvent (validates against schema)
        # event_id and occurred_at are auto-generated by BaseEvent
        indicator_event = IndicatorEvent(
            strategy_id=self._strategy_id,
            symbol=symbol,
            timestamp=timestamp,
            indicators=indicators,
            metadata=metadata if metadata else None,
            source_service="strategy_service",
        )

        # Publish to event bus
        self._event_bus.publish(indicator_event)

        logger.debug(
            "strategy.indicator.emitted",
            strategy_id=self._strategy_id,
            symbol=symbol,
            indicator_count=len(indicators),
            has_metadata=metadata is not None,
        )

        return indicator_event

    def cache_bar(self, event: PriceBarEvent) -> None:
        """
        Cache bar for historical queries.

        Called by StrategyService before on_bar() to maintain rolling window
        of bars per symbol for historical lookups.

        Bars carry both base OHLC fields and the adjusted ClickHouse series
        when available. Strategy price selection happens later via
        adjustment-mode-aware accessors such as get_price() and
        get_price_series().

        Args:
            event: PriceBarEvent to cache (already backward-adjusted from data service)
        """
        symbol = event.symbol

        # Create deque for symbol if first bar
        if symbol not in self._bar_cache:
            self._bar_cache[symbol] = deque(maxlen=self._max_bars)

        # Append bar to cache (deque auto-evicts oldest if at maxlen)
        self._bar_cache[symbol].append(event)

        logger.debug(
            "strategy.context.bar_cached",
            strategy_id=self._strategy_id,
            symbol=symbol,
            timestamp=event.timestamp,
            cached_bars=len(self._bar_cache[symbol]),
            close=str(event.close),
        )

    def record_fill(self, event: FillEvent) -> None:
        """Update the strategy-local position snapshot from a fill event."""
        signed_quantity = event.filled_quantity
        side = event.side.lower()
        if side == "sell":
            signed_quantity = -signed_quantity
        elif side != "buy":
            raise ValueError(f"Unsupported fill side: {event.side!r}")

        new_quantity = self._position_quantities.get(event.symbol, Decimal("0")) + signed_quantity
        if new_quantity == Decimal("0"):
            self._position_quantities.pop(event.symbol, None)
        else:
            self._position_quantities[event.symbol] = new_quantity

        logger.debug(
            "strategy.context.fill_recorded",
            strategy_id=self._strategy_id,
            symbol=event.symbol,
            side=event.side,
            filled_quantity=str(event.filled_quantity),
            net_quantity=str(new_quantity),
            position_state=self.get_position_state(event.symbol).value,
        )

    def get_price(self, symbol: str, basis: PriceBasis | str) -> Optional[Decimal]:
        """
        Get the latest cached price for a symbol resolved to the requested basis.

        Args:
            symbol: Instrument symbol
            basis: Requested price basis (`raw` or `adjusted`)

        Returns:
            Latest price as Decimal, or None if no bars cached
        """
        if symbol not in self._bar_cache or len(self._bar_cache[symbol]) == 0:
            logger.debug(
                "strategy.context.get_price.no_data",
                strategy_id=self._strategy_id,
                symbol=symbol,
            )
            return None

        latest_bar = self._bar_cache[symbol][-1]
        price, field_name = self._resolve_bar_value(latest_bar, "close", basis)
        resolved_basis = self._coerce_basis(basis)

        logger.debug(
            "strategy.context.get_price.success",
            strategy_id=self._strategy_id,
            symbol=symbol,
            price=str(price),
            basis=resolved_basis.value,
            field=field_name,
            timestamp=latest_bar.timestamp,
        )

        return price

    def get_bars(self, symbol: str, n: int, basis: PriceBasis | str) -> Optional[list[BarView]]:
        """
        Get ``n`` most recent bars for a symbol resolved to the requested basis.

        Args:
            symbol: Instrument symbol
            n: Number of bars to retrieve
            basis: Requested price basis (`raw` or `adjusted`)

        Returns:
            List of immutable `BarView` objects in chronological order,
            or None if insufficient bars cached
        """
        cached_bars = self._get_cached_bars_window(symbol=symbol, n=n)
        if cached_bars is None:
            return None

        resolved_basis = self._coerce_basis(basis)
        bars = [self._build_bar_view(bar, resolved_basis) for bar in cached_bars]

        logger.debug(
            "strategy.context.get_bars.success",
            strategy_id=self._strategy_id,
            symbol=symbol,
            requested=n,
            returned=len(bars),
            basis=resolved_basis.value,
        )

        return bars

    def get_price_series(
        self,
        symbol: str,
        n: int,
        basis: PriceBasis | str,
        offset: int = 0,
    ) -> Optional[list[Decimal]]:
        """
        Get ``n`` close prices for a symbol resolved to the requested basis.

        ``offset=0`` includes the latest cached bar, ``offset=1`` excludes the
        latest cached bar, and so on. This allows strategies to request exact
        prior windows without manual slicing bugs.

        Args:
            symbol: Instrument symbol
            n: Number of prices to retrieve
            basis: Requested price basis (`raw` or `adjusted`)
            offset: Number of most-recent bars to skip before building the series

        Returns:
            List of Decimal prices in chronological order (oldest first),
            or None if insufficient bars cached
        """
        cached_bars = self._get_cached_bars_window(symbol=symbol, n=n, offset=offset)
        if cached_bars is None:
            return None

        prices_with_fields = [self._resolve_bar_value(bar, "close", basis) for bar in cached_bars]
        prices = [price for price, _ in prices_with_fields]
        resolved_basis = self._coerce_basis(basis)
        field_name = prices_with_fields[-1][1]

        logger.debug(
            "strategy.context.get_price_series.success",
            strategy_id=self._strategy_id,
            symbol=symbol,
            requested=n,
            returned=len(prices),
            offset=offset,
            basis=resolved_basis.value,
            field=field_name,
        )

        return prices

    def get_position_state(self, symbol: str) -> PositionState:
        """Return the current position state for a symbol."""
        if self._lifecycle_projection is not None:
            return self._lifecycle_projection.get_position_state(self._strategy_id, symbol)

        quantity = self._position_quantities.get(symbol, Decimal("0"))
        if quantity > 0:
            return PositionState.OPEN_LONG
        if quantity < 0:
            return PositionState.OPEN_SHORT
        return PositionState.FLAT

    def get_features(
        self,
        symbol: str,
        date: str,
        columns: Optional[list[str]] = None,
    ) -> Optional[dict[str, Any]]:
        """Fetch precomputed composite features for a symbol on a given date.

        Delegates to the injected FeatureService (QS-Datamaster ClickHouse store).
        Returns None when:
          - No FeatureService was injected at Context construction.
          - The symbol has no secid in ClickHouse.
          - The date falls before the security's warmup window (252 bars).
          - Any ClickHouse connectivity issue occurs.

        Strategies should handle None gracefully (skip logic or use fallback indicators).

        Args:
            symbol: Ticker symbol (e.g. "AAPL").
            date:   Trading date ISO string "YYYY-MM-DD" (typically event.timestamp[:10]).
            columns: Optional list of feature names to restrict the result.
                     When None, all available composite features are returned.

        Returns:
            Dict mapping feature name → value (float or string for regime labels),
            or None if features are not available for this bar.

        Example:
            def on_bar(self, event: PriceBarEvent, context: Context) -> None:
                date = event.timestamp[:10]
                features = context.get_features(event.symbol, date)
                if features is None:
                    return  # warmup or no ClickHouse connection
                momentum = features.get("momentum_score", 0.0)
                regime = features.get("composite_regime", "sideways")
                if momentum > 0.05 and regime in ("bull", "strong_bull"):
                    context.emit_signal(...)
        """
        if self._feature_service is None:
            return None
        return self._feature_service.get_features(symbol, date, columns=columns)

    def get_indicators(
        self,
        symbol: str,
        date: str,
        columns: Optional[list[str]] = None,
    ) -> Optional[dict[str, float]]:
        """Fetch raw equity indicators for a symbol on a given date.

        Similar to get_features() but queries the lower-level
        features_equity_indicators_daily table (more granular data).

        Args:
            symbol: Ticker symbol.
            date:   Trading date ISO string "YYYY-MM-DD".
            columns: Optional list of indicator names to restrict the result.

        Returns:
            Dict mapping indicator name → float, or None if not available.
        """
        if self._feature_service is None:
            return None
        return self._feature_service.get_indicators(symbol, date, columns=columns)

    def get_regime(self, date: str) -> Optional[dict[str, str]]:
        """Fetch market regime labels for a date.

        Args:
            date: Trading date ISO string "YYYY-MM-DD".

        Returns:
            Dict with keys: trend_regime, vol_regime, risk_regime,
            breadth_regime, composite_regime — all string values.
            Returns None if not available.
        """
        if self._feature_service is None:
            return None
        return self._feature_service.get_regime(date)

    @staticmethod
    def _parse_timestamp(timestamp: str) -> datetime:
        """Parse an event timestamp into a timezone-aware datetime."""
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed

    @staticmethod
    def _coerce_basis(basis: PriceBasis | str) -> PriceBasis:
        """Normalize string or enum basis inputs into ``PriceBasis``."""
        if isinstance(basis, PriceBasis):
            return basis
        return PriceBasis(str(basis).strip().lower())

    def _resolve_bar_value(
        self,
        bar: PriceBarEvent,
        base_field: str,
        basis: PriceBasis | str,
    ) -> tuple[Decimal, str]:
        """Return a bar value resolved to the requested basis.

        Adjusted-basis lookups are intentionally strict: if a strategy asks for an
        adjusted field and the cached bar does not provide it, fail loudly instead
        of silently degrading to the raw series.
        """
        resolved_basis = self._coerce_basis(basis)
        if resolved_basis == PriceBasis.ADJUSTED:
            adjusted_field = f"{base_field}_adj"
            adjusted_value = getattr(bar, adjusted_field, None)
            if adjusted_value is not None:
                return adjusted_value, adjusted_field
            raise ValueError(
                "Adjusted price requested but cached bar is missing "
                f"`{adjusted_field}` for {bar.symbol} at {bar.timestamp}"
            )

        return getattr(bar, base_field), base_field

    def _get_cached_bars_window(
        self,
        *,
        symbol: str,
        n: int,
        offset: int = 0,
    ) -> Optional[list[PriceBarEvent]]:
        """Return an exact cached bar window, or ``None`` when unavailable."""
        if n <= 0:
            raise ValueError("n must be positive")
        if offset < 0:
            raise ValueError("offset must be non-negative")

        if symbol not in self._bar_cache or len(self._bar_cache[symbol]) == 0:
            logger.debug(
                "strategy.context.get_bars.no_data",
                strategy_id=self._strategy_id,
                symbol=symbol,
                requested=n,
                offset=offset,
            )
            return None

        cached_bars = list(self._bar_cache[symbol])
        end_index = len(cached_bars) - offset
        if end_index <= 0 or end_index < n:
            logger.debug(
                "strategy.context.get_bars.insufficient",
                strategy_id=self._strategy_id,
                symbol=symbol,
                requested=n,
                offset=offset,
                available=len(cached_bars),
            )
            return None

        start_index = end_index - n
        return cached_bars[start_index:end_index]

    def _build_bar_view(self, bar: PriceBarEvent, basis: PriceBasis) -> BarView:
        """Convert a cached ``PriceBarEvent`` into a strategy-facing ``BarView``."""
        open_price, _ = self._resolve_bar_value(bar, "open", basis)
        high_price, _ = self._resolve_bar_value(bar, "high", basis)
        low_price, _ = self._resolve_bar_value(bar, "low", basis)
        close_price, _ = self._resolve_bar_value(bar, "close", basis)
        return BarView(
            symbol=bar.symbol,
            timestamp=self._parse_timestamp(bar.timestamp),
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=bar.volume,
            basis=basis,
        )

    @property
    def strategy_id(self) -> str:
        """Get the strategy identifier."""
        return self._strategy_id

    @property
    def signal_count(self) -> int:
        """Get total number of signals emitted by this strategy."""
        return self._signal_count

    def get_tracked_indicators(self) -> dict[str, Any]:
        """
        Get currently tracked indicators.

        Used by StrategyService to retrieve indicators for event emission.
        Strategies should not call this directly - use track_indicators() instead.

        Returns:
            Dictionary of indicator name to value
        """
        return self._indicators.copy()

    def clear_tracked_indicators(self) -> None:
        """
        Clear tracked indicators.

        Called by StrategyService at the start of each bar to reset indicator state.
        Strategies should not call this directly.
        """
        self._indicators.clear()
        if hasattr(self, "_indicator_placements"):
            self._indicator_placements.clear()
        if hasattr(self, "_indicator_colors"):
            self._indicator_colors.clear()
