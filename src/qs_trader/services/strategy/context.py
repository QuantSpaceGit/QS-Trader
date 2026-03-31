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
from decimal import Decimal
from typing import Any, Optional

import structlog

from qs_trader.events.event_bus import IEventBus
from qs_trader.events.events import IndicatorEvent, PriceBarEvent, SignalEvent
from qs_trader.services.strategy.models import SignalIntention

logger = structlog.get_logger()


class Context:
    """
    Strategy execution context.

    Provides strategies with:
    1. Signal emission (emit_signal) - FULLY IMPLEMENTED
    2. Price queries (get_price) - IMPLEMENTED
    3. Historical bars (get_bars) - IMPLEMENTED

    Position Tracking Philosophy:
    - Strategies track their own positions via on_position_filled() events
    - No get_position() method - event-driven architecture
    - Strategies can maintain self.positions = {} if needed
    - Most strategies don't need position state - RiskManager handles it

    Current Implementation Status:
    - ✅ emit_signal: Fully implemented
    - ✅ get_price: Queries cached bars
    - ✅ get_bars: Returns historical bars from cache
    - ❌ get_position: REMOVED - use event-driven position tracking instead

    Usage in Strategy:
        ```python
        class MyStrategy(Strategy):
            def __init__(self, config):
                self.config = config
                self.positions = {}  # Optional: track if needed

            def on_position_filled(self, event: PositionFilledEvent, context: Context) -> None:
                # Optional: track position changes via events
                self.positions[event.symbol] = event.quantity

            def on_bar(self, event: PriceBarEvent, context: Context) -> None:
                # Get historical bars for indicator calculation
                bars = context.get_bars(event.symbol, n=20)
                if bars is None or len(bars) < 20:
                    return  # Self-managed warmup

                prices = [bar.close for bar in bars]
                sma = sum(prices) / len(prices)

                # Get current price
                current_price = context.get_price(event.symbol)

                if current_price and current_price > sma:
                    # Emit signal (don't check positions - RiskManager does that)
                    context.emit_signal(
                        timestamp=event.timestamp,
                        symbol=event.symbol,
                        intention=SignalIntention.OPEN_LONG,
                        confidence=0.8,
                        price=current_price,
                        reason=f"Price {current_price} above SMA {sma}"
                    )
        ```

    Note:
        - Context is created per strategy instance (not per bar)
        - Bar history cached per symbol with configurable max_bars
        - Strategies can track their own state (indicators, positions, etc.)
        - For position tracking: implement on_position_filled() lifecycle method
    """

    def __init__(
        self,
        strategy_id: str,
        event_bus: IEventBus,
        max_bars: int = 500,
        config: Optional[dict[str, Any]] = None,
        adjustment_mode: str = "split_adjusted",
    ):
        """
        Initialize context for a strategy.

        Args:
            strategy_id: Unique strategy identifier (from config.name)
            event_bus: Event bus for publishing signals
            max_bars: Maximum bars to cache per symbol (default 500)
            config: Optional strategy configuration dict for feature flags
            adjustment_mode: Adjustment mode for price resolution.
                'split_adjusted' = use open/high/low/close fields (default),
                'total_return' = use open_adj/high_adj/low_adj/close_adj fields.
        """
        self._strategy_id = strategy_id
        self._event_bus = event_bus
        self._signal_count = 0  # Track emitted signals for logging
        self._max_bars = max_bars
        self._config = config or {}
        self._adjustment_mode = adjustment_mode  # Bar cache for indicator calculations: {symbol: deque[PriceBarEvent]}
        # Stores bars with backward-adjusted prices (handled by data service)
        # Uses deque with maxlen for automatic windowing
        self._bar_cache: dict[str, deque[PriceBarEvent]] = {}

        # Indicator tracking for automatic event emission: {indicator_name: value}
        # Reset at start of each bar, emitted at end if log_indicators: true
        self._indicators: dict[str, Any] = {}

        logger.debug(
            "strategy.context.initialized",
            strategy_id=strategy_id,
            max_bars=max_bars,
            log_indicators=self._config.get("log_indicators", False),
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
    ) -> SignalEvent:
        """
        Emit a trading signal to the event bus.

        This is the PRIMARY way strategies communicate their trading intent.
        Signals are published to the event bus where other services can consume them.

        Args:
            timestamp: Signal generation time (ISO8601 UTC string, usually bar.timestamp)
            symbol: Instrument symbol to trade
            intention: Trading intention (OPEN_LONG/CLOSE_LONG/OPEN_SHORT/CLOSE_SHORT)
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

        # Generate unique signal_id using UUID
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
            price=price,
            confidence=confidence,
            reason=reason,
            metadata=metadata,
            stop_loss=stop_loss,
            take_profit=take_profit,
            source_service="strategy_service",
            correlation_id=correlation_id,
            # No causation_id - this is the root event in the chain
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

        Bars are already backward-adjusted by data service - no adjustment needed here.
        All services (Strategy, Portfolio, Reporting) work with the same backward-adjusted prices.

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

    def get_price(self, symbol: str) -> Optional[Decimal]:
        """
        Get latest price for a symbol using configured price field.

        Returns the price of the most recent bar according to strategy's price_field configuration.
        Used for signal emission and order sizing.

        Args:
            symbol: Instrument symbol

        Returns:
            Latest price as Decimal, or None if no bars cached

        Example:
            >>> # Context configured with price_field='close' (default)
            >>> price = context.get_price("AAPL")  # Returns close (split-adjusted)
            >>> if price:
            ...     context.emit_signal(
            ...         timestamp=event.timestamp,
            ...         symbol="AAPL",
            ...         intention=SignalIntention.OPEN_LONG,
            ...         price=price,
            ...         confidence=0.80
            ...     )
            >>>
            >>> # Context configured with price_field='close_adj'
            >>> price = context.get_price("AAPL")  # Returns close_adj (total-return)
            >>> # Useful for long-term strategies avoiding dividend gap noise

        Performance:
            O(1) - fast lookup from cache

        Note:
            Adjustment mode is configured at backtest level via BacktestConfig.strategy_adjustment_mode.
            Default is 'split_adjusted' (uses close field, shows dividend drops).
            Use 'total_return' for total-return adjusted (uses close_adj field, smoothed) prices.
            Use resolve_field() to get other OHLC fields in the same adjustment mode.
        """
        if symbol not in self._bar_cache or len(self._bar_cache[symbol]) == 0:
            logger.debug(
                "strategy.context.get_price.no_data",
                strategy_id=self._strategy_id,
                symbol=symbol,
            )
            return None

        latest_bar = self._bar_cache[symbol][-1]
        # Use resolve_field to get correct field name based on adjustment_mode
        field_name = self.resolve_field("close")
        price: Decimal = getattr(latest_bar, field_name)

        logger.debug(
            "strategy.context.get_price.success",
            strategy_id=self._strategy_id,
            symbol=symbol,
            price=str(price),
            adjustment_mode=self._adjustment_mode,
            field=field_name,
            timestamp=latest_bar.timestamp,
        )

        return price

    def get_bars(self, symbol: str, n: int = 1) -> Optional[list[PriceBarEvent]]:
        """
        Get N most recent bars for a symbol.

        Returns historical bars from cache. Used for indicator calculation
        (SMA, RSI, etc.) and pattern detection.

        Args:
            symbol: Instrument symbol
            n: Number of bars to retrieve (default 1 = just last bar)

        Returns:
            List of PriceBarEvent in chronological order (oldest first),
            or None if insufficient bars cached

        Example:
            >>> # Calculate 20-period SMA manually
            >>> bars = context.get_bars("AAPL", n=20)
            >>> if bars and len(bars) == 20:
            ...     prices = [bar.close for bar in bars]  # Can use .close or .close_adj
            ...     sma_20 = sum(prices) / 20
            ...
            ...     current_price = bars[-1].close
            ...     if current_price > sma_20:
            ...         # Price above SMA - bullish signal
            ...         context.emit_signal(...)
            >>>
            >>> # Or use get_price_series() for configured price field
            >>> prices = context.get_price_series("AAPL", n=20)
            >>> if prices and len(prices) == 20:
            ...     sma_20 = sum(prices) / 20
            >>>
            >>> # Get last 50 bars for pattern detection
            >>> bars = context.get_bars("AAPL", n=50)
            >>> if bars and len(bars) >= 50:
            ...     highs = [bar.high for bar in bars]
            ...     resistance = max(highs[-20:])  # 20-bar resistance

        Performance:
            O(n) - efficient slice from deque

        Note:
            - Returns bars in chronological order (oldest first, newest last)
            - Returns None if fewer than n bars available
            - Maximum bars cached per symbol: self._max_bars (default 500)
            - For longer histories, increase max_bars in Context initialization
            - Bars already backward-adjusted by data service (no gaps in price history)
        """
        if symbol not in self._bar_cache or len(self._bar_cache[symbol]) == 0:
            logger.debug(
                "strategy.context.get_bars.no_data",
                strategy_id=self._strategy_id,
                symbol=symbol,
                requested=n,
            )
            return None

        cached_count = len(self._bar_cache[symbol])

        # Return None if insufficient bars
        if cached_count < n:
            logger.debug(
                "strategy.context.get_bars.insufficient",
                strategy_id=self._strategy_id,
                symbol=symbol,
                requested=n,
                available=cached_count,
            )
            return None

        # Get last n bars and convert deque to list
        bars = list(self._bar_cache[symbol])[-n:]

        logger.debug(
            "strategy.context.get_bars.success",
            strategy_id=self._strategy_id,
            symbol=symbol,
            requested=n,
            returned=len(bars),
        )

        return bars

    def get_price_series(self, symbol: str, n: int) -> Optional[list[Decimal]]:
        """
        Get N most recent prices for a symbol using configured price field.

        Convenience method that extracts prices from bars according to strategy's
        price_field configuration. Useful for indicator calculations.

        Args:
            symbol: Instrument symbol
            n: Number of prices to retrieve

        Returns:
            List of Decimal prices in chronological order (oldest first),
            or None if insufficient bars cached

        Example:
            >>> # Calculate 20-period SMA using configured price field
            >>> prices = context.get_price_series("AAPL", n=20)
            >>> if prices and len(prices) == 20:
            ...     sma_20 = sum(prices) / 20
            ...     if prices[-1] > sma_20:
            ...         context.emit_signal(...)  # Price above SMA
            >>>
            >>> # Equivalent to manual extraction
            >>> bars = context.get_bars("AAPL", n=20)
            >>> prices = [getattr(bar, context._price_field) for bar in bars]

        Performance:
            O(n) - extracts prices from cached bars

        Note:
            Uses same adjustment_mode as get_price() for consistency.
            If adjustment_mode='split_adjusted', returns close field values.
            If adjustment_mode='total_return', returns close_adj field values.
            Use resolve_field() to get other OHLC fields (open, high, low).
        """
        bars = self.get_bars(symbol, n)
        if bars is None:
            return None

        # Use resolve_field to get correct field name based on adjustment_mode
        field_name = self.resolve_field("close")
        prices = [getattr(bar, field_name) for bar in bars]

        logger.debug(
            "strategy.context.get_price_series.success",
            strategy_id=self._strategy_id,
            symbol=symbol,
            requested=n,
            returned=len(prices),
            adjustment_mode=self._adjustment_mode,
            field=field_name,
        )

        return prices

    def resolve_field(self, base_field: str) -> str:
        """Resolve base field name to actual field based on adjustment mode.

        Allows strategies to request any OHLC field and get the correctly adjusted version.

        Args:
            base_field: Base field name ('open', 'high', 'low', 'close', 'vwap')

        Returns:
            Adjusted field name based on adjustment_mode.
            For split_adjusted: returns base_field as-is
            For total_return: returns base_field + '_adj'

        Example:
            >>> context = Context(strategy_id="test", event_bus=bus, price_field="split_adjusted")
            >>> context.resolve_field("open")  # Returns "open"
            >>> context.resolve_field("close")  # Returns "close"
            >>>
            >>> context = Context(strategy_id="test", event_bus=bus, price_field="total_return")
            >>> context.resolve_field("open")  # Returns "open_adj"
            >>> context.resolve_field("close")  # Returns "close_adj"
        """
        if self._adjustment_mode == "split_adjusted":
            return base_field
        elif self._adjustment_mode == "total_return":
            # For total_return mode, append _adj to base field
            return f"{base_field}_adj"
        else:
            raise ValueError(f"Unknown adjustment_mode: {self._adjustment_mode}")

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
