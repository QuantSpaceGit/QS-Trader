"""Pure EventStore → audit-row collection helpers.

This module intentionally contains no database dependencies so reporting and
audit-export paths can reuse the same decision-chain aggregation semantics
without importing SQLAlchemy-backed writer implementations.
"""

from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any

from qs_trader.events.event_store import EventStore
from qs_trader.events.events import (
    FeatureBarEvent,
    FillEvent,
    IndicatorEvent,
    OrderEvent,
    PriceBarEvent,
    SignalEvent,
    TradeEvent,
)

__all__ = ["collect_run_events"]


def _to_float(value: Decimal | float | int | None) -> float | None:
    """Convert Decimal-like values to floats for serialized row payloads."""
    if value is None:
        return None
    return float(value)


def _to_json(data: dict | str | None) -> str | None:
    """Convert dict payloads into JSON strings for downstream consumers."""
    if data is None:
        return None
    if isinstance(data, str):
        return data
    return json.dumps(data)


def _normalize_event_timestamp(timestamp: str) -> str:
    """Normalize event timestamps to a consistent ISO format for grouping."""
    return timestamp.replace("Z", "+00:00")


def _parse_event_timestamp(timestamp: str) -> datetime:
    """Parse an event timestamp into a timezone-aware datetime."""
    return datetime.fromisoformat(_normalize_event_timestamp(timestamp))


def _coerce_numeric_json_value(value: Any) -> float | None:
    """Convert supported numeric-like values into finite floats."""
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, Decimal):
        if not value.is_finite():
            return None
        return float(value)
    if isinstance(value, (int, float)):
        numeric = float(value)
        return numeric if math.isfinite(numeric) else None
    return None


def _filter_numeric_payload(payload: dict[str, Any]) -> dict[str, float] | None:
    """Keep only finite numeric values from a flexible event payload."""
    filtered: dict[str, float] = {}
    for key, value in payload.items():
        numeric_value = _coerce_numeric_json_value(value)
        if numeric_value is not None:
            filtered[str(key)] = numeric_value
    return filtered or None


def _join_unique_values(values: list[str]) -> str | None:
    """Join ordered unique strings for deterministic text persistence."""
    ordered: list[str] = []
    seen: set[str] = set()

    for value in values:
        normalized = value.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)

    return ",".join(ordered) if ordered else None


def collect_run_events(
    experiment_id: str,
    run_id: str,
    event_store: EventStore | None,
) -> list[dict[str, Any]]:
    """Collect per-bar decision-chain rows from the in-memory event store.

    Signal aggregation is intentionally last-wins at the ``(timestamp,
    symbol)`` grain: if multiple ``SignalEvent`` objects share the same bar
    and symbol, the final event encountered in occurred-at order overwrites
    earlier ones. Preserving multiple same-bar signals per strategy would
    require widening the persistence grain (for example by including
    ``strategy_id`` in the primary key), which is outside Phase 1 / Phase 2.
    """
    if event_store is None:
        return []

    bar_events: list[PriceBarEvent] = [
        event for event in event_store.get_by_type("bar") if isinstance(event, PriceBarEvent)
    ]
    feature_events: list[FeatureBarEvent] = [
        event for event in event_store.get_by_type("feature_bar") if isinstance(event, FeatureBarEvent)
    ]
    indicator_events: list[IndicatorEvent] = [
        event for event in event_store.get_by_type("indicator") if isinstance(event, IndicatorEvent)
    ]
    signal_events: list[SignalEvent] = [
        event for event in event_store.get_by_type("signal") if isinstance(event, SignalEvent)
    ]
    order_events: list[OrderEvent] = [
        event for event in event_store.get_by_type("order") if isinstance(event, OrderEvent)
    ]
    fill_events: list[FillEvent] = [event for event in event_store.get_by_type("fill") if isinstance(event, FillEvent)]
    trade_events: list[TradeEvent] = [
        event for event in event_store.get_by_type("trade") if isinstance(event, TradeEvent)
    ]

    if not any((bar_events, feature_events, indicator_events, signal_events, order_events, fill_events, trade_events)):
        return []

    def event_key(timestamp: str, symbol: str) -> tuple[str, str]:
        return (_normalize_event_timestamp(timestamp), symbol)

    all_keys: set[tuple[str, str]] = set()
    signals_by_key: dict[tuple[str, str], SignalEvent] = {}
    orders_by_key: dict[tuple[str, str], list[OrderEvent]] = defaultdict(list)
    fills_by_key: dict[tuple[str, str], list[FillEvent]] = defaultdict(list)
    trade_by_key: dict[tuple[str, str], TradeEvent] = {}
    indicators_by_key: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    features_by_key: dict[tuple[str, str], dict[str, float]] = defaultdict(dict)
    strategy_ids_by_key: dict[tuple[str, str], list[str]] = defaultdict(list)
    strategy_ids_by_symbol: dict[str, list[str]] = defaultdict(list)
    run_strategy_ids: list[str] = []
    fill_to_trade_id: dict[str, str] = {}

    def record_strategy_id(strategy_id: str | None, key: tuple[str, str] | None = None) -> None:
        if not strategy_id:
            return
        if strategy_id not in run_strategy_ids:
            run_strategy_ids.append(strategy_id)
        if key is not None:
            if strategy_id not in strategy_ids_by_key[key]:
                strategy_ids_by_key[key].append(strategy_id)
            if strategy_id not in strategy_ids_by_symbol[key[1]]:
                strategy_ids_by_symbol[key[1]].append(strategy_id)

    for event in bar_events:
        all_keys.add(event_key(event.timestamp, event.symbol))

    for feature_event in feature_events:
        key = event_key(feature_event.timestamp, feature_event.symbol)
        all_keys.add(key)
        numeric_features = _filter_numeric_payload(feature_event.features)
        if numeric_features is not None:
            features_by_key[key].update(numeric_features)

    for indicator_event in indicator_events:
        key = event_key(indicator_event.timestamp, indicator_event.symbol)
        all_keys.add(key)
        record_strategy_id(indicator_event.strategy_id, key)
        numeric_indicators = _filter_numeric_payload(indicator_event.indicators)
        if numeric_indicators is not None:
            indicators_by_key[key].update(numeric_indicators)

    for signal_event in signal_events:
        key = event_key(signal_event.timestamp, signal_event.symbol)
        all_keys.add(key)
        record_strategy_id(signal_event.strategy_id, key)
        signals_by_key[key] = signal_event

    for order_event in order_events:
        key = event_key(order_event.timestamp, order_event.symbol)
        all_keys.add(key)
        record_strategy_id(order_event.source_strategy_id, key)
        orders_by_key[key].append(order_event)

    for fill_event in fill_events:
        key = event_key(fill_event.timestamp, fill_event.symbol)
        all_keys.add(key)
        record_strategy_id(fill_event.strategy_id, key)
        fills_by_key[key].append(fill_event)

    for trade_event_item in trade_events:
        key = event_key(trade_event_item.timestamp, trade_event_item.symbol)
        all_keys.add(key)
        record_strategy_id(trade_event_item.strategy_id, key)
        trade_by_key[key] = trade_event_item
        for fill_id in trade_event_item.fills:
            fill_to_trade_id[fill_id] = trade_event_item.trade_id

    rows: list[dict[str, Any]] = []
    for timestamp_key, symbol in sorted(all_keys):
        key = (timestamp_key, symbol)
        signal = signals_by_key.get(key)
        orders = orders_by_key.get(key, [])
        fills = fills_by_key.get(key, [])
        trade_event: TradeEvent | None = trade_by_key.get(key)

        if trade_event is None:
            for fill in fills:
                trade_id = fill_to_trade_id.get(fill.fill_id)
                if trade_id is None:
                    continue
                for candidate_event in trade_events:
                    if candidate_event.trade_id == trade_id:
                        trade_event = candidate_event
                        break
                if trade_event is not None:
                    break

        strategy_id = (
            _join_unique_values(strategy_ids_by_key.get(key, []))
            or _join_unique_values(strategy_ids_by_symbol.get(symbol, []))
            or _join_unique_values(run_strategy_ids)
            or "unknown"
        )

        order_qty: Decimal | None = None
        if orders:
            order_qty = sum((order.quantity for order in orders), start=Decimal("0"))

        fill_qty: Decimal | None = None
        fill_price: float | None = None
        fill_slippage_bps: float | None = None
        commission: float | None = None
        if fills:
            fill_qty = sum((fill.filled_quantity for fill in fills), start=Decimal("0"))
            if fill_qty > 0:
                fill_value = sum(
                    (fill.filled_quantity * fill.fill_price for fill in fills),
                    start=Decimal("0"),
                )
                fill_price = float(fill_value / fill_qty)

            total_commission = sum((fill.commission for fill in fills), start=Decimal("0"))
            commission = float(total_commission)

            weighted_slippage_total = Decimal("0")
            weighted_slippage_qty = Decimal("0")
            for fill in fills:
                if fill.slippage_bps is None:
                    continue
                weighted_slippage_total += Decimal(fill.slippage_bps) * fill.filled_quantity
                weighted_slippage_qty += fill.filled_quantity

            if weighted_slippage_qty > 0:
                fill_slippage_bps = float(weighted_slippage_total / weighted_slippage_qty)

        rows.append(
            {
                "experiment_id": experiment_id,
                "run_id": run_id,
                "timestamp": _parse_event_timestamp(timestamp_key),
                "symbol": symbol,
                "strategy_id": strategy_id,
                "signal_intention": signal.intention if signal is not None else None,
                "signal_price": _to_float(signal.price) if signal is not None else None,
                "signal_confidence": _to_float(signal.confidence) if signal is not None else None,
                "signal_reason": signal.reason if signal is not None else None,
                "order_side": _join_unique_values([order.side.upper() for order in orders]),
                "order_type": _join_unique_values([order.order_type.upper() for order in orders]),
                "order_qty": int(order_qty) if order_qty is not None else None,
                "fill_qty": int(fill_qty) if fill_qty is not None else None,
                "fill_price": fill_price,
                "fill_slippage_bps": fill_slippage_bps,
                "commission": commission,
                "trade_id": trade_event.trade_id if trade_event is not None else None,
                "trade_status": trade_event.status.upper() if trade_event is not None else None,
                "trade_side": trade_event.side.upper() if trade_event and trade_event.side else None,
                "trade_entry_price": _to_float(trade_event.entry_price) if trade_event is not None else None,
                "trade_exit_price": _to_float(trade_event.exit_price) if trade_event is not None else None,
                "trade_realized_pnl": _to_float(trade_event.realized_pnl) if trade_event is not None else None,
                "indicators_json": _to_json(indicators_by_key.get(key) or None),
                "features_json": _to_json(features_by_key.get(key) or None),
            }
        )

    return rows
