"""Pure EventStore → runtime bar snapshot row collection helpers.

This module contains no database dependencies. The PostgreSQL writer consumes
the row dicts returned by :func:`collect_run_bar_snapshots` and inserts them
into ``run_bar_snapshots`` (schema owned by QS-Research alembic).
"""

from __future__ import annotations

from typing import Any

from qs_trader.events.event_store import EventStore
from qs_trader.events.events import PriceBarEvent

__all__ = ["collect_run_bar_snapshots"]


def _resolve_snapshot_volumes(event: PriceBarEvent) -> tuple[int | None, int | None]:
    """Resolve dual-basis volume columns for snapshot persistence.

    When an adapter emits explicit ``volume_raw`` / ``volume_adj`` fields, those
    values are persisted verbatim. Legacy producers that only emit the single
    strategy-facing ``volume`` field are treated as raw-only for Phase 1 so the
    snapshot ledger still captures the runtime truth without inventing a second
    series.
    """

    if event.volume_raw is not None or event.volume_adj is not None:
        return event.volume_raw, event.volume_adj
    return event.volume, None


def collect_run_bar_snapshots(
    experiment_id: str,
    run_id: str,
    event_store: EventStore | None,
) -> list[dict[str, Any]]:
    """Collect canonical runtime bar snapshot rows from the event store.

    Iterates :class:`PriceBarEvent` entries and normalizes them into one row per
    ``(symbol, timestamp)`` suitable for ``run_bar_snapshots`` persistence.

    Args:
        experiment_id: Expected experiment identifier for the run.
        run_id: Expected run identifier for the run.
        event_store: Event store containing emitted backtest events.

    Returns:
        Parameterized row dictionaries ready for ``run_bar_snapshots``
        insertion, sorted by ``(symbol, bar_timestamp)`` for deterministic
        persistence.
    """
    if event_store is None:
        return []

    rows: list[dict[str, Any]] = []
    for event in event_store.get_by_type("bar"):
        if not isinstance(event, PriceBarEvent):
            continue
        volume_raw, volume_adj = _resolve_snapshot_volumes(event)
        rows.append(
            {
                "experiment_id": experiment_id,
                "run_id": run_id,
                "symbol": event.symbol,
                "bar_timestamp": event.timestamp,
                "timestamp_local": event.timestamp_local,
                "timezone": event.timezone,
                "source_name": event.source,
                "price_currency": event.price_currency,
                "price_scale": event.price_scale,
                "open_raw": event.open,
                "high_raw": event.high,
                "low_raw": event.low,
                "close_raw": event.close,
                "open_adj": event.open_adj,
                "high_adj": event.high_adj,
                "low_adj": event.low_adj,
                "close_adj": event.close_adj,
                "volume_raw": volume_raw,
                "volume_adj": volume_adj,
            }
        )

    rows.sort(key=lambda row: (row["symbol"], row["bar_timestamp"]))
    return rows
