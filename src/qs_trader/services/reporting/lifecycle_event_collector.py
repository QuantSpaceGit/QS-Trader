"""Pure EventStore → lifecycle-ledger row collection helpers.

This module intentionally contains no database dependencies so PostgreSQL
persistence and future audit/projection paths can reuse the same canonical
lifecycle-ledger extraction logic.
"""

from __future__ import annotations

import json
from typing import Any

from qs_trader.events.event_store import EventStore
from qs_trader.events.lifecycle_events import LifecycleBaseEvent, LifecycleValidatedEvent

__all__ = ["collect_run_lifecycle_events"]


def _to_json(data: dict[str, Any]) -> str:
    """Serialize lifecycle payloads for JSONB insertion."""
    return json.dumps(data, separators=(",", ":"), sort_keys=True)


def collect_run_lifecycle_events(
    experiment_id: str,
    run_id: str,
    event_store: EventStore | None,
) -> list[dict[str, Any]]:
    """Collect canonical lifecycle ledger rows from the event store.

    Args:
        experiment_id: Expected experiment identifier for the run.
        run_id: Expected run identifier for the run.
        event_store: Event store containing emitted backtest events.

    Returns:
        Parameterized row dictionaries ready for ``run_lifecycle_events``
        insertion. Non-lifecycle events are ignored.
    """
    if event_store is None:
        return []

    rows: list[dict[str, Any]] = []

    for event in event_store.get_all():
        if not isinstance(event, LifecycleBaseEvent):
            continue

        if event.experiment_id != experiment_id or event.run_id != run_id:
            continue

        if isinstance(event, LifecycleValidatedEvent):
            lifecycle_type = event.get_lifecycle_type()
            price_basis = event.get_lifecycle_price_basis()
        else:
            lifecycle_type = event.event_type
            price_basis = None

        rows.append(
            {
                "event_id": event.event_id,
                "schema_version": event.event_version,
                "experiment_id": event.experiment_id,
                "run_id": event.run_id,
                "strategy_id": getattr(event, "strategy_id", None),
                "symbol": getattr(event, "symbol", None),
                "lifecycle_family": event.event_type,
                "lifecycle_type": str(lifecycle_type),
                "event_timestamp": event.occurred_at,
                "correlation_id": event.correlation_id,
                "causation_id": event.causation_id,
                "price_basis": price_basis,
                "payload_json": _to_json(event.model_dump(mode="json")),
            }
        )

    rows.sort(key=lambda row: row["event_timestamp"])
    return rows
