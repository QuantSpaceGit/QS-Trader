"""Pure EventStore → observability-ledger row collection helpers.

This module contains no database dependencies. The PostgreSQL writer consumes
the row dicts returned by :func:`collect_run_observability_bars` and inserts
them into ``run_observability_bars`` (schema owned by QS-Research alembic).

See: ``QS-Infra/docs/audit-export-v3-observability-bars.md``.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from qs_trader.events.event_store import EventStore
from qs_trader.events.events import IndicatorEvent, RuntimeFeaturesEvent

__all__ = [
    "ObservabilityCollectorError",
    "collect_run_observability_bars",
]

# Hard-coded schema version for rows emitted into ``run_observability_bars``.
# Bumped when the row contract itself changes (distinct from the audit-export
# schema version that lives on the Research side).
_OBSERVABILITY_SCHEMA_VERSION: int = 1


class ObservabilityCollectorError(ValueError):
    """Raised when an observability row cannot be built safely.

    Typical causes: indicator values that cannot be JSON-serialized, or other
    fail-closed conditions detected while normalizing the emit-time payload.
    """


def _json_default(value: Any) -> Any:
    if isinstance(value, Decimal):
        return format(value, "f")
    raise TypeError(f"Type {type(value)!r} is not JSON serializable.")


def _normalize_indicator_value(value: Any) -> Any:
    """Normalize a single indicator value for JSONB storage.

    - ``Decimal`` is preserved as a string (matches the audit-export projection
      normalizer, keeping Research-side column values identical to wire form).
    - ``bool`` / ``int`` / ``float`` / ``str`` / ``None`` pass through.
    - Containers are JSON-serialized so CSV stays scalar-friendly.
    - Other exotic types raise :class:`ObservabilityCollectorError`.

    Asymmetry note (intentional): QS-Research mirrors this helper in
    ``research_api.backtest.audit_export_projection._normalize_indicator_value``
    for CSV projection, but that variant fails *open* by ``str()``-ing exotic
    types (CSV is human-readable; lossy stringification is preferable to
    aborting an audit-export build). This collector fails *closed* because we
    are about to commit the row to JSONB and silent ``str()`` would corrupt
    the wire contract auditors rely on.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float, str)):
        return value
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (list, tuple, dict, set, frozenset)):
        serializable: Any = list(value) if isinstance(value, (set, frozenset)) else value
        try:
            return json.loads(json.dumps(serializable, sort_keys=True, default=_json_default))
        except TypeError as exc:
            raise ObservabilityCollectorError(
                f"Indicator value of type {type(value).__name__} is not JSON-serializable: {exc}"
            ) from exc
    raise ObservabilityCollectorError(f"Unsupported indicator value type: {type(value).__name__}")


def _serialize_indicators(indicators: dict[str, Any]) -> str:
    normalized: dict[str, Any] = {}
    for key, value in indicators.items():
        normalized[str(key)] = _normalize_indicator_value(value)
    try:
        return json.dumps(normalized, sort_keys=True, default=_json_default)
    except TypeError as exc:
        raise ObservabilityCollectorError(f"Failed to serialize indicators payload: {exc}") from exc


def _serialize_runtime_features(features: dict[str, Any]) -> str:
    """Serialize a runtime-features payload for JSONB storage.

    Shares the same normalization rules as :func:`_serialize_indicators` so
    both JSONB columns use the identical wire contract on the Research
    projection side. ``Decimal`` is stringified to preserve exactness;
    containers are JSON-encoded with sorted keys for deterministic rows.
    """
    normalized: dict[str, Any] = {}
    for key, value in features.items():
        normalized[str(key)] = _normalize_indicator_value(value)
    try:
        return json.dumps(normalized, sort_keys=True, default=_json_default)
    except TypeError as exc:
        raise ObservabilityCollectorError(f"Failed to serialize runtime_features payload: {exc}") from exc


def collect_run_observability_bars(
    experiment_id: str,
    run_id: str,
    event_store: EventStore | None,
) -> list[dict[str, Any]]:
    """Collect canonical per-bar observability rows from the event store.

    Iterates both :class:`IndicatorEvent` and :class:`RuntimeFeaturesEvent`
    entries and merges them by ``(strategy_id, symbol, timestamp)`` into a
    single row per bar. A row is emitted whenever at least one of
    ``indicators`` or ``runtime_features`` is non-empty; bars with neither
    stream contribute nothing. Non-observability events are ignored.
    Rows are sorted by ``(strategy_id, symbol, bar_timestamp)`` for
    deterministic persistence.

    Args:
        experiment_id: Expected experiment identifier for the run.
        run_id: Expected run identifier for the run.
        event_store: Event store containing emitted backtest events.

    Returns:
        Parameterized row dictionaries ready for ``run_observability_bars``
        insertion. ``indicators_json`` and ``runtime_features_json`` are
        JSON strings (or ``None`` when absent); the writer casts to JSONB
        via ``CAST(:indicators_json AS jsonb)`` /
        ``CAST(:runtime_features_json AS jsonb)``.

    Raises:
        ObservabilityCollectorError: When a value cannot be serialized.
    """
    if event_store is None:
        return []

    # Merge indicator + runtime-features streams by (strategy, symbol, ts).
    merged: dict[tuple[str, str, str], dict[str, Any]] = {}
    for event in event_store.get_all():
        if isinstance(event, IndicatorEvent):
            if not event.indicators:
                continue
            key = (event.strategy_id, event.symbol, event.timestamp)
            slot = merged.setdefault(
                key,
                {
                    "experiment_id": experiment_id,
                    "run_id": run_id,
                    "strategy_id": event.strategy_id,
                    "symbol": event.symbol,
                    "bar_timestamp": event.timestamp,
                    "schema_version": _OBSERVABILITY_SCHEMA_VERSION,
                    "indicators_json": None,
                    "runtime_features_json": None,
                },
            )
            slot["indicators_json"] = _serialize_indicators(event.indicators)
        elif isinstance(event, RuntimeFeaturesEvent):
            if not event.runtime_features:
                continue
            key = (event.strategy_id, event.symbol, event.timestamp)
            slot = merged.setdefault(
                key,
                {
                    "experiment_id": experiment_id,
                    "run_id": run_id,
                    "strategy_id": event.strategy_id,
                    "symbol": event.symbol,
                    "bar_timestamp": event.timestamp,
                    "schema_version": _OBSERVABILITY_SCHEMA_VERSION,
                    "indicators_json": None,
                    "runtime_features_json": None,
                },
            )
            slot["runtime_features_json"] = _serialize_runtime_features(event.runtime_features)

    # ``run_observability_bars.indicators_json`` is NOT NULL on the Postgres
    # side (see alembic 009). For rows sourced only from a
    # RuntimeFeaturesEvent we materialize an empty ``{}`` indicator payload
    # so the INSERT still satisfies the schema contract.
    rows: list[dict[str, Any]] = []
    for slot in merged.values():
        if slot["indicators_json"] is None:
            slot["indicators_json"] = "{}"
        rows.append(slot)

    rows.sort(
        key=lambda row: (
            row["strategy_id"],
            row["symbol"],
            row["bar_timestamp"],
        )
    )
    return rows
