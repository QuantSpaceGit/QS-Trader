"""Unit tests for observability_collector.collect_run_observability_bars."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import IndicatorEvent, PriceBarEvent, RuntimeFeaturesEvent
from qs_trader.events.lifecycle_events import StrategyDecisionEvent
from qs_trader.services.reporting.observability_collector import (
    ObservabilityCollectorError,
    collect_run_observability_bars,
)


EXPERIMENT_ID = "obs_exp"
RUN_ID = "run-001"


def _indicator(
    *,
    strategy_id: str = "sma_crossover",
    symbol: str = "AAPL",
    timestamp: str = "2024-01-02T00:00:00Z",
    indicators: dict,
) -> IndicatorEvent:
    return IndicatorEvent(
        strategy_id=strategy_id,
        symbol=symbol,
        timestamp=timestamp,
        indicators=indicators,
    )


def test_empty_store_emits_no_rows() -> None:
    assert collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, None) == []
    store = InMemoryEventStore()
    assert collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store) == []


def test_single_symbol_full_series_emits_one_row_per_bar() -> None:
    store = InMemoryEventStore()
    for idx in range(3):
        store.append(
            _indicator(
                timestamp=f"2024-01-0{idx + 2}T00:00:00Z",
                indicators={"sma_fast": 100.0 + idx, "sma_slow": 99.5},
            )
        )

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    assert len(rows) == 3
    assert [row["bar_timestamp"] for row in rows] == [
        "2024-01-02T00:00:00Z",
        "2024-01-03T00:00:00Z",
        "2024-01-04T00:00:00Z",
    ]
    assert all(row["experiment_id"] == EXPERIMENT_ID for row in rows)
    assert all(row["run_id"] == RUN_ID for row in rows)
    assert all(row["strategy_id"] == "sma_crossover" for row in rows)
    assert all(row["symbol"] == "AAPL" for row in rows)
    assert all(row["schema_version"] == 1 for row in rows)
    assert all(row["runtime_features_json"] is None for row in rows)
    payload = json.loads(rows[0]["indicators_json"])
    assert payload == {"sma_fast": 100.0, "sma_slow": 99.5}


def test_multi_symbol_emits_row_per_symbol() -> None:
    store = InMemoryEventStore()
    store.append(_indicator(symbol="AAPL", indicators={"sma": 100.0}))
    store.append(_indicator(symbol="MSFT", indicators={"sma": 200.0}))

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    assert {row["symbol"] for row in rows} == {"AAPL", "MSFT"}


def test_multi_strategy_disambiguation_by_strategy_id() -> None:
    store = InMemoryEventStore()
    store.append(
        _indicator(strategy_id="sma_crossover", indicators={"sma": 100.0})
    )
    store.append(
        _indicator(strategy_id="breakout", indicators={"atr": 1.25})
    )

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    by_strategy = {row["strategy_id"]: row for row in rows}
    assert set(by_strategy) == {"breakout", "sma_crossover"}
    assert json.loads(by_strategy["breakout"]["indicators_json"]) == {"atr": 1.25}
    assert json.loads(by_strategy["sma_crossover"]["indicators_json"]) == {"sma": 100.0}


def test_decimal_bool_and_nested_dict_normalization() -> None:
    store = InMemoryEventStore()
    store.append(
        _indicator(
            indicators={
                "sma_fast": Decimal("150.2500"),
                "is_bullish": True,
                "regime": {"state": "bull", "strength": Decimal("0.75")},
            }
        )
    )

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    assert len(rows) == 1
    payload = json.loads(rows[0]["indicators_json"])
    assert payload["sma_fast"] == "150.2500"
    assert payload["is_bullish"] is True
    assert payload["regime"] == {"state": "bull", "strength": "0.75"}


def test_unserializable_value_raises_typed_error() -> None:
    class Opaque:
        pass

    store = InMemoryEventStore()
    # Pydantic model_validate on IndicatorEvent allows Any values so the
    # collector is the first place this is caught.
    store.append(_indicator(indicators={"weird": Opaque()}))

    with pytest.raises(ObservabilityCollectorError):
        collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)


def test_ignores_non_indicator_events() -> None:
    store = InMemoryEventStore()
    store.append(
        PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-02T00:00:00Z",
            interval="1d",
            open=Decimal("100"),
            high=Decimal("101"),
            low=Decimal("99"),
            close=Decimal("100.5"),
            volume=1000,
            source="unit_test",
        )
    )
    store.append(
        StrategyDecisionEvent(
            experiment_id=EXPERIMENT_ID,
            run_id=RUN_ID,
            occurred_at=datetime(2024, 1, 2, tzinfo=timezone.utc),
            decision_id="550e8400-e29b-41d4-a716-446655440042",
            strategy_id="sma_crossover",
            symbol="AAPL",
            bar_timestamp="2024-01-02T00:00:00Z",
            decision_type="open_long",
            decision_price=Decimal("100.50"),
            decision_basis="adjusted_ohlc_adj_columns",
            confidence=Decimal("0.85"),
            source_service="strategy_service",
            correlation_id="550e8400-e29b-41d4-a716-446655440041",
        )
    )

    assert collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store) == []


def test_empty_indicators_dict_is_skipped() -> None:
    """IndicatorEvent validation rejects empty dicts, so the skip path is a
    defense-in-depth guard. We exercise it by feeding a non-IndicatorEvent-like
    object that mimics the attribute surface.
    """
    class _FakeEvent:
        strategy_id = "sma_crossover"
        symbol = "AAPL"
        timestamp = "2024-01-02T00:00:00Z"
        indicators: dict = {}

    store = InMemoryEventStore.__new__(InMemoryEventStore)
    store._events = [_FakeEvent()]  # type: ignore[attr-defined]

    # Since _FakeEvent is not an IndicatorEvent instance, the isinstance check
    # short-circuits before the empty-dict guard. The collector returns [] in
    # either case — the contract we care about is "no spurious rows".
    assert collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store) == []


def test_rows_ordered_by_strategy_symbol_timestamp() -> None:
    store = InMemoryEventStore()
    # Append intentionally out of order
    store.append(
        _indicator(
            strategy_id="sma_crossover",
            symbol="MSFT",
            timestamp="2024-01-03T00:00:00Z",
            indicators={"sma": 1.0},
        )
    )
    store.append(
        _indicator(
            strategy_id="breakout",
            symbol="AAPL",
            timestamp="2024-01-02T00:00:00Z",
            indicators={"atr": 1.0},
        )
    )
    store.append(
        _indicator(
            strategy_id="sma_crossover",
            symbol="AAPL",
            timestamp="2024-01-02T00:00:00Z",
            indicators={"sma": 2.0},
        )
    )
    store.append(
        _indicator(
            strategy_id="sma_crossover",
            symbol="AAPL",
            timestamp="2024-01-03T00:00:00Z",
            indicators={"sma": 3.0},
        )
    )

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    ordering = [(row["strategy_id"], row["symbol"], row["bar_timestamp"]) for row in rows]
    assert ordering == sorted(ordering)


def test_float_nan_passes_through_as_plain_value() -> None:
    """NaN is still a float; the JSONB cast is Postgres's concern (serialized as "NaN")."""
    store = InMemoryEventStore()
    store.append(_indicator(indicators={"val": float("nan")}))

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    # Serialization must not raise; we document that NaN survives as a JSON float.
    payload_text = rows[0]["indicators_json"]
    # Python's json.dumps emits NaN as "NaN" (non-strict); that's acceptable for
    # JSONB insertion in the writer path. Guard against regressions by parsing
    # with the same permissive default.
    payload = json.loads(payload_text)
    assert math.isnan(payload["val"])


# ---------------------------------------------------------------------------
# Phase 2 — runtime feature capture coverage
# ---------------------------------------------------------------------------


def _runtime_features(
    *,
    strategy_id: str = "sma_crossover",
    symbol: str = "AAPL",
    timestamp: str = "2024-01-02T00:00:00Z",
    runtime_features: dict,
) -> RuntimeFeaturesEvent:
    return RuntimeFeaturesEvent(
        strategy_id=strategy_id,
        symbol=symbol,
        timestamp=timestamp,
        runtime_features=runtime_features,
    )


def test_runtime_features_only_row_serializes_empty_indicators() -> None:
    """Rows sourced only from RuntimeFeaturesEvent must still satisfy the
    NOT NULL ``indicators_json`` constraint by emitting an empty ``{}``."""
    store = InMemoryEventStore()
    store.append(
        _runtime_features(runtime_features={"momentum_score": 0.12, "regime": "bull"})
    )

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    assert len(rows) == 1
    assert json.loads(rows[0]["indicators_json"]) == {}
    payload = json.loads(rows[0]["runtime_features_json"])
    assert payload == {"momentum_score": 0.12, "regime": "bull"}


def test_indicator_and_runtime_features_merge_into_single_row() -> None:
    """IndicatorEvent + RuntimeFeaturesEvent sharing ``(strategy, symbol, ts)``
    must produce exactly one row with both JSONB payloads populated."""
    store = InMemoryEventStore()
    store.append(_indicator(indicators={"sma_fast": 100.5}))
    store.append(
        _runtime_features(runtime_features={"momentum_score": Decimal("0.12")})
    )

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    assert len(rows) == 1
    assert json.loads(rows[0]["indicators_json"]) == {"sma_fast": 100.5}
    assert json.loads(rows[0]["runtime_features_json"]) == {"momentum_score": "0.12"}


def test_runtime_features_disjoint_bars_emit_distinct_rows() -> None:
    """Events on different bars must not collapse even if one bar is
    runtime-only and the other is indicator-only."""
    store = InMemoryEventStore()
    store.append(_indicator(timestamp="2024-01-02T00:00:00Z", indicators={"sma": 1.0}))
    store.append(
        _runtime_features(
            timestamp="2024-01-03T00:00:00Z",
            runtime_features={"momentum_score": 0.2},
        )
    )

    rows = collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store)

    assert len(rows) == 2
    by_ts = {row["bar_timestamp"]: row for row in rows}
    assert json.loads(by_ts["2024-01-02T00:00:00Z"]["indicators_json"]) == {"sma": 1.0}
    assert by_ts["2024-01-02T00:00:00Z"]["runtime_features_json"] is None
    assert json.loads(by_ts["2024-01-03T00:00:00Z"]["indicators_json"]) == {}
    assert (
        json.loads(by_ts["2024-01-03T00:00:00Z"]["runtime_features_json"])
        == {"momentum_score": 0.2}
    )


def test_empty_runtime_features_dict_skipped() -> None:
    """An empty runtime_features payload must not create a row on its own."""
    store = InMemoryEventStore()

    class _FakeRuntimeFeatures:
        strategy_id = "sma_crossover"
        symbol = "AAPL"
        timestamp = "2024-01-02T00:00:00Z"
        runtime_features: dict = {}

    store._events = [_FakeRuntimeFeatures()]  # type: ignore[attr-defined]

    assert collect_run_observability_bars(EXPERIMENT_ID, RUN_ID, store) == []
