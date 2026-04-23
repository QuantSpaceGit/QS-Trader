"""Unit tests for bar_snapshot_collector.collect_run_bar_snapshots."""

from __future__ import annotations

from decimal import Decimal

from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import IndicatorEvent, PriceBarEvent
from qs_trader.services.reporting.bar_snapshot_collector import (
    collect_run_bar_snapshots,
)


EXPERIMENT_ID = "snapshot_exp"
RUN_ID = "run-001"


def test_empty_store_emits_no_rows() -> None:
    assert collect_run_bar_snapshots(EXPERIMENT_ID, RUN_ID, None) == []
    assert collect_run_bar_snapshots(EXPERIMENT_ID, RUN_ID, InMemoryEventStore()) == []


def test_single_bar_emits_runtime_snapshot_row() -> None:
    store = InMemoryEventStore()
    store.append(
        PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-02T21:00:00Z",
            timestamp_local="2024-01-02T16:00:00-05:00",
            timezone="America/New_York",
            interval="1d",
            open=Decimal("100.00"),
            high=Decimal("101.00"),
            low=Decimal("99.00"),
            close=Decimal("100.50"),
            open_adj=Decimal("99.50"),
            high_adj=Decimal("100.50"),
            low_adj=Decimal("98.50"),
            close_adj=Decimal("100.00"),
            volume=1000,
            volume_raw=1200,
            volume_adj=1000,
            price_currency="USD",
            price_scale=2,
            source="qs-datamaster-equity-1d",
        )
    )

    rows = collect_run_bar_snapshots(EXPERIMENT_ID, RUN_ID, store)

    assert len(rows) == 1
    row = rows[0]
    assert row["experiment_id"] == EXPERIMENT_ID
    assert row["run_id"] == RUN_ID
    assert row["symbol"] == "AAPL"
    assert row["bar_timestamp"] == "2024-01-02T21:00:00Z"
    assert row["timestamp_local"] == "2024-01-02T16:00:00-05:00"
    assert row["timezone"] == "America/New_York"
    assert row["source_name"] == "qs-datamaster-equity-1d"
    assert row["price_currency"] == "USD"
    assert row["price_scale"] == 2
    assert row["open_raw"] == Decimal("100.00")
    assert row["close_adj"] == Decimal("100.00")
    assert row["volume_raw"] == 1200
    assert row["volume_adj"] == 1000


def test_legacy_single_volume_falls_back_to_raw_only() -> None:
    store = InMemoryEventStore()
    store.append(
        PriceBarEvent(
            symbol="MSFT",
            timestamp="2024-01-03T21:00:00Z",
            interval="1d",
            open=Decimal("200.00"),
            high=Decimal("201.00"),
            low=Decimal("199.00"),
            close=Decimal("200.50"),
            volume=500,
            source="legacy_csv",
        )
    )

    rows = collect_run_bar_snapshots(EXPERIMENT_ID, RUN_ID, store)

    assert len(rows) == 1
    assert rows[0]["volume_raw"] == 500
    assert rows[0]["volume_adj"] is None


def test_non_bar_events_are_ignored() -> None:
    store = InMemoryEventStore()
    store.append(
        IndicatorEvent(
            strategy_id="sma_crossover",
            symbol="AAPL",
            timestamp="2024-01-02T21:00:00Z",
            indicators={"sma_fast": Decimal("100.25")},
        )
    )

    assert collect_run_bar_snapshots(EXPERIMENT_ID, RUN_ID, store) == []