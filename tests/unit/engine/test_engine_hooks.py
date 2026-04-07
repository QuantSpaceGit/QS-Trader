"""Unit tests for Phase 1 engine hooks.

Covers:
- Progress callback (on_progress) fires with (bars_processed, bars_total)
- on_progress=None is zero-cost (no AttributeError, no extra overhead)
- bars_total estimate is a positive integer
"""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.engine.config import BacktestConfig, DataSelectionConfig, DataSourceConfig, RiskPolicyConfig
from qs_trader.engine.engine import BacktestEngine, BacktestResult
from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_config() -> BacktestConfig:
    return BacktestConfig(
        backtest_id="progress_test",
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 3, 31),
        initial_equity=100_000,
        data=DataSelectionConfig(
            sources=[DataSourceConfig(name="yahoo-us-equity-1d-csv", universe=["AAPL", "MSFT"])]
        ),
        strategies=[],
        risk_policy=RiskPolicyConfig(name="naive"),
    )


def _make_engine(config: BacktestConfig) -> BacktestEngine:
    """Build a BacktestEngine with fully mocked services."""
    event_bus = EventBus()
    event_store = InMemoryEventStore()
    event_bus.attach_store(event_store)

    data_service = MagicMock()
    # Simulate streaming two symbols × 5 bars each
    def fake_stream(**kwargs):
        for _ in range(5):
            event_bus.publish(MagicMock(event_type="bar"))

    data_service.stream_universe.side_effect = fake_stream

    return BacktestEngine(
        config=config,
        event_bus=event_bus,
        data_service=data_service,
        event_store=event_store,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestProgressCallback:
    def test_callback_fires_for_each_bar(self) -> None:
        """on_progress is called once per bar event."""
        config = _minimal_config()
        engine = _make_engine(config)

        calls: list[tuple[int, int]] = []
        engine.run(on_progress=lambda processed, total: calls.append((processed, total)))

        assert len(calls) == 5
        # Processed values should be monotonically increasing from 1
        assert [c[0] for c in calls] == [1, 2, 3, 4, 5]

    def test_callback_bars_total_is_positive(self) -> None:
        """bars_total estimate is always a positive integer."""
        config = _minimal_config()
        engine = _make_engine(config)

        totals: list[int] = []
        engine.run(on_progress=lambda processed, total: totals.append(total))

        assert all(t > 0 for t in totals)
        # All calls share the same bars_total estimate
        assert len(set(totals)) == 1

    def test_callback_none_no_error(self) -> None:
        """Passing on_progress=None does not raise and returns a BacktestResult."""
        config = _minimal_config()
        engine = _make_engine(config)

        result = engine.run(on_progress=None)
        assert isinstance(result, BacktestResult)

    def test_run_default_no_progress(self) -> None:
        """run() works with no argument (default None)."""
        config = _minimal_config()
        engine = _make_engine(config)

        result = engine.run()
        assert result.bars_processed == 5

    def test_callback_receives_consistent_total_across_calls(self) -> None:
        """bars_total is constant across all callback invocations in one run."""
        config = _minimal_config()
        engine = _make_engine(config)

        totals: set[int] = set()
        engine.run(on_progress=lambda p, t: totals.add(t))
        assert len(totals) == 1, "bars_total should be the same for every bar"
