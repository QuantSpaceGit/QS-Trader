"""Tests for always-on indicator emission (audit-export v3).

These guard the contract that ``StrategyService`` emits ``IndicatorEvent``
whenever ``context.track_indicators()`` produced non-empty indicators,
regardless of the legacy ``log_indicators`` config flag. The flag is still
parsed for back-compat but only drives a setup-time deprecation warning.
"""

from __future__ import annotations

import logging
from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import IndicatorEvent, PriceBarEvent
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.strategy.service import StrategyService


class _TrackingStrategyConfig(StrategyConfig):
    name: str = "tracker"
    display_name: str = "Tracker"
    universe: list[str] = ["AAPL"]


class _TrackingStrategy(Strategy):
    def __init__(self, config: _TrackingStrategyConfig) -> None:
        self.config = config

    def setup(self, context: Context) -> None:  # pragma: no cover - trivial
        pass

    def teardown(self, context: Context) -> None:  # pragma: no cover - trivial
        pass

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        context.track_indicators(indicators={"sma": 100.0})


def _bar() -> PriceBarEvent:
    return PriceBarEvent(
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


@pytest.mark.parametrize("log_indicators_value", [True, False])
def test_indicator_event_emitted_regardless_of_log_indicators_flag(
    log_indicators_value: bool,
) -> None:
    """v3 contract: emission is always-on when indicators are tracked."""
    event_bus = EventBus()
    strategy = _TrackingStrategy(
        _TrackingStrategyConfig(log_indicators=log_indicators_value)
    )
    service = StrategyService(event_bus=event_bus, strategies={"tracker": strategy})

    captured: list[IndicatorEvent] = []
    event_bus.subscribe("indicator", lambda evt: captured.append(evt))

    service.setup()
    service.on_bar(_bar())

    assert len(captured) == 1
    assert captured[0].indicators == {"sma": 100.0}


def test_log_indicators_false_triggers_setup_deprecation_warning(
    caplog: pytest.LogCaptureFixture,
) -> None:
    event_bus = EventBus()
    strategy = _TrackingStrategy(
        _TrackingStrategyConfig(log_indicators=False)
    )
    service = StrategyService(event_bus=event_bus, strategies={"tracker": strategy})

    with caplog.at_level(logging.WARNING):
        service.setup()

    # The structlog logger used by strategy.service emits key=value style
    # messages; match against the event token regardless of renderer.
    log_text = "\n".join(record.getMessage() for record in caplog.records)
    assert "log_indicators_deprecated" in log_text or "log_indicators=False" in log_text


def test_log_indicators_default_true_does_not_warn(
    caplog: pytest.LogCaptureFixture,
) -> None:
    event_bus = EventBus()
    strategy = _TrackingStrategy(_TrackingStrategyConfig())  # not set explicitly
    service = StrategyService(event_bus=event_bus, strategies={"tracker": strategy})

    with caplog.at_level(logging.WARNING):
        service.setup()

    log_text = "\n".join(record.getMessage() for record in caplog.records)
    assert "log_indicators_deprecated" not in log_text
