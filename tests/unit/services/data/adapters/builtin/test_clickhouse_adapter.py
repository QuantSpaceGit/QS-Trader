"""Unit tests for ClickhouseDataAdapter.

Uses unittest.mock to stub the clickhouse_connect library so no live
ClickHouse instance is required.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.services.data.adapters.builtin.clickhouse import ClickhouseBar, ClickhouseDataAdapter
from qs_trader.services.data.models import Instrument


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def ch_config():
    """Minimal adapter config (flat style, no 'clickhouse' subkey) for tests."""
    return {
        "host": "localhost",
        "port": 8123,
        "username": "default",
        "password": "testpass",
        "database": "market_test",
        "timezone": "America/New_York",
        "asset_class": "equity",
        "price_currency": "USD",
        "price_scale": 2,
    }


@pytest.fixture
def instrument():
    return Instrument(symbol="AAPL")


@pytest.fixture
def adapter(ch_config, instrument):
    return ClickhouseDataAdapter(ch_config, instrument, dataset_name="qs-datamaster-equity-1d")


def _make_mock_client(rows: list[tuple]) -> MagicMock:
    """Return a mock clickhouse_connect client that returns ``rows`` on query()."""
    result = SimpleNamespace(result_rows=rows)
    client = MagicMock()
    client.query.return_value = result
    return client


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

def test_adapter_reads_connection_config_from_top_level(ch_config, instrument):
    adapter = ClickhouseDataAdapter(ch_config, instrument)
    assert adapter._host == "localhost"
    assert adapter._port == 8123
    assert adapter._username == "default"
    assert adapter._password == "testpass"
    assert adapter._database == "market_test"


def test_adapter_reads_connection_config_from_clickhouse_subkey(instrument):
    config = {
        "clickhouse": {
            "host": "ch-host",
            "port": 9000,
            "username": "analyst",
            "password": "s3cr3t",
            "database": "market_prod",
        },
        "timezone": "America/New_York",
        "price_currency": "USD",
        "price_scale": 2,
        "asset_class": "equity",
    }
    adapter = ClickhouseDataAdapter(config, instrument)
    assert adapter._host == "ch-host"
    assert adapter._port == 9000
    assert adapter._username == "analyst"
    assert adapter._database == "market_prod"


# ---------------------------------------------------------------------------
# read_bars
# ---------------------------------------------------------------------------

def test_read_bars_returns_bars_in_order(adapter):
    rows = [
        (date(2024, 1, 2), 185.5, 186.0, 185.0, 185.8, 184.5, 185.0, 184.0, 185.0, 50_000_000),
        (date(2024, 1, 3), 186.0, 187.0, 185.5, 186.5, 185.0, 186.0, 185.0, 186.0, 45_000_000),
    ]
    mock_client = _make_mock_client(rows)
    adapter._client = mock_client

    bars = list(adapter.read_bars("2024-01-02", "2024-01-03"))

    assert len(bars) == 2
    assert bars[0].trade_date == date(2024, 1, 2)
    assert bars[1].trade_date == date(2024, 1, 3)
    # Prices are quantized to 2 decimal places
    assert bars[0].close == Decimal("185.80")
    assert bars[0].close_adj == Decimal("185.00")
    assert bars[0].volume == 50_000_000


def test_read_bars_uses_cache_on_repeated_call(adapter):
    rows = [
        (date(2024, 1, 2), 185.5, 186.0, 185.0, 185.8, 184.5, 185.0, 184.0, 185.0, 50_000_000),
    ]
    mock_client = _make_mock_client(rows)
    adapter._client = mock_client

    list(adapter.read_bars("2024-01-02", "2024-01-02"))
    list(adapter.read_bars("2024-01-02", "2024-01-02"))

    # query() should have been called exactly once (second call served from cache)
    assert mock_client.query.call_count == 1


def test_read_bars_re_fetches_on_different_range(adapter):
    rows = [
        (date(2024, 1, 2), 185.5, 186.0, 185.0, 185.8, 184.5, 185.0, 184.0, 185.0, 50_000_000),
    ]
    mock_client = _make_mock_client(rows)
    adapter._client = mock_client

    list(adapter.read_bars("2024-01-02", "2024-01-02"))
    list(adapter.read_bars("2024-01-03", "2024-01-05"))

    assert mock_client.query.call_count == 2


def test_read_bars_invalid_range_raises(adapter):
    adapter._client = _make_mock_client([])
    with pytest.raises(ValueError, match="start_date must be <= end_date"):
        list(adapter.read_bars("2024-01-10", "2024-01-05"))


def test_read_bars_empty_result(adapter):
    adapter._client = _make_mock_client([])
    bars = list(adapter.read_bars("2024-01-02", "2024-01-05"))
    assert bars == []


# ---------------------------------------------------------------------------
# to_price_bar_event
# ---------------------------------------------------------------------------

def test_to_price_bar_event_timestamp_is_utc_market_close(adapter):
    bar = ClickhouseBar(
        symbol="AAPL",
        trade_date=date(2024, 1, 2),
        open=Decimal("185.50"),
        high=Decimal("186.00"),
        low=Decimal("185.00"),
        close=Decimal("185.80"),
        open_adj=Decimal("184.50"),
        high_adj=Decimal("185.00"),
        low_adj=Decimal("184.00"),
        close_adj=Decimal("185.00"),
        volume=50_000_000,
    )
    event = adapter.to_price_bar_event(bar)

    # 16:00 ET on 2024-01-02 is 21:00 UTC (EST +5)
    ts = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
    assert ts.tzinfo is not None
    assert ts.hour == 21
    assert ts.minute == 0


def test_to_price_bar_event_fields(adapter):
    bar = ClickhouseBar(
        symbol="AAPL",
        trade_date=date(2024, 3, 15),
        open=Decimal("170.00"),
        high=Decimal("172.00"),
        low=Decimal("169.00"),
        close=Decimal("171.00"),
        open_adj=Decimal("170.50"),
        high_adj=Decimal("172.50"),
        low_adj=Decimal("169.50"),
        close_adj=Decimal("171.50"),
        volume=30_000_000,
    )
    event = adapter.to_price_bar_event(bar)

    assert event.symbol == "AAPL"
    assert event.open == Decimal("170.00")
    assert event.high == Decimal("172.00")
    assert event.low == Decimal("169.00")
    assert event.close == Decimal("171.00")
    assert event.open_adj == Decimal("170.50")
    assert event.close_adj == Decimal("171.50")
    assert event.volume == 30_000_000
    assert event.price_currency == "USD"
    assert event.price_scale == 2
    assert event.source == "qs-datamaster-equity-1d"
    assert event.source_service == "data_service"
    assert event.interval == "1d"


def test_to_price_bar_event_none_adj_fields(adapter):
    bar = ClickhouseBar(
        symbol="AAPL",
        trade_date=date(2024, 1, 2),
        open=Decimal("185.50"),
        high=Decimal("186.00"),
        low=Decimal("185.00"),
        close=Decimal("185.80"),
        open_adj=None,
        high_adj=None,
        low_adj=None,
        close_adj=None,
        volume=0,
    )
    event = adapter.to_price_bar_event(bar)
    assert event.open_adj is None
    assert event.close_adj is None


# ---------------------------------------------------------------------------
# to_corporate_action_event
# ---------------------------------------------------------------------------

def test_to_corporate_action_event_always_none(adapter):
    bar = ClickhouseBar(
        symbol="AAPL",
        trade_date=date(2024, 1, 2),
        open=Decimal("185.50"),
        high=Decimal("186.00"),
        low=Decimal("185.00"),
        close=Decimal("185.80"),
        open_adj=None,
        high_adj=None,
        low_adj=None,
        close_adj=None,
        volume=100,
    )
    assert adapter.to_corporate_action_event(bar) is None
    assert adapter.to_corporate_action_event(bar, prev_bar=bar) is None


# ---------------------------------------------------------------------------
# get_timestamp
# ---------------------------------------------------------------------------

def test_get_timestamp_returns_midnight_utc(adapter):
    bar = ClickhouseBar(
        symbol="AAPL",
        trade_date=date(2024, 6, 15),
        open=Decimal("1"), high=Decimal("1"), low=Decimal("1"), close=Decimal("1"),
        open_adj=None, high_adj=None, low_adj=None, close_adj=None, volume=0,
    )
    ts = adapter.get_timestamp(bar)
    assert ts.date() == date(2024, 6, 15)
    assert ts.hour == 0
    assert ts.minute == 0


# ---------------------------------------------------------------------------
# get_available_date_range
# ---------------------------------------------------------------------------

def test_get_available_date_range_returns_from_clickhouse(adapter):
    rows = [("2020-01-02", "2024-12-31")]
    adapter._client = _make_mock_client(rows)

    min_date, max_date = adapter.get_available_date_range()
    assert min_date == "2020-01-02"
    assert max_date == "2024-12-31"


def test_get_available_date_range_returns_none_on_error(adapter):
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("connection error")
    adapter._client = mock_client

    min_date, max_date = adapter.get_available_date_range()
    assert min_date is None
    assert max_date is None


# ---------------------------------------------------------------------------
# Registry name
# ---------------------------------------------------------------------------

def test_adapter_registry_name():
    """Confirm _generate_adapter_name produces 'clickhouse' for ClickhouseDataAdapter."""
    from qs_trader.libraries.registry import AdapterRegistry

    registry = AdapterRegistry()
    name = registry._generate_adapter_name("ClickhouseDataAdapter")
    assert name == "clickhouse"
