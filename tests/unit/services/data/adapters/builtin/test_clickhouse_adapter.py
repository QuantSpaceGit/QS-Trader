"""Unit tests for ClickhouseDataAdapter.

Uses unittest.mock to stub the clickhouse_connect library so no live
ClickHouse instance is required.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

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


def test_adapter_rejects_unsafe_database_identifier(ch_config, instrument):
    config = dict(ch_config)
    config["database"] = "market-prod"

    with pytest.raises(ValueError, match="database"):
        ClickhouseDataAdapter(config, instrument)


def test_adapter_rejects_unsafe_bars_table_identifier(ch_config, instrument):
    config = dict(ch_config)
    config["bars_table"] = "as_us_equity_ohlc_daily;drop"

    with pytest.raises(ValueError, match="bars_table"):
        ClickhouseDataAdapter(config, instrument)


# ---------------------------------------------------------------------------
# read_bars
# ---------------------------------------------------------------------------


def test_read_bars_returns_bars_in_order(adapter):
    rows = [
        (date(2024, 1, 2), 185.5, 186.0, 185.0, 185.8, 184.5, 185.0, 184.0, 185.0, 60_000_000, 50_000_000),
        (date(2024, 1, 3), 186.0, 187.0, 185.5, 186.5, 185.0, 186.0, 185.0, 186.0, 55_000_000, 45_000_000),
    ]
    mock_client = _make_mock_client(rows)
    adapter._client = mock_client
    adapter.instrument = Instrument(symbol="AAPL", secid=12345)

    bars = list(adapter.read_bars("2024-01-02", "2024-01-03"))

    assert len(bars) == 2
    assert bars[0].trade_date == date(2024, 1, 2)
    assert bars[1].trade_date == date(2024, 1, 3)
    # Prices are quantized to 2 decimal places
    assert bars[0].close == Decimal("185.80")
    assert bars[0].close_adj == Decimal("185.00")
    assert bars[0].volume == 50_000_000
    assert bars[0].volume_raw == 60_000_000
    assert bars[0].volume_adj == 50_000_000


def test_read_bars_uses_cache_on_repeated_call(adapter):
    rows = [
        (date(2024, 1, 2), 185.5, 186.0, 185.0, 185.8, 184.5, 185.0, 184.0, 185.0, 60_000_000, 50_000_000),
    ]
    mock_client = _make_mock_client(rows)
    adapter._client = mock_client
    adapter.instrument = Instrument(symbol="AAPL", secid=12345)

    list(adapter.read_bars("2024-01-02", "2024-01-02"))
    list(adapter.read_bars("2024-01-02", "2024-01-02"))

    # query() should have been called exactly once (second call served from cache)
    assert mock_client.query.call_count == 1


def test_read_bars_re_fetches_on_different_range(adapter):
    rows = [
        (date(2024, 1, 2), 185.5, 186.0, 185.0, 185.8, 184.5, 185.0, 184.0, 185.0, 60_000_000, 50_000_000),
    ]
    mock_client = _make_mock_client(rows)
    adapter._client = mock_client
    adapter.instrument = Instrument(symbol="AAPL", secid=12345)

    list(adapter.read_bars("2024-01-02", "2024-01-02"))
    list(adapter.read_bars("2024-01-03", "2024-01-05"))

    assert mock_client.query.call_count == 2


def test_read_bars_invalid_range_raises(adapter):
    adapter._client = _make_mock_client([])
    with pytest.raises(ValueError, match="start_date must be <= end_date"):
        list(adapter.read_bars("2024-01-10", "2024-01-05"))


def test_read_bars_empty_result(adapter):
    adapter._client = _make_mock_client([])
    adapter.instrument = Instrument(symbol="AAPL", secid=12345)
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
        volume_raw=60_000_000,
        volume_adj=50_000_000,
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
        volume_raw=35_000_000,
        volume_adj=30_000_000,
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
    assert event.volume_raw == 35_000_000
    assert event.volume_adj == 30_000_000
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
    assert event.volume_raw is None
    assert event.volume_adj is None


# ---------------------------------------------------------------------------
# to_corporate_action_event
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Multi-segment chaining
# ---------------------------------------------------------------------------


def _make_mock_client_side_effect(response_list: list[list[tuple]]) -> MagicMock:
    """Return a mock client whose query() returns successive results."""
    results = [SimpleNamespace(result_rows=rows) for rows in response_list]
    client = MagicMock()
    client.query.side_effect = results
    return client


def _googl_segment1_bars() -> list[tuple]:
    """Simulate GOOGL secid 166006 bars ending at boundary."""
    return [
        (date(2014, 4, 3), 571.5, 575.0, 570.0, 573.0, 571.5, 575.0, 570.0, 573.0, 10000000, 10000000),
        (date(2014, 4, 4), 574.0, 578.0, 572.0, 576.0, 574.0, 578.0, 572.0, 576.0, 11000000, 11000000),
        (date(2015, 10, 1), 650.0, 660.0, 648.0, 655.0, 650.0, 660.0, 648.0, 655.0, 12000000, 12000000),
        (date(2015, 10, 2), 653.0, 658.0, 651.0, 656.99, 653.0, 658.0, 651.0, 656.99, 13000000, 13000000),
    ]


def _googl_segment2_bars() -> list[tuple]:
    """Simulate GOOGL secid 4579561 bars starting after boundary."""
    return [
        (date(2015, 10, 5), 33.5, 34.0, 33.2, 33.8, 33.5, 34.0, 33.2, 33.8, 20000000, 20000000),
        (date(2015, 10, 6), 34.0, 34.5, 33.8, 34.2, 34.0, 34.5, 33.8, 34.2, 21000000, 21000000),
        (date(2015, 10, 7), 34.2, 34.8, 34.0, 34.5, 34.2, 34.8, 34.0, 34.5, 22000000, 22000000),
    ]


@pytest.fixture
def multi_seg_adapter(ch_config):
    """Adapter with multi-segment instrument (GOOGL-like)."""
    from qs_trader.services.data.models import SecidSegment
    segments = [
        SecidSegment(secid=166006, start_date=date(2014, 4, 3), end_date=date(2015, 10, 2), ticker="GOOGL"),
        SecidSegment(secid=4579561, start_date=date(2015, 10, 5), end_date=None, ticker="GOOGL"),
    ]
    inst = Instrument(
        symbol="GOOGL",
        secid=166006,
        secid_segments=segments,
    )
    return ClickhouseDataAdapter(ch_config, inst, dataset_name="qs-datamaster-equity-1d")


def test_chained_bars_continuous_at_boundary(multi_seg_adapter):
    """Chained bars have no >1% discontinuity at secid boundary."""
    mock_client = _make_mock_client_side_effect([
        _googl_segment1_bars(),
        _googl_segment2_bars(),
    ])
    multi_seg_adapter._client = mock_client

    bars = multi_seg_adapter._fetch_bars("2014-01-01", "2015-12-31")

    assert len(bars) == 7  # 4 from seg1 + 3 from seg2

    # The boundary bar from seg2 (2015-10-05) should be chained
    # Linking factor: 656.99 / 33.8 ≈ 19.44
    # Chained closeadj = 33.8 * 19.44 ≈ 656.99
    boundary_bars = [b for b in bars if b.trade_date >= date(2015, 10, 5)]
    assert len(boundary_bars) == 3

    # The first boundary bar's close_adj should be close to the last
    # pre-boundary bar's close_adj (656.99)
    last_pre = [b for b in bars if b.trade_date <= date(2015, 10, 2)][-1]
    first_post = boundary_bars[0]
    assert last_pre.close_adj is not None and first_post.close_adj is not None

    # The ratio between the two should be ~1.0 (continuous)
    ratio = float(last_pre.close_adj) / float(first_post.close_adj)
    assert 0.95 < ratio < 1.05, f"Ratio {ratio} indicates discontinuity at boundary"


def test_chained_bars_records_transition(multi_seg_adapter):
    """Secid transition is recorded in _secid_transitions."""
    mock_client = _make_mock_client_side_effect([
        _googl_segment1_bars(),
        _googl_segment2_bars(),
    ])
    multi_seg_adapter._client = mock_client

    multi_seg_adapter._fetch_bars("2014-01-01", "2015-12-31")

    assert date(2015, 10, 5) in multi_seg_adapter._secid_transitions
    transition = multi_seg_adapter._secid_transitions[date(2015, 10, 5)]
    assert transition["from_secid"] == 166006
    assert transition["to_secid"] == 4579561
    assert transition["linking_factor"] > 0


def test_chained_bars_emits_corp_action_event(multi_seg_adapter):
    """to_corporate_action_event returns event at transition boundary."""
    mock_client = _make_mock_client_side_effect([
        _googl_segment1_bars(),
        _googl_segment2_bars(),
    ])
    multi_seg_adapter._client = mock_client

    bars = multi_seg_adapter._fetch_bars("2014-01-01", "2015-12-31")

    # Bar at transition date should emit CorporateActionEvent
    transition_bar = [b for b in bars if b.trade_date == date(2015, 10, 5)][0]
    event = multi_seg_adapter.to_corporate_action_event(transition_bar)
    assert event is not None
    assert event.action_type == "symbol_change"
    assert event.notes is not None
    assert "166006" in event.notes
    assert "4579561" in event.notes
    assert event.price_adjustment_factor is not None

    # Non-transition bar should return None
    normal_bar = [b for b in bars if b.trade_date == date(2014, 4, 3)][0]
    assert multi_seg_adapter.to_corporate_action_event(normal_bar) is None


def test_single_segment_path_works_with_secid(adapter):
    """Single-segment instrument uses secid query path."""
    mock_client = _make_mock_client_side_effect([
        _googl_segment1_bars(),
    ])
    adapter._client = mock_client
    adapter.instrument = Instrument(symbol="AAPL", secid=12345)

    bars = adapter._fetch_bars("2014-01-01", "2015-12-31")
    assert len(bars) == 4
    assert adapter._secid_transitions == {}  # No transitions for single segment


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
        open=Decimal("1"),
        high=Decimal("1"),
        low=Decimal("1"),
        close=Decimal("1"),
        open_adj=None,
        high_adj=None,
        low_adj=None,
        close_adj=None,
        volume=0,
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
    adapter.instrument = Instrument(symbol="AAPL", secid=12345)

    min_date, max_date = adapter.get_available_date_range()
    assert min_date == "2020-01-02"
    assert max_date == "2024-12-31"


def test_get_available_date_range_returns_none_on_error(adapter):
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("connection error")
    adapter._client = mock_client
    adapter.instrument = Instrument(symbol="AAPL", secid=12345)

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


# ---------------------------------------------------------------------------
# Secid-based OHLC loading
# ---------------------------------------------------------------------------


def test_read_bars_uses_secid_when_instrument_has_secid(ch_config):
    """When instrument.secid is set, the query should use WHERE secid = ..."""
    from qs_trader.services.data.models import Instrument

    instrument = Instrument(symbol="AAPL", secid=12345)
    adapter = ClickhouseDataAdapter(ch_config, instrument, dataset_name="qs-datamaster-equity-1d")

    rows = [
        (date(2024, 1, 2), 185.5, 186.0, 185.0, 185.8, 184.5, 185.0, 184.0, 185.0, 60_000_000, 50_000_000),
    ]
    mock_client = _make_mock_client(rows)
    adapter._client = mock_client

    list(adapter.read_bars("2024-01-02", "2024-01-02"))

    sql = mock_client.query.call_args[1]["parameters"]
    assert "secid" in sql
    assert sql["secid"] == 12345
    # ticker should NOT be in the parameters when secid is used
    assert "symbol" not in sql


def test_read_bars_raises_assertion_error_when_secid_is_none(ch_config):
    """When instrument.secid is None, read_bars should raise AssertionError."""
    from qs_trader.services.data.models import Instrument

    instrument = Instrument(symbol="AAPL", secid=None)
    adapter = ClickhouseDataAdapter(ch_config, instrument, dataset_name="qs-datamaster-equity-1d")

    adapter._client = _make_mock_client([])

    with pytest.raises(AssertionError, match="requires secid"):
        list(adapter.read_bars("2024-01-02", "2024-01-02"))


def test_get_available_date_range_uses_secid_when_set(ch_config):
    """get_available_date_range should query by secid when instrument.secid is set."""
    from qs_trader.services.data.models import Instrument

    instrument = Instrument(symbol="AAPL", secid=99999)
    adapter = ClickhouseDataAdapter(ch_config, instrument, dataset_name="qs-datamaster-equity-1d")

    rows = [("2020-01-02", "2024-12-31")]
    adapter._client = _make_mock_client(rows)

    adapter.get_available_date_range()

    sql = adapter._client.query.call_args[1]["parameters"]
    assert "secid" in sql
    assert sql["secid"] == 99999


def test_get_available_date_range_returns_none_when_secid_is_none(ch_config):
    """get_available_date_range should return (None, None) when secid is None."""
    from qs_trader.services.data.models import Instrument

    instrument = Instrument(symbol="AAPL")
    adapter = ClickhouseDataAdapter(ch_config, instrument, dataset_name="qs-datamaster-equity-1d")

    min_date, max_date = adapter.get_available_date_range()
    assert min_date is None
    assert max_date is None


# ---------------------------------------------------------------------------
# Identity fields on PriceBarEvent
# ---------------------------------------------------------------------------


def test_to_price_bar_event_populates_identity_fields_from_instrument(ch_config):
    """Identity fields on PriceBarEvent should come from the instrument."""
    from qs_trader.services.data.models import Instrument

    instrument = Instrument(
        symbol="AAPL",
        secid=12345,
        display_symbol="Apple Inc",
        ticker_at_date="AAPL",
        identity_source="explicit_secid",
    )
    adapter = ClickhouseDataAdapter(ch_config, instrument, dataset_name="qs-datamaster-equity-1d")

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
        volume_raw=60_000_000,
        volume_adj=50_000_000,
    )
    event = adapter.to_price_bar_event(bar)

    assert event.secid == 12345
    assert event.display_symbol == "Apple Inc"
    assert event.ticker_at_date == "AAPL"
    assert event.identity_source == "explicit_secid"


def test_to_price_bar_event_identity_fields_are_none_when_instrument_lacks_them(ch_config):
    """Identity fields should be None when instrument does not have them."""
    from types import SimpleNamespace

    # Use a plain object without identity fields (backward compat)
    instrument = SimpleNamespace(symbol="AAPL")
    adapter = ClickhouseDataAdapter(ch_config, instrument, dataset_name="qs-datamaster-equity-1d")

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

    assert event.secid is None
    assert event.display_symbol is None
    assert event.ticker_at_date is None
    assert event.identity_source is None


# ============================================================================
# Retry Logic Tests
# ============================================================================


class TestClickHouseRetry:
    """Tests for ClickHouse adapter retry with exponential backoff."""

    def test_is_retryable_error_connection(self, ch_config):
        """Connection errors should be retryable."""
        from types import SimpleNamespace
        adapter = ClickhouseDataAdapter(ch_config, SimpleNamespace(symbol="AAPL"))

        assert adapter._is_retryable_error(ConnectionError("connection refused")) is True
        assert adapter._is_retryable_error(TimeoutError("timed out")) is True
        assert adapter._is_retryable_error(OSError("network unreachable")) is True

    def test_is_retryable_error_permanent(self, ch_config):
        """Auth and syntax errors should NOT be retryable."""
        from types import SimpleNamespace
        adapter = ClickhouseDataAdapter(ch_config, SimpleNamespace(symbol="AAPL"))

        assert adapter._is_retryable_error(Exception("authentication failed")) is False
        assert adapter._is_retryable_error(Exception("syntax error in query")) is False
        assert adapter._is_retryable_error(Exception("unknown table foo")) is False
        assert adapter._is_retryable_error(Exception("access denied")) is False

    def test_is_retryable_error_generic_with_retryable_keyword(self, ch_config):
        """Generic exceptions with retryable keywords should be retried."""
        from types import SimpleNamespace
        adapter = ClickhouseDataAdapter(ch_config, SimpleNamespace(symbol="AAPL"))

        assert adapter._is_retryable_error(Exception("connection reset by peer")) is True
        assert adapter._is_retryable_error(Exception("server timeout")) is True

    def test_retry_succeeds_after_transient_failures(self, ch_config):
        """Should succeed when transient failures are followed by success."""
        from types import SimpleNamespace
        adapter = ClickhouseDataAdapter(ch_config, SimpleNamespace(symbol="AAPL"))

        call_count = 0

        def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("connection refused")
            return "success"

        result = adapter._retry_with_backoff("test_op", flaky_operation)
        assert result == "success"
        assert call_count == 3

    def test_retry_exhaustion_raises_last_exception(self, ch_config):
        """Should raise the last exception after all retries exhausted."""
        from types import SimpleNamespace
        adapter = ClickhouseDataAdapter(ch_config, SimpleNamespace(symbol="AAPL"))
        adapter._max_retries = 2
        adapter._retry_base_delay = 0.01  # Fast for testing

        def always_fail():
            raise ConnectionError("persistent failure")

        import pytest
        with pytest.raises(ConnectionError, match="persistent failure"):
            adapter._retry_with_backoff("test_op", always_fail)

    def test_no_retry_on_permanent_error(self, ch_config):
        """Should raise immediately on permanent (non-retryable) errors."""
        from types import SimpleNamespace
        adapter = ClickhouseDataAdapter(ch_config, SimpleNamespace(symbol="AAPL"))
        adapter._max_retries = 3

        call_count = 0

        def auth_fail():
            nonlocal call_count
            call_count += 1
            raise Exception("authentication failed")

        import pytest
        with pytest.raises(Exception, match="authentication failed"):
            adapter._retry_with_backoff("test_op", auth_fail)

        assert call_count == 1  # Only one attempt, no retries

    def test_custom_retry_config_from_ch_config(self):
        """Adapter should read max_retries and retry_base_delay from config."""
        from types import SimpleNamespace
        config = {
            "clickhouse": {
                "host": "localhost",
                "port": 8123,
                "password": "",
                "database": "market",
                "max_retries": 5,
                "retry_base_delay": 2.0,
            }
        }
        adapter = ClickhouseDataAdapter(config, SimpleNamespace(symbol="AAPL"))
        assert adapter._max_retries == 5
        assert adapter._retry_base_delay == 2.0
