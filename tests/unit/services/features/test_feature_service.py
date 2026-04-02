"""Unit tests for FeatureService.

Uses unittest.mock to stub the clickhouse_connect library so no live
ClickHouse instance is required.
"""

from __future__ import annotations

import math
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.services.features.service import FeatureService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _service(
    *,
    host: str = "localhost",
    port: int = 8123,
    username: str = "default",
    password: str = "testpass",
    database: str = "market_test",
    feature_version: str = "v1",
    regime_version: str = "v1",
    connect_timeout: int = 10,
    query_timeout: int = 30,
    default_columns: list[str] | None = None,
) -> FeatureService:
    """Construct a FeatureService with test defaults."""
    return FeatureService(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
        feature_version=feature_version,
        regime_version=regime_version,
        connect_timeout=connect_timeout,
        query_timeout=query_timeout,
        default_columns=default_columns,
    )


def _make_client(rows: list) -> MagicMock:
    """Return a mock clickhouse_connect client that yields ``rows``."""
    result = SimpleNamespace(result_rows=rows)
    client = MagicMock()
    client.query.return_value = result
    return client


# ---------------------------------------------------------------------------
# Construction / from_config
# ---------------------------------------------------------------------------


def test_from_config_reads_clickhouse_subkey():
    config = {
        "clickhouse": {
            "host": "ch-host",
            "port": 9000,
            "username": "analyst",
            "password": "secret",
            "database": "market_prod",
        },
        "feature_version": "v2",
        "regime_version": "v2",
    }
    svc = FeatureService.from_config(config)
    assert svc._host == "ch-host"
    assert svc._port == 9000
    assert svc._username == "analyst"
    assert svc._database == "market_prod"
    assert svc._feature_version == "v2"
    assert svc._regime_version == "v2"


def test_from_config_falls_back_to_env_vars(monkeypatch):
    monkeypatch.setenv("CLICKHOUSE_HOST", "env-host")
    monkeypatch.setenv("CLICKHOUSE_HTTP_PORT", "9876")
    monkeypatch.setenv("CLICKHOUSE_USER", "env-user")
    monkeypatch.setenv("CLICKHOUSE_PASSWORD", "env-pass")
    monkeypatch.setenv("CLICKHOUSE_DATABASE", "env-db")

    svc = FeatureService.from_config({})  # empty config → env vars
    assert svc._host == "env-host"
    assert svc._port == 9876
    assert svc._username == "env-user"
    assert svc._password == "env-pass"
    assert svc._database == "env-db"


def test_from_config_flat_dict():
    """from_config should also accept a flat dict (no 'clickhouse' subkey)."""
    config = {
        "host": "flat-host",
        "password": "flat-pass",
        "database": "flat-db",
    }
    svc = FeatureService.from_config(config)
    assert svc._host == "flat-host"
    assert svc._database == "flat-db"


# ---------------------------------------------------------------------------
# _resolve_secid
# ---------------------------------------------------------------------------


def test_resolve_secid_returns_int(monkeypatch):
    svc = _service()
    svc._client = _make_client([(12345,)])

    secid = svc._resolve_secid("AAPL", as_of_date="2024-01-02")
    assert secid == 12_345


def test_resolve_secid_caches_result(monkeypatch):
    svc = _service()
    svc._client = _make_client([(12345,)])

    svc._resolve_secid("AAPL", as_of_date="2024-01-02")
    svc._resolve_secid("AAPL", as_of_date="2024-01-02")  # Same date → served from cache

    assert svc._client.query.call_count == 1
    assert svc._secid_cache[("AAPL", "2024-01-02")] == 12_345


def test_resolve_secid_different_dates_each_hit_clickhouse():
    """Different as_of_date values must not share a cached secid.

    A recycled ticker that was held by issuer A on 2015-01-02 and by
    issuer B on 2024-01-02 must resolve to the correct secid for each
    date independently.
    """
    svc = _service()
    # Return different secids for the two date-bounded queries
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(111,)]),  # first call: old issuer
        SimpleNamespace(result_rows=[(222,)]),  # second call: recycled issuer
    ]
    svc._client = mock_client

    old_secid = svc._resolve_secid("XYZ", as_of_date="2015-01-02")
    new_secid = svc._resolve_secid("XYZ", as_of_date="2024-01-02")

    assert old_secid == 111
    assert new_secid == 222
    assert mock_client.query.call_count == 2  # both dates hit ClickHouse


def test_resolve_secid_uses_date_filter_in_sql():
    """The SQL should contain the as_of_date so recycled tickers resolve correctly."""
    svc = _service()
    mock_client = _make_client([(99,)])
    svc._client = mock_client

    svc._resolve_secid("AAPL", as_of_date="2024-06-01")

    sql = mock_client.query.call_args[0][0]
    assert "2024-06-01" in sql
    assert "tradedate" in sql


def test_resolve_secid_returns_none_when_not_found():
    svc = _service()
    svc._client = _make_client([])  # no rows

    assert svc._resolve_secid("MISSING", as_of_date="2024-01-02") is None
    assert svc._secid_cache.get(("MISSING", "2024-01-02")) is None  # cached as None


def test_resolve_secid_returns_none_on_exception():
    svc = _service()
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("network error")
    svc._client = mock_client

    result = svc._resolve_secid("AAPL", as_of_date="2024-01-02")
    assert result is None


# ---------------------------------------------------------------------------
# get_features
# ---------------------------------------------------------------------------


def test_get_features_returns_dict_with_all_columns():
    svc = _service()
    # Mock: secid resolution → 99, feature query → one row of 23 floats/strings
    feature_vals = [0.1] * 18 + ["bull", "low", "low", "positive", "risk_on"]
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(99,)]),  # secid
        SimpleNamespace(result_rows=[feature_vals]),  # features
    ]
    svc._client = mock_client

    result = svc.get_features("AAPL", "2024-01-02")

    assert result is not None
    assert len(result) == len(svc._FEATURE_COLUMNS)
    # Numeric columns are floats
    numeric_key = svc._FEATURE_COLUMNS[0]
    assert isinstance(result[numeric_key], float)
    # Regime columns are strings
    assert result["trend_regime"] == "bull"
    assert result["vol_regime"] == "low"


def test_get_features_returns_subset_when_columns_specified():
    svc = _service()
    feature_vals = [0.5] * 18 + ["bull", "low", "low", "positive", "risk_on"]
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(99,)]),
        SimpleNamespace(result_rows=[feature_vals]),
    ]
    svc._client = mock_client

    result = svc.get_features("AAPL", "2024-01-02", columns=["momentum_12_1", "trend_regime"])

    assert result is not None
    assert set(result.keys()).issubset({"momentum_12_1", "trend_regime"})


def test_get_features_caches_result():
    svc = _service()
    feature_vals = [1.0] * 18 + ["bull", "low", "low", "positive", "risk_on"]
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(99,)]),
        SimpleNamespace(result_rows=[feature_vals]),
    ]
    svc._client = mock_client

    svc.get_features("AAPL", "2024-01-02")
    svc.get_features("AAPL", "2024-01-02")  # cache hit

    # secid query + feature query = 2 total, not 4
    assert mock_client.query.call_count == 2


def test_get_features_returns_none_when_no_row():
    svc = _service()
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(99,)]),  # secid found
        SimpleNamespace(result_rows=[]),  # no feature row
    ]
    svc._client = mock_client

    assert svc.get_features("AAPL", "2020-01-01") is None


def test_get_features_returns_none_when_secid_not_found():
    svc = _service()
    svc._client = _make_client([])  # secid lookup fails

    assert svc.get_features("UNKNOWN", "2024-01-02") is None


def test_get_features_handles_inf_as_nan():
    svc = _service()
    feature_vals = [float("inf")] + [0.1] * 17 + ["bull", "low", "low", "positive", "risk_on"]
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(99,)]),
        SimpleNamespace(result_rows=[feature_vals]),
    ]
    svc._client = mock_client

    result = svc.get_features("AAPL", "2024-01-02")
    assert result is not None
    assert math.isnan(result[svc._FEATURE_COLUMNS[0]])


# ---------------------------------------------------------------------------
# get_indicators
# ---------------------------------------------------------------------------


def test_get_indicators_returns_dict():
    svc = _service()
    indicator_vals = [1.5] * len(svc._INDICATOR_COLUMNS)
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(99,)]),
        SimpleNamespace(result_rows=[indicator_vals]),
    ]
    svc._client = mock_client

    result = svc.get_indicators("AAPL", "2024-01-02")
    assert result is not None
    assert len(result) == len(svc._INDICATOR_COLUMNS)
    assert all(isinstance(v, float) for v in result.values())


def test_get_indicators_returns_none_on_empty_result():
    svc = _service()
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(99,)]),
        SimpleNamespace(result_rows=[]),
    ]
    svc._client = mock_client

    assert svc.get_indicators("AAPL", "2024-01-02") is None


def test_get_indicators_caches_none():
    svc = _service()
    mock_client = MagicMock()
    mock_client.query.side_effect = [
        SimpleNamespace(result_rows=[(99,)]),
        SimpleNamespace(result_rows=[]),
    ]
    svc._client = mock_client

    svc.get_indicators("AAPL", "2024-01-01")
    svc.get_indicators("AAPL", "2024-01-01")  # cache hit

    assert mock_client.query.call_count == 2  # secid + indicator, not 4


# ---------------------------------------------------------------------------
# get_regime
# ---------------------------------------------------------------------------


def test_get_regime_returns_dict_of_strings():
    svc = _service()
    svc._client = _make_client([("bull", "low", "low", "positive", "risk_on")])

    result = svc.get_regime("2024-01-02")
    assert result is not None
    assert result["trend_regime"] == "bull"
    assert result["vol_regime"] == "low"
    assert result["risk_regime"] == "low"
    assert result["breadth_regime"] == "positive"
    assert result["composite_regime"] == "risk_on"


def test_get_regime_caches_result():
    svc = _service()
    svc._client = _make_client([("bull", "low", "low", "positive", "risk_on")])

    svc.get_regime("2024-01-02")
    svc.get_regime("2024-01-02")  # cache hit

    assert svc._client.query.call_count == 1


def test_get_regime_returns_none_when_no_row():
    svc = _service()
    svc._client = _make_client([])

    assert svc.get_regime("2020-01-01") is None


def test_get_regime_returns_none_on_exception():
    svc = _service()
    mock_client = MagicMock()
    mock_client.query.side_effect = Exception("timeout")
    svc._client = mock_client

    assert svc.get_regime("2024-01-02") is None


def test_get_regime_converts_non_string_values():
    """Enum values returned as int by some ClickHouse drivers should become str."""
    svc = _service()
    # Row with integer values (simulating Enum8 returned as int)
    svc._client = _make_client([(1, 2, 3, 4, 5)])

    result = svc.get_regime("2024-01-02")
    assert result is not None
    assert all(isinstance(v, str) for v in result.values())


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------


def test_close_calls_client_close_and_resets():
    svc = _service()
    mock_client = MagicMock()
    svc._client = mock_client

    svc.close()

    mock_client.close.assert_called_once()
    assert svc._client is None


def test_close_is_idempotent():
    svc = _service()
    svc.close()  # client is None — should not raise
    svc.close()


# ---------------------------------------------------------------------------
# FeatureBarEvent regime payload (Finding 3)
# ---------------------------------------------------------------------------


def test_feature_bar_event_accepts_regime_strings():
    """FeatureBarEvent.features must accept str regime values without validation error."""
    from qs_trader.events.events import FeatureBarEvent

    features = {
        "momentum_score": 0.75,
        "trend_strength": 0.6,
        "trend_regime": "bull",  # string — would fail if type were dict[str, float]
        "vol_regime": "low",
        "composite_regime": "risk_on",
    }
    event = FeatureBarEvent(
        timestamp="2024-01-02T21:00:00+00:00",
        symbol="AAPL",
        features=features,
        feature_set_version="v1",
    )
    assert event.features["trend_regime"] == "bull"
    assert event.features["momentum_score"] == pytest.approx(0.75)


def test_feature_bar_event_round_trips_full_feature_row() -> None:
    """A complete get_features() row (18 floats + 5 regime strings) must survive the event."""
    from qs_trader.events.events import FeatureBarEvent
    from qs_trader.services.features.service import FeatureService

    # Simulate a full get_features() return value
    numeric_cols = FeatureService._FEATURE_COLUMNS[:18]
    regime_cols = FeatureService._FEATURE_COLUMNS[18:]
    features: dict[str, float | str] = {col: 0.5 for col in numeric_cols}
    features.update({col: "bull" for col in regime_cols})

    event = FeatureBarEvent(
        timestamp="2024-01-02T21:00:00+00:00",
        symbol="MSFT",
        features=features,
        feature_set_version="v1",
    )
    assert len(event.features) == len(FeatureService._FEATURE_COLUMNS)
    assert event.features["trend_regime"] == "bull"
    assert isinstance(event.features["trend_strength"], float)


# ---------------------------------------------------------------------------
# stream_universe + FeatureBarEvent end-to-end (Finding 1)
# ---------------------------------------------------------------------------


def test_stream_universe_emits_feature_bar_event() -> None:
    """When feature_service is passed to stream_universe, FeatureBarEvent is published
    with timestamp matching the corresponding PriceBarEvent."""
    from datetime import date
    from decimal import Decimal
    from unittest.mock import MagicMock

    from qs_trader.events.event_bus import EventBus
    from qs_trader.events.events import BaseEvent, FeatureBarEvent, PriceBarEvent
    from qs_trader.services.data.adapters.builtin.clickhouse import ClickhouseBar, ClickhouseDataAdapter
    from qs_trader.services.data.service import DataService

    # ---- set up a mock DataService backed by a stub ClickhouseDataAdapter ----
    trade_date = date(2024, 1, 2)
    bar = ClickhouseBar(
        symbol="AAPL",
        trade_date=trade_date,
        open=Decimal("185.00"),
        high=Decimal("186.00"),
        low=Decimal("184.00"),
        close=Decimal("185.50"),
        open_adj=Decimal("185.00"),
        high_adj=Decimal("186.00"),
        low_adj=Decimal("184.00"),
        close_adj=Decimal("185.50"),
        volume=1_000_000,
    )

    stub_adapter = MagicMock(spec=ClickhouseDataAdapter)
    stub_adapter.read_bars.return_value = iter([bar])
    stub_adapter.get_timestamp.return_value = __import__("datetime").datetime.combine(
        trade_date, __import__("datetime").time(0, 0, 0)
    )
    stub_adapter.to_price_bar_event.return_value = PriceBarEvent(
        symbol="AAPL",
        asset_class="equity",
        interval="1d",
        timestamp="2024-01-02T21:00:00+00:00",
        open=Decimal("185.00"),
        high=Decimal("186.00"),
        low=Decimal("184.00"),
        close=Decimal("185.50"),
        volume=1_000_000,
        source="test",
    )
    stub_adapter.to_corporate_action_event.return_value = None

    event_bus = EventBus()

    # Use monkeypatch-style substitution on the DataService
    svc = DataService.__new__(DataService)
    svc._event_bus = event_bus

    # ---- mock feature service ----
    mock_feature_service = MagicMock()
    mock_feature_service._feature_version = "v1"
    mock_feature_service.get_features.return_value = {
        "trend_strength": 0.8,
        "trend_regime": "bull",
    }

    # ---- collect published events ----
    published: list[FeatureBarEvent] = []

    def handle_feature_bar(event: BaseEvent) -> None:
        assert isinstance(event, FeatureBarEvent)
        published.append(event)

    event_bus.subscribe("feature_bar", handle_feature_bar)

    with patch.object(DataService, "_create_adapter", return_value=stub_adapter):
        svc.stream_universe(
            symbols=["AAPL"],
            start_date=date(2024, 1, 2),
            end_date=date(2024, 1, 2),
            feature_service=mock_feature_service,
        )

    assert len(published) == 1
    fb = published[0]
    assert isinstance(fb, FeatureBarEvent)
    assert fb.symbol == "AAPL"
    assert fb.timestamp == "2024-01-02T21:00:00+00:00"
    assert fb.features["trend_regime"] == "bull"
    assert fb.features["trend_strength"] == pytest.approx(0.8)
    mock_feature_service.get_features.assert_called_once_with("AAPL", "2024-01-02")
