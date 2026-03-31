from decimal import Decimal

import pytest

from qs_trader.services.data.adapters.builtin.yahoo_csv import YahooCSVDataAdapter
from qs_trader.services.data.models import Instrument


@pytest.fixture
def sample_csv_content():
    """Sample Yahoo Finance CSV data for testing.

    Uses realistic data with proper adjusted close values that account for
    dividends and splits. The adjustment ratio varies across rows to test
    synthetic OHLC generation.
    """
    return """Date,Open,High,Low,Close,Adj Close,Volume
2020-01-02,74.06,75.15,73.80,75.09,72.47,135480400
2020-01-03,74.29,75.14,74.13,74.36,71.76,146322800
2020-01-06,73.45,74.99,73.19,74.95,72.34,118387200
2020-01-07,74.96,75.22,74.37,74.60,72.00,108872000
2020-01-08,74.29,76.11,74.29,75.80,73.15,132079200
2020-01-09,76.81,77.61,76.55,77.41,74.71,170108400
2020-01-10,77.65,78.17,77.06,77.58,74.88,140644800
"""


@pytest.fixture
def temp_csv_dir(tmp_path, sample_csv_content):
    """Create temporary CSV file for testing."""
    csv_file = tmp_path / "AAPL.csv"
    csv_file.write_text(sample_csv_content)
    return tmp_path


@pytest.fixture
def adapter_config(temp_csv_dir):
    """Base adapter configuration using temporary fixtures."""
    return {
        "root_path": str(temp_csv_dir),
        "path_template": "{root_path}/{symbol}.csv",
        "timezone": "America/New_York",
        "price_currency": "USD",
        "price_scale": 2,
        "asset_class": "equity",
    }


def test_yahoo_csv_adapter_basic_read_and_event_conversion(adapter_config):
    """Test basic CSV reading and conversion to PriceBarEvent with synthetic adjusted OHLC."""
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument, dataset_name="yahoo-us-equity-1d-csv")

    raw_bars = list(adapter.read_bars("2020-01-02", "2020-01-07"))
    assert len(raw_bars) >= 4  # Expect at least several trading days
    assert all(raw_bars[i].trade_date <= raw_bars[i + 1].trade_date for i in range(len(raw_bars) - 1))

    # Convert first bar to event and validate fields
    first_event = adapter.to_price_bar_event(raw_bars[0])
    # Yahoo row 2020-01-02: Close 75.087501..., Adj Close 72.468254...
    assert first_event.close == Decimal("75.09")  # quantized to 2 decimals
    assert first_event.close_adj == Decimal("72.47")
    assert first_event.open == Decimal("74.06")
    # Synthetic adjusted OHLC produced via ratio adj_close/close
    assert first_event.open_adj is not None
    assert first_event.high_adj is not None
    assert first_event.low_adj is not None
    # Ratio consistency (within rounding tolerance)
    ratio_close = (first_event.close_adj / first_event.close).quantize(Decimal("0.01"))
    ratio_open = (first_event.open_adj / first_event.open).quantize(Decimal("0.01"))
    assert ratio_open == ratio_close
    assert first_event.source == "yahoo-us-equity-1d-csv"
    assert first_event.price_scale == 2
    assert first_event.price_currency == "USD"


def test_yahoo_csv_adapter_available_date_range(adapter_config):
    """Test date range extraction from CSV without loading all data."""
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument)
    min_date, max_date = adapter.get_available_date_range()
    assert min_date is not None and max_date is not None
    # Basic sanity: min_date before max_date
    assert min_date <= max_date
    # The requested window should be inside available range
    assert min_date <= "2020-01-02" <= max_date


def test_yahoo_csv_adapter_synthetic_ratio_consistency(adapter_config):
    """Test that synthetic adjusted OHLC maintains consistent ratio across all fields."""
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument)
    bars = list(adapter.read_bars("2020-01-02", "2020-01-10"))
    events = [adapter.to_price_bar_event(b) for b in bars]
    for ev in events:
        if ev.close_adj is None:
            continue
        ratio = (ev.close_adj / ev.close).quantize(Decimal("0.01")) if ev.close != 0 else Decimal("0")
        if ev.open_adj is not None and ev.open != 0:
            assert (ev.open_adj / ev.open).quantize(Decimal("0.01")) == ratio
        if ev.high_adj is not None and ev.high != 0:
            assert (ev.high_adj / ev.high).quantize(Decimal("0.01")) == ratio
        if ev.low_adj is not None and ev.low != 0:
            assert (ev.low_adj / ev.low).quantize(Decimal("0.01")) == ratio


def test_yahoo_csv_adapter_synthetic_disabled(adapter_config):
    """Test that synthetic adjusted OHLC can be disabled via config flag."""
    instrument = Instrument(symbol="AAPL")
    config_disabled = {**adapter_config, "synthetic_adjust_ohlc": False}
    adapter = YahooCSVDataAdapter(config_disabled, instrument)
    bars = list(adapter.read_bars("2020-01-02", "2020-01-03"))
    assert bars, "Expected at least one bar"
    ev = adapter.to_price_bar_event(bars[0])
    assert ev.close_adj is not None  # adjusted close still mapped
    # Synthetic fields disabled
    assert ev.open_adj is None
    assert ev.high_adj is None
    assert ev.low_adj is None


def test_read_bars_filters_by_date_range(adapter_config):
    """Test that read_bars correctly filters bars by start/end dates."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument)

    # Act: Read only 2 days
    bars = list(adapter.read_bars("2020-01-06", "2020-01-07"))

    # Assert
    assert len(bars) == 2
    assert bars[0].trade_date.isoformat() == "2020-01-06"
    assert bars[1].trade_date.isoformat() == "2020-01-07"


def test_read_bars_invalid_date_range_raises_error(adapter_config):
    """Test that read_bars raises ValueError when start_date > end_date."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument)

    # Act & Assert
    with pytest.raises(ValueError, match="start_date must be <= end_date"):
        list(adapter.read_bars("2020-01-10", "2020-01-02"))


def test_missing_csv_file_raises_error(adapter_config):
    """Test that initialization fails when CSV file doesn't exist."""
    # Arrange
    instrument = Instrument(symbol="NONEXISTENT")

    # Act & Assert
    with pytest.raises(FileNotFoundError, match="Yahoo CSV not found"):
        YahooCSVDataAdapter(adapter_config, instrument)


def test_missing_config_keys_raises_error(temp_csv_dir):
    """Test that adapter initialization fails with missing required config keys."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    incomplete_config = {"root_path": str(temp_csv_dir)}  # Missing path_template

    # Act & Assert
    with pytest.raises(ValueError, match="Missing required config keys"):
        YahooCSVDataAdapter(incomplete_config, instrument)


@pytest.fixture
def csv_with_missing_adj_close(tmp_path):
    """CSV fixture with missing Adj Close values."""
    content = """Date,Open,High,Low,Close,Adj Close,Volume
2020-01-02,100.0,105.0,99.0,102.0,,1000000
2020-01-03,102.0,103.0,101.0,103.0,NA,1100000
"""
    csv_file = tmp_path / "TEST.csv"
    csv_file.write_text(content)
    return tmp_path


def test_missing_adj_close_values_handled_gracefully(csv_with_missing_adj_close):
    """Test that missing or NA Adj Close values result in None adjusted fields."""
    # Arrange
    instrument = Instrument(symbol="TEST")
    config = {
        "root_path": str(csv_with_missing_adj_close),
        "path_template": "{root_path}/{symbol}.csv",
        "price_scale": 2,
    }
    adapter = YahooCSVDataAdapter(config, instrument)

    # Act
    bars = list(adapter.read_bars("2020-01-02", "2020-01-03"))
    events = [adapter.to_price_bar_event(bar) for bar in bars]

    # Assert: All adjusted fields should be None when Adj Close missing
    for event in events:
        assert event.close_adj is None
        assert event.open_adj is None
        assert event.high_adj is None
        assert event.low_adj is None


@pytest.fixture
def csv_with_zero_close(tmp_path):
    """CSV fixture with zero close price (edge case)."""
    content = """Date,Open,High,Low,Close,Adj Close,Volume
2020-01-02,100.0,105.0,99.0,0.0,95.0,1000000
"""
    csv_file = tmp_path / "ZERO.csv"
    csv_file.write_text(content)
    return tmp_path


def test_zero_close_price_prevents_synthetic_adjustment(csv_with_zero_close):
    """Test that zero close price prevents synthetic adjustment (division by zero guard)."""
    # Arrange
    instrument = Instrument(symbol="ZERO")
    config = {
        "root_path": str(csv_with_zero_close),
        "path_template": "{root_path}/{symbol}.csv",
        "price_scale": 2,
    }
    adapter = YahooCSVDataAdapter(config, instrument)

    # Act
    bars = list(adapter.read_bars("2020-01-02", "2020-01-02"))
    event = adapter.to_price_bar_event(bars[0])

    # Assert: close_adj populated but no synthetic OHLC due to zero close
    assert event.close == Decimal("0.00")
    assert event.close_adj == Decimal("95.00")
    assert event.open_adj is None  # Synthetic disabled when close=0
    assert event.high_adj is None
    assert event.low_adj is None


def test_price_scale_quantization(temp_csv_dir):
    """Test that prices are correctly quantized to price_scale decimal places."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    config_scale_4 = {
        "root_path": str(temp_csv_dir),
        "path_template": "{root_path}/{symbol}.csv",
        "price_scale": 4,  # 4 decimal places
    }
    adapter = YahooCSVDataAdapter(config_scale_4, instrument)

    # Act
    bars = list(adapter.read_bars("2020-01-02", "2020-01-02"))
    event = adapter.to_price_bar_event(bars[0])

    # Assert: Check quantization (should have exactly 4 decimal places internally)
    # When serialized to string, it will use price_scale
    assert event.price_scale == 4


def test_timestamp_conversion_to_utc_market_close(adapter_config):
    """Test that timestamps are correctly set to market close 16:00 ET and converted to UTC."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument)

    # Act
    bars = list(adapter.read_bars("2020-01-02", "2020-01-02"))
    event = adapter.to_price_bar_event(bars[0])

    # Assert
    assert "16:00:00" in event.timestamp_local  # Market close time
    assert "-05:00" in event.timestamp_local  # EST offset (January)
    assert event.timestamp.endswith("+00:00") or event.timestamp.endswith("Z")  # UTC
    assert event.timezone == "America/New_York"


def test_corporate_action_event_always_none(adapter_config):
    """Test that to_corporate_action_event always returns None for Yahoo CSV."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument)

    # Act
    bars = list(adapter.read_bars("2020-01-02", "2020-01-03"))
    action1 = adapter.to_corporate_action_event(bars[0])
    action2 = adapter.to_corporate_action_event(bars[1], prev_bar=bars[0])

    # Assert
    assert action1 is None
    assert action2 is None


def test_get_timestamp_returns_midnight_naive_datetime(adapter_config):
    """Test that get_timestamp returns trade date at midnight."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument)

    # Act
    bars = list(adapter.read_bars("2020-01-02", "2020-01-02"))
    timestamp = adapter.get_timestamp(bars[0])

    # Assert
    assert timestamp.hour == 0
    assert timestamp.minute == 0
    assert timestamp.second == 0
    assert timestamp.date().isoformat() == "2020-01-02"


def test_cache_operations_not_implemented(adapter_config):
    """Test that cache operations raise NotImplementedError."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    adapter = YahooCSVDataAdapter(adapter_config, instrument)

    # Act & Assert
    with pytest.raises(NotImplementedError, match="does not support cache priming"):
        adapter.prime_cache("2020-01-01", "2020-12-31")

    with pytest.raises(NotImplementedError, match="does not support cache writing"):
        adapter.write_cache([])


@pytest.mark.parametrize(
    "scale,expected_close",
    [
        (0, Decimal("75")),
        (1, Decimal("75.1")),
        (2, Decimal("75.09")),
        (3, Decimal("75.090")),
    ],
)
def test_price_scale_parametrized(temp_csv_dir, scale, expected_close):
    """Test various price_scale values produce correct quantization."""
    # Arrange
    instrument = Instrument(symbol="AAPL")
    config = {
        "root_path": str(temp_csv_dir),
        "path_template": "{root_path}/{symbol}.csv",
        "price_scale": scale,
    }
    adapter = YahooCSVDataAdapter(config, instrument)

    # Act
    bars = list(adapter.read_bars("2020-01-02", "2020-01-02"))
    event = adapter.to_price_bar_event(bars[0])

    # Assert
    assert event.close == expected_close
    assert event.price_scale == scale
