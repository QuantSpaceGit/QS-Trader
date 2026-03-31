# Custom Data Adapters

Custom data adapters for loading data from various sources into QS-Trader.

## Overview

QS-Trader's data service uses a registry-based discovery system to find and load adapters. Any class in this directory that:

1. Follows the `IDataAdapter` protocol
1. Has a class name ending with "Adapter"
1. Is exported in `__init__.py`

Will be automatically discovered and available for use.

## Available Adapters

### OHLCVCSVAdapter

Simple CSV adapter for OHLCV data without adjustment factors.

**CSV Format:**

```csv
Date,Open,High,Low,Close,Volume
1/2/2020,74.06,75.15,73.80,75.09,135480400
1/3/2020,74.29,75.14,74.13,74.36,146322800
```

**Features:**

- One CSV file per symbol (filename = ticker)
- Split-adjusted prices only
- No corporate action extraction
- UTF-8 with BOM support
- Date format: M/D/YYYY

**Configuration:**

In `config/data_sources.yaml`:

```yaml
data_sources:
  - name: "my-custom-csv-1d"
    adapter: "ohlcv_csv"  # Registry name (auto-derived from OHLCVCSVAdapter)
    config:
      root_path: "data/sample-csv"
      path_template: "{root_path}/{symbol}.csv"
      timezone: "America/New_York"
      asset_class: "equity"
      exchange: "NASDAQ"
      price_currency: "USD"
      price_scale: 2
```

In backtest config (`experiments/*.yaml`):

```yaml
data:
  sources:
    - name: "my-custom-csv-1d"
      universe: ["AAPL", "MSFT", "GOOGL"]
```

**Limitations:**

- No caching support (designed for small datasets)
- No incremental updates
- Daily frequency only
- No Adj Close or adjustment factors

## Creating Custom Adapters

To create a new adapter:

1. **Create adapter class:**

```python
# my_adapter.py
from typing import Iterator, Optional, Tuple
from datetime import datetime
from qs_trader.events.events import PriceBarEvent, CorporateActionEvent

class MyCustomAdapter:
    """My custom data adapter."""

    def __init__(self, config: dict, instrument, dataset_name: Optional[str] = None):
        """
        Initialize adapter with config and instrument.

        Args:
            config: Configuration dictionary from data_sources.yaml
            instrument: Instrument object with symbol attribute
            dataset_name: Optional dataset name for logging
        """
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name or "my-custom-adapter"
        self.symbol = instrument.symbol

        # Validate required config keys
        required_keys = ["root_path"]  # Add your required keys
        missing = [k for k in required_keys if k not in config]
        if missing:
            raise ValueError(f"Missing required config keys: {missing}")

        # Extract config values
        # ... initialize from config

    def read_bars(self, start_date: str, end_date: str) -> Iterator:
        """Read raw bars from data source."""
        # ... yield bars

    def to_price_bar_event(self, bar) -> PriceBarEvent:
        """Convert raw bar to PriceBarEvent."""
        # ... convert to event

    def to_corporate_action_event(self, bar, prev_bar=None) -> Optional[CorporateActionEvent]:
        """Extract corporate action (if supported)."""
        return None

    def get_timestamp(self, bar) -> datetime:
        """Extract timestamp for synchronization."""
        # ... return timestamp

    def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        """Get (min_date, max_date) in ISO format."""
        # ... return range
```

1. **Export in `__init__.py`:**

```python
from qs_trader.scaffold.library.adapters.my_adapter import MyCustomAdapter

__all__ = ["MyCustomAdapter", "OHLCVCSVAdapter"]
```

1. **Configure in `config/data_sources.yaml`:**

```yaml
data_sources:
  - name: "my-data-source"
    adapter: "my_custom"  # snake_case of MyCustomAdapter
    config:
      # ... adapter-specific config
```

## IDataAdapter Protocol

All adapters must implement these methods:

### Required Methods

- `__init__(config: dict, instrument, dataset_name: Optional[str] = None)`: Initialize with config and instrument
- `read_bars(start_date: str, end_date: str) -> Iterator`: Stream bars
- `to_price_bar_event(bar) -> PriceBarEvent`: Convert to standardized event
- `get_timestamp(bar) -> datetime`: Extract timestamp for sync
- `get_available_date_range() -> Tuple[str, str]`: Get data range

### Optional Methods

- `to_corporate_action_event(bar, prev_bar) -> Optional[CorporateActionEvent]`: Extract corporate action
- `prime_cache(start_date: str, end_date: str) -> int`: Prime cache (if supported)
- `write_cache(bars: list) -> None`: Write cache (if supported)
- `update_to_latest(dry_run: bool) -> Tuple[int, str, str]`: Update cache (if supported)

## Naming Convention

The registry automatically derives the adapter name from the class name:

- Class: `YahooCSVAdapter` → Registry: `yahoo_csv`
- Class: `OHLCVCSVAdapter` → Registry: `ohlcv_csv`
- Class: `MyCustomAdapter` → Registry: `my_custom`

## Best Practices

1. **Use streaming iterators** for `read_bars()` to avoid loading entire files
1. **Validate data** and skip malformed rows gracefully
1. **Handle edge cases**: BOM markers, different encodings, date formats
1. **Document CSV format** clearly in docstrings
1. **Quantize prices** to configured scale for consistency
1. **Use ISO8601 strings** for timestamps in events
1. **Support timezone configuration** for proper timestamp localization

## Testing

Test your adapter:

```python
from qs_trader.services.data.models import Instrument

# Test basic functionality
config = {
    "root_path": "data/test-csv",
    "path_template": "{root_path}/{symbol}.csv"
}
instrument = Instrument(symbol="AAPL")
adapter = MyCustomAdapter(config, instrument)

# Test date range
date_range = adapter.get_available_date_range()
print(f"Available: {date_range}")

# Test reading bars
for bar in adapter.read_bars("2020-01-01", "2020-12-31"):
    event = adapter.to_price_bar_event(bar)
    print(f"{event.timestamp}: {event.close}")
```

## References

- Protocol definition: `src/qs_trader/services/data/adapters/protocol.py`
- Reference implementation: `src/qs_trader/services/data/adapters/builtin/yahoo_csv.py`
- Event schemas: `src/qs_trader/contracts/schemas/data/bar.v1.json`
