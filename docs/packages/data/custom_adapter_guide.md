# Custom Data Adapter Development Guide

## Overview

QS-Trader uses a plugin architecture for data adapters. This guide explains how to create custom adapters for your own data sources.

## Quick Start

### 1. Create Adapter Class

First, scaffold a custom library or create your own directory structure:

```bash
# Option A: Generate library structure with templates
qs-trader init-library ./library --type adapter

# Option B: Create directory manually
mkdir -p library/adapters
```

Then create your adapter class in `library/adapters/`:

```python
# library/adapters/my_custom_adapter.py

from datetime import datetime
from typing import Iterator, Optional
from qs_trader.events.events import PriceBarEvent, CorporateActionEvent

class MyCustomAdapter:
    """
    Custom adapter for MyDataSource.

    Args:
        config: Data source configuration from data_sources.yaml
        instrument: Instrument to load data for
        dataset_name: Name of the dataset (e.g., "my-custom-us-equity-1d")
    """

    def __init__(self, config: dict, instrument, dataset_name: str):
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name
        # Initialize your data source connection here

    def read_bars(self, start_date: str, end_date: str) -> Iterator:
        """
        Read raw bars from data source.

        Args:
            start_date: Start date (YYYY-MM-DD format)
            end_date: End date (YYYY-MM-DD format)

        Yields:
            Raw bar objects (your custom format)
        """
        # Implement your data loading logic here
        # This should yield raw bar objects from your data source
        pass

    def to_price_bar_event(self, bar) -> PriceBarEvent:
        """
        Convert raw bar to canonical PriceBarEvent.

        Args:
            bar: Raw bar from read_bars()

        Returns:
            PriceBarEvent with standardized fields
        """
        # Convert your bar format to PriceBarEvent
        return PriceBarEvent(
            timestamp=self.get_timestamp(bar),
            symbol=self.instrument.symbol,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            volume=bar.volume,
            # ... other fields
        )

    def to_corporate_action_event(self, bar, prev_bar) -> Optional[CorporateActionEvent]:
        """
        Extract corporate action if present.

        Args:
            bar: Current bar
            prev_bar: Previous bar (for comparison)

        Returns:
            CorporateActionEvent if action detected, None otherwise
        """
        # Implement corporate action detection if your data includes splits/dividends
        return None

    def get_timestamp(self, bar) -> datetime:
        """
        Extract timestamp from raw bar.

        Args:
            bar: Raw bar object

        Returns:
            datetime with timezone
        """
        # Convert bar timestamp to timezone-aware datetime
        pass

    def get_available_date_range(self) -> tuple[Optional[str], Optional[str]]:
        """
        Get min/max dates available for this instrument.

        Returns:
            Tuple of (min_date, max_date) in YYYY-MM-DD format
            Return (None, None) if range is unknown
        """
        # Query your data source for available date range
        return (None, None)
```

### 2. Configure in qs_trader.yaml

Add your adapters directory to system configuration. Set paths to `null` for components you don't need:

```yaml
custom_libraries:
  adapters: "./library/adapters" # Your custom adapter directory
  strategies: null # null = use built-in only
  indicators: null # null = use built-in only
  risk_policies: null # null = use built-in only
  metrics: null # null = use built-in only
```

**Note:** Paths are relative to your project root. Use `null` (not empty string) to disable custom components.

### 3. Configure Data Source

Add your data source to `config/data_sources.yaml`:

```yaml
data_sources:
  my-custom-us-equity-1d:
    # Metadata for DataSourceSelector
    provider: my_provider
    asset_class: equity
    data_type: ohlcv
    frequency: 1d
    region: US
    adjusted: false
    timezone: America/New_York
    price_currency: USD
    price_scale: 2

    # Adapter configuration
    adapter: my_custom # Registry auto-generates from class name

    # Your custom config fields
    api_key: "${MY_API_KEY}" # Environment variable substitution
    api_endpoint: "https://api.example.com"
    cache_path: "data/cache/my_custom"
```

### 4. Use in Code

```python
from qs_trader.services.data.adapters.resolver import DataSourceResolver
from qs_trader.services.data.models import Instrument

# Resolver auto-discovers your adapter
resolver = DataSourceResolver()

# Resolve by dataset name
instrument = Instrument(symbol="AAPL")
adapter = resolver.resolve_by_dataset("my-custom-us-equity-1d", instrument)

# Stream bars
for bar in adapter.read_bars("2023-01-01", "2023-12-31"):
    event = adapter.to_price_bar_event(bar)
    print(event)
```

## IDataAdapter Protocol

Your adapter class must implement these methods:

| Method                                 | Required | Description                      |
| -------------------------------------- | -------- | -------------------------------- |
| `read_bars(start, end)`                | ✅       | Yield raw bars from data source  |
| `to_price_bar_event(bar)`              | ✅       | Convert raw bar to PriceBarEvent |
| `to_corporate_action_event(bar, prev)` | ✅       | Extract corporate actions        |
| `get_timestamp(bar)`                   | ✅       | Extract timestamp from bar       |
| `get_available_date_range()`           | ✅       | Get min/max dates available      |

### Optional Methods

| Method                    | Description              |
| ------------------------- | ------------------------ |
| `prime_cache(start, end)` | Pre-load data into cache |
| `write_cache(start, end)` | Write data to cache      |
| `invalidate_cache()`      | Clear cache              |

## Registry Name Generation

The registry automatically generates names from class names:

| Class Name            | Registry Name   |
| --------------------- | --------------- |
| `MyCustomAdapter`     | `my_custom`     |
| `BinanceOHLCAdapter`  | `binance_ohlc`  |
| `PostgresDataAdapter` | `postgres_data` |
| `IEXCloudAdapter`     | `iex_cloud`     |

Rules:

1. Remove suffixes: `Adapter`, `VendorAdapter`, `DataAdapter`
1. Convert CamelCase to snake_case
1. Clean up multiple underscores

## Best Practices

### 1. Streaming Over Materialization

```python
# ✅ Good - streaming iterator
def read_bars(self, start_date, end_date):
    for row in self.data_source.query(start_date, end_date):
        yield self._parse_row(row)

# ❌ Bad - load everything into memory
def read_bars(self, start_date, end_date):
    df = self.data_source.load_all()
    return df.to_dict('records')
```

### 2. Proper Timezone Handling

```python
from zoneinfo import ZoneInfo

# ✅ Good - timezone-aware
def get_timestamp(self, bar):
    tz = ZoneInfo(self.config.get('timezone', 'America/New_York'))
    return datetime.combine(bar.date, bar.time, tzinfo=tz)

# ❌ Bad - naive datetime
def get_timestamp(self, bar):
    return datetime(bar.year, bar.month, bar.day)
```

### 3. Environment Variable Usage

```python
# config/data_sources.yaml
my-source:
  adapter: my_custom
  api_key: "${MY_API_KEY}"  # From environment
  api_secret: "${MY_API_SECRET}"

# my_library/adapters/my_custom_adapter.py
def __init__(self, config, instrument, dataset_name):
    # Config already has env vars substituted
    self.api_key = config['api_key']
```

### 4. Error Handling

```python
def read_bars(self, start_date, end_date):
    try:
        for bar in self._fetch_data(start_date, end_date):
            yield bar
    except ConnectionError as e:
        logger.error(f"Data source connection failed: {e}")
        raise
    except ValueError as e:
        logger.warning(f"Invalid bar data: {e}")
        # Continue streaming, skip invalid bars
```

### 5. Caching (Optional)

```python
def prime_cache(self, start_date: str, end_date: str) -> int:
    """Pre-load data into cache for faster access."""
    cache_path = Path(self.config['cache_path'])
    cache_path.mkdir(parents=True, exist_ok=True)

    count = 0
    for bar in self.read_bars(start_date, end_date):
        # Write to cache
        cache_file = cache_path / f"{bar.date}.parquet"
        self._write_bar(cache_file, bar)
        count += 1

    return count
```

## Examples

### CSV Adapter

```python
import csv
from pathlib import Path

class CSVAdapter:
    def __init__(self, config, instrument, dataset_name):
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name

        # Build file path from template
        path_template = config['path_template']
        self.file_path = Path(path_template.format(
            symbol=instrument.symbol,
            **config
        ))

    def read_bars(self, start_date, end_date):
        with open(self.file_path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                if start_date <= row['date'] <= end_date:
                    yield row

    def to_price_bar_event(self, bar):
        from decimal import Decimal
        return PriceBarEvent(
            timestamp=self.get_timestamp(bar),
            symbol=self.instrument.symbol,
            open=Decimal(bar['open']),
            high=Decimal(bar['high']),
            low=Decimal(bar['low']),
            close=Decimal(bar['close']),
            volume=int(bar['volume']),
        )

    # ... other methods
```

### Database Adapter

```python
import psycopg2

class PostgresAdapter:
    def __init__(self, config, instrument, dataset_name):
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name

        # Connect to database
        self.conn = psycopg2.connect(config['connection_string'])

    def read_bars(self, start_date, end_date):
        cursor = self.conn.cursor()
        query = """
            SELECT date, open, high, low, close, volume
            FROM ohlc_bars
            WHERE symbol = %s
              AND date BETWEEN %s AND %s
            ORDER BY date
        """
        cursor.execute(query, (self.instrument.symbol, start_date, end_date))

        for row in cursor:
            yield {
                'date': row[0],
                'open': row[1],
                'high': row[2],
                'low': row[3],
                'close': row[4],
                'volume': row[5],
            }

    # ... other methods
```

### REST API Adapter

```python
import requests

class APIAdapter:
    def __init__(self, config, instrument, dataset_name):
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name

        self.api_key = config['api_key']
        self.base_url = config['api_endpoint']

    def read_bars(self, start_date, end_date):
        headers = {'Authorization': f'Bearer {self.api_key}'}
        params = {
            'symbol': self.instrument.symbol,
            'start': start_date,
            'end': end_date,
        }

        response = requests.get(
            f"{self.base_url}/bars",
            headers=headers,
            params=params,
            stream=True  # Stream large responses
        )
        response.raise_for_status()

        for bar in response.json()['bars']:
            yield bar

    # ... other methods
```

## Testing Your Adapter

```python
# test_my_adapter.py
from my_library.adapters.my_custom_adapter import MyCustomAdapter
from qs_trader.services.data.models import Instrument

def test_adapter():
    config = {
        'api_key': 'test_key',
        'timezone': 'America/New_York',
    }
    instrument = Instrument(symbol='AAPL')
    adapter = MyCustomAdapter(config, instrument, 'test-dataset')

    # Test bar reading
    bars = list(adapter.read_bars('2023-01-01', '2023-01-10'))
    assert len(bars) > 0

    # Test event conversion
    event = adapter.to_price_bar_event(bars[0])
    assert event.symbol == 'AAPL'
    assert event.open > 0

    print("✓ All tests passed!")

if __name__ == '__main__':
    test_adapter()
```

## Troubleshooting

### Adapter Not Found

```python
# Error: ComponentNotFoundError: adapter 'my_custom' not found

# Check:
# 1. Adapter class implements all required methods
# 2. File is in my_library/adapters/ directory
# 3. qs_trader.yaml has correct adapters path
# 4. Class name matches expected registry name

# Debug:
from qs_trader.libraries.registry import get_adapter_registry
registry = get_adapter_registry()
registry.discover(custom_path='my_library/adapters')
print(registry.list_components())  # Should show your adapter
```

### Import Errors

```python
# Error: ModuleNotFoundError: No module named 'my_library'

# Fix: Ensure project root is in PYTHONPATH
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
```

### Circular Imports

```python
# Error: ImportError: cannot import name 'X' from partially initialized module

# Fix: Use lazy imports in __init__ methods
def __init__(self, config, instrument, dataset_name):
    # ✅ Good - lazy import
    from my_library.adapters.models import MyBar
    self.bar_class = MyBar

# ❌ Bad - top-level import
from my_library.adapters.models import MyBar  # Can cause circular import
```

## See Also

- [Built-in Yahoo CSV Adapter](../../src/qs_trader/services/data/adapters/builtin/yahoo_csv.py)
- [IDataAdapter Protocol](../../src/qs_trader/services/data/adapters/protocol.py)
- [Data Sources Configuration](../../config/data_sources.yaml)
- [Implementation Details](ADAPTER_PLUGIN_IMPLEMENTATION.md)
