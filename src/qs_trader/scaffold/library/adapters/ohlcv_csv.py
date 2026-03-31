"""
OHLCV CSV Data Adapter.

Reads per-symbol CSV files with simple OHLCV format:
Date,Open,High,Low,Close,Volume

This adapter is designed for custom CSV data sources where:
- Each symbol has its own CSV file
- File name is the ticker symbol (e.g., AAPL.csv)
- No adjustment factors are provided (split-adjusted prices only)
- Simple OHLCV columns without additional metadata

CSV Format:
    Date,Open,High,Low,Close,Volume
    1/2/2020,74.06,75.15,73.80,75.09,135480400
    1/3/2020,74.29,75.14,74.13,74.36,146322800

Usage:
    This adapter is automatically discoverable by QS-Trader's data service.
    Configure it in config/data_sources.yaml:

    data_sources:
      - name: "my-custom-csv-1d"
        adapter: "ohlcvcsv"  # Registry name auto-derived from class name
        config:
          root_path: "data/sample-csv"
          path_template: "{root_path}/{symbol}.csv"
          timezone: "America/New_York"
          asset_class: "equity"
          exchange: "NASDAQ"
          price_currency: "USD"
          price_scale: 2

Features:
    - Streaming CSV reader (no full file load)
    - Simple OHLCV format
    - Split-adjusted prices only
    - No corporate action extraction
    - No caching support (designed for small datasets)

Limitations:
    - No Adj Close or adjustment factors
    - No corporate action detection
    - No incremental updates (no caching)
    - Daily frequency only
    - Assumes US date format (M/D/YYYY)
"""

import csv
from datetime import datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Optional, Tuple

import pytz

from qs_trader.events.events import CorporateActionEvent, PriceBarEvent
from qs_trader.scaffold.library.adapters.models.ohlcv_csv import OHLCVBar


class OHLCVCSVAdapter:
    """
    OHLCV CSV data adapter for simple custom CSV files.

    Reads CSV files with format: Date,Open,High,Low,Close,Volume
    File naming: {symbol}.csv (e.g., AAPL.csv)

    Args:
        config: Configuration dictionary containing:
            - root_path: Root directory containing CSV files (required)
            - path_template: Path template for CSV files (required)
            - timezone: Market timezone (default: "America/New_York")
            - asset_class: Asset class (default: "equity")
            - exchange: Exchange name (default: "NASDAQ")
            - price_currency: Price currency (default: "USD")
            - price_scale: Decimal places for prices (default: 2)
        instrument: Instrument object with symbol attribute
        dataset_name: Optional dataset name for logging

    Example:
        >>> from qs_trader.services.data.models import Instrument
        >>> config = {
        ...     "root_path": "data/sample-csv",
        ...     "path_template": "{root_path}/{symbol}.csv",
        ...     "timezone": "America/New_York",
        ...     "price_scale": 2
        ... }
        >>> instrument = Instrument(symbol="AAPL")
        >>> adapter = OHLCVCSVAdapter(config, instrument)
        >>> for bar in adapter.read_bars("2020-01-01", "2020-12-31"):
        ...     print(f"{bar.date}: {bar.close}")
    """

    def __init__(self, config: dict, instrument, dataset_name: Optional[str] = None):
        """
        Initialize OHLCV CSV adapter.

        Args:
            config: Configuration dictionary with adapter settings
            instrument: Instrument object with symbol attribute
            dataset_name: Optional dataset name for logging
        """
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name or "ohlcv-csv"

        # Validate required config keys
        required_keys = ["root_path", "path_template"]
        missing = [k for k in required_keys if k not in config]
        if missing:
            raise ValueError(f"Missing required config keys: {missing}")

        # Extract symbol from instrument
        self.symbol = instrument.symbol

        # Extract config values with defaults
        root_path = config["root_path"]
        path_template = config["path_template"]
        timezone = config.get("timezone", "America/New_York")
        self.asset_class = config.get("asset_class", "equity")
        self.exchange = config.get("exchange", "NASDAQ")
        self.price_currency = config.get("price_currency", "USD")
        self.price_scale = int(config.get("price_scale", 2))

        # Setup paths
        self.root_path = Path(root_path).expanduser()
        self.path_template = path_template
        self.timezone = pytz.timezone(timezone)

        # Construct file path
        self.file_path = Path(
            path_template.format(
                root_path=str(self.root_path),
                symbol=self.symbol,
            )
        )

        # Validate file exists
        if not self.file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {self.file_path}")

    def read_bars(self, start_date: str, end_date: str) -> Iterator[OHLCVBar]:
        """
        Stream bars from CSV file.

        Args:
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD), inclusive

        Yields:
            OHLCVBar objects in chronological order
        """
        # Parse date range
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)

        # Use utf-8-sig to handle BOM (Byte Order Mark) if present
        with open(self.file_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)

            for row in reader:
                try:
                    # Parse date (handle M/D/YYYY format)
                    date_str = row["Date"]
                    bar_date = datetime.strptime(date_str, "%m/%d/%Y")

                    # Filter by date range
                    if bar_date < start_dt or bar_date > end_dt:
                        continue

                    # Parse OHLCV values
                    bar = OHLCVBar(
                        date=bar_date,
                        open=Decimal(row["Open"]),
                        high=Decimal(row["High"]),
                        low=Decimal(row["Low"]),
                        close=Decimal(row["Close"]),
                        volume=int(float(row["Volume"])),
                        symbol=self.symbol,
                    )

                    yield bar

                except (KeyError, ValueError):
                    # Skip malformed rows
                    continue

    def to_price_bar_event(self, bar: OHLCVBar) -> PriceBarEvent:
        """
        Convert OHLCVBar to standardized PriceBarEvent.

        Args:
            bar: OHLCVBar from CSV

        Returns:
            PriceBarEvent with standardized fields
        """
        # Create timestamp at market close (4:00 PM ET)
        close_time = time(16, 0, 0)
        timestamp = datetime.combine(bar.date.date(), close_time)
        timestamp = self.timezone.localize(timestamp)

        # Quantize prices to configured scale
        quantize_value = Decimal(10) ** -self.price_scale

        return PriceBarEvent(
            timestamp=timestamp.isoformat(),  # ISO8601 string required
            symbol=bar.symbol,
            # Split-adjusted OHLCV (no adjustment factors in this CSV format)
            open=bar.open.quantize(quantize_value),
            high=bar.high.quantize(quantize_value),
            low=bar.low.quantize(quantize_value),
            close=bar.close.quantize(quantize_value),
            volume=bar.volume,
            # No total-return adjusted prices available
            open_adj=None,
            high_adj=None,
            low_adj=None,
            close_adj=None,
            # Metadata
            asset_class=self.asset_class,
            price_currency=self.price_currency,
            price_scale=self.price_scale,
            interval="1d",
            timezone=self.timezone.zone,
            source=f"ohlcv_csv:{self.root_path.name}",
        )

    def to_corporate_action_event(
        self, bar: OHLCVBar, prev_bar: Optional[OHLCVBar] = None
    ) -> Optional[CorporateActionEvent]:
        """
        Extract corporate action from bar (not supported for simple CSV).

        This CSV format does not include adjustment factors or corporate action data,
        so this method always returns None.

        Args:
            bar: Current bar
            prev_bar: Previous bar (unused)

        Returns:
            None (no corporate actions in this format)
        """
        return None

    def get_timestamp(self, bar: OHLCVBar) -> datetime:
        """
        Extract timestamp from bar for synchronization.

        Args:
            bar: OHLCVBar from CSV

        Returns:
            Bar's timestamp (market close time)
        """
        # Use market close time for synchronization
        close_time = time(16, 0, 0)
        timestamp = datetime.combine(bar.date.date(), close_time)
        return self.timezone.localize(timestamp)

    def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get available date range for this instrument.

        Returns:
            Tuple of (min_date, max_date) in ISO format

        Note:
            This reads the entire file to find min/max dates.
            For large files, consider caching this result.
        """
        try:
            dates = []
            # Use utf-8-sig to handle BOM (Byte Order Mark) if present
            with open(self.file_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        date_str = row["Date"]
                        bar_date = datetime.strptime(date_str, "%m/%d/%Y")
                        dates.append(bar_date)
                    except (KeyError, ValueError):
                        continue

            if not dates:
                return None, None

            min_date = min(dates).strftime("%Y-%m-%d")
            max_date = max(dates).strftime("%Y-%m-%d")
            return min_date, max_date

        except Exception:
            return None, None

    def prime_cache(self, start_date: str, end_date: str) -> int:
        """
        Prime cache (not supported for simple CSV adapter).

        This adapter is designed for small datasets and does not support caching.
        CSV files are read directly on each request.

        Raises:
            NotImplementedError: Always raised (caching not supported)
        """
        raise NotImplementedError(
            "OHLCVCSVAdapter does not support caching. "
            "This adapter is designed for small datasets that are read directly from CSV files. "
            "For large datasets requiring caching, consider using a database-backed adapter "
            "or implementing your own caching layer."
        )

    def write_cache(self, bars: list[OHLCVBar]) -> None:
        """
        Write cache (not supported for simple CSV adapter).

        This adapter is designed for small datasets and does not support caching.

        Raises:
            NotImplementedError: Always raised (caching not supported)
        """
        raise NotImplementedError(
            "OHLCVCSVAdapter does not support caching. "
            "This adapter reads directly from CSV files without intermediate caching."
        )

    def update_to_latest(self, dry_run: bool = False) -> Tuple[int, str, str]:
        """
        Update cache to latest (not supported for simple CSV adapter).

        This adapter does not support incremental updates since it has no caching.

        Raises:
            NotImplementedError: Always raised (updates not supported)
        """
        raise NotImplementedError(
            "OHLCVCSVAdapter does not support incremental updates. "
            "This adapter reads directly from static CSV files. "
            "To update data, replace the CSV files manually and restart the backtest."
        )


# ============================================================================
# USAGE EXAMPLES
# ============================================================================
#
# 1. Configure in data_sources.yaml:
#
#    data_sources:
#      - name: "my-custom-csv-1d"
#        adapter: "ohlcv_csv"  # Auto-discovered by registry
#        config:
#          root_path: "data/sample-csv"
#          path_template: "{root_path}/{symbol}.csv"
#          timezone: "America/New_York"
#          asset_class: "equity"
#          exchange: "NASDAQ"
#          price_currency: "USD"
#          price_scale: 2
#
# 2. Use in backtest config (experiments/*.yaml):
#
#    data:
#      sources:
#        - name: "my-custom-csv-1d"
#          universe: ["AAPL", "MSFT", "GOOGL"]
#
# 3. Direct usage (for testing):
#
#    >>> from qs_trader.services.data.models import Instrument
#    >>> config = {
#    ...     "root_path": "data/sample-csv",
#    ...     "path_template": "{root_path}/{symbol}.csv",
#    ...     "price_scale": 2
#    ... }
#    >>> instrument = Instrument(symbol="AAPL")
#    >>> adapter = OHLCVCSVAdapter(config, instrument)
#    >>> for bar in adapter.read_bars("2020-01-01", "2020-12-31"):
#    ...     event = adapter.to_price_bar_event(bar)
#    ...     print(f"{event.timestamp}: {event.close}")
#
# ============================================================================
