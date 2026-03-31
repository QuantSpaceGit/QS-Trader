"""Custom data adapter template.

See src/qs_trader/services/data/adapters/builtin/yahoo_csv.py for complete example.
"""

from datetime import datetime
from pathlib import Path

import pandas as pd


class MyDataAdapter:
    """
    Example data adapter implementation.

    Rename this class and implement your data loading logic.

    Data adapters implement the IDataAdapter protocol to integrate with QS-Trader.
    The protocol requires:
    - read_bars(): Iterator yielding native bar objects
    - to_price_bar_event(): Convert native bar to PriceBarEvent
    - get_bar_timestamp(): Extract timestamp from bar for synchronization

    Optional caching support:
    - prime_cache(): Initial cache population
    - write_cache(): Write bars to disk
    - update_to_latest(): Incremental cache updates
    """

    def __init__(self, data_path: Path):
        """
        Initialize the data adapter.

        Args:
            data_path: Path to data files or directory
        """
        self.data_path = data_path

    def load_bars(
        self,
        symbol: str,
        start_date: datetime,
        end_date: datetime,
        interval: str = "1d",
    ) -> pd.DataFrame:
        """
        Load OHLCV bar data for a symbol.

        This is the main method that QS-Trader calls to fetch market data.
        Implement your data loading logic here.

        Args:
            symbol: Ticker symbol (e.g., "AAPL", "BTC-USD")
            start_date: Start date for data range
            end_date: End date for data range
            interval: Time interval (e.g., "1d", "1h", "5m")

        Returns:
            DataFrame with columns: timestamp, open, high, low, close, volume
            Sorted by timestamp (oldest first)

        Raises:
            FileNotFoundError: If data file doesn't exist
            ValueError: If data format is invalid

        Example CSV format:
            timestamp,open,high,low,close,volume
            2024-01-01 09:30:00,100.0,101.5,99.5,101.0,1000000
            2024-01-01 09:31:00,101.0,102.0,100.5,101.5,1100000

        Example implementation:
            file_path = self.data_path / f"{symbol}.csv"
            df = pd.read_csv(file_path, parse_dates=['timestamp'])
            df = df[(df['timestamp'] >= start_date) & (df['timestamp'] <= end_date)]
            return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
        """
        # Implement your data loading logic here
        raise NotImplementedError(
            "Implement load_bars method to load data from your source. "
            "Return a DataFrame with columns: timestamp, open, high, low, close, volume"
        )
