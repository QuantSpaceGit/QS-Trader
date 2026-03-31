"""
Vendor Adapter Protocol - Data layer interface.

Defines the contract for vendor-specific data adapters (Yahoo, custom sources, etc.)
without hard-coding vendor-specific types (VendorBar, etc.).

IMPORTANT FOR ADAPTER AUTHORS:
- If your adapter supports disk-based caching, you MUST implement update_to_latest()
  for incremental updates to work. Cache merge logic is adapter-specific and cannot
  be implemented generically (parquet vs CSV vs SQLite, different column names, etc.).
- Without update_to_latest(), users must use force_reprime=True which deletes and
  re-downloads the entire cache on each update (inefficient for large datasets).
Overview:
- See builtin/yahoo_csv.py for reference implementation.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterator, Optional, Protocol, Tuple

if TYPE_CHECKING:
    from qs_trader.events.events import CorporateActionEvent, PriceBarEvent


class IDataAdapter(Protocol):
    """
    Protocol for data adapters.

    All data adapters must implement these methods to integrate with
    DataService and enable event-driven data streaming.

    The protocol ensures adapters can:
    1. Stream bars from their native storage format
    2. Convert bars to standardized PriceBarEvent
    3. Extract corporate actions from bars
    4. Provide timestamp for heap-merge synchronization
    5. (Optional) Support disk-based caching with incremental updates

    Caching Requirements:
    - If you implement prime_cache() or write_cache(), you MUST also implement
      update_to_latest() for incremental updates to work efficiently.
    - Without update_to_latest(), the updater cannot merge new bars with existing
      cache because it doesn't know your cache format (parquet/CSV/SQLite/etc.).
    - Users will need to use force_reprime=True to delete and re-download on each update.

    Examples:
        >>> class MyCustomAdapter:
        ...     def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
        ...         # Stream bars from custom source
        ...         ...
        ...
        ...     def to_price_bar_event(self, bar: Any) -> PriceBarEvent:
        ...         # Convert custom bar to PriceBarEvent
        ...         ...
        ...
        ...     def to_corporate_action_event(
        ...         self, bar: Any, prev_bar: Optional[Any] = None
        ...     ) -> Optional[CorporateActionEvent]:
        ...         # Extract corporate action if present
        ...         ...
        ...
        ...     def get_timestamp(self, bar: Any) -> datetime:
        ...         # Return bar's timestamp for sorting
        ...         ...
        ...
        ...     # Optional but REQUIRED if you implement caching:
        ...     def update_to_latest(self, dry_run: bool = False) -> Tuple[int, date, date]:
        ...         # Read last cached date, fetch only new bars, merge with cache
        ...         ...
    """

    def read_bars(self, start_date: str, end_date: str) -> Iterator[Any]:
        """
        Stream bars from data source.

        Must yield bars in chronological order (oldest first).
        Returns:
            Iterator[T]: Stream of bar objects.
        Bar type is adapter-specific (defined by each adapter).

        Args:
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD), inclusive

        Yields:
            Bars in adapter's native format

        Examples:
            >>> for bar in adapter.read_bars("2024-01-01", "2024-12-31"):
            ...     print(f"{bar.symbol}: {bar.close}")
        """
        ...

    def to_price_bar_event(self, bar: Any) -> "PriceBarEvent":
        """
        Convert adapter's native bar to standardized PriceBarEvent.

        This method handles all adapter-specific field mapping:
        - Timestamp extraction and timezone handling
        - OHLCV field mapping
        - Adjustment factors
        - Metadata (asset_class, source, etc.)

        Args:
            bar: Bar in adapter's native format

        Returns:
            Standardized PriceBarEvent

        Examples:
            >>> for bar in adapter.read_bars("2024-01-01", "2024-12-31"):
            ...     event = adapter.to_price_bar_event(bar)
            ...     event_bus.publish(event)
        """
        ...

    def to_corporate_action_event(self, bar: Any, prev_bar: Optional[Any] = None) -> Optional["CorporateActionEvent"]:
        """
        Extract corporate action from bar (if present).

        Not all bars contain corporate actions. Returns None if bar
        has no split/dividend/etc.

        Args:
            bar: Current bar in adapter's native format
            prev_bar: Previous bar (required for some action types like dividends)

        Returns:
            CorporateActionEvent if action detected, None otherwise

        Examples:
            >>> prev = None
            >>> for bar in adapter.read_bars("2024-01-01", "2024-12-31"):
            ...     event = adapter.to_corporate_action_event(bar, prev)
            ...     if event:
            ...         event_bus.publish(event)
            ...     prev = bar
        """
        ...

    def get_timestamp(self, bar: Any) -> datetime:
        """
        Extract timestamp from bar for heap-merge synchronization.

        DataService uses this to synchronize multi-symbol streams.
        Must return a comparable datetime object.

        Args:
            bar: Bar in adapter's native format

        Returns:
            Bar's timestamp as datetime

        Examples:
            >>> bar = next(adapter.read_bars("2024-01-01", "2024-01-01"))
            >>> ts = adapter.get_timestamp(bar)
            >>> print(ts)  # datetime(2024, 1, 1, 16, 0, 0)
        """
        ...

    def get_available_date_range(self) -> Tuple[Optional[str], Optional[str]]:
        """
        Get available date range for this instrument.

        Used by DatasetUpdater to determine full backfill range.

        Returns:
            Tuple of (min_date, max_date) in ISO format, or (None, None) if no data

        Examples:
            >>> min_date, max_date = adapter.get_available_date_range()
            >>> print(f"Data available from {min_date} to {max_date}")
        """
        ...

    def prime_cache(self, start_date: str, end_date: str) -> int:
        """
        Efficiently write source data to cache without Python materialization.

        This method is OPTIONAL. Adapters that don't support disk-based caching
        (e.g., pure streaming providers, database-backed adapters) should raise
        NotImplementedError with a clear message.

        This method is the recommended way to initialize cache before using read_bars().
        Implementations should use bulk copy mechanisms (e.g., DuckDB COPY, pyarrow streaming)
        to avoid loading data into Python memory.

        Current Limitation: The reference implementation assumes on-disk parquet caching.
        Adapters using different persistence strategies (CSV, SQLite, databases, etc.)
        must implement their own caching logic or raise NotImplementedError.

        Args:
            start_date: Start date in ISO format (YYYY-MM-DD)
            end_date: End date in ISO format (YYYY-MM-DD), inclusive

        Returns:
            Number of bars written to cache

        Raises:
            NotImplementedError: If adapter doesn't support disk-based caching
            ValueError: If cache_root not configured
            FileNotFoundError: If source data not found

        Examples:
            >>> # Prime cache for full backfill (zero Python memory)
            >>> bars_written = adapter.prime_cache("2020-01-01", "2024-12-31")
            >>> print(f"Cached {bars_written} bars")
            >>>
            >>> # Now read_bars() will use cache (fast)
            >>> for bar in adapter.read_bars("2024-01-01", "2024-12-31"):
            ...     print(bar.close)
            >>>
            >>> # Streaming-only adapter (no caching)
            >>> try:
            ...     adapter.prime_cache("2020-01-01", "2024-12-31")
            ... except NotImplementedError:
            ...     print("This adapter doesn't support caching")

        Notes:
            - OPTIONAL: Raise NotImplementedError if caching not supported
            - Uses efficient bulk transfer (e.g., parquet→parquet via DuckDB)
            - Zero Python memory overhead (no iteration/materialization)
            - Creates cache directory structure automatically
            - Safe to call multiple times (overwrites existing cache)
            - For incremental updates, use update_to_latest() instead
        """
        ...

    def write_cache(self, bars: list[Any]) -> None:
        """
        Write bars to cache (fallback for adapters without prime_cache).

        This is an OPTIONAL method used as a fallback when prime_cache() is not available.
        Adapters that don't support caching should raise NotImplementedError.

        This method materializes bars in memory, so it's less efficient than prime_cache().
        Use prime_cache() when possible for better performance.

        Current Limitation: Assumes disk-based caching. Database-backed or streaming-only
        adapters should raise NotImplementedError with a clear explanation.

        Args:
            bars: List of bars in adapter's native format to write to cache

        Raises:
            NotImplementedError: If adapter doesn't support disk-based caching
            ValueError: If cache_root not configured

        Examples:
            >>> bars = list(adapter.read_bars("2020-01-01", "2020-12-31"))
            >>> adapter.write_cache(bars)
            >>>
            >>> # Database-backed adapter (no disk caching)
            >>> try:
            ...     adapter.write_cache(bars)
            ... except NotImplementedError:
            ...     print("This adapter uses database persistence, not disk cache")

        Notes:
            - OPTIONAL: Raise NotImplementedError if caching not supported
            - This is a fallback method for adapters without prime_cache()
            - Materializes bars in memory (less efficient than prime_cache)
            - Creates cache directory structure automatically
            - Used internally by DatasetUpdater when prime_cache() unavailable
        """
        ...
