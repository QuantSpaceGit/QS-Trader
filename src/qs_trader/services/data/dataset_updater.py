"""
Generic dataset updater.

Provides adapter-agnostic functionality to update cached data from any
source (Algoseek, future providers). Scans cache directories,
detects which symbols need updates, and calls adapter-specific update
methods.

Key features:
- Works with any adapter that supports incremental updates
- Scans cache for symbols requiring updates
- Batch updates with progress tracking
- Dry-run mode for planning
- Verbose logging for debugging
"""

from datetime import date
from pathlib import Path
from typing import Iterator, List, Optional

import structlog

from qs_trader.services.data.adapters.resolver import DataSourceResolver
from qs_trader.services.data.models import Instrument

logger = structlog.get_logger()


class DatasetUpdateResult:
    """
    Result of updating a single symbol.

    Attributes:
        symbol: Stock symbol
        success: Whether update succeeded
        bars_added: Number of new bars added (0 if error or already up-to-date)
        start_date: First date in update range
        end_date: Last date in update range
        error: Error message if failed
    """

    def __init__(
        self,
        symbol: str,
        success: bool,
        bars_added: int = 0,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        error: Optional[str] = None,
    ):
        """
        Initialize update result.

        Args:
            symbol: Stock symbol
            success: Whether update succeeded
            bars_added: Number of new bars added
            start_date: First date in update range
            end_date: Last date in update range
            error: Error message if failed
        """
        self.symbol = symbol
        self.success = success
        self.bars_added = bars_added
        self.start_date = start_date
        self.end_date = end_date
        self.error = error

    def __repr__(self) -> str:
        """String representation."""
        if self.success:
            if self.bars_added == 0:
                return f"DatasetUpdateResult(symbol={self.symbol}, already_current=True)"
            return (
                f"DatasetUpdateResult(symbol={self.symbol}, bars_added={self.bars_added}, "
                f"range={self.start_date} to {self.end_date})"
            )
        return f"DatasetUpdateResult(symbol={self.symbol}, error={self.error})"


class DatasetUpdater:
    """
    Generic dataset updater for any data source.

    Handles incremental updates of cached data across all supported adapters.

    How It Works:
    1. Resolves dataset configuration from data_sources.yaml
    2. Instantiates appropriate adapter for each symbol
    3. Scans cache directory for symbols (if cache_root configured)
    4. For symbols without cache: Performs full backfill using prime_cache() (zero Python memory)
    5. For symbols with cache: Calls update_to_latest() for incremental update

    Persistence Model:
    - Adapters with cache_root configured use efficient bulk caching
    - Full backfill: prime_cache() uses DuckDB COPY (zero Python memory overhead)
    - Incremental update: update_to_latest() appends only new bars to existing cache
    - Cache is created explicitly via prime_cache() (no hidden auto-caching)
    - Subsequent updates use update_to_latest() for efficiency

    Adapter Requirements:
    - REQUIRED: read_bars(start_date, end_date) - Stream bars (no auto-caching)
    - REQUIRED: get_available_date_range() - Get min/max dates for backfill
    - REQUIRED: get_timestamp(bar) - Extract timestamp for any bar type
    - REQUIRED FOR CACHING: At least one of prime_cache() OR write_cache()
    - REQUIRED FOR INCREMENTAL: update_to_latest(dry_run) - Adapter-specific cache merge logic

    Caching Implementation:
    - Adapters MUST implement at least one of: prime_cache() or write_cache()
    - prime_cache() is preferred (efficient bulk copy, zero Python memory)
    - write_cache() is a fallback (materializes bars in memory, slower)
    - If adapter implements neither, cache creation will fail with clear error
    - Streaming-only adapters should raise NotImplementedError from both methods

    If adapter doesn't implement update_to_latest(), incremental updates will fail
    with an error instructing the user to use force_reprime=True, since cache
    merge logic is adapter-specific and cannot be implemented generically.

    Cache Structure:
    ```
    cache_root/
      ├── AAPL/
      │   └── data.parquet
      ├── MSFT/
      │   └── data.parquet
      └── ...
    ```    Example:
        >>> # Update all symbols in dataset
        Examples:
            >>> updater = DatasetUpdater("yahoo-us-equity-1d-csv")
            >>> updater.update_all(["AAPL", "TSLA"])
        >>>
        >>> # Update specific symbols
        >>> results = list(updater.update_symbols(["AAPL", "TSLA"], dry_run=False))
        >>>
        >>> # Check what would be updated (dry run)
        >>> results = list(updater.update_all(dry_run=True))
        >>> for result in results:
        ...     print(f"{result.symbol}: {result.bars_added} bars needed")

    Attributes:
        dataset_name: Dataset identifier from data_sources.yaml
        resolver: Data source resolver for adapter instantiation
        adapter: Instantiated data adapter
        adapter_config: Adapter configuration from YAML
    """

    def __init__(
        self,
        dataset_name: str,
        config_path: Optional[str] = None,
    ):
        """
        Initialize dataset updater.

        Args:
            dataset_name: Name of the dataset in data_sources.yaml
                         (e.g., "yahoo-us-equity-1d-csv")
            config_path: Optional path to data_sources.yaml. If None, uses default.

        Raises:
            ValueError: If dataset not found in configuration
            ValueError: If adapter doesn't support updates
        """
        self.dataset_name = dataset_name
        self.resolver = DataSourceResolver(config_path)

        # Get dataset configuration
        if dataset_name not in self.resolver.sources:
            available = list(self.resolver.sources.keys())
            raise ValueError(f"Dataset '{dataset_name}' not found. Available: {available}")

        self.adapter_config = self.resolver.sources[dataset_name]
        adapter_name = self.adapter_config["adapter"]

        # Get adapter class (but don't instantiate yet - we need symbol-specific instruments)
        self.adapter_class = self.resolver._get_adapter_class(adapter_name)

        # Verify adapter class supports reading bars (basic requirement for data adapters)
        if not hasattr(self.adapter_class, "read_bars"):
            raise ValueError(
                f"Adapter '{adapter_name}' does not have read_bars() method. This is not a valid data adapter."
            )

        # Load universe file if configured
        self.universe_symbols = self._load_universe_symbols()

        logger.info(
            "dataset_updater.initialized",
            dataset=dataset_name,
            adapter=adapter_name,
            cache_enabled=self._supports_caching(),
            universe_symbols=len(self.universe_symbols) if self.universe_symbols else 0,
        )

    def _load_universe_symbols(self) -> Optional[List[str]]:
        """
        Load symbols from configured universe file.

        Returns:
            List of symbols from universe.csv, or None if not configured/not found
        """
        universe_path_str = self.adapter_config.get("universe_file")
        if not universe_path_str:
            return None

        universe_path = Path(universe_path_str)
        if not universe_path.exists():
            logger.debug(
                "dataset_updater.universe_file_not_found",
                path=str(universe_path),
            )
            return None

        try:
            import csv

            symbols = []
            with open(universe_path) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    symbol = row.get("SYMBOL", "").strip()
                    if symbol:
                        symbols.append(symbol)

            logger.info(
                "dataset_updater.universe_loaded",
                path=str(universe_path),
                symbol_count=len(symbols),
            )
            return symbols

        except Exception as e:
            logger.warning(
                "dataset_updater.universe_load_error",
                path=str(universe_path),
                error=str(e),
            )
            return None

    def _supports_caching(self) -> bool:
        """
        Check if adapter supports caching.

        Returns:
            True if adapter class has cache_root in its signature
        """
        # Check config for cache_root (indicates caching support)
        return "cache_root" in self.adapter_config

    def _get_cache_root(self) -> Optional[Path]:
        """
        Get cache root directory.

        Returns:
            Path to cache root or None if caching not supported
        """
        if not self._supports_caching():
            return None
        return Path(self.adapter_config["cache_root"])

    def _scan_cached_symbols(self) -> List[str]:
        """
        Scan cache directory for symbols.

        Returns:
            List of symbols with cached data (empty if caching not supported)
        """
        cache_root = self._get_cache_root()
        if not cache_root or not cache_root.exists():
            logger.warning(
                "dataset_updater.no_cache",
                dataset=self.dataset_name,
                cache_root=str(cache_root) if cache_root else None,
            )
            return []

        # Each symbol has a subdirectory with data.parquet
        symbols = []
        for symbol_dir in cache_root.iterdir():
            if symbol_dir.is_dir() and (symbol_dir / "data.parquet").exists():
                symbols.append(symbol_dir.name)

        logger.info(
            "dataset_updater.cache_scan",
            dataset=self.dataset_name,
            symbols_found=len(symbols),
        )
        return sorted(symbols)

    def _symbol_has_cache(self, symbol: str) -> bool:
        """
        Check if a symbol has cached data.

        Args:
            symbol: Stock symbol to check

        Returns:
            True if symbol has cache directory with data.parquet
        """
        cache_root = self._get_cache_root()
        if not cache_root:
            return False

        symbol_cache = cache_root / symbol / "data.parquet"
        return symbol_cache.exists()

    def update_symbol(
        self,
        symbol: str,
        dry_run: bool = False,
        verbose: bool = False,
        force_reprime: bool = False,
    ) -> DatasetUpdateResult:
        """
        Update a single symbol to latest available data.

        Creates a symbol-specific adapter and calls its update_to_latest() method.
        If adapter supports incremental updates, only fetches new bars.

        Args:
            symbol: Stock symbol to update
            dry_run: If True, only check what would be updated (no API calls)
            verbose: Enable detailed logging
            force_reprime: If True, delete existing cache and re-prime from scratch.
                          Useful when adapter lacks update_to_latest() support.

        Returns:
            Update result with bars added, date range, or error

        Examples:
            >>> updater = DatasetUpdater("yahoo-us-equity-1d-csv")
            >>> result = updater.update_symbol("AAPL", dry_run=False)
            >>> if result.success:
            ...     print(f"Added {result.bars_added} bars")
            ... else:
            ...     print(f"Error: {result.error}")
            >>>
            >>> # Force re-prime for adapter without incremental support
            >>> result = updater.update_symbol("AAPL", force_reprime=True)
        """
        try:
            if verbose:
                logger.info(
                    "dataset_updater.update_symbol.start",
                    symbol=symbol,
                    dataset=self.dataset_name,
                    dry_run=dry_run,
                )

            # Create instrument for this symbol
            instrument = Instrument(symbol=symbol)

            # Create adapter instance for this symbol
            adapter = self.resolver.resolve_by_dataset(self.dataset_name, instrument)

            # Handle force_reprime: delete existing cache before proceeding
            if force_reprime and self._symbol_has_cache(symbol):
                if dry_run:
                    logger.info(
                        "dataset_updater.force_reprime_dry_run",
                        symbol=symbol,
                        message="Would delete cache and re-prime from scratch",
                    )
                else:
                    cache_root = self._get_cache_root()
                    if cache_root:
                        import shutil

                        symbol_cache_dir = cache_root / symbol
                        if symbol_cache_dir.exists():
                            shutil.rmtree(symbol_cache_dir)
                            logger.info(
                                "dataset_updater.cache_deleted",
                                symbol=symbol,
                                cache_dir=str(symbol_cache_dir),
                                reason="force_reprime",
                            )

            # Check if cache exists for this symbol
            cache_exists = self._symbol_has_cache(symbol)

            if not cache_exists:
                # No cache: Full backfill from earliest available
                if verbose:
                    logger.info(
                        "dataset_updater.full_backfill_needed",
                        symbol=symbol,
                        reason="no_cache_found",
                    )

                if dry_run:
                    # Dry run for full backfill
                    return DatasetUpdateResult(
                        symbol=symbol,
                        success=True,
                        bars_added=0,  # Unknown until fetched
                        start_date=None,
                        end_date=None,
                        error="Would perform full backfill (no cache)",
                    )

                # Perform full backfill from earliest available date
                # Query API to get the actual earliest available date for this symbol
                if verbose:
                    logger.info(
                        "dataset_updater.querying_date_range",
                        symbol=symbol,
                    )

                # Get available date range from adapter
                min_date, max_date = adapter.get_available_date_range()

                if not min_date or not max_date:
                    # If we can't get date range, fall back to 20 years
                    logger.warning(
                        "dataset_updater.date_range_unavailable",
                        symbol=symbol,
                        fallback="20_years",
                    )
                    from datetime import timedelta

                    end_date_obj = date.today()
                    start_date_obj = end_date_obj - timedelta(days=7300)  # ~20 years
                    min_date = start_date_obj.isoformat()
                    max_date = end_date_obj.isoformat()

                if verbose:
                    logger.info(
                        "dataset_updater.full_backfill_range",
                        symbol=symbol,
                        start_date=min_date,
                        end_date=max_date,
                    )

                # Use adapter's prime_cache() for efficient bulk caching.
                # This uses DuckDB COPY for zero Python memory overhead
                # (much faster and more memory-efficient than streaming iteration).
                try:
                    bars_added = adapter.prime_cache(min_date, max_date)
                    start_date = date.fromisoformat(min_date)
                    end_date = date.fromisoformat(max_date)
                except (AttributeError, NotImplementedError):
                    # Adapter doesn't support prime_cache() - fall back to streaming
                    # Try write_cache() as fallback, or fail gracefully if not supported
                    if verbose:
                        logger.warning(
                            "dataset_updater.no_prime_cache_fallback",
                            symbol=symbol,
                            message="Adapter doesn't support prime_cache(). Falling back to streaming (slower).",
                        )

                    bars_to_cache = []
                    first_bar = None
                    last_bar = None

                    for bar in adapter.read_bars(min_date, max_date):
                        if first_bar is None:
                            first_bar = bar
                        last_bar = bar
                        bars_to_cache.append(bar)

                    bars_added = len(bars_to_cache)

                    # Extract dates from first/last bars
                    if first_bar is not None and last_bar is not None:
                        first_ts = adapter.get_timestamp(first_bar)
                        last_ts = adapter.get_timestamp(last_bar)
                        start_date = first_ts.date()
                        end_date = last_ts.date()

                        # Persist bars to cache using adapter's write_cache() method
                        try:
                            adapter.write_cache(bars_to_cache)
                            if verbose:
                                logger.info(
                                    "dataset_updater.cache_written_via_fallback",
                                    symbol=symbol,
                                    bars_count=len(bars_to_cache),
                                )
                        except (AttributeError, NotImplementedError) as e:
                            # Adapter doesn't support write_cache either - caching not available
                            error_msg = (
                                f"Adapter {adapter.__class__.__name__} does not support caching. "
                                f"Neither prime_cache() nor write_cache() are implemented. "
                                f"For disk-based caching, implement at least one of these methods. "
                                f"See protocol.py for details."
                            )
                            logger.error(
                                "dataset_updater.no_cache_persistence",
                                symbol=symbol,
                                adapter=adapter.__class__.__name__,
                                error=str(e),
                            )
                            return DatasetUpdateResult(
                                symbol=symbol,
                                success=False,
                                error=error_msg,
                            )
                        except Exception as e:
                            logger.error(
                                "dataset_updater.cache_write_error",
                                symbol=symbol,
                                error=str(e),
                            )
                            return DatasetUpdateResult(
                                symbol=symbol,
                                success=False,
                                error=f"Failed to write cache: {e}",
                            )
                    else:
                        start_date = None
                        end_date = None

                if verbose:
                    logger.info(
                        "dataset_updater.full_backfill_complete",
                        symbol=symbol,
                        bars_cached=bars_added,
                        start_date=str(start_date) if start_date else None,
                        end_date=str(end_date) if end_date else None,
                    )
            else:
                # Cache exists: Incremental update
                if verbose:
                    logger.info(
                        "dataset_updater.incremental_update",
                        symbol=symbol,
                    )

                # Check if adapter supports update_to_latest() method
                if not hasattr(adapter, "update_to_latest"):
                    # Adapter doesn't support incremental updates.
                    # We cannot implement this in the updater because cache format and merge logic
                    # are adapter-specific (parquet vs CSV vs SQLite, column names, partitioning, etc.).
                    # The adapter must implement update_to_latest() to support incremental updates.

                    logger.error(
                        "dataset_updater.no_incremental_support",
                        symbol=symbol,
                        adapter=adapter.__class__.__name__,
                        message=(
                            "Adapter lacks update_to_latest() - cache merge logic is adapter-specific. "
                            "Use force_reprime=True or implement update_to_latest() in adapter."
                        ),
                    )
                    return DatasetUpdateResult(
                        symbol=symbol,
                        success=False,
                        error=(f"Adapter lacks update_to_latest(). Use --force-reprime to re-download."),
                    )
                else:
                    # Call adapter's update method
                    # Adapters should return (bars_added, start_date, end_date)
                    result = adapter.update_to_latest(dry_run=dry_run)

                    # Parse adapter result
                    # Expected format: (bars_added: int, start_date: date | None, end_date: date | None)
                    if isinstance(result, tuple) and len(result) == 3:
                        bars_added, start_date, end_date = result
                    else:
                        # Fallback for adapters that don't return structured result
                        bars_added = result if isinstance(result, int) else 0
                        start_date = None
                        end_date = None

            if verbose:
                logger.info(
                    "dataset_updater.update_symbol.success",
                    symbol=symbol,
                    bars_added=bars_added,
                    start_date=str(start_date) if start_date else None,
                    end_date=str(end_date) if end_date else None,
                )

            return DatasetUpdateResult(
                symbol=symbol,
                success=True,
                bars_added=bars_added,
                start_date=start_date,
                end_date=end_date,
            )

        except Exception as e:
            error_msg = str(e)
            logger.error(
                "dataset_updater.update_symbol.error",
                symbol=symbol,
                dataset=self.dataset_name,
                error=error_msg,
            )
            return DatasetUpdateResult(
                symbol=symbol,
                success=False,
                error=error_msg,
            )

    def update_symbols(
        self,
        symbols: List[str],
        dry_run: bool = False,
        verbose: bool = False,
        force_reprime: bool = False,
    ) -> Iterator[DatasetUpdateResult]:
        """
        Update multiple symbols.

        Yields results as each symbol is processed (not batched).

        Args:
            symbols: List of symbols to update
            dry_run: If True, only check what would be updated
            verbose: Enable detailed logging
            force_reprime: If True, delete existing caches and re-prime from scratch

        Yields:
            Update result for each symbol

        Example:
            >>> updater = DatasetUpdater("yahoo-us-equity-1d-csv")
            >>> symbols = ["AAPL", "TSLA", "NVDA"]
            >>> for result in updater.update_symbols(symbols, verbose=True):
            ...     print(f"{result.symbol}: {result.bars_added} bars")
        """
        logger.info(
            "dataset_updater.update_symbols.start",
            dataset=self.dataset_name,
            symbol_count=len(symbols),
            dry_run=dry_run,
        )

        for symbol in symbols:
            yield self.update_symbol(
                symbol=symbol,
                dry_run=dry_run,
                verbose=verbose,
                force_reprime=force_reprime,
            )

    def update_all(
        self,
        dry_run: bool = False,
        verbose: bool = False,
        force_reprime: bool = False,
    ) -> Iterator[DatasetUpdateResult]:
        """
        Update all symbols in cache.

        Scans cache directory for symbols and updates each one.

        Args:
            dry_run: If True, only check what would be updated
            verbose: Enable detailed logging
            force_reprime: If True, delete existing caches and re-prime from scratch

        Yields:
            Update result for each symbol

        Example:
            >>> updater = DatasetUpdater("yahoo-us-equity-1d-csv")
            >>> results = list(updater.update_all(dry_run=False))
            >>> successful = [r for r in results if r.success]
            >>> print(f"Updated {len(successful)} symbols")
        """
        symbols = self._scan_cached_symbols()

        if not symbols:
            logger.warning(
                "dataset_updater.update_all.no_symbols",
                dataset=self.dataset_name,
            )
            return

        logger.info(
            "dataset_updater.update_all.start",
            dataset=self.dataset_name,
            symbol_count=len(symbols),
            dry_run=dry_run,
        )

        yield from self.update_symbols(
            symbols=symbols,
            dry_run=dry_run,
            verbose=verbose,
            force_reprime=force_reprime,
        )
