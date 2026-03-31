"""Update service - orchestrates dataset updates with progress tracking."""

import json
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

from qs_trader.services.data.dataset_updater import DatasetUpdater, DatasetUpdateResult


class UpdateService:
    """
    Service for orchestrating dataset updates.

    Separates business logic from CLI presentation layer.
    """

    def __init__(self, dataset: str):
        """
        Initialize update service.

        Args:
            dataset: Dataset identifier
        """
        self.updater = DatasetUpdater(dataset)
        self.dataset = dataset

    def get_symbols_to_update(self, explicit_symbols: Optional[List[str]] = None) -> Tuple[List[str], str]:
        """
        Determine which symbols to update based on priority.

        Priority: explicit_symbols > universe.csv > scan cache

        Args:
            explicit_symbols: Optional list of symbols from --symbols flag

        Returns:
            Tuple of (symbols_list, source_description)
        """
        if explicit_symbols:
            return explicit_symbols, f"{len(explicit_symbols)} symbols (--symbols)"

        if self.updater.universe_symbols:
            return (
                self.updater.universe_symbols,
                f"{len(self.updater.universe_symbols)} symbols from universe.csv (full backfill + incremental)",
            )

        cached_symbols = self.updater._scan_cached_symbols()
        if cached_symbols:
            return cached_symbols, f"{len(cached_symbols)} cached symbols"

        return [], "No symbols found"

    def update_symbols(
        self,
        symbols: List[str],
        dry_run: bool = False,
        verbose: bool = False,
        force_reprime: bool = False,
    ) -> Iterator[DatasetUpdateResult]:
        """
        Update multiple symbols with progress tracking.

        Args:
            symbols: List of symbols to update
            dry_run: If True, only check what would be updated
            verbose: Enable detailed logging
            force_reprime: If True, delete existing cache and re-prime from scratch

        Yields:
            DatasetUpdateResult for each symbol
        """
        yield from self.updater.update_symbols(symbols, dry_run=dry_run, verbose=verbose, force_reprime=force_reprime)

    def get_cache_metadata(self, symbol: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        Read cache metadata for a symbol.

        Args:
            symbol: Stock symbol

        Returns:
            Tuple of (start_date, end_date, row_count) or (None, None, None)
        """
        cache_root = self.updater._get_cache_root()
        if not cache_root:
            return None, None, None

        metadata_file = cache_root / symbol / ".metadata.json"
        if not metadata_file.exists():
            return None, None, None

        try:
            with open(metadata_file) as f:
                metadata = json.load(f)
            date_range = metadata.get("date_range", {})
            start_date = date_range.get("start")
            end_date = date_range.get("end")
            row_count = str(metadata.get("row_count", ""))
            return start_date, end_date, row_count
        except Exception:
            return None, None, None

    def get_cache_root(self) -> Optional[Path]:
        """Get cache root directory."""
        return self.updater._get_cache_root()

    def scan_cached_symbols(self) -> List[str]:
        """Scan cache directory for symbols."""
        return self.updater._scan_cached_symbols()

    def read_symbol_metadata(self, symbol: str, cache_root: Path) -> dict:
        """
        Read metadata for a cached symbol.

        Args:
            symbol: Stock symbol
            cache_root: Cache root directory

        Returns:
            Dictionary with metadata or error info
        """
        metadata_file = cache_root / symbol / ".metadata.json"

        if not metadata_file.exists():
            return {
                "symbol": symbol,
                "start_date": "N/A",
                "end_date": "N/A",
                "row_count": "N/A",
                "last_update": "No metadata",
                "error": False,
            }

        try:
            with open(metadata_file) as f:
                metadata = json.load(f)

            date_range = metadata.get("date_range", {})
            return {
                "symbol": symbol,
                "start_date": date_range.get("start", "N/A"),
                "end_date": date_range.get("end", "N/A"),
                "row_count": metadata.get("row_count", "N/A"),
                "last_update": metadata.get("last_update", "N/A"),
                "error": False,
            }
        except Exception as e:
            return {
                "symbol": symbol,
                "start_date": "Error",
                "end_date": "Error",
                "row_count": "Error",
                "last_update": str(e)[:20],
                "error": True,
            }
