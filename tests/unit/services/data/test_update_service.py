"""Unit tests for UpdateService.

Tests the UpdateService implementation using mocks to avoid file system
dependencies. Validates business logic for dataset updates.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

from qs_trader.services.data.dataset_updater import DatasetUpdateResult
from qs_trader.services.data.update_service import UpdateService


class TestUpdateServiceInitialization:
    """Test UpdateService initialization."""

    def test_init_creates_updater_with_dataset(self):
        """Test initialization creates DatasetUpdater with correct dataset."""
        # Arrange & Act
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            service = UpdateService("test-dataset-1d")

            # Assert
            mock_updater_class.assert_called_once_with("test-dataset-1d")
            assert service.dataset == "test-dataset-1d"

    def test_init_stores_dataset_name(self):
        """Test initialization stores dataset name."""
        # Arrange & Act
        with patch("qs_trader.services.data.update_service.DatasetUpdater"):
            service = UpdateService("test-dataset")

            # Assert
            assert service.dataset == "test-dataset"


class TestGetSymbolsToUpdate:
    """Test symbol selection logic."""

    def test_get_symbols_explicit_symbols_takes_priority(self):
        """Test explicit symbols parameter has highest priority."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater.universe_symbols = ["TSLA", "NVDA"]
            mock_updater._scan_cached_symbols.return_value = ["META", "AMZN"]
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")
            explicit = ["AAPL", "MSFT", "GOOGL"]

            # Act
            symbols, source = service.get_symbols_to_update(explicit)

            # Assert
            assert symbols == explicit
            assert source == "3 symbols (--symbols)"

    def test_get_symbols_universe_second_priority(self):
        """Test universe.csv symbols are second priority."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            universe = ["AAPL", "MSFT", "GOOGL", "TSLA"]
            mock_updater.universe_symbols = universe
            mock_updater._scan_cached_symbols.return_value = ["META", "AMZN"]
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            symbols, source = service.get_symbols_to_update(None)

            # Assert
            assert symbols == universe
            assert "4 symbols from universe.csv" in source
            assert "full backfill + incremental" in source

    def test_get_symbols_cache_fallback(self):
        """Test cached symbols are used when no explicit or universe symbols."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            cached = ["META", "AMZN", "NFLX"]
            mock_updater.universe_symbols = []
            mock_updater._scan_cached_symbols.return_value = cached
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            symbols, source = service.get_symbols_to_update(None)

            # Assert
            assert symbols == cached
            assert source == "3 cached symbols"

    def test_get_symbols_empty_when_none_found(self):
        """Test returns empty list when no symbols found."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater.universe_symbols = []
            mock_updater._scan_cached_symbols.return_value = []
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            symbols, source = service.get_symbols_to_update(None)

            # Assert
            assert symbols == []
            assert source == "No symbols found"

    def test_get_symbols_empty_explicit_list_falls_back_to_universe(self):
        """Test empty explicit symbols list falls back to universe (empty list is falsy)."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater.universe_symbols = ["AAPL"]
            mock_updater._scan_cached_symbols.return_value = ["MSFT"]
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            symbols, source = service.get_symbols_to_update([])

            # Assert - Empty list is falsy, so falls back to universe
            assert symbols == ["AAPL"]
            assert "universe.csv" in source


class TestUpdateSymbols:
    """Test symbol update orchestration."""

    def test_update_symbols_delegates_to_updater(self):
        """Test update_symbols delegates to DatasetUpdater."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_results = [
                DatasetUpdateResult("AAPL", True, 5),
                DatasetUpdateResult("MSFT", True, 3),
            ]
            mock_updater.update_symbols.return_value = iter(mock_results)
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")
            symbols = ["AAPL", "MSFT"]

            # Act
            results = list(service.update_symbols(symbols, dry_run=False, verbose=True))

            # Assert
            mock_updater.update_symbols.assert_called_once_with(
                symbols, dry_run=False, verbose=True, force_reprime=False
            )
            assert results == mock_results

    def test_update_symbols_dry_run(self):
        """Test update_symbols passes dry_run flag correctly."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater.update_symbols.return_value = iter([])
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            list(service.update_symbols(["AAPL"], dry_run=True, verbose=False))

            # Assert
            mock_updater.update_symbols.assert_called_once_with(
                ["AAPL"], dry_run=True, verbose=False, force_reprime=False
            )

    def test_update_symbols_verbose(self):
        """Test update_symbols passes verbose flag correctly."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater.update_symbols.return_value = iter([])
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            list(service.update_symbols(["AAPL"], dry_run=False, verbose=True))

            # Assert
            mock_updater.update_symbols.assert_called_once_with(
                ["AAPL"], dry_run=False, verbose=True, force_reprime=False
            )

    def test_update_symbols_empty_list(self):
        """Test update_symbols handles empty symbol list."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater.update_symbols.return_value = iter([])
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            results = list(service.update_symbols([]))

            # Assert
            assert results == []


class TestGetCacheMetadata:
    """Test cache metadata reading."""

    def test_get_cache_metadata_success(self, tmp_path: Path):
        """Test reading cache metadata successfully."""
        # Arrange
        cache_root = tmp_path / "cache"
        symbol_dir = cache_root / "AAPL"
        symbol_dir.mkdir(parents=True)

        metadata = {
            "date_range": {"start": "2020-01-01", "end": "2023-12-31"},
            "row_count": 1000,
        }
        metadata_file = symbol_dir / ".metadata.json"
        metadata_file.write_text(json.dumps(metadata))

        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._get_cache_root.return_value = cache_root
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            start, end, count = service.get_cache_metadata("AAPL")

            # Assert
            assert start == "2020-01-01"
            assert end == "2023-12-31"
            assert count == "1000"

    def test_get_cache_metadata_no_cache_root(self):
        """Test returns None when cache root doesn't exist."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._get_cache_root.return_value = None
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            start, end, count = service.get_cache_metadata("AAPL")

            # Assert
            assert start is None
            assert end is None
            assert count is None

    def test_get_cache_metadata_no_metadata_file(self, tmp_path: Path):
        """Test returns None when metadata file doesn't exist."""
        # Arrange
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._get_cache_root.return_value = cache_root
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            start, end, count = service.get_cache_metadata("AAPL")

            # Assert
            assert start is None
            assert end is None
            assert count is None

    def test_get_cache_metadata_corrupted_json(self, tmp_path: Path):
        """Test handles corrupted JSON gracefully."""
        # Arrange
        cache_root = tmp_path / "cache"
        symbol_dir = cache_root / "AAPL"
        symbol_dir.mkdir(parents=True)

        metadata_file = symbol_dir / ".metadata.json"
        metadata_file.write_text("{ invalid json")

        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._get_cache_root.return_value = cache_root
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            start, end, count = service.get_cache_metadata("AAPL")

            # Assert
            assert start is None
            assert end is None
            assert count is None

    def test_get_cache_metadata_missing_fields(self, tmp_path: Path):
        """Test handles missing metadata fields."""
        # Arrange
        cache_root = tmp_path / "cache"
        symbol_dir = cache_root / "AAPL"
        symbol_dir.mkdir(parents=True)

        metadata = {"some_other_field": "value"}
        metadata_file = symbol_dir / ".metadata.json"
        metadata_file.write_text(json.dumps(metadata))

        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._get_cache_root.return_value = cache_root
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            start, end, count = service.get_cache_metadata("AAPL")

            # Assert
            assert start is None
            assert end is None
            assert count == ""


class TestGetCacheRoot:
    """Test cache root access."""

    def test_get_cache_root_delegates_to_updater(self, tmp_path: Path):
        """Test get_cache_root delegates to DatasetUpdater."""
        # Arrange
        cache_root = tmp_path / "cache"

        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._get_cache_root.return_value = cache_root
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            result = service.get_cache_root()

            # Assert
            mock_updater._get_cache_root.assert_called_once()
            assert result == cache_root

    def test_get_cache_root_returns_none(self):
        """Test get_cache_root returns None when no cache."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._get_cache_root.return_value = None
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            result = service.get_cache_root()

            # Assert
            assert result is None


class TestScanCachedSymbols:
    """Test cached symbols scanning."""

    def test_scan_cached_symbols_delegates_to_updater(self):
        """Test scan_cached_symbols delegates to DatasetUpdater."""
        # Arrange
        cached_symbols = ["AAPL", "MSFT", "GOOGL"]

        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._scan_cached_symbols.return_value = cached_symbols
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            result = service.scan_cached_symbols()

            # Assert
            mock_updater._scan_cached_symbols.assert_called_once()
            assert result == cached_symbols

    def test_scan_cached_symbols_empty(self):
        """Test scan_cached_symbols returns empty list when no cache."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater._scan_cached_symbols.return_value = []
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            result = service.scan_cached_symbols()

            # Assert
            assert result == []


class TestReadSymbolMetadata:
    """Test reading symbol metadata."""

    def test_read_symbol_metadata_success(self, tmp_path: Path):
        """Test reading symbol metadata successfully."""
        # Arrange
        cache_root = tmp_path / "cache"
        symbol_dir = cache_root / "AAPL"
        symbol_dir.mkdir(parents=True)

        metadata = {
            "date_range": {"start": "2020-01-01", "end": "2023-12-31"},
            "row_count": 1000,
            "last_update": "2024-01-15T10:30:00Z",
        }
        metadata_file = symbol_dir / ".metadata.json"
        metadata_file.write_text(json.dumps(metadata))

        with patch("qs_trader.services.data.update_service.DatasetUpdater"):
            service = UpdateService("test-dataset")

            # Act
            result = service.read_symbol_metadata("AAPL", cache_root)

            # Assert
            assert result["symbol"] == "AAPL"
            assert result["start_date"] == "2020-01-01"
            assert result["end_date"] == "2023-12-31"
            assert result["row_count"] == 1000
            assert result["last_update"] == "2024-01-15T10:30:00Z"
            assert result["error"] is False

    def test_read_symbol_metadata_no_file(self, tmp_path: Path):
        """Test reading metadata when file doesn't exist."""
        # Arrange
        cache_root = tmp_path / "cache"
        cache_root.mkdir()

        with patch("qs_trader.services.data.update_service.DatasetUpdater"):
            service = UpdateService("test-dataset")

            # Act
            result = service.read_symbol_metadata("AAPL", cache_root)

            # Assert
            assert result["symbol"] == "AAPL"
            assert result["start_date"] == "N/A"
            assert result["end_date"] == "N/A"
            assert result["row_count"] == "N/A"
            assert result["last_update"] == "No metadata"
            assert result["error"] is False

    def test_read_symbol_metadata_corrupted_json(self, tmp_path: Path):
        """Test reading metadata with corrupted JSON."""
        # Arrange
        cache_root = tmp_path / "cache"
        symbol_dir = cache_root / "AAPL"
        symbol_dir.mkdir(parents=True)

        metadata_file = symbol_dir / ".metadata.json"
        metadata_file.write_text("{ invalid json")

        with patch("qs_trader.services.data.update_service.DatasetUpdater"):
            service = UpdateService("test-dataset")

            # Act
            result = service.read_symbol_metadata("AAPL", cache_root)

            # Assert
            assert result["symbol"] == "AAPL"
            assert result["start_date"] == "Error"
            assert result["end_date"] == "Error"
            assert result["row_count"] == "Error"
            assert "Expecting" in result["last_update"]  # JSON decode error message
            assert result["error"] is True

    def test_read_symbol_metadata_missing_fields(self, tmp_path: Path):
        """Test reading metadata with missing fields."""
        # Arrange
        cache_root = tmp_path / "cache"
        symbol_dir = cache_root / "AAPL"
        symbol_dir.mkdir(parents=True)

        metadata = {"some_other_field": "value"}
        metadata_file = symbol_dir / ".metadata.json"
        metadata_file.write_text(json.dumps(metadata))

        with patch("qs_trader.services.data.update_service.DatasetUpdater"):
            service = UpdateService("test-dataset")

            # Act
            result = service.read_symbol_metadata("AAPL", cache_root)

            # Assert
            assert result["symbol"] == "AAPL"
            assert result["start_date"] == "N/A"
            assert result["end_date"] == "N/A"
            assert result["row_count"] == "N/A"
            assert result["last_update"] == "N/A"
            assert result["error"] is False

    def test_read_symbol_metadata_partial_date_range(self, tmp_path: Path):
        """Test reading metadata with partial date range."""
        # Arrange
        cache_root = tmp_path / "cache"
        symbol_dir = cache_root / "AAPL"
        symbol_dir.mkdir(parents=True)

        metadata = {
            "date_range": {"start": "2020-01-01"},
            "row_count": 500,
        }
        metadata_file = symbol_dir / ".metadata.json"
        metadata_file.write_text(json.dumps(metadata))

        with patch("qs_trader.services.data.update_service.DatasetUpdater"):
            service = UpdateService("test-dataset")

            # Act
            result = service.read_symbol_metadata("AAPL", cache_root)

            # Assert
            assert result["symbol"] == "AAPL"
            assert result["start_date"] == "2020-01-01"
            assert result["end_date"] == "N/A"
            assert result["row_count"] == 500


class TestIntegrationScenarios:
    """Test realistic integration scenarios."""

    def test_typical_update_workflow(self):
        """Test typical workflow: scan symbols, get metadata, update."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            cached_symbols = ["AAPL", "MSFT"]
            mock_updater.universe_symbols = []
            mock_updater._scan_cached_symbols.return_value = cached_symbols
            mock_updater.update_symbols.return_value = iter(
                [
                    DatasetUpdateResult("AAPL", True, 5),
                    DatasetUpdateResult("MSFT", True, 3),
                ]
            )
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act - Step 1: Get symbols
            symbols, source = service.get_symbols_to_update(None)

            # Act - Step 2: Update symbols
            results = list(service.update_symbols(symbols))

            # Assert
            assert symbols == cached_symbols
            assert len(results) == 2
            assert all(r.success for r in results)

    def test_explicit_symbols_override_workflow(self):
        """Test workflow with explicit symbol override."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            explicit = ["TSLA", "NVDA"]
            mock_updater.universe_symbols = ["AAPL", "MSFT"]
            mock_updater.update_symbols.return_value = iter(
                [
                    DatasetUpdateResult("TSLA", True, 10),
                    DatasetUpdateResult("NVDA", True, 15),
                ]
            )
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            symbols, source = service.get_symbols_to_update(explicit)
            results = list(service.update_symbols(symbols, dry_run=False))

            # Assert
            assert symbols == explicit
            assert "2 symbols (--symbols)" in source
            assert len(results) == 2

    def test_no_symbols_workflow(self):
        """Test workflow when no symbols found."""
        # Arrange
        with patch("qs_trader.services.data.update_service.DatasetUpdater") as mock_updater_class:
            mock_updater = MagicMock()
            mock_updater.universe_symbols = []
            mock_updater._scan_cached_symbols.return_value = []
            mock_updater_class.return_value = mock_updater

            service = UpdateService("test-dataset")

            # Act
            symbols, source = service.get_symbols_to_update(None)

            # Assert
            assert symbols == []
            assert source == "No symbols found"
