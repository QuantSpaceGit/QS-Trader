"""Unit tests for qs_trader.data.dataset_updater module.

Tests the generic dataset updater functionality for incremental cache updates
across all data adapters (Algoseek, Binance, etc.).
"""

from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.services.data.dataset_updater import DatasetUpdater, DatasetUpdateResult


class TestDatasetUpdateResult:
    """Tests for DatasetUpdateResult data class."""

    def test_init_success_with_bars(self):
        """Test creating successful result with bars added."""
        # Arrange & Act
        result = DatasetUpdateResult(
            symbol="AAPL",
            success=True,
            bars_added=10,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 10),
        )

        # Assert
        assert result.symbol == "AAPL"
        assert result.success is True
        assert result.bars_added == 10
        assert result.start_date == date(2024, 1, 1)
        assert result.end_date == date(2024, 1, 10)
        assert result.error is None

    def test_init_success_already_current(self):
        """Test creating result when already up-to-date."""
        # Arrange & Act
        result = DatasetUpdateResult(
            symbol="TSLA",
            success=True,
            bars_added=0,
        )

        # Assert
        assert result.symbol == "TSLA"
        assert result.success is True
        assert result.bars_added == 0

    def test_init_failure_with_error(self):
        """Test creating failed result with error message."""
        # Arrange & Act
        result = DatasetUpdateResult(
            symbol="NVDA",
            success=False,
            error="API rate limit exceeded",
        )

        # Assert
        assert result.symbol == "NVDA"
        assert result.success is False
        assert result.error == "API rate limit exceeded"
        assert result.bars_added == 0

    def test_repr_success_with_bars(self):
        """Test string representation for successful update with bars."""
        # Arrange
        result = DatasetUpdateResult(
            symbol="AAPL",
            success=True,
            bars_added=5,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 5),
        )

        # Act
        repr_str = repr(result)

        # Assert
        assert "AAPL" in repr_str
        assert "bars_added=5" in repr_str
        assert "2024-01-01" in repr_str
        assert "2024-01-05" in repr_str

    def test_repr_success_already_current(self):
        """Test string representation when already up-to-date."""
        # Arrange
        result = DatasetUpdateResult(
            symbol="TSLA",
            success=True,
            bars_added=0,
        )

        # Act
        repr_str = repr(result)

        # Assert
        assert "TSLA" in repr_str
        assert "already_current=True" in repr_str

    def test_repr_failure(self):
        """Test string representation for failed update."""
        # Arrange
        result = DatasetUpdateResult(
            symbol="NVDA",
            success=False,
            error="Connection timeout",
        )

        # Act
        repr_str = repr(result)

        # Assert
        assert "NVDA" in repr_str
        assert "error=Connection timeout" in repr_str


@pytest.fixture
def temp_config(tmp_path: Path) -> Path:
    """Create temporary config file with test data sources."""
    config_content = """
data_sources:
  test-dataset-with-cache:
    provider: test_provider
    adapter: test_adapter
    cache_root: "{cache_dir}"
    universe_file: "{universe_file}"

  test-dataset-no-cache:
    provider: test_provider
    adapter: test_adapter
    root_path: "data/sample"

  test-dataset-no-update:
    provider: test_provider
    adapter: testAdapter
"""
    cache_dir = tmp_path / "cache"
    cache_dir.mkdir(exist_ok=True)
    universe_file = tmp_path / "universe.csv"

    # Create universe file
    universe_file.write_text("SYMBOL,NAME\nAAPL,Apple Inc\nTSLA,Tesla Inc\nNVDA,NVIDIA Corp\n")

    config_path = tmp_path / "data_sources.yaml"
    config_path.write_text(
        config_content.format(
            cache_dir=str(cache_dir),
            universe_file=str(universe_file),
        )
    )
    return config_path


@pytest.fixture
def mock_adapter_with_update():
    """Create mock adapter class that supports updates."""
    adapter_class = MagicMock()
    adapter_class.__name__ = "MockAdapter"

    # Mock instance
    adapter_instance = MagicMock()
    adapter_instance.update_to_latest.return_value = (5, date(2024, 1, 1), date(2024, 1, 5))
    adapter_instance.get_available_date_range.return_value = ("2020-01-01", "2024-12-31")

    # Make class callable to return instance
    adapter_class.return_value = adapter_instance

    # Add update_to_latest attribute to class for hasattr check
    adapter_class.update_to_latest = MagicMock()

    return adapter_class


@pytest.fixture
def mock_adapter_no_update():
    """Create mock adapter class without update support."""
    adapter_class = MagicMock()
    adapter_class.__name__ = "MockAdapterNoUpdate"

    # Remove update_to_latest to simulate no update support
    if hasattr(adapter_class, "update_to_latest"):
        delattr(adapter_class, "update_to_latest")

    return adapter_class


class TestDatasetUpdaterInitialization:
    """Tests for DatasetUpdater initialization."""

    def test_init_success(self, temp_config: Path, mock_adapter_with_update):
        """Test successful initialization with valid dataset."""
        # Arrange & Act
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset-with-cache": {
                    "adapter": "test_adapter",
                    "cache_root": "/tmp/cache",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset-with-cache", str(temp_config))

            # Assert
            assert updater.dataset_name == "test-dataset-with-cache"
            assert updater.adapter_class == mock_adapter_with_update
            mock_resolver._get_adapter_class.assert_called_once_with("test_adapter")

    def test_init_dataset_not_found(self, temp_config: Path):
        """Test initialization fails with nonexistent dataset."""
        # Arrange & Act & Assert
        with (
            patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class,
            pytest.raises(ValueError, match="Dataset 'nonexistent' not found"),
        ):
            mock_resolver = MagicMock()
            mock_resolver.sources = {"other-dataset": {}}
            mock_resolver_class.return_value = mock_resolver

            DatasetUpdater("nonexistent", str(temp_config))

    def test_init_adapter_no_read_bars_method(self, temp_config: Path):
        """Test initialization fails when adapter lacks read_bars method."""
        # Arrange
        mock_adapter_no_read_bars = MagicMock()
        mock_adapter_no_read_bars.__name__ = "MockAdapterNoReadBars"
        # Remove read_bars to simulate invalid adapter
        if hasattr(mock_adapter_no_read_bars, "read_bars"):
            delattr(mock_adapter_no_read_bars, "read_bars")

        # Act & Assert
        with (
            patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class,
            pytest.raises(ValueError, match="does not have read_bars"),
        ):
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "testAdapter",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_no_read_bars
            mock_resolver_class.return_value = mock_resolver

            DatasetUpdater("test-dataset", str(temp_config))

    def test_init_loads_universe_symbols(self, temp_config: Path, mock_adapter_with_update):
        """Test that universe file is loaded during initialization."""
        # Arrange & Act
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            universe_file = temp_config.parent / "universe.csv"
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "universe_file": str(universe_file),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Assert
            assert updater.universe_symbols is not None
            assert "AAPL" in updater.universe_symbols
            assert "TSLA" in updater.universe_symbols
            assert "NVDA" in updater.universe_symbols
            assert len(updater.universe_symbols) == 3


class TestDatasetUpdaterCacheSupport:
    """Tests for cache support detection."""

    def test_supports_caching_true(self, temp_config: Path, mock_adapter_with_update):
        """Test detecting caching support when cache_root configured."""
        # Arrange
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": "/tmp/cache",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            supports_cache = updater._supports_caching()

            # Assert
            assert supports_cache is True

    def test_supports_caching_false(self, temp_config: Path, mock_adapter_with_update):
        """Test detecting no caching support when cache_root absent."""
        # Arrange
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "root_path": "/data",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            supports_cache = updater._supports_caching()

            # Assert
            assert supports_cache is False

    def test_get_cache_root_exists(self, temp_config: Path, mock_adapter_with_update):
        """Test getting cache root when configured."""
        # Arrange
        cache_path = "/tmp/test_cache"
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": cache_path,
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            cache_root = updater._get_cache_root()

            # Assert
            assert cache_root == Path(cache_path)

    def test_get_cache_root_none(self, temp_config: Path, mock_adapter_with_update):
        """Test getting cache root when not configured."""
        # Arrange
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            cache_root = updater._get_cache_root()

            # Assert
            assert cache_root is None


class TestDatasetUpdaterCacheScanning:
    """Tests for cache directory scanning."""

    def test_scan_cached_symbols_with_cache(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test scanning cache directory finds symbols."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)

        # Create symbol directories with data.parquet files
        for symbol in ["AAPL", "TSLA", "NVDA"]:
            symbol_dir = cache_dir / symbol
            symbol_dir.mkdir()
            (symbol_dir / "data.parquet").touch()

        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": str(cache_dir),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            symbols = updater._scan_cached_symbols()

            # Assert
            assert len(symbols) == 3
            assert "AAPL" in symbols
            assert "TSLA" in symbols
            assert "NVDA" in symbols
            assert symbols == sorted(symbols), "Symbols should be sorted"

    def test_scan_cached_symbols_no_cache_dir(self, temp_config: Path, mock_adapter_with_update):
        """Test scanning returns empty when cache directory doesn't exist."""
        # Arrange
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": "/nonexistent/cache",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            symbols = updater._scan_cached_symbols()

            # Assert
            assert symbols == []

    def test_scan_cached_symbols_no_parquet_files(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test scanning ignores directories without data.parquet."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)

        # Create symbol directories WITHOUT data.parquet
        (cache_dir / "AAPL").mkdir()
        (cache_dir / "TSLA").mkdir()

        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": str(cache_dir),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            symbols = updater._scan_cached_symbols()

            # Assert
            assert symbols == []

    def test_symbol_has_cache_true(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test checking if symbol has cache returns True."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)
        symbol_dir = cache_dir / "AAPL"
        symbol_dir.mkdir()
        (symbol_dir / "data.parquet").touch()

        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": str(cache_dir),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            has_cache = updater._symbol_has_cache("AAPL")

            # Assert
            assert has_cache is True

    def test_symbol_has_cache_false(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test checking if symbol has cache returns False."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)

        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": str(cache_dir),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            has_cache = updater._symbol_has_cache("TSLA")

            # Assert
            assert has_cache is False


class TestDatasetUpdaterUpdateSymbol:
    """Tests for updating individual symbols."""

    def test_update_symbol_incremental_success(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test successful incremental update of existing cached symbol."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)
        symbol_dir = cache_dir / "AAPL"
        symbol_dir.mkdir()
        (symbol_dir / "data.parquet").touch()

        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": str(cache_dir),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update

            # Mock adapter instance
            mock_adapter_instance = MagicMock()
            mock_adapter_instance.update_to_latest.return_value = (
                5,
                date(2024, 1, 1),
                date(2024, 1, 5),
            )
            mock_resolver.resolve_by_dataset.return_value = mock_adapter_instance

            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            result = updater.update_symbol("AAPL", dry_run=False, verbose=False)

            # Assert
            assert result.success is True
            assert result.symbol == "AAPL"
            assert result.bars_added == 5
            assert result.start_date == date(2024, 1, 1)
            assert result.end_date == date(2024, 1, 5)
            assert result.error is None
            mock_adapter_instance.update_to_latest.assert_called_once_with(dry_run=False)

    def test_update_symbol_error_handling(self, temp_config: Path, mock_adapter_with_update):
        """Test error handling when update fails."""
        # Arrange
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": "/tmp/cache",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver.resolve_by_dataset.side_effect = Exception("API rate limit")
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            result = updater.update_symbol("AAPL", dry_run=False)

            # Assert
            assert result.success is False
            assert result.symbol == "AAPL"
            assert result.error is not None
            assert "API rate limit" in result.error
            assert result.bars_added == 0


class TestDatasetUpdaterUpdateMultiple:
    """Tests for updating multiple symbols."""

    def test_update_symbols_yields_results(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test updating multiple symbols yields results for each."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)

        # Create cache for symbols
        for symbol in ["AAPL", "TSLA"]:
            symbol_dir = cache_dir / symbol
            symbol_dir.mkdir()
            (symbol_dir / "data.parquet").touch()

        with patch("qs_trader.services.data.dataset_updater.DatasetUpdater.update_symbol") as mock_update:
            mock_update.side_effect = [
                DatasetUpdateResult("AAPL", True, bars_added=5),
                DatasetUpdateResult("TSLA", True, bars_added=3),
            ]

            with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
                mock_resolver = MagicMock()
                mock_resolver.sources = {
                    "test-dataset": {
                        "adapter": "test_adapter",
                        "cache_root": str(cache_dir),
                    }
                }
                mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
                mock_resolver_class.return_value = mock_resolver

                updater = DatasetUpdater("test-dataset", str(temp_config))

                # Act
                results = list(updater.update_symbols(["AAPL", "TSLA"], dry_run=False))

                # Assert
                assert len(results) == 2
                assert results[0].symbol == "AAPL"
                assert results[0].bars_added == 5
                assert results[1].symbol == "TSLA"
                assert results[1].bars_added == 3

    def test_update_all_scans_and_updates_cache(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test update_all scans cache and updates all symbols."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)

        # Create cache for multiple symbols
        for symbol in ["AAPL", "TSLA", "NVDA"]:
            symbol_dir = cache_dir / symbol
            symbol_dir.mkdir()
            (symbol_dir / "data.parquet").touch()

        with patch("qs_trader.services.data.dataset_updater.DatasetUpdater.update_symbol") as mock_update:
            mock_update.side_effect = [
                DatasetUpdateResult("AAPL", True, bars_added=5),
                DatasetUpdateResult("NVDA", True, bars_added=3),
                DatasetUpdateResult("TSLA", True, bars_added=7),
            ]

            with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
                mock_resolver = MagicMock()
                mock_resolver.sources = {
                    "test-dataset": {
                        "adapter": "test_adapter",
                        "cache_root": str(cache_dir),
                    }
                }
                mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
                mock_resolver_class.return_value = mock_resolver

                updater = DatasetUpdater("test-dataset", str(temp_config))

                # Act
                results = list(updater.update_all(dry_run=False))

                # Assert
                assert len(results) == 3
                symbols_updated = [r.symbol for r in results]
                assert "AAPL" in symbols_updated
                assert "TSLA" in symbols_updated
                assert "NVDA" in symbols_updated

    def test_update_all_no_symbols_returns_empty(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test update_all returns empty when no cached symbols."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)  # Empty cache

        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": str(cache_dir),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            results = list(updater.update_all(dry_run=False))

            # Assert
            assert len(results) == 0


class TestDatasetUpdaterUniverseLoading:
    """Tests for universe file loading."""

    def test_load_universe_symbols_success(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test successfully loading symbols from universe file."""
        # Arrange
        universe_file = tmp_path / "test_universe.csv"
        universe_file.write_text("SYMBOL,NAME,SECTOR\nAAPL,Apple Inc,Technology\nMSFT,Microsoft,Technology\n")

        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "universe_file": str(universe_file),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Assert
            assert updater.universe_symbols == ["AAPL", "MSFT"]

    def test_load_universe_symbols_file_not_found(self, temp_config: Path, mock_adapter_with_update):
        """Test handling when universe file doesn't exist."""
        # Arrange
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "universe_file": "/nonexistent/universe.csv",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Assert
            assert updater.universe_symbols is None

    def test_load_universe_symbols_no_config(self, temp_config: Path, mock_adapter_with_update):
        """Test when universe_file not configured."""
        # Arrange
        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Assert
            assert updater.universe_symbols is None


class TestDatasetUpdaterDryRun:
    """Tests for dry-run mode."""

    def test_update_symbol_dry_run_with_cache(self, temp_config: Path, mock_adapter_with_update, tmp_path: Path):
        """Test dry-run mode with existing cache."""
        # Arrange
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(exist_ok=True)
        symbol_dir = cache_dir / "AAPL"
        symbol_dir.mkdir()
        (symbol_dir / "data.parquet").touch()

        with patch("qs_trader.services.data.dataset_updater.DataSourceResolver") as mock_resolver_class:
            mock_resolver = MagicMock()
            mock_resolver.sources = {
                "test-dataset": {
                    "adapter": "test_adapter",
                    "cache_root": str(cache_dir),
                }
            }
            mock_resolver._get_adapter_class.return_value = mock_adapter_with_update

            mock_adapter_instance = MagicMock()
            mock_adapter_instance.update_to_latest.return_value = (
                5,
                date(2024, 1, 1),
                date(2024, 1, 5),
            )
            mock_resolver.resolve_by_dataset.return_value = mock_adapter_instance
            mock_resolver_class.return_value = mock_resolver

            updater = DatasetUpdater("test-dataset", str(temp_config))

            # Act
            result = updater.update_symbol("AAPL", dry_run=True)

            # Assert
            assert result.success is True
            mock_adapter_instance.update_to_latest.assert_called_once_with(dry_run=True)
