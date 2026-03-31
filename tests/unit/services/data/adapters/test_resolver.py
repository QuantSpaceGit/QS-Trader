"""Tests for DataSourceResolver."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from qs_trader.services.data.adapters.resolver import DataSourceResolver
from qs_trader.services.data.models import Instrument
from qs_trader.services.data.source_selector import AssetClass, DataSourceSelector, DataType


@pytest.fixture
def temp_config(tmp_path: Path) -> Path:
    """Create temporary config file."""
    config_content = """
data_sources:
  test-dataset-1d:
    provider: test_provider
    asset_class: equity
    data_type: ohlcv
    frequency: 1d
    region: US
    adjustment_mode: unadjusted
    adapter: test_adapter
    root_path: "data/sample"

  binance-crypto-1m:
    provider: binance
    asset_class: crypto
    data_type: ohlcv
    frequency: 1m
    adapter: binance_api

  polygon-us-equity-1d:
    provider: polygon
    asset_class: equity
    data_type: ohlcv
    frequency: 1d
    region: US
    adjustment_mode: adjusted
    adapter: polygonOHLC
    cache_root: "data/cache"
"""
    config_path = tmp_path / "data_sources.yaml"
    config_path.write_text(config_content)
    return config_path


@pytest.fixture
def instrument() -> Instrument:
    """Create test instrument."""
    return Instrument(symbol="AAPL")


class TestResolverBySelectorMatching:
    """Test DataSourceSelector matching logic."""

    def test_match_by_provider(self, temp_config: Path, instrument: Instrument) -> None:
        """Test matching by provider only."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(provider="polygon")

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_selector(selector, instrument)

            # Should match polygon source
            assert mock_create.called
            call_args = mock_create.call_args[0]
            assert call_args[0] == "polygon-us-equity-1d"  # source_name

    def test_match_by_asset_class(self, temp_config: Path, instrument: Instrument) -> None:
        """Test matching by asset class only."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(asset_class=AssetClass.CRYPTO)

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_selector(selector, instrument)

            # Should match binance source
            assert mock_create.called
            call_args = mock_create.call_args[0]
            assert call_args[0] == "binance-crypto-1m"  # source_name

    def test_match_by_multiple_criteria(self, temp_config: Path, instrument: Instrument) -> None:
        """Test matching with multiple criteria."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(
            asset_class=AssetClass.EQUITY,
            frequency="1d",
            region="US",
        )

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_selector(selector, instrument)

            # Should match test-dataset-1d or polygon
            assert mock_create.called
            call_args = mock_create.call_args[0]
            assert call_args[0] in ["test-dataset-1d", "polygon-us-equity-1d"]

    def test_match_by_provider_and_asset_class(self, temp_config: Path, instrument: Instrument) -> None:
        """Test matching by provider and asset class."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(
            provider="test_provider",
            asset_class=AssetClass.EQUITY,
        )

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_selector(selector, instrument)

            # Should match test dataset source
            assert mock_create.called
            call_args = mock_create.call_args[0]
            assert call_args[0] == "test-dataset-1d"

    def test_no_match_raises_error(self, temp_config: Path, instrument: Instrument) -> None:
        """Test that no match raises ValueError."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(
            provider="nonexistent",
            asset_class=AssetClass.EQUITY,
        )

        with pytest.raises(ValueError, match="No data source matches selector"):
            resolver.resolve_by_selector(selector, instrument)

    def test_match_by_data_type(self, temp_config: Path, instrument: Instrument) -> None:
        """Test matching by data type."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(
            asset_class=AssetClass.EQUITY,
            data_type=DataType.OHLCV,
        )

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_selector(selector, instrument)

            # Should match test-dataset-1d or polygon
            assert mock_create.called
            call_args = mock_create.call_args[0]
            assert call_args[0] in ["test-dataset-1d", "polygon-us-equity-1d"]

    def test_match_by_frequency(self, temp_config: Path, instrument: Instrument) -> None:
        """Test matching by frequency."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(frequency="1m")

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_selector(selector, instrument)

            # Should match binance (1m frequency)
            assert mock_create.called
            call_args = mock_create.call_args[0]
            assert call_args[0] == "binance-crypto-1m"


class TestResolverBySelectorFallback:
    """Test fallback provider functionality."""

    def test_fallback_on_primary_failure(self, temp_config: Path, instrument: Instrument) -> None:
        """Test that fallback provider is tried when primary fails."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(
            provider="polygon",
            asset_class=AssetClass.EQUITY,
            fallback_providers=["test_provider"],
        )

        with patch.object(resolver, "_create_adapter") as mock_create:
            # First call (polygon) raises error, second call (test_provider) succeeds
            mock_create.side_effect = [
                Exception("Polygon API error"),
                MagicMock(),  # test_provider succeeds
            ]

            result = resolver.resolve_by_selector(selector, instrument)

            # Should have tried both providers
            assert mock_create.call_count == 2
            assert result is not None

    def test_fallback_succeeds_after_primary_failure(self, temp_config: Path, instrument: Instrument) -> None:
        """Test that fallback provider successfully returns adapter after primary fails."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(
            asset_class=AssetClass.EQUITY,
            fallback_providers=["test_provider"],
        )

        mock_adapter = MagicMock()

        with (
            patch.object(resolver, "_create_adapter") as mock_create,
            patch("qs_trader.services.data.adapters.resolver.logger") as mock_logger,
        ):
            # First call fails, second succeeds
            mock_create.side_effect = [
                Exception("Primary source error"),
                mock_adapter,
            ]

            result = resolver.resolve_by_selector(selector, instrument)

            # Should have logged trying fallback
            fallback_logged = any(call[0][0] == "resolver.trying_fallback" for call in mock_logger.info.call_args_list)
            assert fallback_logged, "Should log fallback attempt"

            # Should return the fallback adapter
            assert result is mock_adapter

    def test_no_fallback_when_primary_succeeds(self, temp_config: Path, instrument: Instrument) -> None:
        """Test that fallback is not tried when primary succeeds."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(
            provider="polygon",
            asset_class=AssetClass.EQUITY,
            fallback_providers=["test_provider"],
        )

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_selector(selector, instrument)

            # Should only call primary
            assert mock_create.call_count == 1
            call_args = mock_create.call_args[0]
            assert call_args[0] == "polygon-us-equity-1d"

    def test_multiple_fallbacks(self, temp_config: Path, instrument: Instrument) -> None:
        """Test multiple fallback providers."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(
            provider="nonexistent",
            asset_class=AssetClass.EQUITY,
            fallback_providers=["polygon", "test_provider"],
        )

        with patch.object(resolver, "_create_adapter") as mock_create:
            # Primary fails (nonexistent not in config), polygon succeeds
            mock_create.return_value = MagicMock()

            # This will fail on primary (no match), then try polygon
            with pytest.raises(ValueError):
                # Primary won't match, so will raise before trying fallbacks
                resolver.resolve_by_selector(selector, instrument)


class TestResolverMultipleMatches:
    """Test behavior when multiple sources match."""

    def test_logs_multiple_matches(self, temp_config: Path, instrument: Instrument) -> None:
        """Test that multiple matches are logged."""
        resolver = DataSourceResolver(str(temp_config))
        # This will match both test dataset and polygon
        selector = DataSourceSelector(asset_class=AssetClass.EQUITY)

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()

            with patch("qs_trader.services.data.adapters.resolver.logger") as mock_logger:
                resolver.resolve_by_selector(selector, instrument)

                # Should log multiple matches
                mock_logger.info.assert_called()
                call_args = mock_logger.info.call_args
                assert "multiple_matches" in str(call_args)

    def test_uses_first_match(self, temp_config: Path, instrument: Instrument) -> None:
        """Test that first match is used."""
        resolver = DataSourceResolver(str(temp_config))
        selector = DataSourceSelector(asset_class=AssetClass.EQUITY)

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_selector(selector, instrument)

            # Should use first match
            assert mock_create.called
            call_args = mock_create.call_args[0]
            # First match could be either depending on dict order
            assert call_args[0] in ["test-dataset-1d", "polygon-us-equity-1d"]


class TestConfigFinding:
    """Test configuration file finding logic."""

    def test_explicit_path_found(self, temp_config: Path) -> None:
        """Test finding config with explicit path."""
        resolver = DataSourceResolver(str(temp_config))
        assert resolver.config_path == temp_config

    def test_explicit_path_not_found(self) -> None:
        """Test error when explicit path doesn't exist."""
        with pytest.raises(FileNotFoundError, match="Config file not found"):
            DataSourceResolver("/nonexistent/path/data_sources.yaml")

    def test_default_locations_project_relative(self, tmp_path: Path, monkeypatch) -> None:
        """Test finding config in project-relative location."""
        # Create config in project-relative location
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_path = config_dir / "data_sources.yaml"
        config_path.write_text("""
data_sources:
  test:
    adapter: test_adapter
""")

        # Change to temp directory
        monkeypatch.chdir(tmp_path)

        resolver = DataSourceResolver()
        assert resolver.config_path.name == "data_sources.yaml"

    def test_default_locations_not_found(self, tmp_path: Path, monkeypatch) -> None:
        """Test error when config not found in any default location."""
        # Change to empty directory
        monkeypatch.chdir(tmp_path)

        with pytest.raises(FileNotFoundError, match="data_sources.yaml not found in any location"):
            DataSourceResolver()


class TestConfigLoading:
    """Test configuration loading and validation."""

    def test_load_valid_config(self, temp_config: Path) -> None:
        """Test loading valid configuration."""
        resolver = DataSourceResolver(str(temp_config))
        assert "test-dataset-1d" in resolver.sources
        assert "polygon-us-equity-1d" in resolver.sources

    def test_load_invalid_yaml_structure(self, tmp_path: Path) -> None:
        """Test error with invalid YAML structure."""
        config_path = tmp_path / "invalid.yaml"
        config_path.write_text("""
sources:  # Wrong key - should be 'data_sources'
  test:
    adapter: test_adapter
""")

        with pytest.raises(ValueError, match="Invalid config format.*Expected 'data_sources' key"):
            DataSourceResolver(str(config_path))

    def test_load_config_missing_adapter_field(self, tmp_path: Path) -> None:
        """Test error when source missing adapter field."""
        config_path = tmp_path / "missing_adapter.yaml"
        config_path.write_text("""
data_sources:
  test:
    provider: test_provider
    # Missing required 'adapter' field
""")

        with pytest.raises(ValueError, match="Data source 'test' missing required 'adapter' field"):
            DataSourceResolver(str(config_path))

    def test_load_config_not_dict(self, tmp_path: Path) -> None:
        """Test error when config is not a dict."""
        config_path = tmp_path / "not_dict.yaml"
        config_path.write_text("- item1\n- item2")  # List, not dict

        with pytest.raises(ValueError, match="Invalid config format"):
            DataSourceResolver(str(config_path))


class TestEnvVarSubstitution:
    """Test environment variable substitution."""

    def test_substitute_simple_var(self, temp_config: Path, monkeypatch) -> None:
        """Test substituting simple environment variable."""
        monkeypatch.setenv("TEST_ROOT", "/custom/path")

        config_path = temp_config.parent / "env_test.yaml"
        config_path.write_text("""
data_sources:
  test:
    adapter: test_adapter
    root_path: "${TEST_ROOT}"
""")

        resolver = DataSourceResolver(str(config_path))
        config = resolver.get_source_config("test")
        assert config["root_path"] == "/custom/path"

    def test_substitute_var_with_default(self, temp_config: Path) -> None:
        """Test substituting variable with default value."""
        config_path = temp_config.parent / "default_test.yaml"
        config_path.write_text("""
data_sources:
  test:
    adapter: test_adapter
    api_key: "${MISSING_VAR:-default_key}"
""")

        resolver = DataSourceResolver(str(config_path))
        config = resolver.get_source_config("test")
        assert config["api_key"] == "default_key"

    def test_substitute_var_missing_no_default(self, temp_config: Path) -> None:
        """Test error when variable missing and no default."""
        config_path = temp_config.parent / "no_default.yaml"
        config_path.write_text("""
data_sources:
  test:
    adapter: test_adapter
    api_key: "${MISSING_REQUIRED_VAR}"
""")

        resolver = DataSourceResolver(str(config_path))

        with pytest.raises(KeyError, match="MISSING_REQUIRED_VAR"):
            resolver.get_source_config("test")

    def test_substitute_nested_config(self, temp_config: Path, monkeypatch) -> None:
        """Test substitution in nested configuration."""
        monkeypatch.setenv("DB_HOST", "localhost")
        monkeypatch.setenv("DB_PORT", "5432")

        config_path = temp_config.parent / "nested_test.yaml"
        config_path.write_text("""
data_sources:
  test:
    adapter: test_adapter
    connection:
      host: "${DB_HOST}"
      port: "${DB_PORT}"
      database: "market_data"
""")

        resolver = DataSourceResolver(str(config_path))
        config = resolver.get_source_config("test")
        assert config["connection"]["host"] == "localhost"
        assert config["connection"]["port"] == "5432"
        assert config["connection"]["database"] == "market_data"

    def test_substitute_non_env_var_string(self, temp_config: Path) -> None:
        """Test that non-env-var strings are not substituted."""
        config_path = temp_config.parent / "normal_string.yaml"
        config_path.write_text("""
data_sources:
  test:
    adapter: test_adapter
    description: "This has ${SYNTAX} but doesn't start/end correctly"
    path: "normal/path"
""")

        resolver = DataSourceResolver(str(config_path))
        config = resolver.get_source_config("test")
        # Should not substitute strings that don't match full pattern
        assert config["description"] == "This has ${SYNTAX} but doesn't start/end correctly"
        assert config["path"] == "normal/path"


class TestAdapterClassLoading:
    """Test dynamic adapter class loading."""

    def test_load_known_adapter_yahoo(self, temp_config: Path) -> None:
        """Test loading yahoo_csv adapter."""
        resolver = DataSourceResolver(str(temp_config))
        adapter_class = resolver._get_adapter_class("yahoo_csv")
        assert adapter_class.__name__ == "YahooCSVDataAdapter"

    def test_load_unknown_adapter(self, temp_config: Path) -> None:
        """Test error when loading unknown adapter."""
        from qs_trader.libraries.registry import ComponentNotFoundError

        resolver = DataSourceResolver(str(temp_config))

        with pytest.raises(ComponentNotFoundError, match="adapter 'nonexistent_adapter' not found"):
            resolver._get_adapter_class("nonexistent_adapter")

    def test_adapter_caching(self, temp_config: Path) -> None:
        """Test that adapter classes are cached in the registry."""
        resolver = DataSourceResolver(str(temp_config))

        # Load twice
        adapter_class1 = resolver._get_adapter_class("yahoo_csv")
        adapter_class2 = resolver._get_adapter_class("yahoo_csv")

        # Should be same object (cached in registry)
        assert adapter_class1 is adapter_class2
        assert "yahoo_csv" in resolver.adapter_registry._registry


class TestResolveByDataset:
    """Test resolve_by_dataset method."""

    def test_resolve_existing_dataset(self, temp_config: Path, instrument: Instrument) -> None:
        """Test resolving with existing dataset name."""
        resolver = DataSourceResolver(str(temp_config))

        with patch.object(resolver, "_create_adapter") as mock_create:
            mock_create.return_value = MagicMock()
            resolver.resolve_by_dataset("polygon-us-equity-1d", instrument)

            # Should call _create_adapter with correct params
            assert mock_create.called
            call_args = mock_create.call_args[0]
            assert call_args[0] == "polygon-us-equity-1d"  # dataset name
            assert call_args[2] == instrument

    def test_resolve_nonexistent_dataset(self, temp_config: Path, instrument: Instrument) -> None:
        """Test error when dataset not configured."""
        resolver = DataSourceResolver(str(temp_config))

        with pytest.raises(KeyError, match="Dataset 'nonexistent' not configured"):
            resolver.resolve_by_dataset("nonexistent", instrument)

    def test_resolve_lists_available_datasets(self, temp_config: Path, instrument: Instrument) -> None:
        """Test that error message lists available datasets."""
        resolver = DataSourceResolver(str(temp_config))

        with pytest.raises(KeyError) as exc_info:
            resolver.resolve_by_dataset("nonexistent", instrument)

        error_msg = str(exc_info.value)
        assert "test-dataset-1d" in error_msg
        assert "polygon-us-equity-1d" in error_msg


class TestUtilityMethods:
    """Test utility methods."""

    def test_list_sources(self, temp_config: Path) -> None:
        """Test listing all data sources."""
        resolver = DataSourceResolver(str(temp_config))
        sources = resolver.list_sources()

        assert isinstance(sources, list)
        assert "test-dataset-1d" in sources
        assert "polygon-us-equity-1d" in sources
        assert "binance-crypto-1m" in sources
        assert len(sources) == 3

    def test_get_source_config_existing(self, temp_config: Path) -> None:
        """Test getting configuration for existing source."""
        resolver = DataSourceResolver(str(temp_config))
        config = resolver.get_source_config("polygon-us-equity-1d")

        assert config["provider"] == "polygon"
        assert config["adapter"] == "polygonOHLC"
        assert config["asset_class"] == "equity"

    def test_get_source_config_nonexistent(self, temp_config: Path) -> None:
        """Test error when getting config for nonexistent source."""
        resolver = DataSourceResolver(str(temp_config))

        with pytest.raises(KeyError, match="Data source 'nonexistent' not configured"):
            resolver.get_source_config("nonexistent")

    def test_get_source_config_applies_env_substitution(self, temp_config: Path, monkeypatch) -> None:
        """Test that get_source_config applies environment variable substitution."""
        monkeypatch.setenv("TEST_PATH", "/custom/data")

        config_path = temp_config.parent / "env_sub.yaml"
        config_path.write_text("""
data_sources:
  test:
    adapter: test_adapter
    root_path: "${TEST_PATH}"
""")

        resolver = DataSourceResolver(str(config_path))
        config = resolver.get_source_config("test")

        assert config["root_path"] == "/custom/data"
