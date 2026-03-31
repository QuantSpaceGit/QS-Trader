"""Tests for data source validator."""

from pathlib import Path
from unittest.mock import patch

import pytest

from qs_trader.services.data.source_validator import (
    DataSourceValidationError,
    DataSourceValidator,
    validate_data_sources,
)


@pytest.fixture
def valid_source_config() -> dict:
    """Valid source configuration."""
    return {
        "provider": "test_provider",
        "asset_class": "equity",
        "data_type": "ohlcv",
        "frequency": "1d",
        "region": "US",
        "adjustment_mode": "unadjusted",
        "adapter": "test_adapter",
        "root_path": "data/sample",
    }


@pytest.fixture
def valid_yaml_config(tmp_path: Path) -> Path:
    """Create valid YAML config file."""
    config_content = """
data_sources:
  test-data-source:
    provider: test_provider
    asset_class: equity
    data_type: ohlcv
    frequency: 1d
    region: US
    adjustment_mode: unadjusted
    adapter: test_adapter
    root_path: "data/sample"
"""
    config_path = tmp_path / "data_sources.yaml"
    config_path.write_text(config_content)
    return config_path


class TestDataSourceValidator:
    """Test DataSourceValidator."""

    def test_validate_valid_source(self, valid_source_config: dict) -> None:
        """Test validating a valid source."""
        # Mock the adapter registry to accept test_adapter
        with patch.object(DataSourceValidator, "_get_valid_adapters", return_value=["test_adapter", "yahoo_csv"]):
            # Should not raise
            DataSourceValidator.validate_source("test-data-source", valid_source_config)

    def test_validate_invalid_name_format(self, valid_source_config: dict) -> None:
        """Test validation fails for invalid name format."""
        with pytest.raises(DataSourceValidationError, match="Invalid name format"):
            DataSourceValidator.validate_source("ALGOSEEK-US-EQUITY", valid_source_config)

    def test_validate_missing_required_field(self) -> None:
        """Test validation fails when required field missing."""
        config = {
            "provider": "test_provider",
            "asset_class": "equity",
            # Missing: data_type, frequency
            "adapter": "test_adapter",
        }
        with pytest.raises(DataSourceValidationError, match="Missing required metadata fields"):
            DataSourceValidator.validate_source("test-equity-1d", config)

    def test_validate_invalid_asset_class(self, valid_source_config: dict) -> None:
        """Test validation fails for invalid asset_class."""
        config = valid_source_config.copy()
        config["asset_class"] = "invalid_asset"
        with pytest.raises(DataSourceValidationError, match="Invalid asset_class"):
            DataSourceValidator.validate_source("test-data-source", config)

    def test_validate_invalid_data_type(self, valid_source_config: dict) -> None:
        """Test validation fails for invalid data_type."""
        config = valid_source_config.copy()
        config["data_type"] = "invalid_type"
        with pytest.raises(DataSourceValidationError, match="Invalid data_type"):
            DataSourceValidator.validate_source("test-data-source", config)

    def test_validate_invalid_frequency_format(self, valid_source_config: dict) -> None:
        """Test validation fails for invalid frequency format."""
        config = valid_source_config.copy()
        config["frequency"] = "daily"  # Should be "1d"
        with pytest.raises(DataSourceValidationError, match="Invalid frequency format"):
            DataSourceValidator.validate_source("test-data-source", config)

    def test_validate_missing_adapter(self, valid_source_config: dict) -> None:
        """Test validation fails when adapter field missing."""
        config = valid_source_config.copy()
        del config["adapter"]
        with pytest.raises(DataSourceValidationError, match="Missing required metadata fields"):
            DataSourceValidator.validate_source("test-data-source", config)

    def test_validate_valid_frequency_formats(self, valid_source_config: dict) -> None:
        """Test various valid frequency formats."""
        valid_frequencies = ["1m", "5m", "15m", "1h", "4h", "1d"]
        with patch.object(DataSourceValidator, "_get_valid_adapters", return_value=["test_adapter", "yahoo_csv"]):
            for freq in valid_frequencies:
                config = valid_source_config.copy()
                config["frequency"] = freq
                # Should not raise
                DataSourceValidator.validate_source(f"test-source-{freq}", config)


class TestFileValidation:
    """Test validation of entire config files."""

    def test_validate_valid_file(self, valid_yaml_config: Path) -> None:
        """Test validating a valid config file."""
        # Mock the adapter registry to accept test_adapter
        with patch.object(DataSourceValidator, "_get_valid_adapters", return_value=["test_adapter", "yahoo_csv"]):
            # Should not raise
            DataSourceValidator.validate_file(valid_yaml_config)

    def test_validate_file_not_found(self) -> None:
        """Test validation fails when file doesn't exist."""
        with pytest.raises(FileNotFoundError):
            DataSourceValidator.validate_file("nonexistent.yaml")

    def test_validate_missing_data_sources_key(self, tmp_path: Path) -> None:
        """Test validation fails when data_sources key missing."""
        config_path = tmp_path / "data_sources.yaml"
        config_path.write_text("invalid: config")

        with pytest.raises(DataSourceValidationError, match="Missing 'data_sources'"):
            DataSourceValidator.validate_file(config_path)

    def test_validate_empty_data_sources(self, tmp_path: Path) -> None:
        """Test validation fails when data_sources is empty."""
        config_path = tmp_path / "data_sources.yaml"
        config_path.write_text("data_sources: {}")

        with pytest.raises(DataSourceValidationError, match="No data sources defined"):
            DataSourceValidator.validate_file(config_path)

    def test_validate_duplicate_configurations(self, tmp_path: Path) -> None:
        """Test detection of duplicate configurations."""
        config_content = """
data_sources:
  test-equity-1d-v1:
    provider: test_provider
    asset_class: equity
    data_type: ohlcv
    frequency: 1d
    region: US
    adjustment_mode: unadjusted
    adapter: test_adapter
    root_path: "data/v1"

  test-equity-1d-v2:
    provider: test_provider
    asset_class: equity
    data_type: ohlcv
    frequency: 1d
    region: US
    adjustment_mode: unadjusted
    adapter: test_adapter
    root_path: "data/v2"  # Different path but same metadata
"""
        config_path = tmp_path / "data_sources.yaml"
        config_path.write_text(config_content)

        with pytest.raises(DataSourceValidationError, match="Duplicate configurations"):
            DataSourceValidator.validate_file(config_path)

    def test_validate_multiple_errors(self, tmp_path: Path) -> None:
        """Test validation reports multiple errors."""
        config_content = """
data_sources:
  INVALID-NAME:
    provider: test_provider
    asset_class: invalid_asset
    # Missing: data_type, frequency
    adapter: test_adapter
"""
        config_path = tmp_path / "data_sources.yaml"
        config_path.write_text(config_content)

        with pytest.raises(DataSourceValidationError) as exc_info:
            DataSourceValidator.validate_file(config_path)

        error_msg = str(exc_info.value)
        assert "Invalid name format" in error_msg
        assert "Missing required metadata fields" in error_msg


class TestConvenienceFunction:
    """Test validate_data_sources convenience function."""

    def test_validate_data_sources_function(self, valid_yaml_config: Path) -> None:
        """Test convenience function."""
        # Mock the adapter registry to accept test_adapter
        with patch.object(DataSourceValidator, "_get_valid_adapters", return_value=["test_adapter", "yahoo_csv"]):
            # Should not raise
            validate_data_sources(valid_yaml_config)

    def test_validate_data_sources_invalid(self, tmp_path: Path) -> None:
        """Test convenience function with invalid config."""
        config_path = tmp_path / "data_sources.yaml"
        config_path.write_text("data_sources: {}")

        with pytest.raises(DataSourceValidationError):
            validate_data_sources(config_path)
