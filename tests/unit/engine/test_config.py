"""
Unit tests for qs_trader.engine.config module.

Tests cover configuration models, validation, and YAML loading.
"""

from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from qs_trader.engine.config import (
    BacktestConfig,
    ConfigLoadError,
    DataSelectionConfig,
    DataSourceConfig,
    FeatureConfig,
    RiskPolicyConfig,
    StrategyConfigItem,
    load_backtest_config,
)

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def valid_data_source_config() -> dict[str, Any]:
    """Provide valid data source configuration."""
    return {
        "name": "yahoo-us-equity-1d-csv",
        "universe": ["AAPL", "MSFT", "GOOGL"],
    }


@pytest.fixture
def valid_strategy_config() -> dict[str, Any]:
    """Provide valid strategy configuration."""
    return {
        "strategy_id": "momentum_20",
        "universe": ["AAPL", "MSFT"],
        "data_sources": ["yahoo-us-equity-1d-csv"],
        "config": {"lookback": 20, "warmup_bars": 21},
    }


@pytest.fixture
def valid_backtest_config() -> dict[str, Any]:
    """Provide valid complete backtest configuration."""
    return {
        "backtest_id": "test_backtest",
        "start_date": "2020-01-01",
        "end_date": "2023-12-31",
        "initial_equity": 100000,
        "replay_speed": 0.0,
        "data": {
            "sources": [
                {
                    "name": "yahoo-us-equity-1d-csv",
                    "universe": ["AAPL", "MSFT", "GOOGL"],
                }
            ]
        },
        "strategies": [
            {
                "strategy_id": "momentum_20",
                "universe": ["AAPL", "MSFT"],
                "data_sources": ["yahoo-us-equity-1d-csv"],
                "config": {"lookback": 20},
            }
        ],
        "risk_policy": {"name": "naive", "config": {"max_pct_position_size": 0.30}},
    }


@pytest.fixture
def temp_config_file(tmp_path: Path, valid_backtest_config: dict[str, Any]) -> Path:
    """Create a temporary config file with valid configuration."""
    config_path = tmp_path / "backtest_config.yaml"
    with config_path.open("w") as f:
        yaml.dump(valid_backtest_config, f)
    return config_path


# ============================================================================
# DataSourceConfig Tests
# ============================================================================


class TestDataSourceConfig:
    """Test suite for DataSourceConfig model."""

    def test_create_valid_config(self, valid_data_source_config: dict[str, Any]) -> None:
        """Test creating valid data source configuration."""
        # Arrange & Act
        config = DataSourceConfig(**valid_data_source_config)

        # Assert
        assert config.name == "yahoo-us-equity-1d-csv"
        assert config.universe == ["AAPL", "MSFT", "GOOGL"]

    def test_missing_required_field_raises_error(self) -> None:
        """Test missing required field raises validation error."""
        # Arrange
        invalid_config = {"name": "test-source"}

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            DataSourceConfig(**invalid_config)  # type: ignore[arg-type]
        assert "universe" in str(exc_info.value)

    def test_empty_universe_allowed(self) -> None:
        """Test empty universe list is allowed."""
        # Arrange
        config_dict = {"name": "test-source", "universe": []}

        # Act
        config = DataSourceConfig(**config_dict)  # type: ignore[arg-type]

        # Assert
        assert config.universe == []


# ============================================================================
# DataSelectionConfig Tests
# ============================================================================


class TestDataSelectionConfig:
    """Test suite for DataSelectionConfig model."""

    def test_create_valid_config(self, valid_data_source_config: dict[str, Any]) -> None:
        """Test creating valid data selection configuration."""
        # Arrange
        config_dict = {"sources": [valid_data_source_config]}

        # Act
        config = DataSelectionConfig(**config_dict)  # type: ignore[arg-type]

        # Assert
        assert len(config.sources) == 1
        assert config.sources[0].name == "yahoo-us-equity-1d-csv"

    def test_multiple_data_sources(self) -> None:
        """Test configuration with multiple data sources."""
        # Arrange
        config_dict = {
            "sources": [
                {"name": "source1", "universe": ["AAPL"]},
                {"name": "source2", "universe": ["MSFT", "GOOGL"]},
            ]
        }

        # Act
        config = DataSelectionConfig(**config_dict)  # type: ignore[arg-type]

        # Assert
        assert len(config.sources) == 2
        assert config.sources[0].name == "source1"
        assert config.sources[1].name == "source2"

    def test_empty_sources_raises_validation_error(self) -> None:
        """Test empty sources list raises validation error (min_length=1)."""
        # Arrange
        config_dict: dict[str, list[Any]] = {"sources": []}

        # Act & Assert - empty sources should raise validation error
        with pytest.raises(ValidationError) as exc_info:
            DataSelectionConfig(**config_dict)

        assert "at least 1 item" in str(exc_info.value)


# ============================================================================
# StrategyConfigItem Tests
# ============================================================================


class TestStrategyConfigItem:
    """Test suite for StrategyConfigItem model."""

    def test_create_valid_config(self, valid_strategy_config: dict[str, Any]) -> None:
        """Test creating valid strategy configuration."""
        # Arrange & Act
        config = StrategyConfigItem(**valid_strategy_config)

        # Assert
        assert config.strategy_id == "momentum_20"
        assert config.universe == ["AAPL", "MSFT"]
        assert config.data_sources == ["yahoo-us-equity-1d-csv"]
        assert config.config == {"lookback": 20, "warmup_bars": 21}

    def test_default_empty_config(self) -> None:
        """Test strategy config defaults to empty dict."""
        # Arrange
        minimal_config = {
            "strategy_id": "test_strategy",
            "universe": ["AAPL"],
            "data_sources": ["test-source"],
        }

        # Act
        config = StrategyConfigItem(**minimal_config)  # type: ignore[arg-type]

        # Assert
        assert config.config == {}

    def test_missing_required_fields_raises_error(self) -> None:
        """Test missing required fields raises validation error."""
        # Arrange
        invalid_config = {"strategy_id": "test"}

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            StrategyConfigItem(**invalid_config)  # type: ignore[arg-type]
        assert "universe" in str(exc_info.value)
        assert "data_sources" in str(exc_info.value)


# ============================================================================
# RiskPolicyConfig Tests
# ============================================================================


class TestRiskPolicyConfig:
    """Test suite for RiskPolicyConfig model."""

    def test_create_valid_config(self) -> None:
        """Test creating valid risk policy configuration."""
        # Arrange
        config_dict = {"name": "naive", "config": {"max_pct_position_size": 0.30}}

        # Act
        config = RiskPolicyConfig(**config_dict)  # type: ignore[arg-type]

        # Assert
        assert config.name == "naive"
        assert config.config == {"max_pct_position_size": 0.30}

    def test_default_empty_config(self) -> None:
        """Test risk policy config defaults to empty dict."""
        # Arrange
        minimal_config = {"name": "test_policy"}

        # Act
        config = RiskPolicyConfig(**minimal_config)  # type: ignore[arg-type]

        # Assert
        assert config.config == {}


# ============================================================================
# BacktestConfig Tests
# ============================================================================


class TestBacktestConfig:
    """Test suite for BacktestConfig model."""

    def test_create_valid_config(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test creating valid backtest configuration."""
        # Arrange & Act
        config = BacktestConfig(**valid_backtest_config)

        # Assert
        assert config.start_date == datetime(2020, 1, 1)
        assert config.end_date == datetime(2023, 12, 31)
        assert config.initial_equity == Decimal("100000")
        assert config.replay_speed == 0.0
        assert len(config.data.sources) == 1
        assert len(config.strategies) == 1
        assert config.risk_policy.name == "naive"

    def test_all_symbols_property(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test all_symbols property returns symbols from single source."""
        # Arrange
        config_dict = valid_backtest_config.copy()
        config_dict["data"]["sources"] = [
            {"name": "source1", "universe": ["AAPL", "MSFT", "GOOGL"]},
        ]

        # Act
        config = BacktestConfig(**config_dict)

        # Assert
        assert config.all_symbols == {"AAPL", "MSFT", "GOOGL"}

    def test_multiple_sources_raises_validation_error(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test multiple data sources raises validation error."""
        # Arrange
        config_dict = valid_backtest_config.copy()
        config_dict["data"]["sources"] = [
            {"name": "source1", "universe": ["AAPL", "MSFT"]},
            {"name": "source2", "universe": ["GOOGL", "AAPL"]},
        ]

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            BacktestConfig(**config_dict)

        assert "Multiple data sources not yet supported" in str(exc_info.value)
        assert "source1" in str(exc_info.value)
        assert "source2" in str(exc_info.value)

    def test_date_validation_end_before_start_raises_error(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test end_date before start_date raises validation error."""
        # Arrange
        invalid_config = valid_backtest_config.copy()
        invalid_config["start_date"] = "2023-12-31"
        invalid_config["end_date"] = "2020-01-01"

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            BacktestConfig(**invalid_config)
        assert "end_date must be after start_date" in str(exc_info.value)

    def test_date_validation_equal_dates_raises_error(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test equal start and end dates raises validation error."""
        # Arrange
        invalid_config = valid_backtest_config.copy()
        invalid_config["start_date"] = "2020-01-01"
        invalid_config["end_date"] = "2020-01-01"

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            BacktestConfig(**invalid_config)
        assert "end_date must be after start_date" in str(exc_info.value)

    def test_strategy_universe_validation_valid_subset(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test strategy universe must be subset of data sources."""
        # Arrange - strategy uses subset of data universe
        config_dict = valid_backtest_config.copy()
        config_dict["data"]["sources"] = [{"name": "source1", "universe": ["AAPL", "MSFT", "GOOGL"]}]
        config_dict["strategies"][0]["universe"] = ["AAPL", "MSFT"]

        # Act
        config = BacktestConfig(**config_dict)

        # Assert
        assert config.strategies[0].universe == ["AAPL", "MSFT"]

    def test_strategy_universe_validation_invalid_symbols_raises_error(
        self, valid_backtest_config: dict[str, Any]
    ) -> None:
        """Test strategy universe with symbols not in data sources raises error."""
        # Arrange - strategy uses symbols not in data
        invalid_config = valid_backtest_config.copy()
        invalid_config["data"]["sources"] = [{"name": "source1", "universe": ["AAPL"]}]
        invalid_config["strategies"][0]["universe"] = ["AAPL", "TSLA"]  # TSLA not loaded

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            BacktestConfig(**invalid_config)
        assert "TSLA" in str(exc_info.value)
        assert "not in data sources" in str(exc_info.value)

    def test_replay_speed_negative_one_valid(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test replay_speed=-1.0 is valid (silent mode)."""
        # Arrange
        config_dict = valid_backtest_config.copy()
        config_dict["replay_speed"] = -1.0

        # Act
        config = BacktestConfig(**config_dict)

        # Assert
        assert config.replay_speed == -1.0

    def test_replay_speed_below_negative_one_raises_error(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test replay_speed < -1.0 raises validation error."""
        # Arrange
        invalid_config = valid_backtest_config.copy()
        invalid_config["replay_speed"] = -2.0

        # Act & Assert
        with pytest.raises(ValidationError) as exc_info:
            BacktestConfig(**invalid_config)
        assert "greater than or equal to -1" in str(exc_info.value).lower()

    def test_initial_equity_as_integer(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test initial_equity accepts integer and converts to Decimal."""
        # Arrange
        config_dict = valid_backtest_config.copy()
        config_dict["initial_equity"] = 50000

        # Act
        config = BacktestConfig(**config_dict)

        # Assert
        assert config.initial_equity == Decimal("50000")
        assert isinstance(config.initial_equity, Decimal)

    def test_initial_equity_as_string(self, valid_backtest_config: dict[str, Any]) -> None:
        """Test initial_equity accepts string and converts to Decimal."""
        # Arrange
        config_dict = valid_backtest_config.copy()
        config_dict["initial_equity"] = "75000.50"

        # Act
        config = BacktestConfig(**config_dict)

        # Assert
        assert config.initial_equity == Decimal("75000.50")


# ============================================================================
# load_backtest_config Tests
# ============================================================================


class TestLoadBacktestConfig:
    """Test suite for load_backtest_config function."""

    def test_load_valid_config_file(self, temp_config_file: Path) -> None:
        """Test loading valid configuration from YAML file."""
        # Arrange & Act
        config = load_backtest_config(temp_config_file)

        # Assert
        assert isinstance(config, BacktestConfig)
        assert config.start_date == datetime(2020, 1, 1)
        assert config.end_date == datetime(2023, 12, 31)
        assert config.initial_equity == Decimal("100000")

    def test_load_config_with_string_path(self, temp_config_file: Path) -> None:
        """Test loading configuration with string path."""
        # Arrange & Act
        config = load_backtest_config(str(temp_config_file))

        # Assert
        assert isinstance(config, BacktestConfig)

    def test_load_nonexistent_file_raises_error(self) -> None:
        """Test loading nonexistent file raises ConfigLoadError."""
        # Arrange
        nonexistent_path = Path("/nonexistent/path/config.yaml")

        # Act & Assert
        with pytest.raises(ConfigLoadError) as exc_info:
            load_backtest_config(nonexistent_path)
        assert "Config file not found" in str(exc_info.value)

    def test_load_invalid_yaml_raises_error(self, tmp_path: Path) -> None:
        """Test loading invalid YAML raises ConfigLoadError."""
        # Arrange
        invalid_yaml_path = tmp_path / "invalid.yaml"
        with invalid_yaml_path.open("w") as f:
            f.write("invalid: yaml: content: [")

        # Act & Assert
        with pytest.raises(ConfigLoadError) as exc_info:
            load_backtest_config(invalid_yaml_path)
        assert "Invalid YAML" in str(exc_info.value)

    def test_load_non_dict_yaml_raises_error(self, tmp_path: Path) -> None:
        """Test loading YAML with non-dict content raises ConfigLoadError."""
        # Arrange
        invalid_config_path = tmp_path / "list_config.yaml"
        with invalid_config_path.open("w") as f:
            yaml.dump(["item1", "item2"], f)

        # Act & Assert
        with pytest.raises(ConfigLoadError) as exc_info:
            load_backtest_config(invalid_config_path)
        assert "must be a YAML dictionary" in str(exc_info.value)

    def test_load_invalid_config_raises_error(self, tmp_path: Path) -> None:
        """Test loading YAML with invalid config data raises ConfigLoadError."""
        # Arrange
        invalid_config_path = tmp_path / "invalid_config.yaml"
        invalid_data = {
            "start_date": "2020-01-01",
            # Missing required fields
        }
        with invalid_config_path.open("w") as f:
            yaml.dump(invalid_data, f)

        # Act & Assert
        with pytest.raises(ConfigLoadError) as exc_info:
            load_backtest_config(invalid_config_path)
        assert "Config validation failed" in str(exc_info.value)


# ============================================================================
# ConfigLoadError Tests
# ============================================================================


class TestConfigLoadError:
    """Test suite for ConfigLoadError exception."""

    def test_create_error_with_message(self) -> None:
        """Test creating ConfigLoadError with message."""
        # Arrange
        error_msg = "Test error message"

        # Act
        error = ConfigLoadError(error_msg)

        # Assert
        assert str(error) == error_msg
        assert isinstance(error, Exception)

    def test_raise_and_catch_error(self) -> None:
        """Test raising and catching ConfigLoadError."""

        # Arrange
        def raise_error() -> None:
            raise ConfigLoadError("Test error")

        # Act & Assert
        with pytest.raises(ConfigLoadError) as exc_info:
            raise_error()
        assert "Test error" in str(exc_info.value)


# ============================================================================
# FeatureConfig Tests
# ============================================================================


class TestFeatureConfig:
    """Tests for the typed FeatureConfig Pydantic model."""

    def test_defaults(self) -> None:
        """FeatureConfig should have sensible defaults."""
        cfg = FeatureConfig()
        assert cfg.feature_version == "v1"
        assert cfg.regime_version == "v1"
        assert cfg.columns is None
        assert cfg.connect_timeout == 10
        assert cfg.query_timeout == 30

    def test_custom_values(self) -> None:
        """Explicit values should be stored correctly."""
        cfg = FeatureConfig(
            feature_version="v2",
            regime_version="v2",
            columns=["trend_strength", "trend_regime"],
            connect_timeout=5,
            query_timeout=60,
        )
        assert cfg.feature_version == "v2"
        assert cfg.columns == ["trend_strength", "trend_regime"]
        assert cfg.connect_timeout == 5

    def test_timeout_must_be_positive(self) -> None:
        """connect_timeout and query_timeout must be >= 1."""
        with pytest.raises(ValidationError):
            FeatureConfig(connect_timeout=0)
        with pytest.raises(ValidationError):
            FeatureConfig(query_timeout=0)

    def test_model_dump_is_passable_to_feature_service(self) -> None:
        """model_dump() output should be a plain dict with expected keys."""
        cfg = FeatureConfig(feature_version="v1", columns=["trend_strength"])
        d = cfg.model_dump()
        assert isinstance(d, dict)
        assert d["feature_version"] == "v1"
        assert d["columns"] == ["trend_strength"]

    def test_backtest_config_parses_feature_config_from_dict(self, tmp_path: Path) -> None:
        """BacktestConfig should coerce a nested feature_config dict into FeatureConfig."""
        raw: dict = {
            "backtest_id": "feat_test",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "initial_equity": "100000",
            "data": {"sources": [{"name": "ds1", "universe": ["AAPL"]}]},
            "strategies": [{"strategy_id": "s", "universe": ["AAPL"], "data_sources": ["ds1"]}],
            "risk_policy": {"name": "naive", "config": {}},
            "feature_config": {"feature_version": "v2", "regime_version": "v2"},
        }
        cfg = BacktestConfig(**raw)
        assert isinstance(cfg.feature_config, FeatureConfig)
        assert cfg.feature_config.feature_version == "v2"

    def test_backtest_config_feature_config_none_by_default(self) -> None:
        """feature_config defaults to None when omitted."""
        raw: dict = {
            "backtest_id": "no_feat",
            "start_date": "2024-01-01",
            "end_date": "2024-12-31",
            "initial_equity": "100000",
            "data": {"sources": [{"name": "ds1", "universe": ["AAPL"]}]},
            "strategies": [{"strategy_id": "s", "universe": ["AAPL"], "data_sources": ["ds1"]}],
            "risk_policy": {"name": "naive", "config": {}},
        }
        cfg = BacktestConfig(**raw)
        assert cfg.feature_config is None
