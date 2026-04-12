"""
Unit tests for system/config.py - Minimal system configuration.

Tests the minimal configuration structure supporting DataService-only engine:
- DataServiceConfig: System-wide data handling (HOW)
- OutputConfig: Results directory and event store
- LoggingConfig: Logging configuration
- SystemConfig: Container with load(), _from_dict(), merge, env substitution
- Singleton functions: get_system_config(), reload_system_config()
"""

from pathlib import Path

from qs_trader.system.config import (
    DatabaseOutputConfig,
    DataServiceConfig,
    LoggingConfig,
    OutputConfig,
    SystemConfig,
    _deep_merge,
    _substitute_env_vars,
    get_system_config,
    reload_system_config,
)


class TestDataServiceConfig:
    """Test DataServiceConfig dataclass."""

    def test_create_with_defaults(self):
        """Test DataServiceConfig uses correct defaults."""
        # Arrange & Act
        config = DataServiceConfig()

        # Assert
        assert config.sources_config == "config/data_sources.yaml"
        assert config.default_timezone == "America/New_York"
        assert config.price_decimals == 4
        assert config.validate_on_load is True

    def test_create_with_custom_values(self):
        """Test DataServiceConfig accepts custom values."""
        # Arrange & Act
        config = DataServiceConfig(
            sources_config="custom/sources.yaml",
            default_timezone="UTC",
            price_decimals=2,
            validate_on_load=False,
        )

        # Assert
        assert config.sources_config == "custom/sources.yaml"
        assert config.default_timezone == "UTC"
        assert config.price_decimals == 2
        assert config.validate_on_load is False

    def test_is_dataclass(self):
        """Test that DataServiceConfig is a dataclass."""
        # Arrange & Act
        config = DataServiceConfig()

        # Assert
        assert hasattr(config, "__dataclass_fields__")


class TestOutputConfig:
    """Test OutputConfig dataclass."""

    def test_create_with_defaults(self):
        """Test OutputConfig uses correct defaults."""
        # Arrange & Act
        config = OutputConfig()

        # Assert
        assert config.experiments_root == "experiments"
        assert config.run_id_format == "%Y%m%d_%H%M%S"
        assert config.capture_git_info is True
        assert config.capture_environment is True

    def test_create_with_custom_values(self):
        """Test OutputConfig accepts custom values."""
        # Arrange & Act
        config = OutputConfig(
            experiments_root="my_experiments",
            run_id_format="%Y-%m-%d",
            capture_git_info=False,
        )

        # Assert
        assert config.experiments_root == "my_experiments"
        assert config.run_id_format == "%Y-%m-%d"
        assert config.capture_git_info is False


class TestLoggingConfig:
    """Test LoggingConfig dataclass."""

    def test_create_with_defaults(self):
        """Test LoggingConfig uses correct defaults."""
        # Arrange & Act
        config = LoggingConfig()

        # Assert
        assert config.level == "INFO"
        assert config.format == "console"
        assert config.timestamp_format == "compact"
        assert config.enable_file is True
        assert config.file_path == "logs/qs_trader.log"
        assert config.file_level == "WARNING"
        assert config.file_rotation is True
        assert config.max_file_size_mb == 10
        assert config.backup_count == 3
        assert config.console_width == 0

    def test_create_with_custom_values(self):
        """Test LoggingConfig accepts custom values."""
        # Arrange & Act
        config = LoggingConfig(
            level="DEBUG",
            format="json",
            timestamp_format="iso",
            enable_file=False,
            file_path="logs/test.log",
            file_level="ERROR",
            file_rotation=False,
            max_file_size_mb=50,
            backup_count=5,
            console_width=120,
        )

        # Assert
        assert config.level == "DEBUG"
        assert config.format == "json"
        assert config.timestamp_format == "iso"
        assert config.enable_file is False
        assert config.file_path == "logs/test.log"
        assert config.file_level == "ERROR"
        assert config.file_rotation is False
        assert config.max_file_size_mb == 50
        assert config.backup_count == 5
        assert config.console_width == 120

    def test_to_logger_config_converts_correctly(self):
        """Test to_logger_config() converts to log_system.LoggingConfig."""
        # Arrange
        config = LoggingConfig(
            level="DEBUG",
            format="json",
            file_path="logs/app.log",
        )

        # Act
        logger_config = config.to_logger_config()

        # Assert
        assert logger_config.level == "DEBUG"
        assert logger_config.format == "json"
        assert logger_config.file_path == Path("logs/app.log")

    def test_to_logger_config_uses_default_file_path(self):
        """Test to_logger_config() uses default file_path when not specified."""
        # Arrange
        config = LoggingConfig()  # Uses default

        # Act
        logger_config = config.to_logger_config()

        # Assert
        assert logger_config.file_path == Path("logs/qs_trader.log")


class TestSystemConfig:
    """Test SystemConfig container."""

    def test_create_with_defaults(self):
        """Test SystemConfig creates with default sub-configs."""
        # Arrange & Act
        config = SystemConfig()

        # Assert
        assert isinstance(config.data, DataServiceConfig)
        assert isinstance(config.output, OutputConfig)
        assert isinstance(config.logging, LoggingConfig)

    def test_create_with_custom_sub_configs(self):
        """Test SystemConfig accepts custom sub-configs."""
        # Arrange
        custom_data = DataServiceConfig(default_timezone="UTC")
        custom_output = OutputConfig(experiments_root="results")
        custom_logging = LoggingConfig(level="DEBUG")

        # Act
        config = SystemConfig(
            data=custom_data,
            output=custom_output,
            logging=custom_logging,
        )

        # Assert
        assert config.data.default_timezone == "UTC"
        assert config.output.experiments_root == "results"
        assert config.logging.level == "DEBUG"

    def test_has_minimal_config_sections(self):
        """Test SystemConfig has only minimal sections (no portfolio/execution/risk)."""
        # Arrange & Act
        config = SystemConfig()

        # Assert - Has minimal sections
        assert hasattr(config, "data")
        assert hasattr(config, "output")
        assert hasattr(config, "logging")
        assert hasattr(config, "config_root")

        # Assert - Does NOT have old sections
        assert not hasattr(config, "portfolio")
        assert not hasattr(config, "execution")
        assert not hasattr(config, "risk")
        assert not hasattr(config, "strategy")


class TestSystemConfigLoad:
    """Test SystemConfig.load() file loading."""

    def test_load_with_defaults_when_no_file(self, tmp_path):
        """Test load() uses built-in defaults when no config file exists."""
        # Arrange - Point to non-existent file
        nonexistent = tmp_path / "nonexistent.yaml"

        # Act
        config = SystemConfig.load(nonexistent)

        # Assert - Should have defaults
        assert config.data.sources_config == "config/data_sources.yaml"
        assert config.output.experiments_root == "experiments"
        assert config.logging.level == "INFO"

    def test_load_from_explicit_path(self, tmp_path):
        """Test load() loads from explicitly provided path."""
        # Arrange
        config_file = tmp_path / "test.yaml"
        config_file.write_text(
            """
data:
  price_decimals: 2

output:
  experiments_root: custom/output

logging:
  level: DEBUG
"""
        )

        # Act
        config = SystemConfig.load(config_file)

        # Assert
        assert config.data.price_decimals == 2
        assert config.output.experiments_root == "custom/output"
        assert config.logging.level == "DEBUG"

    def test_load_merges_partial_config(self, tmp_path):
        """Test load() merges partial config with defaults."""
        # Arrange - Only specify some values
        config_file = tmp_path / "partial.yaml"
        config_file.write_text(
            """
data:
  default_timezone: UTC

logging:
  level: DEBUG
"""
        )

        # Act
        config = SystemConfig.load(config_file)

        # Assert - Specified values
        assert config.data.default_timezone == "UTC"
        assert config.logging.level == "DEBUG"

        # Assert - Unspecified values use defaults
        assert config.data.sources_config == "config/data_sources.yaml"
        assert config.data.price_decimals == 4
        assert config.output.experiments_root == "experiments"

    def test_load_handles_empty_file(self, tmp_path):
        """Test load() handles empty YAML file gracefully."""
        # Arrange
        config_file = tmp_path / "empty.yaml"
        config_file.write_text("")

        # Act
        config = SystemConfig.load(config_file)

        # Assert - Should use all defaults
        assert config.data.default_timezone == "America/New_York"
        assert config.output.experiments_root == "experiments"
        assert config.logging.level == "INFO"

    def test_config_root_set_to_parent_of_config_dir(self, tmp_path):
        """Test config_root is set to the project root (parent of config/ directory)."""
        # Arrange - Create config/qs_trader.yaml structure
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        config_file = config_dir / "qs_trader.yaml"
        config_file.write_text("logging:\n  level: DEBUG\n")

        # Act
        config = SystemConfig.load(config_file)

        # Assert - config_root = parent of the config/ dir = tmp_path
        assert config.config_root == tmp_path

    def test_config_root_set_to_file_parent_for_non_config_dir(self, tmp_path):
        """Test config_root uses file's parent when not in a config/ directory."""
        # Arrange
        config_file = tmp_path / "custom.yaml"
        config_file.write_text("logging:\n  level: DEBUG\n")

        # Act
        config = SystemConfig.load(config_file)

        # Assert - config_root = parent of the file itself
        assert config.config_root == tmp_path

    def test_config_root_defaults_to_cwd_when_no_file(self, tmp_path):
        """Test config_root defaults to CWD when no config file is found."""
        # Arrange
        nonexistent = tmp_path / "nonexistent.yaml"

        # Act
        config = SystemConfig.load(nonexistent)

        # Assert - Falls back to Path.cwd()
        from pathlib import Path

        assert config.config_root == Path.cwd()


class TestSystemConfigFromDict:
    """Test SystemConfig._from_dict() construction."""

    def test_from_dict_with_complete_config(self):
        """Test _from_dict() builds config from complete dictionary."""
        # Arrange
        config_dict = {
            "data": {
                "sources_config": "custom/sources.yaml",
                "default_timezone": "UTC",
                "price_decimals": 2,
                "validate_on_load": False,
            },
            "output": {
                "experiments_root": "results",
                "run_id_format": "%Y-%m-%d",
            },
            "logging": {
                "level": "DEBUG",
                "format": "json",
                "enable_file": False,
            },
        }

        # Act
        config = SystemConfig._from_dict(config_dict)

        # Assert - Data
        assert config.data.sources_config == "custom/sources.yaml"
        assert config.data.default_timezone == "UTC"
        assert config.data.price_decimals == 2
        assert config.data.validate_on_load is False

        # Assert - Output
        assert config.output.experiments_root == "results"
        assert config.output.run_id_format == "%Y-%m-%d"

        # Assert - Logging
        assert config.logging.level == "DEBUG"
        assert config.logging.format == "json"
        assert config.logging.enable_file is False

    def test_from_dict_with_partial_config_uses_defaults(self):
        """Test _from_dict() fills in defaults for missing keys."""
        # Arrange - Only specify data.default_timezone
        config_dict = {
            "data": {
                "default_timezone": "UTC",
            }
        }

        # Act
        config = SystemConfig._from_dict(config_dict)

        # Assert - Specified value
        assert config.data.default_timezone == "UTC"

        # Assert - Defaults for unspecified
        assert config.data.sources_config == "config/data_sources.yaml"
        assert config.data.price_decimals == 4
        assert config.output.experiments_root == "experiments"
        assert config.logging.level == "INFO"

    def test_from_dict_with_empty_dict_uses_all_defaults(self):
        """Test _from_dict() uses all defaults for empty dict."""
        # Arrange
        config_dict = {}

        # Act
        config = SystemConfig._from_dict(config_dict)

        # Assert
        assert config.output.experiments_root == "experiments"
        assert config.logging.level == "INFO"


class TestDeepMerge:
    """Test _deep_merge() helper function."""

    def test_merge_flat_dicts(self):
        """Test merging flat dictionaries."""
        # Arrange
        base = {"a": 1, "b": 2}
        override = {"b": 3, "c": 4}

        # Act
        result = _deep_merge(base, override)

        # Assert
        assert result == {"a": 1, "b": 3, "c": 4}

    def test_merge_nested_dicts(self):
        """Test merging nested dictionaries."""
        # Arrange
        base = {"data": {"mode": "adjusted", "decimals": 4}, "logging": {"level": "INFO"}}
        override = {"data": {"mode": "raw"}, "output": {"dir": "results"}}

        # Act
        result = _deep_merge(base, override)

        # Assert
        assert result["data"]["mode"] == "raw"
        assert result["data"]["decimals"] == 4  # Preserved from base
        assert result["logging"]["level"] == "INFO"  # Preserved
        assert result["output"]["dir"] == "results"  # New key

    def test_merge_preserves_base_when_no_overlap(self):
        """Test merge preserves base values when keys don't overlap."""
        # Arrange
        base = {"a": 1, "b": {"x": 10}}
        override = {"c": 3}

        # Act
        result = _deep_merge(base, override)

        # Assert
        assert result["a"] == 1
        assert result["b"]["x"] == 10
        assert result["c"] == 3

    def test_merge_override_wins_on_conflict(self):
        """Test override takes precedence on conflicting keys."""
        # Arrange
        base = {"key": "base_value"}
        override = {"key": "override_value"}

        # Act
        result = _deep_merge(base, override)

        # Assert
        assert result["key"] == "override_value"

    def test_merge_handles_non_dict_values(self):
        """Test merge replaces non-dict with dict and vice versa."""
        # Arrange
        base = {"a": "string", "b": {"nested": 1}}
        override = {"a": {"now": "dict"}, "b": "now_string"}

        # Act
        result = _deep_merge(base, override)

        # Assert
        assert result["a"] == {"now": "dict"}
        assert result["b"] == "now_string"


class TestSubstituteEnvVars:
    """Test _substitute_env_vars() helper function."""

    def test_substitute_string_value(self, monkeypatch):
        """Test substituting ${VAR} in string."""
        # Arrange
        monkeypatch.setenv("TEST_VAR", "test_value")
        config = {"key": "${TEST_VAR}"}

        # Act
        result = _substitute_env_vars(config)

        # Assert
        assert result["key"] == "test_value"

    def test_substitute_in_nested_dict(self, monkeypatch):
        """Test substitution in nested dictionaries."""
        # Arrange
        monkeypatch.setenv("DATA_PATH", "/data")
        monkeypatch.setenv("OUTPUT_DIR", "/output")
        config = {
            "data": {"path": "${DATA_PATH}/sources.yaml"},
            "output": {"dir": "${OUTPUT_DIR}"},
        }

        # Act
        result = _substitute_env_vars(config)

        # Assert
        assert result["data"]["path"] == "/data/sources.yaml"
        assert result["output"]["dir"] == "/output"

    def test_substitute_in_list(self, monkeypatch):
        """Test substitution in list values."""
        # Arrange
        monkeypatch.setenv("VAR1", "value1")
        monkeypatch.setenv("VAR2", "value2")
        config = {"items": ["${VAR1}", "static", "${VAR2}"]}

        # Act
        result = _substitute_env_vars(config)

        # Assert
        assert result["items"] == ["value1", "static", "value2"]

    def test_undefined_var_keeps_placeholder(self, monkeypatch):
        """Test undefined variable keeps ${VAR} placeholder."""
        # Arrange - Don't set UNDEFINED_VAR
        config = {"key": "${UNDEFINED_VAR}"}

        # Act
        result = _substitute_env_vars(config)

        # Assert
        assert result["key"] == "${UNDEFINED_VAR}"

    def test_multiple_vars_in_single_string(self, monkeypatch):
        """Test multiple ${VAR} in single string."""
        # Arrange
        monkeypatch.setenv("HOST", "localhost")
        monkeypatch.setenv("PORT", "8080")
        config = {"url": "http://${HOST}:${PORT}/api"}

        # Act
        result = _substitute_env_vars(config)

        # Assert
        assert result["url"] == "http://localhost:8080/api"

    def test_non_string_values_unchanged(self):
        """Test non-string values pass through unchanged."""
        # Arrange
        config = {"int": 42, "float": 3.14, "bool": True, "none": None}

        # Act
        result = _substitute_env_vars(config)

        # Assert
        assert result == config


class TestSingletonFunctions:
    """Test get_system_config() and reload_system_config() singletons."""

    def test_get_system_config_returns_config(self):
        """Test get_system_config() returns SystemConfig instance."""
        # Act
        config = get_system_config()

        # Assert
        assert isinstance(config, SystemConfig)

    def test_get_system_config_returns_cached_instance(self):
        """Test get_system_config() caches and returns same instance."""
        # Act
        config1 = get_system_config()
        config2 = get_system_config()

        # Assert
        assert config1 is config2

    def test_reload_system_config_creates_new_instance(self):
        """Test reload_system_config() forces reload and new instance."""
        # Arrange
        config1 = get_system_config()

        # Act
        config2 = reload_system_config()
        config3 = get_system_config()

        # Assert
        assert config1 is not config2  # Reloaded
        assert config2 is config3  # New singleton cached

    def test_get_system_config_with_explicit_path(self, tmp_path):
        """Test get_system_config() with explicit path overrides cached."""
        # Arrange
        custom_file = tmp_path / "custom.yaml"
        custom_file.write_text(
            """
logging:
  level: DEBUG
"""
        )

        # Act
        config = get_system_config(custom_file)

        # Assert
        assert config.logging.level == "DEBUG"


class TestConfigurationIntegration:
    """Integration tests for complete configuration loading."""

    def test_load_config_with_all_sections(self, tmp_path):
        """Test loading complete config file with all sections."""
        # Arrange
        config_file = tmp_path / "complete.yaml"
        config_file.write_text(
            """
data:
  sources_config: config/sources.yaml
  default_timezone: America/New_York
  price_decimals: 4
  validate_on_load: true

output:
  experiments_root: experiments
  run_id_format: "%Y%m%d_%H%M%S"

logging:
  level: INFO
  format: console
  run_id_format: compact
  enable_file: true
  file_path: logs/qs_trader.log
  file_level: WARNING
  file_rotation: true
  max_file_size_mb: 10
  backup_count: 3
  console_width: 0
"""
        )

        # Act
        config = SystemConfig.load(config_file)

        # Assert - All sections present
        assert config.data.sources_config == "config/sources.yaml"
        assert config.output.experiments_root == "experiments"
        assert config.logging.level == "INFO"
        assert config.logging.file_path == "logs/qs_trader.log"

    def test_config_with_env_vars_substitution(self, tmp_path, monkeypatch):
        """Test end-to-end config loading with env var substitution."""
        # Arrange
        monkeypatch.setenv("QS_TRADER_DATA_DIR", "/custom/data")
        monkeypatch.setenv("QS_TRADER_OUTPUT_DIR", "/custom/output")
        monkeypatch.setenv("LOG_LEVEL", "DEBUG")

        config_file = tmp_path / "env_config.yaml"
        config_file.write_text(
            """
data:
  sources_config: ${QS_TRADER_DATA_DIR}/sources.yaml

output:
  experiments_root: ${QS_TRADER_OUTPUT_DIR}

logging:
  level: ${LOG_LEVEL}
"""
        )

        # Act
        config = SystemConfig.load(config_file)

        # Assert
        assert config.data.sources_config == "/custom/data/sources.yaml"
        assert config.output.experiments_root == "/custom/output"
        assert config.logging.level == "DEBUG"


# ---------------------------------------------------------------------------
# DatabaseOutputConfig field tests
# ---------------------------------------------------------------------------


class TestDatabaseOutputConfig:
    """Unit tests for DatabaseOutputConfig defaults and field contract."""

    def test_enabled_defaults_to_false(self) -> None:
        """Database output is disabled by default."""
        cfg = DatabaseOutputConfig()
        assert cfg.enabled is False

    def test_backend_defaults_to_postgres(self) -> None:
        """PostgreSQL is the only supported operational persistence backend."""
        cfg = DatabaseOutputConfig()
        assert cfg.backend == "postgres"

    def test_postgres_url_defaults_to_none(self) -> None:
        """Connection URL stays optional until database persistence is enabled."""
        cfg = DatabaseOutputConfig()
        assert cfg.postgres_url is None


class TestCanonicalInputPolicy:
    """Phase 5: ``canonical_input_policy`` field retired from ``DatabaseOutputConfig``.

    The ``canonical_input_policy`` config option (``reference`` / ``snapshot``) and its
    corresponding ``_from_dict()`` parsing / validation block were removed in Phase 5 of
    the DuckDB/ClickHouse boundary refactor.  All runs now use the ``reference`` path
    implicitly — the ClickHouse manifest is always the source of truth for canonical runs.

    This class is retained as a named marker so git history clearly records the retirement.
    """
