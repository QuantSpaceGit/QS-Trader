"""Tests for backtest metadata.json writer functionality."""

from datetime import datetime
from decimal import Decimal
from pathlib import Path

from qs_trader.services.reporting.writers import write_backtest_metadata


class TestWriteBacktestMetadata:
    """Test suite for write_backtest_metadata function."""

    def test_writes_metadata_json_file(self, tmp_path: Path):
        """Should write metadata.json file with correct structure."""
        # Arrange
        backtest_config = {
            "backtest_id": "test_backtest",
            "start_date": "2023-01-01T00:00:00",
            "end_date": "2023-12-31T00:00:00",
            "initial_equity": "100000",
            "strategies": [
                {
                    "strategy_id": "test_strategy",
                    "config": {"param1": 10, "param2": 20},
                }
            ],
        }

        system_config = {
            "data": {
                "sources_config": "config/data_sources.yaml",
                "default_mode": "adjusted",
            },
            "output": {"experiments_root": "output/backtests"},
            "logging": {"level": "INFO", "format": "console"},
        }

        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        assert output_path.exists()
        assert output_path.is_file()

    def test_write_metadata_json(self, tmp_path: Path):
        """Test writing metadata to JSON file."""
        # Arrange
        backtest_config = {"backtest_id": "test"}
        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        assert "backtest" in metadata
        assert "system" in metadata

    def test_metadata_version_is_string(self, tmp_path: Path):
        """metadata_version should be a string (e.g., '1.0')."""
        # Arrange
        backtest_config = {"backtest_id": "test"}
        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        assert isinstance(metadata["metadata_version"], str)
        assert metadata["metadata_version"] == "1.0"

    def test_generated_at_is_iso_timestamp(self, tmp_path: Path):
        """generated_at should be ISO 8601 formatted timestamp."""
        # Arrange
        backtest_config = {"backtest_id": "test"}
        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        output_path = tmp_path / "metadata.json"

        # Act
        before = datetime.now()
        write_backtest_metadata(backtest_config, system_config, output_path)
        after = datetime.now()

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        timestamp = datetime.fromisoformat(metadata["generated_at"])
        assert before <= timestamp <= after

    def test_backtest_config_preserved(self, tmp_path: Path):
        """Backtest configuration should be preserved exactly."""
        # Arrange
        backtest_config = {
            "backtest_id": "sma_crossover",
            "start_date": "2023-01-01T00:00:00",
            "end_date": "2023-12-31T00:00:00",
            "initial_equity": "100000",
            "strategies": [
                {
                    "strategy_id": "sma",
                    "config": {
                        "fast_period": 10,
                        "slow_period": 50,
                        "confidence": 1.0,
                    },
                }
            ],
            "risk_policy": {"name": "naive", "config": {}},
        }

        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        assert metadata["backtest"]["backtest_id"] == "sma_crossover"
        assert metadata["backtest"]["initial_equity"] == "100000"
        assert len(metadata["backtest"]["strategies"]) == 1
        assert metadata["backtest"]["strategies"][0]["config"]["fast_period"] == 10
        assert metadata["backtest"]["strategies"][0]["config"]["slow_period"] == 50

    def test_system_config_preserved(self, tmp_path: Path):
        """System configuration should be preserved exactly."""
        # Arrange
        backtest_config = {"backtest_id": "test"}
        system_config = {
            "data": {
                "sources_config": "config/data_sources.yaml",
                "default_mode": "adjusted",
                "default_timezone": "America/New_York",
                "price_decimals": 2,
            },
            "output": {
                "experiments_root": "output/backtests",
                "timestamp_format": "%Y%m%d_%H%M%S",
            },
            "logging": {
                "level": "INFO",
                "format": "console",
                "enable_file": True,
            },
        }
        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        sys_cfg = metadata["system"]
        assert sys_cfg["data"]["sources_config"] == "config/data_sources.yaml"
        assert sys_cfg["data"]["default_mode"] == "adjusted"
        assert sys_cfg["output"]["experiments_root"] == "output/backtests"
        assert sys_cfg["logging"]["level"] == "INFO"

    def test_handles_decimal_types(self, tmp_path: Path):
        """Should serialize Decimal types correctly."""
        # Arrange
        backtest_config = {
            "backtest_id": "test",
            "initial_equity": Decimal("100000.00"),
            "strategies": [
                {
                    "strategy_id": "test",
                    "config": {
                        "threshold": Decimal("0.05"),
                        "stop_loss": Decimal("0.02"),
                    },
                }
            ],
        }

        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        # Should be serialized as numbers (DecimalEncoder converts to float)
        assert metadata["backtest"]["initial_equity"] == 100000.0
        assert metadata["backtest"]["strategies"][0]["config"]["threshold"] == 0.05

    def test_handles_nested_structures(self, tmp_path: Path):
        """Should handle deeply nested configuration structures."""
        # Arrange
        backtest_config = {
            "backtest_id": "complex",
            "strategies": [
                {
                    "strategy_id": "multi_indicator",
                    "config": {
                        "indicators": {
                            "sma": {"fast": 10, "slow": 50},
                            "rsi": {"period": 14, "overbought": 70},
                            "bb": {"period": 20, "std": 2.0},
                        },
                        "filters": [
                            {"type": "volume", "min": 1000000},
                            {"type": "price", "min": 10.0, "max": 500.0},
                        ],
                    },
                }
            ],
        }

        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        strategy_cfg = metadata["backtest"]["strategies"][0]["config"]
        assert strategy_cfg["indicators"]["sma"]["fast"] == 10
        assert strategy_cfg["indicators"]["rsi"]["period"] == 14
        assert len(strategy_cfg["filters"]) == 2

    def test_creates_parent_directories(self, tmp_path: Path):
        """Should create parent directories if they don't exist."""
        # Arrange
        backtest_config = {"backtest_id": "test"}
        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        output_path = tmp_path / "nested" / "dir" / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        assert output_path.exists()
        assert output_path.parent.exists()

    def test_overwrites_existing_file(self, tmp_path: Path):
        """Should overwrite existing metadata.json file."""
        # Arrange
        output_path = tmp_path / "metadata.json"
        output_path.write_text('{"old": "data"}')

        backtest_config = {"backtest_id": "new"}
        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        assert "old" not in metadata
        assert metadata["backtest"]["backtest_id"] == "new"

    def test_metadata_is_valid_json(self, tmp_path: Path):
        """Generated file should be valid JSON."""
        # Arrange
        backtest_config = {
            "backtest_id": "test",
            "strategies": [{"strategy_id": "s1", "config": {"p": 10}}],
        }
        system_config = {"data": {"mode": "test"}, "output": {}, "logging": {}}
        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert - should not raise
        import json

        with output_path.open("r") as f:
            json.load(f)  # Will raise if invalid JSON

    def test_empty_configs_handled(self, tmp_path: Path):
        """Should handle empty configuration dictionaries."""
        # Arrange
        backtest_config: dict[str, str] = {}
        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        output_path = tmp_path / "metadata.json"

        # Act
        write_backtest_metadata(backtest_config, system_config, output_path)

        # Assert
        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        assert metadata["backtest"] == {}
        assert metadata["system"]["data"] == {}

    def test_effective_execution_spec_is_written_under_backtest_provenance(self, tmp_path: Path):
        """Runtime provenance should be emitted in metadata.json when available."""
        backtest_config = {
            "backtest_id": "test",
            "strategies": [{"strategy_id": "sma_crossover", "config": {"fast_period": 10}}],
        }
        system_config: dict[str, dict] = {"data": {}, "output": {}, "logging": {}}
        effective_execution_spec = {
            "schema_version": 1,
            "captured_from": "qs_trader.reporting",
            "strategies": [
                {
                    "strategy_id": "sma_crossover",
                    "effective_params": {"fast_period": 10, "slow_period": 50},
                }
            ],
        }
        output_path = tmp_path / "metadata.json"

        write_backtest_metadata(
            backtest_config,
            system_config,
            output_path,
            effective_execution_spec=effective_execution_spec,
        )

        import json

        with output_path.open("r") as f:
            metadata = json.load(f)

        assert metadata["metadata_version"] == "1.1"
        assert metadata["backtest"]["submitted_config"] == backtest_config
        assert metadata["backtest"]["effective_execution_spec"] == effective_execution_spec
