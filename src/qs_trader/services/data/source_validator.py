"""Validation for data_sources.yaml configuration.

Validates:
- Source naming conventions (lowercase-with-hyphens)
- Required metadata fields (provider, asset_class, data_type, frequency, adapter)
- Metadata field values (valid asset classes, data types, adapters)
- Optional fields (adjusted, timezone, price_currency, price_scale)
- Duplicate configurations

Supported:
- Adapters: Dynamically loaded from DataSourceResolver.ADAPTER_REGISTRY
- Asset Classes: equity, futures, options, crypto, forex, fixed_income
- Data Types: ohlcv, trades, quotes
"""

import re
from pathlib import Path
from typing import Any, Dict, List

import yaml


class DataSourceValidationError(Exception):
    """Raised when data source configuration is invalid."""

    pass


class DataSourceValidator:
    """Validates data_sources.yaml configuration."""

    # Naming convention pattern: <provider>-<region>-<asset>-<freq>[-variant]
    # Examples: yahoo-us-equity-1d-csv, custom-crypto-1m
    NAME_PATTERN = re.compile(
        r"^[a-z0-9]+(?:-[a-z0-9]+)*$"  # lowercase alphanumeric with hyphens
    )

    # Frequency pattern: 1m, 5m, 15m, 1h, 1d, etc.
    FREQUENCY_PATTERN = re.compile(r"^\d+[mhd]$")

    # Required metadata fields
    REQUIRED_METADATA = ["provider", "asset_class", "data_type", "frequency", "adapter"]

    # Valid values for metadata fields
    VALID_ASSET_CLASSES = [
        "equity",
        "futures",
        "options",
        "crypto",
        "forex",
        "fixed_income",
    ]
    VALID_DATA_TYPES = ["ohlcv", "trades", "quotes"]

    @classmethod
    def _get_valid_adapters(cls) -> List[str]:
        """
        Get list of valid adapters from the resolver registry.

        This dynamically retrieves the supported adapters so the validator
        doesn't need to be updated when new adapters are added.

        Returns:
            List of valid adapter names
        """
        try:
            # Import here to avoid circular dependencies
            from qs_trader.libraries.registry import get_adapter_registry
            from qs_trader.system.config import get_system_config

            registry = get_adapter_registry()
            system_config = get_system_config()
            registry.discover(custom_path=system_config.custom_libraries.adapters)
            return registry.list_names()
        except (ImportError, Exception):
            # Fallback to hardcoded list if registry not available
            return ["yahoo_csv", "custom_csv"]

    @classmethod
    def validate_file(cls, config_path: str | Path) -> None:
        """
        Validate entire data_sources.yaml file.

        Args:
            config_path: Path to data_sources.yaml file

        Raises:
            DataSourceValidationError: If validation fails
            FileNotFoundError: If file not found
        """
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path) as f:
            config = yaml.safe_load(f)

        if not config or "data_sources" not in config:
            raise DataSourceValidationError("Missing 'data_sources' key in config")

        sources = config["data_sources"]
        if not sources:
            raise DataSourceValidationError("No data sources defined")

        errors: List[str] = []

        for source_name, source_config in sources.items():
            try:
                cls.validate_source(source_name, source_config)
            except DataSourceValidationError as e:
                errors.append(f"Source '{source_name}': {e}")

        # Check for duplicate configurations
        duplicate_errors = cls._check_duplicates(sources)
        errors.extend(duplicate_errors)

        if errors:
            error_msg = "\n".join([f"  - {e}" for e in errors])
            raise DataSourceValidationError(f"Data source validation failed:\n{error_msg}")

    @classmethod
    def validate_source(cls, source_name: str, source_config: Dict[str, Any]) -> None:
        """
        Validate a single data source configuration.

        Args:
            source_name: Name of the data source
            source_config: Configuration dict

        Raises:
            DataSourceValidationError: If validation fails
        """
        errors: List[str] = []

        # Validate naming convention
        if not cls.NAME_PATTERN.match(source_name):
            errors.append(
                f"Invalid name format: '{source_name}'. "
                "Use lowercase alphanumeric with hyphens, e.g., 'provider-region-asset-freq'"
            )

        # Check required metadata fields
        missing_fields = [field for field in cls.REQUIRED_METADATA if field not in source_config]
        if missing_fields:
            errors.append(f"Missing required metadata fields: {missing_fields}")

        # Validate metadata field values
        if "asset_class" in source_config:
            if source_config["asset_class"] not in cls.VALID_ASSET_CLASSES:
                errors.append(
                    f"Invalid asset_class: '{source_config['asset_class']}'. Valid values: {cls.VALID_ASSET_CLASSES}"
                )

        if "data_type" in source_config:
            if source_config["data_type"] not in cls.VALID_DATA_TYPES:
                errors.append(
                    f"Invalid data_type: '{source_config['data_type']}'. Valid values: {cls.VALID_DATA_TYPES}"
                )

        if "adapter" in source_config:
            valid_adapters = cls._get_valid_adapters()
            if source_config["adapter"] not in valid_adapters:
                errors.append(f"Invalid adapter: '{source_config['adapter']}'. Valid values: {valid_adapters}")

        if "adjusted" in source_config:
            if not isinstance(source_config["adjusted"], bool):
                errors.append(f"Invalid adjusted field: must be boolean true/false, got '{source_config['adjusted']}'")

        if "frequency" in source_config:
            freq = source_config["frequency"]
            if not cls.FREQUENCY_PATTERN.match(str(freq)):
                errors.append(f"Invalid frequency format: '{freq}'. Use format like: 1m, 5m, 15m, 1h, 1d")

        # Validate timezone if present (optional but should be valid if provided)
        if "timezone" in source_config:
            timezone = source_config["timezone"]
            # Basic timezone validation - should look like "America/New_York" or "UTC"
            if not isinstance(timezone, str) or not timezone:
                errors.append(f"Invalid timezone: must be non-empty string")

        # Validate price_currency if present (optional)
        if "price_currency" in source_config:
            currency = source_config["price_currency"]
            if not isinstance(currency, str) or len(currency) != 3:
                errors.append(f"Invalid price_currency: must be 3-letter currency code like 'USD'")

        # Validate price_scale if present (optional)
        if "price_scale" in source_config:
            scale = source_config["price_scale"]
            if not isinstance(scale, int) or scale < 0:
                errors.append(f"Invalid price_scale: must be non-negative integer")

        if errors:
            raise DataSourceValidationError("; ".join(errors))

    @classmethod
    def _check_duplicates(cls, sources: Dict[str, Dict[str, Any]]) -> List[str]:
        """
        Check for duplicate configurations.

        Returns configurations that have identical metadata but different names.

        Args:
            sources: Dict of source configurations

        Returns:
            List of error messages for duplicates found
        """
        errors: List[str] = []
        seen_configs: Dict[str, List[str]] = {}

        for source_name, source_config in sources.items():
            # Create signature from metadata (excluding optional fields like timezone, currency)
            signature = (
                source_config.get("provider", ""),
                source_config.get("asset_class", ""),
                source_config.get("data_type", ""),
                source_config.get("frequency", ""),
                source_config.get("region", ""),
                source_config.get("adjusted", None),
            )

            # Convert to string for dict key
            sig_str = str(signature)

            if sig_str in seen_configs:
                seen_configs[sig_str].append(source_name)
            else:
                seen_configs[sig_str] = [source_name]

        # Report duplicates
        for sig_str, source_names in seen_configs.items():
            if len(source_names) > 1:
                errors.append(f"Duplicate configurations found: {source_names}. These sources have identical metadata.")

        return errors


def validate_data_sources(config_path: str | Path) -> None:
    """
    Convenience function to validate data_sources.yaml.

    Args:
        config_path: Path to data_sources.yaml file

    Raises:
        DataSourceValidationError: If validation fails
        FileNotFoundError: If file not found

    Example:
        >>> from qs_trader.config.data_source_validator import validate_data_sources
        >>> validate_data_sources("config/data_sources.yaml")
    """
    DataSourceValidator.validate_file(config_path)
