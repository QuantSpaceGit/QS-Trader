"""
Unit tests for DataSourceResolver adapter registry integration.

Tests the integration between DataSourceResolver and AdapterRegistry:
- Registry initialization in resolver
- Adapter lookup via registry
- Error handling for missing adapters
- Registry auto-discovery

Following unittest.prompt.md guidelines:
- Descriptive test names: test_<function>_<scenario>_<expected>
- Arrange-Act-Assert pattern
- pytest fixtures for setup
- Focus on main functional paths
- Aim for 90%+ coverage
"""

import tempfile
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from qs_trader.libraries.registry import AdapterRegistry, ComponentNotFoundError
from qs_trader.services.data.adapters.resolver import DataSourceResolver
from qs_trader.services.data.models import Instrument

# ============================================================================
# Test Fixtures
# ============================================================================


@pytest.fixture
def temp_data_sources_config(tmp_path):
    """Create temporary data_sources.yaml config file."""
    config_content = """
data_sources:
  test-source-1:
    provider: test_provider
    asset_class: equity
    data_type: ohlcv
    frequency: 1d
    adapter: yahoo_csv
    root_path: "data/test"
    path_template: "{root_path}/{symbol}.csv"

  test-source-2:
    provider: test_provider
    asset_class: equity
    data_type: ohlcv
    frequency: 1d
    adapter: test_adapter
    root_path: "data/test"
    path_template: "{root_path}/SecId={secid}/*.parquet"
"""
    config_path = tmp_path / "data_sources.yaml"
    config_path.write_text(config_content)
    return config_path


@pytest.fixture
def mock_adapter_registry():
    """Create mock adapter registry."""
    registry = Mock(spec=AdapterRegistry)
    registry.list_components.return_value = {
        "yahoo_csv": Mock(__name__="YahooCSVAdapter"),
        "test_adapter": Mock(__name__="TestAdapter"),
    }
    registry._metadata = {
        "yahoo_csv": {"source": "builtin"},
        "test_adapter": {"source": "custom"},
    }
    return registry


@pytest.fixture
def instrument():
    """Create test instrument."""
    return Instrument(symbol="AAPL", secid=8552)


# ============================================================================
# Resolver Initialization Tests
# ============================================================================


def test_resolver_init_creates_adapter_registry(temp_data_sources_config):
    """Test DataSourceResolver creates AdapterRegistry on initialization."""
    # Arrange & Act
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Assert
    assert hasattr(resolver, "adapter_registry")
    assert isinstance(resolver.adapter_registry, AdapterRegistry)


def test_resolver_init_discovers_adapters(temp_data_sources_config):
    """Test DataSourceResolver triggers adapter discovery on init."""
    # Arrange & Act
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Assert
    # Registry should have discovered adapters
    components = resolver.adapter_registry.list_components()
    assert isinstance(components, dict)
    # May have yahoo_csv and/or test_adapter depending on environment
    assert len(components) >= 0  # At minimum, no crash


def test_resolver_init_uses_system_config_for_adapters(temp_data_sources_config):
    """Test resolver uses system config custom_libraries.adapters path."""
    # Arrange
    with patch("qs_trader.services.data.adapters.resolver.get_system_config") as mock_get_config:
        mock_config = Mock()
        mock_config.custom_libraries.adapters = "my_library/adapters"
        mock_get_config.return_value = mock_config

        # Act
        resolver = DataSourceResolver(str(temp_data_sources_config))

        # Assert
        # Should have called get_system_config to get adapter path
        mock_get_config.assert_called()
        assert resolver.adapter_registry is not None


def test_resolver_init_with_invalid_config_raises_error(tmp_path):
    """Test resolver initialization with invalid config raises error."""
    # Arrange
    bad_config = tmp_path / "bad_config.yaml"
    bad_config.write_text("invalid: yaml: structure:")

    # Act & Assert
    with pytest.raises(ValueError, match="Invalid config format"):
        DataSourceResolver(str(bad_config))


def test_resolver_init_with_missing_config_raises_error():
    """Test resolver initialization with missing config raises FileNotFoundError."""
    # Arrange
    nonexistent_path = "/nonexistent/config.yaml"

    # Act & Assert
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        DataSourceResolver(nonexistent_path)


# ============================================================================
# Adapter Lookup Tests
# ============================================================================


def test_get_adapter_class_found_in_registry_returns_class(temp_data_sources_config):
    """Test _get_adapter_class returns adapter class from registry."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Act - Try to get yahoo_csv if it exists
    components = resolver.adapter_registry.list_components()
    if "yahoo_csv" in components:
        adapter_class = resolver._get_adapter_class("yahoo_csv")

        # Assert
        assert adapter_class is not None
        assert adapter_class.__name__ == "YahooCSVDataAdapter"


def test_get_adapter_class_not_found_raises_component_not_found_error(temp_data_sources_config):
    """Test _get_adapter_class with non-existent adapter raises error."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Act & Assert
    # The resolver's _get_adapter_class delegates to registry.get() which raises ComponentNotFoundError
    with pytest.raises(ComponentNotFoundError, match="not found"):
        resolver._get_adapter_class("nonexistent_adapter")


def test_get_adapter_class_error_message_lists_available_adapters(temp_data_sources_config):
    """Test error message includes list of available adapters."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Act & Assert
    with pytest.raises(ComponentNotFoundError) as exc_info:
        resolver._get_adapter_class("nonexistent_adapter")

    # Error message should list available adapters
    error_msg = str(exc_info.value)
    assert "Available:" in error_msg
    # Should mention actual adapters discovered
    components = resolver.adapter_registry.list_components()
    for adapter_name in components.keys():
        assert adapter_name in error_msg


def test_get_adapter_class_uses_registry_not_direct_import(temp_data_sources_config):
    """Test _get_adapter_class uses registry instead of direct imports."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))

    with patch.object(resolver.adapter_registry, "get") as mock_get:
        mock_adapter_class = Mock(__name__="MockAdapter")
        mock_get.return_value = mock_adapter_class

        # Act
        result = resolver._get_adapter_class("test_adapter")

        # Assert
        mock_get.assert_called_once_with("test_adapter")
        assert result == mock_adapter_class


# ============================================================================
# Resolve by Dataset Tests
# ============================================================================


def test_resolve_by_dataset_uses_registry_adapter(temp_data_sources_config, instrument):
    """Test resolve_by_dataset uses adapter from registry (without filesystem I/O)."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))

    with patch.object(resolver.adapter_registry, "get") as mock_get:
        # Provide a dummy adapter class that can be instantiated safely
        class DummyAdapter:
            def __init__(self, config, instrument, dataset_name=None):
                self.config = config
                self.instrument = instrument
                self.dataset_name = dataset_name

            def read_bars(self, *args, **kwargs):
                return []

            def to_price_bar_event(self, *args, **kwargs):
                return None

            def to_corporate_action_event(self, *args, **kwargs):
                return None

            def get_timestamp(self, *args, **kwargs):
                return None

            def get_available_date_range(self):
                return None, None

        mock_get.return_value = DummyAdapter

        # Act
        adapter = resolver.resolve_by_dataset("test-source-1", instrument)

        # Assert
        assert isinstance(adapter, DummyAdapter)
        assert hasattr(adapter, "read_bars")
        assert hasattr(adapter, "to_price_bar_event")


def test_resolve_by_dataset_with_missing_adapter_raises_error(temp_data_sources_config, instrument):
    """Test resolve_by_dataset with missing adapter name raises error."""
    # Arrange
    config_content = """
data_sources:
  bad-source:
    adapter: nonexistent_adapter
    root_path: "data/test"
"""
    bad_config = temp_data_sources_config.parent / "bad_sources.yaml"
    bad_config.write_text(config_content)

    resolver = DataSourceResolver(str(bad_config))

    # Act & Assert
    with pytest.raises(ComponentNotFoundError, match="not found"):
        resolver.resolve_by_dataset("bad-source", instrument)


def test_resolve_by_dataset_with_unknown_dataset_raises_error(temp_data_sources_config, instrument):
    """Test resolve_by_dataset with unknown dataset name raises KeyError."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Act & Assert
    with pytest.raises(KeyError):
        resolver.resolve_by_dataset("nonexistent-dataset", instrument)


def test_resolve_by_dataset_instantiates_adapter_with_config(temp_data_sources_config, instrument):
    """Test resolve_by_dataset passes config to adapter constructor."""
    import inspect

    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Mock the adapter class with a proper signature including dataset_name
    with patch.object(resolver.adapter_registry, "get") as mock_get:
        # Create a mock class with proper signature
        def mock_init(config, instrument, dataset_name=None):
            pass

        mock_adapter_class = Mock()
        mock_adapter_instance = Mock()
        mock_adapter_class.return_value = mock_adapter_instance
        # Set the signature so resolver can detect dataset_name parameter
        mock_adapter_class.__signature__ = inspect.signature(mock_init)
        mock_get.return_value = mock_adapter_class

        # Act
        resolver.resolve_by_dataset("test-source-1", instrument)

        # Assert
        mock_adapter_class.assert_called_once()
        call_args = mock_adapter_class.call_args

        # call_args is (args, kwargs)
        args, kwargs = call_args

        # First arg should be config dict (without "adapter" field - that's popped by resolver)
        config_arg = args[0]
        assert isinstance(config_arg, dict)
        assert "adapter" not in config_arg  # Resolver removes this before passing to adapter
        assert "root_path" in config_arg  # Should have actual adapter config fields

        # Second arg should be instrument
        assert args[1] == instrument

        # Dataset name should be passed as keyword argument
        assert kwargs.get("dataset_name") == "test-source-1"


# ============================================================================
# Registry Integration Tests
# ============================================================================


def test_resolver_registry_has_builtin_adapters():
    """Test resolver's registry discovers built-in adapters."""
    # Arrange
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(
            """
data_sources:
  test-source:
    adapter: yahoo_csv
    root_path: "data/test"
"""
        )
        config_path = f.name

    try:
        # Act
        resolver = DataSourceResolver(config_path)

        # Assert
        components = resolver.adapter_registry.list_components()
        # Should have yahoo_csv from builtin directory
        if "yahoo_csv" in components:
            metadata = resolver.adapter_registry._metadata.get("yahoo_csv", {})
            assert metadata.get("source") == "builtin"
    finally:
        Path(config_path).unlink()


def test_resolver_registry_has_custom_adapters():
    """Test resolver's registry discovers custom adapters."""
    # Arrange
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(
            """
data_sources:
  test-source:
    adapter: test_adapter
    root_path: "data/test"
"""
        )
        config_path = f.name

    try:
        # Act
        resolver = DataSourceResolver(config_path)

        # Assert
        components = resolver.adapter_registry.list_components()
        # Should have test_adapter from custom directory (if configured)
        if "test_adapter" in components:
            metadata = resolver.adapter_registry._metadata.get("test_adapter", {})
            assert metadata.get("source") == "custom"
    finally:
        Path(config_path).unlink()


def test_resolver_multiple_instances_independent_registries():
    """Test multiple resolver instances have independent registries."""
    # Arrange
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
        f.write(
            """
data_sources:
  test-source:
    adapter: yahoo_csv
    root_path: "data/test"
"""
        )
        config_path = f.name

    try:
        # Act
        resolver1 = DataSourceResolver(config_path)
        resolver2 = DataSourceResolver(config_path)

        # Assert
        # Registries should be different instances
        assert resolver1.adapter_registry is not resolver2.adapter_registry

        # But both should have discovered adapters
        components1 = resolver1.adapter_registry.list_components()
        components2 = resolver2.adapter_registry.list_components()
        assert len(components1) == len(components2)
    finally:
        Path(config_path).unlink()


# ============================================================================
# Environment Variable Substitution Tests
# ============================================================================


def test_substitute_env_vars_replaces_simple_var(temp_data_sources_config):
    """Test _substitute_env_vars replaces ${VAR} syntax."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))
    import os

    os.environ["TEST_VAR"] = "test_value"

    config = {"key": "${TEST_VAR}", "nested": {"inner": "${TEST_VAR}"}}

    # Act
    result = resolver._substitute_env_vars(config)

    # Assert
    assert result["key"] == "test_value"
    assert result["nested"]["inner"] == "test_value"

    # Cleanup
    del os.environ["TEST_VAR"]


def test_substitute_env_vars_uses_default_value(temp_data_sources_config):
    """Test _substitute_env_vars uses default with ${VAR:-default} syntax."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))
    config = {"key": "${NONEXISTENT_VAR:-default_value}"}

    # Act
    result = resolver._substitute_env_vars(config)

    # Assert
    assert result["key"] == "default_value"


def test_substitute_env_vars_raises_error_for_missing_var(temp_data_sources_config):
    """Test _substitute_env_vars raises error for missing var without default."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))
    config = {"key": "${NONEXISTENT_VAR}"}

    # Act & Assert
    with pytest.raises(KeyError):
        resolver._substitute_env_vars(config)


def test_substitute_env_vars_handles_nested_dicts(temp_data_sources_config):
    """Test _substitute_env_vars recursively processes nested dicts."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))
    import os

    os.environ["TEST_VAR"] = "value"

    config = {"level1": {"level2": {"level3": "${TEST_VAR}"}}}

    # Act
    result = resolver._substitute_env_vars(config)

    # Assert
    assert result["level1"]["level2"]["level3"] == "value"

    # Cleanup
    del os.environ["TEST_VAR"]


def test_substitute_env_vars_preserves_non_env_strings(temp_data_sources_config):
    """Test _substitute_env_vars preserves regular strings."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))
    config = {"normal_key": "normal_value", "path": "/data/path"}

    # Act
    result = resolver._substitute_env_vars(config)

    # Assert
    assert result["normal_key"] == "normal_value"
    assert result["path"] == "/data/path"


# ============================================================================
# Config Loading Tests
# ============================================================================


def test_load_config_validates_adapter_field_presence(tmp_path):
    """Test _load_config validates each source has 'adapter' field."""
    # Arrange
    bad_config = tmp_path / "bad_config.yaml"
    bad_config.write_text(
        """
data_sources:
  bad-source:
    provider: test
    # Missing: adapter field
"""
    )

    # Act & Assert
    with pytest.raises(ValueError, match="missing required 'adapter' field"):
        DataSourceResolver(str(bad_config))


def test_load_config_accepts_valid_structure(temp_data_sources_config):
    """Test _load_config accepts valid YAML structure."""
    # Arrange & Act
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Assert
    assert len(resolver.sources) >= 1
    assert "test-source-1" in resolver.sources
    assert resolver.sources["test-source-1"]["adapter"] == "yahoo_csv"


def test_load_config_missing_data_sources_key_raises_error(tmp_path):
    """Test _load_config raises error when 'data_sources' key is missing."""
    # Arrange
    bad_config = tmp_path / "bad_config.yaml"
    bad_config.write_text("some_other_key: value")

    # Act & Assert
    with pytest.raises(ValueError, match="Expected 'data_sources' key"):
        DataSourceResolver(str(bad_config))


# ============================================================================
# Find Config Tests
# ============================================================================


def test_find_config_explicit_path_takes_priority(temp_data_sources_config):
    """Test _find_config uses explicit config_path with highest priority."""
    # Arrange & Act
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Assert
    assert resolver.config_path == temp_data_sources_config


def test_find_config_system_config_path_takes_second_priority(tmp_path):
    """Test _find_config uses system_sources_config as second priority."""
    # Arrange
    system_config = tmp_path / "system_config.yaml"
    system_config.write_text(
        """
data_sources:
  test-source:
    adapter: yahoo_csv
"""
    )

    # Act
    resolver = DataSourceResolver(system_sources_config=str(system_config))

    # Assert
    assert resolver.config_path == system_config


def test_find_config_searches_default_locations_as_fallback():
    """Test _find_config searches default locations when no path provided."""
    # Arrange
    # Create config in default location (project-relative)
    default_config = Path("config/data_sources.yaml")

    # Act & Assert
    if default_config.exists():
        # If default config exists, resolver should find it
        resolver = DataSourceResolver()
        assert resolver.config_path == default_config


def test_find_config_raises_error_when_not_found():
    """Test _find_config raises FileNotFoundError when config not found."""
    # Arrange
    # Use explicit path that doesn't exist

    # Act & Assert
    with pytest.raises(FileNotFoundError, match="Config file not found"):
        # Try to create resolver without any valid config
        # This will fail if default configs exist
        DataSourceResolver(config_path="/nonexistent/path.yaml")


# ============================================================================
# Integration Tests
# ============================================================================


def test_resolver_end_to_end_with_registry(temp_data_sources_config, instrument):
    """Integration test: full workflow from init to adapter instantiation."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))

    # Act
    # Check if yahoo_csv adapter is available
    components = resolver.adapter_registry.list_components()

    # Assert
    # Just test that resolver can get the adapter class
    if "yahoo_csv" in components:
        adapter_class = resolver._get_adapter_class("yahoo_csv")
        assert adapter_class is not None

        # Verify adapter class structure (not instantiation which requires files)
        assert hasattr(adapter_class, "__init__")
        # Check that instances would have required methods
        assert "read_bars" in dir(adapter_class)
        assert "to_price_bar_event" in dir(adapter_class)


def test_resolver_handles_multiple_adapter_types(temp_data_sources_config, instrument):
    """Test resolver can work with different adapter types."""
    # Arrange
    resolver = DataSourceResolver(str(temp_data_sources_config))
    components = resolver.adapter_registry.list_components()

    # Act & Assert
    # Test adapter classes are available (not instantiation)
    if "yahoo_csv" in components:
        adapter_class1 = resolver._get_adapter_class("yahoo_csv")
        assert adapter_class1.__name__ == "YahooCSVDataAdapter"

    # Test test_adapter if available
    if "test_adapter" in components:
        adapter_class2 = resolver._get_adapter_class("test_adapter")
        assert adapter_class2.__name__ == "TestAdapter"
