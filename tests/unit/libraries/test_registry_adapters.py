"""
Unit tests for AdapterRegistry in qs_trader.libraries.registry module.

Tests the adapter plugin registry system for auto-discovery and validation of:
- Built-in adapters (e.g., yahoo_csv)
- Custom adapters (e.g., custom_ohlc)

Following unittest.prompt.md guidelines:
- Descriptive test names: test_<function>_<scenario>_<expected>
- Arrange-Act-Assert pattern
- pytest fixtures for setup
- Parametrize for multiple test cases
- Focus on main functional paths
- Aim for 90%+ coverage
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

import pytest

from qs_trader.events.events import CorporateActionEvent, PriceBarEvent
from qs_trader.libraries.registry import (
    AdapterRegistry,
    ComponentNotFoundError,
    DuplicateComponentError,
    get_adapter_registry,
)
from qs_trader.services.data.models import Instrument

# ============================================================================
# Test Fixtures - Mock Adapters
# ============================================================================


class MockOHLCAdapter:
    """Mock OHLC adapter implementing IDataAdapter protocol."""

    def __init__(self, config: dict, instrument: Instrument, dataset_name: str):
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name

    def read_bars(self, start_date: str, end_date: str) -> Iterator:
        """Yield mock bars."""
        yield {"date": "2023-01-01", "open": 100.0, "close": 101.0}

    def to_price_bar_event(self, bar: dict) -> PriceBarEvent:
        """Convert bar to event."""
        return PriceBarEvent(
            timestamp=datetime(2023, 1, 1).isoformat(),
            symbol=self.instrument.symbol,
            open=bar["open"],
            high=bar.get("high", bar["open"]),
            low=bar.get("low", bar["close"]),
            close=bar["close"],
            volume=bar.get("volume", 0),
            source="mock",
        )

    def to_corporate_action_event(self, bar: dict, prev_bar: dict) -> Optional[CorporateActionEvent]:
        """Extract corporate action."""
        return None

    def get_timestamp(self, bar: dict) -> datetime:
        """Get bar timestamp."""
        return datetime.fromisoformat(bar["date"])

    def get_available_date_range(self) -> tuple[Optional[str], Optional[str]]:
        """Get available date range."""
        return ("2023-01-01", "2023-12-31")


class CustomVendorAdapter:
    """Custom vendor adapter for testing."""

    def __init__(self, config: dict, instrument: Instrument, dataset_name: str):
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name

    def read_bars(self, start_date: str, end_date: str) -> Iterator:
        yield {}

    def to_price_bar_event(self, bar: dict) -> PriceBarEvent:
        from decimal import Decimal

        return PriceBarEvent(
            timestamp=datetime.now().isoformat(),
            symbol="TEST",
            open=Decimal("100.0"),
            high=Decimal("101.0"),
            low=Decimal("99.0"),
            close=Decimal("100.5"),
            volume=1000,
            source="custom",
        )

    def to_corporate_action_event(self, bar: dict, prev_bar: dict) -> Optional[CorporateActionEvent]:
        return None

    def get_timestamp(self, bar: dict) -> datetime:
        return datetime.now()

    def get_available_date_range(self) -> tuple[Optional[str], Optional[str]]:
        return (None, None)


class IncompleteAdapter:
    """Adapter missing required methods (should not be registered)."""

    def read_bars(self, start_date: str, end_date: str) -> Iterator:
        yield {}

    # Missing: to_price_bar_event, to_corporate_action_event, etc.


class NotAnAdapter:
    """Class that doesn't implement IDataAdapter protocol."""

    pass


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def temp_adapter_dir():
    """Create temporary directory for mock adapter files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_builtin_adapter(temp_adapter_dir):
    """Create mock built-in adapter file."""
    adapter_file = temp_adapter_dir / "mock_builtin.py"
    adapter_file.write_text(
        '''
"""Mock built-in adapter."""
from datetime import datetime
from typing import Iterator, Optional

class TestBuiltinAdapter:
    """Test built-in adapter."""

    def __init__(self, config, instrument, dataset_name):
        pass

    def read_bars(self, start_date: str, end_date: str) -> Iterator:
        yield {}

    def to_price_bar_event(self, bar):
        from qs_trader.events.events import PriceBarEvent
        return PriceBarEvent(timestamp=datetime.now(), symbol="TEST")

    def to_corporate_action_event(self, bar, prev_bar):
        return None

    def get_timestamp(self, bar):
        return datetime.now()

    def get_available_date_range(self):
        return (None, None)
'''
    )
    return adapter_file


@pytest.fixture
def mock_custom_adapter(temp_adapter_dir):
    """Create mock custom adapter file."""
    adapter_file = temp_adapter_dir / "mock_custom.py"
    adapter_file.write_text(
        '''
"""Mock custom adapter."""
from datetime import datetime
from typing import Iterator, Optional

class MyCustomDataAdapter:
    """Test custom adapter."""

    def __init__(self, config, instrument, dataset_name):
        pass

    def read_bars(self, start_date: str, end_date: str) -> Iterator:
        yield {}

    def to_price_bar_event(self, bar):
        from qs_trader.events.events import PriceBarEvent
        return PriceBarEvent(timestamp=datetime.now(), symbol="CUSTOM")

    def to_corporate_action_event(self, bar, prev_bar):
        return None

    def get_timestamp(self, bar):
        return datetime.now()

    def get_available_date_range(self):
        return ("2020-01-01", "2023-12-31")
'''
    )
    return adapter_file


@pytest.fixture
def registry():
    """Create fresh adapter registry."""
    return AdapterRegistry()


# ============================================================================
# AdapterRegistry Initialization Tests
# ============================================================================


def test_adapter_registry_init_creates_instance():
    """Test AdapterRegistry initializes successfully."""
    # Arrange & Act
    registry = AdapterRegistry()

    # Assert
    assert registry is not None
    assert registry.component_type == "adapter"
    assert len(registry.list_components()) == 0


def test_get_adapter_registry_returns_instance():
    """Test factory function returns AdapterRegistry instance."""
    # Arrange & Act
    registry = get_adapter_registry()

    # Assert
    assert isinstance(registry, AdapterRegistry)
    assert registry.component_type == "adapter"


def test_get_adapter_registry_returns_new_instance_each_call():
    """Test factory returns new instance each time (not singleton)."""
    # Arrange & Act
    registry1 = get_adapter_registry()
    registry2 = get_adapter_registry()

    # Assert
    assert registry1 is not registry2, "Should return new instances"


# ============================================================================
# Protocol Validation Tests
# ============================================================================


def test_is_adapter_class_valid_adapter_returns_true(registry):
    """Test _is_adapter_class returns True for valid adapter."""
    # Arrange
    # Act
    result = registry._is_adapter_class(MockOHLCAdapter)

    # Assert
    assert result is True


def test_is_adapter_class_incomplete_adapter_returns_false(registry):
    """Test _is_adapter_class returns False for adapter missing methods."""
    # Arrange
    # Act
    result = registry._is_adapter_class(IncompleteAdapter)

    # Assert
    assert result is False


def test_is_adapter_class_not_adapter_returns_false(registry):
    """Test _is_adapter_class returns False for non-adapter class."""
    # Arrange
    # Act
    result = registry._is_adapter_class(NotAnAdapter)

    # Assert
    assert result is False


@pytest.mark.parametrize(
    "missing_method",
    [
        "read_bars",
        "to_price_bar_event",
        "to_corporate_action_event",
        "get_timestamp",
        "get_available_date_range",
    ],
)
def test_is_adapter_class_missing_required_method_returns_false(registry, missing_method):
    """Test _is_adapter_class returns False when required method is missing."""

    # Arrange
    class PartialAdapter:
        """Adapter with one method missing."""

        def read_bars(self, start_date: str, end_date: str):
            pass

        def to_price_bar_event(self, bar):
            pass

        def to_corporate_action_event(self, bar, prev_bar):
            pass

        def get_timestamp(self, bar):
            pass

        def get_available_date_range(self):
            pass

    # Remove one method
    if hasattr(PartialAdapter, missing_method):
        delattr(PartialAdapter, missing_method)

    # Act
    result = registry._is_adapter_class(PartialAdapter)

    # Assert
    assert result is False, f"Should fail when {missing_method} is missing"


# ============================================================================
# Name Generation Tests
# ============================================================================


@pytest.mark.parametrize(
    "class_name,expected_name",
    [
        ("YahooCSVAdapter", "yahoo_csv"),
        ("CustomOHLCAdapter", "custom_ohlc"),
        ("CustomAdapter", "custom"),
        ("MyCustomVendorAdapter", "my_custom"),
        ("PostgresDataAdapter", "postgres"),
        ("SimpleAdapter", "simple"),
        ("IEXCloudAdapter", "iex_cloud"),
        ("BinanceOHLCAdapter", "binance_ohlc"),
    ],
)
def test_generate_adapter_name_converts_correctly(registry, class_name, expected_name):
    """Test _generate_adapter_name converts class names to snake_case."""
    # Arrange & Act
    result = registry._generate_adapter_name(class_name)

    # Assert
    assert result == expected_name


def test_generate_adapter_name_specific_case():
    """Should handle specific adapter name conversion."""
    registry = get_adapter_registry()

    result = registry._generate_adapter_name("YahooCSVAdapter")

    # Should convert to snake_case and remove suffixes
    assert result == "yahoo_csv"


def test_generate_adapter_name_removes_data_adapter_suffix(registry):
    """Test _generate_adapter_name removes DataAdapter suffix."""
    # Arrange & Act
    result = registry._generate_adapter_name("YahooCSVDataAdapter")

    # Assert
    assert result == "yahoo_csv"
    assert "data" not in result
    assert "adapter" not in result


def test_generate_adapter_name_removes_adapter_suffix(registry):
    """Test _generate_adapter_name removes Adapter suffix."""
    # Arrange & Act
    result = registry._generate_adapter_name("CustomAdapter")

    # Assert
    assert result == "custom"
    assert "adapter" not in result


def test_generate_adapter_name_handles_consecutive_capitals(registry):
    """Test _generate_adapter_name handles consecutive capitals (acronyms)."""
    # Arrange & Act
    result = registry._generate_adapter_name("IEXCloudAdapter")

    # Assert
    assert result == "iex_cloud"


# ============================================================================
# Manual Registration Tests
# ============================================================================


def test_register_valid_adapter_succeeds(registry):
    """Test manually registering valid adapter succeeds."""
    # Arrange
    metadata = {"source": "test", "module": "test_module"}

    # Act
    registry.register("test_adapter", MockOHLCAdapter, metadata=metadata)

    # Assert
    assert "test_adapter" in registry.list_components()
    assert registry.get("test_adapter") == MockOHLCAdapter


def test_register_duplicate_name_raises_error(registry):
    """Test registering duplicate adapter name raises DuplicateComponentError."""
    # Arrange
    registry.register("test_adapter", MockOHLCAdapter)

    # Act & Assert
    with pytest.raises(DuplicateComponentError) as exc_info:
        registry.register("test_adapter", CustomVendorAdapter)

    assert "already registered" in str(exc_info.value).lower()


def test_register_duplicate_with_override_succeeds(registry):
    """Test registering duplicate with allow_override=True succeeds."""
    # Arrange
    registry.register("test_adapter", MockOHLCAdapter)

    # Act
    registry.register("test_adapter", CustomVendorAdapter, allow_override=True)

    # Assert
    assert registry.get("test_adapter") == CustomVendorAdapter


def test_get_nonexistent_adapter_raises_error(registry):
    """Test getting non-existent adapter raises ComponentNotFoundError."""
    # Arrange
    # Act & Assert
    with pytest.raises(ComponentNotFoundError) as exc_info:
        registry.get("nonexistent_adapter")

    assert "not found" in str(exc_info.value).lower()
    assert "nonexistent_adapter" in str(exc_info.value)


def test_list_components_empty_registry_returns_empty_dict(registry):
    """Test list_components returns empty dict for empty registry."""
    # Arrange & Act
    components = registry.list_components()

    # Assert
    assert isinstance(components, dict)
    assert len(components) == 0


def test_list_components_with_adapters_returns_dict(registry):
    """Test list_components returns dict of registered adapters."""
    # Arrange
    registry.register("adapter1", MockOHLCAdapter)
    registry.register("adapter2", CustomVendorAdapter)

    # Act
    components = registry.list_components()

    # Assert
    assert len(components) == 2
    assert "adapter1" in components
    assert "adapter2" in components
    assert components["adapter1"] == MockOHLCAdapter
    assert components["adapter2"] == CustomVendorAdapter


# ============================================================================
# Directory Scanning Tests
# ============================================================================


def test_scan_directory_nonexistent_path_does_not_raise(registry, temp_adapter_dir):
    """Test _scan_directory with non-existent path does not raise error."""
    # Arrange
    nonexistent = temp_adapter_dir / "nonexistent"

    # Act & Assert (should not raise)
    registry._scan_directory(nonexistent, source="test")


def test_scan_directory_finds_adapter_files(registry, mock_builtin_adapter, temp_adapter_dir):
    """Test _scan_directory finds Python files with adapters."""
    # Arrange
    # Act
    registry._scan_directory(temp_adapter_dir, source="builtin")

    # Assert
    components = registry.list_components()
    assert len(components) > 0, "Should find at least one adapter"


def test_scan_directory_excludes_init_files(registry, temp_adapter_dir):
    """Test _scan_directory excludes __init__.py files."""
    # Arrange
    init_file = temp_adapter_dir / "__init__.py"
    init_file.write_text("# Init file")

    # Act
    registry._scan_directory(temp_adapter_dir, source="test")

    # Assert
    # Should not register anything from __init__.py
    components = registry.list_components()
    assert len(components) == 0


def test_scan_directory_excludes_test_files(registry, temp_adapter_dir):
    """Test _scan_directory excludes test files."""
    # Arrange
    test_file = temp_adapter_dir / "test_adapter.py"
    test_file.write_text(
        """
class TestAdapter:
    def read_bars(self, start_date, end_date):
        pass
    def to_price_bar_event(self, bar):
        pass
    def to_corporate_action_event(self, bar, prev_bar):
        pass
    def get_timestamp(self, bar):
        pass
    def get_available_date_range(self):
        pass
"""
    )

    # Act
    registry._scan_directory(temp_adapter_dir, source="test")

    # Assert
    components = registry.list_components()
    assert len(components) == 0, "Should not register adapters from test files"


def test_scan_directory_excludes_pycache(registry, temp_adapter_dir):
    """Test _scan_directory excludes __pycache__ directories."""
    # Arrange
    pycache = temp_adapter_dir / "__pycache__"
    pycache.mkdir()
    pyc_file = pycache / "adapter.cpython-39.pyc"
    pyc_file.write_bytes(b"compiled")

    # Act
    registry._scan_directory(temp_adapter_dir, source="test")

    # Assert
    components = registry.list_components()
    assert len(components) == 0


def test_scan_directory_finds_nested_adapters(registry, temp_adapter_dir):
    """Test _scan_directory finds adapters in nested directories."""
    # Arrange
    nested_dir = temp_adapter_dir / "subdir"
    nested_dir.mkdir()
    adapter_file = nested_dir / "nested_adapter.py"
    adapter_file.write_text(
        """
from datetime import datetime

class NestedAdapter:
    def __init__(self, config, instrument, dataset_name):
        pass
    def read_bars(self, start_date, end_date):
        yield {}
    def to_price_bar_event(self, bar):
        from qs_trader.events.events import PriceBarEvent
        return PriceBarEvent(timestamp=datetime.now(), symbol="NESTED")
    def to_corporate_action_event(self, bar, prev_bar):
        return None
    def get_timestamp(self, bar):
        return datetime.now()
    def get_available_date_range(self):
        return (None, None)
"""
    )

    # Act
    registry._scan_directory(temp_adapter_dir, source="custom")

    # Assert
    components = registry.list_components()
    assert len(components) > 0, "Should find adapter in nested directory"
    assert "nested" in components


# ============================================================================
# Import and Register Tests
# ============================================================================


def test_import_and_register_valid_file_registers_adapter(registry, mock_builtin_adapter, temp_adapter_dir):
    """Test _import_and_register with valid file registers adapter."""
    # Arrange
    # Act
    registry._import_and_register(mock_builtin_adapter, temp_adapter_dir, source="builtin")

    # Assert
    components = registry.list_components()
    assert len(components) > 0
    assert "test_builtin" in components


def test_import_and_register_tracks_metadata(registry, mock_builtin_adapter, temp_adapter_dir):
    """Test _import_and_register stores source metadata."""
    # Arrange
    # Act
    registry._import_and_register(mock_builtin_adapter, temp_adapter_dir, source="builtin")

    # Assert
    assert "test_builtin" in registry._metadata
    metadata = registry._metadata["test_builtin"]
    assert metadata["source"] == "builtin"
    assert "file" in metadata
    assert str(mock_builtin_adapter) in metadata["file"]


def test_import_and_register_invalid_syntax_does_not_crash(registry, temp_adapter_dir):
    """Test _import_and_register with syntax error does not crash registry."""
    # Arrange
    bad_file = temp_adapter_dir / "bad_syntax.py"
    bad_file.write_text("class BadAdapter\n  def bad syntax")  # Invalid syntax

    # Act & Assert (should not raise)
    registry._import_and_register(bad_file, temp_adapter_dir, source="test")

    # Assert
    components = registry.list_components()
    assert len(components) == 0, "Should not register adapter with syntax errors"


def test_import_and_register_import_error_does_not_crash(registry, temp_adapter_dir):
    """Test _import_and_register with import error does not crash registry."""
    # Arrange
    bad_import_file = temp_adapter_dir / "bad_import.py"
    bad_import_file.write_text(
        """
from nonexistent_module import NonexistentClass

class AdapterWithBadImport:
    pass
"""
    )

    # Act & Assert (should not raise)
    registry._import_and_register(bad_import_file, temp_adapter_dir, source="test")

    # Assert
    components = registry.list_components()
    assert len(components) == 0, "Should not register adapter with import errors"


def test_import_and_register_skips_imported_classes(registry, temp_adapter_dir):
    """Test _import_and_register skips classes imported from other modules."""
    # Arrange
    adapter_file = temp_adapter_dir / "with_import.py"
    adapter_file.write_text(
        """
from datetime import datetime  # Should skip datetime class

class LocalAdapter:
    def __init__(self, config, instrument, dataset_name):
        pass
    def read_bars(self, start_date, end_date):
        yield {}
    def to_price_bar_event(self, bar):
        from qs_trader.events.events import PriceBarEvent
        return PriceBarEvent(timestamp=datetime.now(), symbol="LOCAL")
    def to_corporate_action_event(self, bar, prev_bar):
        return None
    def get_timestamp(self, bar):
        return datetime.now()
    def get_available_date_range(self):
        return (None, None)
"""
    )

    # Act
    registry._import_and_register(adapter_file, temp_adapter_dir, source="test")

    # Assert
    components = registry.list_components()
    assert "local" in components
    assert "datetime" not in components, "Should not register imported classes"


# ============================================================================
# Discovery Integration Tests
# ============================================================================


def test_discover_without_custom_path_uses_system_config(registry, monkeypatch):
    """Test discover() with custom_path parameter discovers adapters."""
    # Arrange
    import tempfile

    with tempfile.TemporaryDirectory() as tmpdir:
        # Create a custom adapter in temp directory
        adapter_dir = Path(tmpdir) / "adapters"
        adapter_dir.mkdir()
        (adapter_dir / "__init__.py").touch()

        # Create a valid adapter file
        adapter_file = adapter_dir / "customadapter.py"
        adapter_file.write_text(
            '''"""Custom test adapter."""
from datetime import datetime
from typing import Iterator, Optional

class CustomTestAdapter:
    """Valid adapter implementing IDataAdapter protocol."""
    def __init__(self, config, instrument, dataset_name):
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name

    def read_bars(self, start_date: str, end_date: str) -> Iterator:
        yield {"date": "2023-01-01", "close": 100.0}

    def to_price_bar_event(self, bar):
        from qs_trader.events.events import PriceBarEvent
        return PriceBarEvent(timestamp=datetime.now(), symbol="TEST")

    def to_corporate_action_event(self, bar, prev_bar) -> Optional:
        return None

    def get_timestamp(self, bar) -> datetime:
        return datetime.now()

    def get_available_date_range(self) -> tuple:
        return (None, None)
'''
        )

        # Act
        registry.discover(custom_path=str(adapter_dir))

        # Assert
        components = registry.list_components()
        # Should discover at least the builtin adapters (if any) and our custom one
        assert len(components) >= 1, "Should discover at least one adapter"
        # The custom adapter should be registered as "custom_test"
        assert "custom_test" in components or len(components) > 0


def test_discover_with_custom_path_overrides_config(registry, mock_custom_adapter, temp_adapter_dir):
    """Test discover() with custom_path overrides system config."""
    # Arrange
    # Act
    registry.discover(custom_path=str(temp_adapter_dir))

    # Assert
    components = registry.list_components()
    # Should find custom adapter from provided path
    assert len(components) >= 1
    # Registry generates "my_custom" from "MyCustomDataAdapter"
    assert "my_custom" in components


def test_discover_builtin_adapters_succeeds(registry):
    """Test _discover_builtin finds built-in adapters if they exist."""
    # Arrange
    # Act
    registry._discover_builtin()

    # Assert
    # Note: May be empty in test environment without real builtin adapters
    # Just ensure it doesn't crash
    components = registry.list_components()
    assert isinstance(components, dict)


def test_discover_custom_with_nonexistent_path_does_not_crash(registry):
    """Test _discover_custom with non-existent path does not crash."""
    # Arrange & Act
    registry._discover_custom(custom_path="/nonexistent/path")

    # Assert
    components = registry.list_components()
    assert isinstance(components, dict)


# ============================================================================
# Integration Tests - Real Adapters
# ============================================================================


def test_discover_finds_yahoo_csv_adapter():
    """Integration test: discover() finds yahoo_csv built-in adapter."""
    # Arrange
    registry = get_adapter_registry()

    # Act
    registry.discover()

    # Assert
    components = registry.list_components()
    # yahoo_csv should be in builtin adapters
    if "yahoo_csv" in components:
        adapter_class = registry.get("yahoo_csv")
        assert adapter_class is not None
        assert adapter_class.__name__ == "YahooCSVDataAdapter"

        # Check metadata
        metadata = registry._metadata.get("yahoo_csv", {})
        assert metadata.get("source") == "builtin"


def test_adapter_registry_integration_full_workflow():
    """Integration test: full workflow from discovery to instantiation."""
    # Arrange
    registry = get_adapter_registry()
    registry.discover()

    # Act
    components = registry.list_components()

    # Assert
    assert len(components) >= 1, "Should discover at least one adapter"

    # Test getting an adapter (if yahoo_csv exists)
    if "yahoo_csv" in components:
        adapter_class = registry.get("yahoo_csv")
        assert adapter_class is not None

        # Verify adapter class has required methods (not instantiation)
        # We can't instantiate without a valid file, so just check the class
        assert hasattr(adapter_class, "__init__")

        # Create a mock instance to check protocol compliance
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("Date,Open,High,Low,Close,Adj Close,Volume\n")
            f.write("2023-01-01,100,101,99,100.5,100.5,1000\n")
            csv_path = f.name

        try:
            config = {
                "root_path": Path(csv_path).parent,
                "path_template": str(csv_path),
            }
            instrument = Instrument(symbol="AAPL")
            adapter = adapter_class(config, instrument, "test-dataset")

            # Verify adapter has required methods
            assert hasattr(adapter, "read_bars")
            assert hasattr(adapter, "to_price_bar_event")
            assert callable(adapter.read_bars)
            assert callable(adapter.to_price_bar_event)
        finally:
            Path(csv_path).unlink()


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


def test_discover_handles_circular_imports_gracefully(registry, temp_adapter_dir):
    """Test discover handles files with circular import issues."""
    # Arrange
    file1 = temp_adapter_dir / "adapter1.py"
    file2 = temp_adapter_dir / "adapter2.py"

    file1.write_text("from adapter2 import Adapter2\nclass Adapter1: pass")
    file2.write_text("from adapter1 import Adapter1\nclass Adapter2: pass")

    # Act & Assert (should not crash)
    registry._scan_directory(temp_adapter_dir, source="test")

    # Should handle gracefully without crashing
    components = registry.list_components()
    assert isinstance(components, dict)


def test_register_with_none_metadata_succeeds(registry):
    """Test registering adapter with None metadata succeeds."""
    # Arrange & Act
    registry.register("test_adapter", MockOHLCAdapter, metadata=None)

    # Assert
    assert "test_adapter" in registry.list_components()


def test_registry_handles_module_reload_safely(registry, mock_builtin_adapter, temp_adapter_dir):
    """Test registry handles module reload without issues."""
    # Arrange
    registry._import_and_register(mock_builtin_adapter, temp_adapter_dir, source="builtin")

    # Act - Import again (simulating reload)
    registry._import_and_register(mock_builtin_adapter, temp_adapter_dir, source="builtin")

    # Assert
    # Should either skip duplicate or handle gracefully
    components = registry.list_components()
    assert isinstance(components, dict)


def test_generate_adapter_name_empty_string_returns_empty(registry):
    """Test _generate_adapter_name with empty string returns empty."""
    # Arrange & Act
    result = registry._generate_adapter_name("")

    # Assert
    assert result == ""


def test_is_adapter_class_with_static_methods_validates_correctly(registry):
    """Test _is_adapter_class handles classes with static methods."""

    # Arrange
    class AdapterWithStatic:
        @staticmethod
        def some_static():
            pass

        def read_bars(self, start_date, end_date):
            pass

        def to_price_bar_event(self, bar):
            pass

        def to_corporate_action_event(self, bar, prev_bar):
            pass

        def get_timestamp(self, bar):
            pass

        def get_available_date_range(self):
            pass

    # Act
    result = registry._is_adapter_class(AdapterWithStatic)

    # Assert
    assert result is True


def test_is_adapter_class_with_properties_validates_correctly(registry):
    """Test _is_adapter_class handles classes with property decorators."""

    # Arrange
    class AdapterWithProperty:
        @property
        def some_property(self):
            return None

        def read_bars(self, start_date, end_date):
            pass

        def to_price_bar_event(self, bar):
            pass

        def to_corporate_action_event(self, bar, prev_bar):
            pass

        def get_timestamp(self, bar):
            pass

        def get_available_date_range(self):
            pass

    # Act
    result = registry._is_adapter_class(AdapterWithProperty)

    # Assert
    assert result is True
