"""
Unit tests for qs_trader.libraries.registry module.

Tests the plugin registry system for auto-discovery and validation of:
- Indicators (built-in and custom)
- Strategies (built-in and custom)
- Risk Policies (built-in and custom)

Following unittest.prompt.md guidelines:
- Descriptive test names: test_<function>_<scenario>_<expected>
- Arrange-Act-Assert pattern
- pytest fixtures for setup
- Parametrize for multiple test cases
- Focus on main functional paths
"""

import tempfile
from pathlib import Path
from typing import Any

import pytest

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.indicators.base import BaseIndicator
from qs_trader.libraries.registry import (
    BaseRegistry,
    ComponentNotFoundError,
    DuplicateComponentError,
    IndicatorRegistry,
    InvalidComponentError,
    RegistryError,
    StrategyRegistry,
    get_indicator_registry,
    get_strategy_registry,
)
from qs_trader.libraries.strategies import Context, Strategy, StrategyConfig
from qs_trader.services.data.models import Bar

# ============================================================================
# Test Fixtures - Mock Components
# ============================================================================


class MockIndicator(BaseIndicator):
    """Mock indicator for testing registry."""

    def __init__(self, period: int = 20, **params: Any):
        self.period = period
        self._value: float | None = None

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        return [None] * len(bars)

    def update(self, bar: Bar) -> float | None:
        self._value = bar.close
        return self._value

    def reset(self) -> None:
        self._value = None

    @property
    def value(self) -> float | None:
        return self._value

    @property
    def is_ready(self) -> bool:
        return self._value is not None


class AnotherIndicator(BaseIndicator):
    """Another mock indicator for testing."""

    def __init__(self, **params: Any):
        self._value: float | None = None

    def calculate(self, bars: list[Bar]) -> list[float | None]:
        return []

    def update(self, bar: Bar) -> float | None:
        return None

    def reset(self) -> None:
        pass

    @property
    def value(self) -> float | None:
        return None

    @property
    def is_ready(self) -> bool:
        return False


class NotAnIndicator:
    """Class that doesn't inherit from BaseIndicator."""

    pass


class MockStrategyConfig(StrategyConfig):
    """Mock strategy config for testing."""

    name: str = "mock_strategy"
    display_name: str = "Mock Strategy"
    warmup_bars: int = 10


class MockStrategy(Strategy):
    """Mock strategy for testing registry."""

    def __init__(self, config: StrategyConfig):
        self.config = config

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        pass


class AnotherStrategy(Strategy):
    """Another mock strategy for testing."""

    def __init__(self, config: StrategyConfig):
        self.config = config

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        pass


# ============================================================================
# Test Fixtures - Pytest Setup
# ============================================================================


@pytest.fixture
def indicator_registry() -> IndicatorRegistry:
    """Create fresh indicator registry."""
    return IndicatorRegistry()


@pytest.fixture
def strategy_registry() -> StrategyRegistry:
    """Create fresh strategy registry."""
    return StrategyRegistry()


@pytest.fixture
def base_registry() -> BaseRegistry[BaseIndicator]:
    """Create fresh base registry for indicators."""
    return BaseRegistry(BaseIndicator, "test_indicator")


@pytest.fixture
def temp_module_dir() -> Any:  # Generator[Path, None, None]
    """Create temporary directory for module discovery tests."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


# ============================================================================
# Test RegistryError Hierarchy
# ============================================================================


class TestRegistryErrors:
    """Test custom exception classes."""

    def test_registry_error_is_exception(self) -> None:
        """RegistryError should be an Exception."""
        # Arrange & Act
        error = RegistryError("test error")

        # Assert
        assert isinstance(error, Exception)
        assert str(error) == "test error"

    def test_component_not_found_error_is_registry_error(self) -> None:
        """ComponentNotFoundError should inherit from RegistryError."""
        # Arrange & Act
        error = ComponentNotFoundError("component not found")

        # Assert
        assert isinstance(error, RegistryError)
        assert isinstance(error, Exception)

    def test_duplicate_component_error_is_registry_error(self) -> None:
        """DuplicateComponentError should inherit from RegistryError."""
        # Arrange & Act
        error = DuplicateComponentError("duplicate component")

        # Assert
        assert isinstance(error, RegistryError)

    def test_invalid_component_error_is_registry_error(self) -> None:
        """InvalidComponentError should inherit from RegistryError."""
        # Arrange & Act
        error = InvalidComponentError("invalid component")

        # Assert
        assert isinstance(error, RegistryError)


# ============================================================================
# Test BaseRegistry - Core Registration
# ============================================================================


class TestBaseRegistryRegistration:
    """Test component registration in BaseRegistry."""

    def test_register_valid_component_succeeds(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """Registering valid component should succeed."""
        # Arrange & Act
        base_registry.register("mock", MockIndicator)

        # Assert
        assert "mock" in base_registry
        assert len(base_registry) == 1

    def test_register_component_with_metadata(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """Registering component with metadata should store metadata."""
        # Arrange
        metadata = {"source": "test", "version": "1.0"}

        # Act
        base_registry.register("mock", MockIndicator, metadata=metadata)

        # Assert
        stored_metadata = base_registry.get_metadata("mock")
        assert stored_metadata == metadata

    def test_register_invalid_component_raises_error(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """Registering class that doesn't inherit from base class should raise InvalidComponentError."""
        # Arrange & Act & Assert
        with pytest.raises(InvalidComponentError) as exc_info:
            base_registry.register("invalid", NotAnIndicator)  # type: ignore[arg-type]

        assert "does not inherit from" in str(exc_info.value)

    def test_register_duplicate_name_raises_error(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """Registering duplicate name should raise DuplicateComponentError."""
        # Arrange
        base_registry.register("mock", MockIndicator)

        # Act & Assert
        with pytest.raises(DuplicateComponentError) as exc_info:
            base_registry.register("mock", AnotherIndicator)

        assert "already registered" in str(exc_info.value)

    def test_register_duplicate_with_override_succeeds(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """Registering duplicate with allow_override=True should succeed."""
        # Arrange
        base_registry.register("mock", MockIndicator)

        # Act
        base_registry.register("mock", AnotherIndicator, allow_override=True)

        # Assert
        component = base_registry.get("mock")
        assert component == AnotherIndicator


# ============================================================================
# Test BaseRegistry - Retrieval and Listing
# ============================================================================


class TestBaseRegistryRetrieval:
    """Test component retrieval from BaseRegistry."""

    def test_get_existing_component_returns_class(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """Getting existing component should return component class."""
        # Arrange
        base_registry.register("mock", MockIndicator)

        # Act
        component = base_registry.get("mock")

        # Assert
        assert component == MockIndicator

    def test_get_nonexistent_component_raises_error(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """Getting nonexistent component should raise ComponentNotFoundError."""
        # Arrange & Act & Assert
        with pytest.raises(ComponentNotFoundError) as exc_info:
            base_registry.get("nonexistent")

        assert "not found" in str(exc_info.value)
        assert "Available:" in str(exc_info.value)

    def test_list_names_returns_sorted_names(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """list_names should return sorted list of component names."""
        # Arrange
        base_registry.register("zebra", MockIndicator)
        base_registry.register("alpha", AnotherIndicator)

        # Act
        names = base_registry.list_names()

        # Assert
        assert names == ["alpha", "zebra"]

    def test_list_names_empty_registry_returns_empty_list(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """list_names on empty registry should return empty list."""
        # Arrange & Act
        names = base_registry.list_names()

        # Assert
        assert names == []

    def test_list_components_returns_all_components(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """list_components should return dict of all registered components."""
        # Arrange
        base_registry.register("mock", MockIndicator)
        base_registry.register("another", AnotherIndicator)

        # Act
        components = base_registry.list_components()

        # Assert
        assert len(components) == 2
        assert components["mock"] == MockIndicator
        assert components["another"] == AnotherIndicator

    def test_get_metadata_existing_component_returns_metadata(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """get_metadata for existing component should return metadata."""
        # Arrange
        metadata = {"source": "test"}
        base_registry.register("mock", MockIndicator, metadata=metadata)

        # Act
        result = base_registry.get_metadata("mock")

        # Assert
        assert result == metadata

    def test_get_metadata_nonexistent_component_raises_error(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """get_metadata for nonexistent component should raise ComponentNotFoundError."""
        # Arrange & Act & Assert
        with pytest.raises(ComponentNotFoundError):
            base_registry.get_metadata("nonexistent")


# ============================================================================
# Test BaseRegistry - Utility Methods
# ============================================================================


class TestBaseRegistryUtilities:
    """Test utility methods of BaseRegistry."""

    def test_clear_removes_all_components(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """clear should remove all registered components."""
        # Arrange
        base_registry.register("mock1", MockIndicator)
        base_registry.register("mock2", AnotherIndicator)

        # Act
        base_registry.clear()

        # Assert
        assert len(base_registry) == 0
        assert base_registry.list_names() == []

    def test_len_returns_component_count(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """len should return number of registered components."""
        # Arrange
        base_registry.register("mock1", MockIndicator)
        base_registry.register("mock2", AnotherIndicator)

        # Act
        length = len(base_registry)

        # Assert
        assert length == 2

    def test_contains_existing_component_returns_true(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """__contains__ should return True for existing component."""
        # Arrange
        base_registry.register("mock", MockIndicator)

        # Act & Assert
        assert "mock" in base_registry

    def test_contains_nonexistent_component_returns_false(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """__contains__ should return False for nonexistent component."""
        # Arrange & Act & Assert
        assert "nonexistent" not in base_registry

    def test_repr_shows_registry_info(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """__repr__ should show registry type and count."""
        # Arrange
        base_registry.register("mock", MockIndicator)

        # Act
        repr_str = repr(base_registry)

        # Assert
        assert "BaseRegistry" in repr_str
        assert "test_indicators=1" in repr_str


# ============================================================================
# Test BaseRegistry - Module Discovery
# ============================================================================


class TestBaseRegistryModuleDiscovery:
    """Test module discovery functionality."""

    def test_discover_from_module_nonexistent_file_returns_zero(
        self, base_registry: BaseRegistry[BaseIndicator]
    ) -> None:
        """discover_from_module with nonexistent file should return 0."""
        # Arrange
        nonexistent = Path("/nonexistent/module.py")

        # Act
        count = base_registry.discover_from_module(nonexistent)

        # Assert
        assert count == 0

    def test_discover_from_module_underscore_file_skipped(
        self, base_registry: BaseRegistry[BaseIndicator], temp_module_dir: Path
    ) -> None:
        """discover_from_module should skip files starting with underscore."""
        # Arrange
        test_file = temp_module_dir / "__init__.py"
        test_file.write_text("")

        # Act
        count = base_registry.discover_from_module(test_file)

        # Assert
        assert count == 0

    def test_discover_from_directory_nonexistent_returns_zero(self, base_registry: BaseRegistry[BaseIndicator]) -> None:
        """discover_from_directory with nonexistent directory should return 0."""
        # Arrange
        nonexistent = Path("/nonexistent/directory")

        # Act
        count = base_registry.discover_from_directory(nonexistent)

        # Assert
        assert count == 0

    def test_discover_from_directory_empty_returns_zero(
        self, base_registry: BaseRegistry[BaseIndicator], temp_module_dir: Path
    ) -> None:
        """discover_from_directory with empty directory should return 0."""
        # Arrange - temp_module_dir is empty

        # Act
        count = base_registry.discover_from_directory(temp_module_dir)

        # Assert
        assert count == 0


# ============================================================================
# Test IndicatorRegistry
# ============================================================================


class TestIndicatorRegistry:
    """Test IndicatorRegistry specialized functionality."""

    def test_init_creates_indicator_registry(self) -> None:
        """__init__ should create registry for BaseIndicator."""
        # Arrange & Act
        registry = IndicatorRegistry()

        # Assert
        assert registry.base_class == BaseIndicator
        assert registry.component_type == "indicator"
        assert len(registry) == 0

    def test_register_valid_indicator_succeeds(self, indicator_registry: IndicatorRegistry) -> None:
        """Registering valid indicator should succeed."""
        # Arrange & Act
        indicator_registry.register("mock", MockIndicator)

        # Assert
        assert "mock" in indicator_registry
        assert indicator_registry.get("mock") == MockIndicator

    def test_discover_with_defaults_returns_counts(self, indicator_registry: IndicatorRegistry) -> None:
        """discover with default paths should return counts dict."""
        # Arrange & Act
        counts = indicator_registry.discover()

        # Assert
        assert isinstance(counts, dict)
        assert "buildin" in counts
        assert "custom" in counts
        assert isinstance(counts["buildin"], int)
        assert isinstance(counts["custom"], int)

    def test_discover_buildin_indicators_from_real_path(self, indicator_registry: IndicatorRegistry) -> None:
        """discover should find built-in indicators from actual path."""
        # Arrange
        buildin_path = Path(__file__).parent.parent.parent.parent / "src/qs_trader/libraries/indicators/buildin"

        # Act
        counts = indicator_registry.discover(buildin_path=buildin_path)

        # Assert - should find at least SMA, EMA, WMA from moving_averages.py
        assert counts["buildin"] >= 3, f"Expected at least 3 builtin indicators, found {counts['buildin']}"


class TestGetIndicatorRegistry:
    """Test get_indicator_registry factory function."""

    def test_get_indicator_registry_returns_instance(self) -> None:
        """get_indicator_registry should return IndicatorRegistry instance."""
        # Arrange & Act
        registry = get_indicator_registry()

        # Assert
        assert isinstance(registry, IndicatorRegistry)


# ============================================================================
# Test StrategyRegistry
# ============================================================================


class TestStrategyRegistry:
    """Test StrategyRegistry specialized functionality."""

    def test_init_creates_strategy_registry(self) -> None:
        """__init__ should create registry for Strategy."""
        # Arrange & Act
        registry = StrategyRegistry()

        # Assert
        assert registry.base_class == Strategy
        assert registry.component_type == "strategy"
        assert len(registry) == 0

    def test_register_valid_strategy_succeeds(self, strategy_registry: StrategyRegistry) -> None:
        """Registering valid strategy should succeed."""
        # Arrange & Act
        strategy_registry.register("mock", MockStrategy)

        # Assert
        assert "mock" in strategy_registry
        assert strategy_registry.get("mock") == MockStrategy

    def test_discover_with_defaults_returns_counts(self, strategy_registry: StrategyRegistry) -> None:
        """discover with default paths should return counts dict."""
        # Arrange & Act
        counts = strategy_registry.discover()

        # Assert
        assert isinstance(counts, dict)
        assert "buildin" in counts
        assert "custom" in counts


class TestGetStrategyRegistry:
    """Test get_strategy_registry factory function."""

    def test_get_strategy_registry_returns_instance(self) -> None:
        """get_strategy_registry should return StrategyRegistry instance."""
        # Arrange & Act
        registry = get_strategy_registry()

        # Assert
        assert isinstance(registry, StrategyRegistry)
