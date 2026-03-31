"""
Unit tests for StrategyRegistry integration with StrategyLoader.

Tests:
- Registry initialization
- Loading strategies from directory
- Getting strategy classes and configs
- Listing strategies with metadata
- Integration between loader and registry

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- pytest fixtures
- Focus on registry integration
"""

from pathlib import Path

import pytest

from qs_trader.libraries.registry import StrategyRegistry
from qs_trader.libraries.strategies import Strategy, StrategyConfig


@pytest.fixture
def fixtures_dir() -> Path:
    """Get path to test fixtures directory."""
    # Go up from test_registry_strategies.py to tests/fixtures/strategies
    return Path(__file__).parent.parent.parent / "fixtures" / "strategies"


@pytest.fixture
def registry() -> StrategyRegistry:
    """Create fresh registry instance."""
    return StrategyRegistry()


class TestStrategyRegistryBasics:
    """Test basic StrategyRegistry functionality."""

    def test_registry_initialization(self, registry: StrategyRegistry) -> None:
        """Registry initializes empty."""
        # Arrange & Act - fixture creates registry

        # Assert
        assert len(registry.list_names()) == 0

    def test_registry_inherits_from_base(self, registry: StrategyRegistry) -> None:
        """StrategyRegistry inherits from BaseRegistry."""
        # Arrange & Act - fixture creates registry

        # Assert
        assert hasattr(registry, "register")
        assert hasattr(registry, "get")
        assert hasattr(registry, "list_names")


class TestLoadFromDirectory:
    """Test loading strategies from directory."""

    def test_load_from_directory(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Can load strategies from directory."""
        # Arrange & Act
        registry.load_from_directory(fixtures_dir)

        # Assert
        names = registry.list_names()
        assert len(names) >= 2
        assert "sma_crossover" in names
        assert "mean_reversion" in names

    def test_load_updates_internal_configs(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Loading stores both classes and configs."""
        # Arrange & Act
        registry.load_from_directory(fixtures_dir)

        # Assert - should have configs stored
        config = registry.get_strategy_config("sma_crossover")
        assert config is not None
        assert config.name == "sma_crossover"


class TestGetStrategyClass:
    """Test getting strategy classes from registry."""

    def test_get_strategy_class_existing(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Can get strategy class by name."""
        # Arrange
        registry.load_from_directory(fixtures_dir)

        # Act
        cls = registry.get_strategy_class("sma_crossover")

        # Assert
        assert cls is not None
        assert issubclass(cls, Strategy)
        assert cls.__name__ == "SMAStrategy"

    def test_get_strategy_class_nonexistent(self, registry: StrategyRegistry) -> None:
        """Raises error for nonexistent strategy."""
        # Arrange & Act & Assert
        with pytest.raises(Exception):  # ComponentNotFoundError
            registry.get_strategy_class("nonexistent")

    def test_get_strategy_class_after_load(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Can get multiple strategy classes."""
        # Arrange
        registry.load_from_directory(fixtures_dir)

        # Act
        sma_cls = registry.get_strategy_class("sma_crossover")
        mr_cls = registry.get_strategy_class("mean_reversion")

        # Assert
        assert sma_cls is not None
        assert mr_cls is not None
        assert sma_cls != mr_cls  # Different classes


class TestGetStrategyConfig:
    """Test getting strategy configs from registry."""

    def test_get_strategy_config_existing(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Can get strategy config by name."""
        # Arrange
        registry.load_from_directory(fixtures_dir)

        # Act
        config = registry.get_strategy_config("sma_crossover")

        # Assert
        assert config is not None
        assert isinstance(config, StrategyConfig)
        assert config.name == "sma_crossover"
        assert config.display_name == "SMA Crossover"

    def test_get_strategy_config_nonexistent(self, registry: StrategyRegistry) -> None:
        """Raises error for nonexistent strategy."""
        # Arrange & Act & Assert
        with pytest.raises(Exception):  # ComponentNotFoundError
            registry.get_strategy_config("nonexistent")

    def test_get_strategy_config_has_parameters(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Config contains strategy parameters."""
        # Arrange
        registry.load_from_directory(fixtures_dir)

        # Act
        config = registry.get_strategy_config("sma_crossover")

        # Assert
        # SMAConfig subclass has fast_period and slow_period
        # Access via getattr since registry returns base StrategyConfig type
        assert getattr(config, "fast_period", None) == 10
        assert getattr(config, "slow_period", None) == 20
        # Config is Pydantic model
        assert hasattr(config, "model_extra")


class TestListStrategies:
    """Test listing strategies with metadata."""

    def test_list_strategies_empty(self, registry: StrategyRegistry) -> None:
        """Empty registry returns empty dict."""
        # Arrange & Act
        strategies = registry.list_strategies()

        # Assert
        assert strategies == {}

    def test_list_strategies_with_metadata(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """List returns metadata for each strategy."""
        # Arrange
        registry.load_from_directory(fixtures_dir)

        # Act
        strategies = registry.list_strategies()

        # Assert
        assert "sma_crossover" in strategies
        metadata = strategies["sma_crossover"]
        assert "class_name" in metadata
        assert "display_name" in metadata
        assert "description" in metadata
        assert metadata["class_name"] == "SMAStrategy"
        assert metadata["display_name"] == "SMA Crossover"

    def test_list_strategies_includes_all_loaded(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """List includes all loaded strategies."""
        # Arrange
        registry.load_from_directory(fixtures_dir)

        # Act
        strategies = registry.list_strategies()

        # Assert
        assert len(strategies) >= 2
        assert "sma_crossover" in strategies
        assert "mean_reversion" in strategies


class TestRegistryIntegration:
    """Test integration between registry and loader."""

    def test_instantiate_from_registry(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Can instantiate strategy from registry."""
        # Arrange
        registry.load_from_directory(fixtures_dir)
        cls = registry.get_strategy_class("sma_crossover")
        config = registry.get_strategy_config("sma_crossover")

        # Act
        strategy = cls(config)

        # Assert
        assert strategy is not None
        assert hasattr(strategy, "on_bar")
        assert strategy.config.name == "sma_crossover"

    def test_registry_get_by_name(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Can use base registry.get() method."""
        # Arrange
        registry.load_from_directory(fixtures_dir)

        # Act
        cls = registry.get("sma_crossover")

        # Assert
        assert cls is not None
        assert issubclass(cls, Strategy)

    def test_class_and_config_match(self, registry: StrategyRegistry, fixtures_dir: Path) -> None:
        """Class and config have consistent names."""
        # Arrange
        registry.load_from_directory(fixtures_dir)

        # Act
        for name in registry.list_names():
            cls = registry.get_strategy_class(name)
            config = registry.get_strategy_config(name)

            # Assert
            assert cls is not None
            assert config is not None
            assert config.name == name
