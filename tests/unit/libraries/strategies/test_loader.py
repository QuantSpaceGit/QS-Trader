"""
Unit tests for StrategyLoader - Auto-discovery of Strategy classes.

Tests:
- Discovery of strategies from directory
- Config extraction (CONFIG variable and fallback)
- Error handling (import errors, missing config, duplicate names)
- Multiple strategies in same directory
- Proper class and config pairing

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- pytest fixtures
- Focus on discovery logic
"""

from pathlib import Path

import pytest

from qs_trader.libraries.strategies import StrategyConfig, StrategyLoader
from qs_trader.libraries.strategies.loader import StrategyLoadError


@pytest.fixture
def fixtures_dir() -> Path:
    """Get path to test fixtures directory."""
    # Go up from test_loader.py to tests/fixtures/strategies
    return Path(__file__).parent.parent.parent.parent / "fixtures" / "strategies"


@pytest.fixture
def loader() -> StrategyLoader:
    """Create fresh loader instance."""
    return StrategyLoader()


class TestStrategyLoaderBasics:
    """Test basic StrategyLoader functionality."""

    def test_loader_initialization(self, loader: StrategyLoader) -> None:
        """Loader initializes with empty registry."""
        # Arrange & Act - fixture creates loader

        # Assert
        assert loader.loaded_count == 0
        assert loader.get_strategy_names() == []

    def test_load_nonexistent_directory_fails(self, loader: StrategyLoader) -> None:
        """Loading from nonexistent directory raises error."""
        # Arrange
        bad_path = Path("/nonexistent/directory")

        # Act & Assert
        with pytest.raises(StrategyLoadError, match="does not exist"):
            loader.load_from_directory(bad_path)

    def test_load_file_as_directory_fails(self, loader: StrategyLoader, tmp_path: Path) -> None:
        """Loading from file path (not directory) raises error."""
        # Arrange
        file_path = tmp_path / "test.py"
        file_path.write_text("# test file")

        # Act & Assert
        with pytest.raises(StrategyLoadError, match="not a directory"):
            loader.load_from_directory(file_path)


class TestStrategyDiscovery:
    """Test strategy discovery from directory."""

    def test_discover_single_strategy(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Can discover a single strategy with CONFIG."""
        # Arrange & Act
        strategies = loader.load_from_directory(fixtures_dir)

        # Assert - should find sma_crossover and mean_reversion
        assert loader.loaded_count >= 1
        assert "sma_crossover" in strategies

        # Check sma_crossover details
        cls, config = strategies["sma_crossover"]
        assert cls.__name__ == "SMAStrategy"
        assert isinstance(config, StrategyConfig)
        assert config.name == "sma_crossover"
        assert config.display_name == "SMA Crossover"

    def test_discover_multiple_strategies(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Can discover multiple strategies from same directory."""
        # Arrange & Act
        strategies = loader.load_from_directory(fixtures_dir)

        # Assert
        assert loader.loaded_count >= 2
        assert "sma_crossover" in strategies
        assert "mean_reversion" in strategies

        # Verify each has proper class and config
        for name, (cls, config) in strategies.items():
            if name == "sma_crossover":
                assert cls.__name__ == "SMAStrategy"
                assert config.display_name == "SMA Crossover"
            elif name == "mean_reversion":
                assert cls.__name__ == "MeanReversionStrategy"
                assert config.display_name == "Mean Reversion"

    def test_get_strategy_names(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Can get list of loaded strategy names."""
        # Arrange
        loader.load_from_directory(fixtures_dir)

        # Act
        names = loader.get_strategy_names()

        # Assert
        assert "sma_crossover" in names
        assert "mean_reversion" in names
        assert isinstance(names, list)


class TestConfigExtraction:
    """Test CONFIG variable extraction."""

    def test_extract_config_with_uppercase_convention(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Extracts CONFIG from uppercase CONFIG variable."""
        # Arrange & Act
        strategies = loader.load_from_directory(fixtures_dir)

        # Assert - sma_crossover uses CONFIG convention
        _, config = strategies["sma_crossover"]
        assert config.name == "sma_crossover"

    def test_extract_config_with_lowercase_fallback(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Falls back to any StrategyConfig instance if CONFIG missing."""
        # Arrange & Act
        strategies = loader.load_from_directory(fixtures_dir)

        # Assert - mean_reversion uses 'config' (lowercase)
        _, config = strategies["mean_reversion"]
        assert config.name == "mean_reversion"


class TestErrorHandling:
    """Test error handling for problematic strategies."""

    def test_skip_file_with_import_errors(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Gracefully skips files with import errors."""
        # Arrange & Act
        strategies = loader.load_from_directory(fixtures_dir)

        # Assert - broken_strategy.py should be skipped
        assert "broken_strategy" not in strategies
        # But other strategies should still load
        assert "sma_crossover" in strategies

    def test_skip_strategy_without_config(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Skips strategies that don't have a config."""
        # Arrange & Act
        strategies = loader.load_from_directory(fixtures_dir)

        # Assert - no_config.py should be skipped
        # (no CONFIG variable and we need config to register)
        assert "no_config_strategy" not in strategies

    def test_duplicate_strategy_names_skips_duplicate(self, loader: StrategyLoader, tmp_path: Path) -> None:
        """Skips duplicate strategy names (graceful handling)."""
        # Arrange - create two strategies with same name
        (tmp_path / "strategy1.py").write_text(
            """
from qs_trader.libraries.strategies import Strategy, StrategyConfig, Context
from qs_trader.events.events import PriceBarEvent

class DupConfig(StrategyConfig):
    name: str = "duplicate"
    display_name: str = "Duplicate"

CONFIG = DupConfig()

class Strategy1(Strategy):
    def __init__(self, config):
        self.config = config
    def on_bar(self, event: PriceBarEvent, context: Context):
        pass
"""
        )

        (tmp_path / "strategy2.py").write_text(
            """
from qs_trader.libraries.strategies import Strategy, StrategyConfig, Context
from qs_trader.events.events import PriceBarEvent

class DupConfig2(StrategyConfig):
    name: str = "duplicate"  # Same name!
    display_name: str = "Duplicate 2"

CONFIG = DupConfig2()

class Strategy2(Strategy):
    def __init__(self, config):
        self.config = config
    def on_bar(self, event: PriceBarEvent, context: Context):
        pass
"""
        )

        # Act
        strategies = loader.load_from_directory(tmp_path)

        # Assert - should only load one (first found), skip duplicate
        assert len(strategies) == 1
        assert "duplicate" in strategies


class TestLoaderState:
    """Test loader state management."""

    def test_clear_loaded_strategies(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Can clear all loaded strategies."""
        # Arrange
        loader.load_from_directory(fixtures_dir)
        assert loader.loaded_count > 0

        # Act
        loader.clear()

        # Assert
        assert loader.loaded_count == 0
        assert loader.get_strategy_names() == []

    def test_load_multiple_times_replaces(self, loader: StrategyLoader, fixtures_dir: Path, tmp_path: Path) -> None:
        """Loading from new directory replaces previous strategies."""
        # Arrange - create another strategy in different dir
        (tmp_path / "extra.py").write_text(
            """
from qs_trader.libraries.strategies import Strategy, StrategyConfig, Context
from qs_trader.events.events import PriceBarEvent

class ExtraConfig(StrategyConfig):
    name: str = "extra_strategy"
    display_name: str = "Extra"

CONFIG = ExtraConfig()

class ExtraStrategy(Strategy):
    def __init__(self, config):
        self.config = config
    def on_bar(self, event: PriceBarEvent, context: Context):
        pass
"""
        )

        # Act
        loader.load_from_directory(fixtures_dir)
        initial_count = loader.loaded_count
        loader.load_from_directory(tmp_path)

        # Assert - second load replaces first
        assert initial_count >= 2  # Had sma_crossover and mean_reversion
        assert loader.loaded_count == 1  # Now only has extra_strategy
        assert "extra_strategy" in loader.get_strategy_names()
        assert "sma_crossover" not in loader.get_strategy_names()


class TestRecursiveLoading:
    """Test recursive directory loading."""

    def test_recursive_loading_disabled_by_default(self, loader: StrategyLoader, tmp_path: Path) -> None:
        """By default, doesn't search subdirectories."""
        # Arrange
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        (subdir / "nested.py").write_text(
            """
from qs_trader.libraries.strategies import Strategy, StrategyConfig, Context
from qs_trader.events.events import PriceBarEvent

class NestedConfig(StrategyConfig):
    name: str = "nested"
    display_name: str = "Nested"

CONFIG = NestedConfig()

class NestedStrategy(Strategy):
    def __init__(self, config):
        self.config = config
    def on_bar(self, event: PriceBarEvent, context: Context):
        pass
"""
        )

        # Act
        strategies = loader.load_from_directory(tmp_path, recursive=False)

        # Assert
        assert "nested" not in strategies

    def test_recursive_loading_finds_nested_strategies(self, loader: StrategyLoader, tmp_path: Path) -> None:
        """With recursive=True, finds strategies in subdirectories."""
        # Arrange
        subdir = tmp_path / "subdir"
        subdir.mkdir()

        (subdir / "nested.py").write_text(
            """
from qs_trader.libraries.strategies import Strategy, StrategyConfig, Context
from qs_trader.events.events import PriceBarEvent

class NestedConfig(StrategyConfig):
    name: str = "nested"
    display_name: str = "Nested"

CONFIG = NestedConfig()

class NestedStrategy(Strategy):
    def __init__(self, config):
        self.config = config
    def on_bar(self, event: PriceBarEvent, context: Context):
        pass
"""
        )

        # Act
        strategies = loader.load_from_directory(tmp_path, recursive=True)

        # Assert
        assert "nested" in strategies


class TestStrategyInstantiation:
    """Test that discovered strategies can be instantiated."""

    def test_instantiate_discovered_strategy(self, loader: StrategyLoader, fixtures_dir: Path) -> None:
        """Can instantiate a strategy class loaded from directory."""
        # Arrange
        strategies = loader.load_from_directory(fixtures_dir)
        strategy_class, config = strategies["sma_crossover"]

        # Act
        strategy = strategy_class(config)

        # Assert
        assert strategy is not None
        assert hasattr(strategy, "on_bar")
        assert hasattr(strategy, "config")
        assert strategy.config.name == "sma_crossover"
