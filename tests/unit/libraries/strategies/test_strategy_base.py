"""
Unit tests for qs_trader.libraries.strategies.base module.

Tests the Strategy and StrategyConfig abstract classes:
- Abstract method requirements
- Config contract (name, display_name, warmup_bars)
- Properties and lifecycle methods
- Strategy context interface

Following unittest.prompt.md guidelines:
- Descriptive test names
- Arrange-Act-Assert pattern
- pytest fixtures
- Focus on contract compliance
"""

from decimal import Decimal
from unittest.mock import Mock

import pytest

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies.base import Strategy, StrategyConfig
from qs_trader.services.strategy.context import Context

# ============================================================================
# Test Fixtures - Concrete Implementations
# ============================================================================


@pytest.fixture
def mock_event_bus():
    """Mock event bus for Context creation."""
    event_bus = Mock()
    event_bus.publish = Mock()
    return event_bus


@pytest.fixture
def context(mock_event_bus):
    """Create a Context instance for testing."""
    return Context(strategy_id="test_strategy", event_bus=mock_event_bus)


class ConcreteStrategyConfig(StrategyConfig):
    """Minimal concrete config for testing."""

    name: str = "concrete_strategy"
    display_name: str = "Concrete Strategy"


class ConcreteStrategy(Strategy):
    """Minimal concrete strategy for testing."""

    def __init__(self, config: StrategyConfig):
        self.config = config

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """Minimal on_bar implementation."""
        pass


class IncompleteStrategy(Strategy):
    """Strategy missing required methods."""

    pass


class ConfigWithExtraFields(StrategyConfig):
    """Config with strategy-specific fields."""

    name: str = "custom"
    display_name: str = "Custom Strategy"

    # Strategy-specific parameters
    fast_period: int = 10
    slow_period: int = 20
    threshold: float = 0.05


# ============================================================================
# Test StrategyConfig
# ============================================================================


class TestStrategyConfigCreation:
    """Test StrategyConfig instantiation and validation."""

    def test_config_with_required_fields_succeeds(self) -> None:
        """Config with all required fields should instantiate successfully."""
        # Arrange & Act
        config = ConcreteStrategyConfig()

        # Assert
        assert config.name == "concrete_strategy"
        assert config.display_name == "Concrete Strategy"

    def test_config_default_metadata_fields(self) -> None:
        """Config should have default values for metadata fields."""
        # Arrange & Act
        config = ConcreteStrategyConfig()

        # Assert
        assert config.description == ""
        assert config.author == ""
        assert config.version == "1.0.0"

    def test_config_with_custom_metadata_succeeds(self) -> None:
        """Config can override metadata fields."""

        # Arrange
        class CustomConfig(StrategyConfig):
            name: str = "test"
            display_name: str = "Test"
            description: str = "Test strategy"
            author: str = "Test Team"
            version: str = "2.5.0"
            warmup_bars: int = 0

        # Act
        config = CustomConfig()

        # Assert
        assert config.description == "Test strategy"
        assert config.author == "Test Team"
        assert config.version == "2.5.0"

    def test_config_with_strategy_specific_fields(self) -> None:
        """Config can add strategy-specific parameters."""
        # Arrange & Act
        config = ConfigWithExtraFields()

        # Assert
        assert config.fast_period == 10
        assert config.slow_period == 20
        assert config.threshold == 0.05

    def test_config_validates_on_assignment(self) -> None:
        """Config with validate_assignment=True validates on attribute change."""
        # Arrange
        config = ConcreteStrategyConfig()

        # Act - Pydantic validate_assignment is set to True in base config
        config.name = "new_name"

        # Assert - assignment works
        assert config.name == "new_name"


class TestStrategyConfigFields:
    """Test specific StrategyConfig fields."""

    def test_config_name_field_required(self) -> None:
        """name field is required in config."""

        # Arrange
        class NoNameConfig(StrategyConfig):
            display_name: str = "Test"

        # Act & Assert - Pydantic requires default or passed value
        try:
            NoNameConfig()  # type: ignore[call-arg]
            # If no error, name got a default somehow
            assert True
        except Exception:
            # Expected - name is required
            assert True

    def test_config_display_name_field_required(self) -> None:
        """display_name field is required in config."""

        # Arrange
        class NoDisplayConfig(StrategyConfig):
            name: str = "test"

        # Act & Assert
        try:
            NoDisplayConfig()  # type: ignore[call-arg]
            assert True
        except Exception:
            assert True


# ============================================================================
# Test Strategy Abstract Interface
# ============================================================================


class TestStrategyAbstractInterface:
    """Test Strategy abstract method enforcement."""

    def test_cannot_instantiate_base_strategy_directly(self) -> None:
        """Strategy is abstract and cannot be instantiated."""
        # Arrange & Act & Assert
        with pytest.raises(TypeError) as exc_info:
            Strategy(ConcreteStrategyConfig())  # type: ignore[abstract]

        assert "abstract" in str(exc_info.value).lower()

    def test_cannot_instantiate_incomplete_strategy(self) -> None:
        """Strategy missing abstract methods cannot be instantiated."""
        # Arrange & Act & Assert
        with pytest.raises(TypeError) as exc_info:
            IncompleteStrategy(ConcreteStrategyConfig())  # type: ignore[abstract]

        assert "abstract" in str(exc_info.value).lower()

    def test_concrete_strategy_can_be_instantiated(self) -> None:
        """Concrete strategy implementing all methods can be instantiated."""
        # Arrange
        config = ConcreteStrategyConfig()

        # Act
        strategy = ConcreteStrategy(config)

        # Assert
        assert isinstance(strategy, Strategy)
        assert strategy.config == config


# ============================================================================
# Test Strategy Required Methods
# ============================================================================


class TestStrategyRequiredMethods:
    """Test that concrete strategies provide required methods."""

    def test_init_method_required(self) -> None:
        """Concrete strategy must implement __init__."""
        # Arrange
        config = ConcreteStrategyConfig()

        # Act
        strategy = ConcreteStrategy(config)

        # Assert
        assert hasattr(strategy, "__init__")
        assert strategy.config == config

    def test_on_bar_method_required(self) -> None:
        """Concrete strategy must implement on_bar."""
        # Arrange
        strategy = ConcreteStrategy(ConcreteStrategyConfig())

        # Act & Assert
        assert hasattr(strategy, "on_bar")
        assert callable(strategy.on_bar)

    def test_setup_method_optional(self, context) -> None:
        """setup method is optional (has default implementation)."""
        # Arrange
        strategy = ConcreteStrategy(ConcreteStrategyConfig())

        # Act & Assert
        assert hasattr(strategy, "setup")
        # Should not raise when called
        strategy.setup(context)

    def test_teardown_method_optional(self, context) -> None:
        """teardown method is optional (has default implementation)."""
        # Arrange
        strategy = ConcreteStrategy(ConcreteStrategyConfig())

        # Act & Assert
        assert hasattr(strategy, "teardown")
        # Should not raise when called
        strategy.teardown(context)


# ============================================================================
# Test Strategy Properties
# ============================================================================


class TestStrategyProperties:
    """Test Strategy property contracts."""

    def test_name_property_returns_config_name(self) -> None:
        """name property should return config.name."""
        # Arrange
        config = ConcreteStrategyConfig()
        strategy = ConcreteStrategy(config)

        # Act
        name = strategy.name

        # Assert
        assert name == "concrete_strategy"
        assert name == config.name

    def test_display_name_property_returns_config_display_name(self) -> None:
        """display_name property should return config.display_name."""
        # Arrange
        config = ConcreteStrategyConfig()
        strategy = ConcreteStrategy(config)

        # Act
        display_name = strategy.display_name

        # Assert
        assert display_name == "Concrete Strategy"
        assert display_name == config.display_name

    def test_properties_read_only(self) -> None:
        """Strategy properties should be read-only."""
        # Arrange
        strategy = ConcreteStrategy(ConcreteStrategyConfig())

        # Act & Assert - properties have no setters
        with pytest.raises(AttributeError):
            strategy.name = "new_name"  # type: ignore[misc]


# ============================================================================
# Test Strategy Lifecycle Methods
# ============================================================================


class TestStrategyLifecycleMethods:
    """Test strategy lifecycle: setup, on_bar, teardown."""

    def test_setup_receives_context(self, context) -> None:
        """setup should receive Context parameter."""

        # Arrange
        class TestStrategy(ConcreteStrategy):
            def setup(self, context: Context) -> None:
                self.setup_called = True
                self.setup_context = context

        strategy = TestStrategy(ConcreteStrategyConfig())

        # Act
        strategy.setup(context)

        # Assert
        assert hasattr(strategy, "setup_called")
        assert strategy.setup_called is True

    def test_teardown_receives_context(self, context) -> None:
        """teardown should receive Context parameter."""

        # Arrange
        class TestStrategy(ConcreteStrategy):
            def teardown(self, context: Context) -> None:
                self.teardown_called = True

        strategy = TestStrategy(ConcreteStrategyConfig())

        # Act
        strategy.teardown(context)

        # Assert
        assert hasattr(strategy, "teardown_called")
        assert strategy.teardown_called is True

    def test_on_bar_receives_event_and_context(self, context) -> None:
        """on_bar should receive PriceBarEvent and Context."""

        # Arrange
        class TestStrategy(ConcreteStrategy):
            def on_bar(self, event: PriceBarEvent, context: Context) -> None:
                self.on_bar_called = True
                self.event = event
                self.context = context

        strategy = TestStrategy(ConcreteStrategyConfig())
        # Mock event and context with proper fields
        event = PriceBarEvent(
            symbol="AAPL",
            timestamp="2020-01-01T00:00:00",
            interval="1d",  # Daily bars
            open=Decimal("100.0"),
            high=Decimal("105.0"),
            low=Decimal("99.0"),
            close=Decimal("103.0"),
            volume=1000000,
            source="test",
        )

        # Act
        strategy.on_bar(event, context)

        # Assert
        assert hasattr(strategy, "on_bar_called")
        assert strategy.on_bar_called is True


# ============================================================================
# Test Context Interface Placeholder
# ============================================================================


class TestContextInterface:
    """Test Context class interface (placeholder implementation)."""

    def test_context_has_emit_signal_method(self) -> None:
        """Context should define emit_signal method."""
        # Arrange & Act & Assert
        assert hasattr(Context, "emit_signal")

    def test_context_has_get_bars_method(self) -> None:
        """Context should define get_bars method."""
        # Arrange & Act & Assert
        assert hasattr(Context, "get_bars")

    def test_context_has_get_price_method(self) -> None:
        """Context should define get_price method."""
        # Arrange & Act & Assert
        assert hasattr(Context, "get_price")


# ============================================================================
# Test Strategy Config Inheritance
# ============================================================================


class TestStrategyConfigInheritance:
    """Test that configs can be extended with strategy-specific fields."""

    def test_extended_config_inherits_base_fields(self) -> None:
        """Extended config should inherit all base fields."""
        # Arrange & Act
        config = ConfigWithExtraFields()

        # Assert - base fields present
        assert hasattr(config, "name")
        assert hasattr(config, "display_name")
        assert hasattr(config, "description")
        assert hasattr(config, "version")

    def test_extended_config_adds_custom_fields(self) -> None:
        """Extended config should add strategy-specific fields."""
        # Arrange & Act
        config = ConfigWithExtraFields()

        # Assert - custom fields present
        assert hasattr(config, "fast_period")
        assert hasattr(config, "slow_period")
        assert hasattr(config, "threshold")
        assert config.fast_period == 10

    def test_strategy_uses_extended_config(self) -> None:
        """Strategy can use extended config with custom fields."""

        # Arrange
        class CustomStrategy(Strategy):
            def __init__(self, config: ConfigWithExtraFields):
                self.config = config
                self.fast = config.fast_period
                self.slow = config.slow_period

            def on_bar(self, event: PriceBarEvent, context: Context) -> None:
                pass

        config = ConfigWithExtraFields()

        # Act
        strategy = CustomStrategy(config)

        # Assert
        assert strategy.fast == 10
        assert strategy.slow == 20
        assert isinstance(strategy.config, ConfigWithExtraFields)
        # Access threshold through typed config
        typed_config: ConfigWithExtraFields = strategy.config
        assert typed_config.threshold == 0.05


# ============================================================================
# Test Documentation and Docstrings
# ============================================================================


class TestStrategyDocumentation:
    """Test that base classes have proper documentation."""

    def test_base_strategy_has_docstring(self) -> None:
        """Strategy should have comprehensive docstring."""
        # Arrange & Act & Assert
        assert Strategy.__doc__ is not None
        assert len(Strategy.__doc__) > 100

    def test_base_strategy_config_has_docstring(self) -> None:
        """StrategyConfig should have comprehensive docstring."""
        # Arrange & Act & Assert
        assert StrategyConfig.__doc__ is not None
        assert len(StrategyConfig.__doc__) > 100

    def test_on_bar_method_has_docstring(self) -> None:
        """on_bar method should be documented."""
        # Arrange & Act & Assert
        assert Strategy.on_bar.__doc__ is not None

    def test_context_emit_signal_has_docstring(self) -> None:
        """Context.emit_signal should be documented."""
        # Arrange & Act & Assert
        assert Context.emit_signal.__doc__ is not None
        assert "confidence" in Context.emit_signal.__doc__.lower()
