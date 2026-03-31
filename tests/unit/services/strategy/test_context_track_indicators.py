"""Tests for Context.track_indicators() display_names feature."""

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import IndicatorEvent
from qs_trader.libraries.strategies import StrategyConfig
from qs_trader.services.strategy.context import Context


@pytest.fixture
def event_bus():
    """Create event bus for testing."""
    return EventBus()


@pytest.fixture
def strategy_config():
    """Create minimal strategy config."""
    return StrategyConfig(
        name="test_strategy",
        display_name="Test Strategy",
        universe=["AAPL"],
        log_indicators=True,  # Enable indicator logging
    )


@pytest.fixture
def context(event_bus, strategy_config):
    """Create context for testing."""
    return Context(
        strategy_id="test_strategy",
        event_bus=event_bus,
        config={"log_indicators": True},  # Feature flag for indicator logging
    )


class TestTrackIndicatorsDisplayNames:
    """Test suite for track_indicators() display_names parameter."""

    def test_track_indicators_without_display_names(self, context):
        """track_indicators should use indicator keys as names when display_names not provided."""
        # Act
        context.track_indicators(
            indicators={
                "sma_10": 150.5,
                "sma_20": 149.8,
                "rsi": 65.4,
            }
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert indicators == {
            "sma_10": 150.5,
            "sma_20": 149.8,
            "rsi": 65.4,
        }

    def test_track_indicators_with_display_names(self, context):
        """track_indicators should use display_names when provided."""
        # Act
        context.track_indicators(
            indicators={
                "fast_sma": 150.5,
                "slow_sma": 149.8,
                "rsi": 65.4,
            },
            display_names={
                "fast_sma": "fast_sma(10)",
                "slow_sma": "slow_sma(50)",
                "rsi": "rsi(14)",
            },
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert indicators == {
            "fast_sma(10)": 150.5,
            "slow_sma(50)": 149.8,
            "rsi(14)": 65.4,
        }

    def test_track_indicators_partial_display_names(self, context):
        """track_indicators should use keys for unmapped indicators."""
        # Act
        context.track_indicators(
            indicators={
                "fast_sma": 150.5,
                "slow_sma": 149.8,
                "crossover": True,
            },
            display_names={
                "fast_sma": "fast_sma(10)",
                "slow_sma": "slow_sma(50)",
                # crossover not mapped - should use key
            },
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert indicators == {
            "fast_sma(10)": 150.5,
            "slow_sma(50)": 149.8,
            "crossover": 1.0,  # bool converted to float
        }

    def test_track_indicators_bool_conversion_with_display_names(self, context):
        """track_indicators should convert bools to floats even with display_names."""
        # Act
        context.track_indicators(
            indicators={
                "golden_cross": True,
                "death_cross": False,
            },
            display_names={
                "golden_cross": "golden_cross",
                "death_cross": "death_cross",
            },
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert indicators == {
            "golden_cross": 1.0,
            "death_cross": 0.0,
        }

    def test_track_indicators_accumulates_with_display_names(self, context):
        """Multiple track_indicators calls should accumulate with display_names."""
        # Act
        context.track_indicators(
            indicators={"fast_sma": 150.5},
            display_names={"fast_sma": "fast_sma(10)"},
        )
        context.track_indicators(
            indicators={"slow_sma": 149.8},
            display_names={"slow_sma": "slow_sma(50)"},
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert indicators == {
            "fast_sma(10)": 150.5,
            "slow_sma(50)": 149.8,
        }

    def test_track_indicators_overwrites_same_display_name(self, context):
        """Later calls should overwrite indicators with same display name."""
        # Act
        context.track_indicators(
            indicators={"sma": 150.5},
            display_names={"sma": "sma(10)"},
        )
        context.track_indicators(
            indicators={"sma": 151.0},
            display_names={"sma": "sma(10)"},
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert indicators == {"sma(10)": 151.0}

    def test_emit_indicator_event_uses_display_names(self, context, event_bus):
        """Emitted IndicatorEvent should contain display names."""
        # Arrange
        events = []
        event_bus.subscribe("indicator", lambda e: events.append(e))

        # Act
        context.track_indicators(
            indicators={
                "fast_sma": 150.5,
                "slow_sma": 149.8,
            },
            display_names={
                "fast_sma": "fast_sma(10)",
                "slow_sma": "slow_sma(50)",
            },
        )

        # Emit event (normally done by StrategyService)
        context.emit_indicator_event(
            symbol="AAPL",
            timestamp="2023-01-01T10:00:00+00:00",
            indicators=context.get_tracked_indicators(),
        )

        # Assert
        assert len(events) == 1
        indicator_event = events[0]
        assert isinstance(indicator_event, IndicatorEvent)
        assert indicator_event.indicators == {
            "fast_sma(10)": 150.5,
            "slow_sma(50)": 149.8,
        }

    def test_track_indicators_complex_display_names(self, context):
        """track_indicators should handle complex display name formats."""
        # Act
        context.track_indicators(
            indicators={
                "bb_upper": 152.0,
                "bb_middle": 150.0,
                "bb_lower": 148.0,
                "ema": 150.5,
            },
            display_names={
                "bb_upper": "bb_upper(period=20,std=2.0,ddof=1)",
                "bb_middle": "bb_middle(period=20,std=2.0,ddof=1)",
                "bb_lower": "bb_lower(period=20,std=2.0,ddof=1)",
                "ema": "ema(20,smoothing=2.5)",
            },
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert "bb_upper(period=20,std=2.0,ddof=1)" in indicators
        assert "bb_middle(period=20,std=2.0,ddof=1)" in indicators
        assert "bb_lower(period=20,std=2.0,ddof=1)" in indicators
        assert "ema(20,smoothing=2.5)" in indicators

    def test_track_indicators_empty_display_names(self, context):
        """track_indicators with empty display_names dict should use keys."""
        # Act
        context.track_indicators(
            indicators={
                "sma": 150.5,
                "ema": 150.8,
            },
            display_names={},  # Empty dict
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert indicators == {
            "sma": 150.5,
            "ema": 150.8,
        }

    def test_track_indicators_display_names_with_special_chars(self, context):
        """track_indicators should handle display names with special characters."""
        # Act
        context.track_indicators(
            indicators={
                "williams_r": -45.5,
                "stoch_k": 75.2,
            },
            display_names={
                "williams_r": "williams_%r(14)",
                "stoch_k": "stoch_%k(14,3,3)",
            },
        )

        # Assert
        indicators = context.get_tracked_indicators()
        assert indicators == {
            "williams_%r(14)": -45.5,
            "stoch_%k(14,3,3)": 75.2,
        }
