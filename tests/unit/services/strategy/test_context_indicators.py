"""
Tests for indicator tracking with visualization metadata.

Tests the enhanced Context.track_indicators() API with placements and colors.
"""

from unittest.mock import MagicMock

from qs_trader.services.strategy.context import Context


class TestTrackIndicatorsWithMetadata:
    """Test Context.track_indicators() with visualization metadata."""

    def test_track_indicators_with_placements(self):
        """Test tracking indicators with placement metadata."""
        # Setup
        event_bus = MagicMock()
        context = Context(strategy_id="test_strategy", event_bus=event_bus)

        # Track indicators with placements
        context.track_indicators(
            indicators={"sma": 150.0, "rsi": 65.0},
            display_names={"sma": "SMA(20)", "rsi": "RSI(14)"},
            placements={"sma": "overlay", "rsi": "subplot"},
        )

        # Verify indicators stored
        indicators = context.get_tracked_indicators()
        assert "SMA(20)" in indicators
        assert "RSI(14)" in indicators

        # Verify placements stored
        assert hasattr(context, "_indicator_placements")
        assert context._indicator_placements["SMA(20)"] == "overlay"
        assert context._indicator_placements["RSI(14)"] == "subplot"

    def test_track_indicators_with_colors(self):
        """Test tracking indicators with color metadata."""
        # Setup
        event_bus = MagicMock()
        context = Context(strategy_id="test_strategy", event_bus=event_bus)

        # Track indicators with colors
        context.track_indicators(
            indicators={"sma": 150.0, "rsi": 65.0},
            display_names={"sma": "SMA(20)", "rsi": "RSI(14)"},
            colors={"sma": "#667eea", "rsi": "#fa709a"},
        )

        # Verify colors stored
        assert hasattr(context, "_indicator_colors")
        assert context._indicator_colors["SMA(20)"] == "#667eea"
        assert context._indicator_colors["RSI(14)"] == "#fa709a"

    def test_track_indicators_with_all_metadata(self):
        """Test tracking indicators with complete metadata."""
        # Setup
        event_bus = MagicMock()
        context = Context(strategy_id="test_strategy", event_bus=event_bus)

        # Track indicators with all metadata
        context.track_indicators(
            indicators={"fast_sma": 152.0, "slow_sma": 148.0, "atr": 2.5, "rsi": 68.0},
            display_names={
                "fast_sma": "SMA(10)",
                "slow_sma": "SMA(50)",
                "atr": "ATR(14)",
                "rsi": "RSI(14)",
            },
            placements={
                "fast_sma": "overlay",
                "slow_sma": "overlay",
                "atr": "subplot",
                "rsi": "subplot",
            },
            colors={
                "fast_sma": "#667eea",
                "slow_sma": "#764ba2",
                "atr": "#f093fb",
                "rsi": "#fa709a",
            },
        )

        # Verify all data stored
        indicators = context.get_tracked_indicators()
        assert len(indicators) == 4

        assert context._indicator_placements["SMA(10)"] == "overlay"
        assert context._indicator_placements["SMA(50)"] == "overlay"
        assert context._indicator_placements["ATR(14)"] == "subplot"
        assert context._indicator_placements["RSI(14)"] == "subplot"

        assert context._indicator_colors["SMA(10)"] == "#667eea"
        assert context._indicator_colors["SMA(50)"] == "#764ba2"
        assert context._indicator_colors["ATR(14)"] == "#f093fb"
        assert context._indicator_colors["RSI(14)"] == "#fa709a"

    def test_track_indicators_without_metadata_backward_compatible(self):
        """Test that track_indicators() works without metadata (backward compatible)."""
        # Setup
        event_bus = MagicMock()
        context = Context(strategy_id="test_strategy", event_bus=event_bus)

        # Track indicators without metadata (old API)
        context.track_indicators(
            indicators={"sma": 150.0, "rsi": 65.0},
            display_names={"sma": "SMA(20)", "rsi": "RSI(14)"},
        )

        # Verify indicators stored
        indicators = context.get_tracked_indicators()
        assert "SMA(20)" in indicators
        assert "RSI(14)" in indicators

        # Metadata attributes should not exist if not provided
        # (or exist but be empty)
        if hasattr(context, "_indicator_placements"):
            assert len(context._indicator_placements) == 0
        if hasattr(context, "_indicator_colors"):
            assert len(context._indicator_colors) == 0

    def test_clear_tracked_indicators_clears_metadata(self):
        """Test that clear_tracked_indicators() clears visualization metadata."""
        # Setup
        event_bus = MagicMock()
        context = Context(strategy_id="test_strategy", event_bus=event_bus)

        # Track indicators with metadata
        context.track_indicators(
            indicators={"sma": 150.0, "rsi": 65.0},
            placements={"sma": "overlay", "rsi": "subplot"},
            colors={"sma": "#667eea", "rsi": "#fa709a"},
        )

        # Verify data stored
        assert len(context.get_tracked_indicators()) > 0
        assert hasattr(context, "_indicator_placements")
        assert hasattr(context, "_indicator_colors")
        assert len(context._indicator_placements) > 0
        assert len(context._indicator_colors) > 0

        # Clear
        context.clear_tracked_indicators()

        # Verify all cleared
        assert len(context.get_tracked_indicators()) == 0
        assert len(context._indicator_placements) == 0
        assert len(context._indicator_colors) == 0

    def test_emit_indicator_event_includes_metadata(self):
        """Test that emit_indicator_event() includes visualization metadata."""
        # Setup
        event_bus = MagicMock()
        context = Context(strategy_id="test_strategy", event_bus=event_bus)

        # Track indicators with metadata
        context.track_indicators(
            indicators={"sma": 150.0},
            placements={"sma": "overlay"},
            colors={"sma": "#667eea"},
        )

        # Emit indicator event
        event = context.emit_indicator_event(
            symbol="AAPL",
            timestamp="2024-01-01T00:00:00Z",
            indicators={"sma": 150.0},
        )

        # Verify metadata included in event
        assert event.metadata is not None
        assert "placements" in event.metadata
        assert "colors" in event.metadata
        assert event.metadata["placements"]["sma"] == "overlay"
        assert event.metadata["colors"]["sma"] == "#667eea"

    def test_multiple_track_calls_accumulate_metadata(self):
        """Test that multiple track_indicators() calls accumulate metadata."""
        # Setup
        event_bus = MagicMock()
        context = Context(strategy_id="test_strategy", event_bus=event_bus)

        # First call
        context.track_indicators(
            indicators={"sma": 150.0},
            placements={"sma": "overlay"},
            colors={"sma": "#667eea"},
        )

        # Second call
        context.track_indicators(
            indicators={"rsi": 65.0},
            placements={"rsi": "subplot"},
            colors={"rsi": "#fa709a"},
        )

        # Verify both sets of metadata stored
        indicators = context.get_tracked_indicators()
        assert len(indicators) == 2

        assert context._indicator_placements["sma"] == "overlay"
        assert context._indicator_placements["rsi"] == "subplot"

        assert context._indicator_colors["sma"] == "#667eea"
        assert context._indicator_colors["rsi"] == "#fa709a"

    def test_track_indicators_respects_display_names_for_metadata(self):
        """Test that placements and colors use display names as keys."""
        # Setup
        event_bus = MagicMock()
        context = Context(strategy_id="test_strategy", event_bus=event_bus)

        # Track with display names
        context.track_indicators(
            indicators={"fast": 152.0, "slow": 148.0},
            display_names={"fast": "SMA(10)", "slow": "SMA(50)"},
            placements={"fast": "overlay", "slow": "overlay"},
            colors={"fast": "#667eea", "slow": "#764ba2"},
        )

        # Verify metadata uses display names
        assert "SMA(10)" in context._indicator_placements
        assert "SMA(50)" in context._indicator_placements
        assert "SMA(10)" in context._indicator_colors
        assert "SMA(50)" in context._indicator_colors

        # Original keys should not be in metadata
        assert "fast" not in context._indicator_placements
        assert "slow" not in context._indicator_placements
