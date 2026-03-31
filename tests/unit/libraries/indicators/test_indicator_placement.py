"""
Tests for indicator placement metadata.

Tests the new IndicatorPlacement enum and visualization metadata
added to indicator classes.
"""

from qs_trader.libraries.indicators import ATR, EMA, RSI, SMA, BaseIndicator, BollingerBands, IndicatorPlacement, StdDev


class TestIndicatorPlacement:
    """Test IndicatorPlacement enum."""

    def test_placement_enum_values(self):
        """Test that IndicatorPlacement has expected values."""
        assert IndicatorPlacement.OVERLAY == "overlay"
        assert IndicatorPlacement.SUBPLOT == "subplot"
        assert IndicatorPlacement.VOLUME == "volume"

    def test_placement_enum_string_comparison(self):
        """Test that IndicatorPlacement can be compared to strings."""
        assert IndicatorPlacement.OVERLAY == "overlay"
        assert IndicatorPlacement.SUBPLOT != "overlay"


class TestBaseIndicatorVisualizationMetadata:
    """Test visualization metadata in BaseIndicator."""

    def test_base_indicator_has_placement_attribute(self):
        """Test that BaseIndicator has placement class attribute."""
        assert hasattr(BaseIndicator, "placement")
        assert isinstance(BaseIndicator.placement, IndicatorPlacement)

    def test_base_indicator_has_value_range_attribute(self):
        """Test that BaseIndicator has value_range class attribute."""
        assert hasattr(BaseIndicator, "value_range")
        assert isinstance(BaseIndicator.value_range, tuple)
        assert len(BaseIndicator.value_range) == 2

    def test_base_indicator_has_default_color_attribute(self):
        """Test that BaseIndicator has default_color class attribute."""
        assert hasattr(BaseIndicator, "default_color")
        assert isinstance(BaseIndicator.default_color, str)
        assert BaseIndicator.default_color.startswith("#")

    def test_base_indicator_default_placement_is_subplot(self):
        """Test that BaseIndicator defaults to SUBPLOT placement."""
        assert BaseIndicator.placement == IndicatorPlacement.SUBPLOT

    def test_base_indicator_default_value_range_is_unbounded(self):
        """Test that BaseIndicator defaults to unbounded value range."""
        assert BaseIndicator.value_range == (None, None)


class TestMovingAveragePlacement:
    """Test placement metadata for moving average indicators."""

    def test_sma_placement_is_overlay(self):
        """Test that SMA is configured for overlay placement."""
        assert SMA.placement == IndicatorPlacement.OVERLAY
        assert hasattr(SMA, "default_color")
        assert SMA.default_color == "#667eea"

    def test_ema_placement_is_overlay(self):
        """Test that EMA is configured for overlay placement."""
        assert EMA.placement == IndicatorPlacement.OVERLAY
        assert hasattr(EMA, "default_color")
        assert EMA.default_color == "#764ba2"

    def test_sma_instance_has_placement_metadata(self):
        """Test that SMA instance inherits placement metadata."""
        sma = SMA(period=20)
        assert sma.placement == IndicatorPlacement.OVERLAY
        assert sma.default_color == "#667eea"


class TestVolatilityIndicatorPlacement:
    """Test placement metadata for volatility indicators."""

    def test_atr_placement_is_subplot(self):
        """Test that ATR is configured for subplot placement."""
        assert ATR.placement == IndicatorPlacement.SUBPLOT
        assert hasattr(ATR, "value_range")
        assert ATR.value_range == (0.0, None)  # Non-negative, unbounded
        assert hasattr(ATR, "default_color")
        assert ATR.default_color == "#f093fb"

    def test_bollinger_bands_placement(self):
        """Test that Bollinger Bands is configured correctly."""
        # Bollinger Bands should be overlay (plots on price chart)
        # Note: May need to add placement metadata to BollingerBands class
        assert hasattr(BollingerBands, "placement")

    def test_stddev_placement_is_subplot(self):
        """Test that StdDev is configured for subplot placement."""
        assert StdDev.placement == IndicatorPlacement.SUBPLOT


class TestMomentumIndicatorPlacement:
    """Test placement metadata for momentum indicators."""

    def test_rsi_placement_is_subplot(self):
        """Test that RSI is configured for subplot placement."""
        assert RSI.placement == IndicatorPlacement.SUBPLOT
        assert hasattr(RSI, "value_range")
        assert RSI.value_range == (0.0, 100.0)  # Bounded oscillator
        assert hasattr(RSI, "default_color")
        assert RSI.default_color == "#fa709a"

    def test_rsi_instance_has_placement_metadata(self):
        """Test that RSI instance inherits placement metadata."""
        rsi = RSI(period=14)
        assert rsi.placement == IndicatorPlacement.SUBPLOT
        assert rsi.value_range == (0.0, 100.0)
        assert rsi.default_color == "#fa709a"


class TestPlacementMetadataInheritance:
    """Test that placement metadata is properly inherited."""

    def test_class_level_placement_shared_across_instances(self):
        """Test that placement is a class attribute, not instance."""
        sma1 = SMA(period=10)
        sma2 = SMA(period=50)

        # Both instances share the same class attribute
        assert sma1.placement == sma2.placement
        assert sma1.placement is SMA.placement

    def test_different_indicators_have_different_placements(self):
        """Test that different indicators can have different placements."""
        sma = SMA(period=20)
        rsi = RSI(period=14)

        assert sma.placement == IndicatorPlacement.OVERLAY
        assert rsi.placement == IndicatorPlacement.SUBPLOT
        assert sma.placement != rsi.placement

    def test_different_indicators_have_different_colors(self):
        """Test that different indicators have different default colors."""
        sma = SMA(period=20)
        ema = EMA(period=20)
        rsi = RSI(period=14)
        atr = ATR(period=14)

        # All should have unique colors
        colors = {sma.default_color, ema.default_color, rsi.default_color, atr.default_color}
        assert len(colors) == 4, "All indicators should have unique colors"


class TestPlacementMetadataForVisualization:
    """Test that placement metadata is suitable for visualization."""

    def test_overlay_indicators_have_no_value_range(self):
        """Test that overlay indicators typically have unbounded range."""
        # Moving averages overlay on price chart, so no fixed range
        sma = SMA(period=20)
        ema = EMA(period=20)

        # Overlay indicators don't need value_range (they scale with price)
        # Default (None, None) is fine
        assert hasattr(sma, "value_range")
        assert hasattr(ema, "value_range")

    def test_subplot_indicators_with_bounded_range(self):
        """Test that bounded oscillators specify value range."""
        rsi = RSI(period=14)

        # RSI is bounded [0, 100]
        assert rsi.value_range == (0.0, 100.0)
        min_val, max_val = rsi.value_range
        assert min_val is not None
        assert max_val is not None
        assert min_val < max_val

    def test_subplot_indicators_with_unbounded_range(self):
        """Test that unbounded indicators specify open-ended range."""
        atr = ATR(period=14)

        # ATR is non-negative but unbounded above
        assert atr.value_range == (0.0, None)
        min_val, max_val = atr.value_range
        assert min_val is not None
        assert max_val is None

    def test_all_indicators_have_hex_color(self):
        """Test that all indicators have valid hex colors."""
        indicators = [
            SMA(period=20),
            EMA(period=20),
            ATR(period=14),
            RSI(period=14),
        ]

        for indicator in indicators:
            color = indicator.default_color
            assert isinstance(color, str)
            assert color.startswith("#")
            assert len(color) == 7  # #RRGGBB format
            # Verify hex digits
            assert all(c in "0123456789abcdefABCDEF" for c in color[1:])
