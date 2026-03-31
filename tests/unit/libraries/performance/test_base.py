"""Unit tests for performance base module.

Tests cover:
- BacktestResult placeholder class structure
- BaseMetric abstract base class
- Default implementations of optional methods
- Abstract method enforcement
- Metric properties and formatting
"""

from datetime import datetime, timezone
from typing import Any

import pytest

from qs_trader.libraries.performance.base import BacktestResult, BaseMetric


class TestBacktestResult:
    """Test BacktestResult placeholder class."""

    def test_backtest_result_has_required_attributes(self):
        """Test that BacktestResult defines expected attributes."""
        # Verify class has the expected type hints
        assert hasattr(BacktestResult, "__annotations__")
        annotations = BacktestResult.__annotations__

        # Check all required attributes are defined
        assert "equity_curve" in annotations
        assert "trades" in annotations
        assert "returns" in annotations
        assert "initial_equity" in annotations
        assert "final_equity" in annotations
        assert "start_date" in annotations
        assert "end_date" in annotations


class ConcreteMetric(BaseMetric):
    """Concrete implementation of BaseMetric for testing."""

    def __init__(self, **params: Any):
        """Initialize with optional parameters."""
        self.params = params
        self.risk_free_rate = params.get("risk_free_rate", 0.02)

    def compute(self, results: BacktestResult) -> float:
        """Simple computation for testing."""
        if results.final_equity == 0:
            return 0.0
        return (results.final_equity / results.initial_equity - 1.0) * 100

    @property
    def name(self) -> str:
        """Return metric name."""
        return "test_metric"

    @property
    def display_name(self) -> str:
        """Return display name."""
        return "Test Metric"


class TestBaseMetric:
    """Test BaseMetric abstract base class."""

    def test_cannot_instantiate_base_metric_directly(self):
        """Test that BaseMetric cannot be instantiated directly."""
        with pytest.raises(TypeError, match="Can't instantiate abstract class"):
            BaseMetric()

    def test_concrete_metric_can_be_instantiated(self):
        """Test that concrete implementation can be instantiated."""
        metric = ConcreteMetric()

        assert metric is not None
        assert isinstance(metric, BaseMetric)

    def test_concrete_metric_initialization_with_params(self):
        """Test metric initialization with parameters."""
        metric = ConcreteMetric(risk_free_rate=0.03, window=252)

        assert metric.risk_free_rate == 0.03
        assert metric.params["window"] == 252

    def test_concrete_metric_initialization_without_params(self):
        """Test metric initialization with default parameters."""
        metric = ConcreteMetric()

        assert metric.risk_free_rate == 0.02
        assert metric.params == {}

    def test_metric_name_property(self):
        """Test that name property returns correct value."""
        metric = ConcreteMetric()

        assert metric.name == "test_metric"

    def test_metric_display_name_property(self):
        """Test that display_name property returns correct value."""
        metric = ConcreteMetric()

        assert metric.display_name == "Test Metric"

    def test_metric_category_default_value(self):
        """Test that category has default value of 'other'."""
        metric = ConcreteMetric()

        assert metric.category == "other"

    def test_metric_format_spec_default_value(self):
        """Test that format_spec has default value."""
        metric = ConcreteMetric()

        assert metric.format_spec == ".4f"

    def test_metric_interpretation_default_returns_empty_string(self):
        """Test that default interpretation returns empty string."""
        metric = ConcreteMetric()

        result = metric.interpretation(1.5)

        assert result == ""

    def test_metric_format_value_uses_format_spec(self):
        """Test that format_value uses format_spec property."""
        metric = ConcreteMetric()

        result = metric.format_value(1.23456789)

        assert result == "1.2346"

    def test_metric_format_value_with_custom_format_spec(self):
        """Test format_value with custom format specification."""

        class CustomFormatMetric(ConcreteMetric):
            @property
            def format_spec(self) -> str:
                return ".2%"

        metric = CustomFormatMetric()
        result = metric.format_value(0.1234)

        assert result == "12.34%"

    def test_metric_compute_with_mock_results(self):
        """Test compute method with mock backtest results."""

        class MockBacktestResult:
            def __init__(self):
                self.initial_equity = 100000.0
                self.final_equity = 125000.0
                self.equity_curve = []
                self.trades = []
                self.returns = []
                self.start_date = datetime(2024, 1, 1, tzinfo=timezone.utc)
                self.end_date = datetime(2024, 12, 31, tzinfo=timezone.utc)

        results = MockBacktestResult()
        metric = ConcreteMetric()

        value = metric.compute(results)

        # (125000 / 100000 - 1) * 100 = 25.0
        assert value == 25.0

    def test_metric_compute_with_zero_final_equity(self):
        """Test compute handles zero final equity."""

        class MockBacktestResult:
            def __init__(self):
                self.initial_equity = 100000.0
                self.final_equity = 0.0

        results = MockBacktestResult()
        metric = ConcreteMetric()

        value = metric.compute(results)

        assert value == 0.0

    def test_metric_compute_with_loss(self):
        """Test compute with losing backtest."""

        class MockBacktestResult:
            def __init__(self):
                self.initial_equity = 100000.0
                self.final_equity = 85000.0

        results = MockBacktestResult()
        metric = ConcreteMetric()

        value = metric.compute(results)

        # (85000 / 100000 - 1) * 100 = -15.0
        assert value == pytest.approx(-15.0)


class TestBaseMetricCategoryOverride:
    """Test category property override."""

    def test_metric_category_can_be_overridden(self):
        """Test that category property can be overridden."""

        class ReturnMetric(ConcreteMetric):
            @property
            def category(self) -> str:
                return "return"

        metric = ReturnMetric()

        assert metric.category == "return"

    def test_metric_category_risk(self):
        """Test risk category override."""

        class RiskMetric(ConcreteMetric):
            @property
            def category(self) -> str:
                return "risk"

        metric = RiskMetric()

        assert metric.category == "risk"

    def test_metric_category_risk_adjusted(self):
        """Test risk_adjusted category override."""

        class RiskAdjustedMetric(ConcreteMetric):
            @property
            def category(self) -> str:
                return "risk_adjusted"

        metric = RiskAdjustedMetric()

        assert metric.category == "risk_adjusted"

    def test_metric_category_trade(self):
        """Test trade category override."""

        class TradeMetric(ConcreteMetric):
            @property
            def category(self) -> str:
                return "trade"

        metric = TradeMetric()

        assert metric.category == "trade"


class TestBaseMetricFormatSpecOverride:
    """Test format_spec property override."""

    def test_format_spec_percentage(self):
        """Test percentage format specification."""

        class PercentageMetric(ConcreteMetric):
            @property
            def format_spec(self) -> str:
                return ".2%"

        metric = PercentageMetric()

        assert metric.format_spec == ".2%"
        assert metric.format_value(0.1234) == "12.34%"

    def test_format_spec_integer_with_comma(self):
        """Test integer format with thousands separator."""

        class IntegerMetric(ConcreteMetric):
            @property
            def format_spec(self) -> str:
                return ",.0f"

        metric = IntegerMetric()

        assert metric.format_spec == ",.0f"
        assert metric.format_value(1234567.89) == "1,234,568"

    def test_format_spec_two_decimals(self):
        """Test two decimal places format."""

        class TwoDecimalMetric(ConcreteMetric):
            @property
            def format_spec(self) -> str:
                return ".2f"

        metric = TwoDecimalMetric()

        assert metric.format_spec == ".2f"
        assert metric.format_value(1.23456) == "1.23"


class TestBaseMetricInterpretationOverride:
    """Test interpretation method override."""

    def test_interpretation_can_be_overridden(self):
        """Test that interpretation method can be overridden."""

        class InterpretedMetric(ConcreteMetric):
            def interpretation(self, value: float) -> str:
                if value > 2.0:
                    return "Excellent"
                elif value > 1.0:
                    return "Good"
                elif value > 0:
                    return "Acceptable"
                else:
                    return "Poor"

        metric = InterpretedMetric()

        assert metric.interpretation(2.5) == "Excellent"
        assert metric.interpretation(1.5) == "Good"
        assert metric.interpretation(0.5) == "Acceptable"
        assert metric.interpretation(-0.5) == "Poor"

    def test_interpretation_sharpe_ratio_style(self):
        """Test interpretation similar to Sharpe ratio example."""

        class SharpeStyleMetric(ConcreteMetric):
            def interpretation(self, value: float) -> str:
                if value > 3:
                    return "Exceptional"
                elif value > 2:
                    return "Very Good"
                elif value > 1:
                    return "Good"
                elif value > 0:
                    return "Acceptable"
                else:
                    return "Poor"

        metric = SharpeStyleMetric()

        assert metric.interpretation(3.5) == "Exceptional"
        assert metric.interpretation(2.5) == "Very Good"
        assert metric.interpretation(1.5) == "Good"
        assert metric.interpretation(0.5) == "Acceptable"
        assert metric.interpretation(-0.5) == "Poor"


class TestBaseMetricAbstractMethodEnforcement:
    """Test that abstract methods must be implemented."""

    def test_missing_init_raises_error(self):
        """Test that missing __init__ raises TypeError."""
        with pytest.raises(TypeError):

            class MissingInitMetric(BaseMetric):
                def compute(self, results: BacktestResult) -> float:
                    return 0.0

                @property
                def name(self) -> str:
                    return "test"

                @property
                def display_name(self) -> str:
                    return "Test"

            MissingInitMetric()

    def test_missing_compute_raises_error(self):
        """Test that missing compute raises TypeError."""
        with pytest.raises(TypeError):

            class MissingComputeMetric(BaseMetric):
                def __init__(self, **params: Any):
                    pass

                @property
                def name(self) -> str:
                    return "test"

                @property
                def display_name(self) -> str:
                    return "Test"

            MissingComputeMetric()

    def test_missing_name_raises_error(self):
        """Test that missing name property raises TypeError."""
        with pytest.raises(TypeError):

            class MissingNameMetric(BaseMetric):
                def __init__(self, **params: Any):
                    pass

                def compute(self, results: BacktestResult) -> float:
                    return 0.0

                @property
                def display_name(self) -> str:
                    return "Test"

            MissingNameMetric()

    def test_missing_display_name_raises_error(self):
        """Test that missing display_name property raises TypeError."""
        with pytest.raises(TypeError):

            class MissingDisplayNameMetric(BaseMetric):
                def __init__(self, **params: Any):
                    pass

                def compute(self, results: BacktestResult) -> float:
                    return 0.0

                @property
                def name(self) -> str:
                    return "test"

            MissingDisplayNameMetric()


class TestBaseMetricFormatValueEdgeCases:
    """Test format_value with various edge cases."""

    def test_format_value_with_zero(self):
        """Test formatting zero value."""
        metric = ConcreteMetric()

        result = metric.format_value(0.0)

        assert result == "0.0000"

    def test_format_value_with_negative(self):
        """Test formatting negative value."""
        metric = ConcreteMetric()

        result = metric.format_value(-123.456)

        assert result == "-123.4560"

    def test_format_value_with_very_large_number(self):
        """Test formatting very large number."""
        metric = ConcreteMetric()

        result = metric.format_value(1234567.89)

        assert result == "1234567.8900"

    def test_format_value_with_very_small_number(self):
        """Test formatting very small number."""
        metric = ConcreteMetric()

        result = metric.format_value(0.000123)

        assert result == "0.0001"


class TestBaseMetricStatelessness:
    """Test that metrics are stateless."""

    def test_metric_compute_is_deterministic(self):
        """Test that same input produces same output."""

        class MockBacktestResult:
            def __init__(self):
                self.initial_equity = 100000.0
                self.final_equity = 125000.0

        metric = ConcreteMetric()
        results = MockBacktestResult()

        # Call compute multiple times
        value1 = metric.compute(results)
        value2 = metric.compute(results)
        value3 = metric.compute(results)

        # All results should be identical
        assert value1 == value2 == value3 == 25.0

    def test_multiple_metrics_are_independent(self):
        """Test that multiple metric instances are independent."""
        metric1 = ConcreteMetric(risk_free_rate=0.02)
        metric2 = ConcreteMetric(risk_free_rate=0.05)

        assert metric1.risk_free_rate == 0.02
        assert metric2.risk_free_rate == 0.05

        # Modifying one should not affect the other
        metric1.risk_free_rate = 0.03

        assert metric1.risk_free_rate == 0.03
        assert metric2.risk_free_rate == 0.05
