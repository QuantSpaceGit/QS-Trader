"""Unit tests for risk sizing tools.

Tests pure functions in libraries/risk/tools/sizing.py following pytest best practices.

Coverage focus:
- calculate_fixed_fraction_size: Core sizing logic, edge cases, validation
- calculate_equal_weight_size: Equal weight allocation, validation
- validate_sizing_inputs: Input validation for both models
"""

from decimal import Decimal

import pytest

from qs_trader.libraries.risk.tools.sizing import (
    calculate_equal_weight_size,
    calculate_fixed_fraction_size,
    validate_sizing_inputs,
)


class TestCalculateFixedFractionSize:
    """Test suite for calculate_fixed_fraction_size function."""

    def test_full_strength_signal_returns_correct_size(self):
        """Test full strength (1.0) signal uses full fraction of capital."""
        # Arrange
        allocated_capital = Decimal("100000")  # $100k
        signal_strength = 1.0
        current_price = Decimal("50")
        fraction = Decimal("0.10")  # 10% sizing

        # Act
        size = calculate_fixed_fraction_size(
            allocated_capital=allocated_capital,
            signal_strength=signal_strength,
            current_price=current_price,
            fraction=fraction,
        )

        # Assert
        # 1.0 * 0.10 * 100000 = $10,000 / $50 = 200 shares
        assert size == 200
        assert isinstance(size, int)

    def test_half_strength_signal_returns_half_size(self):
        """Test half strength (0.5) signal uses half the fraction."""
        size = calculate_fixed_fraction_size(
            allocated_capital=Decimal("100000"),
            signal_strength=0.5,
            current_price=Decimal("50"),
            fraction=Decimal("0.10"),
        )
        # 0.5 * 0.10 * 100000 = $5,000 / $50 = 100 shares
        assert size == 100

    def test_zero_signal_strength_returns_zero(self):
        """Test zero signal strength returns zero size."""
        size = calculate_fixed_fraction_size(
            allocated_capital=Decimal("100000"),
            signal_strength=0.0,
            current_price=Decimal("50"),
            fraction=Decimal("0.10"),
        )
        assert size == 0

    def test_negative_signal_uses_absolute_value(self):
        """Test negative signal (short) uses absolute value for sizing."""
        size = calculate_fixed_fraction_size(
            allocated_capital=Decimal("100000"),
            signal_strength=-0.8,
            current_price=Decimal("50"),
            fraction=Decimal("0.10"),
        )
        # |-0.8| = 0.8, so 0.8 * 0.10 * 100000 = $8,000 / $50 = 160 shares
        assert size == 160

    def test_min_quantity_enforced_returns_zero_when_below(self):
        """Test returns 0 when calculated quantity below min_quantity."""
        size = calculate_fixed_fraction_size(
            allocated_capital=Decimal("100000"),
            signal_strength=1.0,
            current_price=Decimal("1000"),  # Expensive stock
            fraction=Decimal("0.01"),  # Small fraction
            min_quantity=10,
        )
        # 1.0 * 0.01 * 100000 = $1,000 / $1,000 = 1 share
        # 1 < min_quantity(10), so returns 0
        assert size == 0

    def test_lot_size_rounding_rounds_to_nearest_lot(self):
        """Test lot size correctly rounds to nearest multiple."""
        size = calculate_fixed_fraction_size(
            allocated_capital=Decimal("100000"),
            signal_strength=1.0,
            current_price=Decimal("45"),
            fraction=Decimal("0.10"),
            lot_size=100,
        )
        # 1.0 * 0.10 * 100000 = $10,000 / $45 = 222.22 shares
        # floor(222.22 / 100) * 100 = 200 shares
        assert size == 200

    def test_fractional_shares_rounded_down_to_int(self):
        """Test fractional shares are rounded down to nearest integer."""
        size = calculate_fixed_fraction_size(
            allocated_capital=Decimal("100000"),
            signal_strength=1.0,
            current_price=Decimal("47"),
            fraction=Decimal("0.10"),
        )
        # 1.0 * 0.10 * 100000 = $10,000 / $47 = 212.76... shares
        # floor(212.76) = 212 shares
        assert size == 212

    def test_zero_price_returns_zero(self):
        """Test zero price returns zero size (edge case)."""
        size = calculate_fixed_fraction_size(
            allocated_capital=Decimal("100000"),
            signal_strength=1.0,
            current_price=Decimal("0"),
            fraction=Decimal("0.10"),
        )
        assert size == 0

    def test_negative_allocated_capital_raises_value_error(self):
        """Test negative allocated capital raises ValueError."""
        with pytest.raises(ValueError, match="allocated_capital must be non-negative"):
            calculate_fixed_fraction_size(
                allocated_capital=Decimal("-100000"),
                signal_strength=1.0,
                current_price=Decimal("50"),
                fraction=Decimal("0.10"),
            )

    def test_negative_price_raises_value_error(self):
        """Test negative price raises ValueError."""
        with pytest.raises(ValueError, match="current_price must be non-negative"):
            calculate_fixed_fraction_size(
                allocated_capital=Decimal("100000"),
                signal_strength=1.0,
                current_price=Decimal("-50"),
                fraction=Decimal("0.10"),
            )

    def test_fraction_above_1_raises_value_error(self):
        """Test fraction > 1.0 raises ValueError."""
        with pytest.raises(ValueError, match="fraction must be in"):
            calculate_fixed_fraction_size(
                allocated_capital=Decimal("100000"),
                signal_strength=1.0,
                current_price=Decimal("50"),
                fraction=Decimal("1.5"),
            )

    def test_fraction_below_0_raises_value_error(self):
        """Test fraction < 0 raises ValueError."""
        with pytest.raises(ValueError, match="fraction must be in"):
            calculate_fixed_fraction_size(
                allocated_capital=Decimal("100000"),
                signal_strength=1.0,
                current_price=Decimal("50"),
                fraction=Decimal("-0.1"),
            )

    def test_lot_size_zero_raises_value_error(self):
        """Test lot_size = 0 raises ValueError."""
        with pytest.raises(ValueError, match="lot_size must be positive"):
            calculate_fixed_fraction_size(
                allocated_capital=Decimal("100000"),
                signal_strength=1.0,
                current_price=Decimal("50"),
                fraction=Decimal("0.10"),
                lot_size=0,
            )

    def test_negative_min_quantity_raises_value_error(self):
        """Test negative min_quantity raises ValueError."""
        with pytest.raises(ValueError, match="min_quantity must be non-negative"):
            calculate_fixed_fraction_size(
                allocated_capital=Decimal("100000"),
                signal_strength=1.0,
                current_price=Decimal("50"),
                fraction=Decimal("0.10"),
                min_quantity=-5,
            )


class TestCalculateEqualWeightSize:
    """Test suite for calculate_equal_weight_size function."""

    def test_single_position_uses_full_capital(self):
        """Test with 1 position uses all allocated capital."""
        size = calculate_equal_weight_size(
            allocated_capital=Decimal("100000"),
            num_positions=1,
            current_price=Decimal("50"),
        )
        # $100,000 / 1 = $100,000 / $50 = 2000 shares
        assert size == 2000

    def test_five_positions_equal_allocation(self):
        """Test 5 positions get equal 1/5 allocation each."""
        size = calculate_equal_weight_size(
            allocated_capital=Decimal("100000"),
            num_positions=5,
            current_price=Decimal("100"),
        )
        # $100,000 / 5 = $20,000 / $100 = 200 shares
        assert size == 200

    def test_fractional_shares_rounded_down(self):
        """Test fractional shares are rounded down to integer."""
        size = calculate_equal_weight_size(
            allocated_capital=Decimal("100000"),
            num_positions=7,
            current_price=Decimal("33"),
        )
        # $100,000 / 7 = $14,285.71 / $33 = 432.89... shares
        # floor(432.89) = 432 shares
        assert size == 432

    def test_expensive_stock_many_positions_small_size(self):
        """Test expensive stock with many positions results in small size."""
        size = calculate_equal_weight_size(
            allocated_capital=Decimal("100000"),
            num_positions=100,
            current_price=Decimal("500"),
        )
        # $100,000 / 100 = $1,000 / $500 = 2 shares
        assert size == 2

    def test_min_quantity_enforced_returns_zero_when_below(self):
        """Test returns 0 when calculated quantity below min_quantity."""
        size = calculate_equal_weight_size(
            allocated_capital=Decimal("100000"),
            num_positions=100,
            current_price=Decimal("2000"),  # Very expensive
            min_quantity=5,
        )
        # $100,000 / 100 = $1,000 / $2,000 = 0.5 shares
        # 0 < min_quantity(5), so returns 0
        assert size == 0

    def test_lot_size_rounding_applied_correctly(self):
        """Test lot size rounding works correctly."""
        size = calculate_equal_weight_size(
            allocated_capital=Decimal("100000"),
            num_positions=3,
            current_price=Decimal("40"),
            lot_size=100,
        )
        # $100,000 / 3 = $33,333.33 / $40 = 833.33 shares
        # floor(833.33 / 100) * 100 = 800 shares
        assert size == 800

    def test_zero_price_returns_zero(self):
        """Test zero price returns zero size (edge case)."""
        size = calculate_equal_weight_size(
            allocated_capital=Decimal("100000"),
            num_positions=5,
            current_price=Decimal("0"),
        )
        assert size == 0

    def test_zero_positions_raises_value_error(self):
        """Test num_positions = 0 raises ValueError."""
        with pytest.raises(ValueError, match="num_positions must be positive"):
            calculate_equal_weight_size(
                allocated_capital=Decimal("100000"),
                num_positions=0,
                current_price=Decimal("50"),
            )

    def test_negative_positions_raises_value_error(self):
        """Test negative num_positions raises ValueError."""
        with pytest.raises(ValueError, match="num_positions must be positive"):
            calculate_equal_weight_size(
                allocated_capital=Decimal("100000"),
                num_positions=-5,
                current_price=Decimal("50"),
            )

    def test_negative_allocated_capital_raises_value_error(self):
        """Test negative allocated capital raises ValueError."""
        with pytest.raises(ValueError, match="allocated_capital must be non-negative"):
            calculate_equal_weight_size(
                allocated_capital=Decimal("-100000"),
                num_positions=5,
                current_price=Decimal("50"),
            )

    def test_negative_price_raises_value_error(self):
        """Test negative price raises ValueError."""
        with pytest.raises(ValueError, match="current_price must be non-negative"):
            calculate_equal_weight_size(
                allocated_capital=Decimal("100000"),
                num_positions=5,
                current_price=Decimal("-50"),
            )


class TestValidateSizingInputs:
    """Test suite for validate_sizing_inputs function."""

    def test_valid_fixed_fraction_inputs_no_error(self):
        """Test valid fixed_fraction inputs do not raise error."""
        # Should not raise
        validate_sizing_inputs(
            model="fixed_fraction",
            allocated_capital=Decimal("100000"),
            current_price=Decimal("50"),
            fraction=Decimal("0.10"),
        )

    def test_valid_equal_weight_inputs_no_error(self):
        """Test valid equal_weight inputs do not raise error."""
        # Should not raise
        validate_sizing_inputs(
            model="equal_weight",
            allocated_capital=Decimal("100000"),
            current_price=Decimal("50"),
            num_positions=5,
        )

    def test_invalid_model_raises_value_error(self):
        """Test invalid model raises ValueError."""
        with pytest.raises(ValueError, match="model must be"):
            validate_sizing_inputs(
                model="invalid_model",  # pyright: ignore[reportArgumentType]
                allocated_capital=Decimal("100000"),
                current_price=Decimal("50"),
            )

    def test_negative_allocated_capital_raises_value_error(self):
        """Test negative allocated capital raises ValueError."""
        with pytest.raises(ValueError, match="allocated_capital must be non-negative"):
            validate_sizing_inputs(
                model="fixed_fraction",
                allocated_capital=Decimal("-100000"),
                current_price=Decimal("50"),
                fraction=Decimal("0.10"),
            )

    def test_negative_price_raises_value_error(self):
        """Test negative price raises ValueError."""
        with pytest.raises(ValueError, match="current_price must be non-negative"):
            validate_sizing_inputs(
                model="fixed_fraction",
                allocated_capital=Decimal("100000"),
                current_price=Decimal("-50"),
                fraction=Decimal("0.10"),
            )

    def test_fixed_fraction_missing_fraction_raises_value_error(self):
        """Test fixed_fraction without fraction raises ValueError."""
        with pytest.raises(ValueError, match="fraction is required"):
            validate_sizing_inputs(
                model="fixed_fraction",
                allocated_capital=Decimal("100000"),
                current_price=Decimal("50"),
                fraction=None,
            )

    def test_equal_weight_missing_num_positions_raises_value_error(self):
        """Test equal_weight without num_positions raises ValueError."""
        with pytest.raises(ValueError, match="num_positions is required"):
            validate_sizing_inputs(
                model="equal_weight",
                allocated_capital=Decimal("100000"),
                current_price=Decimal("50"),
                num_positions=None,
            )

    def test_fraction_below_0_raises_value_error(self):
        """Test fraction < 0 raises ValueError."""
        with pytest.raises(ValueError, match="fraction must be in"):
            validate_sizing_inputs(
                model="fixed_fraction",
                allocated_capital=Decimal("100000"),
                current_price=Decimal("50"),
                fraction=Decimal("-0.1"),
            )

    def test_fraction_above_1_raises_value_error(self):
        """Test fraction > 1 raises ValueError."""
        with pytest.raises(ValueError, match="fraction must be in"):
            validate_sizing_inputs(
                model="fixed_fraction",
                allocated_capital=Decimal("100000"),
                current_price=Decimal("50"),
                fraction=Decimal("1.5"),
            )

    def test_zero_num_positions_raises_value_error(self):
        """Test num_positions = 0 raises ValueError."""
        with pytest.raises(ValueError, match="num_positions must be positive"):
            validate_sizing_inputs(
                model="equal_weight",
                allocated_capital=Decimal("100000"),
                current_price=Decimal("50"),
                num_positions=0,
            )
