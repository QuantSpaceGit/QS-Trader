"""Tests for commission calculator with advanced models."""

from decimal import Decimal

import pytest

from qs_trader.services.execution.commission import CommissionCalculator
from qs_trader.services.execution.config import CommissionConfig


class TestCommissionConfigValidation:
    """Test commission configuration validation."""

    def test_must_specify_at_least_one_model(self) -> None:
        """Test that at least one commission model must be specified."""
        with pytest.raises(ValueError, match="Must specify at least one commission model"):
            CommissionConfig()

    def test_cannot_specify_multiple_models_per_share_and_flat(self) -> None:
        """Test that cannot specify both per_share and flat_fee."""
        with pytest.raises(ValueError, match="Cannot specify multiple commission models"):
            CommissionConfig(per_share=Decimal("0.005"), flat_fee=Decimal("5.00"))

    def test_cannot_specify_multiple_models_per_share_and_percentage(self) -> None:
        """Test that cannot specify both per_share and percentage."""
        with pytest.raises(ValueError, match="Cannot specify multiple commission models"):
            CommissionConfig(per_share=Decimal("0.005"), percentage=Decimal("0.001"))

    def test_cannot_specify_multiple_models_per_share_and_tiers(self) -> None:
        """Test that cannot specify both per_share and tiers."""
        with pytest.raises(ValueError, match="Cannot specify multiple commission models"):
            CommissionConfig(
                per_share=Decimal("0.005"),
                tiers=[(Decimal("1000"), Decimal("0.01"))],
            )

    def test_cannot_specify_flat_and_percentage(self) -> None:
        """Test that cannot specify both flat_fee and percentage."""
        with pytest.raises(ValueError, match="Cannot specify both flat_fee and percentage"):
            CommissionConfig(flat_fee=Decimal("5.00"), percentage=Decimal("0.001"))

    def test_cannot_specify_all_models(self) -> None:
        """Test that cannot specify all commission models."""
        with pytest.raises(ValueError, match="Cannot specify multiple commission models"):
            CommissionConfig(
                per_share=Decimal("0.005"),
                flat_fee=Decimal("5.00"),
                tiers=[(Decimal("1000"), Decimal("0.01"))],
            )

    def test_tiers_cannot_be_empty(self) -> None:
        """Test that tiers cannot be empty list."""
        with pytest.raises(ValueError, match="Tiers cannot be empty"):
            CommissionConfig(tiers=[])

    def test_tier_max_qty_must_be_positive(self) -> None:
        """Test that tier max_qty must be positive."""
        with pytest.raises(ValueError, match="Tier max_qty must be positive"):
            CommissionConfig(tiers=[(Decimal("0"), Decimal("0.01"))])

    def test_tier_rate_cannot_be_negative(self) -> None:
        """Test that tier rate cannot be negative."""
        with pytest.raises(ValueError, match="Tier rate cannot be negative"):
            CommissionConfig(tiers=[(Decimal("1000"), Decimal("-0.01"))])

    def test_cap_cannot_be_negative(self) -> None:
        """Test that commission cap cannot be negative."""
        with pytest.raises(ValueError, match="Commission cap cannot be negative"):
            CommissionConfig(per_share=Decimal("0.005"), cap=Decimal("-10"))

    def test_minimum_cannot_be_negative(self) -> None:
        """Test that minimum commission cannot be negative."""
        with pytest.raises(ValueError, match="Minimum commission cannot be negative"):
            CommissionConfig(per_share=Decimal("0.005"), minimum=Decimal("-1"))


class TestPerShareCommission:
    """Test per-share commission model."""

    def test_basic_per_share_calculation(self) -> None:
        """Test basic per-share commission calculation."""
        config = CommissionConfig(per_share=Decimal("0.005"))
        calc = CommissionCalculator(config)

        commission = calc.calculate(Decimal("100"))
        assert commission == Decimal("0.50")

    def test_per_share_with_minimum(self) -> None:
        """Test per-share commission with minimum."""
        config = CommissionConfig(per_share=Decimal("0.005"), minimum=Decimal("1.00"))
        calc = CommissionCalculator(config)

        # Below minimum
        commission = calc.calculate(Decimal("100"))
        assert commission == Decimal("1.00")

        # Above minimum
        commission = calc.calculate(Decimal("500"))
        assert commission == Decimal("2.50")

    def test_per_share_with_cap(self) -> None:
        """Test per-share commission with cap."""
        config = CommissionConfig(per_share=Decimal("0.01"), cap=Decimal("50.00"))
        calc = CommissionCalculator(config)

        # Below cap
        commission = calc.calculate(Decimal("1000"))
        assert commission == Decimal("10.00")

        # Above cap
        commission = calc.calculate(Decimal("10000"))
        assert commission == Decimal("50.00")

    def test_per_share_with_minimum_and_cap(self) -> None:
        """Test per-share commission with both minimum and cap."""
        config = CommissionConfig(
            per_share=Decimal("0.01"),
            minimum=Decimal("1.00"),
            cap=Decimal("50.00"),
        )
        calc = CommissionCalculator(config)

        # Below minimum
        commission = calc.calculate(Decimal("50"))
        assert commission == Decimal("1.00")

        # Between minimum and cap
        commission = calc.calculate(Decimal("1000"))
        assert commission == Decimal("10.00")

        # Above cap
        commission = calc.calculate(Decimal("10000"))
        assert commission == Decimal("50.00")

    def test_per_share_zero_quantity(self) -> None:
        """Test per-share commission with zero quantity."""
        config = CommissionConfig(per_share=Decimal("0.005"), minimum=Decimal("1.00"))
        calc = CommissionCalculator(config)

        commission = calc.calculate(Decimal("0"))
        assert commission == Decimal("1.00")  # Returns minimum


class TestPerTradeFlatFeeCommission:
    """Test per-trade flat fee commission model."""

    def test_flat_fee_basic(self) -> None:
        """Test flat fee commission."""
        config = CommissionConfig(flat_fee=Decimal("5.00"))
        calc = CommissionCalculator(config)

        # Any quantity gets flat fee
        commission = calc.calculate(Decimal("100"))
        assert commission == Decimal("5.00")

        commission = calc.calculate(Decimal("1000"))
        assert commission == Decimal("5.00")

    def test_flat_fee_with_minimum(self) -> None:
        """Test flat fee with minimum."""
        config = CommissionConfig(flat_fee=Decimal("5.00"), minimum=Decimal("10.00"))
        calc = CommissionCalculator(config)

        # Flat fee below minimum, returns minimum
        commission = calc.calculate(Decimal("100"))
        assert commission == Decimal("10.00")

    def test_flat_fee_with_cap(self) -> None:
        """Test flat fee with cap."""
        config = CommissionConfig(flat_fee=Decimal("10.00"), cap=Decimal("8.00"))
        calc = CommissionCalculator(config)

        # Flat fee above cap, returns cap
        commission = calc.calculate(Decimal("100"))
        assert commission == Decimal("8.00")


class TestPerTradePercentageCommission:
    """Test per-trade percentage commission model."""

    def test_percentage_basic(self) -> None:
        """Test percentage-based commission."""
        config = CommissionConfig(percentage=Decimal("0.001"))  # 0.1%
        calc = CommissionCalculator(config)

        # 100 shares @ $50 = $5000 notional, 0.1% = $5.00
        commission = calc.calculate(Decimal("100"), Decimal("50.00"))
        assert commission == Decimal("5.00")

        # 500 shares @ $20 = $10000 notional, 0.1% = $10.00
        commission = calc.calculate(Decimal("500"), Decimal("20.00"))
        assert commission == Decimal("10.00")

    def test_percentage_with_minimum(self) -> None:
        """Test percentage-based commission with minimum."""
        config = CommissionConfig(percentage=Decimal("0.001"), minimum=Decimal("10.00"))
        calc = CommissionCalculator(config)

        # Below minimum: 10 shares @ $50 = $500, 0.1% = $0.50
        commission = calc.calculate(Decimal("10"), Decimal("50.00"))
        assert commission == Decimal("10.00")

        # Above minimum: 1000 shares @ $50 = $50000, 0.1% = $50.00
        commission = calc.calculate(Decimal("1000"), Decimal("50.00"))
        assert commission == Decimal("50.00")

    def test_percentage_with_cap(self) -> None:
        """Test percentage-based commission with cap."""
        config = CommissionConfig(percentage=Decimal("0.001"), cap=Decimal("50.00"))
        calc = CommissionCalculator(config)

        # Below cap: 1000 shares @ $20 = $20000, 0.1% = $20.00
        commission = calc.calculate(Decimal("1000"), Decimal("20.00"))
        assert commission == Decimal("20.00")

        # Above cap: 10000 shares @ $50 = $500000, 0.1% = $500
        commission = calc.calculate(Decimal("10000"), Decimal("50.00"))
        assert commission == Decimal("50.00")

    def test_percentage_requires_price(self) -> None:
        """Test that percentage model requires price parameter."""
        config = CommissionConfig(percentage=Decimal("0.001"))
        calc = CommissionCalculator(config)

        with pytest.raises(ValueError, match="Price is required"):
            calc.calculate(Decimal("100"))


class TestTieredCommission:
    """Test tiered commission model."""

    def test_tiered_single_tier(self) -> None:
        """Test tiered commission with single tier."""
        config = CommissionConfig(tiers=[(Decimal("inf"), Decimal("0.01"))])
        calc = CommissionCalculator(config)

        commission = calc.calculate(Decimal("500"))
        assert commission == Decimal("5.00")

    def test_tiered_two_tiers_first_tier(self) -> None:
        """Test tiered commission in first tier."""
        config = CommissionConfig(
            tiers=[
                (Decimal("1000"), Decimal("0.01")),
                (Decimal("inf"), Decimal("0.005")),
            ]
        )
        calc = CommissionCalculator(config)

        # 500 shares: all in first tier
        commission = calc.calculate(Decimal("500"))
        assert commission == Decimal("5.00")

    def test_tiered_two_tiers_both_tiers(self) -> None:
        """Test tiered commission spanning two tiers."""
        config = CommissionConfig(
            tiers=[
                (Decimal("1000"), Decimal("0.01")),
                (Decimal("inf"), Decimal("0.005")),
            ]
        )
        calc = CommissionCalculator(config)

        # 1500 shares: 1000 @ 0.01 + 500 @ 0.005
        commission = calc.calculate(Decimal("1500"))
        expected = Decimal("1000") * Decimal("0.01") + Decimal("500") * Decimal("0.005")
        assert commission == expected  # $10.00 + $2.50 = $12.50

    def test_tiered_three_tiers(self) -> None:
        """Test tiered commission with three tiers."""
        config = CommissionConfig(
            tiers=[
                (Decimal("1000"), Decimal("0.01")),
                (Decimal("5000"), Decimal("0.005")),
                (Decimal("inf"), Decimal("0.003")),
            ]
        )
        calc = CommissionCalculator(config)

        # 7000 shares: 1000 @ 0.01 + 4000 @ 0.005 + 2000 @ 0.003
        commission = calc.calculate(Decimal("7000"))
        expected = (
            Decimal("1000") * Decimal("0.01") + Decimal("4000") * Decimal("0.005") + Decimal("2000") * Decimal("0.003")
        )
        assert commission == expected  # $10 + $20 + $6 = $36

    def test_tiered_with_minimum(self) -> None:
        """Test tiered commission with minimum."""
        config = CommissionConfig(
            tiers=[(Decimal("inf"), Decimal("0.001"))],
            minimum=Decimal("5.00"),
        )
        calc = CommissionCalculator(config)

        # Below minimum: 100 shares @ 0.001 = $0.10
        commission = calc.calculate(Decimal("100"))
        assert commission == Decimal("5.00")

        # Above minimum: 10000 shares @ 0.001 = $10.00
        commission = calc.calculate(Decimal("10000"))
        assert commission == Decimal("10.00")

    def test_tiered_with_cap(self) -> None:
        """Test tiered commission with cap."""
        config = CommissionConfig(
            tiers=[(Decimal("inf"), Decimal("0.01"))],
            cap=Decimal("50.00"),
        )
        calc = CommissionCalculator(config)

        # Below cap: 1000 shares @ 0.01 = $10.00
        commission = calc.calculate(Decimal("1000"))
        assert commission == Decimal("10.00")

        # Above cap: 10000 shares @ 0.01 = $100.00
        commission = calc.calculate(Decimal("10000"))
        assert commission == Decimal("50.00")

    def test_tiered_exact_boundary(self) -> None:
        """Test tiered commission at exact tier boundary."""
        config = CommissionConfig(
            tiers=[
                (Decimal("1000"), Decimal("0.01")),
                (Decimal("inf"), Decimal("0.005")),
            ]
        )
        calc = CommissionCalculator(config)

        # Exactly 1000 shares: all at first tier rate
        commission = calc.calculate(Decimal("1000"))
        assert commission == Decimal("10.00")


class TestCommissionCalculatorErrors:
    """Test error handling in commission calculator."""

    def test_negative_quantity(self) -> None:
        """Test that negative quantity raises error."""
        config = CommissionConfig(per_share=Decimal("0.005"))
        calc = CommissionCalculator(config)

        with pytest.raises(ValueError, match="Quantity cannot be negative"):
            calc.calculate(Decimal("-100"))

    def test_negative_price(self) -> None:
        """Test that negative price raises error."""
        config = CommissionConfig(percentage=Decimal("0.001"))
        calc = CommissionCalculator(config)

        with pytest.raises(ValueError, match="Price cannot be negative"):
            calc.calculate(Decimal("100"), Decimal("-50"))

    def test_no_model_configured(self) -> None:
        """Test that error is raised if no model is configured (should be impossible)."""
        # This is a defensive test - config validation should prevent this
        config = CommissionConfig.__new__(CommissionConfig)
        config.per_share = None
        config.flat_fee = None
        config.percentage = None
        config.tiers = None
        config.minimum = Decimal("0")
        config.cap = None

        calc = CommissionCalculator(config)

        with pytest.raises(ValueError, match="No commission model configured"):
            calc.calculate(Decimal("100"))


class TestCommissionRealWorldScenarios:
    """Test realistic commission scenarios."""

    def test_retail_broker_per_share(self) -> None:
        """Test typical retail broker per-share commission."""
        # Interactive Brokers style: $0.005 per share, $1 minimum, $1% cap
        config = CommissionConfig(
            per_share=Decimal("0.005"),
            minimum=Decimal("1.00"),
            cap=Decimal("0") + Decimal("50"),  # Would be 1% of large notional
        )
        calc = CommissionCalculator(config)

        # Small order: 50 shares
        commission = calc.calculate(Decimal("50"))
        assert commission == Decimal("1.00")  # Minimum applies

        # Medium order: 1000 shares
        commission = calc.calculate(Decimal("1000"))
        assert commission == Decimal("5.00")  # $0.005 * 1000

        # Large order: 50000 shares
        commission = calc.calculate(Decimal("50000"))
        assert commission == Decimal("50.00")  # Capped

    def test_discount_broker_flat_fee(self) -> None:
        """Test discount broker with flat fee per trade."""
        # Robinhood style: $0 (we'll use $0.01 for testing)
        config = CommissionConfig(flat_fee=Decimal("0.01"))
        calc = CommissionCalculator(config)

        commission = calc.calculate(Decimal("1"))
        assert commission == Decimal("0.01")

        commission = calc.calculate(Decimal("100000"))
        assert commission == Decimal("0.01")

    def test_high_volume_tiered(self) -> None:
        """Test high-volume trader with tiered rates."""
        config = CommissionConfig(
            tiers=[
                (Decimal("10000"), Decimal("0.005")),
                (Decimal("100000"), Decimal("0.003")),
                (Decimal("inf"), Decimal("0.001")),
            ],
            minimum=Decimal("1.00"),
        )
        calc = CommissionCalculator(config)

        # Low volume: 5000 shares
        commission = calc.calculate(Decimal("5000"))
        assert commission == Decimal("25.00")  # 5000 * 0.005

        # Medium volume: 50000 shares
        # 10000 @ 0.005 + 40000 @ 0.003 = $50 + $120 = $170
        commission = calc.calculate(Decimal("50000"))
        assert commission == Decimal("170.00")

        # High volume: 200000 shares
        # 10000 @ 0.005 + 90000 @ 0.003 + 100000 @ 0.001
        # = $50 + $270 + $100 = $420
        commission = calc.calculate(Decimal("200000"))
        assert commission == Decimal("420.00")
