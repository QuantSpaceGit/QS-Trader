"""Tests for slippage models."""

from datetime import datetime
from decimal import Decimal

import pytest

from qs_trader.services.data.models import Bar
from qs_trader.services.execution.models import Order, OrderSide, OrderType
from qs_trader.services.execution.slippage import (
    FixedBpsSlippage,
    SlippageCalculatorFactory,
    SlippageModel,
    SpreadBasedSlippage,
    TimeOfDaySlippage,
    VolumeBasedSlippage,
)


class TestFixedBpsSlippage:
    """Test fixed BPS slippage model."""

    def test_create_calculator(self) -> None:
        """Test creating fixed BPS calculator."""
        calc = FixedBpsSlippage(bps=Decimal("5"))
        assert calc.bps == Decimal("5")

    def test_negative_bps_raises(self) -> None:
        """Test that negative BPS raises error."""
        with pytest.raises(ValueError, match="BPS cannot be negative"):
            FixedBpsSlippage(bps=Decimal("-5"))

    def test_buy_order_pays_more(self) -> None:
        """Test buy order pays slippage."""
        calc = FixedBpsSlippage(bps=Decimal("5"))
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10000,
        )

        # 5 bps = 0.05%, so 100 * 1.0005 = 100.05
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        expected = Decimal("100") * Decimal("1.0005")
        assert fill_price == expected

    def test_sell_order_receives_less(self) -> None:
        """Test sell order receives less with slippage."""
        calc = FixedBpsSlippage(bps=Decimal("5"))
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.SELL,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10000,
        )

        # Sell: price * (2 - 1.0005) = 100 * 0.9995
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        expected = Decimal("100") * Decimal("0.9995")
        assert fill_price == expected

    def test_zero_bps_no_slippage(self) -> None:
        """Test zero BPS means no slippage."""
        calc = FixedBpsSlippage(bps=Decimal("0"))
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10000,
        )

        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        assert fill_price == Decimal("100")


class TestVolumeBasedSlippage:
    """Test volume-based slippage model."""

    def test_create_calculator(self) -> None:
        """Test creating volume-based calculator."""
        calc = VolumeBasedSlippage(base_bps=Decimal("5"), impact_factor=Decimal("10"))
        assert calc.base_bps == Decimal("5")
        assert calc.impact_factor == Decimal("10")

    def test_negative_base_bps_raises(self) -> None:
        """Test that negative base BPS raises error."""
        with pytest.raises(ValueError, match="Base BPS cannot be negative"):
            VolumeBasedSlippage(base_bps=Decimal("-5"), impact_factor=Decimal("10"))

    def test_negative_impact_factor_raises(self) -> None:
        """Test that negative impact factor raises error."""
        with pytest.raises(ValueError, match="Impact factor cannot be negative"):
            VolumeBasedSlippage(base_bps=Decimal("5"), impact_factor=Decimal("-10"))

    def test_low_participation_uses_base_slippage(self) -> None:
        """Test low participation uses mostly base slippage."""
        calc = VolumeBasedSlippage(base_bps=Decimal("5"), impact_factor=Decimal("10"))
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10000,  # Filling 100/10000 = 1% of volume
        )

        # Participation = 100/10000 = 0.01
        # Total BPS = 5 + 0.01 * 10 = 5.1 bps
        # Price = 100 * 1.00051
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        expected_bps = Decimal("5") + (Decimal("100") / Decimal("10000")) * Decimal("10")
        expected = Decimal("100") * (Decimal("1") + expected_bps / Decimal("10000"))
        assert fill_price == expected

    def test_high_participation_increases_slippage(self) -> None:
        """Test high participation increases slippage significantly."""
        calc = VolumeBasedSlippage(base_bps=Decimal("5"), impact_factor=Decimal("10"))
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("5000"),
            order_type=OrderType.MARKET,
        )
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10000,  # Filling 5000/10000 = 50% of volume
        )

        # Participation = 5000/10000 = 0.5
        # Total BPS = 5 + 0.5 * 10 = 10 bps
        fill_price = calc.calculate(order, bar, Decimal("5000"), Decimal("100"))
        expected_bps = Decimal("5") + Decimal("0.5") * Decimal("10")
        expected = Decimal("100") * (Decimal("1") + expected_bps / Decimal("10000"))
        assert fill_price == expected

    def test_zero_volume_uses_base_slippage(self) -> None:
        """Test zero volume bar uses only base slippage."""
        calc = VolumeBasedSlippage(base_bps=Decimal("5"), impact_factor=Decimal("10"))
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=0,
        )

        # Zero volume: participation = 0, use base slippage only
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        expected = Decimal("100") * Decimal("1.0005")  # 5 bps
        assert fill_price == expected


class TestSpreadBasedSlippage:
    """Test spread-based slippage model."""

    def test_create_calculator(self) -> None:
        """Test creating spread-based calculator."""
        calc = SpreadBasedSlippage(fallback_bps=Decimal("5"), spread_fraction=Decimal("0.5"))
        assert calc.fallback_bps == Decimal("5")
        assert calc.spread_fraction == Decimal("0.5")

    def test_negative_fallback_bps_raises(self) -> None:
        """Test that negative fallback BPS raises error."""
        with pytest.raises(ValueError, match="Fallback BPS cannot be negative"):
            SpreadBasedSlippage(fallback_bps=Decimal("-5"))

    def test_invalid_spread_fraction_raises(self) -> None:
        """Test that invalid spread fraction raises error."""
        with pytest.raises(ValueError, match="Spread fraction must be in"):
            SpreadBasedSlippage(fallback_bps=Decimal("5"), spread_fraction=Decimal("1.5"))

    def test_uses_half_spread_when_available(self) -> None:
        """Test uses half-spread when bar has spread data."""
        calc = SpreadBasedSlippage(fallback_bps=Decimal("5"), spread_fraction=Decimal("0.5"))
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=102.0,  # Spread = 102 - 98 = 4
            low=98.0,
            close=100.0,
            volume=10000,
        )

        # Spread = 4, half-spread = 2
        # As BPS: (2 / 100) * 10000 = 200 bps
        # Price = 100 * 1.02
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        spread = Decimal("102") - Decimal("98")
        half_spread = spread * Decimal("0.5")
        spread_bps = half_spread / Decimal("100") * Decimal("10000")
        expected = Decimal("100") * (Decimal("1") + spread_bps / Decimal("10000"))
        assert fill_price == expected

    def test_uses_fallback_when_no_spread(self) -> None:
        """Test uses fallback BPS when no spread data."""
        calc = SpreadBasedSlippage(fallback_bps=Decimal("5"), spread_fraction=Decimal("0.5"))
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=100.0,  # No spread
            low=100.0,
            close=100.0,
            volume=10000,
        )

        # No spread: use fallback 5 bps
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        expected = Decimal("100") * Decimal("1.0005")
        assert fill_price == expected


class TestTimeOfDaySlippage:
    """Test time-of-day slippage model."""

    def test_create_calculator(self) -> None:
        """Test creating time-of-day calculator."""
        calc = TimeOfDaySlippage(
            base_bps=Decimal("5"),
            open_multiplier=Decimal("2.0"),
            close_multiplier=Decimal("1.5"),
        )
        assert calc.base_bps == Decimal("5")
        assert calc.open_multiplier == Decimal("2.0")
        assert calc.close_multiplier == Decimal("1.5")

    def test_negative_base_bps_raises(self) -> None:
        """Test that negative base BPS raises error."""
        with pytest.raises(ValueError, match="Base BPS cannot be negative"):
            TimeOfDaySlippage(base_bps=Decimal("-5"))

    def test_invalid_open_multiplier_raises(self) -> None:
        """Test that open multiplier < 1.0 raises error."""
        with pytest.raises(ValueError, match="Open multiplier must be"):
            TimeOfDaySlippage(base_bps=Decimal("5"), open_multiplier=Decimal("0.5"))

    def test_market_open_elevated_slippage(self) -> None:
        """Test elevated slippage at market open."""
        calc = TimeOfDaySlippage(
            base_bps=Decimal("5"),
            open_multiplier=Decimal("2.0"),
            open_minutes=30,
        )
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        # 9:30 AM - within open period
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 9, 30),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10000,
        )

        # Within open period: 5 * 2.0 = 10 bps
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        expected = Decimal("100") * Decimal("1.001")  # 10 bps
        assert fill_price == expected

    def test_market_close_elevated_slippage(self) -> None:
        """Test elevated slippage at market close."""
        calc = TimeOfDaySlippage(
            base_bps=Decimal("5"),
            close_multiplier=Decimal("1.5"),
            close_minutes=30,
        )
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        # 3:30 PM - within close period
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 15, 30),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10000,
        )

        # Within close period: 5 * 1.5 = 7.5 bps
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        expected = Decimal("100") * Decimal("1.00075")  # 7.5 bps
        assert fill_price == expected

    def test_mid_day_normal_slippage(self) -> None:
        """Test normal slippage during mid-day hours."""
        calc = TimeOfDaySlippage(
            base_bps=Decimal("5"),
            open_multiplier=Decimal("2.0"),
            close_multiplier=Decimal("1.5"),
        )
        order = Order(
            order_id="test",
            created_at=datetime.now(),
            symbol="AAPL",
            side=OrderSide.BUY,
            quantity=Decimal("100"),
            order_type=OrderType.MARKET,
        )
        # 12:00 PM - mid-day
        bar = Bar(
            trade_datetime=datetime(2024, 1, 1, 12, 0),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.5,
            volume=10000,
        )

        # Mid-day: base slippage only = 5 bps
        fill_price = calc.calculate(order, bar, Decimal("100"), Decimal("100"))
        expected = Decimal("100") * Decimal("1.0005")  # 5 bps
        assert fill_price == expected


class TestSlippageCalculatorFactory:
    """Test slippage calculator factory."""

    def test_create_fixed_bps(self) -> None:
        """Test creating fixed BPS calculator via factory."""
        calc = SlippageCalculatorFactory.create(SlippageModel.FIXED_BPS, bps=Decimal("5"))
        assert isinstance(calc, FixedBpsSlippage)
        assert calc.bps == Decimal("5")

    def test_create_volume_based(self) -> None:
        """Test creating volume-based calculator via factory."""
        calc = SlippageCalculatorFactory.create(
            SlippageModel.VOLUME_BASED,
            base_bps=Decimal("5"),
            impact_factor=Decimal("10"),
        )
        assert isinstance(calc, VolumeBasedSlippage)
        assert calc.base_bps == Decimal("5")
        assert calc.impact_factor == Decimal("10")

    def test_create_spread_based(self) -> None:
        """Test creating spread-based calculator via factory."""
        calc = SlippageCalculatorFactory.create(
            SlippageModel.SPREAD_BASED,
            fallback_bps=Decimal("5"),
            spread_fraction=Decimal("0.5"),
        )
        assert isinstance(calc, SpreadBasedSlippage)
        assert calc.fallback_bps == Decimal("5")
        assert calc.spread_fraction == Decimal("0.5")

    def test_create_time_of_day(self) -> None:
        """Test creating time-of-day calculator via factory."""
        calc = SlippageCalculatorFactory.create(
            SlippageModel.TIME_OF_DAY,
            base_bps=Decimal("5"),
            open_multiplier=Decimal("2.0"),
            close_multiplier=Decimal("1.5"),
        )
        assert isinstance(calc, TimeOfDaySlippage)
        assert calc.base_bps == Decimal("5")
        assert calc.open_multiplier == Decimal("2.0")
        assert calc.close_multiplier == Decimal("1.5")

    def test_missing_required_parameter_raises(self) -> None:
        """Test that missing required parameter raises error."""
        with pytest.raises(ValueError, match="requires 'bps' parameter"):
            SlippageCalculatorFactory.create(SlippageModel.FIXED_BPS)

    def test_unknown_model_raises(self) -> None:
        """Test that unknown model raises error."""
        with pytest.raises(ValueError, match="Unknown slippage model"):
            SlippageCalculatorFactory.create("invalid_model", bps=Decimal("5"))  # type: ignore
