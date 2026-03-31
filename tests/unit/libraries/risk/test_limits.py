"""Unit tests for risk limit checking tools."""

from decimal import Decimal

import pytest

from qs_trader.libraries.risk.tools.limits import (
    Position,
    ProposedOrder,
    check_all_limits,
    check_concentration_limit,
    check_leverage_limits,
)


class TestDataClasses:
    """Tests for immutable data classes."""

    def test_position_is_frozen(self):
        """Test Position is immutable."""
        position = Position(symbol="AAPL", quantity=100, market_value=Decimal("15000"))
        with pytest.raises(AttributeError):
            position.quantity = 200  # pyright: ignore[reportAttributeAccessIssue]

    def test_proposed_order_is_frozen(self):
        """Test ProposedOrder is immutable."""
        order = ProposedOrder(symbol="GOOGL", side="BUY", quantity=50)
        with pytest.raises(AttributeError):
            order.quantity = 100  # pyright: ignore[reportAttributeAccessIssue]


class TestCheckConcentrationLimit:
    """Tests for check_concentration_limit function."""

    def test_new_buy_within_limit_returns_none(self):
        """Test new BUY position within limit."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=100)
        violation = check_concentration_limit(
            order=order,
            current_positions=[],
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_position_pct=0.20,
        )
        assert violation is None

    def test_new_buy_exceeds_limit_returns_violation(self):
        """Test new BUY position exceeding limit."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=200)
        violation = check_concentration_limit(
            order=order,
            current_positions=[],
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_position_pct=0.20,
        )
        assert violation is not None
        assert violation.proposed_pct == 0.30

    def test_adding_to_existing_position_within_limit(self):
        """Test adding to existing position stays within limit."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=50)
        current_positions = [Position(symbol="AAPL", quantity=50, market_value=Decimal("7500"))]
        violation = check_concentration_limit(
            order=order,
            current_positions=current_positions,
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_position_pct=0.20,
        )
        assert violation is None

    def test_short_position_uses_absolute_value(self):
        """Test short positions use absolute value."""
        order = ProposedOrder(symbol="AAPL", side="SELL", quantity=200)
        current_positions = [Position(symbol="AAPL", quantity=-100, market_value=Decimal("-15000"))]
        violation = check_concentration_limit(
            order=order,
            current_positions=current_positions,
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_position_pct=0.20,
        )
        assert violation is not None
        assert violation.proposed_pct == 0.45

    def test_negative_equity_raises_error(self):
        """Test negative equity raises ValueError."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=100)
        with pytest.raises(ValueError, match="equity must be non-negative"):
            check_concentration_limit(
                order=order,
                current_positions=[],
                equity=Decimal("-100000"),
                current_price=Decimal("150"),
                max_position_pct=0.20,
            )


class TestCheckLeverageLimits:
    """Tests for check_leverage_limits function."""

    def test_no_positions_within_limits_returns_none(self):
        """Test no positions, new order within limits."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=100)
        violation = check_leverage_limits(
            order=order,
            current_positions=[],
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
        )
        assert violation is None

    def test_gross_leverage_exceeded_returns_violation(self):
        """Test gross leverage exceeding limit."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=200)
        current_positions = [
            Position(symbol="GOOGL", quantity=400, market_value=Decimal("60000")),
            Position(symbol="MSFT", quantity=300, market_value=Decimal("45000")),
        ]
        violation = check_leverage_limits(
            order=order,
            current_positions=current_positions,
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
        )
        assert violation is not None
        assert violation.proposed_pct == 1.35

    def test_net_leverage_exceeded_returns_violation(self):
        """Test net leverage exceeding limit."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=500)
        current_positions = [
            Position(symbol="GOOGL", quantity=200, market_value=Decimal("30000")),
        ]
        violation = check_leverage_limits(
            order=order,
            current_positions=current_positions,
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_gross_leverage=2.0,
            max_net_leverage=0.50,
        )
        assert violation is not None
        assert violation.proposed_pct == 1.05


class TestCheckAllLimits:
    """Tests for check_all_limits convenience function."""

    def test_no_violations_returns_empty_list(self):
        """Test order within all limits."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=100)
        violations = check_all_limits(
            order=order,
            current_positions=[],
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_position_pct=0.30,
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
        )
        assert violations == []

    def test_multiple_violations_returns_all(self):
        """Test multiple violations returned."""
        order = ProposedOrder(symbol="AAPL", side="BUY", quantity=1000)
        violations = check_all_limits(
            order=order,
            current_positions=[],
            equity=Decimal("100000"),
            current_price=Decimal("150"),
            max_position_pct=0.20,
            max_gross_leverage=1.0,
            max_net_leverage=1.0,
        )
        assert len(violations) == 2
        assert any(v.limit_type == "concentration" for v in violations)
        assert any(v.limit_type == "leverage" for v in violations)
