"""
Integration tests for Corporate Actions in Portfolio Service.

Tests end-to-end event-driven flow:
DataService publishes CorporateActionEvent → Portfolio handles → Updates applied.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import CorporateActionEvent
from qs_trader.services.portfolio import PortfolioConfig, PortfolioService


@pytest.fixture
def event_bus() -> EventBus:
    """Create event bus for testing."""
    return EventBus()


@pytest.fixture
def timestamp() -> datetime:
    """Fixed timestamp for testing."""
    return datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)


@pytest.fixture
def service(event_bus: EventBus) -> PortfolioService:
    """Create service with event bus."""
    config = PortfolioConfig(initial_cash=Decimal("100000"))
    return PortfolioService(config, event_bus=event_bus)


class TestDividendEventIntegration:
    """Test dividend processing via events."""

    def test_dividend_event_increases_cash_for_long_position(
        self,
        service: PortfolioService,
        event_bus: EventBus,
        timestamp: datetime,
    ) -> None:
        """Test that dividend event properly increases cash for long position."""
        # 1. Open long position: Buy 100 shares @ $150
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        initial_cash = service.get_cash()
        assert initial_cash == Decimal("100000") - Decimal("15010.00")  # 100*150 + 10 commission

        # 2. Publish dividend event
        dividend_event = CorporateActionEvent(
            symbol="AAPL",
            asset_class="equity",
            action_type="dividend",
            announcement_date="2024-01-10",
            ex_date="2024-01-15",
            effective_date="2024-01-20",
            source="test",
            dividend_amount=Decimal("0.82"),
            dividend_currency="USD",
            dividend_type="ordinary",
        )

        event_bus.publish(dividend_event)

        # 3. Verify cash increased by dividend amount
        expected_dividend = Decimal("100") * Decimal("0.82")  # 100 shares * $0.82
        final_cash = service.get_cash()
        assert final_cash == initial_cash + expected_dividend

        # 4. Verify position tracking
        position = service.get_position("AAPL")
        assert position is not None
        assert position.dividends_received == expected_dividend

        # 5. Verify global tracking
        state = service.get_state()
        assert state.total_dividends_received == expected_dividend

    def test_dividend_event_decreases_cash_for_short_position(
        self,
        service: PortfolioService,
        event_bus: EventBus,
        timestamp: datetime,
    ) -> None:
        """Test that dividend event properly decreases cash for short position."""
        # 1. Open short position: Sell 50 shares @ $150
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )

        initial_cash = service.get_cash()
        # Short sale increases cash: 100000 + 50*150 - 10 = 107490
        assert initial_cash == Decimal("100000") + Decimal("7490.00")

        # 2. Publish dividend event
        dividend_event = CorporateActionEvent(
            symbol="AAPL",
            asset_class="equity",
            action_type="dividend",
            announcement_date="2024-01-10",
            ex_date="2024-01-15",
            effective_date="2024-01-20",
            source="test",
            dividend_amount=Decimal("0.82"),
        )

        event_bus.publish(dividend_event)

        # 3. Verify cash decreased (short position pays dividend)
        dividend_cost = Decimal("50") * Decimal("0.82")  # 50 shares * $0.82
        final_cash = service.get_cash()
        assert final_cash == initial_cash - dividend_cost

        # 4. Verify position tracking (negative quantity for short)
        position = service.get_position("AAPL")
        assert position is not None
        assert position.dividends_paid == dividend_cost

        # 5. Verify global tracking
        state = service.get_state()
        assert state.total_dividends_paid == dividend_cost

    def test_dividend_event_ignored_when_no_position(
        self,
        service: PortfolioService,
        event_bus: EventBus,
    ) -> None:
        """Test that dividend event is silently ignored when no position exists."""
        initial_cash = service.get_cash()

        # Publish dividend event for symbol we don't hold
        dividend_event = CorporateActionEvent(
            symbol="MSFT",
            asset_class="equity",
            action_type="dividend",
            announcement_date="2024-01-10",
            ex_date="2024-01-15",
            effective_date="2024-01-20",
            source="test",
            dividend_amount=Decimal("0.75"),
        )

        event_bus.publish(dividend_event)

        # Verify cash unchanged
        assert service.get_cash() == initial_cash

        # Verify no dividend tracking
        state = service.get_state()
        assert state.total_dividends_received == Decimal("0")
        assert state.total_dividends_paid == Decimal("0")


class TestSplitEventIntegration:
    """Test stock split processing via events."""

    def test_split_event_adjusts_long_position(
        self,
        service: PortfolioService,
        event_bus: EventBus,
        timestamp: datetime,
    ) -> None:
        """Test that 4-for-1 split properly adjusts long position."""
        # 1. Open long position: Buy 100 shares @ $400
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("400.00"),
            commission=Decimal("10.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("100")
        assert position.avg_price == Decimal("400.00")
        initial_total_cost = position.total_cost

        # 2. Publish 4-for-1 split event
        split_event = CorporateActionEvent(
            symbol="AAPL",
            asset_class="equity",
            action_type="split",
            announcement_date="2024-01-10",
            ex_date="2024-01-15",
            effective_date="2024-01-15",
            source="test",
            split_from=1,
            split_to=4,
            split_ratio=Decimal("4.0"),
        )

        event_bus.publish(split_event)

        # 3. Verify position adjusted
        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("400")  # 100 * 4
        assert position.avg_price == Decimal("100.00")  # 400 / 4
        assert position.total_cost == initial_total_cost  # Value preserved

    def test_split_event_adjusts_short_position(
        self,
        service: PortfolioService,
        event_bus: EventBus,
        timestamp: datetime,
    ) -> None:
        """Test that split properly adjusts short position."""
        # 1. Open short position: Sell 50 shares @ $400
        service.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("400.00"),
            commission=Decimal("10.00"),
        )

        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("-50")  # Negative for short
        assert position.avg_price == Decimal("400.00")
        initial_total_cost = position.total_cost

        # 2. Publish 4-for-1 split event
        split_event = CorporateActionEvent(
            symbol="AAPL",
            asset_class="equity",
            action_type="split",
            announcement_date="2024-01-10",
            ex_date="2024-01-15",
            effective_date="2024-01-15",
            source="test",
            split_ratio=Decimal("4.0"),
        )

        event_bus.publish(split_event)

        # 3. Verify position adjusted (still negative, but 4x quantity)
        position = service.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("-200")  # -50 * 4
        assert position.avg_price == Decimal("100.00")  # 400 / 4
        assert position.total_cost == initial_total_cost  # Value preserved

    def test_reverse_split_event(
        self,
        service: PortfolioService,
        event_bus: EventBus,
        timestamp: datetime,
    ) -> None:
        """Test that 1-for-4 reverse split properly adjusts position."""
        # 1. Open long position: Buy 400 shares @ $10
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="XYZ",
            side="buy",
            quantity=Decimal("400"),
            price=Decimal("10.00"),
            commission=Decimal("10.00"),
        )

        position = service.get_position("XYZ")
        assert position is not None
        assert position.quantity == Decimal("400")
        assert position.avg_price == Decimal("10.00")
        initial_total_cost = position.total_cost

        # 2. Publish 1-for-4 reverse split event (ratio = 0.25)
        split_event = CorporateActionEvent(
            symbol="XYZ",
            asset_class="equity",
            action_type="split",
            announcement_date="2024-01-10",
            ex_date="2024-01-15",
            effective_date="2024-01-15",
            source="test",
            split_from=4,
            split_to=1,
            split_ratio=Decimal("0.25"),
        )

        event_bus.publish(split_event)

        # 3. Verify position adjusted
        position = service.get_position("XYZ")
        assert position is not None
        assert position.quantity == Decimal("100")  # 400 * 0.25
        assert position.avg_price == Decimal("40.00")  # 10 / 0.25
        assert position.total_cost == initial_total_cost  # Value preserved

    def test_split_event_ignored_when_no_position(
        self,
        service: PortfolioService,
        event_bus: EventBus,
    ) -> None:
        """Test that split event is silently ignored when no position exists."""
        # Publish split event for symbol we don't hold
        split_event = CorporateActionEvent(
            symbol="MSFT",
            asset_class="equity",
            action_type="split",
            announcement_date="2024-01-10",
            ex_date="2024-01-15",
            effective_date="2024-01-15",
            source="test",
            split_ratio=Decimal("2.0"),
        )

        # Should not raise error, just log and ignore
        event_bus.publish(split_event)

        # Verify no position created
        position = service.get_position("MSFT")
        assert position is None


class TestMultiplePositionsCorporateActions:
    """Test corporate actions with multiple positions (different strategies)."""

    def test_dividend_applies_to_all_strategy_positions(
        self,
        event_bus: EventBus,
        timestamp: datetime,
    ) -> None:
        """Test dividend applies to same symbol across multiple strategies."""
        config = PortfolioConfig(initial_cash=Decimal("200000"))
        service = PortfolioService(config, event_bus=event_bus)

        # Strategy 1: Buy 100 shares
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_1",
        )

        # Strategy 2: Buy 50 shares
        service.apply_fill(
            fill_id="buy_002",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("150.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_2",
        )

        initial_cash = service.get_cash()

        # Publish dividend event
        dividend_event = CorporateActionEvent(
            symbol="AAPL",
            asset_class="equity",
            action_type="dividend",
            announcement_date="2024-01-10",
            ex_date="2024-01-15",
            effective_date="2024-01-20",
            source="test",
            dividend_amount=Decimal("0.82"),
        )

        event_bus.publish(dividend_event)

        # Total dividend should be for all 150 shares
        expected_total_dividend = Decimal("150") * Decimal("0.82")
        final_cash = service.get_cash()
        assert final_cash == initial_cash + expected_total_dividend

        # Each position should track its own dividend
        pos1 = service.get_position("AAPL", strategy_id="strategy_1")
        assert pos1 is not None
        assert pos1.dividends_received == Decimal("100") * Decimal("0.82")

        pos2 = service.get_position("AAPL", strategy_id="strategy_2")
        assert pos2 is not None
        assert pos2.dividends_received == Decimal("50") * Decimal("0.82")
