"""
Test multi-strategy position tracking.

Tests that multiple strategies can independently hold positions in the same symbol,
with proper attribution and isolation.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.services.portfolio.models import PortfolioConfig
from qs_trader.services.portfolio.service import PortfolioService


@pytest.fixture
def service() -> PortfolioService:
    """Create portfolio service with default config."""
    config = PortfolioConfig(
        start_datetime=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
        initial_cash=Decimal("1000000.00"),  # $1M starting cash
    )
    return PortfolioService(config=config)


@pytest.fixture
def timestamp() -> datetime:
    """Standard timestamp for all tests."""
    return datetime(2024, 1, 15, 10, 30, tzinfo=timezone.utc)


class TestMultiStrategyAttribution:
    """Test that multiple strategies can hold the same symbol independently."""

    def test_two_strategies_same_symbol_independent_positions(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test two strategies holding same symbol maintain separate positions."""
        # Strategy A buys 100 AAPL @ $150
        service.apply_fill(
            fill_id="strat_a_buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_a",
        )

        # Strategy B buys 50 AAPL @ $152
        service.apply_fill(
            fill_id="strat_b_buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("152.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_b",
        )

        # Verify both positions exist and are independent
        positions = service.get_positions()
        assert len(positions) == 2

        # Check strategy A position
        pos_a = positions[("strategy_a", "AAPL")]
        assert pos_a.strategy_id == "strategy_a"
        assert pos_a.quantity == Decimal("100")
        assert pos_a.avg_price == Decimal("150.00")
        assert pos_a.total_cost == Decimal("15000.00")

        # Check strategy B position
        pos_b = positions[("strategy_b", "AAPL")]
        assert pos_b.strategy_id == "strategy_b"
        assert pos_b.quantity == Decimal("50")
        assert pos_b.avg_price == Decimal("152.00")
        assert pos_b.total_cost == Decimal("7600.00")

        # Verify get_position works with strategy_id
        assert service.get_position("AAPL", "strategy_a") == pos_a
        assert service.get_position("AAPL", "strategy_b") == pos_b

    def test_strategy_close_does_not_affect_other_strategy(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test closing one strategy's position doesn't affect the other."""
        # Both strategies buy AAPL
        service.apply_fill(
            fill_id="a_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_a",
        )

        service.apply_fill(
            fill_id="b_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("150.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_b",
        )

        # Strategy A closes its position
        service.apply_fill(
            fill_id="a_sell",
            timestamp=timestamp,
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("155.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_a",
        )

        # Strategy A should be closed (0 quantity)
        pos_a = service.get_position("AAPL", "strategy_a")
        assert pos_a is not None  # Position exists
        assert pos_a.quantity == Decimal("0")  # But is fully closed
        assert len(pos_a.lots) == 0  # No lots remaining

        # Strategy B should still exist with original values
        pos_b = service.get_position("AAPL", "strategy_b")
        assert pos_b is not None
        assert pos_b.quantity == Decimal("50")
        assert pos_b.avg_price == Decimal("150.00")

    def test_unattributed_strategy_default(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test fills without strategy_id use 'unattributed' default."""
        service.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
            strategy_id=None,  # No strategy
        )

        positions = service.get_positions()
        assert len(positions) == 1
        assert ("unattributed", "AAPL") in positions

        pos = positions[("unattributed", "AAPL")]
        assert pos.strategy_id == "unattributed"
        assert pos.quantity == Decimal("100")

    def test_snapshot_preserves_strategy_attribution(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test snapshot correctly serializes multi-strategy positions."""
        # Two strategies, same symbol
        service.apply_fill(
            fill_id="a_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_a",
        )

        service.apply_fill(
            fill_id="b_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("152.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_b",
        )

        # Get snapshot
        snapshot = service.get_snapshot(timestamp)

        # Verify snapshot has both positions with correct keys
        positions = snapshot["positions"]
        assert len(positions) == 2
        assert "strategy_a:AAPL" in positions
        assert "strategy_b:AAPL" in positions

        # Verify quantities preserved
        assert Decimal(positions["strategy_a:AAPL"]["quantity"]) == Decimal("100")
        assert Decimal(positions["strategy_b:AAPL"]["quantity"]) == Decimal("50")

    def test_restore_preserves_strategy_attribution(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test restore correctly deserializes multi-strategy positions."""
        # Setup: Two strategies
        service.apply_fill(
            fill_id="a_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_a",
        )

        service.apply_fill(
            fill_id="b_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("152.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_b",
        )

        # Take snapshot
        snapshot = service.get_snapshot(timestamp)

        # Restore to new service
        config = PortfolioConfig(
            start_datetime=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
            initial_cash=Decimal("1000000.00"),
        )
        new_service = PortfolioService(config=config)
        new_service.restore_from_snapshot(snapshot)

        # Verify both positions restored correctly
        positions = new_service.get_positions()
        assert len(positions) == 2

        pos_a = new_service.get_position("AAPL", "strategy_a")
        assert pos_a is not None
        assert pos_a.quantity == Decimal("100")
        assert pos_a.strategy_id == "strategy_a"

        pos_b = new_service.get_position("AAPL", "strategy_b")
        assert pos_b is not None
        assert pos_b.quantity == Decimal("50")
        assert pos_b.strategy_id == "strategy_b"

    def test_corporate_actions_apply_to_all_strategies(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test stock splits apply to all strategies holding the symbol."""
        # Two strategies buy AAPL
        service.apply_fill(
            fill_id="a_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("400.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_a",
        )

        service.apply_fill(
            fill_id="b_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("400.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_b",
        )

        # 4-for-1 split
        service.process_split(
            symbol="AAPL",
            split_date=timestamp,
            ratio=Decimal("4.0"),
        )

        # Both strategies should have 4x quantity, 1/4 price
        pos_a = service.get_position("AAPL", "strategy_a")
        assert pos_a is not None
        assert pos_a.quantity == Decimal("400")  # 100 * 4
        assert pos_a.avg_price == Decimal("100.00")  # 400 / 4

        pos_b = service.get_position("AAPL", "strategy_b")
        assert pos_b is not None
        assert pos_b.quantity == Decimal("200")  # 50 * 4
        assert pos_b.avg_price == Decimal("100.00")  # 400 / 4

    def test_dividends_apply_to_all_strategies(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test dividends apply proportionally to all strategies."""
        # Two strategies buy AAPL
        service.apply_fill(
            fill_id="a_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_a",
        )

        service.apply_fill(
            fill_id="b_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("150.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_b",
        )

        initial_cash = service.get_cash()

        # $0.82 dividend per share
        service.process_dividend(
            symbol="AAPL",
            effective_date=timestamp,
            amount_per_share=Decimal("0.82"),
        )

        # Total dividend: 100 * 0.82 + 50 * 0.82 = $82 + $41 = $123
        expected_cash = initial_cash + Decimal("123.00")
        assert service.get_cash() == expected_cash

        # Verify dividends tracked on each position
        pos_a = service.get_position("AAPL", "strategy_a")
        assert pos_a is not None
        assert pos_a.dividends_received == Decimal("82.00")

        pos_b = service.get_position("AAPL", "strategy_b")
        assert pos_b is not None
        assert pos_b.dividends_received == Decimal("41.00")

    def test_get_all_lots_filtered_by_strategy(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test get_all_lots can filter by strategy."""
        # Strategy A buys AAPL
        service.apply_fill(
            fill_id="a_buy",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
            strategy_id="strategy_a",
        )

        # Strategy B buys AAPL and TSLA
        service.apply_fill(
            fill_id="b_buy_aapl",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("50"),
            price=Decimal("152.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_b",
        )

        service.apply_fill(
            fill_id="b_buy_tsla",
            timestamp=timestamp,
            symbol="TSLA",
            side="buy",
            quantity=Decimal("25"),
            price=Decimal("200.00"),
            commission=Decimal("5.00"),
            strategy_id="strategy_b",
        )

        # Get all lots for strategy A
        lots_a = service.get_all_lots(strategy_id="strategy_a")
        assert len(lots_a) == 1
        assert lots_a[0].symbol == "AAPL"
        assert lots_a[0].quantity == Decimal("100")

        # Get all lots for strategy B
        lots_b = service.get_all_lots(strategy_id="strategy_b")
        assert len(lots_b) == 2
        symbols = {lot.symbol for lot in lots_b}
        assert symbols == {"AAPL", "TSLA"}

        # Get AAPL lots for strategy A
        aapl_a_lots = service.get_all_lots(symbol="AAPL", strategy_id="strategy_a")
        assert len(aapl_a_lots) == 1
        assert aapl_a_lots[0].quantity == Decimal("100")

        # Get AAPL lots for strategy B
        aapl_b_lots = service.get_all_lots(symbol="AAPL", strategy_id="strategy_b")
        assert len(aapl_b_lots) == 1
        assert aapl_b_lots[0].quantity == Decimal("50")
