"""Unit tests for portfolio state management - Week 4."""

import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.services.portfolio import PortfolioConfig, PortfolioService


@pytest.fixture
def timestamp() -> datetime:
    """Standard timestamp for tests."""
    return datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)


@pytest.fixture
def basic_config() -> PortfolioConfig:
    """Standard portfolio configuration."""
    return PortfolioConfig(
        initial_cash=Decimal("100000.00"),
        default_borrow_rate_apr=Decimal("0.05"),
        margin_rate_apr=Decimal("0.07"),
    )


@pytest.fixture
def service(basic_config: PortfolioConfig) -> PortfolioService:
    """Standard portfolio service instance."""
    return PortfolioService(config=basic_config)


@pytest.fixture
def populated_service(basic_config: PortfolioConfig, timestamp: datetime) -> PortfolioService:
    """Portfolio service with some positions and history."""
    service = PortfolioService(config=basic_config)

    # Buy AAPL
    service.apply_fill(
        fill_id="buy_aapl_001",
        timestamp=timestamp,
        symbol="AAPL",
        side="buy",
        quantity=Decimal("100"),
        price=Decimal("150.00"),
        commission=Decimal("10.00"),
    )

    # Add another lot
    service.apply_fill(
        fill_id="buy_aapl_002",
        timestamp=timestamp.replace(hour=11),
        symbol="AAPL",
        side="buy",
        quantity=Decimal("50"),
        price=Decimal("152.00"),
        commission=Decimal("5.00"),
    )

    # Short TSLA
    service.apply_fill(
        fill_id="sell_tsla_001",
        timestamp=timestamp.replace(hour=12),
        symbol="TSLA",
        side="sell",
        quantity=Decimal("25"),
        price=Decimal("200.00"),
        commission=Decimal("5.00"),
    )

    # Update prices
    service.update_prices({"AAPL": Decimal("155.00"), "TSLA": Decimal("198.00")})

    return service


class TestSnapshot:
    """Test portfolio snapshot functionality."""

    def test_snapshot_empty_portfolio(
        self,
        service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test snapshot of empty portfolio."""
        snapshot = service.get_snapshot(timestamp)

        assert snapshot["metadata"]["snapshot_version"] == "1.0"
        assert Decimal(snapshot["cash"]) == Decimal("100000.00")
        assert len(snapshot["positions"]) == 0
        assert len(snapshot["ledger"]) == 0

    def test_snapshot_with_positions(
        self,
        populated_service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test snapshot with positions."""
        snapshot = populated_service.get_snapshot(timestamp)

        # Check positions captured
        assert len(snapshot["positions"]) == 2
        assert "unattributed:AAPL" in snapshot["positions"]
        assert "unattributed:TSLA" in snapshot["positions"]

        # Check AAPL position
        aapl_pos = snapshot["positions"]["unattributed:AAPL"]
        assert Decimal(aapl_pos["quantity"]) == Decimal("150")  # 100 + 50
        assert len(aapl_pos["lots"]) == 2  # Two buy lots

        # Check TSLA position
        tsla_pos = snapshot["positions"]["unattributed:TSLA"]
        assert Decimal(tsla_pos["quantity"]) == Decimal("-25")  # Short
        assert len(tsla_pos["lots"]) == 1

        # Check ledger captured
        assert len(snapshot["ledger"]) == 3  # 3 fills

        # Check cumulative metrics
        metrics = snapshot["cumulative_metrics"]
        assert Decimal(metrics["total_commissions"]) == Decimal("20.00")  # 10 + 5 + 5

    def test_snapshot_json_serializable(
        self,
        populated_service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that snapshot can be serialized to JSON."""
        snapshot = populated_service.get_snapshot(timestamp)

        # Should be able to serialize to JSON and back
        json_str = json.dumps(snapshot)
        restored_data = json.loads(json_str)

        assert restored_data["metadata"]["snapshot_version"] == "1.0"
        assert len(restored_data["positions"]) == 2

    def test_snapshot_preserves_lot_details(
        self,
        populated_service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test that lot details are preserved in snapshot."""
        snapshot = populated_service.get_snapshot(timestamp)

        aapl_pos = snapshot["positions"]["unattributed:AAPL"]
        lots = aapl_pos["lots"]

        # Should have 2 lots with different prices
        assert len(lots) == 2
        prices = [Decimal(lot["entry_price"]) for lot in lots]
        assert Decimal("150.00") in prices
        assert Decimal("152.00") in prices


class TestRestore:
    """Test portfolio restore functionality."""

    def test_restore_empty_portfolio(
        self,
        service: PortfolioService,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """Test restoring empty portfolio."""
        snapshot = service.get_snapshot(timestamp)

        # Create new service and restore
        new_service = PortfolioService(config=basic_config)
        new_service.restore_from_snapshot(snapshot)

        assert new_service.get_cash() == Decimal("100000.00")
        assert len(new_service.get_positions()) == 0

    def test_restore_with_positions(
        self,
        populated_service: PortfolioService,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """Test restoring portfolio with positions."""
        # Get original state
        original_cash = populated_service.get_cash()
        original_positions = populated_service.get_positions()

        # Create snapshot
        snapshot = populated_service.get_snapshot(timestamp)

        # Restore to new service
        new_service = PortfolioService(config=basic_config)
        new_service.restore_from_snapshot(snapshot)

        # Verify cash matches
        assert new_service.get_cash() == original_cash

        # Verify positions match
        restored_positions = new_service.get_positions()
        assert len(restored_positions) == len(original_positions)

        for key in original_positions:
            strategy_id, symbol = key
            orig_pos = populated_service.get_position(symbol, strategy_id)
            restored_pos = new_service.get_position(symbol, strategy_id)
            assert restored_pos is not None
            assert orig_pos is not None
            assert restored_pos.quantity == orig_pos.quantity
            assert restored_pos.avg_price == orig_pos.avg_price
            assert restored_pos.market_value == orig_pos.market_value

        # Verify lots restored
        orig_aapl_lots = populated_service.get_all_lots("AAPL")
        restored_aapl_lots = new_service.get_all_lots("AAPL")
        assert len(restored_aapl_lots) == len(orig_aapl_lots)

    def test_restore_preserves_cumulative_metrics(
        self,
        populated_service: PortfolioService,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """Test that cumulative metrics are preserved."""
        # Get original metrics
        original_state = populated_service.get_state()

        # Snapshot and restore
        snapshot = populated_service.get_snapshot(timestamp)
        new_service = PortfolioService(config=basic_config)
        new_service.restore_from_snapshot(snapshot)

        # Verify metrics match
        restored_state = new_service.get_state()
        assert restored_state.total_commissions == original_state.total_commissions
        assert restored_state.realized_pnl == original_state.realized_pnl

    def test_restore_invalid_snapshot_missing_keys(
        self,
        service: PortfolioService,
    ) -> None:
        """Test that invalid snapshot raises error."""
        invalid_snapshot = {"cash": "100000", "positions": {}}  # Missing required keys

        with pytest.raises(ValueError, match="Invalid snapshot"):
            service.restore_from_snapshot(invalid_snapshot)

    def test_snapshot_restore_round_trip_with_operations(
        self,
        basic_config: PortfolioConfig,
        timestamp: datetime,
    ) -> None:
        """Test full round trip with operations after restore."""
        # Create portfolio with positions
        service1 = PortfolioService(config=basic_config)
        service1.apply_fill(
            fill_id="buy_001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("100"),
            price=Decimal("150.00"),
            commission=Decimal("10.00"),
        )
        service1.update_prices({"AAPL": Decimal("155.00")})

        # Snapshot
        snapshot = service1.get_snapshot(timestamp)

        # Restore to new service
        service2 = PortfolioService(config=basic_config)
        service2.restore_from_snapshot(snapshot)

        # Perform new operation on restored service
        service2.apply_fill(
            fill_id="sell_001",
            timestamp=timestamp.replace(hour=14),
            symbol="AAPL",
            side="sell",
            quantity=Decimal("50"),
            price=Decimal("156.00"),
            commission=Decimal("5.00"),
        )

        # Should now have 50 shares remaining
        position = service2.get_position("AAPL")
        assert position is not None
        assert position.quantity == Decimal("50")


class TestQueryMethods:
    """Test query methods."""

    def test_get_fills_all(
        self,
        populated_service: PortfolioService,
    ) -> None:
        """Test getting all fills."""
        fills = populated_service.get_fills()

        assert len(fills) == 3  # 2 AAPL buys + 1 TSLA sell

    def test_get_fills_by_symbol(
        self,
        populated_service: PortfolioService,
    ) -> None:
        """Test filtering fills by symbol."""
        aapl_fills = populated_service.get_fills(symbol="AAPL")

        assert len(aapl_fills) == 2
        assert all(f.symbol == "AAPL" for f in aapl_fills)

    def test_get_fills_by_side(
        self,
        populated_service: PortfolioService,
    ) -> None:
        """Test filtering fills by side."""
        buys = populated_service.get_fills(side="buy")
        sells = populated_service.get_fills(side="sell")

        assert len(buys) == 2  # 2 AAPL buys
        assert len(sells) == 1  # 1 TSLA sell

    def test_get_fills_by_date_range(
        self,
        populated_service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test filtering fills by date range."""
        # Get fills before 12:30
        early_fills = populated_service.get_fills(until=timestamp.replace(hour=12, minute=30))

        # Should get first two fills (10:30 and 11:00)
        assert len(early_fills) == 2

    def test_get_all_lots_by_symbol(
        self,
        populated_service: PortfolioService,
    ) -> None:
        """Test getting lots by symbol."""
        aapl_lots = populated_service.get_all_lots("AAPL")

        assert len(aapl_lots) == 2  # Two separate buy lots
        assert all(lot.symbol == "AAPL" for lot in aapl_lots)

    def test_get_all_lots_all_symbols(
        self,
        populated_service: PortfolioService,
    ) -> None:
        """Test getting all lots across all symbols."""
        all_lots = populated_service.get_all_lots()

        assert len(all_lots) == 3  # 2 AAPL + 1 TSLA

    def test_get_all_lots_nonexistent_symbol(
        self,
        populated_service: PortfolioService,
    ) -> None:
        """Test getting lots for symbol with no position."""
        lots = populated_service.get_all_lots("MSFT")

        assert len(lots) == 0


class TestUtilityMethods:
    """Test utility methods."""

    def test_clear_positions(
        self,
        populated_service: PortfolioService,
    ) -> None:
        """Test clearing all positions."""
        # Verify we have positions
        assert len(populated_service.get_positions()) == 2

        cash_before = populated_service.get_cash()

        # Clear positions
        populated_service.clear_positions()

        # Verify positions cleared
        assert len(populated_service.get_positions()) == 0
        assert len(populated_service.get_all_lots()) == 0

        # Cash should remain unchanged
        assert populated_service.get_cash() == cash_before

    def test_validate_state_valid(
        self,
        populated_service: PortfolioService,
    ) -> None:
        """Test state validation on valid portfolio."""
        results = populated_service.validate_state()

        # All checks should pass
        assert results["position_lot_match"] is True
        assert results["realized_pnl_match"] is True
        assert results["positions_have_trackers"] is True
        assert results["no_orphaned_trackers"] is True

    def test_validate_state_after_close(
        self,
        populated_service: PortfolioService,
        timestamp: datetime,
    ) -> None:
        """Test state validation after closing position."""
        # Close partial AAPL position
        populated_service.apply_fill(
            fill_id="sell_aapl_001",
            timestamp=timestamp.replace(hour=14),
            symbol="AAPL",
            side="sell",
            quantity=Decimal("100"),
            price=Decimal("156.00"),
            commission=Decimal("5.00"),
        )

        # Validation should still pass
        results = populated_service.validate_state()
        assert all(results.values()), f"Validation failed: {results}"
