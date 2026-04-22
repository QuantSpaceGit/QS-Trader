"""Unit tests for the shared lifecycle-intent projection."""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.events.events import FillEvent
from qs_trader.events.lifecycle_events import OrderIntentEvent, OrderLifecycleEvent
from qs_trader.services.manager.lifecycle_intent_projection import LifecycleIntentProjection
from qs_trader.services.strategy.models import PositionState

_OCCURRED_AT = datetime(2024, 1, 2, 14, 30, tzinfo=timezone.utc)
_PRICE_BASIS = "adjusted_ohlc_adj_columns"


def _make_intent(
    *,
    intent_id: str,
    intent_type: str = "open",
    intent_state: str = "accepted",
    direction: str = "long",
) -> OrderIntentEvent:
    """Create a schema-valid canonical intent event for projection tests."""
    return OrderIntentEvent(
        experiment_id="exp",
        run_id="run-001",
        occurred_at=_OCCURRED_AT,
        intent_id=intent_id,
        strategy_id="sma_crossover",
        symbol="AAPL",
        intent_type=intent_type,
        intent_state=intent_state,
        direction=direction,
        target_quantity=Decimal("100"),
        price_basis=_PRICE_BASIS,
        source_service="manager_service",
        correlation_id="550e8400-e29b-41d4-a716-446655440201",
    )


def _make_order_lifecycle(
    *,
    intent_id: str,
    order_state: str,
    side: str,
    filled_quantity: str,
) -> OrderLifecycleEvent:
    """Create a schema-valid order lifecycle transition for projection tests."""
    return OrderLifecycleEvent(
        experiment_id="exp",
        run_id="run-001",
        occurred_at=_OCCURRED_AT,
        order_id="550e8400-e29b-41d4-a716-446655440202",
        intent_id=intent_id,
        strategy_id="sma_crossover",
        symbol="AAPL",
        order_state=order_state,
        side=side,
        quantity=Decimal("100"),
        filled_quantity=Decimal(filled_quantity),
        order_type="market",
        time_in_force="GTC",
        price_basis=_PRICE_BASIS,
        idempotency_key="order-key-001",
        source_service="execution_service",
        correlation_id="550e8400-e29b-41d4-a716-446655440201",
    )


class TestLifecycleIntentProjection:
    """Projection state transition coverage."""

    def test_pending_open_transitions_to_open_after_fill_completion(self) -> None:
        """Accepted opens should surface as pending until realized quantity lands."""
        projection = LifecycleIntentProjection()
        projection.apply_order_intent(_make_intent(intent_id="550e8400-e29b-41d4-a716-446655440203"))

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.PENDING_OPEN_LONG
        assert projection.get_same_side_open_suppression_reason("sma_crossover", "AAPL", "long") == (
            "duplicate_same_side_pending"
        )

        projection.apply_order_lifecycle(
            _make_order_lifecycle(
                intent_id="550e8400-e29b-41d4-a716-446655440203",
                order_state="filled",
                side="buy",
                filled_quantity="100",
            )
        )
        projection.sync_position_quantity("sma_crossover", "AAPL", Decimal("100"))

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.OPEN_LONG
        assert projection.get_same_side_open_suppression_reason("sma_crossover", "AAPL", "long") == (
            "duplicate_open_without_scale_in"
        )

    @pytest.mark.parametrize("terminal_state", ["rejected", "cancelled"])
    def test_pending_open_rolls_back_to_flat_on_zero_fill_terminal_state(self, terminal_state: str) -> None:
        """Rejected or cancelled zero-fill opens should clear pending state."""
        projection = LifecycleIntentProjection()
        intent_id = "550e8400-e29b-41d4-a716-446655440204"
        projection.apply_order_intent(_make_intent(intent_id=intent_id))

        projection.apply_order_lifecycle(
            _make_order_lifecycle(
                intent_id=intent_id,
                order_state=terminal_state,
                side="buy",
                filled_quantity="0",
            )
        )

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.FLAT
        assert projection.has_same_side_pending_or_open("sma_crossover", "AAPL", "long") is False

    def test_pending_close_rolls_back_to_open_after_cancellation(self) -> None:
        """Cancelled closes should restore the realized open state."""
        projection = LifecycleIntentProjection()
        projection.sync_position_quantity("sma_crossover", "AAPL", Decimal("100"))
        projection.apply_order_intent(
            _make_intent(
                intent_id="550e8400-e29b-41d4-a716-446655440205",
                intent_type="close",
                direction="long",
            )
        )

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.PENDING_CLOSE_LONG

        projection.apply_order_lifecycle(
            _make_order_lifecycle(
                intent_id="550e8400-e29b-41d4-a716-446655440205",
                order_state="cancelled",
                side="sell",
                filled_quantity="0",
            )
        )

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.OPEN_LONG

    def test_pending_scale_in_blocks_follow_on_same_side_open_without_hiding_realized_state(self) -> None:
        """Accepted scale-ins should keep open state while blocking additional same-side opens."""
        projection = LifecycleIntentProjection()
        intent_id = "550e8400-e29b-41d4-a716-446655440205"
        projection.sync_position_quantity("sma_crossover", "AAPL", Decimal("100"))
        projection.apply_order_intent(
            _make_intent(
                intent_id=intent_id,
                intent_type="scale_in",
                direction="long",
            )
        )

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.OPEN_LONG
        assert projection.get_same_side_open_suppression_reason("sma_crossover", "AAPL", "long") == (
            "duplicate_same_side_pending"
        )

        projection.apply_order_lifecycle(
            _make_order_lifecycle(
                intent_id=intent_id,
                order_state="filled",
                side="buy",
                filled_quantity="50",
            )
        )
        projection.sync_position_quantity("sma_crossover", "AAPL", Decimal("150"))

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.OPEN_LONG
        assert projection.get_same_side_open_suppression_reason("sma_crossover", "AAPL", "long") == (
            "duplicate_open_without_scale_in"
        )

    def test_fill_events_update_realized_quantity_for_shared_projection(self) -> None:
        """The projection should track realized state directly from execution fills."""
        projection = LifecycleIntentProjection()

        projection.apply_fill(
            FillEvent(
                fill_id="550e8400-e29b-41d4-a716-446655440206",
                source_order_id="550e8400-e29b-41d4-a716-446655440207",
                timestamp="2024-01-02T14:30:00Z",
                symbol="AAPL",
                side="buy",
                filled_quantity=Decimal("25"),
                fill_price=Decimal("100.00"),
                strategy_id="sma_crossover",
            )
        )

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.OPEN_LONG

        projection.apply_fill(
            FillEvent(
                fill_id="550e8400-e29b-41d4-a716-446655440208",
                source_order_id="550e8400-e29b-41d4-a716-446655440209",
                timestamp="2024-01-02T14:31:00Z",
                symbol="AAPL",
                side="sell",
                filled_quantity=Decimal("25"),
                fill_price=Decimal("101.00"),
                strategy_id="sma_crossover",
            )
        )

        assert projection.get_position_state("sma_crossover", "AAPL") == PositionState.FLAT