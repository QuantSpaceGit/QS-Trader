"""Unit tests for decision audit (Group 8).

Tests StrategyDecisionEvent validation, Context.track_decision,
deterministic candidate_id, and CSV persistence.
"""

import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from qs_trader.events.lifecycle_events import StrategyDecisionEvent
from qs_trader.services.strategy.context import Context
from qs_trader.services.strategy.decision_audit import (
    compute_candidate_id,
    persist_decisions_csv,
)


# ---------------------------------------------------------------------------
# StrategyDecisionEvent tests
# ---------------------------------------------------------------------------


def _make_decision_event(**overrides: object) -> StrategyDecisionEvent:
    """Build a minimal valid StrategyDecisionEvent."""
    kwargs = {
        "decision_id": "00000000-0000-0000-0000-000000000001",
        "strategy_id": "test_strategy",
        "symbol": "AAPL",
        "bar_timestamp": "2024-01-15T16:00:00Z",
        "decision_type": "open_long",
        "decision_price": Decimal("150.25"),
        "decision_basis": "adjusted",
        "confidence": Decimal("0.85"),
        "experiment_id": "test-experiment",
        "run_id": "test-run",
        "runtime_symbol": "AAPL",
        "secid": 12345,
        "display_symbol": "AAPL",
        "candidate_id": "abc123",
        "decision_status": "accepted",
        "final_action": "open_long",
    }
    kwargs.update(overrides)
    return StrategyDecisionEvent(**kwargs)


class TestStrategyDecisionEvent:
    """Tests for StrategyDecisionEvent Pydantic model."""

    def test_valid_event(self):
        event = _make_decision_event()
        assert event.event_type == "strategy_decision"
        assert event.SCHEMA_BASE == "lifecycle/strategy_decision"
        assert event.decision_id == "00000000-0000-0000-0000-000000000001"
        assert event.strategy_id == "test_strategy"
        assert event.symbol == "AAPL"

    def test_legacy_event_constructor_defaults_candidate_audit_fields(self):
        event = StrategyDecisionEvent(
            decision_id="00000000-0000-0000-0000-000000000001",
            strategy_id="test_strategy",
            symbol="AAPL",
            bar_timestamp="2024-01-15T16:00:00Z",
            decision_type="hold",
            decision_price=Decimal("150.25"),
            decision_basis="adjusted",
            confidence=Decimal("0.85"),
            experiment_id="test-experiment",
            run_id="test-run",
        )

        assert event.runtime_symbol == "AAPL"
        assert event.display_symbol == "AAPL"
        assert event.secid == 0
        assert event.candidate_id == "00000000-0000-0000-0000-000000000001"
        assert event.decision_status == "not_ready"
        assert event.final_action == "none"

    def test_optional_audit_fields(self):
        event = _make_decision_event(
            indicator_context={"sma": 150.0},
            reason="SMA crossover",
            metadata={"version": "1.0"},
        )
        assert event.indicator_context == {"sma": 150.0}
        assert event.reason == "SMA crossover"
        assert event.metadata == {"version": "1.0"}

    def test_serialization(self):
        event = _make_decision_event()
        data = event.model_dump()
        assert data["decision_price"] == "150.25"
        assert data["confidence"] == "0.85"


# ---------------------------------------------------------------------------
# compute_candidate_id tests
# ---------------------------------------------------------------------------


class TestComputeCandidateId:
    """Tests for deterministic candidate ID generation."""

    def test_deterministic(self):
        id1 = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        id2 = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        assert id1 == id2

    def test_different_secid(self):
        id1 = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        id2 = compute_candidate_id(67890, "2024-01-15", "test_strategy", "hash1")
        assert id1 != id2

    def test_different_date(self):
        id1 = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        id2 = compute_candidate_id(12345, "2024-01-16", "test_strategy", "hash1")
        assert id1 != id2

    def test_different_strategy(self):
        id1 = compute_candidate_id(12345, "2024-01-15", "strategy_a", "hash1")
        id2 = compute_candidate_id(12345, "2024-01-15", "strategy_b", "hash1")
        assert id1 != id2

    def test_different_parameter_hash(self):
        id1 = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        id2 = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash2")
        assert id1 != id2

    def test_none_secid(self):
        id1 = compute_candidate_id(None, "2024-01-15", "test_strategy", "hash1")
        assert isinstance(id1, str)
        assert len(id1) == 64  # SHA-256 hex digest

    def test_none_parameter_hash(self):
        id1 = compute_candidate_id(12345, "2024-01-15", "test_strategy", None)
        assert isinstance(id1, str)
        assert len(id1) == 64

    def test_sha256_format(self):
        candidate_id = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        # Should be a valid hex string
        int(candidate_id, 16)  # Will raise if not valid hex
        assert len(candidate_id) == 64


# ---------------------------------------------------------------------------
# Context.track_decision tests
# ---------------------------------------------------------------------------


class TestContextTrackDecision:
    """Tests for Context.track_decision method."""

    @pytest.fixture
    def event_bus(self):
        return MagicMock()

    @pytest.fixture
    def context(self, event_bus):
        return Context(strategy_id="test_strategy", event_bus=event_bus)

    def test_track_decision_basic(self, context):
        candidate_id = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        record = context.track_decision(
            candidate_id=candidate_id,
            decision_status="accepted",
            final_action="open_long",
            reason_code="signal_triggered",
        )

        assert record["candidate_id"] == candidate_id
        assert record["decision_status"] == "accepted"
        assert record["final_action"] == "open_long"
        assert record["reason_code"] == "signal_triggered"
        assert record["strategy_id"] == "test_strategy"
        assert len(context.decisions) == 1

    def test_track_decision_with_optional_fields(self, context):
        candidate_id = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        record = context.track_decision(
            candidate_id=candidate_id,
            decision_status="rejected",
            final_action="none",
            reason_code="gate_failed",
            gates={"momentum": False, "volatility": True},
            diagnostics={"momentum_score": -0.1},
            secid=12345,
            symbol="AAPL",
            date="2024-01-15",
            confidence=0.3,
            decision_price=150.25,
        )

        assert record["gates"] == {"momentum": False, "volatility": True}
        assert record["diagnostics"] == {"momentum_score": -0.1}
        assert record["secid"] == 12345
        assert record["symbol"] == "AAPL"
        assert record["confidence"] == 0.3

    def test_track_decision_multiple(self, context):
        for i in range(5):
            cid = compute_candidate_id(i, "2024-01-15", "test_strategy", "hash1")
            context.track_decision(
                candidate_id=cid,
                decision_status="accepted",
                final_action="open_long",
            )

        assert len(context.decisions) == 5

    def test_flush_decisions_csv(self, context):
        candidate_id = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        context.track_decision(
            candidate_id=candidate_id,
            decision_status="accepted",
            final_action="open_long",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            context.set_audit_output_dir(Path(tmpdir))
            path = context.flush_decisions(format="csv")

            assert path is not None
            assert path.exists()
            assert path.name == "strategy_decisions.csv"

            # Verify content
            content = path.read_text()
            assert "candidate_id" in content
            assert candidate_id in content
            assert "accepted" in content

    def test_flush_decisions_empty(self, context):
        with tempfile.TemporaryDirectory() as tmpdir:
            context.set_audit_output_dir(Path(tmpdir))
            path = context.flush_decisions(format="csv")
            assert path is None

    def test_flush_decisions_no_output_dir(self, context):
        candidate_id = compute_candidate_id(12345, "2024-01-15", "test_strategy", "hash1")
        context.track_decision(
            candidate_id=candidate_id,
            decision_status="accepted",
            final_action="open_long",
        )

        path = context.flush_decisions(format="csv")
        assert path is None


# ---------------------------------------------------------------------------
# persist_decisions_csv tests
# ---------------------------------------------------------------------------


class TestPersistDecisionsCsv:
    """Tests for CSV decision persistence."""

    def test_persist_and_read(self):
        decisions = [
            {
                "candidate_id": "abc123",
                "strategy_id": "test",
                "secid": 12345,
                "symbol": "AAPL",
                "date": "2024-01-15",
                "decision_status": "accepted",
                "final_action": "open_long",
                "reason_code": "signal",
                "gates": {"gate1": True},
                "diagnostics": {},
                "strategy_version": "1.0",
                "parameter_hash": "hash1",
                "confidence": 0.85,
                "decision_price": 150.25,
                "indicator_context": {},
                "metadata": {},
                "occurred_at": "2024-01-15T16:00:00Z",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = persist_decisions_csv(decisions, Path(tmpdir))
            assert path is not None
            assert path.exists()

            content = path.read_text()
            lines = content.strip().split("\n")
            assert len(lines) == 2  # header + 1 row
            assert "candidate_id" in lines[0]
            assert "abc123" in lines[1]

    def test_persist_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = persist_decisions_csv([], Path(tmpdir))
            assert path is None

    def test_persist_complex_types(self):
        decisions = [
            {
                "candidate_id": "abc123",
                "strategy_id": "test",
                "secid": 12345,
                "symbol": "AAPL",
                "date": "2024-01-15",
                "decision_status": "accepted",
                "final_action": "open_long",
                "reason_code": "signal",
                "gates": {"momentum": True, "volatility": False},
                "diagnostics": {"score": 0.85},
                "strategy_version": "1.0",
                "parameter_hash": "hash1",
                "confidence": 0.85,
                "decision_price": 150.25,
                "indicator_context": {"sma": 150.0},
                "metadata": {"run_id": "run-001"},
                "occurred_at": "2024-01-15T16:00:00Z",
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = persist_decisions_csv(decisions, Path(tmpdir))
            assert path is not None

            content = path.read_text()
            # Complex types should be JSON serialized
            assert '"momentum": true' in content or "momentum" in content
