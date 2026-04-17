"""Unit tests for Phase 1 open-trade MTM synthesis.

Covers:
    T6.1 — TradeRecord status field (both values)
    T6.2 — _synthesize_open_trades() with one and multiple open positions
    T6.3 — _synthesize_open_trades() with zero open positions (no-op)
    T6.4 — _build_full_metrics() includes open_trades / realized_pnl / unrealized_pnl
    T6.5 — _trade_stats_calc does NOT include open trades
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import (
    PortfolioPosition,
    PortfolioStateEvent,
    StrategyGroup,
    TradeEvent,
)
from qs_trader.libraries.performance.models import FullMetrics, TradeRecord
from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.service import ReportingService

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ENTRY_TS = "2024-01-10T20:00:00Z"
_ENTRY_DT = datetime(2024, 1, 10, 20, 0, 0, tzinfo=timezone.utc)
_END_DT = datetime(2024, 3, 31, 20, 0, 0, tzinfo=timezone.utc)


def _build_service(tmp_path: Path) -> ReportingService:
    """Build a minimal, isolated ReportingService for unit tests."""
    config = ReportingConfig(
        write_parquet=False,
        write_json=False,
        write_html_report=False,
        write_csv_timeline=False,
        display_final_report=False,
    )
    output_dir = tmp_path / "experiments" / "test_exp" / "runs" / "20260101_000000"
    output_dir.mkdir(parents=True, exist_ok=True)
    svc = ReportingService(event_bus=EventBus(), config=config, output_dir=output_dir)
    svc._backtest_id = "test_exp"
    svc._start_datetime = _ENTRY_DT
    svc._end_datetime = _END_DT
    svc._initial_equity = Decimal("100000")
    # Silence calculator mocks
    svc._returns_calc = MagicMock()
    svc._returns_calc.returns = []
    svc._equity_calc = MagicMock()
    svc._equity_calc.latest_timestamp.return_value = _END_DT
    svc._equity_calc.latest_equity.return_value = Decimal("115000")
    svc._equity_calc.get_curve.return_value = []
    svc._drawdown_calc = MagicMock()
    svc._drawdown_calc.max_drawdown_pct = Decimal("5")
    svc._drawdown_calc.current_drawdown_pct = Decimal("0")
    svc._drawdown_calc.drawdown_periods = []
    svc._trade_stats_calc = MagicMock()
    svc._trade_stats_calc.trades = []
    svc._trade_stats_calc.total_trades = 0
    svc._trade_stats_calc.winning_trades = 0
    svc._trade_stats_calc.losing_trades = 0
    svc._trade_stats_calc.max_consecutive_wins = 0
    svc._trade_stats_calc.max_consecutive_losses = 0
    svc._period_calc = MagicMock()
    svc._period_calc.calculate_periods.return_value = []
    svc._strategy_perf_calc = MagicMock()
    svc._strategy_perf_calc.calculate_performance.return_value = []
    svc._last_portfolio_state = None
    return svc


def _make_trade_event(
    *,
    trade_id: str = "T00001",
    strategy_id: str = "sma",
    symbol: str = "AAPL",
    side: str = "long",
    entry_price: str = "100.00",
    entry_timestamp: str = _ENTRY_TS,
    current_quantity: str = "50",
    commission_total: str = "1.00",
    status: str = "open",
) -> TradeEvent:
    """Build a minimal TradeEvent for testing."""
    return TradeEvent(
        trade_id=trade_id,
        timestamp=entry_timestamp,
        strategy_id=strategy_id,
        symbol=symbol,
        status=status,
        fills=["a1b2c3d4-e5f6-7890-abcd-ef1234567890"],  # valid UUID per schema
        side=side,
        entry_price=Decimal(entry_price),
        current_quantity=Decimal(current_quantity),
        commission_total=Decimal(commission_total),
        entry_timestamp=entry_timestamp,
    )


def _make_portfolio_position(
    *,
    symbol: str = "AAPL",
    side: str = "long",
    open_quantity: int = 50,
    average_fill_price: str = "100.00",
    market_price: str = "130.00",
    unrealized_pl: str = "1500.00",
) -> PortfolioPosition:
    """Build a minimal PortfolioPosition for testing."""
    mp = Decimal(market_price)
    qty = open_quantity
    cost = Decimal(average_fill_price) * abs(qty)
    gmv = mp * qty if side == "long" else -(mp * abs(qty))
    return PortfolioPosition(
        symbol=symbol,
        side=side,
        open_quantity=open_quantity,
        average_fill_price=Decimal(average_fill_price),
        commission_paid=Decimal("1.00"),
        cost_basis=cost,
        market_price=mp,
        gross_market_value=gmv,
        unrealized_pl=Decimal(unrealized_pl),
        realized_pl=Decimal("0"),
        dividends_received=Decimal("0"),
        dividends_paid=Decimal("0"),
        total_position_value=gmv,
        currency="USD",
        last_updated=_ENTRY_TS,
    )


def _make_portfolio_state(groups: list[StrategyGroup]) -> PortfolioStateEvent:
    """Build a minimal PortfolioStateEvent via model_construct (no schema validation)."""
    return PortfolioStateEvent.model_construct(
        portfolio_id="p-001",
        start_datetime=_ENTRY_TS,
        snapshot_datetime="2024-03-31T20:00:00Z",
        reporting_currency="USD",
        initial_portfolio_equity=Decimal("100000"),
        cash_balance=Decimal("85000"),
        current_portfolio_equity=Decimal("115000"),
        total_market_value=Decimal("15000"),
        total_unrealized_pl=Decimal("1500"),
        total_realized_pl=Decimal("0"),
        total_pl=Decimal("1500"),
        long_exposure=Decimal("0.15"),
        short_exposure=Decimal("0"),
        net_exposure=Decimal("0.15"),
        gross_exposure=Decimal("0.15"),
        leverage=Decimal("0.15"),
        total_commissions_paid=Decimal("1.00"),
        strategies_groups=groups,
    )


# ---------------------------------------------------------------------------
# T6.1 — TradeRecord status field
# ---------------------------------------------------------------------------


class TestTradeRecordStatus:
    """T6.1: TradeRecord gains status field with correct defaults and literal constraints."""

    def test_trade_record_defaults_to_closed(self) -> None:
        """TradeRecord without explicit status should be 'closed' for backward compat."""
        record = TradeRecord(
            trade_id="T00001",
            strategy_id="sma",
            symbol="AAPL",
            entry_timestamp=_ENTRY_DT,
            exit_timestamp=_END_DT,
            entry_price=Decimal("100"),
            exit_price=Decimal("130"),
            quantity=50,
            side="long",
            pnl=Decimal("1500"),
            pnl_pct=Decimal("30.00"),
            commission=Decimal("1.00"),
            duration_seconds=7200,
        )
        assert record.status == "closed"

    def test_trade_record_accepts_open_status(self) -> None:
        """TradeRecord should accept status='open' for synthesized MTM trades."""
        record = TradeRecord(
            trade_id="T00001",
            strategy_id="sma",
            symbol="AAPL",
            entry_timestamp=_ENTRY_DT,
            exit_timestamp=_END_DT,
            entry_price=Decimal("100"),
            exit_price=Decimal("130"),
            quantity=50,
            side="long",
            pnl=Decimal("1500"),
            pnl_pct=Decimal("30.00"),
            commission=Decimal("1.00"),
            duration_seconds=7200,
            status="open",
        )
        assert record.status == "open"

    def test_trade_record_rejects_invalid_status(self) -> None:
        """TradeRecord should reject any status value outside the literal set."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TradeRecord(
                trade_id="T00001",
                strategy_id="sma",
                symbol="AAPL",
                entry_timestamp=_ENTRY_DT,
                exit_timestamp=_END_DT,
                entry_price=Decimal("100"),
                exit_price=Decimal("130"),
                quantity=50,
                side="long",
                pnl=Decimal("1500"),
                pnl_pct=Decimal("30.00"),
                commission=Decimal("1.00"),
                duration_seconds=7200,
                status="pending",  # type: ignore[arg-type]
            )

    def test_trade_record_is_winner_works_for_open_trades(self) -> None:
        """is_winner property should work correctly for open (MTM) trades."""
        open_winner = TradeRecord(
            trade_id="T00001",
            strategy_id="sma",
            symbol="AAPL",
            entry_timestamp=_ENTRY_DT,
            exit_timestamp=_END_DT,
            entry_price=Decimal("100"),
            exit_price=Decimal("130"),
            quantity=50,
            side="long",
            pnl=Decimal("1500"),
            pnl_pct=Decimal("30.00"),
            commission=Decimal("1.00"),
            duration_seconds=7200,
            status="open",
        )
        open_loser = TradeRecord(
            trade_id="T00002",
            strategy_id="sma",
            symbol="MSFT",
            entry_timestamp=_ENTRY_DT,
            exit_timestamp=_END_DT,
            entry_price=Decimal("200"),
            exit_price=Decimal("180"),
            quantity=10,
            side="long",
            pnl=Decimal("-200"),
            pnl_pct=Decimal("-10.00"),
            commission=Decimal("1.00"),
            duration_seconds=7200,
            status="open",
        )
        assert open_winner.is_winner is True
        assert open_loser.is_winner is False


# ---------------------------------------------------------------------------
# T6.3 — _synthesize_open_trades() with zero open positions
# ---------------------------------------------------------------------------


class TestSynthesizeOpenTradesNoOp:
    """T6.3: When no positions are open at teardown, synthesis is a no-op."""

    def test_synthesize_open_trades_empty_active_events_is_noop(self, tmp_path: Path) -> None:
        """With no active trade events, _open_trade_records should remain empty."""
        svc = _build_service(tmp_path)
        svc._active_trade_events = {}
        svc._last_portfolio_state = _make_portfolio_state([])

        svc._synthesize_open_trades()

        assert svc._open_trade_records == []

    def test_synthesize_open_trades_no_portfolio_state_is_noop(self, tmp_path: Path) -> None:
        """Without a last portfolio state, synthesis should skip gracefully."""
        svc = _build_service(tmp_path)
        svc._active_trade_events = {
            "T00001": _make_trade_event(trade_id="T00001"),
        }
        svc._last_portfolio_state = None

        svc._synthesize_open_trades()

        assert svc._open_trade_records == []

    def test_synthesize_open_trades_no_end_datetime_is_noop(self, tmp_path: Path) -> None:
        """Without _end_datetime, synthesis should skip gracefully."""
        svc = _build_service(tmp_path)
        svc._end_datetime = None
        svc._active_trade_events = {
            "T00001": _make_trade_event(trade_id="T00001"),
        }
        pos = _make_portfolio_position(symbol="AAPL")
        svc._last_portfolio_state = _make_portfolio_state(
            [StrategyGroup(strategy_id="sma", positions=[pos])]
        )

        svc._synthesize_open_trades()

        assert svc._open_trade_records == []


# ---------------------------------------------------------------------------
# T6.2 — _synthesize_open_trades() with open positions
# ---------------------------------------------------------------------------


class TestSynthesizeOpenTrades:
    """T6.2: MTM TradeRecord synthesis for one or more open positions."""

    def test_single_open_position_builds_correct_trade_record(self, tmp_path: Path) -> None:
        """A single open position should produce one TradeRecord with status='open'."""
        svc = _build_service(tmp_path)

        trade_event = _make_trade_event(
            trade_id="T00001",
            strategy_id="sma",
            symbol="AAPL",
            entry_price="100.00",
            entry_timestamp=_ENTRY_TS,
            current_quantity="50",
            commission_total="1.00",
        )
        svc._active_trade_events = {"T00001": trade_event}

        pos = _make_portfolio_position(
            symbol="AAPL",
            side="long",
            open_quantity=50,
            average_fill_price="100.00",
            market_price="130.00",
            unrealized_pl="1500.00",
        )
        svc._last_portfolio_state = _make_portfolio_state(
            [StrategyGroup(strategy_id="sma", positions=[pos])]
        )

        svc._synthesize_open_trades()

        assert len(svc._open_trade_records) == 1
        rec = svc._open_trade_records[0]
        assert rec.trade_id == "T00001"
        assert rec.strategy_id == "sma"
        assert rec.symbol == "AAPL"
        assert rec.status == "open"
        assert rec.entry_price == Decimal("100.00")
        assert rec.exit_price == Decimal("130.00")  # MTM from last bar
        assert rec.quantity == 50
        assert rec.side == "long"
        assert rec.pnl == Decimal("1500.00")
        assert rec.commission == Decimal("1.00")
        assert rec.exit_timestamp == _END_DT
        assert rec.duration_seconds > 0

    def test_multiple_open_positions_builds_all_records(self, tmp_path: Path) -> None:
        """Multiple open positions in different strategies should each get a record."""
        svc = _build_service(tmp_path)

        te1 = _make_trade_event(
            trade_id="T00001",
            strategy_id="sma",
            symbol="AAPL",
            entry_price="100.00",
            current_quantity="50",
            commission_total="1.00",
        )
        te2 = _make_trade_event(
            trade_id="T00002",
            strategy_id="mom",
            symbol="NVDA",
            entry_price="400.00",
            current_quantity="10",
            commission_total="2.00",
        )
        svc._active_trade_events = {"T00001": te1, "T00002": te2}

        pos_aapl = _make_portfolio_position(
            symbol="AAPL",
            open_quantity=50,
            market_price="130.00",
            unrealized_pl="1500.00",
        )
        pos_nvda = _make_portfolio_position(
            symbol="NVDA",
            open_quantity=10,
            average_fill_price="400.00",
            market_price="600.00",
            unrealized_pl="2000.00",
        )
        svc._last_portfolio_state = _make_portfolio_state(
            [
                StrategyGroup(strategy_id="sma", positions=[pos_aapl]),
                StrategyGroup(strategy_id="mom", positions=[pos_nvda]),
            ]
        )

        svc._synthesize_open_trades()

        assert len(svc._open_trade_records) == 2
        symbols = {r.symbol for r in svc._open_trade_records}
        assert symbols == {"AAPL", "NVDA"}
        for rec in svc._open_trade_records:
            assert rec.status == "open"

    def test_open_trade_not_in_trade_stats_calc(self, tmp_path: Path) -> None:
        """T6.5: _trade_stats_calc.trades must NOT contain synthesized open trades."""
        svc = _build_service(tmp_path)
        svc._trade_stats_calc = MagicMock()
        svc._trade_stats_calc.trades = []

        trade_event = _make_trade_event(trade_id="T00001", symbol="AAPL")
        svc._active_trade_events = {"T00001": trade_event}

        pos = _make_portfolio_position(symbol="AAPL")
        svc._last_portfolio_state = _make_portfolio_state(
            [StrategyGroup(strategy_id="sma", positions=[pos])]
        )

        svc._synthesize_open_trades()

        # Trade stats calc must be untouched
        svc._trade_stats_calc.add_trade.assert_not_called()
        assert svc._open_trade_records  # open trade went to _open_trade_records only

    def test_active_trade_events_cleared_when_trade_closes(self, tmp_path: Path) -> None:
        """A closed TradeEvent should remove the trade from _active_trade_events."""
        svc = _build_service(tmp_path)
        # Simulate: trade opens then closes
        open_event = _make_trade_event(trade_id="T00001", status="open")
        svc._active_trade_events["T00001"] = open_event

        from qs_trader.events.event_bus import EventBus

        closed_event = _make_trade_event(
            trade_id="T00001",
            status="closed",
            entry_price="100.00",
            entry_timestamp=_ENTRY_TS,
        )
        # Manually trigger the handler (the event bus is not wired in this unit test)
        from qs_trader.events.events import BaseEvent

        svc._handle_trade(closed_event)

        # Closed trade must be removed from active events
        assert "T00001" not in svc._active_trade_events

    def test_position_not_found_skips_gracefully(self, tmp_path: Path) -> None:
        """If a position is missing from the last portfolio state, skip that trade."""
        svc = _build_service(tmp_path)
        svc._active_trade_events = {
            "T00001": _make_trade_event(trade_id="T00001", symbol="MISSING"),
        }
        # Portfolio state has no positions for "MISSING"
        svc._last_portfolio_state = _make_portfolio_state([])

        svc._synthesize_open_trades()

        assert svc._open_trade_records == []

    def test_pnl_pct_calculation_is_correct(self, tmp_path: Path) -> None:
        """pnl_pct should equal (pnl / (entry_price * qty)) * 100."""
        svc = _build_service(tmp_path)
        svc._active_trade_events = {
            "T00001": _make_trade_event(
                trade_id="T00001",
                entry_price="100.00",
                current_quantity="50",
            )
        }
        pos = _make_portfolio_position(
            symbol="AAPL",
            open_quantity=50,
            average_fill_price="100.00",
            market_price="130.00",
            unrealized_pl="1500.00",
        )
        svc._last_portfolio_state = _make_portfolio_state(
            [StrategyGroup(strategy_id="sma", positions=[pos])]
        )

        svc._synthesize_open_trades()

        rec = svc._open_trade_records[0]
        expected_pct = (Decimal("1500") / (Decimal("100") * 50)) * Decimal("100")
        assert rec.pnl_pct == expected_pct


# ---------------------------------------------------------------------------
# T6.4 — _build_full_metrics() includes new fields
# ---------------------------------------------------------------------------


class TestBuildFullMetricsNewFields:
    """T6.4: _build_full_metrics() should populate open_trades, realized_pnl, unrealized_pnl."""

    def _build_service_with_trades(
        self, tmp_path: Path, closed_pnls: list[str], open_pnls: list[str]
    ) -> ReportingService:
        svc = _build_service(tmp_path)

        # Closed trades via _trade_stats_calc.trades
        closed_records = [
            TradeRecord(
                trade_id=f"T{i:05d}",
                strategy_id="sma",
                symbol="AAPL",
                entry_timestamp=_ENTRY_DT,
                exit_timestamp=_END_DT,
                entry_price=Decimal("100"),
                exit_price=Decimal("110"),
                quantity=10,
                side="long",
                pnl=Decimal(p),
                pnl_pct=Decimal("10"),
                commission=Decimal("1"),
                duration_seconds=86400,
                status="closed",
            )
            for i, p in enumerate(closed_pnls)
        ]
        svc._trade_stats_calc = MagicMock()
        svc._trade_stats_calc.trades = closed_records
        svc._trade_stats_calc.total_trades = len(closed_records)
        svc._trade_stats_calc.winning_trades = sum(1 for r in closed_records if r.pnl > 0)
        svc._trade_stats_calc.losing_trades = sum(1 for r in closed_records if r.pnl <= 0)
        svc._trade_stats_calc.max_consecutive_wins = 0
        svc._trade_stats_calc.max_consecutive_losses = 0

        # Open trades via _open_trade_records
        svc._open_trade_records = [
            TradeRecord(
                trade_id=f"O{i:05d}",
                strategy_id="sma",
                symbol="NVDA",
                entry_timestamp=_ENTRY_DT,
                exit_timestamp=_END_DT,
                entry_price=Decimal("400"),
                exit_price=Decimal("430"),
                quantity=10,
                side="long",
                pnl=Decimal(p),
                pnl_pct=Decimal("7.5"),
                commission=Decimal("1"),
                duration_seconds=86400,
                status="open",
            )
            for i, p in enumerate(open_pnls)
        ]
        return svc

    def test_full_metrics_open_trades_count(self, tmp_path: Path) -> None:
        """open_trades should equal the number of synthesized open TradeRecords."""
        svc = self._build_service_with_trades(
            tmp_path,
            closed_pnls=["500", "300"],
            open_pnls=["1500"],
        )
        final_equity = Decimal("115000")
        total_return_pct = Decimal("15.00")

        metrics = svc._build_full_metrics(final_equity, total_return_pct)

        assert metrics.open_trades == 1

    def test_full_metrics_realized_pnl_is_sum_of_closed_trades(self, tmp_path: Path) -> None:
        """realized_pnl should be the sum of pnl from all closed trades."""
        svc = self._build_service_with_trades(
            tmp_path,
            closed_pnls=["500", "300"],
            open_pnls=["1500"],
        )
        metrics = svc._build_full_metrics(Decimal("115000"), Decimal("15.00"))

        assert metrics.realized_pnl == Decimal("800")

    def test_full_metrics_unrealized_pnl_is_sum_of_open_trades(self, tmp_path: Path) -> None:
        """unrealized_pnl should be the sum of pnl from all synthesized open trades."""
        svc = self._build_service_with_trades(
            tmp_path,
            closed_pnls=["500"],
            open_pnls=["1500", "500"],
        )
        metrics = svc._build_full_metrics(Decimal("115000"), Decimal("15.00"))

        assert metrics.unrealized_pnl == Decimal("2000")

    def test_full_metrics_defaults_zero_when_no_open_trades(self, tmp_path: Path) -> None:
        """When no open trades, open_trades=0 and unrealized_pnl=0."""
        svc = self._build_service_with_trades(
            tmp_path,
            closed_pnls=["500"],
            open_pnls=[],
        )
        metrics = svc._build_full_metrics(Decimal("115000"), Decimal("15.00"))

        assert metrics.open_trades == 0
        assert metrics.unrealized_pnl == Decimal("0")

    def test_full_metrics_defaults_zero_when_no_closed_trades(self, tmp_path: Path) -> None:
        """When no closed trades, realized_pnl=0."""
        svc = self._build_service_with_trades(
            tmp_path,
            closed_pnls=[],
            open_pnls=["1500"],
        )
        metrics = svc._build_full_metrics(Decimal("115000"), Decimal("15.00"))

        assert metrics.realized_pnl == Decimal("0")


# ---------------------------------------------------------------------------
# T6.5 — _trade_stats_calc stays closed-trades only
# ---------------------------------------------------------------------------


class TestTradeStatsCalcClosedOnly:
    """T6.5: Verifies _trade_stats_calc never receives open-trade events."""

    def test_handle_trade_open_does_not_add_to_stats_calc(self, tmp_path: Path) -> None:
        """A TradeEvent(status='open') must not invoke _trade_stats_calc.add_trade."""
        svc = _build_service(tmp_path)
        real_stats_calc = MagicMock()
        svc._trade_stats_calc = real_stats_calc

        open_event = _make_trade_event(trade_id="T00001", status="open")
        svc._handle_trade(open_event)

        real_stats_calc.add_trade.assert_not_called()
        assert "T00001" in svc._active_trade_events

    def test_synthesize_open_trades_does_not_add_to_stats_calc(self, tmp_path: Path) -> None:
        """_synthesize_open_trades must not invoke _trade_stats_calc.add_trade."""
        svc = _build_service(tmp_path)
        real_stats_calc = MagicMock()
        svc._trade_stats_calc = real_stats_calc
        svc._trade_stats_calc.trades = []

        svc._active_trade_events = {
            "T00001": _make_trade_event(trade_id="T00001", symbol="AAPL"),
        }
        pos = _make_portfolio_position(symbol="AAPL")
        svc._last_portfolio_state = _make_portfolio_state(
            [StrategyGroup(strategy_id="sma", positions=[pos])]
        )

        svc._synthesize_open_trades()

        real_stats_calc.add_trade.assert_not_called()
        assert len(svc._open_trade_records) == 1
