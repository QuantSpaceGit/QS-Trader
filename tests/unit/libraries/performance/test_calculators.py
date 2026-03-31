"""Unit tests for performance calculators.

Tests PeriodAggregationCalculator and StrategyPerformanceCalculator classes.
"""

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from qs_trader.libraries.performance.calculators import PeriodAggregationCalculator, StrategyPerformanceCalculator
from qs_trader.libraries.performance.models import PeriodMetrics, StrategyPerformance, TradeRecord


class TestPeriodAggregationCalculator:
    """Test PeriodAggregationCalculator functionality."""

    @pytest.fixture
    def calculator(self) -> PeriodAggregationCalculator:
        """Create calculator instance."""
        return PeriodAggregationCalculator()

    @pytest.fixture
    def sample_trade(self) -> TradeRecord:
        """Create a sample trade for testing."""
        return TradeRecord(
            trade_id="T001",
            strategy_id="S001",
            symbol="AAPL",
            entry_timestamp=datetime(2025, 1, 5, 9, 30, tzinfo=timezone.utc),
            exit_timestamp=datetime(2025, 1, 15, 16, 0, tzinfo=timezone.utc),
            entry_price=Decimal("150.00"),
            exit_price=Decimal("155.00"),
            quantity=100,
            side="long",
            pnl=Decimal("500.00"),
            pnl_pct=Decimal("3.33"),
            commission=Decimal("2.00"),
            duration_seconds=864000,
        )

    def test_calculate_periods_empty_data_returns_empty_list(self, calculator):
        """Test that calculate_periods with no data returns empty list."""
        # Arrange
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        assert result == []

    def test_calculate_periods_single_month_single_point(self, calculator):
        """Test period calculation with single month and single equity point."""
        # Arrange
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("105000"))
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        assert len(result) == 1
        period = result[0]
        assert isinstance(period, PeriodMetrics)
        assert period.period == "2025-01"
        assert period.period_type == "monthly"
        assert period.start_equity == Decimal("100000")
        assert period.end_equity == Decimal("105000")
        assert period.return_pct == Decimal("5.00")
        assert period.num_trades == 0

    def test_calculate_periods_multiple_months_tracks_equity(self, calculator):
        """Test period calculation across multiple months."""
        # Arrange
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("105000"))
        calculator.update(datetime(2025, 2, 15, tzinfo=timezone.utc), Decimal("110000"))
        calculator.update(datetime(2025, 3, 15, tzinfo=timezone.utc), Decimal("108000"))
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        assert len(result) == 3

        # January: 100k -> 105k = +5%
        jan = result[0]
        assert jan.period == "2025-01"
        assert jan.start_equity == Decimal("100000")
        assert jan.end_equity == Decimal("105000")
        assert jan.return_pct == Decimal("5.00")

        # February: 105k -> 110k = +4.76%
        feb = result[1]
        assert feb.period == "2025-02"
        assert feb.start_equity == Decimal("105000")
        assert feb.end_equity == Decimal("110000")
        assert feb.return_pct == Decimal("4.76")

        # March: 110k -> 108k = -1.82%
        mar = result[2]
        assert mar.period == "2025-03"
        assert mar.start_equity == Decimal("110000")
        assert mar.end_equity == Decimal("108000")
        assert mar.return_pct == Decimal("-1.82")

    def test_calculate_periods_quarterly_aggregation(self, calculator):
        """Test quarterly period aggregation."""
        # Arrange
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("105000"))
        calculator.update(datetime(2025, 2, 15, tzinfo=timezone.utc), Decimal("107000"))
        calculator.update(datetime(2025, 3, 15, tzinfo=timezone.utc), Decimal("110000"))
        calculator.update(datetime(2025, 4, 15, tzinfo=timezone.utc), Decimal("112000"))
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("quarterly", start_equity)

        # Assert
        assert len(result) == 2

        # Q1: Jan, Feb, Mar
        q1 = result[0]
        assert q1.period == "2025-Q1"
        assert q1.period_type == "quarterly"
        assert q1.start_equity == Decimal("100000")
        assert q1.end_equity == Decimal("110000")  # Last point in Q1
        assert q1.return_pct == Decimal("10.00")

        # Q2: Apr
        q2 = result[1]
        assert q2.period == "2025-Q2"
        assert q2.start_equity == Decimal("110000")
        assert q2.end_equity == Decimal("112000")
        assert q2.return_pct == Decimal("1.82")

    def test_calculate_periods_annual_aggregation(self, calculator):
        """Test annual period aggregation."""
        # Arrange
        calculator.update(datetime(2024, 6, 15, tzinfo=timezone.utc), Decimal("105000"))
        calculator.update(datetime(2024, 12, 15, tzinfo=timezone.utc), Decimal("110000"))
        calculator.update(datetime(2025, 6, 15, tzinfo=timezone.utc), Decimal("115000"))
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("annual", start_equity)

        # Assert
        assert len(result) == 2

        # 2024
        y2024 = result[0]
        assert y2024.period == "2024"
        assert y2024.period_type == "annual"
        assert y2024.start_equity == Decimal("100000")
        assert y2024.end_equity == Decimal("110000")
        assert y2024.return_pct == Decimal("10.00")

        # 2025
        y2025 = result[1]
        assert y2025.period == "2025"
        assert y2025.start_equity == Decimal("110000")
        assert y2025.end_equity == Decimal("115000")
        assert y2025.return_pct == Decimal("4.55")

    def test_calculate_periods_trades_assigned_to_correct_period(self, calculator, sample_trade):
        """Test that trades are assigned to periods based on exit timestamp."""
        # Arrange
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("105000"))
        calculator.update(datetime(2025, 2, 15, tzinfo=timezone.utc), Decimal("110000"))

        # Trade exits in January
        calculator.add_trade(sample_trade)

        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        assert len(result) == 2

        # January should have the trade
        jan = result[0]
        assert jan.num_trades == 1
        assert jan.winning_trades == 1
        assert jan.losing_trades == 0
        assert jan.best_trade_pct == Decimal("3.33")
        assert jan.worst_trade_pct == Decimal("3.33")

        # February should have no trades
        feb = result[1]
        assert feb.num_trades == 0
        assert feb.winning_trades == 0
        assert feb.losing_trades == 0

    def test_calculate_periods_trade_statistics_multiple_trades(self, calculator):
        """Test trade statistics with multiple winning and losing trades."""
        # Arrange
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("105000"))

        # Add winning and losing trades
        winning_trade = TradeRecord(
            trade_id="T001",
            strategy_id="S001",
            symbol="AAPL",
            entry_timestamp=datetime(2025, 1, 5, tzinfo=timezone.utc),
            exit_timestamp=datetime(2025, 1, 10, tzinfo=timezone.utc),
            entry_price=Decimal("150.00"),
            exit_price=Decimal("155.00"),
            quantity=100,
            side="long",
            pnl=Decimal("500.00"),
            pnl_pct=Decimal("3.33"),
            commission=Decimal("2.00"),
            duration_seconds=432000,
        )

        losing_trade = TradeRecord(
            trade_id="T002",
            strategy_id="S001",
            symbol="MSFT",
            entry_timestamp=datetime(2025, 1, 12, tzinfo=timezone.utc),
            exit_timestamp=datetime(2025, 1, 15, tzinfo=timezone.utc),
            entry_price=Decimal("300.00"),
            exit_price=Decimal("295.00"),
            quantity=50,
            side="long",
            pnl=Decimal("-250.00"),
            pnl_pct=Decimal("-1.67"),
            commission=Decimal("2.00"),
            duration_seconds=259200,
        )

        calculator.add_trade(winning_trade)
        calculator.add_trade(losing_trade)

        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        jan = result[0]
        assert jan.num_trades == 2
        assert jan.winning_trades == 1
        assert jan.losing_trades == 1
        assert jan.best_trade_pct == Decimal("3.33")
        assert jan.worst_trade_pct == Decimal("-1.67")

    def test_calculate_periods_zero_start_equity_returns_zero_return(self, calculator):
        """Test that zero start equity returns 0% return (edge case)."""
        # Arrange
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("100000"))
        start_equity = Decimal("0")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        assert len(result) == 1
        assert result[0].return_pct == Decimal("0")

    def test_calculate_periods_negative_returns(self, calculator):
        """Test period with negative returns."""
        # Arrange
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("95000"))
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        assert len(result) == 1
        assert result[0].return_pct == Decimal("-5.00")
        assert result[0].start_equity == Decimal("100000")
        assert result[0].end_equity == Decimal("95000")

    def test_get_period_key_monthly_format(self, calculator):
        """Test period key generation for monthly aggregation."""
        # Arrange
        timestamp = datetime(2025, 3, 15, tzinfo=timezone.utc)

        # Act
        result = calculator._get_period_key(timestamp, "monthly")

        # Assert
        assert result == "2025-03"

    def test_get_period_key_quarterly_format(self, calculator):
        """Test period key generation for quarterly aggregation."""
        # Arrange & Act & Assert
        assert calculator._get_period_key(datetime(2025, 1, 15, tzinfo=timezone.utc), "quarterly") == "2025-Q1"
        assert calculator._get_period_key(datetime(2025, 3, 31, tzinfo=timezone.utc), "quarterly") == "2025-Q1"
        assert calculator._get_period_key(datetime(2025, 4, 1, tzinfo=timezone.utc), "quarterly") == "2025-Q2"
        assert calculator._get_period_key(datetime(2025, 6, 30, tzinfo=timezone.utc), "quarterly") == "2025-Q2"
        assert calculator._get_period_key(datetime(2025, 7, 1, tzinfo=timezone.utc), "quarterly") == "2025-Q3"
        assert calculator._get_period_key(datetime(2025, 10, 1, tzinfo=timezone.utc), "quarterly") == "2025-Q4"

    def test_get_period_key_annual_format(self, calculator):
        """Test period key generation for annual aggregation."""
        # Arrange
        timestamp = datetime(2025, 6, 15, tzinfo=timezone.utc)

        # Act
        result = calculator._get_period_key(timestamp, "annual")

        # Assert
        assert result == "2025"

    def test_get_period_key_invalid_period_type_raises_error(self, calculator):
        """Test that invalid period type raises ValueError."""
        # Arrange
        timestamp = datetime(2025, 1, 15, tzinfo=timezone.utc)

        # Act & Assert
        with pytest.raises(ValueError, match="Invalid period_type: weekly"):
            calculator._get_period_key(timestamp, "weekly")

    def test_calculate_periods_period_sorting(self, calculator):
        """Test that periods are returned in chronological order."""
        # Arrange - Add data out of order
        calculator.update(datetime(2025, 3, 15, tzinfo=timezone.utc), Decimal("110000"))
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("105000"))
        calculator.update(datetime(2025, 2, 15, tzinfo=timezone.utc), Decimal("107000"))
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert - Should be sorted chronologically
        assert len(result) == 3
        assert result[0].period == "2025-01"
        assert result[1].period == "2025-02"
        assert result[2].period == "2025-03"

    def test_calculate_periods_multiple_points_in_same_period(self, calculator):
        """Test that multiple equity points in same period use last point."""
        # Arrange
        calculator.update(datetime(2025, 1, 5, tzinfo=timezone.utc), Decimal("102000"))
        calculator.update(datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("105000"))
        calculator.update(datetime(2025, 1, 25, tzinfo=timezone.utc), Decimal("107000"))
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        assert len(result) == 1
        jan = result[0]
        assert jan.end_equity == Decimal("107000")  # Last point in period
        assert jan.return_pct == Decimal("7.00")

    def test_calculate_periods_start_and_end_dates(self, calculator):
        """Test that period start/end dates are correctly captured."""
        # Arrange
        calculator.update(datetime(2025, 1, 5, tzinfo=timezone.utc), Decimal("102000"))
        calculator.update(datetime(2025, 1, 25, tzinfo=timezone.utc), Decimal("105000"))
        start_equity = Decimal("100000")

        # Act
        result = calculator.calculate_periods("monthly", start_equity)

        # Assert
        jan = result[0]
        assert jan.start_date == "2025-01-05"
        assert jan.end_date == "2025-01-25"


class TestStrategyPerformanceCalculator:
    """Test StrategyPerformanceCalculator functionality."""

    @pytest.fixture
    def calculator(self) -> StrategyPerformanceCalculator:
        """Create calculator instance with sample strategies."""
        return StrategyPerformanceCalculator(["strategy_a", "strategy_b"])

    @pytest.fixture
    def sample_trade(self) -> TradeRecord:
        """Create a sample trade for testing."""
        return TradeRecord(
            trade_id="T001",
            strategy_id="strategy_a",
            symbol="AAPL",
            entry_timestamp=datetime(2025, 1, 5, 9, 30, tzinfo=timezone.utc),
            exit_timestamp=datetime(2025, 1, 15, 16, 0, tzinfo=timezone.utc),
            entry_price=Decimal("150.00"),
            exit_price=Decimal("155.00"),
            quantity=100,
            side="long",
            pnl=Decimal("500.00"),
            pnl_pct=Decimal("3.33"),
            commission=Decimal("2.00"),
            duration_seconds=864000,
        )

    def test_calculate_performance_no_data_returns_empty_list(self, calculator):
        """Test that calculate_performance with no data returns empty list."""
        # Act
        result = calculator.calculate_performance()

        # Assert
        assert result == []

    def test_calculate_performance_single_strategy_basic_metrics(self, calculator):
        """Test basic performance metrics for single strategy."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("50000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("52500"))

        # Act
        result = calculator.calculate_performance()

        # Assert
        assert len(result) == 1
        perf = result[0]
        assert isinstance(perf, StrategyPerformance)
        assert perf.strategy_id == "strategy_a"
        assert perf.equity_allocated == Decimal("50000")
        assert perf.final_equity == Decimal("52500")
        assert perf.return_pct == Decimal("5.00")
        assert perf.total_trades == 0

    def test_calculate_performance_multiple_strategies_isolation(self, calculator):
        """Test that multiple strategies are tracked independently."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("50000"))
        calculator.update_allocation("strategy_b", Decimal("50000"))

        calculator.update_equity("strategy_a", datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("55000"))
        calculator.update_equity("strategy_b", datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("48000"))

        # Act
        result = calculator.calculate_performance()

        # Assert
        assert len(result) == 2

        # Strategy A: +10%
        perf_a = next(p for p in result if p.strategy_id == "strategy_a")
        assert perf_a.equity_allocated == Decimal("50000")
        assert perf_a.final_equity == Decimal("55000")
        assert perf_a.return_pct == Decimal("10.00")

        # Strategy B: -4%
        perf_b = next(p for p in result if p.strategy_id == "strategy_b")
        assert perf_b.equity_allocated == Decimal("50000")
        assert perf_b.final_equity == Decimal("48000")
        assert perf_b.return_pct == Decimal("-4.00")

    def test_calculate_performance_trade_attribution(self, calculator, sample_trade):
        """Test that trades are correctly attributed to strategies."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("50000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("50500"))

        calculator.add_trade(sample_trade)

        # Act
        result = calculator.calculate_performance()

        # Assert
        perf = result[0]
        assert perf.total_trades == 1
        assert perf.winning_trades == 1
        assert perf.losing_trades == 0

    def test_calculate_performance_trade_statistics(self, calculator):
        """Test calculation of trade statistics (win rate, profit factor)."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("50000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 30, tzinfo=timezone.utc), Decimal("51000"))

        # Add winning and losing trades
        winning_trade = TradeRecord(
            trade_id="T001",
            strategy_id="strategy_a",
            symbol="AAPL",
            entry_timestamp=datetime(2025, 1, 5, tzinfo=timezone.utc),
            exit_timestamp=datetime(2025, 1, 10, tzinfo=timezone.utc),
            entry_price=Decimal("150.00"),
            exit_price=Decimal("155.00"),
            quantity=100,
            side="long",
            pnl=Decimal("500.00"),
            pnl_pct=Decimal("5.00"),
            commission=Decimal("2.00"),
            duration_seconds=432000,
        )

        losing_trade = TradeRecord(
            trade_id="T002",
            strategy_id="strategy_a",
            symbol="MSFT",
            entry_timestamp=datetime(2025, 1, 15, tzinfo=timezone.utc),
            exit_timestamp=datetime(2025, 1, 20, tzinfo=timezone.utc),
            entry_price=Decimal("300.00"),
            exit_price=Decimal("295.00"),
            quantity=50,
            side="long",
            pnl=Decimal("-250.00"),
            pnl_pct=Decimal("-2.00"),
            commission=Decimal("2.00"),
            duration_seconds=432000,
        )

        calculator.add_trade(winning_trade)
        calculator.add_trade(losing_trade)

        # Act
        result = calculator.calculate_performance()

        # Assert
        perf = result[0]
        assert perf.total_trades == 2
        assert perf.winning_trades == 1
        assert perf.losing_trades == 1
        assert perf.win_rate == Decimal("50.00")
        assert perf.avg_win_pct == Decimal("5.00")
        assert perf.avg_loss_pct == Decimal("-2.00")
        assert perf.profit_factor == Decimal("2.00")  # 500 / 250

    def test_calculate_performance_no_losing_trades_profit_factor_none(self, calculator):
        """Test that profit factor is None when there are no losing trades."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("50000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("50500"))

        winning_trade = TradeRecord(
            trade_id="T001",
            strategy_id="strategy_a",
            symbol="AAPL",
            entry_timestamp=datetime(2025, 1, 5, tzinfo=timezone.utc),
            exit_timestamp=datetime(2025, 1, 10, tzinfo=timezone.utc),
            entry_price=Decimal("150.00"),
            exit_price=Decimal("155.00"),
            quantity=100,
            side="long",
            pnl=Decimal("500.00"),
            pnl_pct=Decimal("3.33"),
            commission=Decimal("2.00"),
            duration_seconds=432000,
        )

        calculator.add_trade(winning_trade)

        # Act
        result = calculator.calculate_performance()

        # Assert
        perf = result[0]
        assert perf.profit_factor is None

    def test_calculate_performance_max_drawdown_calculation(self, calculator):
        """Test maximum drawdown calculation for strategy."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("100000"))

        # Equity curve with drawdown: 100k -> 110k -> 95k -> 105k
        calculator.update_equity("strategy_a", datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 10, tzinfo=timezone.utc), Decimal("110000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 20, tzinfo=timezone.utc), Decimal("95000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 30, tzinfo=timezone.utc), Decimal("105000"))

        # Act
        result = calculator.calculate_performance()

        # Assert
        perf = result[0]
        # Max drawdown: (110k - 95k) / 110k = 13.64%
        assert perf.max_drawdown_pct == Decimal("13.64")

    def test_calculate_performance_sharpe_ratio_calculation(self, calculator):
        """Test Sharpe ratio calculation from strategy returns."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("100000"))

        # Add equity curve with multiple points for returns calculation
        calculator.update_equity("strategy_a", datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 10, tzinfo=timezone.utc), Decimal("102000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 20, tzinfo=timezone.utc), Decimal("104000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 30, tzinfo=timezone.utc), Decimal("106000"))

        # Act
        result = calculator.calculate_performance()

        # Assert
        perf = result[0]
        # Should have a Sharpe ratio calculated (value depends on returns)
        assert perf.sharpe_ratio is not None
        assert isinstance(perf.sharpe_ratio, Decimal)

    def test_calculate_performance_insufficient_data_sharpe_none(self, calculator):
        """Test that Sharpe ratio is None with insufficient equity points."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("100000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000"))

        # Act
        result = calculator.calculate_performance()

        # Assert
        perf = result[0]
        assert perf.sharpe_ratio is None  # Need at least 2 points

    def test_calculate_performance_zero_allocation_zero_return(self, calculator):
        """Test that zero allocation returns 0% return (edge case)."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("0"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("100000"))

        # Act
        result = calculator.calculate_performance()

        # Assert
        perf = result[0]
        assert perf.return_pct == Decimal("0")

    def test_calculate_performance_negative_returns(self, calculator):
        """Test strategy with negative returns."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("100000"))
        calculator.update_equity("strategy_a", datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("95000"))

        # Act
        result = calculator.calculate_performance()

        # Assert
        perf = result[0]
        assert perf.return_pct == Decimal("-5.00")

    def test_update_allocation_new_strategy_added_dynamically(self, calculator):
        """Test that new strategy can be added dynamically."""
        # Arrange & Act
        calculator.update_allocation("strategy_c", Decimal("30000"))
        calculator.update_equity("strategy_c", datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("31500"))

        result = calculator.calculate_performance()

        # Assert
        perf_c = next(p for p in result if p.strategy_id == "strategy_c")
        assert perf_c.equity_allocated == Decimal("30000")
        assert perf_c.final_equity == Decimal("31500")
        assert perf_c.return_pct == Decimal("5.00")

    def test_calculate_performance_strategy_without_equity_skipped(self, calculator):
        """Test that strategies without equity points are skipped."""
        # Arrange
        calculator.update_allocation("strategy_a", Decimal("50000"))
        # No equity updates for strategy_a
        # strategy_b has no allocation or equity

        # Act
        result = calculator.calculate_performance()

        # Assert
        assert len(result) == 0  # No strategies with equity data

    def test_calculate_returns_multiple_periods(self, calculator):
        """Test that _calculate_returns produces correct period returns."""
        # Arrange
        equity_points = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),
            (datetime(2025, 1, 10, tzinfo=timezone.utc), Decimal("102000")),
            (datetime(2025, 1, 20, tzinfo=timezone.utc), Decimal("104040")),
        ]

        # Act
        returns = calculator._calculate_returns(equity_points)

        # Assert
        assert len(returns) == 2
        # First return: (102000 - 100000) / 100000 = 0.02
        assert abs(returns[0] - Decimal("0.02")) < Decimal("0.0001")
        # Second return: (104040 - 102000) / 102000 = 0.02
        assert abs(returns[1] - Decimal("0.02")) < Decimal("0.0001")

    def test_calculate_returns_empty_list(self, calculator):
        """Test that _calculate_returns with empty list returns empty."""
        # Act
        returns = calculator._calculate_returns([])

        # Assert
        assert returns == []

    def test_calculate_returns_single_point(self, calculator):
        """Test that _calculate_returns with single point returns empty."""
        # Arrange
        equity_points = [(datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000"))]

        # Act
        returns = calculator._calculate_returns(equity_points)

        # Assert
        assert returns == []

    def test_calculate_max_drawdown_no_drawdown(self, calculator):
        """Test max drawdown calculation with no drawdown (only gains)."""
        # Arrange
        equity_points = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),
            (datetime(2025, 1, 10, tzinfo=timezone.utc), Decimal("105000")),
            (datetime(2025, 1, 20, tzinfo=timezone.utc), Decimal("110000")),
        ]

        # Act
        result = calculator._calculate_max_drawdown(equity_points)

        # Assert
        assert result == Decimal("0")

    def test_calculate_max_drawdown_empty_list(self, calculator):
        """Test max drawdown calculation with empty equity list."""
        # Act
        result = calculator._calculate_max_drawdown([])

        # Assert
        assert result == Decimal("0")

    def test_calculate_max_drawdown_largest_decline(self, calculator):
        """Test max drawdown identifies largest decline from peak."""
        # Arrange
        equity_points = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),
            (datetime(2025, 1, 5, tzinfo=timezone.utc), Decimal("110000")),  # Peak 1
            (datetime(2025, 1, 10, tzinfo=timezone.utc), Decimal("105000")),  # -4.55%
            (datetime(2025, 1, 15, tzinfo=timezone.utc), Decimal("115000")),  # Peak 2
            (datetime(2025, 1, 20, tzinfo=timezone.utc), Decimal("95000")),  # -17.39%
            (datetime(2025, 1, 25, tzinfo=timezone.utc), Decimal("100000")),
        ]

        # Act
        result = calculator._calculate_max_drawdown(equity_points)

        # Assert
        # Max drawdown: (115000 - 95000) / 115000 = 17.39%
        assert result == Decimal("17.39")

    def test_add_trade_new_strategy_creates_tracking(self, calculator):
        """Test that adding trade for new strategy creates tracking."""
        # Arrange
        trade = TradeRecord(
            trade_id="T001",
            strategy_id="strategy_new",
            symbol="AAPL",
            entry_timestamp=datetime(2025, 1, 5, tzinfo=timezone.utc),
            exit_timestamp=datetime(2025, 1, 10, tzinfo=timezone.utc),
            entry_price=Decimal("150.00"),
            exit_price=Decimal("155.00"),
            quantity=100,
            side="long",
            pnl=Decimal("500.00"),
            pnl_pct=Decimal("3.33"),
            commission=Decimal("2.00"),
            duration_seconds=432000,
        )

        # Act
        calculator.add_trade(trade)

        # Assert
        assert "strategy_new" in calculator._strategy_trades
        assert len(calculator._strategy_trades["strategy_new"]) == 1
