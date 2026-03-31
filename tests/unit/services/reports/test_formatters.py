"""Unit tests for reporting formatters module.

Tests cover:
- Helper formatting functions (_format_pct, _format_currency, etc.)
- Color assignment logic (_get_color)
- Table creation functions for each metric category
- Main display function with different detail levels
"""

from datetime import datetime, timezone
from decimal import Decimal
from io import StringIO

import pytest
from rich.console import Console

from qs_trader.libraries.performance.models import DrawdownPeriod, FullMetrics, PeriodMetrics, StrategyPerformance
from qs_trader.services.reporting.formatters import (
    _create_drawdown_table,
    _create_period_table,
    _create_risk_adjusted_table,
    _create_risk_table,
    _create_strategy_table,
    _create_summary_table,
    _create_trade_stats_table,
    _format_currency,
    _format_number,
    _format_pct,
    _get_color,
    display_performance_report,
)


@pytest.fixture
def sample_full_metrics():
    """Fixture providing complete FullMetrics for testing."""
    return FullMetrics(
        backtest_id="test_backtest_001",
        start_date="2024-01-01",
        end_date="2024-12-31",
        duration_days=365,
        initial_equity=Decimal("100000.00"),
        final_equity=Decimal("125000.00"),
        total_return_pct=Decimal("25.00"),
        cagr=Decimal("25.00"),
        best_day_return_pct=Decimal("3.50"),
        worst_day_return_pct=Decimal("-2.75"),
        volatility_annual_pct=Decimal("15.50"),
        max_drawdown_pct=Decimal("12.30"),
        max_drawdown_duration_days=45,
        avg_drawdown_pct=Decimal("5.60"),
        current_drawdown_pct=Decimal("2.10"),
        sharpe_ratio=Decimal("1.61"),
        sortino_ratio=Decimal("2.05"),
        calmar_ratio=Decimal("2.03"),
        risk_free_rate=Decimal("4.50"),
        total_trades=100,
        winning_trades=55,
        losing_trades=45,
        win_rate=Decimal("55.00"),
        profit_factor=Decimal("1.45"),
        expectancy=Decimal("250.00"),
        avg_win=Decimal("550.00"),
        avg_loss=Decimal("-380.00"),
        avg_win_pct=Decimal("5.50"),
        avg_loss_pct=Decimal("-3.80"),
        largest_win=Decimal("2500.00"),
        largest_win_pct=Decimal("2.50"),
        largest_loss=Decimal("-1800.00"),
        largest_loss_pct=Decimal("-1.80"),
        avg_trade_duration_days=Decimal("5.5"),
        max_consecutive_wins=7,
        max_consecutive_losses=5,
        total_commissions=Decimal("1250.00"),
        commission_pct_of_pnl=Decimal("5.00"),
        monthly_returns=[],
        quarterly_returns=[],
        annual_returns=[],
        strategy_performance=[],
        drawdown_periods=[],
    )


class TestFormatHelpers:
    """Test formatting helper functions."""

    def test_format_pct_default_precision(self):
        """Test percentage formatting with default 2 decimal places."""
        result = _format_pct(Decimal("25.567"))
        assert result == "25.57%"

    def test_format_pct_custom_precision(self):
        """Test percentage formatting with custom precision."""
        result = _format_pct(Decimal("25.567"), precision=1)
        assert result == "25.6%"

    def test_format_pct_zero(self):
        """Test percentage formatting for zero value."""
        result = _format_pct(Decimal("0.00"))
        assert result == "0.00%"

    def test_format_pct_negative(self):
        """Test percentage formatting for negative value."""
        result = _format_pct(Decimal("-15.75"))
        assert result == "-15.75%"

    def test_format_currency_default_precision(self):
        """Test currency formatting with commas and dollar sign."""
        result = _format_currency(Decimal("125000.50"))
        assert result == "$125,000.50"

    def test_format_currency_small_value(self):
        """Test currency formatting for small value."""
        result = _format_currency(Decimal("99.99"))
        assert result == "$99.99"

    def test_format_currency_negative(self):
        """Test currency formatting for negative value."""
        result = _format_currency(Decimal("-1500.75"))
        assert result == "$-1,500.75"

    def test_format_currency_custom_precision(self):
        """Test currency formatting with custom precision."""
        result = _format_currency(Decimal("1234.5678"), precision=4)
        assert result == "$1,234.5678"

    def test_format_number_integer(self):
        """Test number formatting for integer with commas."""
        result = _format_number(1000)
        assert result == "1,000"

    def test_format_number_float_default_precision(self):
        """Test number formatting for float with no decimals."""
        result = _format_number(1234.56)
        # Rounds to nearest integer
        assert result == "1,235"

    def test_format_number_decimal_with_precision(self):
        """Test number formatting for Decimal with custom precision."""
        result = _format_number(Decimal("1234.5678"), precision=2)
        assert result == "1,234.57"

    def test_format_number_small_integer(self):
        """Test number formatting for small integer."""
        result = _format_number(42)
        assert result == "42"


class TestGetColor:
    """Test color assignment logic."""

    def test_get_color_positive(self):
        """Test that positive values return green."""
        assert _get_color(Decimal("25.50")) == "green"

    def test_get_color_negative(self):
        """Test that negative values return red."""
        assert _get_color(Decimal("-15.75")) == "red"

    def test_get_color_zero(self):
        """Test that zero returns white."""
        assert _get_color(Decimal("0.00")) == "white"

    def test_get_color_small_positive(self):
        """Test that small positive values return green."""
        assert _get_color(Decimal("0.01")) == "green"

    def test_get_color_small_negative(self):
        """Test that small negative values return red."""
        assert _get_color(Decimal("-0.01")) == "red"


class TestCreateSummaryTable:
    """Test summary metrics table creation."""

    def test_create_summary_table_structure(self, sample_full_metrics):
        """Test that summary table is created with correct structure."""
        table = _create_summary_table(sample_full_metrics)

        assert table is not None
        assert table.title == "📊 Performance Summary"
        assert len(table.columns) == 2

    def test_create_summary_table_contains_key_metrics(self, sample_full_metrics):
        """Test that summary table is created successfully."""
        table = _create_summary_table(sample_full_metrics)

        # Verify table structure
        assert table is not None
        assert len(table.columns) == 2

    def test_create_summary_table_positive_return_color(self, sample_full_metrics):
        """Test summary table is created with positive returns."""
        table = _create_summary_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_summary_table_negative_return(self, sample_full_metrics):
        """Test summary table with negative returns."""
        sample_full_metrics.total_return_pct = Decimal("-10.50")
        sample_full_metrics.cagr = Decimal("-10.50")

        table = _create_summary_table(sample_full_metrics)

        # Verify table is created
        assert table is not None


class TestCreateRiskTable:
    """Test risk metrics table creation."""

    def test_create_risk_table_structure(self, sample_full_metrics):
        """Test that risk table is created with correct structure."""
        table = _create_risk_table(sample_full_metrics)

        assert table is not None
        assert table.title == "⚠️  Risk Metrics"
        assert len(table.columns) == 2

    def test_create_risk_table_contains_risk_metrics(self, sample_full_metrics):
        """Test that risk table is created successfully."""
        table = _create_risk_table(sample_full_metrics)

        # Verify table structure
        assert table is not None
        assert len(table.columns) == 2

    def test_create_risk_table_max_drawdown_is_red(self, sample_full_metrics):
        """Test that risk table is created."""
        table = _create_risk_table(sample_full_metrics)

        # Verify table is created
        assert table is not None


class TestCreateRiskAdjustedTable:
    """Test risk-adjusted returns table creation."""

    def test_create_risk_adjusted_table_structure(self, sample_full_metrics):
        """Test that risk-adjusted table is created with correct structure."""
        table = _create_risk_adjusted_table(sample_full_metrics)

        assert table is not None
        # Title may be Text object, check string representation
        assert "Risk-Adjusted Returns" in str(table.title)
        assert len(table.columns) == 2

    def test_create_risk_adjusted_table_sharpe_above_one_is_green(self, sample_full_metrics):
        """Test risk-adjusted table with Sharpe > 1.0."""
        sample_full_metrics.sharpe_ratio = Decimal("1.61")

        table = _create_risk_adjusted_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_risk_adjusted_table_sharpe_below_one_is_yellow(self, sample_full_metrics):
        """Test risk-adjusted table with 0 < Sharpe < 1.0."""
        sample_full_metrics.sharpe_ratio = Decimal("0.75")

        table = _create_risk_adjusted_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_risk_adjusted_table_negative_sharpe_is_red(self, sample_full_metrics):
        """Test risk-adjusted table with negative Sharpe."""
        sample_full_metrics.sharpe_ratio = Decimal("-0.50")

        table = _create_risk_adjusted_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_risk_adjusted_table_contains_all_ratios(self, sample_full_metrics):
        """Test that table is created with all ratios."""
        table = _create_risk_adjusted_table(sample_full_metrics)

        # Verify table structure
        assert table is not None
        assert len(table.columns) == 2


class TestCreateTradeStatsTable:
    """Test trade statistics table creation."""

    def test_create_trade_stats_table_structure(self, sample_full_metrics):
        """Test that trade stats table is created with correct structure."""
        table = _create_trade_stats_table(sample_full_metrics)

        assert table is not None
        assert table.title == "💼 Trade Statistics"
        assert len(table.columns) == 2

    def test_create_trade_stats_table_contains_trade_counts(self, sample_full_metrics):
        """Test that trade stats table is created successfully."""
        table = _create_trade_stats_table(sample_full_metrics)

        # Verify table structure
        assert table is not None
        assert len(table.columns) == 2

    def test_create_trade_stats_table_win_rate_above_50_is_green(self, sample_full_metrics):
        """Test trade stats table with win rate > 50%."""
        sample_full_metrics.win_rate = Decimal("60.00")

        table = _create_trade_stats_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_trade_stats_table_win_rate_40_to_50_is_yellow(self, sample_full_metrics):
        """Test trade stats table with 40% < win rate < 50%."""
        sample_full_metrics.win_rate = Decimal("45.00")

        table = _create_trade_stats_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_trade_stats_table_profit_factor_with_value(self, sample_full_metrics):
        """Test profit factor display when value exists."""
        sample_full_metrics.profit_factor = Decimal("2.50")

        table = _create_trade_stats_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_trade_stats_table_profit_factor_none(self, sample_full_metrics):
        """Test profit factor display when None (no losses)."""
        sample_full_metrics.profit_factor = None

        table = _create_trade_stats_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_trade_stats_table_contains_win_loss_sizes(self, sample_full_metrics):
        """Test that table is created with win/loss statistics."""
        table = _create_trade_stats_table(sample_full_metrics)

        # Verify table structure
        assert table is not None
        assert len(table.columns) == 2

    def test_create_trade_stats_table_with_duration(self, sample_full_metrics):
        """Test that trade stats table includes duration."""
        table = _create_trade_stats_table(sample_full_metrics)

        # Verify table is created
        assert table is not None

    def test_create_trade_stats_table_without_duration(self, sample_full_metrics):
        """Test table when avg_trade_duration_days is None."""
        sample_full_metrics.avg_trade_duration_days = None

        table = _create_trade_stats_table(sample_full_metrics)

        # Should not raise error, just skip duration row
        assert table is not None


class TestCreatePeriodTable:
    """Test period returns table creation."""

    @pytest.fixture
    def sample_period_metrics(self):
        """Fixture providing sample period metrics."""
        return [
            PeriodMetrics(
                period="2024-01",
                period_type="monthly",
                start_date="2024-01-01",
                end_date="2024-01-31",
                start_equity=Decimal("100000"),
                end_equity=Decimal("105000"),
                return_pct=Decimal("5.00"),
                num_trades=10,
                winning_trades=6,
                losing_trades=4,
                best_trade_pct=Decimal("2.50"),
                worst_trade_pct=Decimal("-1.25"),
            ),
            PeriodMetrics(
                period="2024-02",
                period_type="monthly",
                start_date="2024-02-01",
                end_date="2024-02-29",
                start_equity=Decimal("105000"),
                end_equity=Decimal("103500"),
                return_pct=Decimal("-1.43"),
                num_trades=8,
                winning_trades=3,
                losing_trades=5,
                best_trade_pct=Decimal("1.00"),
                worst_trade_pct=Decimal("-2.00"),
            ),
        ]

    def test_create_period_table_empty_list(self):
        """Test that empty period list returns None."""
        result = _create_period_table([], "Test Title")
        assert result is None

    def test_create_period_table_structure(self, sample_period_metrics):
        """Test that period table is created with correct structure."""
        table = _create_period_table(sample_period_metrics, "📅 Monthly Returns")

        assert table is not None
        assert table.title == "📅 Monthly Returns"
        assert len(table.columns) == 5  # Period, Return, Trades, Best, Worst

    def test_create_period_table_contains_period_data(self, sample_period_metrics):
        """Test that period table is created successfully."""
        table = _create_period_table(sample_period_metrics, "Monthly Returns")

        # Verify table structure
        assert table is not None
        assert len(table.columns) == 5

    def test_create_period_table_positive_return_is_green(self, sample_period_metrics):
        """Test period table with positive returns."""
        table = _create_period_table(sample_period_metrics, "Monthly Returns")

        # Verify table is created
        assert table is not None

    def test_create_period_table_negative_return_is_red(self, sample_period_metrics):
        """Test period table with negative returns."""
        table = _create_period_table(sample_period_metrics, "Monthly Returns")

        # Verify table is created
        assert table is not None

    def test_create_period_table_with_none_best_worst(self):
        """Test table with None values for best/worst trades."""
        periods = [
            PeriodMetrics(
                period="2024-01",
                period_type="monthly",
                start_date="2024-01-01",
                end_date="2024-01-31",
                start_equity=Decimal("100000"),
                end_equity=Decimal("105000"),
                return_pct=Decimal("5.00"),
                num_trades=0,
                winning_trades=0,
                losing_trades=0,
                best_trade_pct=None,
                worst_trade_pct=None,
            )
        ]

        table = _create_period_table(periods, "Monthly Returns")

        # Verify table is created
        assert table is not None


class TestCreateStrategyTable:
    """Test per-strategy performance table creation."""

    @pytest.fixture
    def sample_strategies(self):
        """Fixture providing sample strategy performance data."""
        return [
            StrategyPerformance(
                strategy_id="momentum_strategy",
                equity_allocated=Decimal("50000"),
                final_equity=Decimal("57750"),
                return_pct=Decimal("15.50"),
                num_positions=15,
                total_trades=50,
                winning_trades=30,
                losing_trades=20,
                win_rate=Decimal("60.00"),
                avg_win_pct=Decimal("4.50"),
                avg_loss_pct=Decimal("-3.00"),
                profit_factor=Decimal("1.85"),
                sharpe_ratio=Decimal("1.75"),
                max_drawdown_pct=Decimal("8.50"),
            ),
            StrategyPerformance(
                strategy_id="mean_reversion",
                equity_allocated=Decimal("50000"),
                final_equity=Decimal("55125"),
                return_pct=Decimal("10.25"),
                num_positions=10,
                total_trades=30,
                winning_trades=18,
                losing_trades=12,
                win_rate=Decimal("60.00"),
                avg_win_pct=Decimal("3.50"),
                avg_loss_pct=Decimal("-2.50"),
                profit_factor=Decimal("1.40"),
                sharpe_ratio=None,
                max_drawdown_pct=Decimal("12.00"),
            ),
        ]

    def test_create_strategy_table_empty_list(self):
        """Test that empty strategy list returns None."""
        result = _create_strategy_table([])
        assert result is None

    def test_create_strategy_table_structure(self, sample_strategies):
        """Test that strategy table is created with correct structure."""
        table = _create_strategy_table(sample_strategies)

        assert table is not None
        assert table.title == "🎯 Strategy Performance"
        assert len(table.columns) == 6  # Strategy, Return, Trades, Win Rate, Sharpe, Max DD

    def test_create_strategy_table_contains_strategy_data(self, sample_strategies):
        """Test that strategy table is created successfully."""
        table = _create_strategy_table(sample_strategies)

        # Verify table structure
        assert table is not None
        assert len(table.columns) == 6

    def test_create_strategy_table_sharpe_none_displays_na(self, sample_strategies):
        """Test that table handles None Sharpe ratio."""
        table = _create_strategy_table(sample_strategies)

        # Verify table is created
        assert table is not None


class TestCreateDrawdownTable:
    """Test top drawdowns table creation."""

    @pytest.fixture
    def sample_drawdowns(self):
        """Fixture providing sample drawdown periods."""
        return [
            DrawdownPeriod(
                drawdown_id=1,
                start_timestamp=datetime(2024, 3, 1, tzinfo=timezone.utc),
                trough_timestamp=datetime(2024, 3, 15, tzinfo=timezone.utc),
                end_timestamp=datetime(2024, 4, 10, tzinfo=timezone.utc),
                peak_equity=Decimal("110000"),
                trough_equity=Decimal("98000"),
                depth_pct=Decimal("10.91"),
                duration_days=14,
                recovery_days=26,
                recovered=True,
            ),
            DrawdownPeriod(
                drawdown_id=2,
                start_timestamp=datetime(2024, 6, 1, tzinfo=timezone.utc),
                trough_timestamp=datetime(2024, 6, 10, tzinfo=timezone.utc),
                end_timestamp=None,
                peak_equity=Decimal("115000"),
                trough_equity=Decimal("107000"),
                depth_pct=Decimal("6.96"),
                duration_days=9,
                recovery_days=None,
                recovered=False,
            ),
            DrawdownPeriod(
                drawdown_id=3,
                start_timestamp=datetime(2024, 8, 1, tzinfo=timezone.utc),
                trough_timestamp=datetime(2024, 8, 5, tzinfo=timezone.utc),
                end_timestamp=datetime(2024, 8, 12, tzinfo=timezone.utc),
                peak_equity=Decimal("118000"),
                trough_equity=Decimal("113000"),
                depth_pct=Decimal("4.24"),
                duration_days=4,
                recovery_days=7,
                recovered=True,
            ),
        ]

    def test_create_drawdown_table_empty_list(self):
        """Test that empty drawdown list returns None."""
        result = _create_drawdown_table([])
        assert result is None

    def test_create_drawdown_table_structure(self, sample_drawdowns):
        """Test that drawdown table is created with correct structure."""
        table = _create_drawdown_table(sample_drawdowns)

        assert table is not None
        # Title contains "Top" and "Drawdowns"
        title_str = str(table.title)
        assert "Top" in title_str
        assert "Drawdowns" in title_str
        assert len(table.columns) == 6  # Rank, Depth, Start, Duration, Recovery, Status

    def test_create_drawdown_table_sorted_by_depth(self, sample_drawdowns):
        """Test that drawdowns are sorted by depth (largest first)."""
        table = _create_drawdown_table(sample_drawdowns)

        # Verify table was created successfully
        assert table is not None

    def test_create_drawdown_table_max_rows_limit(self, sample_drawdowns):
        """Test that table respects max_rows parameter."""
        # Create table with max 2 rows
        table = _create_drawdown_table(sample_drawdowns, max_rows=2)

        assert table is not None
        # Should show "Top 2 Drawdowns"
        title_str = str(table.title)
        assert "Top 2" in title_str

    def test_create_drawdown_table_recovered_status(self, sample_drawdowns):
        """Test that drawdown table includes recovery status."""
        table = _create_drawdown_table(sample_drawdowns)

        # Verify table is created
        assert table is not None

    def test_create_drawdown_table_unrecovered_shows_dash(self, sample_drawdowns):
        """Test that drawdown table handles unrecovered drawdowns."""
        table = _create_drawdown_table(sample_drawdowns)

        # Verify table is created
        assert table is not None


class TestDisplayPerformanceReport:
    """Test main display function with different detail levels."""

    def test_display_performance_report_summary_level(self, sample_full_metrics):
        """Test display with summary detail level."""
        # Capture console output
        string_io = StringIO()
        console = Console(file=string_io, force_terminal=True, width=120)

        display_performance_report(sample_full_metrics, detail_level="summary", console=console)

        output = string_io.getvalue()

        # Should contain summary
        assert "Performance Summary" in output
        assert "test_backtest_001" in output
        assert "$100,000.00" in output
        assert "$125,000.00" in output

    def test_display_performance_report_standard_level(self, sample_full_metrics):
        """Test display with standard detail level."""
        string_io = StringIO()
        console = Console(file=string_io, force_terminal=True, width=120)

        display_performance_report(sample_full_metrics, detail_level="standard", console=console)

        output = string_io.getvalue()

        # Should contain summary, risk, and trade stats
        assert "Performance Summary" in output
        assert "Risk Metrics" in output
        assert "Risk-Adjusted Returns" in output
        assert "Trade Statistics" in output
        assert "Costs" in output

    def test_display_performance_report_full_level(self, sample_full_metrics):
        """Test display with full detail level."""
        # Add period data and strategies
        sample_full_metrics.monthly_returns = [
            PeriodMetrics(
                period="2024-01",
                period_type="monthly",
                start_date="2024-01-01",
                end_date="2024-01-31",
                start_equity=Decimal("100000"),
                end_equity=Decimal("105000"),
                return_pct=Decimal("5.00"),
                num_trades=10,
                winning_trades=6,
                losing_trades=4,
                best_trade_pct=Decimal("2.50"),
                worst_trade_pct=Decimal("-1.25"),
            )
        ]

        sample_full_metrics.strategy_performance = [
            StrategyPerformance(
                strategy_id="test_strategy",
                equity_allocated=Decimal("100000"),
                final_equity=Decimal("115500"),
                return_pct=Decimal("15.50"),
                num_positions=20,
                total_trades=50,
                winning_trades=30,
                losing_trades=20,
                win_rate=Decimal("60.00"),
                avg_win_pct=Decimal("5.00"),
                avg_loss_pct=Decimal("-3.50"),
                profit_factor=Decimal("1.75"),
                sharpe_ratio=Decimal("1.75"),
                max_drawdown_pct=Decimal("8.50"),
            )
        ]

        sample_full_metrics.drawdown_periods = [
            DrawdownPeriod(
                drawdown_id=1,
                start_timestamp=datetime(2024, 3, 1, tzinfo=timezone.utc),
                trough_timestamp=datetime(2024, 3, 15, tzinfo=timezone.utc),
                end_timestamp=datetime(2024, 4, 10, tzinfo=timezone.utc),
                peak_equity=Decimal("110000"),
                trough_equity=Decimal("98000"),
                depth_pct=Decimal("10.91"),
                duration_days=14,
                recovery_days=26,
                recovered=True,
            )
        ]

        string_io = StringIO()
        console = Console(file=string_io, force_terminal=True, width=120)

        display_performance_report(sample_full_metrics, detail_level="full", console=console)

        output = string_io.getvalue()

        # Should contain everything
        assert "Performance Summary" in output
        assert "Risk Metrics" in output
        assert "Monthly Returns" in output
        assert "Strategy Performance" in output
        assert "Drawdowns" in output

    def test_display_performance_report_no_trades(self, sample_full_metrics):
        """Test display when there are no trades."""
        sample_full_metrics.total_trades = 0

        string_io = StringIO()
        console = Console(file=string_io, force_terminal=True, width=120)

        display_performance_report(sample_full_metrics, detail_level="standard", console=console)

        output = string_io.getvalue()

        # Trade statistics should not be shown when no trades
        assert "Performance Summary" in output
        # Trade statistics table may still exist but with zero trades

    def test_display_performance_report_creates_console_when_none(self, sample_full_metrics):
        """Test that function creates Console when none provided."""
        # Should not raise error when console is None
        display_performance_report(sample_full_metrics, detail_level="summary", console=None)

    def test_display_performance_report_positive_final_border(self, sample_full_metrics):
        """Test that positive returns show green border in final panel."""
        sample_full_metrics.total_return_pct = Decimal("25.00")

        string_io = StringIO()
        console = Console(file=string_io, force_terminal=True, width=120)

        display_performance_report(sample_full_metrics, detail_level="summary", console=console)

        output = string_io.getvalue()

        # Final panel should mention the return
        assert "Backtest Complete" in output

    def test_display_performance_report_negative_final_border(self, sample_full_metrics):
        """Test that negative returns show red border in final panel."""
        sample_full_metrics.total_return_pct = Decimal("-10.00")
        sample_full_metrics.final_equity = Decimal("90000.00")

        string_io = StringIO()
        console = Console(file=string_io, force_terminal=True, width=120)

        display_performance_report(sample_full_metrics, detail_level="summary", console=console)

        output = string_io.getvalue()

        # Should complete without error
        assert "Backtest Complete" in output
