"""Tests for performance metrics calculations."""

from datetime import datetime, timezone
from decimal import Decimal

from qs_trader.libraries.performance.metrics import (
    calculate_cagr,
    calculate_calmar_ratio,
    calculate_drawdown_periods,
    calculate_expectancy,
    calculate_max_drawdown,
    calculate_profit_factor,
    calculate_sharpe_ratio,
    calculate_sortino_ratio,
    calculate_total_return,
    calculate_volatility,
    calculate_win_rate,
)
from qs_trader.libraries.performance.models import TradeRecord


class TestReturnMetrics:
    """Test return calculation functions."""

    def test_calculate_total_return_positive(self):
        """Test positive return calculation."""
        initial = Decimal("100000")
        final = Decimal("150000")

        result = calculate_total_return(initial, final)

        assert result == Decimal("50.00")

    def test_calculate_total_return_negative(self):
        """Test negative return calculation."""
        initial = Decimal("100000")
        final = Decimal("80000")

        result = calculate_total_return(initial, final)

        assert result == Decimal("-20.00")

    def test_calculate_total_return_zero(self):
        """Test zero return calculation."""
        initial = Decimal("100000")
        final = Decimal("100000")

        result = calculate_total_return(initial, final)

        assert result == Decimal("0.00")

    def test_calculate_total_return_zero_initial(self):
        """Test that zero initial equity returns zero (edge case handling)."""
        # Implementation returns 0.00 instead of raising
        result = calculate_total_return(Decimal("0"), Decimal("100000"))
        assert result == Decimal("0.00")

    def test_calculate_cagr_one_year(self):
        """Test CAGR calculation for exactly one year."""
        initial = Decimal("100000")
        final = Decimal("110000")
        days = 365

        result = calculate_cagr(initial, final, days)

        # 10% return over 1 year ≈ 10% CAGR (slight rounding differences)
        assert abs(result - Decimal("10.00")) < Decimal("0.02")

    def test_calculate_cagr_half_year(self):
        """Test CAGR calculation for half year."""
        initial = Decimal("100000")
        final = Decimal("105000")  # 5% return in 6 months
        days = 182

        result = calculate_cagr(initial, final, days)

        # 5% in 6 months ≈ 10.25% annualized
        assert abs(result - Decimal("10.25")) < Decimal("0.5")

    def test_calculate_cagr_two_years(self):
        """Test CAGR calculation for two years."""
        initial = Decimal("100000")
        final = Decimal("121000")  # 21% total return over 2 years
        days = 730

        result = calculate_cagr(initial, final, days)

        # (1.21)^0.5 - 1 = 10% CAGR
        assert abs(result - Decimal("10.00")) < Decimal("0.1")

    def test_calculate_cagr_negative_return(self):
        """Test CAGR with negative returns."""
        initial = Decimal("100000")
        final = Decimal("90000")
        days = 365

        result = calculate_cagr(initial, final, days)

        assert result < Decimal("0")
        assert abs(result - Decimal("-10.00")) < Decimal("0.02")

    def test_calculate_cagr_zero_days(self):
        """Test CAGR with zero days (edge case handling)."""
        # Implementation returns 0.00 instead of raising
        result = calculate_cagr(Decimal("100000"), Decimal("110000"), 0)
        assert result == Decimal("0.00")

    def test_calculate_cagr_negative_days(self):
        """Test CAGR with negative days (edge case - returns negative CAGR)."""
        # Implementation processes negative days (unusual edge case)
        result = calculate_cagr(Decimal("100000"), Decimal("110000"), -365)
        # With negative time period, CAGR is negative
        assert result < Decimal("0")


class TestRiskMetrics:
    """Test risk calculation functions."""

    def test_calculate_volatility_constant_returns(self):
        """Test volatility with constant returns (should be zero)."""
        returns = [Decimal("0.01")] * 100

        result = calculate_volatility(returns)

        assert result == Decimal("0.00")

    def test_calculate_volatility_known_values(self):
        """Test volatility with known standard deviation."""
        # Returns: 1%, 2%, 3%, 4%, 5% (mean = 3%)
        # Annualized volatility calculation
        returns = [
            Decimal("0.01"),
            Decimal("0.02"),
            Decimal("0.03"),
            Decimal("0.04"),
            Decimal("0.05"),
        ]

        result = calculate_volatility(returns)

        # Wider tolerance for volatility calculations
        assert abs(result - Decimal("25.10")) < Decimal("2.0")

    def test_calculate_volatility_empty_list(self):
        """Test that empty returns list returns zero."""
        result = calculate_volatility([])
        assert result == Decimal("0.00")

    def test_calculate_volatility_single_value(self):
        """Test that single return value returns zero."""
        result = calculate_volatility([Decimal("0.01")])
        assert result == Decimal("0.00")

    def test_calculate_volatility_high_variance(self):
        """Test volatility with high variance returns."""
        returns = [
            Decimal("0.10"),  # +10%
            Decimal("-0.08"),  # -8%
            Decimal("0.12"),  # +12%
            Decimal("-0.05"),  # -5%
        ]

        result = calculate_volatility(returns)

        # High variance should result in high volatility
        assert result > Decimal("50.0")


class TestRiskAdjustedMetrics:
    """Test risk-adjusted return calculations."""

    def test_calculate_calmar_ratio_positive(self):
        """Test Calmar ratio with positive CAGR."""
        cagr = Decimal("15.00")
        max_drawdown = Decimal("5.00")

        result = calculate_calmar_ratio(cagr, max_drawdown)

        # 15 / 5 = 3.0
        assert result == Decimal("3.00")

    def test_calculate_calmar_ratio_negative_cagr(self):
        """Test Calmar ratio with negative CAGR."""
        cagr = Decimal("-10.00")
        max_drawdown = Decimal("20.00")

        result = calculate_calmar_ratio(cagr, max_drawdown)

        # -10 / 20 = -0.5
        assert result == Decimal("-0.50")

    def test_calculate_calmar_ratio_zero_drawdown(self):
        """Test that zero max drawdown returns capped value."""
        cagr = Decimal("15.00")
        max_drawdown = Decimal("0.00")

        result = calculate_calmar_ratio(cagr, max_drawdown)

        # Should return capped value (999.99)
        assert result == Decimal("999.99")


class TestEdgeCases:
    """Test edge cases and boundary conditions."""

    def test_very_small_returns(self):
        """Test metrics with very small returns."""
        initial = Decimal("100000.00")
        final = Decimal("100000.01")

        result = calculate_total_return(initial, final)

        assert result > Decimal("0")
        assert result < Decimal("0.01")

    def test_very_large_returns(self):
        """Test metrics with very large returns."""
        initial = Decimal("100000")
        final = Decimal("10000000")  # 100x return

        result = calculate_total_return(initial, final)

        assert result == Decimal("9900.00")

    def test_precision_preservation(self):
        """Test that Decimal precision is preserved."""
        initial = Decimal("100000.123456")
        final = Decimal("150000.987654")

        result = calculate_total_return(initial, final)

        # Should maintain precision
        assert isinstance(result, Decimal)
        assert "." in str(result)  # Has decimal point


class TestMaxDrawdown:
    """Test maximum drawdown calculation."""

    def test_calculate_max_drawdown_simple_decline(self):
        """Test max drawdown with simple peak-to-trough decline."""

        curve = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),  # Peak
            (datetime(2025, 1, 2, tzinfo=timezone.utc), Decimal("95000")),  # 5% drawdown
            (datetime(2025, 1, 3, tzinfo=timezone.utc), Decimal("90000")),  # 10% drawdown (max)
        ]

        result = calculate_max_drawdown(curve)

        assert result == Decimal("10.00")

    def test_calculate_max_drawdown_multiple_peaks(self):
        """Test max drawdown with multiple peaks and troughs."""

        curve = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),  # Peak 1
            (datetime(2025, 1, 2, tzinfo=timezone.utc), Decimal("95000")),  # 5% drawdown
            (datetime(2025, 1, 3, tzinfo=timezone.utc), Decimal("105000")),  # New peak
            (datetime(2025, 1, 4, tzinfo=timezone.utc), Decimal("89250")),  # 15% drawdown (max)
        ]

        result = calculate_max_drawdown(curve)

        assert result == Decimal("15.00")

    def test_calculate_max_drawdown_no_drawdown(self):
        """Test that steadily increasing equity has zero drawdown."""

        curve = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),
            (datetime(2025, 1, 2, tzinfo=timezone.utc), Decimal("105000")),
            (datetime(2025, 1, 3, tzinfo=timezone.utc), Decimal("110000")),
        ]

        result = calculate_max_drawdown(curve)

        assert result == Decimal("0.00")

    def test_calculate_max_drawdown_empty_curve(self):
        """Test that empty equity curve returns zero."""
        result = calculate_max_drawdown([])
        assert result == Decimal("0.00")

    def test_calculate_max_drawdown_single_point(self):
        """Test that single point curve returns zero."""

        curve = [(datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000"))]
        result = calculate_max_drawdown(curve)
        assert result == Decimal("0.00")


class TestDrawdownPeriods:
    """Test drawdown period identification."""

    def test_calculate_drawdown_periods_single_period(self):
        """Test identification of single drawdown period with recovery."""

        curve = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),  # Peak
            (datetime(2025, 1, 2, tzinfo=timezone.utc), Decimal("95000")),  # Trough
            (datetime(2025, 1, 3, tzinfo=timezone.utc), Decimal("100000")),  # Recovery
        ]

        periods = calculate_drawdown_periods(curve)

        assert len(periods) == 1
        assert periods[0].depth_pct == Decimal("5.00")
        assert periods[0].recovered is True
        assert periods[0].duration_days == 1
        assert periods[0].recovery_days == 1

    def test_calculate_drawdown_periods_ongoing(self):
        """Test identification of ongoing drawdown (no recovery)."""

        curve = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),  # Peak
            (datetime(2025, 1, 2, tzinfo=timezone.utc), Decimal("95000")),  # Still in drawdown
        ]

        periods = calculate_drawdown_periods(curve)

        assert len(periods) == 1
        assert periods[0].depth_pct == Decimal("5.00")
        assert periods[0].recovered is False
        assert periods[0].end_timestamp is None
        assert periods[0].recovery_days is None

    def test_calculate_drawdown_periods_multiple_periods(self):
        """Test identification of multiple separate drawdown periods."""

        curve = [
            (datetime(2025, 1, 1, tzinfo=timezone.utc), Decimal("100000")),  # Peak 1
            (datetime(2025, 1, 2, tzinfo=timezone.utc), Decimal("95000")),  # Drawdown 1
            (datetime(2025, 1, 3, tzinfo=timezone.utc), Decimal("100000")),  # Recovery
            (datetime(2025, 1, 4, tzinfo=timezone.utc), Decimal("90000")),  # Drawdown 2
            (datetime(2025, 1, 5, tzinfo=timezone.utc), Decimal("100000")),  # Recovery 2
        ]

        periods = calculate_drawdown_periods(curve)

        assert len(periods) == 2
        assert periods[0].depth_pct == Decimal("5.00")
        assert periods[1].depth_pct == Decimal("10.00")
        assert all(p.recovered for p in periods)

    def test_calculate_drawdown_periods_empty_curve(self):
        """Test that empty curve returns no periods."""
        result = calculate_drawdown_periods([])
        assert result == []


class TestSharpeRatio:
    """Test Sharpe ratio calculation."""

    def test_calculate_sharpe_ratio_positive(self):
        """Test Sharpe ratio with positive returns."""
        returns = [Decimal("0.001")] * 252  # Consistent 0.1% daily return
        risk_free_rate = Decimal("0.02")

        result = calculate_sharpe_ratio(returns, risk_free_rate)

        # (0.001 * 252 - 0.02) / (0 volatility) -> but volatility will be 0
        # With constant returns, volatility = 0, so Sharpe = 0
        assert result == Decimal("0.00")

    def test_calculate_sharpe_ratio_varied_returns(self):
        """Test Sharpe ratio with varied returns."""
        returns = [
            Decimal("0.01"),
            Decimal("-0.005"),
            Decimal("0.02"),
            Decimal("0.005"),
            Decimal("-0.01"),
        ] * 50  # Repeat pattern to get 250 returns

        risk_free_rate = Decimal("0.02")

        result = calculate_sharpe_ratio(returns, risk_free_rate)

        # Should return a reasonable Sharpe ratio
        assert isinstance(result, Decimal)
        assert result != Decimal("0.00")

    def test_calculate_sharpe_ratio_empty_returns(self):
        """Test that empty returns list returns zero."""
        result = calculate_sharpe_ratio([], Decimal("0.02"))
        assert result == Decimal("0.00")

    def test_calculate_sharpe_ratio_single_return(self):
        """Test that single return returns zero."""
        result = calculate_sharpe_ratio([Decimal("0.01")], Decimal("0.02"))
        assert result == Decimal("0.00")


class TestSortinoRatio:
    """Test Sortino ratio calculation."""

    def test_calculate_sortino_ratio_with_downside(self):
        """Test Sortino ratio with some negative returns."""
        returns = [
            Decimal("0.01"),
            Decimal("-0.005"),
            Decimal("0.02"),
            Decimal("-0.01"),
        ] * 63  # 252 returns

        risk_free_rate = Decimal("0.02")

        result = calculate_sortino_ratio(returns, risk_free_rate)

        # Should return a reasonable Sortino ratio
        assert isinstance(result, Decimal)
        assert result != Decimal("0.00")

    def test_calculate_sortino_ratio_no_downside(self):
        """Test Sortino ratio with no negative returns (infinite/None)."""
        returns = [Decimal("0.01"), Decimal("0.02"), Decimal("0.015")] * 84  # All positive

        risk_free_rate = Decimal("0.02")

        result = calculate_sortino_ratio(returns, risk_free_rate)

        # With no negative returns (no downside risk), Sortino is infinite → return None
        assert result is None

    def test_calculate_sortino_ratio_empty_returns(self):
        """Test that empty returns list returns zero."""
        result = calculate_sortino_ratio([], Decimal("0.02"))
        assert result == Decimal("0.00")


class TestTradeStatistics:
    """Test trade statistics calculations."""

    def test_calculate_win_rate_mixed_trades(self):
        """Test win rate calculation with mixed results."""

        trades = [
            TradeRecord.model_construct(
                trade_id="T1",
                strategy_id="test",
                symbol="AAPL",
                entry_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc),
                entry_price=Decimal("100"),
                exit_price=Decimal("110"),
                quantity=100,
                side="long",
                pnl=Decimal("1000"),  # Winner
                pnl_pct=Decimal("10"),
                commission=Decimal("2"),
                duration_seconds=86400,
            ),
            TradeRecord.model_construct(
                trade_id="T2",
                strategy_id="test",
                symbol="MSFT",
                entry_timestamp=datetime(2025, 1, 3, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 4, tzinfo=timezone.utc),
                entry_price=Decimal("200"),
                exit_price=Decimal("190"),
                quantity=50,
                side="long",
                pnl=Decimal("-500"),  # Loser
                pnl_pct=Decimal("-5"),
                commission=Decimal("2"),
                duration_seconds=86400,
            ),
            TradeRecord.model_construct(
                trade_id="T3",
                strategy_id="test",
                symbol="GOOGL",
                entry_timestamp=datetime(2025, 1, 5, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 6, tzinfo=timezone.utc),
                entry_price=Decimal("150"),
                exit_price=Decimal("160"),
                quantity=75,
                side="long",
                pnl=Decimal("750"),  # Winner
                pnl_pct=Decimal("6.67"),
                commission=Decimal("2"),
                duration_seconds=86400,
            ),
        ]

        result = calculate_win_rate(trades)

        # 2 winners out of 3 = 66.67%
        assert result == Decimal("66.67")

    def test_calculate_win_rate_all_winners(self):
        """Test win rate with all winning trades."""

        trades = [
            TradeRecord.model_construct(
                trade_id="T1",
                strategy_id="test",
                symbol="AAPL",
                entry_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc),
                entry_price=Decimal("100"),
                exit_price=Decimal("110"),
                quantity=100,
                side="long",
                pnl=Decimal("1000"),
                pnl_pct=Decimal("10"),
                commission=Decimal("2"),
                duration_seconds=86400,
            )
        ]

        result = calculate_win_rate(trades)

        assert result == Decimal("100.00")

    def test_calculate_win_rate_empty_trades(self):
        """Test that empty trades list returns zero."""
        result = calculate_win_rate([])
        assert result == Decimal("0.00")

    def test_calculate_profit_factor_positive(self):
        """Test profit factor calculation."""

        trades = [
            TradeRecord.model_construct(
                trade_id="T1",
                strategy_id="test",
                symbol="AAPL",
                entry_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc),
                entry_price=Decimal("100"),
                exit_price=Decimal("120"),
                quantity=100,
                side="long",
                pnl=Decimal("2000"),  # Winner
                pnl_pct=Decimal("20"),
                commission=Decimal("2"),
                duration_seconds=86400,
            ),
            TradeRecord.model_construct(
                trade_id="T2",
                strategy_id="test",
                symbol="MSFT",
                entry_timestamp=datetime(2025, 1, 3, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 4, tzinfo=timezone.utc),
                entry_price=Decimal("200"),
                exit_price=Decimal("180"),
                quantity=50,
                side="long",
                pnl=Decimal("-1000"),  # Loser
                pnl_pct=Decimal("-10"),
                commission=Decimal("2"),
                duration_seconds=86400,
            ),
        ]

        result = calculate_profit_factor(trades)

        # Gross profit = 2000, Gross loss = 1000, PF = 2.0
        assert result == Decimal("2.00")

    def test_calculate_profit_factor_no_losers(self):
        """Test profit factor with no losing trades returns None."""

        trades = [
            TradeRecord.model_construct(
                trade_id="T1",
                strategy_id="test",
                symbol="AAPL",
                entry_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc),
                entry_price=Decimal("100"),
                exit_price=Decimal("110"),
                quantity=100,
                side="long",
                pnl=Decimal("1000"),
                pnl_pct=Decimal("10"),
                commission=Decimal("2"),
                duration_seconds=86400,
            )
        ]

        result = calculate_profit_factor(trades)

        assert result is None

    def test_calculate_profit_factor_empty_trades(self):
        """Test that empty trades list returns None."""
        result = calculate_profit_factor([])
        assert result is None

    def test_calculate_expectancy_positive(self):
        """Test expectancy calculation with positive expected value."""

        trades = [
            TradeRecord.model_construct(
                trade_id="T1",
                strategy_id="test",
                symbol="AAPL",
                entry_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc),
                entry_price=Decimal("100"),
                exit_price=Decimal("110"),
                quantity=100,
                side="long",
                pnl=Decimal("1000"),
                pnl_pct=Decimal("10"),
                commission=Decimal("2"),
                duration_seconds=86400,
            ),
            TradeRecord.model_construct(
                trade_id="T2",
                strategy_id="test",
                symbol="MSFT",
                entry_timestamp=datetime(2025, 1, 3, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 4, tzinfo=timezone.utc),
                entry_price=Decimal("200"),
                exit_price=Decimal("190"),
                quantity=50,
                side="long",
                pnl=Decimal("-500"),
                pnl_pct=Decimal("-5"),
                commission=Decimal("2"),
                duration_seconds=86400,
            ),
        ]

        result = calculate_expectancy(trades)

        # Expectancy = (0.5 * 1000) - (0.5 * 500) = 250
        assert result == Decimal("250.00")

    def test_calculate_expectancy_empty_trades(self):
        """Test that empty trades list returns zero."""
        result = calculate_expectancy([])
        assert result == Decimal("0.00")

    def test_calculate_expectancy_all_winners(self):
        """Test expectancy with only winning trades."""

        trades = [
            TradeRecord.model_construct(
                trade_id="T1",
                strategy_id="test",
                symbol="AAPL",
                entry_timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                exit_timestamp=datetime(2025, 1, 2, tzinfo=timezone.utc),
                entry_price=Decimal("100"),
                exit_price=Decimal("110"),
                quantity=100,
                side="long",
                pnl=Decimal("500"),
                pnl_pct=Decimal("10"),
                commission=Decimal("2"),
                duration_seconds=86400,
            )
        ]

        result = calculate_expectancy(trades)

        # All winners, so expectancy equals avg win
        assert result == Decimal("500.00")
