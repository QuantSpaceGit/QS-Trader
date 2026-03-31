"""Rich console formatters for performance reports.

Provides terminal display of final performance metrics with tables,
colors, and formatting using the Rich library.
"""

from decimal import Decimal
from typing import Literal

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from qs_trader.libraries.performance.models import DrawdownPeriod, FullMetrics, PeriodMetrics, StrategyPerformance


def _format_pct(value: Decimal, precision: int = 2) -> str:
    """Format percentage with optional color coding."""
    return f"{float(value):.{precision}f}%"


def _format_currency(value: Decimal, precision: int = 2) -> str:
    """Format currency value."""
    return f"${float(value):,.{precision}f}"


def _format_number(value: int | float | Decimal, precision: int = 0) -> str:
    """Format numeric value."""
    if isinstance(value, int):
        return f"{value:,}"
    return f"{float(value):,.{precision}f}"


def _get_color(value: Decimal) -> str:
    """Get color based on positive/negative value."""
    if value > 0:
        return "green"
    elif value < 0:
        return "red"
    return "white"


def _create_summary_table(metrics: FullMetrics) -> Table:
    """Create summary metrics table."""
    table = Table(title="📊 Performance Summary", show_header=False, box=None, padding=(0, 2))

    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")

    # Backtest info
    table.add_row("Backtest ID", metrics.backtest_id)
    table.add_row("Period", f"{metrics.start_date} to {metrics.end_date}")
    table.add_row("Duration", f"{metrics.duration_days} days")
    table.add_row("", "")  # Spacer

    # Returns
    table.add_row("Initial Equity", _format_currency(metrics.initial_equity))
    table.add_row("Final Equity", _format_currency(metrics.final_equity))

    total_return_color = _get_color(metrics.total_return_pct)
    table.add_row(
        "Total Return",
        f"[{total_return_color}]{_format_pct(metrics.total_return_pct)}[/{total_return_color}]",
    )

    cagr_color = _get_color(metrics.cagr)
    table.add_row("CAGR", f"[{cagr_color}]{_format_pct(metrics.cagr)}[/{cagr_color}]")

    return table


def _create_risk_table(metrics: FullMetrics) -> Table:
    """Create risk metrics table."""
    table = Table(title="⚠️  Risk Metrics", show_header=False, box=None, padding=(0, 2))

    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")

    table.add_row("Volatility (Annual)", _format_pct(metrics.volatility_annual_pct))
    table.add_row("Max Drawdown", f"[red]{_format_pct(metrics.max_drawdown_pct)}[/red]")
    table.add_row("Max DD Duration", f"{metrics.max_drawdown_duration_days} days")
    table.add_row("Avg Drawdown", _format_pct(metrics.avg_drawdown_pct))
    table.add_row("Current Drawdown", _format_pct(metrics.current_drawdown_pct))

    return table


def _create_risk_adjusted_table(metrics: FullMetrics) -> Table:
    """Create risk-adjusted returns table."""
    table = Table(title=" 📈 Risk-Adjusted Returns ", show_header=False, box=None, padding=(0, 2))

    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")

    # Sharpe ratio with color
    sharpe_color = (
        "green" if metrics.sharpe_ratio > Decimal("1.0") else "yellow" if metrics.sharpe_ratio > Decimal("0") else "red"
    )
    table.add_row("Sharpe Ratio", f"[{sharpe_color}]{float(metrics.sharpe_ratio):.2f}[/{sharpe_color}]")

    # Sortino ratio with color (can be None if no downside risk)
    if metrics.sortino_ratio is not None:
        sortino_color = (
            "green"
            if metrics.sortino_ratio > Decimal("1.0")
            else "yellow"
            if metrics.sortino_ratio > Decimal("0")
            else "red"
        )
        table.add_row("Sortino Ratio", f"[{sortino_color}]{float(metrics.sortino_ratio):.2f}[/{sortino_color}]")
    else:
        table.add_row("Sortino Ratio", "[dim]∞ (no downside)[/dim]")

    # Calmar ratio
    calmar_color = (
        "green" if metrics.calmar_ratio > Decimal("1.0") else "yellow" if metrics.calmar_ratio > Decimal("0") else "red"
    )
    table.add_row("Calmar Ratio", f"[{calmar_color}]{float(metrics.calmar_ratio):.2f}[/{calmar_color}]")

    table.add_row("Risk-Free Rate", _format_pct(metrics.risk_free_rate))

    return table


def _create_trade_stats_table(metrics: FullMetrics) -> Table:
    """Create trade statistics table."""
    table = Table(title="💼 Trade Statistics", show_header=False, box=None, padding=(0, 2))

    table.add_column("Metric", style="cyan")
    table.add_column("Value", justify="right")

    table.add_row("Total Trades", _format_number(metrics.total_trades))
    table.add_row("Winning Trades", f"[green]{_format_number(metrics.winning_trades)}[/green]")
    table.add_row("Losing Trades", f"[red]{_format_number(metrics.losing_trades)}[/red]")

    win_rate_color = (
        "green" if metrics.win_rate > Decimal("50") else "yellow" if metrics.win_rate > Decimal("40") else "red"
    )
    table.add_row("Win Rate", f"[{win_rate_color}]{_format_pct(metrics.win_rate)}[/{win_rate_color}]")

    if metrics.profit_factor is not None:
        pf_color = (
            "green"
            if metrics.profit_factor > Decimal("2.0")
            else "yellow"
            if metrics.profit_factor > Decimal("1.0")
            else "red"
        )
        table.add_row("Profit Factor", f"[{pf_color}]{float(metrics.profit_factor):.2f}[/{pf_color}]")
    else:
        table.add_row("Profit Factor", "N/A (no losses)")

    expectancy_color = _get_color(metrics.expectancy)
    table.add_row("Expectancy", f"[{expectancy_color}]{_format_currency(metrics.expectancy)}[/{expectancy_color}]")

    table.add_row("", "")  # Spacer
    table.add_row("Avg Win", f"[green]{_format_currency(metrics.avg_win)}[/green]")
    table.add_row("Avg Loss", f"[red]{_format_currency(metrics.avg_loss)}[/red]")
    table.add_row(
        "Largest Win",
        f"[green]{_format_currency(metrics.largest_win)} ({_format_pct(metrics.largest_win_pct)})[/green]",
    )
    table.add_row(
        "Largest Loss", f"[red]{_format_currency(metrics.largest_loss)} ({_format_pct(metrics.largest_loss_pct)})[/red]"
    )

    if metrics.avg_trade_duration_days is not None:
        table.add_row("Avg Duration", f"{float(metrics.avg_trade_duration_days):.1f} days")

    table.add_row("Max Consecutive Wins", _format_number(metrics.max_consecutive_wins))
    table.add_row("Max Consecutive Losses", _format_number(metrics.max_consecutive_losses))

    return table


def _create_period_table(periods: list[PeriodMetrics], title: str) -> Table | None:
    """Create period returns table (monthly/quarterly/annual)."""
    if not periods:
        return None

    table = Table(title=title, box=None, padding=(0, 1))

    table.add_column("Period", style="cyan")
    table.add_column("Return", justify="right")
    table.add_column("Trades", justify="right")
    table.add_column("Best", justify="right", style="green")
    table.add_column("Worst", justify="right", style="red")

    for period in periods:
        return_color = _get_color(period.return_pct)
        table.add_row(
            period.period,
            f"[{return_color}]{_format_pct(period.return_pct)}[/{return_color}]",
            _format_number(period.num_trades),
            _format_pct(period.best_trade_pct) if period.best_trade_pct else "—",
            _format_pct(period.worst_trade_pct) if period.worst_trade_pct else "—",
        )

    return table


def _create_strategy_table(strategies: list[StrategyPerformance]) -> Table | None:
    """Create per-strategy performance table."""
    if not strategies:
        return None

    table = Table(title="🎯 Strategy Performance", box=None, padding=(0, 1))

    table.add_column("Strategy", style="cyan")
    table.add_column("Return", justify="right")
    table.add_column("Trades", justify="right")
    table.add_column("Win Rate", justify="right")
    table.add_column("Sharpe", justify="right")
    table.add_column("Max DD", justify="right", style="red")

    for strategy in strategies:
        return_color = _get_color(strategy.return_pct)
        sharpe_str = f"{float(strategy.sharpe_ratio):.2f}" if strategy.sharpe_ratio else "N/A"

        table.add_row(
            strategy.strategy_id,
            f"[{return_color}]{_format_pct(strategy.return_pct)}[/{return_color}]",
            _format_number(strategy.total_trades),
            _format_pct(strategy.win_rate),
            sharpe_str,
            _format_pct(strategy.max_drawdown_pct),
        )

    return table


def _create_drawdown_table(drawdowns: list[DrawdownPeriod], max_rows: int = 5) -> Table | None:
    """Create top drawdowns table."""
    if not drawdowns:
        return None

    # Sort by depth and take top N
    sorted_drawdowns = sorted(drawdowns, key=lambda d: d.depth_pct, reverse=True)[:max_rows]

    table = Table(title=f"📉 Top {min(max_rows, len(drawdowns))} Drawdowns", box=None, padding=(0, 1))

    table.add_column("Rank", justify="right", style="dim")
    table.add_column("Depth", justify="right", style="red")
    table.add_column("Start", style="cyan")
    table.add_column("Duration", justify="right")
    table.add_column("Recovery", justify="right")
    table.add_column("Status", justify="center")

    for i, dd in enumerate(sorted_drawdowns, 1):
        status = "✅" if dd.recovered else "🔴"
        recovery_str = f"{dd.recovery_days} days" if dd.recovery_days else "—"

        table.add_row(
            str(i),
            _format_pct(dd.depth_pct),
            dd.start_timestamp.strftime("%Y-%m-%d"),
            f"{dd.duration_days} days",
            recovery_str,
            status,
        )

    return table


def display_performance_report(
    metrics: FullMetrics,
    detail_level: Literal["summary", "standard", "full"] = "standard",
    console: Console | None = None,
) -> None:
    """
    Display performance metrics in Rich-formatted console output.

    Args:
        metrics: Complete performance metrics
        detail_level: Level of detail to display:
            - "summary": Key metrics only (returns, Sharpe, max DD)
            - "standard": Summary + trade stats + risk metrics
            - "full": Everything including period breakdowns and strategies
        console: Rich Console instance (creates new if None)

    Example:
        >>> metrics = FullMetrics(...)
        >>> display_performance_report(metrics, detail_level="full")
    """
    if console is None:
        console = Console()

    console.print()  # Blank line

    # Always show summary
    console.print(_create_summary_table(metrics))
    console.print()

    if detail_level in ["standard", "full"]:
        # Risk metrics
        console.print(_create_risk_table(metrics))
        console.print()

        # Risk-adjusted returns
        console.print(_create_risk_adjusted_table(metrics))
        console.print()

        # Trade statistics (if trades exist)
        if metrics.total_trades > 0:
            console.print(_create_trade_stats_table(metrics))
            console.print()

        # Costs
        console.print(
            Panel(
                f"Total Commissions: {_format_currency(metrics.total_commissions)}\n"
                f"Commission % of P&L: {_format_pct(metrics.commission_pct_of_pnl)}",
                title="💰 Costs",
                border_style="yellow",
            )
        )
        console.print()

    if detail_level == "full":
        # Period breakdowns
        if metrics.monthly_returns:
            table = _create_period_table(metrics.monthly_returns, "📅 Monthly Returns")
            if table:
                console.print(table)
                console.print()

        if metrics.quarterly_returns:
            table = _create_period_table(metrics.quarterly_returns, "📅 Quarterly Returns")
            if table:
                console.print(table)
                console.print()

        if metrics.annual_returns:
            table = _create_period_table(metrics.annual_returns, "📅 Annual Returns")
            if table:
                console.print(table)
                console.print()

        # Strategy performance
        if metrics.strategy_performance:
            table = _create_strategy_table(metrics.strategy_performance)
            if table:
                console.print(table)
                console.print()

        # Top drawdowns
        if metrics.drawdown_periods:
            table = _create_drawdown_table(metrics.drawdown_periods)
            if table:
                console.print(table)
                console.print()

    # Final summary panel
    summary_text = Text()
    summary_text.append("🏁 Backtest Complete: ", style="bold")
    summary_text.append(
        f"{_format_currency(metrics.initial_equity)} → {_format_currency(metrics.final_equity)}", style="bold cyan"
    )
    summary_text.append(
        f" ({_format_pct(metrics.total_return_pct)})", style=f"bold {_get_color(metrics.total_return_pct)}"
    )

    console.print(Panel(summary_text, border_style="green" if metrics.total_return_pct > 0 else "red"))
    console.print()
