"""Performance metrics calculation functions.

Pure functions for calculating performance statistics from equity curves,
returns series, and trade history. All functions are stateless and testable.

Philosophy:
- Pure functions: same inputs always produce same outputs
- No side effects: don't modify inputs or global state
- Type-safe: explicit parameter and return types
- Well-documented: clear descriptions and examples

Usage:
    >>> from qs_trader.libraries.performance import metrics
    >>> from decimal import Decimal
    >>>
    >>> # Calculate total return
    >>> total_return = metrics.calculate_total_return(
    ...     initial_equity=Decimal("100000"),
    ...     final_equity=Decimal("125000")
    ... )
    >>> # Returns: Decimal("25.00")  # 25% return
    >>>
    >>> # Calculate Sharpe ratio
    >>> sharpe = metrics.calculate_sharpe(
    ...     returns=[Decimal("0.01"), Decimal("-0.005"), Decimal("0.02")],
    ...     risk_free_rate=Decimal("0.02")
    ... )
"""

import math
from datetime import datetime
from decimal import Decimal
from typing import Sequence

from qs_trader.libraries.performance.models import DrawdownPeriod, TradeRecord


def calculate_total_return(initial_equity: Decimal, final_equity: Decimal) -> Decimal:
    """
    Calculate total return percentage.

    Args:
        initial_equity: Starting equity
        final_equity: Ending equity

    Returns:
        Total return as percentage (e.g., 25.0 for 25% return)

    Example:
        >>> calculate_total_return(Decimal("100000"), Decimal("125000"))
        Decimal('25.00')
    """
    if initial_equity == Decimal("0"):
        return Decimal("0")

    return ((final_equity / initial_equity) - Decimal("1")) * Decimal("100")


def calculate_cagr(initial_equity: Decimal, final_equity: Decimal, duration_days: int) -> Decimal:
    """
    Calculate Compound Annual Growth Rate.

    Args:
        initial_equity: Starting equity
        final_equity: Ending equity
        duration_days: Number of days in period

    Returns:
        CAGR as percentage (e.g., 15.5 for 15.5% annual growth)

    Example:
        >>> calculate_cagr(Decimal("100000"), Decimal("150000"), 730)
        Decimal('22.47')
    """
    if initial_equity == Decimal("0") or duration_days == 0:
        return Decimal("0")

    years = Decimal(duration_days) / Decimal("365.25")

    if final_equity <= Decimal("0"):
        return Decimal("-100.00")

    # Calculate (final / initial) ^ (1 / years) - 1
    ratio = float(final_equity / initial_equity)
    cagr = (ratio ** (1 / float(years))) - 1

    return Decimal(str(cagr * 100)).quantize(Decimal("0.01"))


def calculate_volatility(returns: Sequence[Decimal], annualization_factor: int = 252) -> Decimal:
    """
    Calculate annualized volatility (standard deviation of returns).

    Args:
        returns: Sequence of period returns (e.g., daily returns)
        annualization_factor: Factor to annualize (252 for daily, 52 for weekly)

    Returns:
        Annualized volatility as percentage

    Example:
        >>> returns = [Decimal("0.01"), Decimal("-0.005"), Decimal("0.02")]
        >>> calculate_volatility(returns)
        Decimal('19.93')
    """
    if not returns or len(returns) < 2:
        return Decimal("0")

    # Convert to float for calculations
    returns_float = [float(r) for r in returns]

    # Calculate mean
    mean_return = sum(returns_float) / len(returns_float)

    # Calculate variance
    variance = sum((r - mean_return) ** 2 for r in returns_float) / (len(returns_float) - 1)

    # Calculate standard deviation and annualize
    std_dev = math.sqrt(variance)
    annualized_vol = std_dev * math.sqrt(annualization_factor)

    return Decimal(str(annualized_vol * 100)).quantize(Decimal("0.01"))


def calculate_max_drawdown(equity_curve: Sequence[tuple[datetime, Decimal]]) -> Decimal:
    """
    Calculate maximum drawdown percentage.

    Args:
        equity_curve: Sequence of (timestamp, equity) tuples

    Returns:
        Maximum drawdown as positive percentage (e.g., 15.0 for 15% drawdown)

    Example:
        >>> from datetime import datetime
        >>> curve = [
        ...     (datetime(2025, 1, 1), Decimal("100000")),
        ...     (datetime(2025, 1, 2), Decimal("105000")),
        ...     (datetime(2025, 1, 3), Decimal("95000")),
        ... ]
        >>> calculate_max_drawdown(curve)
        Decimal('9.52')  # 9.52% drawdown from peak
    """
    if not equity_curve or len(equity_curve) < 2:
        return Decimal("0")

    max_dd = Decimal("0")
    peak = equity_curve[0][1]

    for _, equity in equity_curve:
        if equity > peak:
            peak = equity
        elif peak > Decimal("0"):
            dd = ((peak - equity) / peak) * Decimal("100")
            if dd > max_dd:
                max_dd = dd

    return max_dd.quantize(Decimal("0.01"))


def calculate_drawdown_periods(equity_curve: Sequence[tuple[datetime, Decimal]]) -> list[DrawdownPeriod]:
    """
    Identify all drawdown periods in equity curve.

    A drawdown period starts at a peak, reaches a trough, and ends when
    equity recovers to the peak level (or remains unrecovered).

    Args:
        equity_curve: Sequence of (timestamp, equity) tuples

    Returns:
        List of DrawdownPeriod objects

    Example:
        >>> curve = [
        ...     (datetime(2025, 1, 1), Decimal("100000")),  # Peak
        ...     (datetime(2025, 1, 2), Decimal("95000")),   # Trough
        ...     (datetime(2025, 1, 3), Decimal("100000")),  # Recovery
        ... ]
        >>> periods = calculate_drawdown_periods(curve)
        >>> len(periods)
        1
        >>> periods[0].depth_pct
        Decimal('5.00')
    """
    if not equity_curve or len(equity_curve) < 2:
        return []

    periods: list[DrawdownPeriod] = []
    peak_equity = equity_curve[0][1]
    peak_timestamp = equity_curve[0][0]
    trough_equity = peak_equity
    trough_timestamp = peak_timestamp
    in_drawdown = False
    drawdown_id = 0

    for timestamp, equity in equity_curve[1:]:
        if equity >= peak_equity:
            # New peak or recovery
            if in_drawdown:
                # Drawdown recovered
                duration_days = (trough_timestamp - peak_timestamp).days
                recovery_days = (timestamp - trough_timestamp).days
                depth_pct = ((peak_equity - trough_equity) / peak_equity * Decimal("100")).quantize(Decimal("0.01"))

                periods.append(
                    DrawdownPeriod(
                        drawdown_id=drawdown_id,
                        start_timestamp=peak_timestamp,
                        trough_timestamp=trough_timestamp,
                        end_timestamp=timestamp,
                        peak_equity=peak_equity,
                        trough_equity=trough_equity,
                        depth_pct=depth_pct,
                        duration_days=duration_days,
                        recovery_days=recovery_days,
                        recovered=True,
                    )
                )
                drawdown_id += 1
                in_drawdown = False

            peak_equity = equity
            peak_timestamp = timestamp
            trough_equity = equity
            trough_timestamp = timestamp
        elif equity < trough_equity:
            # New trough
            trough_equity = equity
            trough_timestamp = timestamp
            in_drawdown = True

    # Handle ongoing drawdown at end
    if in_drawdown:
        duration_days = (trough_timestamp - peak_timestamp).days
        depth_pct = ((peak_equity - trough_equity) / peak_equity * Decimal("100")).quantize(Decimal("0.01"))

        periods.append(
            DrawdownPeriod(
                drawdown_id=drawdown_id,
                start_timestamp=peak_timestamp,
                trough_timestamp=trough_timestamp,
                end_timestamp=None,
                peak_equity=peak_equity,
                trough_equity=trough_equity,
                depth_pct=depth_pct,
                duration_days=duration_days,
                recovery_days=None,
                recovered=False,
            )
        )

    return periods


def calculate_sharpe_ratio(
    returns: Sequence[Decimal],
    risk_free_rate: Decimal,
    annualization_factor: int = 252,
) -> Decimal:
    """
    Calculate Sharpe ratio (risk-adjusted return).

    Sharpe = (Return - RiskFreeRate) / Volatility

    Args:
        returns: Sequence of period returns
        risk_free_rate: Annual risk-free rate as decimal (e.g., 0.02 for 2%)
        annualization_factor: Factor to annualize (252 for daily returns)

    Returns:
        Sharpe ratio (dimensionless)

    Example:
        >>> returns = [Decimal("0.001") for _ in range(252)]  # Consistent 0.1% daily
        >>> calculate_sharpe_ratio(returns, Decimal("0.02"))
        Decimal('3.18')  # Good Sharpe ratio
    """
    if not returns or len(returns) < 2:
        return Decimal("0")

    # Calculate annualized return
    returns_float = [float(r) for r in returns]
    mean_return = sum(returns_float) / len(returns_float)
    annualized_return = mean_return * annualization_factor

    # Calculate volatility
    volatility = float(calculate_volatility(returns, annualization_factor)) / 100

    if volatility == 0:
        return Decimal("0")

    # Sharpe = (return - risk_free) / volatility
    sharpe = (annualized_return - float(risk_free_rate)) / volatility

    return Decimal(str(sharpe)).quantize(Decimal("0.01"))


def calculate_sortino_ratio(
    returns: Sequence[Decimal],
    risk_free_rate: Decimal,
    annualization_factor: int = 252,
) -> Decimal | None:
    """
    Calculate Sortino ratio (risk-adjusted return using downside deviation).

    Similar to Sharpe but only penalizes downside volatility.

    Args:
        returns: Sequence of period returns
        risk_free_rate: Annual risk-free rate as decimal
        annualization_factor: Factor to annualize

    Returns:
        Sortino ratio (dimensionless), or None if no downside risk exists
        (which indicates infinite Sortino - all returns are positive)

    Example:
        >>> returns = [Decimal("0.01"), Decimal("-0.005"), Decimal("0.02")]
        >>> calculate_sortino_ratio(returns, Decimal("0.02"))
        Decimal('2.45')
    """
    if not returns or len(returns) < 2:
        return Decimal("0")

    # Calculate annualized return
    returns_float = [float(r) for r in returns]
    mean_return = sum(returns_float) / len(returns_float)
    annualized_return = mean_return * annualization_factor

    # Calculate downside deviation (only negative returns)
    downside_returns = [r for r in returns_float if r < 0]

    if not downside_returns:
        # No negative returns - infinite Sortino (no downside risk)
        # Return None to indicate this special case
        return None

    downside_variance = sum(r**2 for r in downside_returns) / len(returns_float)
    downside_deviation = math.sqrt(downside_variance) * math.sqrt(annualization_factor)

    if downside_deviation == 0:
        return None

    sortino = (annualized_return - float(risk_free_rate)) / downside_deviation

    return Decimal(str(sortino)).quantize(Decimal("0.01"))


def calculate_calmar_ratio(cagr: Decimal, max_drawdown_pct: Decimal) -> Decimal:
    """
    Calculate Calmar ratio (CAGR / Max Drawdown).

    Measures return relative to downside risk.

    Args:
        cagr: Compound annual growth rate as percentage
        max_drawdown_pct: Maximum drawdown as positive percentage

    Returns:
        Calmar ratio (dimensionless)

    Example:
        >>> calculate_calmar_ratio(Decimal("20.0"), Decimal("10.0"))
        Decimal('2.00')
    """
    if max_drawdown_pct == Decimal("0"):
        return Decimal("999.99")  # Cap at reasonable value

    return (cagr / max_drawdown_pct).quantize(Decimal("0.01"))


def calculate_win_rate(trades: Sequence[TradeRecord]) -> Decimal:
    """
    Calculate win rate (percentage of profitable trades).

    Args:
        trades: Sequence of TradeRecord objects

    Returns:
        Win rate as percentage (0-100)

    Example:
        >>> trades = [
        ...     TradeRecord(pnl=Decimal("100"), ...),
        ...     TradeRecord(pnl=Decimal("-50"), ...),
        ...     TradeRecord(pnl=Decimal("200"), ...),
        ... ]
        >>> calculate_win_rate(trades)
        Decimal('66.67')
    """
    if not trades:
        return Decimal("0")

    winning_trades = sum(1 for t in trades if t.is_winner)
    win_rate = (Decimal(winning_trades) / Decimal(len(trades))) * Decimal("100")

    return win_rate.quantize(Decimal("0.01"))


def calculate_profit_factor(trades: Sequence[TradeRecord]) -> Decimal | None:
    """
    Calculate profit factor (gross profit / gross loss).

    Args:
        trades: Sequence of TradeRecord objects

    Returns:
        Profit factor (dimensionless), or None if no losing trades

    Example:
        >>> trades = [
        ...     TradeRecord(pnl=Decimal("100"), ...),
        ...     TradeRecord(pnl=Decimal("-50"), ...),
        ... ]
        >>> calculate_profit_factor(trades)
        Decimal('2.00')
    """
    if not trades:
        return None

    gross_profit = sum(t.pnl for t in trades if t.is_winner)
    gross_loss = sum(abs(t.pnl) for t in trades if not t.is_winner)

    if gross_loss == Decimal("0"):
        return None  # Can't divide by zero, no losing trades

    profit_factor = Decimal(gross_profit) / Decimal(gross_loss)
    return profit_factor.quantize(Decimal("0.01"))


def calculate_expectancy(trades: Sequence[TradeRecord]) -> Decimal:
    """
    Calculate expectancy (expected value per trade).

    Expectancy = (Win% × AvgWin) - (Loss% × AvgLoss)

    Args:
        trades: Sequence of TradeRecord objects

    Returns:
        Expected value per trade in currency units

    Example:
        >>> trades = [
        ...     TradeRecord(pnl=Decimal("100"), ...),
        ...     TradeRecord(pnl=Decimal("-50"), ...),
        ... ]
        >>> calculate_expectancy(trades)
        Decimal('25.00')
    """
    if not trades:
        return Decimal("0")

    winning_trades = [t for t in trades if t.is_winner]
    losing_trades = [t for t in trades if not t.is_winner]

    if not winning_trades and not losing_trades:
        return Decimal("0")

    win_rate = Decimal(len(winning_trades)) / Decimal(len(trades))
    loss_rate = Decimal(len(losing_trades)) / Decimal(len(trades))

    avg_win = sum(t.pnl for t in winning_trades) / Decimal(len(winning_trades)) if winning_trades else Decimal("0")
    avg_loss = abs(sum(t.pnl for t in losing_trades) / Decimal(len(losing_trades))) if losing_trades else Decimal("0")

    expectancy = (win_rate * avg_win) - (loss_rate * avg_loss)

    return expectancy.quantize(Decimal("0.01"))
