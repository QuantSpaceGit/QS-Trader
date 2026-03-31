"""Performance metrics library for backtest analysis.

This library provides comprehensive performance analysis capabilities:

1. **Models** (`models.py`): Pydantic data structures
   - TradeRecord: Round-trip trade tracking
   - DrawdownPeriod: Peak-trough-recovery analysis
   - PeriodMetrics: Monthly/quarterly/annual breakdowns
   - StrategyPerformance: Per-strategy attribution
   - FullMetrics: Complete performance report

2. **Metrics** (`metrics.py`): Pure calculation functions
   - Returns: total_return, CAGR
   - Risk: volatility, max_drawdown, drawdown_periods
   - Risk-adjusted: Sharpe, Sortino, Calmar
   - Trade stats: win_rate, profit_factor, expectancy

3. **Calculators** (`calculators.py`): Stateful incremental calculators
   - EquityCurveCalculator: Sampled equity tracking
   - DrawdownCalculator: Real-time drawdown monitoring
   - ReturnsCalculator: Period-over-period returns
   - TradeStatisticsCalculator: Running trade aggregates

Usage:
    # Pure calculations
    >>> from qs_trader.libraries.performance.metrics import calculate_sharpe_ratio
    >>> sharpe = calculate_sharpe_ratio(returns, risk_free_rate=Decimal("0.02"))

    # Incremental tracking
    >>> from qs_trader.libraries.performance.calculators import DrawdownCalculator
    >>> dd_calc = DrawdownCalculator()
    >>> dd_calc.update(timestamp, equity)
    >>> print(f"Max DD: {dd_calc.max_drawdown_pct}%")

Architecture:
    - Models: Immutable data structures (Pydantic)
    - Metrics: Stateless pure functions (testable, composable)
    - Calculators: Stateful classes (efficient incremental updates)

Design Principles:
    - Decimal precision for financial calculations
    - Explicit edge case handling (zero trades, no losses, etc.)
    - Memory-efficient sampling for large backtests
    - Composable components for custom metrics
"""

# Stateful calculators
from qs_trader.libraries.performance.calculators import (
    DrawdownCalculator,
    EquityCurveCalculator,
    ReturnsCalculator,
    TradeStatisticsCalculator,
)

# Pure calculation functions
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

# Models
from qs_trader.libraries.performance.models import (
    DrawdownPeriod,
    EquityCurvePoint,
    FullMetrics,
    PeriodMetrics,
    ReturnPoint,
    StrategyPerformance,
    TradeRecord,
)

__all__ = [
    # Models
    "TradeRecord",
    "DrawdownPeriod",
    "PeriodMetrics",
    "StrategyPerformance",
    "FullMetrics",
    "EquityCurvePoint",
    "ReturnPoint",
    # Metrics (pure functions)
    "calculate_total_return",
    "calculate_cagr",
    "calculate_volatility",
    "calculate_max_drawdown",
    "calculate_drawdown_periods",
    "calculate_sharpe_ratio",
    "calculate_sortino_ratio",
    "calculate_calmar_ratio",
    "calculate_win_rate",
    "calculate_profit_factor",
    "calculate_expectancy",
    # Calculators (stateful)
    "EquityCurveCalculator",
    "DrawdownCalculator",
    "ReturnsCalculator",
    "TradeStatisticsCalculator",
]
