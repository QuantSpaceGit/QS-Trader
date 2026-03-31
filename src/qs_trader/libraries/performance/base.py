"""
Base Performance Metric Abstract Class.

All performance metrics must inherit from BaseMetric and implement the required methods.
Metrics calculate performance statistics from backtest results.

Philosophy:
- Metrics are calculation engines for performance analysis
- Metrics are stateless (compute from BacktestResult)
- Metrics are used by ReportingService
- Metrics can be combined to create comprehensive reports

Registry Name: Derived from class name (e.g., SharpeRatioMetric → "sharpe_ratio")
"""

from abc import ABC, abstractmethod
from typing import Any


class BacktestResult:
    """
    Backtest result data for metric calculation (placeholder for type hints).

    Actual implementation in qs_trader.engine.engine.
    Provides metrics access to:
    - equity_curve: Daily equity values
    - trades: List of all trades with P&L
    - positions: Position history
    - returns: Daily returns series
    - start_date: Backtest start
    - end_date: Backtest end
    - initial_equity: Starting equity
    - final_equity: Ending equity
    """

    equity_curve: list[tuple[Any, float]]  # [(datetime, equity), ...]
    trades: list[dict[str, Any]]
    returns: list[float]
    initial_equity: float
    final_equity: float
    start_date: Any
    end_date: Any


class BaseMetric(ABC):
    """
    Abstract base class for all performance metrics.

    Responsibilities:
    - Calculate performance statistics from backtest results
    - Provide interpretation/context for values
    - Support comparison across backtests
    - Validate input data

    Does NOT:
    - Store historical data (stateless computation)
    - Subscribe to events during backtest
    - Modify backtest results

    Metric Categories:

    1. Return Metrics:
       - Total Return, Annualized Return, CAGR
       - Monthly/Yearly returns

    2. Risk Metrics:
       - Volatility (Annualized Standard Deviation)
       - Maximum Drawdown, Average Drawdown
       - Value at Risk (VaR), Conditional VaR

    3. Risk-Adjusted Return:
       - Sharpe Ratio, Sortino Ratio
       - Calmar Ratio, MAR Ratio
       - Information Ratio, Treynor Ratio

    4. Trade Statistics:
       - Win Rate, Profit Factor
       - Average Win/Loss
       - Number of Trades
       - Average Trade Duration

    Example Implementation:
        ```python
        class SharpeRatioMetric(BaseMetric):
            def __init__(self, risk_free_rate: float = 0.02):
                self.risk_free_rate = risk_free_rate

            def compute(self, results: BacktestResult) -> float:
                # Calculate annualized return
                total_return = (
                    results.final_equity / results.initial_equity - 1
                )
                years = (results.end_date - results.start_date).days / 365.25
                annualized_return = (1 + total_return) ** (1/years) - 1

                # Calculate annualized volatility
                daily_returns = results.returns
                import numpy as np
                volatility = np.std(daily_returns) * np.sqrt(252)

                # Sharpe = (Return - RFR) / Volatility
                if volatility == 0:
                    return 0.0

                sharpe = (annualized_return - self.risk_free_rate) / volatility
                return sharpe

            @property
            def name(self) -> str:
                return "sharpe_ratio"

            @property
            def display_name(self) -> str:
                return "Sharpe Ratio"

            def interpretation(self, value: float) -> str:
                if value > 3:
                    return "Exceptional"
                elif value > 2:
                    return "Very Good"
                elif value > 1:
                    return "Good"
                elif value > 0:
                    return "Acceptable"
                else:
                    return "Poor"
        ```
    """

    @abstractmethod
    def __init__(self, **params: Any):
        """
        Initialize metric with parameters.

        Args:
            **params: Metric-specific parameters

        Example:
            SharpeRatio(risk_free_rate=0.02)
            MaxDrawdown(window_days=252)
            WinRate(min_profit=0.0)

        Note:
            Store parameters needed for calculation.
            Metrics should be stateless (no mutable state).
        """
        pass

    @abstractmethod
    def compute(self, results: BacktestResult) -> float:
        """
        Compute metric value from backtest results.

        Args:
            results: Backtest results containing:
                    - equity_curve: Daily equity values
                    - trades: List of all trades
                    - returns: Daily returns
                    - start_date, end_date: Date range
                    - initial_equity, final_equity: Equity values

        Returns:
            Metric value (float)

        Examples:
            # Return metrics
            >>> total_return = (final_equity / initial_equity) - 1

            # Risk metrics
            >>> max_dd = max(peak - equity for peak, equity in drawdowns)

            # Risk-adjusted
            >>> sharpe = (return - rfr) / volatility

        Note:
            - Should handle edge cases (empty trades, zero volatility)
            - Should be deterministic (same input → same output)
            - Can raise ValueError if data insufficient
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Metric identifier for registry and storage.

        Returns:
            Metric name in snake_case (e.g., "sharpe_ratio")

        Used for:
            - Registry lookup
            - JSON/CSV output keys
            - Logging and debugging

        Example:
            SharpeRatioMetric → "sharpe_ratio"
            MaxDrawdownMetric → "max_drawdown"
        """
        pass

    @property
    @abstractmethod
    def display_name(self) -> str:
        """
        Human-readable metric name for reports.

        Returns:
            Display name (e.g., "Sharpe Ratio")

        Used for:
            - Console output
            - HTML reports
            - Chart labels

        Example:
            "Sharpe Ratio"
            "Maximum Drawdown"
            "Win Rate (%)"
        """
        pass

    @property
    def category(self) -> str:
        """
        Metric category for grouping in reports.

        Returns:
            Category name: "return", "risk", "risk_adjusted", "trade"

        Default:
            Returns "other" - override to specify category

        Used for:
            - Organizing metrics in reports
            - Filtering metrics by type
            - Group display in UI
        """
        return "other"

    @property
    def format_spec(self) -> str:
        """
        Python format specification for displaying value.

        Returns:
            Format spec string (e.g., ".2f", ".2%", ",.0f")

        Default:
            Returns ".4f" (4 decimal places)

        Examples:
            ".2%" - Percentage with 2 decimals (e.g., "12.34%")
            ".4f" - Float with 4 decimals (e.g., "1.2345")
            ",.0f" - Integer with thousands separator (e.g., "1,234")

        Used by:
            - ReportingService for formatting output
            - Console display
            - CSV/JSON export with formatted strings
        """
        return ".4f"

    def interpretation(self, value: float) -> str:
        """
        Provide interpretation/context for metric value.

        Args:
            value: Computed metric value

        Returns:
            Human-readable interpretation

        Default:
            Returns empty string - override to provide context

        Examples:
            Sharpe > 2: "Very Good"
            Max Drawdown > 30%: "High Risk"
            Win Rate > 60%: "Strong Performance"

        Used for:
            - Adding context to reports
            - Highlighting concerns
            - User education
        """
        return ""

    def format_value(self, value: float) -> str:
        """
        Format metric value for display.

        Args:
            value: Raw metric value

        Returns:
            Formatted string using format_spec

        Example:
            >>> metric = SharpeRatio()
            >>> metric.format_spec = ".2f"
            >>> metric.format_value(1.2345)
            "1.23"

        Note:
            Uses format_spec property by default.
            Override for custom formatting logic.
        """
        return f"{value:{self.format_spec}}"
