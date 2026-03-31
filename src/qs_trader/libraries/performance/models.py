"""Performance metrics data models.

Pydantic models for structured performance analysis data.
These models are used by ReportingService and output writers.
"""

from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class TradeRecord(BaseModel):
    """
    Record of a completed round-trip trade.

    A round-trip trade is defined as entry + full exit (all shares closed).
    Tracks P&L, duration, and metadata for trade analysis.
    """

    trade_id: str
    strategy_id: str
    symbol: str
    entry_timestamp: datetime
    exit_timestamp: datetime
    entry_price: Decimal
    exit_price: Decimal
    quantity: int  # Positive for long, negative for short
    side: Literal["long", "short"]
    pnl: Decimal  # Profit/loss in currency units
    pnl_pct: Decimal  # Percentage return
    commission: Decimal
    duration_seconds: int

    @property
    def is_winner(self) -> bool:
        """Trade was profitable."""
        return self.pnl > Decimal("0")

    @property
    def duration_days(self) -> float:
        """Trade duration in days."""
        return self.duration_seconds / 86400


class DrawdownPeriod(BaseModel):
    """
    Record of a drawdown period (peak to trough to recovery).

    Tracks equity decline from peak, duration underwater, and recovery time.
    """

    drawdown_id: int
    start_timestamp: datetime  # Peak timestamp
    trough_timestamp: datetime  # Lowest point
    end_timestamp: datetime | None  # Recovery (None if not recovered)
    peak_equity: Decimal
    trough_equity: Decimal
    depth_pct: Decimal  # Drawdown depth as percentage
    duration_days: int  # Days from peak to trough
    recovery_days: int | None  # Days from trough to recovery (None if not recovered)
    recovered: bool

    @property
    def total_days_underwater(self) -> int | None:
        """Total days from peak to recovery."""
        if not self.recovered or self.end_timestamp is None:
            return None
        return (self.end_timestamp - self.start_timestamp).days


class PeriodMetrics(BaseModel):
    """
    Performance metrics for a specific time period (month, quarter, year).

    Used for period-by-period breakdown analysis.
    """

    period: str  # "2025-01", "2025-Q1", "2025"
    period_type: Literal["monthly", "quarterly", "annual"]
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    start_equity: Decimal
    end_equity: Decimal
    return_pct: Decimal
    num_trades: int
    winning_trades: int
    losing_trades: int
    best_trade_pct: Decimal | None
    worst_trade_pct: Decimal | None


class StrategyPerformance(BaseModel):
    """
    Performance metrics for a single strategy.

    Tracks strategy-level returns, positions, and trade statistics.
    Used for attribution analysis in multi-strategy portfolios.
    """

    strategy_id: str
    equity_allocated: Decimal
    final_equity: Decimal
    return_pct: Decimal
    num_positions: int
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: Decimal
    avg_win_pct: Decimal
    avg_loss_pct: Decimal
    profit_factor: Decimal | None  # None if no losing trades
    sharpe_ratio: Decimal | None
    max_drawdown_pct: Decimal


class FullMetrics(BaseModel):
    """
    Complete performance report with all calculated metrics.

    Generated at backtest teardown. Includes returns, risk, risk-adjusted,
    trade statistics, costs, and optional per-strategy breakdown.

    Written to performance.json in output directory.
    """

    # Backtest metadata
    backtest_id: str
    start_date: str  # YYYY-MM-DD
    end_date: str  # YYYY-MM-DD
    duration_days: int
    initial_equity: Decimal
    final_equity: Decimal

    # Returns
    total_return_pct: Decimal
    cagr: Decimal  # Compound annual growth rate
    best_day_return_pct: Decimal | None
    worst_day_return_pct: Decimal | None

    # Risk metrics
    volatility_annual_pct: Decimal  # Annualized standard deviation
    max_drawdown_pct: Decimal
    max_drawdown_duration_days: int
    avg_drawdown_pct: Decimal
    current_drawdown_pct: Decimal  # Drawdown at backtest end

    # Risk-adjusted returns
    sharpe_ratio: Decimal
    sortino_ratio: Decimal | None  # None if no downside risk (infinite Sortino)
    calmar_ratio: Decimal  # CAGR / max_drawdown
    risk_free_rate: Decimal  # Used for Sharpe/Sortino calculations

    # Trade statistics
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: Decimal  # Percentage of winning trades
    profit_factor: Decimal | None  # Gross profit / gross loss (None if no losses)
    avg_win: Decimal
    avg_loss: Decimal
    avg_win_pct: Decimal
    avg_loss_pct: Decimal
    largest_win: Decimal
    largest_loss: Decimal
    largest_win_pct: Decimal
    largest_loss_pct: Decimal
    expectancy: Decimal  # Expected value per trade
    max_consecutive_wins: int
    max_consecutive_losses: int
    avg_trade_duration_days: Decimal | None

    # Costs
    total_commissions: Decimal
    commission_pct_of_pnl: Decimal  # Commissions as % of total P&L

    # Period breakdowns (decision 3: always calculate)
    monthly_returns: list[PeriodMetrics] = Field(default_factory=list)
    quarterly_returns: list[PeriodMetrics] = Field(default_factory=list)
    annual_returns: list[PeriodMetrics] = Field(default_factory=list)

    # Per-strategy metrics (decision 4: both portfolio and per-strategy)
    strategy_performance: list[StrategyPerformance] = Field(default_factory=list)

    # Drawdown history
    drawdown_periods: list[DrawdownPeriod] = Field(default_factory=list)

    # Benchmark (decision 1: extensible but not implemented yet)
    benchmark_symbol: str | None = None
    benchmark_return_pct: Decimal | None = None
    beta: Decimal | None = None
    alpha: Decimal | None = None
    correlation: Decimal | None = None
    tracking_error: Decimal | None = None


class EquityCurvePoint(BaseModel):
    """
    Single point on the equity curve.

    Used for Parquet time-series output and visualization.
    """

    timestamp: datetime
    equity: Decimal
    cash: Decimal
    positions_value: Decimal
    num_positions: int
    gross_exposure: Decimal
    net_exposure: Decimal
    leverage: Decimal
    drawdown_pct: Decimal
    underwater: bool  # True if currently in drawdown


class ReturnPoint(BaseModel):
    """
    Single period return data point.

    Used for Parquet time-series output and statistical analysis.
    """

    timestamp: datetime
    period_return: Decimal  # Single-period return
    cumulative_return: Decimal  # Cumulative from start
    log_return: Decimal  # Natural log return for statistical calculations
