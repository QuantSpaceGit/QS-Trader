"""Stateful performance calculators for incremental updates.

Calculators maintain state and update incrementally as new data arrives.
Used by ReportingService to track metrics during backtest execution.

Philosophy:
- Stateful: Maintain internal state between updates
- Incremental: Efficient updates without recalculating from scratch
- Composable: Calculators can be combined for complex metrics
- Testable: Clear state transitions and observable outputs

Usage:
    >>> from qs_trader.libraries.performance.calculators import DrawdownCalculator
    >>> from decimal import Decimal
    >>>
    >>> calc = DrawdownCalculator()
    >>> calc.update(timestamp, Decimal("100000"))
    >>> calc.update(timestamp2, Decimal("95000"))  # Drawdown starts
    >>> calc.current_drawdown_pct
    Decimal('5.00')
    >>> calc.max_drawdown_pct
    Decimal('5.00')
"""

from datetime import datetime
from decimal import Decimal

from qs_trader.libraries.performance.models import DrawdownPeriod, TradeRecord


class EquityCurveCalculator:
    """
    Tracks equity curve with optional sampling for memory efficiency.

    For intraday backtests with thousands of bars, stores sampled points
    rather than every update to limit memory usage.
    """

    def __init__(self, max_points: int = 10_000):
        """
        Initialize equity curve calculator.

        Args:
            max_points: Maximum number of points to store (default: 10,000)
                       If exceeded, will intelligently sample to maintain
                       important points (peaks, troughs, regime changes)
        """
        self._max_points = max_points
        self._points: list[tuple[datetime, Decimal]] = []
        self._sample_interval = 1
        self._counter = 0

    def update(self, timestamp: datetime, equity: Decimal) -> None:
        """
        Add new equity point to curve.

        If max_points exceeded, will sample intelligently.

        Args:
            timestamp: Point timestamp
            equity: Equity value
        """
        self._counter += 1

        # Always store if below max or if it's a sample point
        if len(self._points) < self._max_points or self._counter % self._sample_interval == 0:
            self._points.append((timestamp, equity))

            # Adjust sample interval if approaching max
            if len(self._points) >= self._max_points * 0.9:
                self._sample_interval += 1

    def get_curve(self) -> list[tuple[datetime, Decimal]]:
        """
        Get equity curve points.

        Returns:
            List of (timestamp, equity) tuples
        """
        return self._points.copy()

    def latest_equity(self) -> Decimal | None:
        """Get most recent equity value."""
        if not self._points:
            return None
        return self._points[-1][1]

    def latest_timestamp(self) -> datetime | None:
        """Get most recent timestamp."""
        if not self._points:
            return None
        return self._points[-1][0]

    def __len__(self) -> int:
        """Number of points in curve."""
        return len(self._points)


class DrawdownCalculator:
    """
    Tracks drawdown metrics incrementally.

    Maintains peak equity and calculates current/max drawdown on each update.
    Identifies drawdown periods for detailed analysis.
    """

    def __init__(self) -> None:
        """Initialize drawdown calculator."""
        self._peak_equity = Decimal("0")
        self._peak_timestamp: datetime | None = None
        self._max_drawdown_pct = Decimal("0")
        self._current_drawdown_pct = Decimal("0")
        self._trough_equity = Decimal("0")
        self._trough_timestamp: datetime | None = None
        self._in_drawdown = False
        self._drawdown_periods: list[DrawdownPeriod] = []
        self._drawdown_id_counter = 0

    def update(self, timestamp: datetime, equity: Decimal) -> None:
        """
        Update drawdown calculations with new equity value.

        Args:
            timestamp: Current timestamp
            equity: Current equity value
        """
        # Initialize peak on first update
        if self._peak_equity == Decimal("0"):
            self._peak_equity = equity
            self._peak_timestamp = timestamp
            self._trough_equity = equity
            self._trough_timestamp = timestamp
            return

        # Check for new peak (recovery or new high)
        if equity >= self._peak_equity:
            if self._in_drawdown:
                # Drawdown recovered - record the period
                self._record_drawdown_period(timestamp, recovered=True)
                self._in_drawdown = False

            self._peak_equity = equity
            self._peak_timestamp = timestamp
            self._trough_equity = equity
            self._trough_timestamp = timestamp
            self._current_drawdown_pct = Decimal("0")

        # Check for new trough
        elif equity < self._trough_equity:
            self._trough_equity = equity
            self._trough_timestamp = timestamp
            self._in_drawdown = True

            # Update current drawdown
            self._current_drawdown_pct = ((self._peak_equity - equity) / self._peak_equity * Decimal("100")).quantize(
                Decimal("0.01")
            )

            # Update max drawdown if necessary
            if self._current_drawdown_pct > self._max_drawdown_pct:
                self._max_drawdown_pct = self._current_drawdown_pct

        # Equity between peak and trough - update current drawdown
        elif self._in_drawdown:
            self._current_drawdown_pct = ((self._peak_equity - equity) / self._peak_equity * Decimal("100")).quantize(
                Decimal("0.01")
            )

    def _record_drawdown_period(self, recovery_timestamp: datetime, recovered: bool) -> None:
        """Record a completed or ongoing drawdown period."""
        if self._peak_timestamp is None or self._trough_timestamp is None:
            return

        duration_days = (self._trough_timestamp - self._peak_timestamp).days
        recovery_days = (recovery_timestamp - self._trough_timestamp).days if recovered else None
        depth_pct = self._current_drawdown_pct

        period = DrawdownPeriod(
            drawdown_id=self._drawdown_id_counter,
            start_timestamp=self._peak_timestamp,
            trough_timestamp=self._trough_timestamp,
            end_timestamp=recovery_timestamp if recovered else None,
            peak_equity=self._peak_equity,
            trough_equity=self._trough_equity,
            depth_pct=depth_pct,
            duration_days=duration_days,
            recovery_days=recovery_days,
            recovered=recovered,
        )

        self._drawdown_periods.append(period)
        self._drawdown_id_counter += 1

    def finalize(self, final_timestamp: datetime) -> None:
        """
        Finalize drawdown calculation at backtest end.

        If still in drawdown, record it as unrecovered.

        Args:
            final_timestamp: Backtest end timestamp
        """
        if self._in_drawdown:
            self._record_drawdown_period(final_timestamp, recovered=False)

    @property
    def current_drawdown_pct(self) -> Decimal:
        """Current drawdown percentage."""
        return Decimal(self._current_drawdown_pct)

    @property
    def max_drawdown_pct(self) -> Decimal:
        """Maximum drawdown percentage observed."""
        return Decimal(self._max_drawdown_pct)

    @property
    def peak_equity(self) -> Decimal:
        """Current peak equity."""
        return Decimal(self._peak_equity)

    @property
    def drawdown_periods(self) -> list[DrawdownPeriod]:
        """All recorded drawdown periods."""
        return self._drawdown_periods.copy()

    @property
    def is_underwater(self) -> bool:
        """True if currently in drawdown."""
        return bool(self._in_drawdown)


class ReturnsCalculator:
    """
    Calculates period-over-period returns incrementally.

    Tracks returns series for statistical analysis (Sharpe, Sortino, etc.).
    """

    def __init__(self) -> None:
        """Initialize returns calculator."""
        self._returns: list[Decimal] = []
        self._prev_equity: Decimal | None = None

    def update(self, equity: Decimal) -> Decimal | None:
        """
        Calculate return since last update.

        Args:
            equity: Current equity value

        Returns:
            Period return as decimal (e.g., 0.01 for 1% return),
            or None if this is the first update
        """
        if self._prev_equity is None:
            self._prev_equity = equity
            return None

        if self._prev_equity == Decimal("0"):
            period_return = Decimal("0")
        else:
            period_return = (equity / self._prev_equity) - Decimal("1")

        self._returns.append(period_return)
        self._prev_equity = equity

        return period_return

    @property
    def returns(self) -> list[Decimal]:
        """All calculated returns."""
        return self._returns.copy()

    @property
    def cumulative_return(self) -> Decimal:
        """Cumulative return from start."""
        if not self._returns:
            return Decimal("0")

        cumulative = Decimal("1")
        for r in self._returns:
            cumulative *= Decimal("1") + r

        return cumulative - Decimal("1")

    def __len__(self) -> int:
        """Number of return periods."""
        return len(self._returns)


class TradeStatisticsCalculator:
    """
    Tracks trade statistics incrementally.

    Maintains running counts and aggregates for win rate, profit factor,
    consecutive wins/losses, etc.
    """

    def __init__(self) -> None:
        """Initialize trade statistics calculator."""
        self._trades: list[TradeRecord] = []
        self._total_trades = 0
        self._winning_trades = 0
        self._losing_trades = 0
        self._consecutive_wins = 0
        self._consecutive_losses = 0
        self._max_consecutive_wins = 0
        self._max_consecutive_losses = 0

    def add_trade(self, trade: TradeRecord) -> None:
        """
        Add completed trade to statistics.

        Args:
            trade: TradeRecord object
        """
        self._trades.append(trade)
        self._total_trades += 1

        if trade.is_winner:
            self._winning_trades += 1
            self._consecutive_wins += 1
            self._consecutive_losses = 0

            if self._consecutive_wins > self._max_consecutive_wins:
                self._max_consecutive_wins = self._consecutive_wins
        else:
            self._losing_trades += 1
            self._consecutive_losses += 1
            self._consecutive_wins = 0

            if self._consecutive_losses > self._max_consecutive_losses:
                self._max_consecutive_losses = self._consecutive_losses

    @property
    def total_trades(self) -> int:
        """Total number of trades."""
        return int(self._total_trades)

    @property
    def winning_trades(self) -> int:
        """Number of winning trades."""
        return int(self._winning_trades)

    @property
    def losing_trades(self) -> int:
        """Number of losing trades."""
        return int(self._losing_trades)

    @property
    def win_rate(self) -> Decimal:
        """Win rate as percentage (0-100)."""
        if self._total_trades == 0:
            return Decimal("0")

        return (Decimal(self._winning_trades) / Decimal(self._total_trades) * Decimal("100")).quantize(Decimal("0.01"))

    @property
    def max_consecutive_wins(self) -> int:
        """Maximum consecutive winning trades."""
        return int(self._max_consecutive_wins)

    @property
    def max_consecutive_losses(self) -> int:
        """Maximum consecutive losing trades."""
        return int(self._max_consecutive_losses)

    @property
    def trades(self) -> list[TradeRecord]:
        """All recorded trades."""
        return self._trades.copy()

    @property
    def gross_profit(self) -> Decimal:
        """Total profit from winning trades."""
        return Decimal(sum(t.pnl for t in self._trades if t.is_winner))

    @property
    def gross_loss(self) -> Decimal:
        """Total loss from losing trades (as positive number)."""
        return Decimal(sum(abs(t.pnl) for t in self._trades if not t.is_winner))

    @property
    def largest_win(self) -> Decimal:
        """Largest winning trade P&L."""
        winners = [t.pnl for t in self._trades if t.is_winner]
        return max(winners) if winners else Decimal("0")

    @property
    def largest_loss(self) -> Decimal:
        """Largest losing trade P&L (as negative number)."""
        losers = [t.pnl for t in self._trades if not t.is_winner]
        return min(losers) if losers else Decimal("0")


class PeriodAggregationCalculator:
    """
    Aggregates performance metrics by time period (monthly, quarterly, annual).

    Tracks equity points and trades, then aggregates them into period-level
    metrics for performance analysis and reporting.
    """

    def __init__(self) -> None:
        """Initialize period aggregation calculator."""
        self._equity_points: list[tuple[datetime, Decimal]] = []
        self._trades: list[TradeRecord] = []

    def update(self, timestamp: datetime, equity: Decimal) -> None:
        """
        Add equity point for period tracking.

        Args:
            timestamp: Point timestamp
            equity: Equity value
        """
        self._equity_points.append((timestamp, equity))

    def add_trade(self, trade: TradeRecord) -> None:
        """
        Add completed trade to period tracking.

        Args:
            trade: TradeRecord object
        """
        self._trades.append(trade)

    def calculate_periods(
        self,
        period_type: str,
        start_equity: Decimal,
    ) -> list:
        """
        Calculate aggregated metrics for each period.

        Args:
            period_type: "monthly", "quarterly", or "annual"
            start_equity: Initial equity for return calculations

        Returns:
            List of PeriodMetrics for each period
        """
        from qs_trader.libraries.performance.models import PeriodMetrics

        if not self._equity_points:
            return []

        # Group equity points by period
        periods: dict[str, dict] = {}

        for timestamp, equity in self._equity_points:
            period_key = self._get_period_key(timestamp, period_type)

            if period_key not in periods:
                periods[period_key] = {
                    "timestamps": [],
                    "equities": [],
                    "trades": [],
                }

            periods[period_key]["timestamps"].append(timestamp)
            periods[period_key]["equities"].append(equity)

        # Assign trades to periods
        for trade in self._trades:
            period_key = self._get_period_key(trade.exit_timestamp, period_type)
            if period_key in periods:
                periods[period_key]["trades"].append(trade)

        # Calculate metrics for each period
        results: list[PeriodMetrics] = []
        prev_equity = start_equity

        for period_key in sorted(periods.keys()):
            data = periods[period_key]
            start_date = min(data["timestamps"])
            end_date = max(data["timestamps"])
            start_period_equity = data["equities"][0] if prev_equity is None else prev_equity
            end_period_equity = data["equities"][-1]

            # Calculate return
            if start_period_equity > Decimal("0"):
                return_pct = (
                    (end_period_equity - start_period_equity) / start_period_equity * Decimal("100")
                ).quantize(Decimal("0.01"))
            else:
                return_pct = Decimal("0")

            # Trade statistics
            period_trades = data["trades"]
            winning = [t for t in period_trades if t.is_winner]
            losing = [t for t in period_trades if not t.is_winner]

            best_trade_pct = max((t.pnl_pct for t in period_trades), default=None)
            worst_trade_pct = min((t.pnl_pct for t in period_trades), default=None)

            metric = PeriodMetrics(
                period=period_key,
                period_type=period_type,  # type: ignore
                start_date=start_date.date().isoformat(),
                end_date=end_date.date().isoformat(),
                start_equity=start_period_equity,
                end_equity=end_period_equity,
                return_pct=return_pct,
                num_trades=len(period_trades),
                winning_trades=len(winning),
                losing_trades=len(losing),
                best_trade_pct=best_trade_pct,
                worst_trade_pct=worst_trade_pct,
            )

            results.append(metric)
            prev_equity = end_period_equity

        return results

    def _get_period_key(self, timestamp: datetime, period_type: str) -> str:
        """
        Get period identifier for a timestamp.

        Args:
            timestamp: Datetime to categorize
            period_type: "monthly", "quarterly", or "annual"

        Returns:
            Period key (e.g., "2024-01", "2024-Q1", "2024")
        """
        if period_type == "monthly":
            return timestamp.strftime("%Y-%m")
        elif period_type == "quarterly":
            quarter = (timestamp.month - 1) // 3 + 1
            return f"{timestamp.year}-Q{quarter}"
        elif period_type == "annual":
            return str(timestamp.year)
        else:
            raise ValueError(f"Invalid period_type: {period_type}")


class StrategyPerformanceCalculator:
    """
    Tracks per-strategy performance metrics for multi-strategy portfolios.

    Maintains strategy-level equity curves, trade attribution, and calculates
    strategy-specific performance metrics.
    """

    def __init__(self, strategy_ids: list[str]) -> None:
        """
        Initialize strategy performance calculator.

        Args:
            strategy_ids: List of strategy identifiers to track
        """
        self._strategy_ids = strategy_ids
        # Strategy equity tracking
        self._strategy_equity: dict[str, list[tuple[datetime, Decimal]]] = {sid: [] for sid in strategy_ids}
        # Strategy trade tracking
        self._strategy_trades: dict[str, list[TradeRecord]] = {sid: [] for sid in strategy_ids}
        # Initial allocations
        self._allocations: dict[str, Decimal] = {sid: Decimal("0") for sid in strategy_ids}

    def update_allocation(self, strategy_id: str, allocated_capital: Decimal) -> None:
        """
        Set or update capital allocation for a strategy.

        Args:
            strategy_id: Strategy identifier
            allocated_capital: Capital allocated to this strategy
        """
        if strategy_id not in self._strategy_ids:
            self._strategy_ids.append(strategy_id)
            self._strategy_equity[strategy_id] = []
            self._strategy_trades[strategy_id] = []

        self._allocations[strategy_id] = allocated_capital

    def update_equity(self, strategy_id: str, timestamp: datetime, equity: Decimal) -> None:
        """
        Update equity for a specific strategy.

        Args:
            strategy_id: Strategy identifier
            timestamp: Equity snapshot timestamp
            equity: Strategy equity value
        """
        if strategy_id not in self._strategy_equity:
            self._strategy_equity[strategy_id] = []

        self._strategy_equity[strategy_id].append((timestamp, equity))

    def add_trade(self, trade: TradeRecord) -> None:
        """
        Assign trade to its originating strategy.

        Args:
            trade: TradeRecord with strategy_id field
        """
        strategy_id = trade.strategy_id
        if strategy_id not in self._strategy_trades:
            self._strategy_trades[strategy_id] = []

        self._strategy_trades[strategy_id].append(trade)

    def calculate_performance(self) -> list:
        """
        Calculate performance metrics for each strategy.

        Returns:
            List of StrategyPerformance for each tracked strategy
        """
        from qs_trader.libraries.performance.metrics import (
            calculate_profit_factor,
            calculate_sharpe_ratio,
            calculate_win_rate,
        )
        from qs_trader.libraries.performance.models import StrategyPerformance

        results: list[StrategyPerformance] = []

        for strategy_id in self._strategy_ids:
            equity_points = self._strategy_equity.get(strategy_id, [])
            trades = self._strategy_trades.get(strategy_id, [])
            allocated = self._allocations.get(strategy_id, Decimal("0"))

            if not equity_points:
                continue

            # Get final equity
            final_equity = equity_points[-1][1] if equity_points else allocated

            # Calculate return
            if allocated > Decimal("0"):
                return_pct = ((final_equity - allocated) / allocated * Decimal("100")).quantize(Decimal("0.01"))
            else:
                return_pct = Decimal("0")

            # Calculate drawdown for this strategy
            max_dd = self._calculate_max_drawdown(equity_points)

            # Trade statistics
            winning = [t for t in trades if t.is_winner]
            losing = [t for t in trades if not t.is_winner]

            avg_win_pct = Decimal(str(sum(t.pnl_pct for t in winning) / len(winning))) if winning else Decimal("0")
            avg_loss_pct = Decimal(str(sum(t.pnl_pct for t in losing) / len(losing))) if losing else Decimal("0")

            win_rate = calculate_win_rate(trades) if trades else Decimal("0")
            profit_factor = calculate_profit_factor(trades) if trades else None

            # Calculate strategy returns for Sharpe
            returns = self._calculate_returns(equity_points)
            sharpe = calculate_sharpe_ratio(returns, Decimal("0")) if len(returns) >= 2 else None

            # Count open positions (this will be updated from portfolio state)
            num_positions = 0  # Will be populated by ReportingService from PortfolioStateEvent

            performance = StrategyPerformance(
                strategy_id=strategy_id,
                equity_allocated=allocated,
                final_equity=final_equity,
                return_pct=return_pct,
                num_positions=num_positions,
                total_trades=len(trades),
                winning_trades=len(winning),
                losing_trades=len(losing),
                win_rate=win_rate,
                avg_win_pct=avg_win_pct,
                avg_loss_pct=avg_loss_pct,
                profit_factor=profit_factor,
                sharpe_ratio=sharpe,
                max_drawdown_pct=max_dd,
            )

            results.append(performance)

        return results

    def _calculate_max_drawdown(self, equity_points: list[tuple[datetime, Decimal]]) -> Decimal:
        """Calculate maximum drawdown from equity curve."""
        if not equity_points:
            return Decimal("0")

        peak = equity_points[0][1]
        max_dd = Decimal("0")

        for _, equity in equity_points:
            if equity > peak:
                peak = equity
            elif peak > Decimal("0"):
                dd = ((peak - equity) / peak * Decimal("100")).quantize(Decimal("0.01"))
                if dd > max_dd:
                    max_dd = dd

        return max_dd

    def _calculate_returns(self, equity_points: list[tuple[datetime, Decimal]]) -> list[Decimal]:
        """Calculate period returns from equity curve."""
        if len(equity_points) < 2:
            return []

        returns: list[Decimal] = []
        for i in range(1, len(equity_points)):
            prev_equity = equity_points[i - 1][1]
            curr_equity = equity_points[i][1]

            if prev_equity > Decimal("0"):
                ret = (curr_equity / prev_equity) - Decimal("1")
                returns.append(ret)

        return returns
