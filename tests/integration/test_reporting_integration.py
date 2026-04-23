"""Integration tests for ReportingService with BacktestEngine.

Tests the full end-to-end flow of reporting service integration using fixtures.
All tests are self-contained and don't depend on user-modifiable config files.
"""

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import MagicMock, Mock, patch

from qs_trader.engine.engine import BacktestEngine
from qs_trader.events.event_bus import EventBus
from qs_trader.events.event_store import InMemoryEventStore
from qs_trader.events.events import (
    FeatureBarEvent,
    FillEvent,
    IndicatorEvent,
    OrderEvent,
    PriceBarEvent,
    SignalEvent,
    TradeEvent,
)
from qs_trader.events.price_basis import PriceBasis
from qs_trader.services.reporting.config import ReportingConfig
from qs_trader.services.reporting.manifest import ClickHouseInputManifest
from qs_trader.services.reporting.service import ReportingService


def _integration_audit_event_store() -> InMemoryEventStore:
    store = InMemoryEventStore()
    timestamp = "2024-01-02T21:00:00+00:00"
    fill_id = "550e8400-e29b-41d4-a716-446655440099"

    store.append(
        PriceBarEvent(
            symbol="AAPL",
            timestamp=timestamp,
            interval="1d",
            open=Decimal("100.00"),
            high=Decimal("101.00"),
            low=Decimal("99.50"),
            close=Decimal("100.75"),
            volume=1000,
            volume_raw=1200,
            volume_adj=1000,
            source="integration_test",
        )
    )
    store.append(FeatureBarEvent(timestamp=timestamp, symbol="AAPL", features={"alpha": Decimal("1.25")}))
    store.append(
        IndicatorEvent(
            strategy_id="sma_crossover",
            symbol="AAPL",
            timestamp=timestamp,
            indicators={"SMA(10)": Decimal("101.50")},
        )
    )
    store.append(
        SignalEvent(
            signal_id="signal-550e8400-e29b-41d4-a716-446655440001",
            timestamp=timestamp,
            strategy_id="sma_crossover",
            symbol="AAPL",
            intention="OPEN_LONG",
            price=Decimal("100.50"),
            confidence=Decimal("0.85"),
            reason="golden cross",
        )
    )
    store.append(
        OrderEvent(
            intent_id="signal-550e8400-e29b-41d4-a716-446655440001",
            idempotency_key="order-key-1",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            quantity=Decimal("10"),
            order_type="market",
            source_strategy_id="sma_crossover",
        )
    )
    store.append(
        FillEvent(
            fill_id=fill_id,
            source_order_id="order-001",
            timestamp=timestamp,
            symbol="AAPL",
            side="buy",
            filled_quantity=Decimal("10"),
            fill_price=Decimal("100.60"),
            commission=Decimal("1.25"),
            slippage_bps=4,
            strategy_id="sma_crossover",
        )
    )
    store.append(
        TradeEvent(
            trade_id="T00042",
            timestamp=timestamp,
            strategy_id="sma_crossover",
            symbol="AAPL",
            status="closed",
            side="long",
            fills=[fill_id],
            entry_price=Decimal("100.60"),
            exit_price=Decimal("104.00"),
            current_quantity=Decimal("0"),
            realized_pnl=Decimal("34.00"),
            commission_total=Decimal("1.25"),
            entry_timestamp=timestamp,
            exit_timestamp="2024-01-10T00:00:00Z",
        )
    )
    return store


def _integration_manifest(
    *,
    database: str = "market",
    bars_table: str = "as_us_equity_ohlc_daily",
) -> ClickHouseInputManifest:
    return ClickHouseInputManifest(
        source_name="qs-datamaster-equity-1d",
        database=database,
        bars_table=bars_table,
        symbols=("AAPL",),
        start_date=date(2024, 1, 2),
        end_date=date(2024, 1, 2),
        price_basis=PriceBasis.ADJUSTED,
    )


class TestReportingIntegration:
    """Integration tests for reporting service."""

    def test_backtest_generates_performance_json(self, buy_hold_backtest_config, mock_system_config):
        """Test that backtest generates performance.json output.

        Uses programmatically-created config, not external files.
        Writes to temporary directory, not user-facing output folder.
        """
        config = buy_hold_backtest_config

        # Create engine with mocked system config
        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)

            # Run backtest
            result = engine.run()

        # Check result
        assert result.bars_processed > 0

        # Find the output directory - use temp dir from mock config
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        assert output_base.exists()

        # Should have runs/ directory with timestamped subdirectories
        runs_dir = output_base / "runs"
        assert runs_dir.exists()
        timestamped_dirs = list(runs_dir.iterdir())
        assert len(timestamped_dirs) > 0

        latest_dir = max(timestamped_dirs, key=lambda p: p.name)

        # Check for performance.json
        perf_json = latest_dir / "performance.json"
        assert perf_json.exists(), f"performance.json not found in {latest_dir}"

        # Load and validate JSON
        with open(perf_json) as f:
            perf_data = json.load(f)

        # Validate key fields
        assert "backtest_id" in perf_data
        assert "total_return_pct" in perf_data
        assert "cagr" in perf_data
        assert "sharpe_ratio" in perf_data
        assert "total_trades" in perf_data

        # Validate metrics are reasonable
        assert float(perf_data["total_return_pct"]) != 0.0  # Should have returns
        assert int(perf_data["total_trades"]) >= 0  # Should have trade count

        # Cleanup
        engine.shutdown()

    def test_backtest_generates_timeseries_json(self, buy_hold_backtest_config, mock_system_config):
        """Test that backtest generates time-series JSON files.

        Uses programmatically-created config, not external files.
        Writes to temporary directory, not user-facing output folder.
        """
        config = buy_hold_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            result = engine.run()

        assert result.bars_processed > 0

        # Find output directory - use temp dir from mock config
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)

        # Check for timeseries directory
        ts_dir = latest_dir / "timeseries"
        assert ts_dir.exists(), f"timeseries directory not found in {latest_dir}"

        # Check for core expected JSON files
        # Note: trades.json is only generated if there are closed trades
        expected_files = [
            "equity_curve.json",
            "returns.json",
            "drawdowns.json",
        ]

        for filename in expected_files:
            filepath = ts_dir / filename
            assert filepath.exists(), f"{filename} not found in {ts_dir}"
            assert filepath.stat().st_size > 0, f"{filename} is empty"

        # trades.json is optional (only generated if there are closed trades)
        trades_file = ts_dir / "trades.json"
        if trades_file.exists():
            assert trades_file.stat().st_size > 0, "trades.json is empty"  # Cleanup
        engine.shutdown()

    def test_backtest_event_store_and_reporting_same_directory(self, buy_hold_backtest_config, mock_system_config):
        """Test that event store and reporting outputs are in same timestamped directory.

        Uses programmatically-created config, not external files.
        Writes to temporary directory, not user-facing output folder.
        """
        config = buy_hold_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            result = engine.run()

        assert result.bars_processed > 0

        # Find output directory - use temp dir from mock config
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)

        # Check that both event store and reporting are in same directory
        # (assuming parquet event store is configured in system.yaml)

        # Reporting outputs
        perf_json = latest_dir / "performance.json"
        ts_dir = latest_dir / "timeseries"

        assert perf_json.exists()
        assert ts_dir.exists()

        # Event store output (if using parquet backend)
        events_parquet = latest_dir / "events.parquet"
        if events_parquet.exists():
            # If event store is enabled, verify it's in same directory
            assert events_parquet.parent == perf_json.parent
            assert events_parquet.stat().st_size > 0

        # Cleanup
        engine.shutdown()

    def test_performance_metrics_accuracy(self, buy_hold_backtest_config, mock_system_config):
        """Test that calculated metrics match expected values for known backtest.

        Uses programmatically-created config, not external files.
        Writes to temporary directory, not user-facing output folder.
        """
        config = buy_hold_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            _ = engine.run()

        # Find performance.json - use temp dir from mock config
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)
        perf_json = latest_dir / "performance.json"

        with open(perf_json) as f:
            metrics = json.load(f)

        # Validate metric relationships and sanity checks
        total_return = float(metrics["total_return_pct"])
        cagr = float(metrics["cagr"])

        # If positive return, CAGR should also be positive
        if total_return > 0:
            assert cagr > 0, "CAGR should be positive with positive returns"

        # Sharpe ratio should be reasonable
        sharpe = float(metrics["sharpe_ratio"])
        assert -10 < sharpe < 10, f"Sharpe ratio {sharpe} seems unreasonable"

        # Trade statistics should be consistent
        total_trades = int(metrics["total_trades"])
        winning_trades = int(metrics["winning_trades"])
        losing_trades = int(metrics["losing_trades"])

        assert total_trades == winning_trades + losing_trades, "Total trades should equal winning + losing trades"

        if total_trades > 0:
            win_rate = float(metrics["win_rate"])
            expected_win_rate = (winning_trades / total_trades) * 100
            assert abs(win_rate - expected_win_rate) < 0.01, (
                f"Win rate {win_rate} doesn't match expected {expected_win_rate}"
            )

        # Cleanup
        engine.shutdown()

    def test_reporting_handles_zero_trades(self, zero_trades_backtest_config, mock_system_config):
        """Test that reporting handles backtests with zero trades gracefully.

        Uses config with empty strategy universe to generate zero trades.
        Writes to temporary directory, not user-facing output folder.
        """
        config = zero_trades_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            result = engine.run()

        assert result.bars_processed > 0, "Should have processed bars"

        # Find output directory - use temp dir from mock config
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)

        # Check performance.json exists
        perf_json = latest_dir / "performance.json"
        assert perf_json.exists()

        with open(perf_json) as f:
            metrics = json.load(f)

        # Validate zero trades scenario
        assert int(metrics["total_trades"]) == 0, "Should have zero trades"
        assert int(metrics["winning_trades"]) == 0
        assert int(metrics["losing_trades"]) == 0
        assert float(metrics["win_rate"]) == 0.0

        # Note: Returns may be non-zero due to dividends/corporate actions
        # The key is that trade statistics are correctly handled at zero
        total_return = float(metrics["total_return_pct"])
        assert -100 < total_return < 100, "Return should be reasonable even with zero trades"

        # Cleanup
        engine.shutdown()

    def test_total_commissions_includes_open_positions(self, buy_hold_backtest_config, mock_system_config):
        """Test that total_commissions includes commissions from open positions.

        Buy-and-hold strategy opens a position but never closes it.
        The commission from the entry fill should still be reported in total_commissions.

        Regression test for bug where total_commissions only summed from closed trades,
        showing $0.00 for strategies with only open positions.
        """
        config = buy_hold_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            result = engine.run()

        assert result.bars_processed > 0

        # Find output directory
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)

        # Load performance.json
        perf_json = latest_dir / "performance.json"
        with open(perf_json) as f:
            metrics = json.load(f)

        # Buy-and-hold with default commission ($0.005/share, $1.00 min) should have non-zero commission
        # The strategy buys ~$100k of AAPL at ~$100-500/share = ~200-1000 shares
        # Commission = max(shares * $0.005, $1.00) > $1.00
        total_commissions = float(metrics["total_commissions"])
        assert total_commissions > 0, (
            f"total_commissions should be > 0 for buy-and-hold with open position, got {total_commissions}"
        )

        # Verify no closed trades (position is still open)
        total_trades = int(metrics["total_trades"])
        assert total_trades == 0, f"Buy-and-hold should have 0 closed trades, got {total_trades}"

        # The fact that total_commissions > 0 while total_trades == 0 proves
        # we're correctly including commissions from open positions
        engine.shutdown()

    def test_reporting_handles_large_backtest(self, buy_hold_backtest_config, mock_system_config):
        """Test that reporting correctly samples equity curve for large backtests.

        Verifies max_equity_points sampling works correctly.
        Writes to temporary directory, not user-facing output folder.
        """
        # Extend date range for more data points
        config = buy_hold_backtest_config
        config.reporting.max_equity_points = 50  # Force sampling

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            result = engine.run()

        assert result.bars_processed > 0

        # Find output - use temp dir from mock config
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)

        # Check equity curve was sampled
        import json

        equity_file = latest_dir / "timeseries" / "equity_curve.json"
        with open(equity_file) as f:
            data = json.load(f)
        row_count = len(data)

        # Should be <= max_equity_points (plus a few for peaks/troughs)
        assert row_count <= config.reporting.max_equity_points + 10, f"Equity curve should be sampled: {row_count} rows"

        # Cleanup
        engine.shutdown()

    def test_performance_json_contains_period_aggregations(self, buy_hold_backtest_config, mock_system_config):
        """Test that performance.json includes period aggregation metrics.

        Validates that monthly, quarterly, and annual returns are populated
        with correct structure and data types.
        """
        config = buy_hold_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            _ = engine.run()

        # Find performance.json
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)
        perf_json = latest_dir / "performance.json"

        with open(perf_json) as f:
            metrics = json.load(f)

        # Validate monthly_returns field exists and has correct structure
        assert "monthly_returns" in metrics, "monthly_returns field missing from performance.json"
        monthly_returns = metrics["monthly_returns"]
        assert isinstance(monthly_returns, list), "monthly_returns should be a list"

        if monthly_returns:
            # Validate structure of first monthly period
            month = monthly_returns[0]
            assert "period" in month, "Monthly period missing 'period' field"
            assert "period_type" in month, "Monthly period missing 'period_type' field"
            assert "start_date" in month, "Monthly period missing 'start_date' field"
            assert "end_date" in month, "Monthly period missing 'end_date' field"
            assert "return_pct" in month, "Monthly period missing 'return_pct' field"
            assert "num_trades" in month, "Monthly period missing 'num_trades' field"
            assert "winning_trades" in month, "Monthly period missing 'winning_trades' field"
            assert "losing_trades" in month, "Monthly period missing 'losing_trades' field"

            # Validate period format (YYYY-MM)
            assert len(month["period"]) == 7, "Monthly period format should be YYYY-MM"
            assert month["period"][4] == "-", "Monthly period should contain hyphen"
            assert month["period_type"] == "monthly", "Period type should be 'monthly'"

            # Validate data types
            assert isinstance(month["return_pct"], str), "return_pct should be string (Decimal)"
            assert isinstance(month["num_trades"], int), "num_trades should be integer"

        # Validate quarterly_returns field exists and has correct structure
        assert "quarterly_returns" in metrics, "quarterly_returns field missing from performance.json"
        quarterly_returns = metrics["quarterly_returns"]
        assert isinstance(quarterly_returns, list), "quarterly_returns should be a list"

        if quarterly_returns:
            # Validate structure of first quarterly period
            quarter = quarterly_returns[0]
            assert "period" in quarter, "Quarterly period missing 'period' field"
            assert "period_type" in quarter, "Quarterly period missing 'period_type' field"

            # Validate period format (YYYY-Q#)
            assert "Q" in quarter["period"], "Quarterly period should contain 'Q'"
            assert quarter["period_type"] == "quarterly", "Period type should be 'quarterly'"

        # Validate annual_returns field exists and has correct structure
        assert "annual_returns" in metrics, "annual_returns field missing from performance.json"
        annual_returns = metrics["annual_returns"]
        assert isinstance(annual_returns, list), "annual_returns should be a list"

        if annual_returns:
            # Validate structure of first annual period
            year = annual_returns[0]
            assert "period" in year, "Annual period missing 'period' field"
            assert "period_type" in year, "Annual period missing 'period_type' field"

            # Validate period format (YYYY)
            assert len(year["period"]) == 4, "Annual period format should be YYYY"
            assert year["period_type"] == "annual", "Period type should be 'annual'"

        # Cleanup
        engine.shutdown()

    def test_performance_json_contains_strategy_performance(self, buy_hold_backtest_config, mock_system_config):
        """Test that performance.json includes per-strategy performance metrics.

        Validates that strategy_performance is populated with correct structure
        and contains metrics for each strategy in the backtest.

        Note: Currently strategy_performance may be empty because per-strategy
        equity tracking is not yet implemented. This test validates the structure
        exists and will validate content once equity tracking is added.
        """
        config = buy_hold_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            _ = engine.run()

        # Find performance.json
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)
        perf_json = latest_dir / "performance.json"

        with open(perf_json) as f:
            metrics = json.load(f)

        # Validate strategy_performance field exists
        assert "strategy_performance" in metrics, "strategy_performance field missing from performance.json"
        strategy_performance = metrics["strategy_performance"]
        assert isinstance(strategy_performance, list), "strategy_performance should be a list"

        # If strategies are tracked, validate structure
        if len(strategy_performance) > 0:
            # Validate structure of first strategy
            strategy = strategy_performance[0]
            assert "strategy_id" in strategy, "Strategy missing 'strategy_id' field"
            assert "equity_allocated" in strategy, "Strategy missing 'equity_allocated' field"
            assert "final_equity" in strategy, "Strategy missing 'final_equity' field"
            assert "return_pct" in strategy, "Strategy missing 'return_pct' field"
            assert "num_positions" in strategy, "Strategy missing 'num_positions' field"
            assert "total_trades" in strategy, "Strategy missing 'total_trades' field"
            assert "winning_trades" in strategy, "Strategy missing 'winning_trades' field"
            assert "losing_trades" in strategy, "Strategy missing 'losing_trades' field"
            assert "win_rate" in strategy, "Strategy missing 'win_rate' field"
            assert "avg_win_pct" in strategy, "Strategy missing 'avg_win_pct' field"
            assert "avg_loss_pct" in strategy, "Strategy missing 'avg_loss_pct' field"
            assert "profit_factor" in strategy, "Strategy missing 'profit_factor' field"
            assert "sharpe_ratio" in strategy, "Strategy missing 'sharpe_ratio' field"
            assert "max_drawdown_pct" in strategy, "Strategy missing 'max_drawdown_pct' field"

            # Validate data types
            assert isinstance(strategy["strategy_id"], str), "strategy_id should be string"
            assert isinstance(strategy["equity_allocated"], str), "equity_allocated should be string (Decimal)"
            assert isinstance(strategy["final_equity"], str), "final_equity should be string (Decimal)"
            assert isinstance(strategy["return_pct"], str), "return_pct should be string (Decimal)"
            assert isinstance(strategy["total_trades"], int), "total_trades should be integer"

            # Validate metrics are reasonable
            assert strategy["total_trades"] >= 0, "total_trades should be non-negative"
            assert strategy["total_trades"] == strategy["winning_trades"] + strategy["losing_trades"], (
                "total_trades should equal winning_trades + losing_trades"
            )

        # Cleanup
        engine.shutdown()

    def test_period_aggregations_calculate_correct_returns(self, buy_hold_backtest_config, mock_system_config):
        """Test that period aggregations calculate returns correctly.

        Validates that period returns are calculated accurately and trade
        statistics are correctly assigned to periods.
        """
        config = buy_hold_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            _ = engine.run()

        # Find performance.json
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)
        perf_json = latest_dir / "performance.json"

        with open(perf_json) as f:
            metrics = json.load(f)

        # Validate monthly returns
        monthly_returns = metrics["monthly_returns"]
        if monthly_returns:
            for period in monthly_returns:
                # Return percentage should be reasonable
                return_pct = float(period["return_pct"])
                assert -100 <= return_pct <= 1000, f"Monthly return {return_pct}% seems unreasonable"

                # Trade counts should be consistent
                num_trades = period["num_trades"]
                winning_trades = period["winning_trades"]
                losing_trades = period["losing_trades"]
                assert num_trades == winning_trades + losing_trades, f"Period {period['period']}: trades don't add up"

                # Dates should be valid ISO format
                start_date = period["start_date"]
                end_date = period["end_date"]
                assert len(start_date) == 10, "start_date should be YYYY-MM-DD"
                assert len(end_date) == 10, "end_date should be YYYY-MM-DD"
                assert start_date <= end_date, "start_date should be <= end_date"

        # Validate that periods are in chronological order
        if len(monthly_returns) > 1:
            for i in range(len(monthly_returns) - 1):
                current_period = monthly_returns[i]["period"]
                next_period = monthly_returns[i + 1]["period"]
                assert current_period < next_period, "Monthly periods should be in chronological order"

        # Cleanup
        engine.shutdown()

    def test_strategy_performance_metrics_are_reasonable(self, buy_hold_backtest_config, mock_system_config):
        """Test that strategy performance metrics have reasonable values.

        Validates that strategy metrics are within expected ranges and
        relationships between metrics are consistent.

        Note: Currently strategy_performance may be empty because per-strategy
        equity tracking is not yet implemented. This test will validate metrics
        once equity tracking is added.
        """
        config = buy_hold_backtest_config

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            engine = BacktestEngine.from_config(config)
            _ = engine.run()

        # Find performance.json
        output_base = Path(mock_system_config.output.experiments_root) / config.sanitized_backtest_id
        runs_dir = output_base / "runs"
        timestamped_dirs = list(runs_dir.iterdir())
        latest_dir = max(timestamped_dirs, key=lambda p: p.name)
        perf_json = latest_dir / "performance.json"

        with open(perf_json) as f:
            metrics = json.load(f)

        strategy_performance = metrics["strategy_performance"]

        # If strategies are tracked, validate metrics
        if len(strategy_performance) > 0:
            # Each strategy should have reasonable metrics
            for strategy in strategy_performance:
                # Equity allocated should be non-negative
                equity_allocated = float(strategy["equity_allocated"])
                assert equity_allocated >= 0, f"Strategy {strategy['strategy_id']} has negative allocation"

                # Final equity should be non-negative
                final_equity = float(strategy["final_equity"])
                assert final_equity >= 0, f"Strategy {strategy['strategy_id']} has negative final equity"

                # Win rate should be 0-100
                win_rate = float(strategy["win_rate"])
                assert 0 <= win_rate <= 100, f"Strategy {strategy['strategy_id']} has invalid win rate: {win_rate}"

                # Max drawdown should be non-negative
                max_dd = float(strategy["max_drawdown_pct"])
                assert max_dd >= 0, f"Strategy {strategy['strategy_id']} has negative drawdown: {max_dd}"

                # If there are trades, validate trade statistics
                if strategy["total_trades"] > 0:
                    # Win rate calculation should be consistent
                    expected_win_rate = (strategy["winning_trades"] / strategy["total_trades"]) * 100
                    assert abs(win_rate - expected_win_rate) < 0.01, (
                        f"Strategy {strategy['strategy_id']}: win rate mismatch"
                    )

        # Cleanup
        engine.shutdown()

    def test_reporting_teardown_skips_trader_audit_export_generation(
        self, tmp_path: Path, mock_system_config
    ) -> None:
        """Phase 4 removes Trader-side audit export generation during teardown."""
        event_store = _integration_audit_event_store()
        output_dir = tmp_path / "experiments" / "audit_exp" / "runs" / "run-001"
        output_dir.mkdir(parents=True, exist_ok=True)

        service = ReportingService(
            event_bus=EventBus(),
            config=ReportingConfig(
                write_parquet=False,
                write_json=False,
                write_html_report=False,
                write_csv_timeline=False,
                display_final_report=False,
            ),
            output_dir=output_dir,
            event_store=event_store,
        )

        service.setup(
            {
                "backtest_id": "audit_exp",
                "strategy_ids": ["sma_crossover"],
                "input_manifest": _integration_manifest(
                    database="audit_market",
                    bars_table="audit_bars_daily",
                ),
                "backtest_config": SimpleNamespace(
                    run_id="run-001",
                    job_group_id=None,
                    submission_source="integration_test",
                    split_pct=None,
                    split_role=None,
                ),
                "effective_execution_spec": {"risk_policy": {"name": "naive"}},
            }
        )
        service._start_datetime = datetime(2024, 1, 1, tzinfo=timezone.utc)
        service._initial_equity = Decimal("100000")
        service._equity_calc = MagicMock()
        service._equity_calc.latest_timestamp.return_value = datetime(2024, 1, 2, tzinfo=timezone.utc)
        service._equity_calc.latest_equity.return_value = Decimal("100500")
        service._equity_calc.get_curve.return_value = []
        service._returns_calc = MagicMock()
        service._returns_calc.returns = []
        service._drawdown_calc = MagicMock()
        service._drawdown_calc.max_drawdown_pct = Decimal("0")
        service._drawdown_calc.current_drawdown_pct = Decimal("0")
        service._drawdown_calc.drawdown_periods = []
        service._trade_stats_calc = MagicMock()
        service._trade_stats_calc.trades = []
        service._trade_stats_calc.total_trades = 0
        service._trade_stats_calc.winning_trades = 0
        service._trade_stats_calc.losing_trades = 0
        service._trade_stats_calc.max_consecutive_wins = 0
        service._trade_stats_calc.max_consecutive_losses = 0
        service._period_calc = MagicMock()
        service._period_calc.calculate_periods.return_value = []
        service._strategy_perf_calc = MagicMock()
        service._strategy_perf_calc.calculate_performance.return_value = []
        cast(Any, service)._last_portfolio_state = SimpleNamespace(
            snapshot_datetime="2024-01-02T00:00:00Z",
            total_commissions_paid=Decimal("1.25"),
            total_realized_pl=Decimal("34.00"),
            total_unrealized_pl=Decimal("0"),
        )

        mock_system_config.output.database = Mock(enabled=False, backend="postgres", postgres_url=None)
        mock_system_config.output.artifact_policy.mode = "filesystem"
        mock_system_config.output.experiments_root = str(tmp_path / "experiments")
        mock_system_config.config_root = tmp_path / "QS-Trader"
        mock_system_config.config_root.mkdir(parents=True, exist_ok=True)

        with patch("qs_trader.system.config.get_system_config", return_value=mock_system_config):
            service.teardown({})

        audit_zip = Path(mock_system_config.output.experiments_root) / "audit-exports" / "audit_exp" / "run-001.zip"
        assert not audit_zip.exists()


# ---------------------------------------------------------------------------
# Postgres-backed end-to-end observability persistence (v3)
# ---------------------------------------------------------------------------
#
# Exercises the always-on ``run_observability_bars`` persistence path. The
# fixture is opt-in: the test is skipped when the RESEARCH_POSTGRES_* env is
# not wired (local dev without stack, CI without DB), matching the pattern
# used by ``QS-Research/tests/test_pg_delete_restore.py``.


def _pg_observability_connection_url() -> str | None:
    import os
    import socket

    required = [
        "RESEARCH_POSTGRES_DB",
        "RESEARCH_POSTGRES_USER",
        "RESEARCH_POSTGRES_PASSWORD",
        "RESEARCH_POSTGRES_HOST",
    ]
    if any(not os.environ.get(name) for name in required):
        return None
    host = os.environ["RESEARCH_POSTGRES_HOST"]
    # Fall back to localhost when the configured host is only reachable from
    # inside the docker network (matches the QS-Research test pattern).
    if not Path("/.dockerenv").exists():
        try:
            socket.getaddrinfo(host, None)
        except socket.gaierror:
            if host == "postgres":
                host = "localhost"
            else:
                return None
    port = os.environ.get("RESEARCH_POSTGRES_PORT", "5432")
    db = os.environ["RESEARCH_POSTGRES_DB"]
    user = os.environ["RESEARCH_POSTGRES_USER"]
    password = os.environ["RESEARCH_POSTGRES_PASSWORD"]
    return (
        f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db}"
    )


def test_save_run_persists_bar_snapshots_and_observability_rows_and_cascades_on_resave() -> None:
    """Run persistence must populate both snapshot and observability ledgers.

    The test drives ``PostgreSQLWriter.save_run`` twice with different
    bar + indicator payloads for the same ``(experiment_id, run_id)`` and
    asserts:

    - the first save inserts one runtime bar snapshot row and one
      observability row per emitted bar;
    - ``_delete_existing_run`` cascades the prior rows on re-save so both
      ledgers reflect only the latest event stream;
    - the round-trip leaves the database in a clean state when the test
      cleans up after itself.
    """
    import pytest
    from sqlalchemy import create_engine, text

    connection_url = _pg_observability_connection_url()
    if connection_url is None:
        pytest.skip(
            "RESEARCH_POSTGRES_* env not configured; "
            "postgres observability integration test requires a live DB."
        )

    from qs_trader.libraries.performance.models import FullMetrics
    from qs_trader.services.reporting.postgres_writer import PostgreSQLWriter

    experiment_id = "observability_integration"
    run_id = "obs-int-001"
    strategy_id = "sma_crossover"
    symbol = "AAPL"

    metrics = FullMetrics(
        backtest_id=experiment_id,
        start_date="2024-01-01",
        end_date="2024-01-05",
        duration_days=5,
        initial_equity=Decimal("100000"),
        final_equity=Decimal("100100"),
        total_return_pct=Decimal("0.10"),
        cagr=Decimal("0.10"),
        best_day_return_pct=Decimal("0.05"),
        worst_day_return_pct=Decimal("-0.01"),
        volatility_annual_pct=Decimal("1.00"),
        max_drawdown_pct=Decimal("-0.10"),
        max_drawdown_duration_days=1,
        avg_drawdown_pct=Decimal("-0.05"),
        current_drawdown_pct=Decimal("0.00"),
        sharpe_ratio=Decimal("1.00"),
        sortino_ratio=Decimal("1.00"),
        calmar_ratio=Decimal("1.00"),
        risk_free_rate=Decimal("0.00"),
        total_trades=0,
        winning_trades=0,
        losing_trades=0,
        win_rate=Decimal("0"),
        profit_factor=Decimal("0"),
        avg_win=Decimal("0"),
        avg_loss=Decimal("0"),
        avg_win_pct=Decimal("0"),
        avg_loss_pct=Decimal("0"),
        largest_win=Decimal("0"),
        largest_loss=Decimal("0"),
        largest_win_pct=Decimal("0"),
        largest_loss_pct=Decimal("0"),
        expectancy=Decimal("0"),
        max_consecutive_wins=0,
        max_consecutive_losses=0,
        avg_trade_duration_days=Decimal("0"),
        total_commissions=Decimal("0"),
        commission_pct_of_pnl=Decimal("0"),
    )

    def _event_store_with_indicators(values: list[tuple[str, Decimal]]) -> InMemoryEventStore:
        store = InMemoryEventStore()
        for bar_timestamp, value in values:
            store.append(
                PriceBarEvent(
                    symbol=symbol,
                    timestamp=bar_timestamp,
                    interval="1d",
                    open=Decimal("100.00"),
                    high=Decimal("101.00"),
                    low=Decimal("99.50"),
                    close=Decimal("100.75"),
                    open_adj=Decimal("99.75"),
                    high_adj=Decimal("100.75"),
                    low_adj=Decimal("99.25"),
                    close_adj=Decimal("100.25"),
                    volume=int(value * Decimal("10")),
                    volume_raw=int(value * Decimal("12")),
                    volume_adj=int(value * Decimal("10")),
                    source="qs-datamaster-equity-1d",
                )
            )
            store.append(
                IndicatorEvent(
                    strategy_id=strategy_id,
                    symbol=symbol,
                    timestamp=bar_timestamp,
                    indicators={"sma_fast": value},
                )
            )
        return store

    writer = PostgreSQLWriter(connection_url)
    engine = create_engine(connection_url)

    try:
        writer.save_run(
            experiment_id=experiment_id,
            run_id=run_id,
            metrics=metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            event_store=_event_store_with_indicators(
                [
                    ("2024-01-02T00:00:00Z", Decimal("100.25")),
                    ("2024-01-03T00:00:00Z", Decimal("101.00")),
                ]
            ),
            artifact_mode="database_only",
        )

        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT strategy_id, symbol, bar_timestamp, indicators_json "
                    "FROM run_observability_bars "
                    "WHERE experiment_id = :experiment_id AND run_id = :run_id "
                    "ORDER BY bar_timestamp"
                ),
                {"experiment_id": experiment_id, "run_id": run_id},
            ).fetchall()
            snapshot_rows = conn.execute(
                text(
                    "SELECT symbol, bar_timestamp, volume_raw, volume_adj "
                    "FROM run_bar_snapshots "
                    "WHERE experiment_id = :experiment_id AND run_id = :run_id "
                    "ORDER BY bar_timestamp"
                ),
                {"experiment_id": experiment_id, "run_id": run_id},
            ).fetchall()

        assert len(rows) == 2, (
            f"Expected 2 observability rows after first save, got {len(rows)}"
        )
        assert len(snapshot_rows) == 2, (
            f"Expected 2 bar snapshot rows after first save, got {len(snapshot_rows)}"
        )
        assert all(row.strategy_id == strategy_id for row in rows)
        assert all(row.symbol == symbol for row in rows)
        indicators_first = rows[0].indicators_json
        assert indicators_first.get("sma_fast") == "100.25"
        assert snapshot_rows[0].volume_raw == 1203
        assert snapshot_rows[0].volume_adj == 1002

        writer.save_run(
            experiment_id=experiment_id,
            run_id=run_id,
            metrics=metrics,
            equity_curve=[],
            returns=[],
            trades=[],
            drawdowns=[],
            event_store=_event_store_with_indicators(
                [("2024-01-04T00:00:00Z", Decimal("102.50"))]
            ),
            artifact_mode="database_only",
        )

        with engine.connect() as conn:
            rows_after = conn.execute(
                text(
                    "SELECT bar_timestamp, indicators_json "
                    "FROM run_observability_bars "
                    "WHERE experiment_id = :experiment_id AND run_id = :run_id "
                    "ORDER BY bar_timestamp"
                ),
                {"experiment_id": experiment_id, "run_id": run_id},
            ).fetchall()
            snapshot_rows_after = conn.execute(
                text(
                    "SELECT bar_timestamp, volume_raw, volume_adj "
                    "FROM run_bar_snapshots "
                    "WHERE experiment_id = :experiment_id AND run_id = :run_id "
                    "ORDER BY bar_timestamp"
                ),
                {"experiment_id": experiment_id, "run_id": run_id},
            ).fetchall()

        assert len(rows_after) == 1, (
            "Re-save must cascade the prior observability rows before "
            f"inserting the new set, got {len(rows_after)}"
        )
        assert rows_after[0].indicators_json.get("sma_fast") == "102.50"
        assert len(snapshot_rows_after) == 1, (
            "Re-save must cascade the prior bar snapshot rows before "
            f"inserting the new set, got {len(snapshot_rows_after)}"
        )
        assert snapshot_rows_after[0].volume_raw == 1230
        assert snapshot_rows_after[0].volume_adj == 1025
    finally:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "DELETE FROM run_bar_snapshots "
                    "WHERE experiment_id = :experiment_id AND run_id = :run_id"
                ),
                {"experiment_id": experiment_id, "run_id": run_id},
            )
            conn.execute(
                text(
                    "DELETE FROM run_observability_bars "
                    "WHERE experiment_id = :experiment_id AND run_id = :run_id"
                ),
                {"experiment_id": experiment_id, "run_id": run_id},
            )
            conn.execute(
                text(
                    "DELETE FROM runs "
                    "WHERE experiment_id = :experiment_id AND run_id = :run_id"
                ),
                {"experiment_id": experiment_id, "run_id": run_id},
            )
        writer.close()
        engine.dispose()


