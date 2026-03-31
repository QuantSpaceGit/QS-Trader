"""
Integration test for Phase 3: BacktestEngine + StrategyService.

Tests the complete data -> strategy -> signals pipeline with real historical data.
Uses fixtures instead of external config files.
Writes to temporary directories, not user-facing output folders.
"""

from datetime import datetime
from decimal import Decimal
from unittest.mock import patch

from qs_trader.engine.engine import BacktestEngine
from qs_trader.events.events import SignalEvent


class TestStrategyIntegration:
    """Test end-to-end strategy execution in backtest."""

    def test_buy_and_hold_strategy_emits_signal(self, buy_hold_backtest_config, mock_system_config):
        """
        Test that buy_and_hold strategy discovers, loads, and emits signal.

        This tests:
        1. Strategy auto-discovery from tests/fixtures/strategies
        2. Strategy instantiation from fixture config
        3. DataService streaming bars to StrategyService
        4. Strategy receiving bars and emitting signals
        5. Signal publishing to EventBus
        6. Metrics tracking

        Uses temporary output directory.
        """
        # Use fixture config
        config = buy_hold_backtest_config

        # Shorten date range for faster test
        config.start_date = datetime(2020, 8, 1)
        config.end_date = datetime(2020, 8, 15)

        # Verify config has strategies
        assert len(config.strategies) > 0, "No strategies configured"
        assert config.strategies[0].strategy_id == "buy_and_hold"

        # Collect signals
        signals_collected = []

        def collect_signals(event):
            if isinstance(event, SignalEvent):
                signals_collected.append(event)

        # Run backtest with mocked system config
        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            with BacktestEngine.from_config(config) as engine:
                # Subscribe to signal events
                engine._event_bus.subscribe("signal", collect_signals)

                # Run backtest
                result = engine.run()

                # Verify bars processed
                assert result.bars_processed > 0, "No bars processed"

                # Verify strategy metrics
                if engine._strategy_service:
                    metrics = engine._strategy_service.get_metrics()
                    assert "buy_and_hold" in metrics, "Strategy not in metrics"

                    strategy_metrics = metrics["buy_and_hold"]
                    assert strategy_metrics["bars_processed"] > 0, "Strategy processed no bars"
                    assert strategy_metrics["signals_emitted"] == 1, "Expected exactly 1 signal"
                    assert strategy_metrics["errors"] == 0, "Strategy had errors"

                # Verify signal was collected
                assert len(signals_collected) == 1, "Expected exactly 1 signal event"

                signal = signals_collected[0]
                assert signal.symbol == "AAPL"
                assert signal.intention == "OPEN_LONG"
                assert isinstance(signal.price, Decimal)
                assert signal.price > 0
                assert signal.confidence == Decimal("1.0")
                assert "Buy and hold" in signal.reason

    def test_strategy_universe_filtering(self, buy_hold_backtest_config, mock_system_config):
        """
        Test that strategies only receive bars for symbols in their universe.

        Uses fixture config with single symbol universe.
        Writes to temporary output directory.
        """
        # Use fixture config
        config = buy_hold_backtest_config

        # Shorten date range for faster test
        config.start_date = datetime(2020, 8, 1)
        config.end_date = datetime(2020, 8, 15)

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            with BacktestEngine.from_config(config) as engine:
                result = engine.run()

                # Verify strategy only processed AAPL bars
                if engine._strategy_service:
                    metrics = engine._strategy_service.get_metrics()
                    strategy_metrics = metrics["buy_and_hold"]

                    # The strategy universe is ['AAPL']
                    # So bars_processed should equal total bars for AAPL
                    assert strategy_metrics["bars_processed"] == result.bars_processed

    def test_strategy_lifecycle_methods_called(self, buy_hold_backtest_config, mock_system_config):
        """
        Test that strategy setup and teardown are called.

        Uses fixture config to verify lifecycle methods.
        Writes to temporary output directory.
        """
        config = buy_hold_backtest_config

        # Shorten date range for faster test
        config.start_date = datetime(2020, 8, 1)
        config.end_date = datetime(2020, 8, 15)

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            with BacktestEngine.from_config(config) as engine:
                # Check strategy service exists
                assert engine._strategy_service is not None

                # Run backtest (this calls setup and teardown)
                result = engine.run()

                # If we got here without exceptions, setup and teardown succeeded
                assert result.bars_processed > 0

    def test_backtest_performance(self, buy_hold_backtest_config, mock_system_config):
        """
        Test that backtest completes in reasonable time.

        Smoke test using fixture config with short date range.
        Writes to temporary output directory.
        """
        config = buy_hold_backtest_config

        # Short date range for smoke test
        config.start_date = datetime(2020, 8, 1)
        config.end_date = datetime(2020, 8, 15)

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_system_config):
            with BacktestEngine.from_config(config) as engine:
                result = engine.run()

                # Verify backtest completed and processed bars
                assert result.bars_processed >= 10, "Should process at least 10 bars"
                assert result.duration.total_seconds() < 5.0, "Should complete within 5 seconds"
