"""Unit tests for scan mode (Group 10).

Tests forward returns, MFE, MAE calculations, scan runner, and CLI command.
"""

import math
import tempfile
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from qs_trader.services.scan.calculations import (
    compute_scan_metrics,
    forward_return,
    mae,
    mfe,
)
from qs_trader.services.scan.runner import ScanRunner, ScanResult, ScanSummary


# ---------------------------------------------------------------------------
# Forward return tests
# ---------------------------------------------------------------------------


class TestForwardReturn:
    """Tests for log forward return calculation."""

    def test_basic_forward_return(self):
        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
        # log(105/100) = log(1.05) ≈ 0.04879
        result = forward_return(closes, 0, 5)
        assert result == pytest.approx(math.log(105.0 / 100.0), rel=1e-6)

    def test_zero_horizon(self):
        closes = [100.0, 101.0]
        result = forward_return(closes, 0, 0)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_insufficient_data(self):
        closes = [100.0, 101.0]
        result = forward_return(closes, 0, 10)
        assert math.isnan(result)

    def test_negative_price(self):
        closes = [-100.0, 101.0]
        result = forward_return(closes, 0, 1)
        assert math.isnan(result)

    def test_zero_price(self):
        closes = [0.0, 101.0]
        result = forward_return(closes, 0, 1)
        assert math.isnan(result)

    def test_index_out_of_bounds(self):
        closes = [100.0, 101.0]
        result = forward_return(closes, 5, 1)
        assert math.isnan(result)


# ---------------------------------------------------------------------------
# MFE tests
# ---------------------------------------------------------------------------


class TestMFE:
    """Tests for Max Favorable Excursion calculation."""

    def test_basic_mfe(self):
        highs = [100.0, 105.0, 103.0, 102.0]
        closes = [100.0, 101.0, 102.0, 103.0]
        # max(100, 105, 103) / 100 - 1 = 105/100 - 1 = 0.05
        result = mfe(highs, closes, 0, 3)
        assert result == pytest.approx(0.05, rel=1e-6)

    def test_mfe_single_bar(self):
        highs = [100.0, 105.0]
        closes = [100.0, 101.0]
        result = mfe(highs, closes, 0, 1)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_mfe_insufficient_data(self):
        highs = [100.0]
        closes = [100.0]
        # Index beyond array length
        result = mfe(highs, closes, 5, 10)
        assert math.isnan(result)

    def test_mfe_zero_close(self):
        highs = [100.0]
        closes = [0.0]
        result = mfe(highs, closes, 0, 1)
        assert math.isnan(result)


# ---------------------------------------------------------------------------
# MAE tests
# ---------------------------------------------------------------------------


class TestMAE:
    """Tests for Max Adverse Excursion calculation."""

    def test_basic_mae(self):
        lows = [100.0, 95.0, 97.0, 98.0]
        closes = [100.0, 101.0, 102.0, 103.0]
        # min(100, 95, 97) / 100 - 1 = 95/100 - 1 = -0.05
        result = mae(lows, closes, 0, 3)
        assert result == pytest.approx(-0.05, rel=1e-6)

    def test_mae_single_bar(self):
        lows = [100.0, 95.0]
        closes = [100.0, 101.0]
        result = mae(lows, closes, 0, 1)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_mae_insufficient_data(self):
        lows = [100.0]
        closes = [100.0]
        # Index beyond array length
        result = mae(lows, closes, 5, 10)
        assert math.isnan(result)

    def test_mae_zero_close(self):
        lows = [100.0]
        closes = [0.0]
        result = mae(lows, closes, 0, 1)
        assert math.isnan(result)


# ---------------------------------------------------------------------------
# compute_scan_metrics tests
# ---------------------------------------------------------------------------


class TestComputeScanMetrics:
    """Tests for combined scan metrics computation."""

    def test_all_metrics(self):
        closes = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
        highs = [100.0, 105.0, 103.0, 102.0, 104.0, 106.0]
        lows = [100.0, 95.0, 97.0, 98.0, 99.0, 100.0]

        metrics = compute_scan_metrics(closes, highs, lows, 0, horizons=[5])

        assert "forward_return_5d" in metrics
        assert "mfe_5d" in metrics
        assert "mae_5d" in metrics
        assert not math.isnan(metrics["forward_return_5d"])

    def test_default_horizons(self):
        closes = [100.0] * 30
        highs = [100.0] * 30
        lows = [100.0] * 30

        metrics = compute_scan_metrics(closes, highs, lows, 0)

        # Default horizons: 5, 10, 20
        assert "forward_return_5d" in metrics
        assert "forward_return_10d" in metrics
        assert "forward_return_20d" in metrics
        assert "mfe_20d" in metrics
        assert "mae_20d" in metrics

    def test_insufficient_data_returns_nan(self):
        closes = [100.0]
        highs = [100.0]
        lows = [100.0]

        metrics = compute_scan_metrics(closes, highs, lows, 0, horizons=[5])

        # Forward return is NaN (no future data)
        assert math.isnan(metrics["forward_return_5d"])
        # MFE/MAE with single-element window return 0.0 (window has 1 element)
        assert metrics["mfe_5d"] == pytest.approx(0.0, abs=1e-10)
        assert metrics["mae_5d"] == pytest.approx(0.0, abs=1e-10)


# ---------------------------------------------------------------------------
# ScanRunner tests
# ---------------------------------------------------------------------------


class TestScanRunner:
    """Tests for ScanRunner execution."""

    def _make_resolver(self):
        """Create a mock instrument resolver."""
        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self, secid, display_symbol, ticker_at_date):
                self.secid = secid
                self.display_symbol = display_symbol
                self.ticker_at_date = ticker_at_date

        resolver.resolve_batch.return_value = {
            "AAPL": ResolvedInstrument(12345, "AAPL", "AAPL"),
            "MSFT": ResolvedInstrument(67890, "MSFT", "MSFT"),
        }
        return resolver

    def _make_data_loader(self):
        """Create a data loader with synthetic data."""
        def loader(identifier):
            return {
                "closes": [100.0, 101.0, 102.0, 103.0, 104.0, 105.0],
                "highs": [100.0, 105.0, 103.0, 102.0, 104.0, 106.0],
                "lows": [100.0, 95.0, 97.0, 98.0, 99.0, 100.0],
                "dates": [
                    date(2024, 1, 15),
                    date(2024, 1, 16),
                    date(2024, 1, 17),
                    date(2024, 1, 18),
                    date(2024, 1, 19),
                    date(2024, 1, 22),
                ],
            }
        return loader

    def _make_candidate_rule(self):
        """Create a simple candidate rule."""
        def rule(secid, date_str, features):
            return "candidate", "default", 0.5, {}, features or {}
        return rule

    def test_run_basic(self):
        resolver = self._make_resolver()
        data_loader = self._make_data_loader()
        candidate_rule = self._make_candidate_rule()

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=data_loader,
            candidate_rule=candidate_rule,
            strategy_id="test_strategy",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            results, summary = runner.run(
                tickers=["AAPL", "MSFT"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            assert summary.total_instruments == 2
            assert summary.instruments_processed == 2
            assert summary.instruments_failed == 0
            assert summary.total_rows == 12  # 2 instruments * 6 bars

            # Verify CSV was written
            csv_path = Path(tmpdir) / "candidate_scan_results.csv"
            assert csv_path.exists()

    def test_run_no_resolver(self):
        """Test that RuntimeError is raised when resolver is missing."""
        data_loader = self._make_data_loader()
        candidate_rule = self._make_candidate_rule()

        runner = ScanRunner(
            instrument_resolver=None,
            data_loader=data_loader,
            candidate_rule=candidate_rule,
            strategy_id="test_strategy",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            with pytest.raises(RuntimeError, match="InstrumentResolver is required"):
                runner.run(
                    tickers=["AAPL"],
                    date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                    output_dir=Path(tmpdir),
                )

    def test_run_with_failure(self):
        """Test error handling for individual instrument failures."""
        def failing_loader(identifier):
            if identifier == 67890:
                raise RuntimeError("Data unavailable")
            return {
                "closes": [100.0, 101.0],
                "highs": [100.0, 101.0],
                "lows": [100.0, 101.0],
                "dates": [date(2024, 1, 15), date(2024, 1, 16)],
            }

        resolver = self._make_resolver()
        candidate_rule = self._make_candidate_rule()

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=failing_loader,
            candidate_rule=candidate_rule,
            strategy_id="test_strategy",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            results, summary = runner.run(
                tickers=["AAPL", "MSFT"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            assert summary.instruments_processed == 1
            assert summary.instruments_failed == 1
            assert len(summary.failures) == 1
            assert "MSFT" in summary.failures[0]

    def test_persist_results(self):
        """Test CSV persistence of scan results."""
        results = [
            ScanResult(
                date="2024-01-15",
                secid=12345,
                display_symbol="AAPL",
                ticker_at_date="AAPL",
                runtime_symbol="AAPL",
                strategy_id="test",
                candidate_status="candidate",
                reason_code="default",
                score=0.5,
                forward_return_5d=0.01,
                mfe_20d=0.05,
                mae_20d=-0.03,
            )
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = ScanRunner._persist_results(results, Path(tmpdir) / "test.csv")
            assert path is not None
            assert path.exists()

            content = path.read_text()
            assert "secid" in content
            assert "12345" in content
            assert "0.01" in content

    def test_run_passes_features_from_data_loader(self):
        """Test that feature columns from the data loader are extracted per-bar
        and passed to the candidate rule, then persisted in features_json."""
        captured_features: list[dict] = []

        def feature_loader(identifier):
            return {
                "closes": [100.0, 101.0, 102.0],
                "highs": [100.0, 101.0, 102.0],
                "lows": [100.0, 101.0, 102.0],
                "dates": [
                    date(2024, 1, 15),
                    date(2024, 1, 16),
                    date(2024, 1, 17),
                ],
                "momentum": [0.1, 0.2, 0.3],
                "volatility": [0.05, 0.06, 0.07],
            }

        def rule_with_capture(secid, date_str, features):
            captured_features.append(dict(features))
            return "candidate", "default", 0.5, {}, features

        # Resolver with only one instrument
        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self, secid, display_symbol, ticker_at_date):
                self.secid = secid
                self.display_symbol = display_symbol
                self.ticker_at_date = ticker_at_date

        resolver.resolve_batch.return_value = {
            "AAPL": ResolvedInstrument(12345, "AAPL", "AAPL"),
        }

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=feature_loader,
            candidate_rule=rule_with_capture,
            strategy_id="test_strategy",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            results, summary = runner.run(
                tickers=["AAPL"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            assert summary.instruments_processed == 1
            assert summary.total_rows == 3

            # Verify features were captured per-bar
            assert len(captured_features) == 3
            assert captured_features[0] == {"momentum": 0.1, "volatility": 0.05}
            assert captured_features[1] == {"momentum": 0.2, "volatility": 0.06}
            assert captured_features[2] == {"momentum": 0.3, "volatility": 0.07}

            # Verify features_json is populated in CSV
            csv_path = Path(tmpdir) / "candidate_scan_results.csv"
            content = csv_path.read_text()
            assert "momentum" in content
            assert "volatility" in content


# ---------------------------------------------------------------------------
# ScanSummary tests
# ---------------------------------------------------------------------------


class TestScanSummary:
    """Tests for ScanSummary dataclass."""

    def test_default_values(self):
        summary = ScanSummary()
        assert summary.total_instruments == 0
        assert summary.instruments_processed == 0
        assert summary.instruments_failed == 0
        assert summary.total_rows == 0
        assert summary.failures == []

    def test_custom_values(self):
        summary = ScanSummary(
            total_instruments=10,
            instruments_processed=8,
            instruments_failed=2,
            total_rows=100,
            failures=["AAPL: error", "MSFT: error"],
        )
        assert summary.total_instruments == 10
        assert len(summary.failures) == 2
