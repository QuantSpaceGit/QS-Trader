"""Unit tests for scan mode (Group 10).

Tests forward returns, MFE, MAE calculations, scan runner, context models,
decision types, tuple adapter, parameter hashing, feature validation,
price basis resolution, and CLI command.
"""

import json
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
from qs_trader.services.scan.models import (
    DEFAULT_PRICE_BASIS,
    ScanDecision,
    ScanRuleContext,
    VALID_STATUSES,
    canonicalize_parameters,
    decision_from_tuple,
    hash_parameters,
    resolve_price_basis,
    validate_feature_column,
    validate_feature_columns,
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
# ScanRuleContext tests (Section 1)
# ---------------------------------------------------------------------------


class TestScanRuleContext:
    """Tests for ScanRuleContext dataclass."""

    def _make_context(self, **overrides):
        """Create a minimal ScanRuleContext with defaults."""
        defaults = {
            "secid": 12345,
            "display_symbol": "AAPL",
            "ticker_at_date": "AAPL",
            "identity_source": "ticker",
            "runtime_symbol": "AAPL",
            "date": "2024-01-15",
            "bar_index": 0,
            "dates": [date(2024, 1, 15), date(2024, 1, 16), date(2024, 1, 17)],
            "open": [100.0, 101.0, 102.0],
            "high": [105.0, 106.0, 107.0],
            "low": [99.0, 100.0, 101.0],
            "close": [102.0, 103.0, 104.0],
            "volume": [1000.0, 1100.0, 1200.0],
        }
        defaults.update(overrides)
        return ScanRuleContext(**defaults)

    def test_minimal_construction(self):
        """Task 1.10: Construct with minimum required fields."""
        ctx = self._make_context()
        assert ctx.secid == 12345
        assert ctx.display_symbol == "AAPL"
        assert ctx.bar_index == 0

    def test_bar_index_access(self):
        """Task 1.11: bar_index can read matching entry from dates and close."""
        ctx = self._make_context(bar_index=1)
        assert ctx.dates[ctx.bar_index] == date(2024, 1, 16)
        assert ctx.close[ctx.bar_index] == 103.0

    def test_rolling_access(self):
        """Task 4.11: Rule can compute rolling value using prior bars."""
        ctx = self._make_context(bar_index=2)
        # 3-bar rolling average of close
        window = ctx.close[ctx.bar_index - 2: ctx.bar_index + 1]
        avg = sum(window) / len(window)
        assert avg == pytest.approx((102.0 + 103.0 + 104.0) / 3.0)

    def test_identity_fields(self):
        """Task 4.5: Context identity fields populated from resolved instrument."""
        ctx = self._make_context(
            secid=99999,
            display_symbol="BRK.B",
            ticker_at_date="BRK.B",
            identity_source="secid",
            runtime_symbol="BRK-B",
        )
        assert ctx.secid == 99999
        assert ctx.display_symbol == "BRK.B"
        assert ctx.identity_source == "secid"

    def test_metadata_fields(self):
        """Task 4.9: Context metadata fields from runner/CLI config."""
        ctx = self._make_context(
            data_source="qs-datamaster",
            price_basis="adjusted_ohlc_adj_columns",
            parameters={"lookback": 20},
            parameter_hash="abc123",
        )
        assert ctx.data_source == "qs-datamaster"
        assert ctx.price_basis == "adjusted_ohlc_adj_columns"
        assert ctx.parameters == {"lookback": 20}
        assert ctx.parameter_hash == "abc123"

    def test_features_and_feature_columns(self):
        """Task 4.8: Context features populated with current bar values."""
        ctx = self._make_context(
            features={"momentum": 0.5, "volatility": 0.02},
            feature_columns=["momentum", "volatility"],
        )
        assert ctx.features["momentum"] == 0.5
        assert "volatility" in ctx.feature_columns

    def test_context_is_frozen(self):
        """Context should be immutable (frozen dataclass)."""
        ctx = self._make_context()
        with pytest.raises(Exception):  # FrozenInstanceError
            ctx.secid = 99999


# ---------------------------------------------------------------------------
# ScanDecision tests (Section 2)
# ---------------------------------------------------------------------------


class TestScanDecision:
    """Tests for ScanDecision dataclass."""

    def test_accepted_decision(self):
        """Task 2.7: Accepted decision."""
        decision = ScanDecision(
            candidate_status="accepted",
            reason_code="breakout_detected",
            score=0.85,
            gates={"volume_gate": True, "trend_gate": True},
            diagnostics={"avg_volume": 1500000},
        )
        assert decision.candidate_status == "accepted"
        assert decision.score == 0.85
        assert decision.gates["volume_gate"] is True

    def test_rejected_decision_with_failed_gate(self):
        """Task 2.8: Rejected decision with one failed gate."""
        decision = ScanDecision(
            candidate_status="rejected",
            reason_code="volume_too_low",
            score=0.2,
            gates={"volume_gate": False},
            diagnostics={"avg_volume": 50000, "min_volume": 100000},
        )
        assert decision.candidate_status == "rejected"
        assert decision.gates["volume_gate"] is False
        assert decision.diagnostics["min_volume"] == 100000

    def test_invalid_status_raises(self):
        """Task 2.9: Invalid status values fail clearly."""
        with pytest.raises(ValueError, match="Unsupported candidate_status"):
            ScanDecision(candidate_status="invalid_status", reason_code="test")

    def test_safe_defaults(self):
        """Task 2.5: Safe defaults for optional maps."""
        decision = ScanDecision(candidate_status="ignored", reason_code="no_data")
        assert decision.gates == {}
        assert decision.diagnostics == {}
        assert decision.features == {}
        assert decision.score is None

    def test_all_valid_statuses(self):
        """Task 2.3: All allowed status values work."""
        for status in VALID_STATUSES:
            d = ScanDecision(candidate_status=status, reason_code="test")
            assert d.candidate_status == status

    def test_not_ready_decision(self):
        """Task: not_ready decision for insufficient warmup."""
        decision = ScanDecision(
            candidate_status="not_ready",
            reason_code="insufficient_history",
        )
        assert decision.candidate_status == "not_ready"


# ---------------------------------------------------------------------------
# Tuple compatibility adapter tests (Section 3)
# ---------------------------------------------------------------------------


class TestTupleAdapter:
    """Tests for decision_from_tuple adapter."""

    def test_tuple_to_decision(self):
        """Task 3.6: Tuple-to-decision conversion."""
        result = ("candidate", "default", 0.5, {"gate": True}, {"feat": 1.0})
        decision = decision_from_tuple(result)
        assert decision.candidate_status == "accepted"  # "candidate" mapped
        assert decision.reason_code == "default"
        assert decision.score == 0.5
        assert decision.gates == {"gate": True}
        assert decision.features == {"feat": 1.0}
        assert decision.diagnostics == {}

    def test_malformed_tuple_raises(self):
        """Task 3.7: Malformed tuple results fail with clear error."""
        with pytest.raises(ValueError, match="Expected a 5-tuple"):
            decision_from_tuple(("accepted", "reason"))  # too short

        with pytest.raises(ValueError, match="Expected a 5-tuple"):
            decision_from_tuple([1, 2, 3, 4, 5])  # list, not tuple

    def test_legacy_candidate_mapped_to_accepted(self):
        """Task 3.3: Legacy 'candidate' status maps to 'accepted'."""
        result = ("candidate", "test", None, {}, {})
        decision = decision_from_tuple(result)
        assert decision.candidate_status == "accepted"

    def test_direct_accepted_status_preserved(self):
        """Direct 'accepted' status is preserved (not double-mapped)."""
        result = ("accepted", "test", None, {}, {})
        decision = decision_from_tuple(result)
        assert decision.candidate_status == "accepted"

    def test_unsupported_status_raises(self):
        """Unsupported status from tuple raises ValueError."""
        result = ("bogus", "test", None, {}, {})
        with pytest.raises(ValueError, match="Unsupported status"):
            decision_from_tuple(result)

    def test_none_fields_get_safe_defaults(self):
        """Task 3.4-3.5: None fields get safe defaults."""
        result = ("accepted", None, None, None, None)
        decision = decision_from_tuple(result)
        assert decision.reason_code == ""
        assert decision.gates == {}
        assert decision.features == {}


# ---------------------------------------------------------------------------
# ScanRunner tests (Sections 4, 5, 6, 11, 12)
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
                self.identity_source = "ticker"

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
                "opens": [99.0, 100.0, 101.0, 102.0, 103.0, 104.0],
                "volumes": [1000.0, 1100.0, 1200.0, 1300.0, 1400.0, 1500.0],
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
        """Create a simple candidate rule (legacy tuple-return)."""
        def rule(context):
            return "candidate", "default", 0.5, {}, context.features or {}
        return rule

    def _make_context_rule(self):
        """Create a new-style context rule."""
        def rule(context):
            close = context.close[context.bar_index]
            if context.bar_index < 2:
                return ScanDecision(
                    candidate_status="not_ready",
                    reason_code="insufficient_history",
                )
            if close > 102.0:
                return ScanDecision(
                    candidate_status="accepted",
                    reason_code="price_above_threshold",
                    score=close,
                    gates={"price_gate": True},
                    diagnostics={"close": close},
                )
            return ScanDecision(
                candidate_status="rejected",
                reason_code="price_below_threshold",
                score=close,
                gates={"price_gate": False},
                diagnostics={"close": close},
            )
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
                "opens": [99.0, 100.0],
                "volumes": [1000.0, 1100.0],
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
        captured_contexts: list = []

        def feature_loader(identifier):
            return {
                "closes": [100.0, 101.0, 102.0],
                "highs": [100.0, 101.0, 102.0],
                "lows": [100.0, 101.0, 102.0],
                "opens": [99.0, 100.0, 101.0],
                "volumes": [1000.0, 1100.0, 1200.0],
                "dates": [
                    date(2024, 1, 15),
                    date(2024, 1, 16),
                    date(2024, 1, 17),
                ],
                "momentum": [0.1, 0.2, 0.3],
                "volatility": [0.05, 0.06, 0.07],
            }

        def rule_with_capture(context):
            captured_contexts.append(context)
            return "candidate", "default", 0.5, {}, context.features

        # Resolver with only one instrument
        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self, secid, display_symbol, ticker_at_date):
                self.secid = secid
                self.display_symbol = display_symbol
                self.ticker_at_date = ticker_at_date
                self.identity_source = "ticker"

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
            assert len(captured_contexts) == 3
            assert captured_contexts[0].features == {"momentum": 0.1, "volatility": 0.05}
            assert captured_contexts[1].features == {"momentum": 0.2, "volatility": 0.06}
            assert captured_contexts[2].features == {"momentum": 0.3, "volatility": 0.07}

            # Verify features_json is populated in CSV
            csv_path = Path(tmpdir) / "candidate_scan_results.csv"
            content = csv_path.read_text()
            assert "momentum" in content
            assert "volatility" in content

    def test_context_rule_close_and_bar_index(self):
        """Task 4.10: Rule can read context.close[context.bar_index]."""
        captured_closes = []

        def rule(context):
            captured_closes.append(context.close[context.bar_index])
            return ScanDecision(candidate_status="accepted", reason_code="test")

        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        def loader(_):
            return {
                "closes": [10.0, 20.0, 30.0],
                "highs": [10.0, 20.0, 30.0],
                "lows": [10.0, 20.0, 30.0],
                "opens": [10.0, 20.0, 30.0],
                "volumes": [100.0, 200.0, 300.0],
                "dates": [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            }

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=loader,
            candidate_rule=rule,
            strategy_id="test",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

        assert captured_closes == [10.0, 20.0, 30.0]

    def test_new_style_context_rule(self):
        """Task 5.6: New-style context rule works."""
        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=self._make_data_loader(),
            candidate_rule=self._make_context_rule(),
            strategy_id="test",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            results, summary = runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            assert summary.total_rows == 6
            # First 2 bars should be not_ready
            assert results[0].candidate_status == "not_ready"
            assert results[1].candidate_status == "not_ready"
            # Bar 2: close=102.0, not > 102.0 → rejected
            assert results[2].candidate_status == "rejected"
            # Bars 3-5: close > 102 → accepted
            assert results[3].candidate_status == "accepted"
            assert results[4].candidate_status == "accepted"
            assert results[5].candidate_status == "accepted"

    def test_old_tuple_rule_still_works(self):
        """Task 5.7: Old tuple-return rule still works through adapter."""
        def old_rule(context):
            return ("candidate", "legacy", 0.5, {}, context.features)

        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=self._make_data_loader(),
            candidate_rule=old_rule,
            strategy_id="test",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            results, summary = runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            assert summary.total_rows == 6
            # Legacy "candidate" should be adapted to "accepted"
            assert results[0].candidate_status == "accepted"
            assert results[0].reason_code == "legacy"

    def test_diagnostics_written_to_csv(self):
        """Task 6.5: Diagnostics are written to CSV."""
        def rule_with_diagnostics(context):
            return ScanDecision(
                candidate_status="accepted",
                reason_code="test",
                diagnostics={"close": context.close[context.bar_index]},
            )

        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=self._make_data_loader(),
            candidate_rule=rule_with_diagnostics,
            strategy_id="test",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            csv_path = Path(tmpdir) / "candidate_scan_results.csv"
            content = csv_path.read_text()
            assert "diagnostics_json" in content
            assert "close" in content

    def test_empty_diagnostics_produce_empty_value(self):
        """Task 6.6: Empty diagnostics produce empty or {} value."""
        def rule_empty_diag(context):
            return ScanDecision(
                candidate_status="accepted",
                reason_code="test",
            )

        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=self._make_data_loader(),
            candidate_rule=rule_empty_diag,
            strategy_id="test",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            csv_path = Path(tmpdir) / "candidate_scan_results.csv"
            content = csv_path.read_text()
            lines = content.strip().split("\n")
            header = lines[0]
            assert "diagnostics_json" in header
            # Check that diagnostics column exists and is empty for rows
            first_data = lines[1]
            # The diagnostics_json field should be empty string
            fields = first_data.split(",")
            diag_idx = header.split(",").index("diagnostics_json")
            assert fields[diag_idx] == ""

    def test_manifest_file_created(self):
        """Task 11.11: Manifest file is created."""
        resolver = self._make_resolver()
        data_loader = self._make_data_loader()
        candidate_rule = self._make_candidate_rule()

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=data_loader,
            candidate_rule=candidate_rule,
            strategy_id="test_strategy",
            data_source="qs-datamaster",
            price_basis="adjusted_ohlc_adj_columns",
            parameters={"lookback": 20},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            runner.run(
                tickers=["AAPL"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            manifest_path = Path(tmpdir) / "scan_manifest.json"
            assert manifest_path.exists()

    def test_manifest_required_keys(self):
        """Task 11.12: Required manifest keys are present."""
        resolver = self._make_resolver()
        data_loader = self._make_data_loader()
        candidate_rule = self._make_candidate_rule()

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=data_loader,
            candidate_rule=candidate_rule,
            strategy_id="test_strategy",
            data_source="qs-datamaster",
            price_basis="adjusted_ohlc_adj_columns",
            parameters={"lookback": 20},
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            runner.run(
                tickers=["AAPL"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            manifest_path = Path(tmpdir) / "scan_manifest.json"
            manifest = json.loads(manifest_path.read_text())

            assert "schema_version" in manifest
            assert "generated_at" in manifest
            assert "rule" in manifest
            assert "data" in manifest
            assert "date_range" in manifest
            assert "universe" in manifest
            assert "resolved_instruments" in manifest
            assert "output_files" in manifest
            assert "summary" in manifest

            # Rule metadata
            assert "strategy_id" in manifest["rule"]
            assert "parameter_snapshot" in manifest["rule"]
            assert "parameter_hash" in manifest["rule"]

            # Data metadata
            assert "data_source" in manifest["data"]
            assert "price_basis" in manifest["data"]

            # Date range
            assert "start_date" in manifest["date_range"]
            assert "end_date" in manifest["date_range"]

            # Universe
            assert "requested_tickers" in manifest["universe"]
            assert "requested_secids" in manifest["universe"]
            assert "ticker_policy" in manifest["universe"]

            # Summary
            assert "total_instruments" in manifest["summary"]
            assert "instruments_processed" in manifest["summary"]
            assert "instruments_failed" in manifest["summary"]
            assert "total_rows" in manifest["summary"]

    def test_existing_columns_preserved(self):
        """Task 12.6: Regression test — existing required columns still exist."""
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
            runner.run(
                tickers=["AAPL"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            csv_path = Path(tmpdir) / "candidate_scan_results.csv"
            content = csv_path.read_text()
            header = content.split("\n")[0]

            # Existing columns must still be present
            required_columns = [
                "date",
                "secid",
                "display_symbol",
                "ticker_at_date",
                "runtime_symbol",
                "strategy_id",
                "candidate_status",
                "reason_code",
                "score",
                "gates_json",
                "features_json",
                "forward_return_5d",
                "forward_return_10d",
                "forward_return_20d",
                "mfe_20d",
                "mae_20d",
            ]
            for col in required_columns:
                assert col in header, f"Missing required column: {col}"

            # New additive column should also be present
            assert "diagnostics_json" in header


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


# ---------------------------------------------------------------------------
# Parameter hashing tests (Section 10)
# ---------------------------------------------------------------------------


class TestParameterHashing:
    """Tests for parameter canonicalization and hashing."""

    def test_canonicalize_empty(self):
        assert canonicalize_parameters(None) == "{}"
        assert canonicalize_parameters({}) == "{}"

    def test_canonicalize_sorted_keys(self):
        result = canonicalize_parameters({"b": 2, "a": 1})
        assert result == '{"a":1,"b":2}'

    def test_stable_hash_across_key_order(self):
        """Task 10.6: Same content, different key order → same hash."""
        h1 = hash_parameters({"b": 2, "a": 1})
        h2 = hash_parameters({"a": 1, "b": 2})
        assert h1 == h2

    def test_different_values_different_hashes(self):
        """Task 10.7: Different parameter values → different hashes."""
        h1 = hash_parameters({"lookback": 20})
        h2 = hash_parameters({"lookback": 30})
        assert h1 != h2

    def test_hash_is_sha256_hex(self):
        h = hash_parameters({"key": "value"})
        assert len(h) == 64  # SHA-256 hex length
        assert all(c in "0123456789abcdef" for c in h)


# ---------------------------------------------------------------------------
# Feature column validation tests (Section 8)
# ---------------------------------------------------------------------------


class TestFeatureColumnValidation:
    """Tests for feature column name validation."""

    def test_valid_names(self):
        """Task 8.6: Valid feature column names pass."""
        assert validate_feature_column("momentum") == "momentum"
        assert validate_feature_column("volatility_20d") == "volatility_20d"
        assert validate_feature_column("RSI14") == "RSI14"
        assert validate_feature_column("a1_b2_c3") == "a1_b2_c3"

    def test_empty_name_rejected(self):
        """Task 8.3: Empty feature column names rejected."""
        with pytest.raises(ValueError, match="must not be empty"):
            validate_feature_column("")

    def test_spaces_rejected(self):
        """Task 8.4: Names with spaces rejected."""
        with pytest.raises(ValueError, match="Invalid feature column name"):
            validate_feature_column("my column")

    def test_sql_operators_rejected(self):
        """Task 8.4: SQL operators rejected."""
        with pytest.raises(ValueError):
            validate_feature_column("col; DROP TABLE")

    def test_quotes_rejected(self):
        """Task 8.4: Quotes rejected."""
        with pytest.raises(ValueError):
            validate_feature_column("col'name")

    def test_punctuation_rejected(self):
        """Task 8.4: Punctuation rejected."""
        with pytest.raises(ValueError):
            validate_feature_column("col.name")

    def test_validate_list(self):
        """Task 8.6: validate_feature_columns works on lists."""
        result = validate_feature_columns(["momentum", "volatility"])
        assert result == ["momentum", "volatility"]

    def test_validate_list_none(self):
        assert validate_feature_columns(None) == []
        assert validate_feature_columns([]) == []

    def test_validate_list_invalid(self):
        """Task 8.7: Invalid names in list raise."""
        with pytest.raises(ValueError):
            validate_feature_columns(["valid", "in valid"])


# ---------------------------------------------------------------------------
# Price basis resolution tests (Section 7)
# ---------------------------------------------------------------------------


class TestPriceBasisResolution:
    """Tests for price basis column mapping."""

    def test_default_price_basis(self):
        """Task 7.3: Default price basis is adjusted."""
        assert DEFAULT_PRICE_BASIS == "adjusted_ohlc_adj_columns"

    def test_adjusted_column_mapping(self):
        """Task 7.7: Default adjusted column mapping."""
        name, mapping = resolve_price_basis(None)
        assert name == "adjusted_ohlc_adj_columns"
        assert mapping["open"] == "openadj"
        assert mapping["high"] == "highadj"
        assert mapping["low"] == "lowadj"
        assert mapping["close"] == "closeadj"
        assert mapping["volume"] == "dailyvolumeadj"

    def test_explicit_adjusted(self):
        name, mapping = resolve_price_basis("adjusted_ohlc_adj_columns")
        assert name == "adjusted_ohlc_adj_columns"
        assert mapping["close"] == "closeadj"

    def test_raw_column_mapping(self):
        name, mapping = resolve_price_basis("raw")
        assert name == "raw"
        assert mapping["open"] == "open"
        assert mapping["close"] == "close"
        assert mapping["volume"] == "dailyvolume"

    def test_unsupported_price_basis(self):
        """Task 7.8: Unsupported price basis fails with clear error."""
        with pytest.raises(ValueError, match="Unsupported price basis"):
            resolve_price_basis("nonexistent_basis")


# ---------------------------------------------------------------------------
# Smoke example rule (Section 14)
# ---------------------------------------------------------------------------


class TestSmokeExampleRule:
    """Tests for the focused smoke example rule."""

    def _make_smoke_rule(self, warmup_bars=3):
        """Task 14.1-14.4: Minimal rolling-window scan rule."""
        def smoke_rule(context):
            # Task 14.2: Return not_ready until warmup bars available
            if context.bar_index < warmup_bars:
                return ScanDecision(
                    candidate_status="not_ready",
                    reason_code="insufficient_history",
                    diagnostics={"bar_index": context.bar_index, "warmup": warmup_bars},
                )

            # Compute simple rolling condition: close > rolling mean
            window = context.close[context.bar_index - warmup_bars: context.bar_index]
            rolling_mean = sum(window) / len(window)
            current_close = context.close[context.bar_index]

            # Task 14.3: Return accepted when condition passes
            if current_close > rolling_mean:
                return ScanDecision(
                    candidate_status="accepted",
                    reason_code="close_above_rolling_mean",
                    score=current_close,
                    gates={"rolling_mean_gate": True},
                    diagnostics={
                        "close": current_close,
                        "rolling_mean": rolling_mean,
                    },
                )

            # Task 14.4: Return rejected with reason code when fails
            return ScanDecision(
                candidate_status="rejected",
                reason_code="close_below_rolling_mean",
                score=current_close,
                gates={"rolling_mean_gate": False},
                diagnostics={
                    "close": current_close,
                    "rolling_mean": rolling_mean,
                },
            )
        return smoke_rule

    def test_smoke_rule_not_ready(self):
        """Task 14.2: Returns not_ready during warmup."""
        rule = self._make_smoke_rule(warmup_bars=3)
        ctx = ScanRuleContext(
            secid=1,
            display_symbol="TST",
            ticker_at_date="TST",
            identity_source="ticker",
            runtime_symbol="TST",
            date="2024-01-01",
            bar_index=0,
            dates=[date(2024, 1, 1)],
            open=[100.0],
            high=[100.0],
            low=[100.0],
            close=[100.0],
            volume=[1000.0],
        )
        decision = rule(ctx)
        assert decision.candidate_status == "not_ready"
        assert decision.reason_code == "insufficient_history"

    def test_smoke_rule_accepted(self):
        """Task 14.3: Returns accepted when condition passes."""
        rule = self._make_smoke_rule(warmup_bars=2)
        ctx = ScanRuleContext(
            secid=1,
            display_symbol="TST",
            ticker_at_date="TST",
            identity_source="ticker",
            runtime_symbol="TST",
            date="2024-01-03",
            bar_index=2,
            dates=[date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            open=[100.0, 100.0, 100.0],
            high=[100.0, 100.0, 100.0],
            low=[100.0, 100.0, 100.0],
            close=[100.0, 100.0, 110.0],  # close > rolling mean
            volume=[1000.0, 1000.0, 1000.0],
        )
        decision = rule(ctx)
        assert decision.candidate_status == "accepted"
        assert decision.reason_code == "close_above_rolling_mean"

    def test_smoke_rule_rejected(self):
        """Task 14.4: Returns rejected when condition fails."""
        rule = self._make_smoke_rule(warmup_bars=2)
        ctx = ScanRuleContext(
            secid=1,
            display_symbol="TST",
            ticker_at_date="TST",
            identity_source="ticker",
            runtime_symbol="TST",
            date="2024-01-03",
            bar_index=2,
            dates=[date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)],
            open=[100.0, 100.0, 100.0],
            high=[100.0, 100.0, 100.0],
            low=[100.0, 100.0, 100.0],
            close=[100.0, 100.0, 90.0],  # close < rolling mean
            volume=[1000.0, 1000.0, 1000.0],
        )
        decision = rule(ctx)
        assert decision.candidate_status == "rejected"
        assert decision.reason_code == "close_below_rolling_mean"

    def test_smoke_rule_through_runner(self):
        """Task 14.5: Smoke test through ScanRunner without ClickHouse."""
        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        def loader(_):
            return {
                "closes": [100.0, 100.0, 100.0, 110.0, 90.0],
                "highs": [100.0, 100.0, 100.0, 110.0, 90.0],
                "lows": [100.0, 100.0, 100.0, 110.0, 90.0],
                "opens": [100.0, 100.0, 100.0, 110.0, 90.0],
                "volumes": [1000.0, 1000.0, 1000.0, 1000.0, 1000.0],
                "dates": [
                    date(2024, 1, 1),
                    date(2024, 1, 2),
                    date(2024, 1, 3),
                    date(2024, 1, 4),
                    date(2024, 1, 5),
                ],
            }

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=loader,
            candidate_rule=self._make_smoke_rule(warmup_bars=2),
            strategy_id="smoke_test",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            results, summary = runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            assert summary.total_rows == 5
            # First 2 bars: not_ready
            assert results[0].candidate_status == "not_ready"
            assert results[1].candidate_status == "not_ready"
            # Bar 3: close=100, rolling_mean=100 → not above → rejected
            assert results[2].candidate_status == "rejected"
            # Bar 4: close=110, rolling_mean=100 → accepted
            assert results[3].candidate_status == "accepted"
            # Bar 5: close=90, rolling_mean=105 → rejected
            assert results[4].candidate_status == "rejected"

            # Verify manifest was written
            manifest_path = Path(tmpdir) / "scan_manifest.json"
            assert manifest_path.exists()

            # Verify CSV was written
            csv_path = Path(tmpdir) / "candidate_scan_results.csv"
            assert csv_path.exists()


# ---------------------------------------------------------------------------
# Regression tests for review findings (section 16)
# ---------------------------------------------------------------------------


class TestReviewRegressionFindings:
    """Regression tests for external review findings in section 16 of tasks.md."""

    def test_legacy_3arg_rule_shape_through_runner(self):
        """16.1 regression: Old 3-arg rule (secid, date_str, features) still works."""

        def legacy_3arg_rule(secid, date_str, features):
            return ("candidate", "legacy_rule", 0.5, {}, features or {})

        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        def loader(_):
            return {
                "closes": [100.0, 101.0],
                "highs": [100.0, 101.0],
                "lows": [100.0, 101.0],
                "opens": [100.0, 101.0],
                "volumes": [1000.0, 1000.0],
                "dates": [date(2024, 1, 1), date(2024, 1, 2)],
            }

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=loader,
            candidate_rule=legacy_3arg_rule,
            strategy_id="legacy_3arg_test",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            results, summary = runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            assert summary.instruments_processed == 1
            assert len(results) == 2
            # Legacy "candidate" status should be adapted to "accepted"
            assert results[0].candidate_status == "accepted"
            assert results[0].reason_code == "legacy_rule"

    def test_cli_manifest_metadata_propagation(self):
        """16.2 regression: ScanRunner receives and persists manifest metadata."""
        resolver = MagicMock()

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        def loader(_):
            return {
                "closes": [100.0],
                "highs": [100.0],
                "lows": [100.0],
                "opens": [100.0],
                "volumes": [1000.0],
                "dates": [date(2024, 1, 1)],
            }

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=loader,
            candidate_rule=lambda ctx: ScanDecision(
                candidate_status="accepted", reason_code="test"
            ),
            strategy_id="manifest_test",
            rule_import_path="my_module.my_rule",
            database="market",
            bars_table="as_us_equity_ohlc_daily",
            source_columns={
                "open": "openadj",
                "high": "highadj",
                "low": "lowadj",
                "close": "closeadj",
                "volume": "dailyvolumeadj",
            },
            price_basis="adjusted_ohlc_adj_columns",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 1, 31)),
                output_dir=Path(tmpdir),
            )

            manifest_path = Path(tmpdir) / "scan_manifest.json"
            assert manifest_path.exists()
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            # Verify rule metadata
            assert manifest["rule"]["rule_import_path"] == "my_module.my_rule"
            assert manifest["rule"]["strategy_id"] == "manifest_test"

            # Verify data metadata
            assert manifest["data"]["database"] == "market"
            assert manifest["data"]["bars_table"] == "as_us_equity_ohlc_daily"
            assert manifest["data"]["price_basis"] == "adjusted_ohlc_adj_columns"
            assert manifest["data"]["source_columns"]["close"] == "closeadj"

    def test_resolved_instrument_coverage_and_ambiguity_serialization(self):
        """16.2 regression: Manifest uses correct ResolvedInstrument attributes."""
        resolver = MagicMock()

        class CandidateMapping:
            def __init__(self):
                self.secid = 2
                self.display_symbol = "OLD"
                self.overlap_start = date(2023, 1, 1)
                self.overlap_end = date(2023, 6, 30)

        class ResolvedInstrument:
            def __init__(self):
                self.secid = 1
                self.display_symbol = "TST"
                self.ticker_at_date = "TST"
                self.identity_source = "ticker"
                self.first_date = date(2020, 1, 1)
                self.last_date = date(2024, 12, 31)
                self.requested_start_date = date(2024, 1, 1)
                self.requested_end_date = date(2024, 12, 31)
                self.ambiguous = True
                self.candidates = [CandidateMapping()]

        resolver.resolve_batch.return_value = {"TST": ResolvedInstrument()}

        def loader(_):
            return {
                "closes": [100.0],
                "highs": [100.0],
                "lows": [100.0],
                "opens": [100.0],
                "volumes": [1000.0],
                "dates": [date(2024, 1, 1)],
            }

        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=loader,
            candidate_rule=lambda ctx: ScanDecision(
                candidate_status="accepted", reason_code="test"
            ),
            strategy_id="ambiguity_test",
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            runner.run(
                tickers=["TST"],
                date_range=(date(2024, 1, 1), date(2024, 12, 31)),
                output_dir=Path(tmpdir),
            )

            manifest_path = Path(tmpdir) / "scan_manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

            instrument = manifest["resolved_instruments"][0]

            # Verify coverage dates use correct attributes
            assert "coverage_dates" in instrument
            assert instrument["coverage_dates"]["first_date"] == "2020-01-01"
            assert instrument["coverage_dates"]["last_date"] == "2024-12-31"

            # Verify requested range
            assert "requested_range" in instrument
            assert instrument["requested_range"]["start_date"] == "2024-01-01"
            assert instrument["requested_range"]["end_date"] == "2024-12-31"

            # Verify ambiguity flag
            assert instrument["ambiguous"] is True

            # Verify candidate mappings
            assert "candidates" in instrument
            assert len(instrument["candidates"]) == 1
            assert instrument["candidates"][0]["secid"] == 2
            assert instrument["candidates"][0]["display_symbol"] == "OLD"
            assert instrument["candidates"][0]["overlap_start"] == "2023-01-01"
            assert instrument["candidates"][0]["overlap_end"] == "2023-06-30"

    def test_identifier_rejects_leading_digit(self):
        """16.4 regression: Feature column validator rejects leading digits."""
        # Leading digit should fail
        with pytest.raises(ValueError, match="start with a letter or underscore"):
            validate_feature_column("123bad")

        # Valid identifiers should pass
        assert validate_feature_column("good_col") == "good_col"
        assert validate_feature_column("_private") == "_private"
        assert validate_feature_column("Col123") == "Col123"

        # Other invalid cases
        with pytest.raises(ValueError):
            validate_feature_column("")
        with pytest.raises(ValueError):
            validate_feature_column("has space")
        with pytest.raises(ValueError):
            validate_feature_column("has-dash")
