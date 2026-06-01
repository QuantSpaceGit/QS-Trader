"""Tests for Phase 2A.3 — Benchmark overlay.

Covers:

* :func:`derive_benchmark_child_config` unit tests (T3.2): instrument set,
  strategy swap, run_range overrides, input not mutated, type preservation.
* :func:`check_benchmark_data_availability` (T3.3): no data and partial
  coverage both raise ``BenchmarkDataUnavailableError``.
* :class:`BenchmarkSpec` plan-load strictness: empty instrument, bogus
  strategy name, extra fields rejected.
* Plan-hash stability: ``benchmark=None`` keeps the ``428e27b2`` static plan
  pin intact.
* Runner integration: benchmark child run successful path writes a
  ``benchmark/`` directory and surfaces a ``ChildRunRef`` with
  ``role='benchmark'``; failure path returns ``status='failed'``.
* CLI integration: pre-flight unavailable → exit 3 + reason code; dry-run
  prints the benchmark line; end-to-end with a successful benchmark emits
  the ``benchmark`` summary block alongside ``cost_scenarios`` when both
  are declared.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml
from pydantic import ValidationError

from qs_trader.engine.config import (
    BacktestConfig,
    DataSelectionConfig,
    DataSourceConfig,
    RiskPolicyConfig,
    StrategyConfigItem,
)
from qs_trader.validation.benchmark import (
    BENCHMARK_FOLD_ID,
    BENCHMARK_ROLE,
    BenchmarkDataUnavailableError,
    benchmark_full_range,
    check_benchmark_data_availability,
    derive_benchmark_child_config,
)
from qs_trader.validation.plan import (
    BenchmarkSpec,
    CostScenarioSpec,
    DateRange,
    ExecutionSpec,
    StaticSplitSpec,
    ValidationPlan,
    _plan_to_canonical_dict,
)
from qs_trader.validation.runner import SequentialValidationRunner

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers (mirrored from test_cost_scenarios.py for parity)
# ---------------------------------------------------------------------------


def _make_base_config() -> BacktestConfig:
    return BacktestConfig(
        backtest_id="benchmark_test",
        start_date=datetime(2020, 1, 2),
        end_date=datetime(2020, 12, 30),
        initial_equity=Decimal("100000"),
        data=DataSelectionConfig(sources=[DataSourceConfig(name="yahoo-us-equity-1d-csv", universe=["AAPL"])]),
        strategies=[
            StrategyConfigItem(
                strategy_id="custom_momentum",
                universe=["AAPL"],
                data_sources=["yahoo-us-equity-1d-csv"],
                config={"lookback": 20},
            )
        ],
        risk_policy=RiskPolicyConfig(name="naive", config={}),
    )


def _make_plan(
    *,
    benchmark: BenchmarkSpec | None,
    cost_scenarios: list[CostScenarioSpec] | None = None,
) -> ValidationPlan:
    return ValidationPlan(
        validation_id="benchmark_test",
        strategy_experiment="test_strategy",
        base_config=FIXTURES_DIR / "runner_base_config.yaml",
        mode="static_is_oos",
        splits=StaticSplitSpec(
            in_sample=DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 6, 30)),
            out_of_sample=DateRange(start_date=date(2020, 7, 1), end_date=date(2020, 12, 30)),
        ),
        execution=ExecutionSpec(on_child_failure="continue"),
        cost_scenarios=cost_scenarios,
        benchmark=benchmark,
    )


def _make_splits() -> list[Any]:
    from qs_trader.validation.splits.base import ValidationSplit

    return [
        ValidationSplit(
            fold_index=0,
            role="is",
            test_range=DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 6, 30)),
        ),
        ValidationSplit(
            fold_index=1,
            role="oos",
            test_range=DateRange(start_date=date(2020, 7, 1), end_date=date(2020, 12, 30)),
        ),
    ]


def _make_mock_engine(bars: int = 100) -> MagicMock:
    mock_result = Mock()
    mock_result.bars_processed = bars
    mock_result.duration = timedelta(seconds=1)
    mock_engine = MagicMock()
    mock_engine.run.return_value = mock_result
    mock_engine.__enter__ = Mock(return_value=mock_engine)
    mock_engine.__exit__ = Mock(return_value=False)
    return mock_engine


def _make_failing_engine() -> MagicMock:
    mock_engine = MagicMock()
    mock_engine.run.side_effect = RuntimeError("simulated benchmark failure")
    mock_engine.__enter__ = Mock(return_value=mock_engine)
    mock_engine.__exit__ = Mock(return_value=False)
    return mock_engine


# ---------------------------------------------------------------------------
# BenchmarkSpec — plan-load strictness
# ---------------------------------------------------------------------------


class TestBenchmarkSpec:
    def test_minimal_construction(self) -> None:
        spec = BenchmarkSpec(instrument="SPY")
        assert spec.instrument == "SPY"
        assert spec.strategy == "buy_and_hold"
        assert spec.reinvest_dividends is True

    def test_empty_instrument_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BenchmarkSpec(instrument="")

    def test_whitespace_instrument_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BenchmarkSpec(instrument="   ")

    def test_invalid_instrument_characters_rejected(self) -> None:
        with pytest.raises(ValidationError, match=r"must match"):
            BenchmarkSpec(instrument="SPY/USD")

    def test_bogus_strategy_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BenchmarkSpec(instrument="SPY", strategy="momentum")  # type: ignore[arg-type]

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            BenchmarkSpec(instrument="SPY", bogus_extra=True)  # type: ignore[call-arg]


# ---------------------------------------------------------------------------
# benchmark_full_range
# ---------------------------------------------------------------------------


class TestBenchmarkFullRange:
    def test_static_plan_returns_is_oos_union(self) -> None:
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        full = benchmark_full_range(plan)
        assert full.start_date == date(2020, 1, 2)
        assert full.end_date == date(2020, 12, 30)


# ---------------------------------------------------------------------------
# T3.2 — derive_benchmark_child_config
# ---------------------------------------------------------------------------


class TestDeriveBenchmarkChildConfig:
    def test_instrument_overrides_universe(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        full = benchmark_full_range(plan)
        child = derive_benchmark_child_config(plan, full, base)
        assert child.data.sources[0].universe == ["SPY"]
        assert child.strategies[0].universe == ["SPY"]

    def test_strategy_swapped_to_buy_and_hold(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        full = benchmark_full_range(plan)
        child = derive_benchmark_child_config(plan, full, base)
        assert len(child.strategies) == 1
        assert child.strategies[0].strategy_id == "buy_and_hold"
        assert child.strategies[0].config == {"reinvest_dividends": True}

    def test_reinvest_dividends_propagates(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY", reinvest_dividends=False))
        full = benchmark_full_range(plan)
        child = derive_benchmark_child_config(plan, full, base)
        assert child.strategies[0].config == {"reinvest_dividends": False}

    def test_run_range_set_to_full_range(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        full = benchmark_full_range(plan)
        child = derive_benchmark_child_config(plan, full, base)
        assert child.start_date == datetime(2020, 1, 2)
        assert child.end_date == datetime(2020, 12, 30)

    def test_base_config_not_mutated(self) -> None:
        base = _make_base_config()
        original_dump = base.model_dump(mode="json")
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        _ = derive_benchmark_child_config(plan, benchmark_full_range(plan), base)
        assert base.model_dump(mode="json") == original_dump

    def test_type_preserved(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        child = derive_benchmark_child_config(plan, benchmark_full_range(plan), base)
        assert isinstance(child, BacktestConfig)

    def test_backtest_id_labelled(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        child = derive_benchmark_child_config(plan, benchmark_full_range(plan), base)
        assert child.backtest_id.endswith("__benchmark")

    def test_data_source_name_preserved(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        child = derive_benchmark_child_config(plan, benchmark_full_range(plan), base)
        assert child.data.sources[0].name == base.data.sources[0].name

    def test_requires_benchmark_declared(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=None)
        with pytest.raises(ValueError, match="plan.benchmark"):
            derive_benchmark_child_config(
                plan, DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 12, 30)), base
            )


# ---------------------------------------------------------------------------
# T3.3 — check_benchmark_data_availability
# ---------------------------------------------------------------------------


class TestCheckBenchmarkDataAvailability:
    def test_noop_when_no_benchmark(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=None)
        # Must not raise; injected loader would normally explode if called.
        check_benchmark_data_availability(
            plan,
            DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 12, 30)),
            base,
            loader=lambda *a, **kw: (_ for _ in ()).throw(AssertionError("loader called")),
        )

    def test_no_data_raises(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        with pytest.raises(BenchmarkDataUnavailableError) as exc_info:
            check_benchmark_data_availability(
                plan,
                benchmark_full_range(plan),
                base,
                loader=lambda *a, **kw: [],
            )
        assert exc_info.value.instrument == "SPY"

    def test_partial_coverage_raises(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        full = benchmark_full_range(plan)
        # Loader returns bars that start after full.start_date.
        bars = [date(2020, 6, 1), date(2020, 6, 2), date(2020, 12, 30)]
        with pytest.raises(BenchmarkDataUnavailableError, match="partial coverage"):
            check_benchmark_data_availability(plan, full, base, loader=lambda *a, **kw: bars)

    def test_complete_coverage_passes(self) -> None:
        base = _make_base_config()
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        full = benchmark_full_range(plan)
        bars = [full.start_date, date(2020, 6, 1), full.end_date]
        # Should not raise
        check_benchmark_data_availability(plan, full, base, loader=lambda *a, **kw: bars)


# ---------------------------------------------------------------------------
# Plan-hash stability — benchmark=None must keep 428e27b2 pin
# ---------------------------------------------------------------------------


class TestPlanHashStabilityForBenchmark:
    def test_benchmark_none_kept_in_canonical_dict(self) -> None:
        """Phase 1 plans serialise ``benchmark: null`` — preserve that shape."""
        plan = _make_plan(benchmark=None)
        d = _plan_to_canonical_dict(plan)
        # Either explicitly null OR absent — both shapes must keep the
        # 428e27b2 pin intact. The current implementation keeps the key
        # present with a null value (see plan._plan_to_canonical_dict).
        assert d.get("benchmark") is None

    def test_benchmark_declared_changes_canonical_dict(self) -> None:
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        d = _plan_to_canonical_dict(plan)
        assert d["benchmark"] == {
            "instrument": "SPY",
            "strategy": "buy_and_hold",
            "reinvest_dividends": True,
        }

    def test_static_reference_plan_hash_pin_unchanged(self) -> None:
        ref_plan = (
            Path(__file__).resolve().parents[3]
            / "QS-Research"
            / ("experiments/buy_hold/validations/buy_hold_oos_2024.yaml")
        )
        if not ref_plan.exists():
            pytest.skip("QS-Research sibling repo not present")
        from qs_trader.validation.plan import compute_plan_sha256, load_validation_plan

        plan = load_validation_plan(ref_plan)
        sha = compute_plan_sha256(plan, plan.base_config)
        assert sha.startswith("428e27b2"), f"Static plan hash drifted: {sha[:12]}"


# ---------------------------------------------------------------------------
# Runner integration — run_benchmark()
# ---------------------------------------------------------------------------


class TestRunBenchmark:
    def _runner(self, plan: ValidationPlan, tmp_path: Path) -> SequentialValidationRunner:
        return SequentialValidationRunner(
            plan=plan,
            splits=_make_splits(),
            base_config=_make_base_config(),
            validations_dir=tmp_path / "validations" / plan.validation_id,
        )

    def test_run_benchmark_success(self, tmp_path: Path) -> None:
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        runner = self._runner(plan, tmp_path)
        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = _make_mock_engine()
            ref = runner.run_benchmark()
        assert ref.status == "success"
        assert ref.fold_id == BENCHMARK_FOLD_ID
        assert ref.role == BENCHMARK_ROLE
        assert ref.scenario is None
        assert ref.run_dir.is_dir()
        assert ref.run_dir.name == "benchmark"
        assert ref.run_id == "val_benchmark_test__benchmark"

    def test_run_benchmark_failure_returns_failed_ref(self, tmp_path: Path) -> None:
        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        runner = self._runner(plan, tmp_path)
        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = _make_failing_engine()
            ref = runner.run_benchmark()
        assert ref.status == "failed"
        assert ref.error is not None
        assert "simulated benchmark failure" in ref.error

    def test_run_benchmark_requires_declaration(self, tmp_path: Path) -> None:
        plan = _make_plan(benchmark=None)
        runner = self._runner(plan, tmp_path)
        with pytest.raises(ValueError, match="plan.benchmark"):
            runner.run_benchmark()

    def test_run_benchmark_forwards_injected_system_config(self, tmp_path: Path) -> None:
        """Injected system_config must reach BacktestEngine.from_config (regression)."""
        from unittest.mock import sentinel

        plan = _make_plan(benchmark=BenchmarkSpec(instrument="SPY"))
        sentinel_cfg = sentinel.system_config

        runner = SequentialValidationRunner(
            plan=plan,
            splits=_make_splits(),
            base_config=_make_base_config(),
            validations_dir=tmp_path / "validations" / plan.validation_id,
            system_config=sentinel_cfg,
        )

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = _make_mock_engine()
            runner.run_benchmark()

        call_kwargs = MockEngine.from_config.call_args
        assert call_kwargs is not None, "BacktestEngine.from_config was not called"
        assert call_kwargs.kwargs.get("system_config") is sentinel_cfg, (
            "run_benchmark() did not forward system_config to BacktestEngine.from_config"
        )


# ---------------------------------------------------------------------------
# CLI end-to-end — dry-run, pre-flight failure, summary block
# ---------------------------------------------------------------------------


def _write_plan_yaml(
    tmp_path: Path,
    *,
    validation_id: str,
    benchmark: dict | None = None,
    cost_scenarios: list[dict] | None = None,
) -> Path:
    base_cfg_path = tmp_path / "base.yaml"
    base_cfg_path.write_text((FIXTURES_DIR / "runner_base_config.yaml").read_text())
    plan_yaml = tmp_path / f"{validation_id}.yaml"
    body: dict[str, Any] = {
        "validation_id": validation_id,
        "strategy_experiment": "test_strategy",
        "base_config": str(base_cfg_path),
        "mode": "static_is_oos",
        "splits": {
            "in_sample": {"start_date": "2020-01-02", "end_date": "2020-06-30"},
            "out_of_sample": {"start_date": "2020-07-01", "end_date": "2020-12-30"},
        },
        "decision": {"rules": {"oos_sharpe_min": 0.5}},
    }
    if benchmark is not None:
        body["benchmark"] = benchmark
    if cost_scenarios is not None:
        body["cost_scenarios"] = cost_scenarios
    plan_yaml.write_text(yaml.safe_dump(body))
    return plan_yaml


class TestCliDryRunBenchmark:
    def test_dry_run_prints_benchmark_line(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml = _write_plan_yaml(
            tmp_path,
            validation_id="dr_bench",
            benchmark={"instrument": "SPY", "strategy": "buy_and_hold", "reinvest_dividends": True},
        )
        result = CliRunner().invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "Benchmark:" in result.output
        assert "instrument=SPY" in result.output
        assert "strategy=buy_and_hold" in result.output
        assert "reinvest_dividends=True" in result.output

    def test_dry_run_no_benchmark_section_when_absent(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml = _write_plan_yaml(tmp_path, validation_id="dr_nobench")
        result = CliRunner().invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert result.exit_code == 0, result.output
        assert "Benchmark:" not in result.output


class TestCliBenchmarkDataUnavailable:
    def test_preflight_failure_exits_3(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml = _write_plan_yaml(
            tmp_path,
            validation_id="bench_missing",
            benchmark={"instrument": "MISSING"},
        )

        with patch(
            "qs_trader.validation.benchmark._default_benchmark_loader",
            return_value=[],
        ):
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])
        assert result.exit_code == 3, result.output
        assert "benchmark_data_unavailable:MISSING" in (result.output + (result.stderr or ""))


class TestCliBenchmarkEndToEnd:
    def _write_perf(self, run_dir: Path, sharpe: float, total_return: float = 0.1) -> None:
        run_dir.mkdir(parents=True, exist_ok=True)
        perf = {
            "total_return": total_return,
            "cagr": 0.1,
            "sharpe_ratio": sharpe,
            "max_drawdown": 0.05,
            "volatility": 0.1,
            "num_trades": 40,
        }
        (run_dir / "performance.json").write_text(json.dumps(perf))

    def test_benchmark_summary_block_written(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml = _write_plan_yaml(
            tmp_path,
            validation_id="bench_ok",
            benchmark={"instrument": "SPY"},
        )
        out_dir = plan_yaml.parent / "bench_ok"

        def fake_from_config(child_config: Any, **kwargs: Any) -> MagicMock:
            results_dir = Path(kwargs["results_dir"])
            # Strategy folds get sharpe=1.5; benchmark child gets sharpe=0.8.
            sharpe = 0.8 if "benchmark" in results_dir.name else 1.5
            total = 0.05 if "benchmark" in results_dir.name else 0.2
            self._write_perf(results_dir, sharpe=sharpe, total_return=total)
            return _make_mock_engine()

        with (
            patch("qs_trader.engine.engine.BacktestEngine") as MockEngine,
            patch("qs_trader.validation.benchmark._default_benchmark_loader") as mock_loader,
        ):
            MockEngine.from_config.side_effect = fake_from_config
            mock_loader.return_value = [date(2020, 1, 2), date(2020, 12, 30)]
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])
        assert result.exit_code in (0, 1, 2, 3), result.output

        # Benchmark directory exists.
        assert (out_dir / "benchmark").is_dir()
        assert (out_dir / "benchmark" / "performance.json").exists()

        summary = json.loads((out_dir / "summary.json").read_text())
        assert "benchmark" in summary
        block = summary["benchmark"]
        assert block["instrument"] == "SPY"
        assert block["metrics"]["sharpe_ratio"] == 0.8
        # strategy(OOS) - benchmark = 1.5 - 0.8 = 0.7
        assert block["strategy_minus_benchmark"]["sharpe_ratio"] == pytest.approx(0.7)
        assert block["strategy_minus_benchmark"]["total_return"] == pytest.approx(0.15)

    def test_benchmark_absent_omits_summary_block(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml = _write_plan_yaml(tmp_path, validation_id="no_bench")
        out_dir = plan_yaml.parent / "no_bench"

        def fake_from_config(child_config: Any, **kwargs: Any) -> MagicMock:
            results_dir = Path(kwargs["results_dir"])
            self._write_perf(results_dir, sharpe=1.5)
            return _make_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = fake_from_config
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])
        assert result.exit_code in (0, 1, 2, 3), result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert "benchmark" not in summary
        assert not (out_dir / "benchmark").exists()

    def test_benchmark_run_failure_marks_invalid(self, tmp_path: Path) -> None:
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml = _write_plan_yaml(
            tmp_path,
            validation_id="bench_fail",
            benchmark={"instrument": "SPY"},
        )
        out_dir = plan_yaml.parent / "bench_fail"

        call_count = {"n": 0}

        def fake_from_config(child_config: Any, **kwargs: Any) -> MagicMock:
            results_dir = Path(kwargs["results_dir"])
            call_count["n"] += 1
            if "benchmark" in results_dir.name:
                return _make_failing_engine()
            self._write_perf(results_dir, sharpe=1.5)
            return _make_mock_engine()

        with (
            patch("qs_trader.engine.engine.BacktestEngine") as MockEngine,
            patch("qs_trader.validation.benchmark._default_benchmark_loader") as mock_loader,
        ):
            MockEngine.from_config.side_effect = fake_from_config
            mock_loader.return_value = [date(2020, 1, 2), date(2020, 12, 30)]
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])
        assert result.exit_code == 3, result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] == "Invalid"
        assert "benchmark_run_failed" in summary["reason_codes"]
        # No benchmark block when run failed.
        assert "benchmark" not in summary

    def test_benchmark_failure_in_continue_mode_marks_invalid(self, tmp_path: Path) -> None:
        """With ``on_child_failure: continue`` + a failing strategy fold + a
        failing benchmark child, the top-level outcome must still be
        ``Invalid`` and ``benchmark_run_failed`` must appear in the reason
        codes list.  Locks the contract that a failed benchmark is fatal
        regardless of fold-failure policy.
        """
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml = _write_plan_yaml(
            tmp_path,
            validation_id="bench_cont_fail",
            benchmark={"instrument": "SPY"},
        )
        # Inject continue mode + a fold that always fails alongside the
        # failing benchmark child.
        body = yaml.safe_load(plan_yaml.read_text())
        body["execution"] = {"on_child_failure": "continue"}
        plan_yaml.write_text(yaml.safe_dump(body))

        out_dir = plan_yaml.parent / "bench_cont_fail"

        def fake_from_config(child_config: Any, **kwargs: Any) -> MagicMock:
            # All children (strategy folds AND benchmark) fail.
            return _make_failing_engine()

        with (
            patch("qs_trader.engine.engine.BacktestEngine") as MockEngine,
            patch("qs_trader.validation.benchmark._default_benchmark_loader") as mock_loader,
        ):
            MockEngine.from_config.side_effect = fake_from_config
            mock_loader.return_value = [date(2020, 1, 2), date(2020, 12, 30)]
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code == 3, result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert summary["outcome"] == "Invalid"
        assert "benchmark_run_failed" in summary["reason_codes"]

    def test_cost_scenarios_and_benchmark_coexist(self, tmp_path: Path) -> None:
        """Both ``cost_scenarios`` and ``benchmark`` blocks populate independently."""
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        plan_yaml = _write_plan_yaml(
            tmp_path,
            validation_id="combo",
            benchmark={"instrument": "SPY"},
            cost_scenarios=[
                {"name": "base", "overrides": {}},
                {"name": "high", "overrides": {"replay_speed": 0.0}},
            ],
        )
        out_dir = plan_yaml.parent / "combo"

        def fake_from_config(child_config: Any, **kwargs: Any) -> MagicMock:
            results_dir = Path(kwargs["results_dir"])
            sharpe = 0.8 if "benchmark" in results_dir.name else 1.2
            self._write_perf(results_dir, sharpe=sharpe)
            return _make_mock_engine()

        with (
            patch("qs_trader.engine.engine.BacktestEngine") as MockEngine,
            patch("qs_trader.validation.benchmark._default_benchmark_loader") as mock_loader,
        ):
            MockEngine.from_config.side_effect = fake_from_config
            mock_loader.return_value = [date(2020, 1, 2), date(2020, 12, 30)]
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])
        assert result.exit_code in (0, 1, 2, 3), result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert "cost_scenarios" in summary
        assert "benchmark" in summary
        assert summary["benchmark"]["instrument"] == "SPY"
        # Both scenarios should be present in the cost block.
        names = [s["name"] for s in summary["cost_scenarios"]]
        assert names == ["base", "high"]
        # Benchmark dir co-exists with scenarios dir.
        assert (out_dir / "benchmark").is_dir()
        assert (out_dir / "scenarios" / "base").is_dir()
        assert (out_dir / "scenarios" / "high").is_dir()


# ---------------------------------------------------------------------------
# compute_strategy_minus_benchmark — unit
# ---------------------------------------------------------------------------


class TestComputeStrategyMinusBenchmark:
    def test_returns_subset_diff(self) -> None:
        from qs_trader.validation.reporting.summary import compute_strategy_minus_benchmark

        d = compute_strategy_minus_benchmark(
            {"sharpe_ratio": 1.5, "total_return": 0.2, "extra": 99.0},
            {"sharpe_ratio": 0.8, "total_return": 0.05, "extra": 1.0},
        )
        assert d == {"sharpe_ratio": pytest.approx(0.7), "total_return": pytest.approx(0.15)}

    def test_missing_metric_skipped(self) -> None:
        from qs_trader.validation.reporting.summary import compute_strategy_minus_benchmark

        d = compute_strategy_minus_benchmark(
            {"sharpe_ratio": 1.5},
            {"sharpe_ratio": 0.8, "total_return": 0.05},
        )
        assert d == {"sharpe_ratio": pytest.approx(0.7)}


# ---------------------------------------------------------------------------
# First-party strategy registration — unmocked end-to-end
# ---------------------------------------------------------------------------


def _make_real_system_config(tmp_path: Path, custom_strategies_dir: Path | None) -> Mock:
    """Build a Mock ``SystemConfig`` that points at real test fixtures.

    Mirrors ``tests/integration/conftest.py::mock_system_config`` but exposes
    a configurable ``custom_libraries.strategies`` so the test can prove the
    engine resolves first-party ``buy_and_hold`` without operator-provided
    custom libraries.
    """

    mock_config = Mock()

    mock_config.data = Mock()
    mock_config.data.sources_config = "tests/fixtures/config/data_sources.yaml"
    mock_config.data.default_mode = "adjusted"
    mock_config.data.default_timezone = "America/New_York"

    output_dir = tmp_path / "experiments"
    output_dir.mkdir(parents=True, exist_ok=True)
    mock_config.output = Mock()
    mock_config.output.experiments_root = str(output_dir)
    mock_config.output.run_id_format = "%Y%m%d_%H%M%S"
    mock_config.output.artifact_policy = Mock()
    mock_config.output.artifact_policy.mode = "filesystem"
    mock_config.output.capture_git_info = False
    mock_config.output.capture_environment = False
    mock_config.output.event_store = Mock()
    mock_config.output.event_store.backend = "parquet"
    mock_config.output.event_store.filename = "events.parquet"

    mock_config.custom_libraries = Mock()
    mock_config.custom_libraries.strategies = str(custom_strategies_dir) if custom_strategies_dir is not None else None

    mock_config.logging = Mock()
    logger_cfg = Mock()
    logger_cfg.level = "WARNING"
    logger_cfg.format = "json"
    logger_cfg.timestamp_format = "compact"
    logger_cfg.enable_file = False
    logger_cfg.file_path = None
    logger_cfg.file_level = "WARNING"
    logger_cfg.file_rotation = False
    logger_cfg.max_file_size_mb = 10
    logger_cfg.backup_count = 3
    logger_cfg.console_width = 0
    mock_config.logging.to_logger_config = Mock(return_value=logger_cfg)

    return mock_config


class TestBuiltinStrategyRegistrationEndToEnd:
    """Unmocked end-to-end backstop for the Phase 2A.3 benchmark overlay.

    The validation framework's benchmark child run depends on the engine
    being able to resolve ``buy_and_hold`` via the strategy registry.  All
    other tests in this module mock ``BacktestEngine.from_config``, so they
    would not catch a regression where the first-party
    ``qs_trader.strategies`` package is no longer auto-registered.  These
    tests pin the registration contract by:

    1. Exercising :class:`BacktestEngine` directly with
       ``custom_libraries.strategies = None`` to prove the engine instantiates
       first-party ``buy_and_hold`` without any user library.
    2. Running the full validation CLI end-to-end (no engine mock) and
       verifying ``benchmark/performance.json`` is written and
       ``summary.json`` carries a populated ``benchmark`` block with
       non-zero metrics.
    """

    def test_engine_resolves_builtin_buy_and_hold_without_custom_library(self, tmp_path: Path) -> None:
        from qs_trader.engine.engine import BacktestEngine

        config = BacktestConfig(
            backtest_id="builtin_benchmark_probe",
            start_date=datetime(2020, 8, 3),
            end_date=datetime(2020, 8, 14),
            initial_equity=Decimal("100000"),
            replay_speed=0.0,
            data=DataSelectionConfig(sources=[DataSourceConfig(name="yahoo-us-equity-1d-csv", universe=["AAPL"])]),
            strategies=[
                StrategyConfigItem(
                    strategy_id="buy_and_hold",
                    universe=["AAPL"],
                    data_sources=["yahoo-us-equity-1d-csv"],
                    config={},
                )
            ],
            risk_policy=RiskPolicyConfig(name="naive", config={}),
        )
        sys_cfg = _make_real_system_config(tmp_path, custom_strategies_dir=None)

        with patch("qs_trader.engine.engine.get_system_config", return_value=sys_cfg):
            with BacktestEngine.from_config(config, results_dir=tmp_path / "probe") as engine:
                result = engine.run()
                # Strategy service must have been built and seen bars.
                assert engine._strategy_service is not None
                metrics = engine._strategy_service.get_metrics()
                assert "buy_and_hold" in metrics
                assert metrics["buy_and_hold"]["bars_processed"] > 0
                assert metrics["buy_and_hold"]["signals_emitted"] == 1
                assert result.bars_processed > 0

    def test_validation_cli_benchmark_end_to_end_unmocked(self, tmp_path: Path) -> None:
        """Run the CLI without mocking the engine; verify benchmark block is
        populated end-to-end via first-party strategy registration."""
        from click.testing import CliRunner

        from qs_trader.validation.cli import validate_command

        # Short windows keep the test under a few seconds while still
        # producing non-zero metrics (the benchmark always buys at bar 0
        # and rides to the end).
        base_cfg_path = tmp_path / "base.yaml"
        base_cfg_path.write_text(
            "backtest_id: e2e_benchmark\n"
            'start_date: "2020-01-02"\n'
            'end_date: "2020-12-30"\n'
            "initial_equity: 100000\n"
            "data:\n"
            "  sources:\n"
            "    - name: yahoo-us-equity-1d-csv\n"
            "      universe:\n"
            "        - AAPL\n"
            "strategies:\n"
            "  - strategy_id: buy_and_hold\n"
            "    universe:\n"
            "      - AAPL\n"
            "    data_sources:\n"
            "      - yahoo-us-equity-1d-csv\n"
            "    config: {}\n"
            "risk_policy:\n"
            "  name: naive\n"
            "  config: {}\n"
            "reporting:\n"
            "  emit_metrics_events: false\n"
            "  write_json: true\n"
            "  write_parquet: false\n"
            "  write_csv_timeline: false\n"
            "  display_final_report: false\n"
        )
        plan_yaml = tmp_path / "plan.yaml"
        plan_yaml.write_text(
            yaml.safe_dump(
                {
                    "validation_id": "e2e_benchmark",
                    "strategy_experiment": "e2e_benchmark",
                    "base_config": str(base_cfg_path),
                    "mode": "static_is_oos",
                    "splits": {
                        "in_sample": {"start_date": "2020-01-02", "end_date": "2020-06-30"},
                        "out_of_sample": {"start_date": "2020-07-01", "end_date": "2020-12-30"},
                    },
                    "decision": {"rules": {"oos_sharpe_min": -10.0}},
                    "benchmark": {"instrument": "AAPL"},
                }
            )
        )

        # Empty custom-strategies dir → forces first-party registration to
        # supply ``buy_and_hold`` for both the strategy folds AND the
        # benchmark child.
        empty_custom = tmp_path / "empty_custom_strategies"
        empty_custom.mkdir()
        sys_cfg = _make_real_system_config(tmp_path, custom_strategies_dir=empty_custom)

        # Patch _default_benchmark_loader to a stub returning bars across the
        # full range (INFO #3 — adapter integration deferred to Phase 2A.5).
        # The benchmark CHILD run itself still uses the real engine + real
        # data adapter; only the pre-flight availability probe is stubbed.
        stub_bars = [date(2020, 1, 2), date(2020, 6, 30), date(2020, 12, 30)]

        with (
            patch("qs_trader.engine.engine.get_system_config", return_value=sys_cfg),
            patch(
                "qs_trader.validation.benchmark._default_benchmark_loader",
                return_value=stub_bars,
            ),
        ):
            result = CliRunner().invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        # Exit code may be 0 or 2 (Valid / Inconclusive) depending on
        # threshold; what matters is that no benchmark_run_failed reason
        # appears (i.e. the benchmark child reached success).
        out_dir = plan_yaml.parent / "e2e_benchmark"
        assert (out_dir / "summary.json").exists(), result.output
        summary = json.loads((out_dir / "summary.json").read_text())
        assert "benchmark_run_failed" not in summary.get("reason_codes", []), (
            f"benchmark child failed: reason_codes={summary.get('reason_codes')} output={result.output}"
        )
        assert "benchmark" in summary, f"summary missing benchmark block: {summary}"
        bench_block = summary["benchmark"]
        assert bench_block["instrument"] == "AAPL"
        metrics = bench_block["metrics"]
        # Non-zero metrics — buy-and-hold across 2020 AAPL must produce
        # observable return and finite Sharpe.  Reporting layer emits
        # ``*_pct`` strings; just confirm key fields are populated.
        assert metrics.get("total_return_pct") not in (None, "0", "0.0", "0.00")
        assert metrics.get("sharpe_ratio") is not None
        assert metrics.get("cagr") is not None
        # benchmark/performance.json was actually written by the real engine.
        assert (out_dir / "benchmark" / "performance.json").exists()
