"""Unit and integration tests for qs_trader.validation.runner module.

Covers T4.1–T4.4 of the OOS Validation Framework Phase 1.2:
- T4.1  Deterministic fold output (same bars_processed across runs)
- T4.2  fold/manifest artifact layout
- T4.3  on_child_failure modes (fail_fast / continue)
- T4.4  Collision guard (FileExistsError when dir exists and force=False)
"""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError
from datetime import date
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

from qs_trader.engine.config import (
    BacktestConfig,
    DataSelectionConfig,
    DataSourceConfig,
    RiskPolicyConfig,
    StrategyConfigItem,
)
from qs_trader.validation.plan import DateRange, ExecutionSpec, StaticSplitSpec, ValidationPlan, load_validation_plan
from qs_trader.validation.runner import ChildRunFailedError, ChildRunRef, SequentialValidationRunner
from qs_trader.validation.splits.base import ValidationSplit
from qs_trader.validation.splits.static import StaticSplitGenerator

FIXTURES_DIR = Path(__file__).parent / "fixtures"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_system_config(tmp_path: Path) -> Mock:
    """Build a fully-configured mock system config for integration tests."""
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
    mock_config.custom_libraries.strategies = "tests/fixtures/strategies"

    mock_config.logging = Mock()
    mock_logger_config = Mock()
    mock_logger_config.level = "WARNING"
    mock_logger_config.format = "json"
    mock_logger_config.timestamp_format = "compact"
    mock_logger_config.enable_file = False
    mock_logger_config.file_path = None
    mock_logger_config.file_level = "WARNING"
    mock_logger_config.file_rotation = False
    mock_logger_config.max_file_size_mb = 10
    mock_logger_config.backup_count = 3
    mock_logger_config.console_width = 0
    mock_config.logging.to_logger_config = Mock(return_value=mock_logger_config)

    return mock_config


def _load_runner_plan() -> ValidationPlan:
    """Load the runner_test_plan fixture."""
    return load_validation_plan(FIXTURES_DIR / "runner_test_plan.yaml")


def _make_aapl_config(start: date, end: date) -> BacktestConfig:
    """Build a minimal AAPL-only BacktestConfig."""
    from datetime import datetime, time

    return BacktestConfig(
        backtest_id="test_strategy",
        start_date=datetime.combine(start, time.min),
        end_date=datetime.combine(end, time.min),
        initial_equity=Decimal("100000"),
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


def _make_mock_engine(bars: int = 100) -> MagicMock:
    """Return a mock engine context manager whose run() succeeds."""
    from datetime import timedelta as td

    mock_result = Mock()
    mock_result.bars_processed = bars
    mock_result.duration = td(seconds=1)

    mock_engine = MagicMock()
    mock_engine.run.return_value = mock_result
    mock_engine.__enter__ = Mock(return_value=mock_engine)
    mock_engine.__exit__ = Mock(return_value=False)
    return mock_engine


def _make_failing_mock_engine() -> MagicMock:
    """Return a mock engine context manager whose run() raises RuntimeError."""
    mock_engine = MagicMock()
    mock_engine.run.side_effect = RuntimeError("simulated fold failure")
    mock_engine.__enter__ = Mock(return_value=mock_engine)
    mock_engine.__exit__ = Mock(return_value=False)
    return mock_engine


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def runner_plan() -> ValidationPlan:
    return _load_runner_plan()


@pytest.fixture()
def runner_splits(runner_plan: ValidationPlan) -> list[ValidationSplit]:
    return StaticSplitGenerator().generate(runner_plan)


@pytest.fixture()
def runner_base_config() -> BacktestConfig:
    from qs_trader.engine.config import load_backtest_config

    return load_backtest_config(FIXTURES_DIR / "runner_base_config.yaml")


# ---------------------------------------------------------------------------
# TestChildRunRef — unit tests (no engine)
# ---------------------------------------------------------------------------


class TestChildRunRef:
    """Unit tests for ChildRunRef dataclass naming and immutability."""

    def test_fold_id_naming(self, tmp_path: Path) -> None:
        """fold_id should follow the 'f{index}__{role}' pattern."""
        ref = ChildRunRef(
            fold_id="f0__is",
            run_id="val_mypln__f0__is",
            experiment_id="test_strategy",
            role="is",
            run_dir=tmp_path,
            status="success",
            error=None,
        )
        assert ref.fold_id == "f0__is"

        ref2 = ChildRunRef(
            fold_id="f1__oos",
            run_id="val_mypln__f1__oos",
            experiment_id="test_strategy",
            role="oos",
            run_dir=tmp_path,
            status="success",
            error=None,
        )
        assert ref2.fold_id == "f1__oos"

    def test_run_id_naming(self, tmp_path: Path) -> None:
        """run_id should follow the 'val_{vid}__f{index}__{role}' pattern."""
        ref = ChildRunRef(
            fold_id="f0__is",
            run_id="val_my_plan__f0__is",
            experiment_id="test_strategy",
            role="is",
            run_dir=tmp_path,
            status="success",
            error=None,
        )
        assert ref.run_id == "val_my_plan__f0__is"
        assert ref.run_id.startswith("val_")
        assert "__f0__is" in ref.run_id

    def test_is_frozen_dataclass(self, tmp_path: Path) -> None:
        """ChildRunRef must be immutable (frozen dataclass)."""
        ref = ChildRunRef(
            fold_id="f0__is",
            run_id="val_mypln__f0__is",
            experiment_id="test_strategy",
            role="is",
            run_dir=tmp_path,
            status="success",
            error=None,
        )
        with pytest.raises(FrozenInstanceError):
            ref.status = "failed"  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TestSequentialValidationRunner — integration tests (real engine)
# ---------------------------------------------------------------------------


class TestSequentialValidationRunner:
    """Integration tests that run the real BacktestEngine with AAPL CSV data."""

    def test_successful_run_creates_fold_dirs(
        self,
        runner_plan: ValidationPlan,
        runner_splits: list[ValidationSplit],
        runner_base_config: BacktestConfig,
        tmp_path: Path,
    ) -> None:
        """Both fold directories must exist after a successful run."""
        validations_dir = tmp_path / "validations" / "runner_test_plan"
        mock_cfg = _make_mock_system_config(tmp_path)

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_cfg):
            runner = SequentialValidationRunner(
                plan=runner_plan,
                splits=runner_splits,
                base_config=runner_base_config,
                validations_dir=validations_dir,
            )
            runner.run()

        assert (validations_dir / "folds" / "f0__is").is_dir()
        assert (validations_dir / "folds" / "f1__oos").is_dir()

    def test_successful_run_returns_child_refs(
        self,
        runner_plan: ValidationPlan,
        runner_splits: list[ValidationSplit],
        runner_base_config: BacktestConfig,
        tmp_path: Path,
    ) -> None:
        """run() must return 2 ChildRunRefs, both with status='success'."""
        validations_dir = tmp_path / "validations" / "runner_test_plan"
        mock_cfg = _make_mock_system_config(tmp_path)

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_cfg):
            runner = SequentialValidationRunner(
                plan=runner_plan,
                splits=runner_splits,
                base_config=runner_base_config,
                validations_dir=validations_dir,
            )
            refs = runner.run()

        assert len(refs) == 2
        assert refs[0].status == "success"
        assert refs[0].fold_id == "f0__is"
        assert refs[0].role == "is"
        assert refs[1].status == "success"
        assert refs[1].fold_id == "f1__oos"
        assert refs[1].role == "oos"

    def test_manifest_written_with_validation_context(
        self,
        runner_plan: ValidationPlan,
        runner_splits: list[ValidationSplit],
        runner_base_config: BacktestConfig,
        tmp_path: Path,
    ) -> None:
        """manifest.json for fold 0 must contain correct validation_context fields."""
        validations_dir = tmp_path / "validations" / "runner_test_plan"
        mock_cfg = _make_mock_system_config(tmp_path)

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_cfg):
            runner = SequentialValidationRunner(
                plan=runner_plan,
                splits=runner_splits,
                base_config=runner_base_config,
                validations_dir=validations_dir,
            )
            runner.run()

        manifest_path = validations_dir / "folds" / "f0__is" / "manifest.json"
        assert manifest_path.exists()

        manifest = json.loads(manifest_path.read_text())
        ctx = manifest["metrics"]["validation_context"]

        assert ctx["fold_id"] == "f0__is"
        assert ctx["split_role"] == "is"
        assert ctx["validation_id"] == "runner_test_plan"
        assert ctx["parent_plan_id"] == "runner_test_plan"
        assert ctx["parent_experiment_id"] == "test_strategy"
        assert isinstance(ctx["plan_sha256"], str)
        assert manifest["status"] == "success"

    def test_run_id_in_manifest(
        self,
        runner_plan: ValidationPlan,
        runner_splits: list[ValidationSplit],
        runner_base_config: BacktestConfig,
        tmp_path: Path,
    ) -> None:
        """manifest.json run_id must start with 'val_runner_test_plan__f'."""
        validations_dir = tmp_path / "validations" / "runner_test_plan"
        mock_cfg = _make_mock_system_config(tmp_path)

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_cfg):
            runner = SequentialValidationRunner(
                plan=runner_plan,
                splits=runner_splits,
                base_config=runner_base_config,
                validations_dir=validations_dir,
            )
            runner.run()

        manifest_path = validations_dir / "folds" / "f0__is" / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        assert manifest["run_id"].startswith("val_runner_test_plan__f")

    def test_determinism(
        self,
        runner_plan: ValidationPlan,
        runner_splits: list[ValidationSplit],
        runner_base_config: BacktestConfig,
        tmp_path: Path,
    ) -> None:
        """Running the same IS fold twice must produce identical bars_processed (T4.1)."""
        mock_cfg = _make_mock_system_config(tmp_path)

        validations_dir_a = tmp_path / "run_a" / "runner_test_plan"
        validations_dir_b = tmp_path / "run_b" / "runner_test_plan"

        # Use only IS fold to keep the test fast
        is_splits = [s for s in runner_splits if s.role == "is"]

        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_cfg):
            refs_a = SequentialValidationRunner(
                plan=runner_plan,
                splits=is_splits,
                base_config=runner_base_config,
                validations_dir=validations_dir_a,
            ).run()

        mock_cfg_b = _make_mock_system_config(tmp_path / "b")
        with patch("qs_trader.engine.engine.get_system_config", return_value=mock_cfg_b):
            refs_b = SequentialValidationRunner(
                plan=runner_plan,
                splits=is_splits,
                base_config=runner_base_config,
                validations_dir=validations_dir_b,
            ).run()

        assert refs_a[0].status == "success"
        assert refs_b[0].status == "success"

        manifest_a = json.loads((validations_dir_a / "folds" / "f0__is" / "manifest.json").read_text())
        manifest_b = json.loads((validations_dir_b / "folds" / "f0__is" / "manifest.json").read_text())
        assert manifest_a["metrics"]["bars_processed"] == manifest_b["metrics"]["bars_processed"]


# ---------------------------------------------------------------------------
# TestOnChildFailure — unit tests (mocked engine)
# ---------------------------------------------------------------------------


class TestOnChildFailure:
    """Tests for on_child_failure=fail_fast and =continue behaviour."""

    def _make_plan_with_failure_mode(self, mode: str) -> ValidationPlan:
        return ValidationPlan(
            validation_id="fail_test",
            strategy_experiment="test_strategy",
            base_config=FIXTURES_DIR / "runner_base_config.yaml",
            mode="static_is_oos",
            splits=StaticSplitSpec(
                in_sample=DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 6, 30)),
                out_of_sample=DateRange(start_date=date(2020, 7, 1), end_date=date(2020, 12, 30)),
            ),
            execution=ExecutionSpec(on_child_failure=mode),  # type: ignore[arg-type]
        )

    def _make_splits(self) -> list[ValidationSplit]:
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

    def _make_base_config(self) -> BacktestConfig:
        from datetime import datetime

        return BacktestConfig(
            backtest_id="fail_test",
            start_date=datetime(2020, 1, 2),
            end_date=datetime(2020, 6, 30),
            initial_equity=Decimal("100000"),
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

    def test_fail_fast_propagates_exception(self, tmp_path: Path) -> None:
        """fail_fast mode must raise ChildRunFailedError carrying the original cause."""
        plan = self._make_plan_with_failure_mode("fail_fast")
        splits = self._make_splits()
        base_config = self._make_base_config()
        validations_dir = tmp_path / "validations" / "fail_fast"

        failing_engine = _make_failing_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = failing_engine
            runner = SequentialValidationRunner(
                plan=plan,
                splits=splits,
                base_config=base_config,
                validations_dir=validations_dir,
            )
            with pytest.raises(ChildRunFailedError) as exc_info:
                runner.run()
        # The original RuntimeError must be carried as .cause
        assert isinstance(exc_info.value.cause, RuntimeError)
        assert "simulated fold failure" in str(exc_info.value.cause)

    def test_fail_fast_stops_on_first_failure(self, tmp_path: Path) -> None:
        """fail_fast must stop after the first failure — only one fold dir created."""
        plan = self._make_plan_with_failure_mode("fail_fast")
        splits = self._make_splits()
        base_config = self._make_base_config()
        validations_dir = tmp_path / "validations" / "fail_fast_stop"

        failing_engine = _make_failing_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = failing_engine
            runner = SequentialValidationRunner(
                plan=plan,
                splits=splits,
                base_config=base_config,
                validations_dir=validations_dir,
            )
            with pytest.raises(ChildRunFailedError):
                runner.run()

        folds_dir = validations_dir / "folds"
        # Only first fold should have been created
        assert (folds_dir / "f0__is").is_dir()
        assert not (folds_dir / "f1__oos").is_dir()

    def test_fail_fast_manifest_written_with_failed_status(self, tmp_path: Path) -> None:
        """fail_fast must write manifest.json with status=failed before raising."""
        plan = self._make_plan_with_failure_mode("fail_fast")
        splits = self._make_splits()
        base_config = self._make_base_config()
        validations_dir = tmp_path / "validations" / "fail_fast_manifest"

        failing_engine = _make_failing_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = failing_engine
            runner = SequentialValidationRunner(
                plan=plan,
                splits=splits,
                base_config=base_config,
                validations_dir=validations_dir,
            )
            with pytest.raises(ChildRunFailedError):
                runner.run()

        manifest_path = validations_dir / "folds" / "f0__is" / "manifest.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        assert manifest["status"] == "failed"
        assert manifest["error"] is not None

    def test_continue_mode_runs_all_folds(self, tmp_path: Path) -> None:
        """continue mode must execute all folds even when each fails."""
        plan = self._make_plan_with_failure_mode("continue")
        splits = self._make_splits()
        base_config = self._make_base_config()
        validations_dir = tmp_path / "validations" / "continue_mode"

        failing_engine = _make_failing_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = failing_engine
            runner = SequentialValidationRunner(
                plan=plan,
                splits=splits,
                base_config=base_config,
                validations_dir=validations_dir,
            )
            refs = runner.run()

        assert len(refs) == 2
        assert refs[0].status == "failed"
        assert refs[1].status == "failed"

    def test_continue_mode_failed_manifest_written(self, tmp_path: Path) -> None:
        """continue mode must write manifest.json with status=failed and error set."""
        plan = self._make_plan_with_failure_mode("continue")
        splits = self._make_splits()
        base_config = self._make_base_config()
        validations_dir = tmp_path / "validations" / "continue_manifest"

        failing_engine = _make_failing_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = failing_engine
            runner = SequentialValidationRunner(
                plan=plan,
                splits=splits,
                base_config=base_config,
                validations_dir=validations_dir,
            )
            runner.run()

        manifest_path = validations_dir / "folds" / "f0__is" / "manifest.json"
        assert manifest_path.exists()
        manifest = json.loads(manifest_path.read_text())
        assert manifest["status"] == "failed"
        assert manifest["error"] is not None

    def test_engine_receives_planned_run_id(self, tmp_path: Path) -> None:
        """BacktestEngine.from_config must receive child_config with run_id equal to the
        planned val_<vid>__f<n>__<role> identifier for every fold.
        """
        plan = self._make_plan_with_failure_mode("fail_fast")
        splits = self._make_splits()
        base_config = self._make_base_config()
        validations_dir = tmp_path / "validations" / "run_id_contract"

        captured_run_ids: list[str | None] = []

        def fake_from_config(config: BacktestConfig, **kwargs: object) -> MagicMock:
            captured_run_ids.append(config.run_id)
            return _make_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.side_effect = fake_from_config
            runner = SequentialValidationRunner(
                plan=plan,
                splits=splits,
                base_config=base_config,
                validations_dir=validations_dir,
            )
            runner.run()

        assert len(captured_run_ids) == 2
        assert captured_run_ids[0] == "val_fail_test__f0__is"
        assert captured_run_ids[1] == "val_fail_test__f1__oos"


# ---------------------------------------------------------------------------
# TestCollisionGuard — unit tests (no engine needed)
# ---------------------------------------------------------------------------


class TestCollisionGuard:
    """Tests for the validations_dir collision guard."""

    def _make_minimal_plan(self) -> ValidationPlan:
        return ValidationPlan(
            validation_id="guard_test",
            strategy_experiment="test_strategy",
            base_config=FIXTURES_DIR / "runner_base_config.yaml",
            mode="static_is_oos",
            splits=StaticSplitSpec(
                in_sample=DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 6, 30)),
                out_of_sample=DateRange(start_date=date(2020, 7, 1), end_date=date(2020, 12, 30)),
            ),
        )

    def _make_splits(self) -> list[ValidationSplit]:
        return [
            ValidationSplit(
                fold_index=0,
                role="is",
                test_range=DateRange(start_date=date(2020, 1, 2), end_date=date(2020, 6, 30)),
            ),
        ]

    def _make_base_config(self) -> BacktestConfig:
        from datetime import datetime

        return BacktestConfig(
            backtest_id="guard_test",
            start_date=datetime(2020, 1, 2),
            end_date=datetime(2020, 6, 30),
            initial_equity=Decimal("100000"),
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

    def test_collision_raises_if_dir_exists(self, tmp_path: Path) -> None:
        """FileExistsError must be raised when validations_dir exists and force=False."""
        plan = self._make_minimal_plan()
        splits = self._make_splits()
        base_config = self._make_base_config()
        validations_dir = tmp_path / "validations" / "guard_test"
        validations_dir.mkdir(parents=True)

        runner = SequentialValidationRunner(
            plan=plan,
            splits=splits,
            base_config=base_config,
            validations_dir=validations_dir,
            force=False,
        )
        with pytest.raises(FileExistsError, match="Pass force=True"):
            runner.run()

    def test_force_flag_allows_overwrite(self, tmp_path: Path) -> None:
        """force=True must allow run() to proceed when validations_dir already exists."""
        plan = self._make_minimal_plan()
        splits = self._make_splits()
        base_config = self._make_base_config()
        validations_dir = tmp_path / "validations" / "guard_force"
        validations_dir.mkdir(parents=True)

        mock_engine = _make_mock_engine()

        with patch("qs_trader.engine.engine.BacktestEngine") as MockEngine:
            MockEngine.from_config.return_value = mock_engine
            runner = SequentialValidationRunner(
                plan=plan,
                splits=splits,
                base_config=base_config,
                validations_dir=validations_dir,
                force=True,
            )
            refs = runner.run()

        assert len(refs) == 1
        assert refs[0].status == "success"
