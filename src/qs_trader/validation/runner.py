"""Sequential validation runner for QS-Trader OOS validation framework.

Executes a list of :class:`~qs_trader.validation.splits.base.ValidationSplit` objects
in order, writing per-fold artifact directories and ``manifest.json`` run-metadata
files.  Supports both ``fail_fast`` and ``continue`` on-child-failure modes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

from qs_trader.engine.experiment import ExperimentMetadata, RunMetadata
from qs_trader.validation.child_config import derive_child_config
from qs_trader.validation.cost_scenarios import apply_scenario_overrides
from qs_trader.validation.plan import ValidationPlan, compute_plan_sha256
from qs_trader.validation.splits.base import ValidationSplit

if TYPE_CHECKING:
    from qs_trader.engine.config import BacktestConfig
    from qs_trader.system.config import SystemConfig

logger = structlog.get_logger(__name__)


class ChildRunFailedError(Exception):
    """Raised by :class:`SequentialValidationRunner` on fail-fast child failure.

    Attributes:
        partial_refs: All :class:`ChildRunRef` objects collected before the
            failure, including the failed fold itself.
        fold_id: The fold identifier that triggered the failure.
        cause: The original exception that caused the fold to fail.
    """

    def __init__(self, partial_refs: list["ChildRunRef"], fold_id: str, cause: BaseException) -> None:
        super().__init__(f"Fold {fold_id!r} failed: {cause}")
        self.partial_refs = partial_refs
        self.fold_id = fold_id
        self.cause = cause


@dataclass(frozen=True)
class ChildRunRef:
    """Reference to the output of a single fold execution.

    Attributes:
        fold_id: Fold identifier string, e.g. ``'f0__is'``, ``'f1__oos'``.
        run_id: Unique run ID, e.g. ``'val_my_plan__f0__is'``.
        experiment_id: Strategy experiment ID from the validation plan.
        role: Split role (``'is'``, ``'oos'``, ``'holdout'``, ``'warmup_only'``)
              or ``'benchmark'`` for the Phase 2A.3 single benchmark child run.
        run_dir: Absolute path to the fold's artifact directory.
        status: Execution outcome, either ``'success'`` or ``'failed'``.
        error: Error message string on failure, or ``None`` on success.
    """

    fold_id: str
    run_id: str
    experiment_id: str
    role: str
    run_dir: Path
    status: str
    error: str | None
    scenario: str | None = None


class SequentialValidationRunner:
    """Runs validation folds sequentially, writing manifests for each fold.

    Args:
        plan: Validated :class:`~qs_trader.validation.plan.ValidationPlan`.
        splits: Ordered list of
                :class:`~qs_trader.validation.splits.base.ValidationSplit` objects
                to execute.
        base_config: Base :class:`~qs_trader.engine.config.BacktestConfig` from
                     which child configs are derived.
        validations_dir: Root output directory for this validation run
                         (``<experiment>/validations/<validation_id>/``).
        force: If ``True``, allow ``validations_dir`` to already exist.
               Defaults to ``False``.
    """

    def __init__(
        self,
        plan: ValidationPlan,
        splits: list[ValidationSplit],
        base_config: "BacktestConfig",
        validations_dir: Path,
        force: bool = False,
        system_config: "SystemConfig | None" = None,
    ) -> None:
        self._plan = plan
        self._splits = splits
        self._base_config = base_config
        self._validations_dir = validations_dir
        self._force = force
        self._system_config = system_config

    def run(self) -> list[ChildRunRef]:
        """Execute all validation folds in order.

        When ``plan.cost_scenarios`` is ``None`` the runner emits folds under
        ``validations_dir/folds/<fold_id>/`` for byte-identical behaviour with
        Phase 1 / Phase 2A.1.  When ``cost_scenarios`` is declared the
        (fold × scenario) matrix is iterated in scenario-major order and each
        fold lands under ``validations_dir/scenarios/<name>/folds/<fold_id>/``.
        ``fail_fast`` / ``continue`` semantics are preserved across the matrix;
        a failure within one scenario does not abort subsequent scenarios under
        ``continue``.

        Returns:
            List of :class:`ChildRunRef` objects, one per executed fold.  When
            scenarios are declared each ref carries the ``scenario`` name.

        Raises:
            FileExistsError: If ``validations_dir`` already exists and
                             ``force=False``.
            ChildRunFailedError: When ``plan.execution.on_child_failure == 'fail_fast'``
                and any fold (in any scenario) fails.  The exception carries
                ``partial_refs`` with all :class:`ChildRunRef` objects collected
                up to and including the failed fold across the entire matrix.
        """
        plan = self._plan
        validations_dir = self._validations_dir

        # --- Collision guard -------------------------------------------
        if validations_dir.exists() and not self._force:
            raise FileExistsError(
                f"Validation output directory already exists: {validations_dir}. Pass force=True to overwrite."
            )

        # --- Plan hash (preflight — must succeed before touching filesystem) --
        plan_sha256 = compute_plan_sha256(plan, plan.base_config)

        on_child_failure = plan.execution.on_child_failure
        scenarios = plan.cost_scenarios

        # --- Legacy (no cost_scenarios) path: identical to Phase 1/2A.1 ---
        if scenarios is None:
            folds_dir = validations_dir / "folds"
            folds_dir.mkdir(parents=True, exist_ok=True)
            return self._run_scenario(
                scenario_name=None,
                scenario_base_config=self._base_config,
                folds_dir=folds_dir,
                plan_sha256=plan_sha256,
                on_child_failure=on_child_failure,
                accumulated=[],
            )

        # --- (fold × scenario) matrix path ---
        all_refs: list[ChildRunRef] = []
        for scenario in scenarios:
            scenario_base = apply_scenario_overrides(self._base_config, scenario.overrides)
            scenario_folds_dir = validations_dir / "scenarios" / scenario.name / "folds"
            scenario_folds_dir.mkdir(parents=True, exist_ok=True)
            all_refs = self._run_scenario(
                scenario_name=scenario.name,
                scenario_base_config=scenario_base,
                folds_dir=scenario_folds_dir,
                plan_sha256=plan_sha256,
                on_child_failure=on_child_failure,
                accumulated=all_refs,
            )
        return all_refs

    def _run_scenario(
        self,
        *,
        scenario_name: str | None,
        scenario_base_config: "BacktestConfig",
        folds_dir: Path,
        plan_sha256: str,
        on_child_failure: str,
        accumulated: list[ChildRunRef],
    ) -> list[ChildRunRef]:
        """Execute the split list for a single scenario, returning the running ref list.

        Appends to ``accumulated`` and returns the same list so callers can
        chain across scenarios.  On ``fail_fast`` a :class:`ChildRunFailedError`
        is raised carrying every ref collected across all scenarios.
        """
        # Lazy import to avoid circular dependency with the engine package.
        from qs_trader.engine.engine import BacktestEngine  # noqa: PLC0415

        plan = self._plan
        splits = self._splits
        refs: list[ChildRunRef] = accumulated

        for split in splits:
            fold_id = f"f{split.fold_index}__{split.role}"
            scenario_tag = f"__{scenario_name}" if scenario_name else ""
            run_id = f"val_{plan.validation_id}{scenario_tag}__f{split.fold_index}__{split.role}"
            fold_dir = folds_dir / fold_id
            fold_dir.mkdir(parents=True, exist_ok=True)

            child_config = derive_child_config(plan, split, scenario_base_config)
            child_config = child_config.model_copy(update={"run_id": run_id})

            validation_context = {
                "validation_id": plan.validation_id,
                "fold_id": fold_id,
                "split_role": split.role,
                "parent_plan_id": plan.validation_id,
                "parent_experiment_id": plan.strategy_experiment,
                "plan_sha256": plan_sha256,
            }
            if scenario_name is not None:
                validation_context["scenario"] = scenario_name

            started_at = datetime.now().isoformat()

            try:
                logger.info(
                    "validation.fold.starting",
                    fold_id=fold_id,
                    run_id=run_id,
                    role=split.role,
                    scenario=scenario_name,
                )

                with BacktestEngine.from_config(child_config, results_dir=fold_dir, system_config=self._system_config) as engine:
                    result = engine.run()

                finished_at = datetime.now().isoformat()

                run_metadata = RunMetadata(
                    experiment_id=plan.strategy_experiment,
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="success",
                    metrics={
                        "bars_processed": result.bars_processed,
                        "duration_seconds": result.duration.total_seconds(),
                        "validation_context": validation_context,
                    },
                )
                ExperimentMetadata.write_run_metadata(fold_dir, run_metadata)

                logger.info(
                    "validation.fold.succeeded",
                    fold_id=fold_id,
                    run_id=run_id,
                    scenario=scenario_name,
                )

                refs.append(
                    ChildRunRef(
                        fold_id=fold_id,
                        run_id=run_id,
                        experiment_id=plan.strategy_experiment,
                        role=split.role,
                        run_dir=fold_dir,
                        status="success",
                        error=None,
                        scenario=scenario_name,
                    )
                )

            except Exception as exc:
                finished_at = datetime.now().isoformat()

                run_metadata = RunMetadata(
                    experiment_id=plan.strategy_experiment,
                    run_id=run_id,
                    started_at=started_at,
                    finished_at=finished_at,
                    status="failed",
                    error=str(exc),
                    metrics={"validation_context": validation_context},
                )
                ExperimentMetadata.write_run_metadata(fold_dir, run_metadata)

                logger.error(
                    "validation.fold.failed",
                    fold_id=fold_id,
                    run_id=run_id,
                    scenario=scenario_name,
                    error=str(exc),
                )

                refs.append(
                    ChildRunRef(
                        fold_id=fold_id,
                        run_id=run_id,
                        experiment_id=plan.strategy_experiment,
                        role=split.role,
                        run_dir=fold_dir,
                        status="failed",
                        error=str(exc),
                        scenario=scenario_name,
                    )
                )

                if on_child_failure == "fail_fast":
                    raise ChildRunFailedError(refs, fold_id, exc) from exc

        return refs

    def run_benchmark(self) -> ChildRunRef:
        """Execute the synthetic benchmark child run for the plan.

        Returns a :class:`ChildRunRef` describing the benchmark child run.
        Writes artifacts under ``validations_dir/benchmark/``.  Caller must
        only invoke this method when ``self._plan.benchmark`` is declared and
        after :meth:`run` has completed (or been skipped by the CLI).

        The benchmark run uses the same engine machinery as fold runs.  On
        failure the returned :class:`ChildRunRef` has ``status='failed'`` and
        ``error`` populated; this method never raises
        :class:`ChildRunFailedError` because the CLI applies its own
        benchmark-specific reason-code mapping
        (``benchmark_run_failed``).
        """
        from qs_trader.engine.engine import BacktestEngine  # noqa: PLC0415
        from qs_trader.validation.benchmark import (  # noqa: PLC0415
            BENCHMARK_FOLD_ID,
            BENCHMARK_ROLE,
            benchmark_full_range,
            derive_benchmark_child_config,
        )

        plan = self._plan
        if plan.benchmark is None:
            raise ValueError("run_benchmark requires plan.benchmark to be declared")

        full_range = benchmark_full_range(plan)
        bench_dir = self._validations_dir / BENCHMARK_FOLD_ID
        bench_dir.mkdir(parents=True, exist_ok=True)

        run_id = f"val_{plan.validation_id}__{BENCHMARK_FOLD_ID}"
        child_config = derive_benchmark_child_config(plan, full_range, self._base_config)
        child_config = child_config.model_copy(update={"run_id": run_id})
        plan_sha256 = compute_plan_sha256(plan, plan.base_config)

        validation_context = {
            "validation_id": plan.validation_id,
            "fold_id": BENCHMARK_FOLD_ID,
            "split_role": BENCHMARK_ROLE,
            "parent_plan_id": plan.validation_id,
            "parent_experiment_id": plan.strategy_experiment,
            "plan_sha256": plan_sha256,
            "benchmark_instrument": plan.benchmark.instrument,
        }
        started_at = datetime.now().isoformat()

        try:
            logger.info(
                "validation.benchmark.starting",
                run_id=run_id,
                instrument=plan.benchmark.instrument,
            )
            with BacktestEngine.from_config(child_config, results_dir=bench_dir, system_config=self._system_config) as engine:
                result = engine.run()

            finished_at = datetime.now().isoformat()
            run_metadata = RunMetadata(
                experiment_id=plan.strategy_experiment,
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                status="success",
                metrics={
                    "bars_processed": result.bars_processed,
                    "duration_seconds": result.duration.total_seconds(),
                    "validation_context": validation_context,
                },
            )
            ExperimentMetadata.write_run_metadata(bench_dir, run_metadata)
            logger.info("validation.benchmark.succeeded", run_id=run_id)
            return ChildRunRef(
                fold_id=BENCHMARK_FOLD_ID,
                run_id=run_id,
                experiment_id=plan.strategy_experiment,
                role=BENCHMARK_ROLE,
                run_dir=bench_dir,
                status="success",
                error=None,
                scenario=None,
            )
        except Exception as exc:
            finished_at = datetime.now().isoformat()
            run_metadata = RunMetadata(
                experiment_id=plan.strategy_experiment,
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                status="failed",
                error=str(exc),
                metrics={"validation_context": validation_context},
            )
            ExperimentMetadata.write_run_metadata(bench_dir, run_metadata)
            logger.error("validation.benchmark.failed", run_id=run_id, error=str(exc))
            return ChildRunRef(
                fold_id=BENCHMARK_FOLD_ID,
                run_id=run_id,
                experiment_id=plan.strategy_experiment,
                role=BENCHMARK_ROLE,
                run_dir=bench_dir,
                status="failed",
                error=str(exc),
                scenario=None,
            )
