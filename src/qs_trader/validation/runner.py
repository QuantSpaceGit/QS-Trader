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
from qs_trader.validation.plan import ValidationPlan, compute_plan_sha256
from qs_trader.validation.splits.base import ValidationSplit

if TYPE_CHECKING:
    from qs_trader.engine.config import BacktestConfig

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
        role: Split role (``'is'``, ``'oos'``, ``'holdout'``, ``'warmup_only'``).
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
    ) -> None:
        self._plan = plan
        self._splits = splits
        self._base_config = base_config
        self._validations_dir = validations_dir
        self._force = force

    def run(self) -> list[ChildRunRef]:
        """Execute all validation folds in order.

        Returns:
            List of :class:`ChildRunRef` objects, one per executed fold.

        Raises:
            FileExistsError: If ``validations_dir`` already exists and
                             ``force=False``.
            ChildRunFailedError: When ``plan.execution.on_child_failure == 'fail_fast'``
                and a fold fails.  The exception carries ``partial_refs`` with
                all :class:`ChildRunRef` objects collected up to and including
                the failed fold.
        """
        # Lazy import to avoid circular dependency with the engine package.
        from qs_trader.engine.engine import BacktestEngine  # noqa: PLC0415

        plan = self._plan
        splits = self._splits
        base_config = self._base_config
        validations_dir = self._validations_dir

        # --- Collision guard -------------------------------------------
        if validations_dir.exists() and not self._force:
            raise FileExistsError(
                f"Validation output directory already exists: {validations_dir}. Pass force=True to overwrite."
            )

        folds_dir = validations_dir / "folds"

        # --- Plan hash (preflight — must succeed before touching filesystem) --
        plan_sha256 = compute_plan_sha256(plan, plan.base_config)

        folds_dir.mkdir(parents=True, exist_ok=True)

        on_child_failure = plan.execution.on_child_failure

        refs: list[ChildRunRef] = []

        for split in splits:
            fold_id = f"f{split.fold_index}__{split.role}"
            run_id = f"val_{plan.validation_id}__f{split.fold_index}__{split.role}"
            fold_dir = folds_dir / fold_id
            fold_dir.mkdir(parents=True, exist_ok=True)

            child_config = derive_child_config(plan, split, base_config)
            child_config = child_config.model_copy(update={"run_id": run_id})

            validation_context = {
                "validation_id": plan.validation_id,
                "fold_id": fold_id,
                "split_role": split.role,
                "parent_plan_id": plan.validation_id,
                "parent_experiment_id": plan.strategy_experiment,
                "plan_sha256": plan_sha256,
            }

            started_at = datetime.now().isoformat()

            try:
                logger.info(
                    "validation.fold.starting",
                    fold_id=fold_id,
                    run_id=run_id,
                    role=split.role,
                )

                with BacktestEngine.from_config(child_config, results_dir=fold_dir) as engine:
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
                    )
                )

                if on_child_failure == "fail_fast":
                    raise ChildRunFailedError(refs, fold_id, exc) from exc

        return refs
