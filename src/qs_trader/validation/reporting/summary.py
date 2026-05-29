"""Summary JSON and effective plan YAML writers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog
import yaml

from qs_trader.validation.aggregation import MetricComparison
from qs_trader.validation.decision import RuleResult, ValidationDecision
from qs_trader.validation.plan import ValidationPlan
from qs_trader.validation.runner import ChildRunRef

logger = structlog.get_logger(__name__)

__all__ = ["SummaryWriter"]


def _serialize_comparison(comparison: dict[str, MetricComparison]) -> dict[str, dict[str, Any]]:
    """Serialize comparison dict, mapping ``is_val`` field to ``"is"`` JSON key."""
    result: dict[str, dict[str, Any]] = {}
    for metric, mc in comparison.items():
        result[metric] = {
            "is": mc.is_val,
            "oos": mc.oos,
            "full": mc.full,
            "decay": mc.decay,
        }
    return result


def _serialize_rule_result(rr: RuleResult) -> dict[str, Any]:
    return {
        "rule": rr.rule,
        "threshold": rr.threshold,
        "actual": rr.actual,
        "passed": rr.passed,
    }


def _serialize_decision(decision: ValidationDecision) -> dict[str, Any]:
    return {
        "outcome": decision.outcome,
        "reason_codes": decision.reason_codes,
        "rule_results": [_serialize_rule_result(rr) for rr in decision.rule_results],
    }


def _load_fold_metrics(run_dir: Path) -> dict[str, Any]:
    """Load performance.json from a fold run directory, returning {} on missing."""
    perf_path = run_dir / "performance.json"
    if perf_path.exists():
        try:
            with perf_path.open() as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
        except Exception:
            logger.warning("failed_to_load_performance_json", path=str(perf_path))
    return {}


def _fold_date_range(child_ref: ChildRunRef, plan: ValidationPlan) -> tuple[str | None, str | None]:
    """Return (start_date, end_date) ISO strings for a fold by role."""
    from qs_trader.validation.plan import StaticSplitSpec

    role = child_ref.role
    if role == "is":
        if isinstance(plan.splits, StaticSplitSpec):
            dr = plan.splits.in_sample
            return str(dr.start_date), str(dr.end_date)
        return None, None
    if role == "oos":
        if isinstance(plan.splits, StaticSplitSpec):
            dr = plan.splits.out_of_sample
            return str(dr.start_date), str(dr.end_date)
        return None, None
    return None, None


def _serialize_folds(child_refs: list[ChildRunRef], plan: ValidationPlan) -> list[dict[str, Any]]:
    """Build the folds array for summary.json."""
    folds: list[dict[str, Any]] = []
    for ref in child_refs:
        start_date, end_date = _fold_date_range(ref, plan)
        metrics = _load_fold_metrics(ref.run_dir) if ref.run_dir else {}
        folds.append(
            {
                "fold_id": ref.fold_id,
                "role": ref.role,
                "run_id": ref.run_id,
                "experiment_id": ref.experiment_id,
                "start_date": start_date,
                "end_date": end_date,
                "status": ref.status,
                "error": ref.error,
                "metrics": metrics,
            }
        )
    return folds


class SummaryWriter:
    """Writes ``summary.json`` and ``effective_plan.yaml`` to the validation output directory."""

    def write_summary(
        self,
        validation_id: str,
        plan: ValidationPlan,
        plan_sha256: str,
        base_config_sha256: str,
        outcome: str,
        reason_codes: list[str],
        folds: list[ChildRunRef],
        comparison: dict[str, MetricComparison],
        decision: ValidationDecision,
        audit: dict[str, Any],
        started_at: str,
        finished_at: str,
        out_dir: Path,
    ) -> dict[str, Any]:
        """Write summary.json and return the summary dict.

        The ``is_val`` field on :class:`MetricComparison` is serialized as
        ``"is"`` in the JSON output per §4.3.
        """
        summary: dict[str, Any] = {
            "validation_id": validation_id,
            "plan_sha256": plan_sha256,
            "base_config_sha256": base_config_sha256,
            "strategy_experiment": plan.strategy_experiment,
            "mode": plan.mode,
            "started_at": started_at,
            "finished_at": finished_at,
            "outcome": outcome,
            "reason_codes": reason_codes,
            "folds": _serialize_folds(folds, plan),
            "comparison": _serialize_comparison(comparison),
            "decision": _serialize_decision(decision),
            "audit": audit,
        }
        out_dir.mkdir(parents=True, exist_ok=True)
        summary_path = out_dir / "summary.json"
        with summary_path.open("w") as f:
            json.dump(summary, f, indent=2, default=str)
        logger.info("summary_written", path=str(summary_path))
        return summary

    def write_effective_plan(self, plan: ValidationPlan, out_dir: Path) -> None:
        """Serialize ``plan.model_dump(mode='json')`` to ``out_dir/effective_plan.yaml``."""
        out_dir.mkdir(parents=True, exist_ok=True)
        effective_plan_path = out_dir / "effective_plan.yaml"
        plan_dict = plan.model_dump(mode="json")
        # Convert Path objects to strings for YAML serialization
        if "base_config" in plan_dict:
            plan_dict["base_config"] = str(plan_dict["base_config"])
        # description is non-execution metadata; omit when None to preserve
        # byte-identical effective_plan.yaml for Phase 1 static_is_oos plans.
        if plan_dict.get("description") is None:
            plan_dict.pop("description", None)
        with effective_plan_path.open("w") as f:
            yaml.dump(plan_dict, f, default_flow_style=False, allow_unicode=True)
        logger.info("effective_plan_written", path=str(effective_plan_path))
