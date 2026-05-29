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

__all__ = ["SummaryWriter", "compute_strategy_minus_benchmark"]


# Metrics included in the ``strategy_minus_benchmark`` delta block.  Kept
# narrow and stable so the diff is meaningful; can be extended in Phase 2A.4
# once the walk-forward aggregator publishes a richer canonical metric set.
_BENCHMARK_DELTA_METRICS: tuple[str, ...] = ("sharpe_ratio", "total_return")


def compute_strategy_minus_benchmark(
    strategy_metrics: dict[str, Any],
    benchmark_metrics: dict[str, Any],
) -> dict[str, float]:
    """Return ``strategy[m] - benchmark[m]`` for the comparable metric subset.

    Only entries where both sides expose a numeric value participate; missing
    or non-numeric entries are silently skipped so a partial metric set does
    not poison the whole block.  The metric subset is :data:`_BENCHMARK_DELTA_METRICS`.

    TODO(Phase 2A.4): replace the static subset with the walk-forward
    aggregator's canonical metric catalog once it lands; under walk-forward
    plans the ``strategy_metrics`` argument should then be the aggregate
    (median Sharpe etc.) rather than a per-fold snapshot.
    """
    deltas: dict[str, float] = {}
    for metric in _BENCHMARK_DELTA_METRICS:
        s = strategy_metrics.get(metric)
        b = benchmark_metrics.get(metric)
        if isinstance(s, (int, float)) and isinstance(b, (int, float)):
            deltas[metric] = float(s) - float(b)
    return deltas


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
        scenario_summaries: list[dict[str, Any]] | None = None,
        benchmark_summary: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Write summary.json and return the summary dict.

        The ``is_val`` field on :class:`MetricComparison` is serialized as
        ``"is"`` in the JSON output per §4.3.

        When ``scenario_summaries`` is supplied (Phase 2A.2, only when
        ``plan.cost_scenarios`` is declared) a ``cost_scenarios`` block is
        emitted of shape::

            [{"name": str, "decision": "Pass|Fail|...", "reason_codes": [...],
              "folds": [{"fold_id": str, "role": str, "status": str}, ...]}]

        Per-scenario decisions are aggregated into the top-level ``outcome``
        and ``reason_codes`` by the CLI before this writer is called: the
        worst severity wins (Fail > ReviewRequired > Invalid > Pass) and a
        ``cost_scenario_failed:<name>`` reason code is appended for every
        non-Pass scenario.  The top-level ``comparison`` block remains
        anchored to the first declared scenario (typically ``base``) — it
        renders the IS/OOS metric table for that scenario only, not the
        overall decision.

        Note: per-scenario *aggregate metrics* (e.g. ``median_oos_sharpe``
        across folds) are still deferred to Phase 2A.4 alongside the
        walk-forward aggregator; only the decision/reason-code propagation
        landed in Phase 2A.2.
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
        # Phase 2A.2: cost-scenarios block only when declared. When None the
        # key is omitted entirely (no `null`) to preserve byte-identical
        # output for Phase 1 / Phase 2A.1 plans.
        if scenario_summaries is not None:
            summary["cost_scenarios"] = scenario_summaries
        # Phase 2A.3: benchmark block only when declared AND the benchmark
        # child run produced a summary payload. When the plan does not declare
        # ``benchmark`` (or the benchmark child failed and the CLI suppressed
        # the block) the key is omitted entirely — no `null`.
        if benchmark_summary is not None:
            summary["benchmark"] = benchmark_summary
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
        # Phase 2A.2: same convention for cost_scenarios. When the plan did not
        # declare scenarios the field is dropped from the effective plan so
        # Phase 1 / Phase 2A.1 artifacts remain byte-identical.
        if plan_dict.get("cost_scenarios") is None:
            plan_dict.pop("cost_scenarios", None)
        # Phase 2A.3: same convention for benchmark. When the plan did not
        # declare a benchmark the field is dropped from the effective plan
        # so Phase 1 / Phase 2A.1 / Phase 2A.2 artifacts remain byte-identical.
        if plan_dict.get("benchmark") is None:
            plan_dict.pop("benchmark", None)
        with effective_plan_path.open("w") as f:
            yaml.dump(plan_dict, f, default_flow_style=False, allow_unicode=True)
        logger.info("effective_plan_written", path=str(effective_plan_path))
