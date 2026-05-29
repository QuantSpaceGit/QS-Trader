"""``qs-trader validate`` CLI command."""

from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import click
import structlog

from qs_trader.engine.config import load_backtest_config
from qs_trader.validation.aggregation import MetricsAggregator
from qs_trader.validation.audit import AuditWriter
from qs_trader.validation.decision import DecisionEngine
from qs_trader.validation.plan import compute_plan_sha256, load_validation_plan
from qs_trader.validation.reporting import SummaryWriter, ValidationHTMLReporter
from qs_trader.validation.runner import ChildRunFailedError, SequentialValidationRunner
from qs_trader.validation.splits import get_split_generator

logger = structlog.get_logger(__name__)

_OUTCOME_EXIT_CODES: dict[str, int] = {
    "Pass": 0,
    "Fail": 1,
    "ReviewRequired": 2,
    "Invalid": 3,
}


# Severity ordering for cross-scenario aggregation (Phase 2A.2 §4).
# Higher value = more severe.  Matches the prose contract in
# ``docs/validation-framework.md``: Fail > ReviewRequired > Invalid > Pass.
_SCENARIO_OUTCOME_SEVERITY: dict[str, int] = {
    "Pass": 0,
    "Invalid": 1,
    "ReviewRequired": 2,
    "Fail": 3,
}


def _aggregate_scenario_outcomes(outcomes: list[str]) -> str:
    """Return the worst outcome across cost-scenario decisions.

    The ordering Fail > ReviewRequired > Invalid > Pass mirrors the §4
    cross-scenario aggregation rule: any failing scenario produces a top-level
    Fail, any ReviewRequired (in the absence of Fail) produces ReviewRequired,
    any Invalid (in the absence of Fail / ReviewRequired) produces Invalid,
    otherwise Pass.  Unknown labels are treated as the most severe so a
    silent enum drift can never weaken the top-level outcome.
    """
    if not outcomes:
        return "Pass"
    worst = max(outcomes, key=lambda o: _SCENARIO_OUTCOME_SEVERITY.get(o, 99))
    return worst


def _sha256_file(path: Path) -> str:
    """Return the hex SHA256 of a file's raw bytes."""
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _now_iso() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@click.command("validate")
@click.argument("plan_path", type=click.Path(exists=True, path_type=Path))
@click.option("--silent", "-s", is_flag=True, help="Suppress per-bar event display")
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    show_default=True,
)
@click.option(
    "--html-report/--no-html-report",
    default=True,
    show_default=True,
    help="Generate HTML report",
)
@click.option(
    "--on-child-failure",
    type=click.Choice(["fail_fast", "continue"], case_sensitive=False),
    default=None,
    help="Override plan on_child_failure",
)
@click.option("--dry-run", is_flag=True, help="Resolve and print effective plan; no execution, no writes")
@click.option("--force", is_flag=True, help="Allow overwriting existing validations/<vid>/ directory")
def validate_command(
    plan_path: Path,
    silent: bool,
    log_level: str,
    html_report: bool,
    on_child_failure: str | None,
    dry_run: bool,
    force: bool,
) -> None:
    """Run out-of-sample validation for a strategy validation plan."""
    try:
        _run_validate(
            plan_path=plan_path,
            silent=silent,
            log_level=log_level,
            html_report=html_report,
            on_child_failure=on_child_failure,
            dry_run=dry_run,
            force=force,
        )
    except SystemExit:
        raise
    except Exception as exc:
        click.echo(f"ERROR: {exc}", err=True)
        sys.exit(4)


def _run_validate(
    plan_path: Path,
    silent: bool,
    log_level: str,
    html_report: bool,
    on_child_failure: str | None,
    dry_run: bool,
    force: bool,
) -> None:
    # ── Apply log level ────────────────────────────────────────────────────
    if log_level and log_level.upper() != "INFO":
        from typing import Literal, cast  # noqa: PLC0415

        from qs_trader.system import LoggerFactory  # noqa: PLC0415
        from qs_trader.system.config import get_system_config  # noqa: PLC0415

        system_config = get_system_config()
        level = cast(Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"], log_level.upper())
        system_config.logging.level = level
        LoggerFactory.configure(system_config.logging.to_logger_config())

    # ── Load plan ──────────────────────────────────────────────────────────
    plan = load_validation_plan(plan_path)

    # Override on_child_failure if CLI option supplied
    if on_child_failure is not None:
        plan = plan.model_copy(
            update={"execution": plan.execution.model_copy(update={"on_child_failure": on_child_failure})}
        )

    # ── Generate splits ────────────────────────────────────────────────────
    splits = get_split_generator(plan).generate(plan)

    # ── Dry-run: print and exit ────────────────────────────────────────────
    if dry_run:
        plan_dict = plan.model_dump(mode="json")
        if "base_config" in plan_dict:
            plan_dict["base_config"] = str(plan_dict["base_config"])
        click.echo(json.dumps(plan_dict, indent=2, default=str))
        click.echo("\nSplits:")
        for s in splits:
            status_tag = f" [INVALID: {s.reason}]" if s.status == "invalid" else ""
            click.echo(
                f"  fold={s.fold_index} role={s.role}"
                f" {s.test_range.start_date} \u2192 {s.test_range.end_date}{status_tag}"
            )
        if plan.cost_scenarios is not None:
            click.echo("\nCost scenarios:")
            # Pretty-align: pad scenario names to the longest name so the
            # ``folds=N`` column lines up regardless of name length.
            name_width = max(len(s.name) for s in plan.cost_scenarios)
            for scenario in plan.cost_scenarios:
                click.echo(f"  {scenario.name:<{name_width}}  folds={len(splits)}")
        return  # exit 0

    # ── Hard gate: walk_forward non-dry-run not yet implemented ───────────
    if plan.mode == "walk_forward":
        click.echo(
            "Error: walk_forward execution is not yet supported (Phase 2A.2+). "
            "Use --dry-run to inspect the generated splits.",
            err=True,
        )
        sys.exit(_OUTCOME_EXIT_CODES["Invalid"])  # exit 3

    # ── Resolve output directory ───────────────────────────────────────────
    # Layout: experiments/<exp>/validations/<vid>.yaml  (file-form)
    #      or experiments/<exp>/validations/<vid>/       (dir-form)
    # out_dir must always be:  experiments/<exp>/validations/<vid>/
    if plan_path.is_dir():
        out_dir = plan_path.resolve()
    else:
        out_dir = (plan_path.parent / plan.validation_id).resolve()

    # ── Load base config and compute sha256s ───────────────────────────────
    base_config = load_backtest_config(plan.base_config)
    plan_sha256 = compute_plan_sha256(plan, plan.base_config)
    base_config_sha256 = _sha256_file(plan.base_config)

    started_at = _now_iso()

    # ── Apply --silent to base config before passing to runner ──────────────
    if silent:
        base_config = base_config.model_copy(update={"replay_speed": -1.0, "display_events": None})

    # ── Run folds ──────────────────────────────────────────────────────────
    runner = SequentialValidationRunner(plan, splits, base_config, out_dir, force=force)
    try:
        child_refs = runner.run()
    except ChildRunFailedError as e:
        # fail_fast mode: write Invalid evidence pack then exit 3.
        # The exception carries all refs collected so far (including the failed
        # fold), so the decision engine will correctly produce Invalid.
        child_refs = e.partial_refs
        logger.error("validation_runner_fail_fast", fold_id=e.fold_id, error=str(e.cause))

    finished_at = _now_iso()

    # ── Aggregate per-scenario (Phase 2A.2) ─────────────────────────────────
    # When the plan declared cost_scenarios, run the existing
    # MetricsAggregator + DecisionEngine once per scenario.  Top-level summary
    # fields stay anchored to the first scenario (typically "base") for
    # downstream compatibility; per-scenario decisions are reported under the
    # new ``cost_scenarios`` block in summary.json.
    # When cost_scenarios is None, behavior is byte-identical to Phase 1 /
    # Phase 2A.1.
    scenario_summaries: list[dict[str, Any]] | None = None

    def _load_role_metrics(refs: list[Any]) -> tuple[dict[str, float], dict[str, float]]:
        is_m: dict[str, float] = {}
        oos_m: dict[str, float] = {}
        for ref in refs:
            if ref.status != "success":
                continue
            perf_path = ref.run_dir / "performance.json"
            if not perf_path.exists():
                continue
            try:
                data = json.loads(perf_path.read_text())
            except Exception:
                continue
            if not isinstance(data, dict):
                continue
            numeric = {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
            if ref.role == "is":
                is_m = numeric
            elif ref.role == "oos":
                oos_m = numeric
        return is_m, oos_m

    if plan.cost_scenarios is None:
        is_metrics, oos_metrics = _load_role_metrics(child_refs)
        comparison = MetricsAggregator().aggregate(is_metrics, oos_metrics, plan.metrics)
        decision = DecisionEngine(plan.metrics).evaluate(comparison, plan.decision, child_refs)
        top_outcome: str = decision.outcome
        top_reason_codes: list[str] = list(decision.reason_codes)
    else:
        # Group refs by scenario and compute a per-scenario decision.
        from qs_trader.validation.decision import ValidationDecision  # noqa: PLC0415

        scenario_summaries = []
        grouped: dict[str, list[Any]] = {s.name: [] for s in plan.cost_scenarios}
        for ref in child_refs:
            grouped.setdefault(ref.scenario or "", []).append(ref)
        first_scenario_name = plan.cost_scenarios[0].name
        comparison = {}
        # Defensive default if first-scenario refs were absent (e.g. fail_fast
        # aborted before any refs landed under it).
        decision = ValidationDecision(outcome="Invalid", reason_codes=["child_fold_failed"], rule_results=[])
        scenario_decisions: list[tuple[str, str, list[str]]] = []
        # I6: Only scenarios that actually executed (i.e. produced at least one
        # ChildRunRef) participate in aggregation and per-scenario reporting.
        # Under ``fail_fast`` the runner aborts at the first failing fold, so
        # scenarios scheduled after that point have no refs at all; treating
        # them as Invalid would inject spurious ``cost_scenario_failed:<name>``
        # and ``missing_metric:*`` codes that the user never asked for.  The
        # predicate is simply ``bool(srefs)`` — absence from the grouped refs
        # dict means the runner never reached that scenario.
        for scenario in plan.cost_scenarios:
            srefs = grouped.get(scenario.name, [])
            if not srefs:
                continue
            is_m, oos_m = _load_role_metrics(srefs)
            s_comparison = MetricsAggregator().aggregate(is_m, oos_m, plan.metrics)
            s_decision = DecisionEngine(plan.metrics).evaluate(s_comparison, plan.decision, srefs)
            scenario_summaries.append(
                {
                    "name": scenario.name,
                    "decision": s_decision.outcome,
                    "reason_codes": list(s_decision.reason_codes),
                    "folds": [{"fold_id": r.fold_id, "role": r.role, "status": r.status} for r in srefs],
                }
            )
            scenario_decisions.append((scenario.name, s_decision.outcome, list(s_decision.reason_codes)))
            if scenario.name == first_scenario_name and srefs:
                comparison = s_comparison
                decision = s_decision

        # ── Cross-scenario aggregation (Phase 2A.2 §4) ──────────────────
        # The top-level ``outcome`` reflects the worst severity across all
        # scenarios that produced a decision.  Severity ordering matches the
        # requirement doc: Fail > ReviewRequired > Invalid > Pass.  For every
        # non-Pass scenario a ``cost_scenario_failed:<name>`` reason code is
        # appended to the top-level ``reason_codes`` (the base scenario's own
        # rule-level reason codes propagate as well so audits keep the
        # detailed cause).  Under ``fail_fast`` the runner aborts mid-matrix,
        # so any unreached scenario contributes nothing to the top-level
        # decision (I6); the aborted scenario itself is recorded as Invalid by
        # the decision engine (its OOS fold never produced metrics).
        #
        # I5 carve-out: when the plan declares exactly one scenario named
        # ``base`` (the implicit collapsed case), suppress the otherwise
        # redundant ``cost_scenario_failed:base`` marker — the underlying
        # per-fold reason codes already carry the full story and the prefix
        # is only meaningful when distinguishing between multiple declared
        # scenarios.
        lone_base = len(plan.cost_scenarios) == 1 and plan.cost_scenarios[0].name == "base"
        if scenario_decisions:
            top_outcome = _aggregate_scenario_outcomes([o for _, o, _ in scenario_decisions])
            top_reason_codes = list(decision.reason_codes)
            if not lone_base:
                for s_name, s_outcome, _s_codes in scenario_decisions:
                    if s_outcome != "Pass":
                        code = f"cost_scenario_failed:{s_name}"
                        if code not in top_reason_codes:
                            top_reason_codes.append(code)
        else:
            top_outcome = decision.outcome
            top_reason_codes = list(decision.reason_codes)

    # ── Write audit pack ───────────────────────────────────────────────────
    audit_summary = AuditWriter().write_audit(plan, plan_sha256, base_config_sha256, started_at, finished_at, out_dir)

    # ── Write summary.json + effective_plan.yaml ───────────────────────────
    summary_writer = SummaryWriter()
    summary_dict = summary_writer.write_summary(
        validation_id=plan.validation_id,
        plan=plan,
        plan_sha256=plan_sha256,
        base_config_sha256=base_config_sha256,
        outcome=top_outcome,
        reason_codes=top_reason_codes,
        folds=child_refs,
        comparison=comparison,
        decision=decision,
        audit=audit_summary,
        started_at=started_at,
        finished_at=finished_at,
        out_dir=out_dir,
        scenario_summaries=scenario_summaries,
    )
    summary_writer.write_effective_plan(plan, out_dir)

    # ── HTML report ────────────────────────────────────────────────────────
    if html_report:
        ValidationHTMLReporter().render(summary_dict, out_dir / "report.html")

    # ── Rich console summary ───────────────────────────────────────────────
    if plan.reporting.console_summary:
        _print_rich_summary(summary_dict, decision)

    # ── Exit code ──────────────────────────────────────────────────────────
    exit_code = _OUTCOME_EXIT_CODES.get(top_outcome, 4)
    if exit_code != 0:
        sys.exit(exit_code)


def _print_rich_summary(summary: dict[str, Any], decision: Any) -> None:
    """Print a Rich console summary of the validation outcome."""
    try:
        from rich.console import Console  # noqa: PLC0415
        from rich.table import Table  # noqa: PLC0415

        console = Console()
        outcome = summary.get("outcome", "")
        _outcome_styles: dict[str, str] = {
            "Pass": "bold green",
            "Fail": "bold red",
            "ReviewRequired": "bold yellow",
            "Invalid": "bold magenta",
        }
        style = _outcome_styles.get(outcome, "bold white")
        console.print(f"\n[{style}]Outcome: {outcome}[/{style}]")

        reason_codes = summary.get("reason_codes", [])
        if reason_codes:
            console.print(f"[dim]Reason codes:[/dim] {', '.join(reason_codes)}")

        rule_results = summary.get("decision", {}).get("rule_results", [])
        if rule_results:
            table = Table(title="Decision Rules", show_header=True, header_style="bold cyan")
            table.add_column("Rule")
            table.add_column("Threshold")
            table.add_column("Actual")
            table.add_column("Passed")
            for rr in rule_results:
                passed_str = "[green]✓[/green]" if rr.get("passed") else "[red]✗[/red]"
                table.add_row(
                    str(rr.get("rule", "")),
                    str(rr.get("threshold", "")),
                    str(rr.get("actual", "")),
                    passed_str,
                )
            console.print(table)
    except Exception as exc:
        # Rich is optional for the summary; log and move on
        logger.warning("rich_summary_failed", error=str(exc))
        click.echo(f"Outcome: {summary.get('outcome', '')}")
