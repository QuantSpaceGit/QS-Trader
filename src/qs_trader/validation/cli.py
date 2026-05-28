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
from qs_trader.validation.runner import SequentialValidationRunner
from qs_trader.validation.splits.static import StaticSplitGenerator

logger = structlog.get_logger(__name__)

_OUTCOME_EXIT_CODES: dict[str, int] = {
    "Pass": 0,
    "Fail": 1,
    "ReviewRequired": 2,
    "Invalid": 3,
}


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
        plan = plan.model_copy(update={"execution": plan.execution.model_copy(update={"on_child_failure": on_child_failure})})

    # ── Generate splits ────────────────────────────────────────────────────
    splits = StaticSplitGenerator().generate(plan)

    # ── Dry-run: print and exit ────────────────────────────────────────────
    if dry_run:
        plan_dict = plan.model_dump(mode="json")
        if "base_config" in plan_dict:
            plan_dict["base_config"] = str(plan_dict["base_config"])
        click.echo(json.dumps(plan_dict, indent=2, default=str))
        click.echo("\nSplits:")
        for s in splits:
            click.echo(
                f"  fold={s.fold_index} role={s.role} "
                f"{s.test_range.start_date} → {s.test_range.end_date}"
            )
        return  # exit 0

    # ── Resolve output directory ───────────────────────────────────────────
    # Layout: experiments/<exp>/validations/<plan>.yaml
    # out_dir = plan_path.parent  (= experiments/<exp>/validations/<vid>/)
    if plan_path.is_dir():
        out_dir = plan_path.resolve()
    else:
        out_dir = plan_path.parent.resolve()

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
    child_refs = runner.run()

    finished_at = _now_iso()

    # ── Load per-fold metrics ──────────────────────────────────────────────
    is_metrics: dict[str, float] = {}
    oos_metrics: dict[str, float] = {}
    for ref in child_refs:
        if ref.status == "success":
            perf_path = ref.run_dir / "performance.json"
            if perf_path.exists():
                try:
                    data = json.loads(perf_path.read_text())
                    if isinstance(data, dict):
                        if ref.role == "is":
                            is_metrics = {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
                        elif ref.role == "oos":
                            oos_metrics = {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
                except Exception:
                    pass

    # ── Aggregate and decide ───────────────────────────────────────────────
    comparison = MetricsAggregator().aggregate(is_metrics, oos_metrics, plan.metrics)
    decision = DecisionEngine(plan.metrics).evaluate(comparison, plan.decision, child_refs)

    # ── Write audit pack ───────────────────────────────────────────────────
    audit_summary = AuditWriter().write_audit(
        plan, plan_sha256, base_config_sha256, started_at, finished_at, out_dir
    )

    # ── Write summary.json + effective_plan.yaml ───────────────────────────
    summary_writer = SummaryWriter()
    summary_dict = summary_writer.write_summary(
        validation_id=plan.validation_id,
        plan=plan,
        plan_sha256=plan_sha256,
        base_config_sha256=base_config_sha256,
        outcome=decision.outcome,
        reason_codes=decision.reason_codes,
        folds=child_refs,
        comparison=comparison,
        decision=decision,
        audit=audit_summary,
        started_at=started_at,
        finished_at=finished_at,
        out_dir=out_dir,
    )
    summary_writer.write_effective_plan(plan, out_dir)

    # ── HTML report ────────────────────────────────────────────────────────
    if html_report:
        ValidationHTMLReporter().render(summary_dict, out_dir / "report.html")

    # ── Rich console summary ───────────────────────────────────────────────
    if plan.reporting.console_summary and not silent:
        _print_rich_summary(summary_dict, decision)

    # ── Exit code ──────────────────────────────────────────────────────────
    exit_code = _OUTCOME_EXIT_CODES.get(decision.outcome, 4)
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
