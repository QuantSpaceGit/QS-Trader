"""Standalone HTML reporter for validation results."""

from __future__ import annotations

import base64
import html as _html
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

__all__ = ["ValidationHTMLReporter"]

_OUTCOME_COLORS: dict[str, str] = {
    "Pass": "#22c55e",
    "Fail": "#ef4444",
    "ReviewRequired": "#f97316",
    "Invalid": "#eab308",
}

_CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #0f172a; color: #e2e8f0; padding: 2rem; }
h1 { font-size: 1.5rem; font-weight: 700; margin-bottom: 1.5rem; color: #f8fafc; }
h2 { font-size: 1.1rem; font-weight: 600; margin: 1.5rem 0 0.75rem; color: #cbd5e1; }
table { width: 100%; border-collapse: collapse; font-size: 0.875rem; }
th { text-align: left; padding: 0.5rem 0.75rem; background: #1e293b;
     color: #94a3b8; font-weight: 600; border-bottom: 1px solid #334155; }
td { padding: 0.5rem 0.75rem; border-bottom: 1px solid #1e293b; }
tr:hover td { background: #1e293b; }
.badge { display: inline-block; padding: 0.2rem 0.6rem; border-radius: 9999px;
         font-size: 0.75rem; font-weight: 700; color: #fff; }
.pass-icon { color: #22c55e; }
.fail-icon { color: #ef4444; }
.section { background: #1e293b; border-radius: 0.5rem; padding: 1rem;
           margin-bottom: 1rem; }
"""


def _outcome_badge(outcome: str) -> str:
    color = _OUTCOME_COLORS.get(outcome, "#64748b")
    return f'<span class="badge" style="background:{color}">{_html.escape(outcome)}</span>'


def _bool_icon(passed: bool) -> str:
    if passed:
        return '<span class="pass-icon">&#10003;</span>'
    return '<span class="fail-icon">&#10007;</span>'


def _fmt_val(v: Any) -> str:
    if v is None:
        return "<em>null</em>"
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v)


class ValidationHTMLReporter:
    """Renders a standalone HTML validation report.

    The output has no external CSS, JS, or CDN dependencies (R7).
    """

    def render(
        self,
        summary: dict[str, Any],
        out_path: Path,
        equity_chart_png: bytes | None = None,
    ) -> None:
        """Render the validation summary as standalone HTML.

        Args:
            summary: The summary dict produced by :class:`SummaryWriter`.
            out_path: File path to write the HTML report.
            equity_chart_png: Optional raw PNG bytes to embed as an equity
                overlay chart.  Generate with
                :func:`~qs_trader.validation.reporting.charts.generate_equity_overlay`
                and pass the result here.  Keeps ``html.py`` decoupled from
                matplotlib — the caller decides whether to generate the chart.
        """
        validation_id: str = summary.get("validation_id", "")
        outcome: str = summary.get("outcome", "")
        started_at: str = summary.get("started_at", "")
        finished_at: str = summary.get("finished_at", "")
        plan_sha256: str = summary.get("plan_sha256", "")
        sha_short = plan_sha256[:12] if plan_sha256 else ""

        folds: list[dict[str, Any]] = summary.get("folds", [])
        comparison: dict[str, Any] = summary.get("comparison", {})
        decision: dict[str, Any] = summary.get("decision", {})
        rule_results: list[dict[str, Any]] = decision.get("rule_results", [])

        vid_e = _html.escape(validation_id)
        started_e = _html.escape(started_at)
        finished_e = _html.escape(finished_at)
        sha_e = _html.escape(sha_short)

        # ── Summary table ────────────────────────────────────────────────
        summary_table = f"""
<table>
  <tr><th>Field</th><th>Value</th></tr>
  <tr><td>validation_id</td><td>{vid_e}</td></tr>
  <tr><td>outcome</td><td>{_outcome_badge(outcome)}</td></tr>
  <tr><td>started_at</td><td>{started_e}</td></tr>
  <tr><td>finished_at</td><td>{finished_e}</td></tr>
  <tr><td>plan_sha256</td><td><code>{sha_e}</code></td></tr>
</table>"""

        # ── Decision table ───────────────────────────────────────────────
        decision_rows = "".join(
            f"<tr><td>{_html.escape(rr['rule'])}</td><td>{_fmt_val(rr.get('threshold'))}</td>"
            f"<td>{_fmt_val(rr.get('actual'))}</td>"
            f"<td>{_bool_icon(bool(rr.get('passed')))}</td></tr>"
            for rr in rule_results
        )
        decision_table = f"""
<table>
  <tr><th>Rule</th><th>Threshold</th><th>Actual</th><th>Passed</th></tr>
  {decision_rows}
</table>"""

        # ── Comparison table ─────────────────────────────────────────────
        comparison_rows = "".join(
            f"<tr><td>{_html.escape(metric)}</td><td>{_fmt_val(vals.get('is'))}</td>"
            f"<td>{_fmt_val(vals.get('oos'))}</td><td>{_fmt_val(vals.get('decay'))}</td></tr>"
            for metric, vals in comparison.items()
        )
        comparison_table = f"""
<table>
  <tr><th>Metric</th><th>IS</th><th>OOS</th><th>Decay</th></tr>
  {comparison_rows}
</table>"""

        # ── Folds table ──────────────────────────────────────────────────
        _top_metrics = ("sharpe_ratio", "total_return", "max_drawdown")
        fold_rows = ""
        for fold in folds:
            metrics: dict[str, Any] = fold.get("metrics", {})
            metric_cells = " ".join(f"<td>{_fmt_val(metrics.get(m))}</td>" for m in _top_metrics)
            error_cell = f"<td>{_html.escape(fold.get('error') or '')}</td>"
            fold_rows += (
                f"<tr><td>{_html.escape(fold.get('fold_id', ''))}</td>"
                f"<td>{_html.escape(fold.get('role', ''))}</td>"
                f"<td>{_html.escape(fold.get('status', ''))}</td>"
                f"{error_cell}"
                f"{metric_cells}</tr>"
            )
        folds_table = f"""
<table>
  <tr><th>fold_id</th><th>role</th><th>status</th><th>error</th>
      <th>sharpe_ratio</th><th>total_return</th><th>max_drawdown</th></tr>
  {fold_rows}
</table>"""

        # ── Walk-forward aggregate section ───────────────────────────────────
        wf_aggregate_html = ""
        fold_aggregates: dict[str, Any] | None = summary.get("fold_aggregates")
        if fold_aggregates:
            agg_rows = (
                f"<tr><td>metric</td><td>{_html.escape(str(fold_aggregates.get('metric', '')))}</td></tr>"
                f"<tr><td>median</td><td>{_fmt_val(fold_aggregates.get('median'))}</td></tr>"
                f"<tr><td>IQR</td><td>{_fmt_val(fold_aggregates.get('iqr'))}</td></tr>"
                f"<tr><td>min</td><td>{_fmt_val(fold_aggregates.get('min'))}</td></tr>"
                f"<tr><td>max</td><td>{_fmt_val(fold_aggregates.get('max'))}</td></tr>"
                f"<tr><td>count_pass_folds</td><td>{_fmt_val(fold_aggregates.get('count_pass_folds'))}</td></tr>"
                f"<tr><td>count_total_folds</td><td>{_fmt_val(fold_aggregates.get('count_total_folds'))}</td></tr>"
            )
            wf_aggregate_html = (
                '<div class="section">'
                "<h2>Walk-Forward Aggregates</h2>"
                "<table><tr><th>Field</th><th>Value</th></tr>"
                f"{agg_rows}"
                "</table></div>"
            )

        # ── Walk-forward fold details (walk_forward mode only) ───────────────
        wf_folds_html = ""
        mode: str = summary.get("mode", "")
        if mode == "walk_forward":
            oos_folds = [f for f in folds if f.get("role") == "oos"]
            wf_fold_rows = "".join(
                f"<tr><td>{_fmt_val(fold.get('fold_index'))}</td>"
                f"<td>{_outcome_badge(fold.get('per_fold_decision') or 'N/A')}</td>"
                f"<td>{_fmt_val(fold.get('metrics', {}).get('sharpe_ratio'))}</td>"
                f"<td>{_fmt_val(fold.get('metrics', {}).get('total_return'))}</td>"
                f"<td>{_fmt_val(fold.get('metrics', {}).get('max_drawdown'))}</td></tr>"
                for fold in oos_folds
            )
            wf_folds_html = (
                '<div class="section">'
                "<h2>Walk-Forward Fold Details (OOS)</h2>"
                "<table>"
                "<tr><th>fold_index</th><th>per_fold_decision</th>"
                "<th>sharpe_ratio</th><th>total_return</th><th>max_drawdown</th></tr>"
                f"{wf_fold_rows}"
                "</table></div>"
            )

        # ── Cost scenarios section ───────────────────────────────────────────
        cost_scenarios_html = ""
        cost_scenarios: list[dict[str, Any]] = summary.get("cost_scenarios") or []
        if cost_scenarios:
            scenario_rows = "".join(
                f"<tr><td>{_html.escape(str(sc.get('name', '')))}</td>"
                f"<td>{_outcome_badge(sc.get('decision') or 'N/A')}</td>"
                f"<td>{_fmt_val(sc.get('median_oos_sharpe'))}</td></tr>"
                for sc in cost_scenarios
            )
            cost_scenarios_html = (
                '<div class="section">'
                "<h2>Cost Scenarios</h2>"
                "<table>"
                "<tr><th>Scenario</th><th>Decision</th><th>Median OOS Sharpe</th></tr>"
                f"{scenario_rows}"
                "</table></div>"
            )

        # ── Benchmark section ────────────────────────────────────────────────
        benchmark_html = ""
        benchmark: dict[str, Any] | None = summary.get("benchmark")
        if benchmark:
            instrument_e = _html.escape(str(benchmark.get("instrument", "")))
            bench_metrics: dict[str, Any] = benchmark.get("metrics", {})
            strat_minus: dict[str, Any] = benchmark.get("strategy_minus_benchmark", {})
            benchmark_html = (
                '<div class="section">'
                "<h2>Benchmark</h2>"
                "<table>"
                "<tr><th>Instrument</th><th>Benchmark Sharpe</th><th>Benchmark Return</th>"
                "<th>+/- Sharpe</th><th>+/- Return</th></tr>"
                f"<tr><td>{instrument_e}</td>"
                f"<td>{_fmt_val(bench_metrics.get('sharpe_ratio'))}</td>"
                f"<td>{_fmt_val(bench_metrics.get('total_return'))}</td>"
                f"<td>{_fmt_val(strat_minus.get('sharpe_ratio'))}</td>"
                f"<td>{_fmt_val(strat_minus.get('total_return'))}</td></tr>"
                "</table></div>"
            )

        # ── Equity overlay PNG section ───────────────────────────────────────
        equity_chart_html = ""
        if equity_chart_png is not None:
            png_b64 = base64.b64encode(equity_chart_png).decode("ascii")
            equity_chart_html = (
                '<div class="section">'
                "<h2>Equity Overlay</h2>"
                f'<img src="data:image/png;base64,{png_b64}" '
                'style="max-width:100%;border-radius:0.5rem;" alt="Equity overlay chart">'
                "</div>"
            )

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Validation Report: {_html.escape(validation_id)}</title>
<style>{_CSS}</style>
</head>
<body>
<h1>Validation Report: {vid_e}</h1>
<div class="section">
  <h2>Summary</h2>
  {summary_table}
</div>
<div class="section">
  <h2>Decision Rules</h2>
  {decision_table}
</div>
<div class="section">
  <h2>Metric Comparison (IS vs OOS)</h2>
  {comparison_table}
</div>
<div class="section">
  <h2>Folds</h2>
  {folds_table}
</div>
{wf_aggregate_html}
{wf_folds_html}
{cost_scenarios_html}
{benchmark_html}
{equity_chart_html}
</body>
</html>"""

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html, encoding="utf-8")
        logger.info("html_report_written", path=str(out_path))
