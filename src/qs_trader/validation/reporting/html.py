"""Standalone HTML reporter for validation results."""

from __future__ import annotations

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
    return f'<span class="badge" style="background:{color}">{outcome}</span>'


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

    def render(self, summary: dict[str, Any], out_path: Path) -> None:
        """Render the validation summary as standalone HTML.

        Args:
            summary: The summary dict produced by :class:`SummaryWriter`.
            out_path: File path to write the HTML report.
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

        # ── Summary table ────────────────────────────────────────────────
        summary_table = f"""
<table>
  <tr><th>Field</th><th>Value</th></tr>
  <tr><td>validation_id</td><td>{validation_id}</td></tr>
  <tr><td>outcome</td><td>{_outcome_badge(outcome)}</td></tr>
  <tr><td>started_at</td><td>{started_at}</td></tr>
  <tr><td>finished_at</td><td>{finished_at}</td></tr>
  <tr><td>plan_sha256</td><td><code>{sha_short}</code></td></tr>
</table>"""

        # ── Decision table ───────────────────────────────────────────────
        decision_rows = "".join(
            f"<tr><td>{rr['rule']}</td><td>{_fmt_val(rr.get('threshold'))}</td>"
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
            f"<tr><td>{metric}</td><td>{_fmt_val(vals.get('is'))}</td>"
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
            metric_cells = " ".join(
                f"<td>{_fmt_val(metrics.get(m))}</td>" for m in _top_metrics
            )
            fold_rows += (
                f"<tr><td>{fold.get('fold_id','')}</td>"
                f"<td>{fold.get('role','')}</td>"
                f"<td>{fold.get('status','')}</td>"
                f"{metric_cells}</tr>"
            )
        folds_table = f"""
<table>
  <tr><th>fold_id</th><th>role</th><th>status</th>
      <th>sharpe_ratio</th><th>total_return</th><th>max_drawdown</th></tr>
  {fold_rows}
</table>"""

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Validation Report: {validation_id}</title>
<style>{_CSS}</style>
</head>
<body>
<h1>Validation Report: {validation_id}</h1>
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
</body>
</html>"""

        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(html, encoding="utf-8")
        logger.info("html_report_written", path=str(out_path))
