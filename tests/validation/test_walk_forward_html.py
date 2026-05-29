"""Tests for Phase 2A.5 \u2014 Reporting (HTML + equity overlay PNG).

Covers T5.1\u2013T5.4:

* T5.1 \u2014 ``generate_equity_overlay`` returns PNG bytes starting with the PNG
  magic header ``\\x89PNG``.
* T5.1 \u2014 chart renders without error for 0, 1, and 5 fold boundaries.
* T5.2 \u2014 ``render()`` with a ``walk_forward`` summary dict produces HTML
  containing the fold-aggregates block, the per-fold decision table, the
  cost-scenarios table, and the benchmark section.
* T5.2 \u2014 when ``equity_chart_png`` bytes are supplied the HTML contains a
  ``data:image/png;base64,`` URI.
* T5.2 \u2014 ``static_is_oos`` summary still renders without walk-forward sections.
* T5.3 \u2014 hostile XSS strings in user-controlled fields are escaped in the
  output (no raw ``<script>`` or injected ``onerror=`` attribute).
* T5.4 \u2014 importing ``charts.py`` does NOT import matplotlib at module level.
"""

from __future__ import annotations

import html as _html
import importlib
import sys
from pathlib import Path
from typing import Any

import pytest

from qs_trader.validation.reporting.charts import generate_equity_overlay
from qs_trader.validation.reporting.html import ValidationHTMLReporter


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wf_summary() -> dict[str, Any]:
    return {
        "validation_id": "wf-test-v1",
        "outcome": "Pass",
        "started_at": "2026-01-01T00:00:00",
        "finished_at": "2026-01-02T00:00:00",
        "plan_sha256": "abc123def456",
        "mode": "walk_forward",
        "folds": [
            {
                "fold_id": "f0__oos",
                "fold_index": 0,
                "role": "oos",
                "status": "success",
                "metrics": {
                    "sharpe_ratio": 0.80,
                    "total_return": 0.22,
                    "max_drawdown": 0.15,
                },
                "per_fold_decision": "Pass",
                "per_fold_reason_codes": [],
                "error": None,
            },
            {
                "fold_id": "f1__oos",
                "fold_index": 1,
                "role": "oos",
                "status": "success",
                "metrics": {
                    "sharpe_ratio": 0.65,
                    "total_return": 0.18,
                    "max_drawdown": 0.20,
                },
                "per_fold_decision": "Pass",
                "per_fold_reason_codes": [],
                "error": None,
            },
        ],
        "fold_aggregates": {
            "metric": "sharpe_ratio",
            "median": 0.71,
            "iqr": 0.18,
            "min": 0.42,
            "max": 0.95,
            "count_pass_folds": 6,
            "count_total_folds": 7,
        },
        "cost_scenarios": [
            {"name": "base", "decision": "Pass", "median_oos_sharpe": 0.71},
            {"name": "high", "decision": "Fail", "median_oos_sharpe": 0.18},
        ],
        "benchmark": {
            "instrument": "SPY",
            "metrics": {"sharpe_ratio": 0.60, "total_return": 0.18},
            "strategy_minus_benchmark": {
                "sharpe_ratio": 0.11,
                "total_return": 0.04,
            },
        },
        "comparison": {},
        "decision": {"rule_results": []},
    }


def _static_summary() -> dict[str, Any]:
    return {
        "validation_id": "static-test-v1",
        "outcome": "Pass",
        "started_at": "2026-01-01T00:00:00",
        "finished_at": "2026-01-02T00:00:00",
        "plan_sha256": "000111222333",
        "mode": "static_is_oos",
        "folds": [
            {
                "fold_id": "f0__is",
                "role": "is",
                "status": "success",
                "metrics": {
                    "sharpe_ratio": 1.20,
                    "total_return": 0.35,
                    "max_drawdown": 0.10,
                },
                "error": None,
            },
            {
                "fold_id": "f1__oos",
                "role": "oos",
                "status": "success",
                "metrics": {
                    "sharpe_ratio": 0.90,
                    "total_return": 0.25,
                    "max_drawdown": 0.12,
                },
                "error": None,
            },
        ],
        "comparison": {
            "sharpe_ratio": {"is": 1.20, "oos": 0.90, "decay": 0.25},
        },
        "decision": {
            "rule_results": [
                {"rule": "oos_sharpe_min", "threshold": 0.5, "actual": 0.90, "passed": True},
            ]
        },
    }


# ---------------------------------------------------------------------------
# T5.1 \u2014 generate_equity_overlay
# ---------------------------------------------------------------------------


def test_generate_equity_overlay_returns_png_bytes() -> None:
    """T5.1: returned bytes must start with the PNG magic header."""
    strategy = [1.0, 1.02, 1.05, 1.03, 1.08]
    benchmark = [1.0, 1.01, 1.03, 1.02, 1.04]
    result = generate_equity_overlay(strategy, benchmark, fold_boundaries=[2])
    assert isinstance(result, bytes), "result must be bytes"
    assert result[:4] == b"\x89PNG", "first 4 bytes must be PNG magic"


@pytest.mark.parametrize("n_boundaries", [0, 1, 5])
def test_generate_equity_overlay_fold_boundary_counts(n_boundaries: int) -> None:
    """T5.1: chart renders without error for varying fold-boundary counts."""
    n = 20
    strategy = [1.0 + i * 0.01 for i in range(n)]
    benchmark = [1.0 + i * 0.008 for i in range(n)]
    boundaries = list(range(2, 2 + n_boundaries * 3, 3))[:n_boundaries]
    result = generate_equity_overlay(strategy, benchmark, fold_boundaries=boundaries)
    assert result[:4] == b"\x89PNG"


def test_generate_equity_overlay_raises_for_unequal_lengths() -> None:
    """W1: ValueError when strategy and benchmark equity lists differ in length."""
    with pytest.raises(ValueError, match="equal length"):
        generate_equity_overlay([1.0, 1.1, 1.2], [1.0, 1.1], fold_boundaries=[])


def test_generate_equity_overlay_raises_for_out_of_range_boundary() -> None:
    """N2: ValueError when a fold boundary index is outside the equity series range."""
    strategy = [1.0, 1.01, 1.02, 1.03, 1.04]
    benchmark = [1.0, 1.01, 1.02, 1.03, 1.04]
    with pytest.raises(ValueError, match="out of range"):
        generate_equity_overlay(strategy, benchmark, fold_boundaries=[5])  # index == len


# ---------------------------------------------------------------------------
# T5.2 \u2014 ValidationHTMLReporter walk-forward sections
# ---------------------------------------------------------------------------


def test_wf_html_fold_aggregates_section(tmp_path: Path) -> None:
    """T5.2: walk_forward summary produces a fold-aggregates block."""
    reporter = ValidationHTMLReporter()
    out = tmp_path / "report.html"
    reporter.render(_wf_summary(), out)
    content = out.read_text()

    assert "Walk-Forward Aggregates" in content
    assert "median" in content
    assert "count_pass_folds" in content


def test_wf_html_fold_details_section(tmp_path: Path) -> None:
    """T5.2: walk_forward summary produces a per-fold decision table."""
    reporter = ValidationHTMLReporter()
    out = tmp_path / "report.html"
    reporter.render(_wf_summary(), out)
    content = out.read_text()

    assert "Walk-Forward Fold Details" in content
    assert "per_fold_decision" in content
    # Fold id appears in the fold details table
    assert "f0__oos" in content


def test_wf_html_cost_scenarios_section(tmp_path: Path) -> None:
    """T5.2: cost_scenarios table appears when present."""
    reporter = ValidationHTMLReporter()
    out = tmp_path / "report.html"
    reporter.render(_wf_summary(), out)
    content = out.read_text()

    assert "Cost Scenarios" in content
    assert "base" in content
    assert "high" in content


def test_wf_html_benchmark_section(tmp_path: Path) -> None:
    """T5.2: benchmark section appears when present."""
    reporter = ValidationHTMLReporter()
    out = tmp_path / "report.html"
    reporter.render(_wf_summary(), out)
    content = out.read_text()

    assert "Benchmark" in content
    assert "SPY" in content


def test_wf_html_equity_png_embedded(tmp_path: Path) -> None:
    """T5.2: equity_chart_png bytes are embedded as a base64 data URI."""
    strategy = [1.0, 1.02, 1.04]
    benchmark = [1.0, 1.01, 1.02]
    png_bytes = generate_equity_overlay(strategy, benchmark, fold_boundaries=[])

    reporter = ValidationHTMLReporter()
    out = tmp_path / "report.html"
    reporter.render(_wf_summary(), out, equity_chart_png=png_bytes)
    content = out.read_text()

    assert "data:image/png;base64," in content
    assert "Equity Overlay" in content


def test_wf_html_no_png_when_not_supplied(tmp_path: Path) -> None:
    """T5.2: equity overlay section is absent when no PNG is passed."""
    reporter = ValidationHTMLReporter()
    out = tmp_path / "report.html"
    reporter.render(_wf_summary(), out)
    content = out.read_text()

    assert "data:image/png;base64," not in content


def test_static_mode_unchanged(tmp_path: Path) -> None:
    """T5.2: static_is_oos summary renders correctly without WF sections."""
    reporter = ValidationHTMLReporter()
    out = tmp_path / "report.html"
    reporter.render(_static_summary(), out)
    content = out.read_text()

    # Core sections must still be present
    assert "Validation Report" in content
    assert "f0__is" in content
    assert "f1__oos" in content

    # Walk-forward sections must NOT appear
    assert "Walk-Forward Aggregates" not in content
    assert "Walk-Forward Fold Details" not in content


# ---------------------------------------------------------------------------
# T5.3 \u2014 XSS safety
# ---------------------------------------------------------------------------


def test_xss_escaping_in_all_user_fields(tmp_path: Path) -> None:
    """T5.3: hostile strings in user-controlled fields are HTML-escaped."""
    xss_script = "<script>alert(1)</script>"
    xss_attr = '"><img onerror=alert(1)>'

    summary = _wf_summary()

    # Inject into every user-controlled field covered by the reporter
    summary["validation_id"] = xss_script
    summary["folds"][0]["fold_id"] = xss_attr
    summary["folds"][0]["error"] = xss_script
    summary["folds"][0]["per_fold_decision"] = xss_script
    summary["fold_aggregates"]["metric"] = xss_script  # type: ignore[index]
    summary["cost_scenarios"][0]["name"] = xss_attr  # type: ignore[index]
    summary["benchmark"]["instrument"] = xss_script  # type: ignore[index]
    summary["comparison"] = {xss_attr: {"is": 1.0, "oos": 0.9, "decay": 0.1}}

    reporter = ValidationHTMLReporter()
    out = tmp_path / "report.html"
    reporter.render(summary, out)
    content = out.read_text()

    # No raw unescaped hostile tags must appear
    assert "<script>" not in content
    assert "</script>" not in content
    # onerror= inside a live tag would look like: onerror=... preceded by <img
    # After escaping "><img onerror=alert(1)> becomes &quot;&gt;&lt;img onerror=...
    # so "<img" must not appear as a tag opener
    assert "<img onerror" not in content

    # The escaped forms must be present (validates that escape was applied, not dropped)
    assert _html.escape(xss_script) in content
    assert _html.escape(xss_attr) in content


# ---------------------------------------------------------------------------
# T5.4 \u2014 Lazy matplotlib import
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# W1 — ImportError when matplotlib absent
# ---------------------------------------------------------------------------


def test_generate_equity_overlay_import_error_when_matplotlib_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Charts raises ImportError when matplotlib is not available."""
    import builtins

    real_import = builtins.__import__

    def mock_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "matplotlib" or name.startswith("matplotlib."):
            raise ImportError("No module named 'matplotlib'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)

    # Must also remove from sys.modules so the lazy import actually runs
    import sys

    mpl_keys = [k for k in sys.modules if k == "matplotlib" or k.startswith("matplotlib.")]
    for k in mpl_keys:
        monkeypatch.delitem(sys.modules, k)

    from qs_trader.validation.reporting import charts

    with pytest.raises(ImportError, match="matplotlib"):
        charts.generate_equity_overlay([1.0, 1.1], [1.0, 1.05], [])


# ---------------------------------------------------------------------------
# W2 — None per_fold_decision does not crash render()
# ---------------------------------------------------------------------------


def test_wf_html_handles_none_per_fold_decision(tmp_path: Path) -> None:
    """render() must not crash when per_fold_decision is explicitly None."""
    summary: dict[str, Any] = {
        "validation_id": "test-none-decision",
        "mode": "walk_forward",
        "outcome": "Fail",
        "started_at": "2024-01-01T00:00:00",
        "finished_at": "2024-01-01T01:00:00",
        "plan_sha256": "aaaa",
        "folds": [
            {
                "fold_id": "f0__oos",
                "fold_index": 0,
                "role": "oos",
                "status": "error",
                "metrics": {},
                "per_fold_decision": None,  # explicitly None
                "error": "simulation failed",
            }
        ],
        "decision": {"rule_results": []},
    }
    out = tmp_path / "report.html"
    ValidationHTMLReporter().render(summary, out)
    content = out.read_text()
    assert "simulation failed" in content  # page was rendered
    assert "N/A" in content  # error fold shows labelled grey pill


# ---------------------------------------------------------------------------
# T5.4 — Lazy matplotlib import
# ---------------------------------------------------------------------------


def test_lazy_import_no_matplotlib_at_module_level() -> None:
    """T5.4: importing charts.py must NOT pull in matplotlib at module level."""
    charts_mod = "qs_trader.validation.reporting.charts"

    # Drop the module from the import cache so we get a fresh import
    sys.modules.pop(charts_mod, None)

    # Snapshot which matplotlib modules are present before import
    mpl_before = {k for k in sys.modules if k.startswith("matplotlib")}

    importlib.import_module(charts_mod)

    mpl_after = {k for k in sys.modules if k.startswith("matplotlib")}
    new_mpl = mpl_after - mpl_before

    assert not new_mpl, f"charts.py imported matplotlib at module level; new keys: {new_mpl}"
