"""Phase 2A.6 backward-compatibility golden-file tests.

These tests prove that static_is_oos (Phase 1) plans continue to produce
stable artifacts after every Phase 2A change:

- ``summary.json`` is checked for **byte-identical** equality (all fields are
  fully deterministic under the fixed-timestamp and fixed-audit mocks).
- ``effective_plan.yaml`` is checked for **normalized semantic equivalence**:
  the ``base_config`` field is reduced to its basename (so tests are portable
  across checkout locations), then both sides are re-serialised with
  ``yaml.dump(sort_keys=True)`` before comparison.  Key-ordering and
  quote-style drift are therefore not caught — but structural regressions
  (added/removed fields, changed values) are.

Golden files are committed in tests/validation/fixtures/static_is_oos_golden/.
To regenerate them (intentional changes only — see docs/validation-framework.md
for the deliberate-change procedure), delete the golden files and re-run the
test suite once with REGEN_GOLDEN=1:

    REGEN_GOLDEN=1 uv run pytest tests/validation/test_static_mode_backward_compatibility.py -v
"""

from __future__ import annotations

import json
import os
import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from click.testing import CliRunner

from qs_trader.validation.cli import validate_command
from qs_trader.validation.runner import ChildRunRef

FIXTURES = Path(__file__).parent / "fixtures"
GOLDEN_DIR = FIXTURES / "static_is_oos_golden"

# ---------------------------------------------------------------------------
# Determinism helpers
# ---------------------------------------------------------------------------

# Fixed timestamp eliminates started_at / finished_at non-determinism.
_FIXED_NOW = "2026-01-01T00:00:00+00:00"

# Fixed audit dict eliminates git commit hash and python-version variance.
_FIXED_AUDIT: dict[str, Any] = {
    "code_commit": "0000000000000000000000000000000000000000",
    "code_dirty": False,
    "python_version": "3.11.0 (golden)",
    "qs_trader_version": "test",
    "holdout_declared": False,
    "holdout_consumed": False,
}


def _normalize_effective_plan(content: str) -> str:
    """Normalize effective_plan.yaml to remove machine-specific absolute paths.

    Replaces the ``base_config`` absolute path with just the filename so that
    golden-file comparison is portable across checkout locations (local dev,
    CI, different OS paths).  All other fields are left unchanged.

    This normalization is applied to both the produced file and the committed
    golden before comparing, so structural regressions are still caught.
    """
    data = yaml.safe_load(content)
    if isinstance(data, dict) and "base_config" in data:
        data["base_config"] = Path(str(data["base_config"])).name
    return yaml.dump(data, default_flow_style=False, allow_unicode=True, sort_keys=True)


# ---------------------------------------------------------------------------
# Shared setup
# ---------------------------------------------------------------------------


def _setup_golden_run(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, list[ChildRunRef]]:
    """Copy fixture plan + base_config to tmp_path, wire deterministic mocks.

    Returns ``(plan_yaml_path, stubs)`` where ``stubs`` are the pre-built
    :class:`ChildRunRef` objects the mocked runner will return.
    """
    # Copy plan and base_config into tmp_path so the CLI can resolve them.
    shutil.copy(GOLDEN_DIR / "plan.yaml", tmp_path / "plan.yaml")
    shutil.copy(GOLDEN_DIR / "base_config.yaml", tmp_path / "base_config.yaml")
    plan_yaml_path = tmp_path / "plan.yaml"

    # Pre-create performance.json files that _load_role_metrics will read.
    # Metrics are chosen so all five decision rules produce Pass:
    #   oos_sharpe_min: 0.5       → oos sharpe 0.9  >= 0.5   ✓
    #   oos_max_drawdown_max: 0.30 → oos mdd  0.15 <= 0.30  ✓
    #   is_to_oos_sharpe_decay_max: 0.6 → (1.2-0.9)/1.2 = 0.25 <= 0.6 ✓
    #   min_oos_trades: 10        → oos num_trades 30 >= 10  ✓
    #   require_positive_oos_total_return → oos total_return 0.22 > 0  ✓
    out_dir = tmp_path / "golden_static_v1"
    is_dir = out_dir / "folds" / "f0__is"
    oos_dir = out_dir / "folds" / "f1__oos"
    is_dir.mkdir(parents=True, exist_ok=True)
    oos_dir.mkdir(parents=True, exist_ok=True)

    (is_dir / "performance.json").write_text(
        json.dumps(
            {
                "sharpe_ratio": 1.2,
                "total_return": 0.35,
                "max_drawdown": 0.10,
                "num_trades": 45,
                "cagr": 0.10,
                "volatility": 0.15,
                "sortino_ratio": 1.8,
            }
        )
    )
    (oos_dir / "performance.json").write_text(
        json.dumps(
            {
                "sharpe_ratio": 0.9,
                "total_return": 0.22,
                "max_drawdown": 0.15,
                "num_trades": 30,
                "cagr": 0.07,
                "volatility": 0.16,
                "sortino_ratio": 1.3,
            }
        )
    )

    stubs: list[ChildRunRef] = [
        ChildRunRef(
            fold_id="f0__is",
            run_id="val_golden_static_v1__f0__is",
            experiment_id="buy_and_hold",
            role="is",
            run_dir=is_dir,
            status="success",
            error=None,
        ),
        ChildRunRef(
            fold_id="f1__oos",
            run_id="val_golden_static_v1__f1__oos",
            experiment_id="buy_and_hold",
            role="oos",
            run_dir=oos_dir,
            status="success",
            error=None,
        ),
    ]

    # 1. Fixed timestamp — eliminates started_at / finished_at non-determinism.
    monkeypatch.setattr("qs_trader.validation.cli._now_iso", lambda: _FIXED_NOW)

    # 2. Fixed audit — eliminates git commit, python version, and qs_trader_version
    #    variance across machines and CI environments.
    monkeypatch.setattr(
        "qs_trader.validation.audit.AuditWriter.write_audit",
        lambda self, plan, plan_sha256, base_config_sha256, started_at, finished_at, out_dir: _FIXED_AUDIT,
    )

    # 3. Pre-built stubs — no real backtest execution.
    monkeypatch.setattr(
        "qs_trader.validation.runner.SequentialValidationRunner.run",
        lambda self: stubs,
    )

    return plan_yaml_path, stubs


# ---------------------------------------------------------------------------
# T6.1 / T6.2 — golden-file round-trip
# ---------------------------------------------------------------------------


def test_static_is_oos_round_trip_byte_identical(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Golden-file: re-running the Phase 1 static_is_oos plan produces
    byte-identical summary.json and semantically equivalent effective_plan.yaml.

    ``summary.json`` is compared byte-for-byte against the committed golden.
    ``effective_plan.yaml`` is compared after normalisation (base_config reduced
    to basename, keys sorted) so the check is portable across checkout locations
    while still catching structural regressions.

    When REGEN_GOLDEN=1 is set the golden files are written to the committed
    fixture directory and the test is skipped.  On all subsequent runs the
    produced artifacts are compared against those golden files.
    """
    plan_yaml_path, _ = _setup_golden_run(tmp_path, monkeypatch)

    result = CliRunner().invoke(validate_command, [str(plan_yaml_path), "--no-html-report"])
    assert result.exit_code == 0, (
        f"CLI exited with code {result.exit_code}.\nOutput:\n{result.output}\nException: {result.exception}"
    )

    out_dir = tmp_path / "golden_static_v1"
    produced_summary = (out_dir / "summary.json").read_text()
    produced_plan_normalized = _normalize_effective_plan((out_dir / "effective_plan.yaml").read_text())

    regen = os.environ.get("REGEN_GOLDEN", "").strip().lower() in ("1", "true", "yes")

    if regen:
        # Write (or overwrite) committed golden files.
        GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
        (GOLDEN_DIR / "summary.json").write_text(produced_summary)
        (GOLDEN_DIR / "effective_plan.yaml").write_text(produced_plan_normalized)
        pytest.skip("REGEN_GOLDEN=1: golden files written — re-run without REGEN_GOLDEN to assert.")

    # Normal run: assert byte-identical match.
    assert (GOLDEN_DIR / "summary.json").exists(), (
        "Golden summary.json missing. Run with REGEN_GOLDEN=1 to generate it."
    )
    assert (GOLDEN_DIR / "effective_plan.yaml").exists(), (
        "Golden effective_plan.yaml missing. Run with REGEN_GOLDEN=1 to generate it."
    )

    golden_summary = (GOLDEN_DIR / "summary.json").read_text()
    golden_plan = _normalize_effective_plan((GOLDEN_DIR / "effective_plan.yaml").read_text())

    assert produced_summary == golden_summary, (
        "summary.json does not match golden. "
        "If this change is intentional, see docs/validation-framework.md "
        "§ 'Deliberate-change procedure'."
    )
    assert produced_plan_normalized == golden_plan, (
        "effective_plan.yaml does not match golden (after base_config path normalisation). "
        "If this change is intentional, see docs/validation-framework.md "
        "§ 'Deliberate-change procedure'."
    )


# ---------------------------------------------------------------------------
# R9 — Phase 2A keys must not leak into Phase 1 artifacts
# ---------------------------------------------------------------------------


def test_static_mode_has_no_walk_forward_keys(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """R9: Phase 2A-only top-level keys must be absent from static_is_oos summary.json.

    Asserts that ``fold_aggregates``, ``benchmark``, and ``cost_scenarios`` are
    NOT present at the top level of summary.json when the plan does not declare
    them.  This proves that Phase 2A extensions do not silently inject keys into
    Phase 1 artifacts (no ``null`` sentinels, no empty blocks).
    """
    plan_yaml_path, _ = _setup_golden_run(tmp_path, monkeypatch)

    result = CliRunner().invoke(validate_command, [str(plan_yaml_path), "--no-html-report"])
    assert result.exit_code == 0, f"CLI exited {result.exit_code}.\nOutput:\n{result.output}"

    out_dir = tmp_path / "golden_static_v1"
    summary = json.loads((out_dir / "summary.json").read_text())

    assert "fold_aggregates" not in summary, (
        "fold_aggregates must not appear in static_is_oos summary.json (Phase 2A.4 key leaked)"
    )
    assert "benchmark" not in summary, (
        "benchmark must not appear in summary.json when the plan does not declare it (Phase 2A.3 key leaked)"
    )
    assert "cost_scenarios" not in summary, (
        "cost_scenarios must not appear in summary.json when the plan does not declare it (Phase 2A.2 key leaked)"
    )
