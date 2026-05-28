"""Tests for SummaryWriter, AuditWriter, and ValidationHTMLReporter."""

from __future__ import annotations

import json
from pathlib import Path


import pytest
import yaml

from qs_trader.validation.aggregation import MetricComparison
from qs_trader.validation.audit import AuditWriter
from qs_trader.validation.decision import RuleResult, ValidationDecision
from qs_trader.validation.plan import load_validation_plan
from qs_trader.validation.reporting import SummaryWriter, ValidationHTMLReporter
from qs_trader.validation.runner import ChildRunRef

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _make_plan(tmp_path: Path):
    """Build a minimal ValidationPlan pointing to tmp base config."""
    base_cfg = tmp_path / "base.yaml"
    base_cfg.write_text(
        (FIXTURES_DIR / "base_config.yaml").read_text()
    )
    plan_yaml = tmp_path / "test_plan.yaml"
    plan_yaml.write_text(
        f"""
validation_id: test_vid
strategy_experiment: test_exp
base_config: {base_cfg}
mode: static_is_oos
splits:
  in_sample:
    start_date: "2018-01-02"
    end_date: "2021-12-31"
  out_of_sample:
    start_date: "2022-01-03"
    end_date: "2024-12-31"
holdout:
  start_date: "2025-01-02"
  end_date: "2025-12-31"
decision:
  rules:
    oos_sharpe_min: 0.8
    oos_max_drawdown_max: 0.25
    min_oos_trades: 30
"""
    )
    return load_validation_plan(plan_yaml)


def _make_child_refs(tmp_path: Path) -> list[ChildRunRef]:
    fold0_dir = tmp_path / "folds" / "f0__is"
    fold0_dir.mkdir(parents=True)
    perf0 = fold0_dir / "performance.json"
    perf0.write_text(json.dumps({"sharpe_ratio": 1.2, "total_return": 0.5, "max_drawdown": 0.15, "num_trades": 100}))

    fold1_dir = tmp_path / "folds" / "f1__oos"
    fold1_dir.mkdir(parents=True)
    perf1 = fold1_dir / "performance.json"
    perf1.write_text(json.dumps({"sharpe_ratio": 0.9, "total_return": 0.3, "max_drawdown": 0.2, "num_trades": 50}))

    return [
        ChildRunRef(
            fold_id="f0__is",
            run_id="val_test_vid__f0__is",
            experiment_id="test_exp",
            role="is",
            run_dir=fold0_dir,
            status="success",
            error=None,
        ),
        ChildRunRef(
            fold_id="f1__oos",
            run_id="val_test_vid__f1__oos",
            experiment_id="test_exp",
            role="oos",
            run_dir=fold1_dir,
            status="success",
            error=None,
        ),
    ]


def _make_comparison() -> dict[str, MetricComparison]:
    return {
        "sharpe_ratio": MetricComparison(is_val=1.2, oos=0.9, full=None, decay=0.25),
        "total_return": MetricComparison(is_val=0.5, oos=0.3, full=None, decay=None),
        "max_drawdown": MetricComparison(is_val=0.15, oos=0.2, full=None, decay=None),
    }


def _make_decision() -> ValidationDecision:
    return ValidationDecision(
        outcome="Pass",
        reason_codes=[],
        rule_results=[
            RuleResult(rule="oos_sharpe_min", threshold=0.8, actual=0.9, passed=True),
            RuleResult(rule="oos_max_drawdown_max", threshold=0.25, actual=0.2, passed=True),
        ],
    )


# ---------------------------------------------------------------------------
# T7.1: summary.json schema
# ---------------------------------------------------------------------------


class TestSummaryJsonSchema:
    def test_required_top_level_keys_present(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        comparison = _make_comparison()
        decision = _make_decision()
        audit = {"code_commit": "abc", "holdout_declared": True, "holdout_consumed": False}

        writer = SummaryWriter()
        result = writer.write_summary(
            validation_id="test_vid",
            plan=plan,
            plan_sha256="sha_abc",
            base_config_sha256="sha_xyz",
            outcome="Pass",
            reason_codes=[],
            folds=child_refs,
            comparison=comparison,
            decision=decision,
            audit=audit,
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:05:00+00:00",
            out_dir=tmp_path,
        )

        required_keys = {
            "validation_id",
            "plan_sha256",
            "base_config_sha256",
            "strategy_experiment",
            "mode",
            "started_at",
            "finished_at",
            "outcome",
            "reason_codes",
            "folds",
            "comparison",
            "decision",
            "audit",
        }
        assert required_keys.issubset(result.keys())

    def test_summary_json_file_written(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        writer = SummaryWriter()
        writer.write_summary(
            validation_id="test_vid",
            plan=plan,
            plan_sha256="sha_abc",
            base_config_sha256="sha_xyz",
            outcome="Pass",
            reason_codes=[],
            folds=child_refs,
            comparison=_make_comparison(),
            decision=_make_decision(),
            audit={},
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:05:00+00:00",
            out_dir=tmp_path,
        )
        summary_path = tmp_path / "summary.json"
        assert summary_path.exists()
        with summary_path.open() as f:
            data = json.load(f)
        assert data["validation_id"] == "test_vid"
        assert data["outcome"] == "Pass"


# ---------------------------------------------------------------------------
# T7.1: is_val → "is" JSON key (critical contract)
# ---------------------------------------------------------------------------


class TestIsKeySerialisation:
    def test_comparison_uses_is_key_not_is_val(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        writer = SummaryWriter()
        writer.write_summary(
            validation_id="test_vid",
            plan=plan,
            plan_sha256="sha_abc",
            base_config_sha256="sha_xyz",
            outcome="Pass",
            reason_codes=[],
            folds=child_refs,
            comparison=_make_comparison(),
            decision=_make_decision(),
            audit={},
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:05:00+00:00",
            out_dir=tmp_path,
        )
        with (tmp_path / "summary.json").open() as f:
            data = json.load(f)
        for metric, vals in data["comparison"].items():
            assert "is" in vals, f"Key 'is' missing for metric {metric}"
            assert "is_val" not in vals, f"Key 'is_val' must not appear for metric {metric}"

    def test_is_value_correct(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        writer = SummaryWriter()
        writer.write_summary(
            validation_id="test_vid",
            plan=plan,
            plan_sha256="sha_abc",
            base_config_sha256="sha_xyz",
            outcome="Pass",
            reason_codes=[],
            folds=child_refs,
            comparison=_make_comparison(),
            decision=_make_decision(),
            audit={},
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:05:00+00:00",
            out_dir=tmp_path,
        )
        with (tmp_path / "summary.json").open() as f:
            data = json.load(f)
        assert data["comparison"]["sharpe_ratio"]["is"] == pytest.approx(1.2)
        assert data["comparison"]["sharpe_ratio"]["oos"] == pytest.approx(0.9)


# ---------------------------------------------------------------------------
# T7.2: effective_plan.yaml
# ---------------------------------------------------------------------------


class TestEffectivePlanYaml:
    def test_effective_plan_yaml_readable(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        writer = SummaryWriter()
        writer.write_effective_plan(plan, tmp_path)
        ep_path = tmp_path / "effective_plan.yaml"
        assert ep_path.exists()
        with ep_path.open() as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict)
        assert "validation_id" in data
        assert data["validation_id"] == "test_vid"


# ---------------------------------------------------------------------------
# T8: AuditWriter
# ---------------------------------------------------------------------------


class TestAuditWriter:
    def test_all_audit_files_created(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        AuditWriter().write_audit(plan, "sha_plan", "sha_base", "2026-01-01T00:00:00", "2026-01-01T00:05:00", tmp_path)
        audit_dir = tmp_path / "audit"
        assert (audit_dir / "environment.json").exists()
        assert (audit_dir / "git.json").exists()
        assert (audit_dir / "holdout.json").exists()
        assert (audit_dir / "plan_sha256.txt").exists()
        assert (audit_dir / "base_config_sha256.txt").exists()

    def test_audit_returns_summary_dict(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        result = AuditWriter().write_audit(plan, "sha_plan", "sha_base", "t1", "t2", tmp_path)
        assert isinstance(result, dict)
        assert "holdout_declared" in result
        assert "holdout_consumed" in result


# ---------------------------------------------------------------------------
# T8.1: AuditWriter — no secrets in environment.json (R6)
# ---------------------------------------------------------------------------


class TestAuditNoSecrets:
    def test_environment_json_contains_only_allowlist_keys(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        AuditWriter().write_audit(plan, "sha_plan", "sha_base", "t1", "t2", tmp_path)
        with (tmp_path / "audit" / "environment.json").open() as f:
            data = json.load(f)
        allowed = {"python_version", "qs_trader_version", "platform", "os_name"}
        assert set(data.keys()) == allowed, f"Unexpected keys: {set(data.keys()) - allowed}"

    def test_environment_json_no_env_var_keys(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)
        AuditWriter().write_audit(plan, "sha_plan", "sha_base", "t1", "t2", tmp_path)
        with (tmp_path / "audit" / "environment.json").open() as f:
            data = json.load(f)
        # Must not look like environment variables (upper-case names / PATH / HOME etc.)
        for key in data:
            assert key == key.lower(), f"Key {key!r} appears to be an env var name"


# ---------------------------------------------------------------------------
# T8.3: Holdout record
# ---------------------------------------------------------------------------


class TestHoldoutRecord:
    def test_holdout_declared_true_when_holdout_present(self, tmp_path: Path) -> None:
        plan = _make_plan(tmp_path)  # plan includes holdout
        AuditWriter().write_audit(plan, "sha_plan", "sha_base", "t1", "t2", tmp_path)
        with (tmp_path / "audit" / "holdout.json").open() as f:
            data = json.load(f)
        assert data["declared"] is True
        assert data["consumed"] is False

    def test_holdout_declared_false_when_no_holdout(self, tmp_path: Path) -> None:
        # Plan without holdout
        base_cfg = tmp_path / "base2.yaml"
        base_cfg.write_text((FIXTURES_DIR / "base_config.yaml").read_text())
        plan_yaml = tmp_path / "plan_no_holdout.yaml"
        plan_yaml.write_text(
            f"""
validation_id: no_holdout
strategy_experiment: test_exp
base_config: {base_cfg}
mode: static_is_oos
splits:
  in_sample:
    start_date: "2018-01-02"
    end_date: "2021-12-31"
  out_of_sample:
    start_date: "2022-01-03"
    end_date: "2024-12-31"
"""
        )
        plan = load_validation_plan(plan_yaml)
        AuditWriter().write_audit(plan, "sha", "sha", "t1", "t2", tmp_path)
        with (tmp_path / "audit" / "holdout.json").open() as f:
            data = json.load(f)
        assert data["declared"] is False


# ---------------------------------------------------------------------------
# T9.1: ValidationHTMLReporter
# ---------------------------------------------------------------------------


class TestHTMLReporterOutput:
    def _make_summary(self) -> dict:
        return {
            "validation_id": "html_test_vid",
            "plan_sha256": "abc123def456",
            "base_config_sha256": "xyz",
            "strategy_experiment": "test_exp",
            "mode": "static_is_oos",
            "started_at": "2026-01-01T00:00:00+00:00",
            "finished_at": "2026-01-01T00:05:00+00:00",
            "outcome": "Pass",
            "reason_codes": [],
            "folds": [
                {
                    "fold_id": "f0__is",
                    "role": "is",
                    "run_id": "val_html_test_vid__f0__is",
                    "experiment_id": "test_exp",
                    "start_date": "2018-01-02",
                    "end_date": "2021-12-31",
                    "status": "success",
                    "metrics": {"sharpe_ratio": 1.2, "total_return": 0.5},
                }
            ],
            "comparison": {
                "sharpe_ratio": {"is": 1.2, "oos": 0.9, "full": None, "decay": 0.25},
            },
            "decision": {
                "outcome": "Pass",
                "reason_codes": [],
                "rule_results": [
                    {"rule": "oos_sharpe_min", "threshold": 0.8, "actual": 0.9, "passed": True},
                ],
            },
            "audit": {},
        }

    def test_html_contains_validation_id(self, tmp_path: Path) -> None:
        summary = self._make_summary()
        out_path = tmp_path / "report.html"
        ValidationHTMLReporter().render(summary, out_path)
        assert out_path.exists()
        html = out_path.read_text()
        assert "html_test_vid" in html

    def test_html_contains_table_tag(self, tmp_path: Path) -> None:
        summary = self._make_summary()
        out_path = tmp_path / "report.html"
        ValidationHTMLReporter().render(summary, out_path)
        html = out_path.read_text()
        assert "<table" in html.lower()

    def test_html_contains_rule_name(self, tmp_path: Path) -> None:
        summary = self._make_summary()
        out_path = tmp_path / "report.html"
        ValidationHTMLReporter().render(summary, out_path)
        html = out_path.read_text()
        assert "oos_sharpe_min" in html

    def test_html_no_external_links(self, tmp_path: Path) -> None:
        """HTML must be standalone — no CDN/external URLs (R7)."""
        summary = self._make_summary()
        out_path = tmp_path / "report.html"
        ValidationHTMLReporter().render(summary, out_path)
        html = out_path.read_text()
        assert "https://" not in html
        assert "http://" not in html

    def test_html_outcome_present(self, tmp_path: Path) -> None:
        summary = self._make_summary()
        out_path = tmp_path / "report.html"
        ValidationHTMLReporter().render(summary, out_path)
        html = out_path.read_text()
        assert "Pass" in html
