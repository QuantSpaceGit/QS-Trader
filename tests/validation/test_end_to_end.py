"""End-to-end integration test for the validation pipeline (no BacktestEngine execution)."""

from __future__ import annotations

import json
from pathlib import Path
import yaml

from qs_trader.validation.aggregation import MetricsAggregator
from qs_trader.validation.audit import AuditWriter
from qs_trader.validation.decision import DecisionEngine
from qs_trader.validation.plan import compute_plan_sha256, load_validation_plan
from qs_trader.validation.reporting import SummaryWriter, ValidationHTMLReporter
from qs_trader.validation.runner import ChildRunRef


FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _write_minimal_plan(tmp_path: Path) -> tuple[Path, Path]:
    """Write a minimal plan YAML + base config to tmp_path. Returns (plan_yaml, base_cfg)."""
    base_cfg = tmp_path / "base.yaml"
    base_cfg.write_text((FIXTURES_DIR / "base_config.yaml").read_text())

    plan_yaml = tmp_path / "e2e_plan.yaml"
    plan_yaml.write_text(
        f"""
validation_id: e2e_vid
strategy_experiment: e2e_exp
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
metrics:
  required:
    - total_return
    - cagr
    - sharpe_ratio
    - max_drawdown
    - volatility
    - num_trades
  recommended:
    - sortino_ratio
decision:
  rules:
    oos_sharpe_min: 0.8
    oos_max_drawdown_max: 0.25
    min_oos_trades: 30
reporting:
  html: true
  console_summary: false
"""
    )
    return plan_yaml, base_cfg


def _stub_child_refs(out_dir: Path) -> list[ChildRunRef]:
    """Create stub ChildRunRefs with performance.json files so metrics can be loaded."""
    fold0_dir = out_dir / "folds" / "f0__is"
    fold0_dir.mkdir(parents=True, exist_ok=True)
    (fold0_dir / "performance.json").write_text(
        json.dumps(
            {
                "sharpe_ratio": 1.3,
                "total_return": 0.55,
                "max_drawdown": 0.14,
                "num_trades": 120,
                "cagr": 0.12,
                "volatility": 0.18,
            }
        )
    )

    fold1_dir = out_dir / "folds" / "f1__oos"
    fold1_dir.mkdir(parents=True, exist_ok=True)
    (fold1_dir / "performance.json").write_text(
        json.dumps(
            {
                "sharpe_ratio": 0.95,
                "total_return": 0.32,
                "max_drawdown": 0.19,
                "num_trades": 60,
                "cagr": 0.10,
                "volatility": 0.17,
            }
        )
    )

    return [
        ChildRunRef(
            fold_id="f0__is",
            run_id="val_e2e_vid__f0__is",
            experiment_id="e2e_exp",
            role="is",
            run_dir=fold0_dir,
            status="success",
            error=None,
        ),
        ChildRunRef(
            fold_id="f1__oos",
            run_id="val_e2e_vid__f1__oos",
            experiment_id="e2e_exp",
            role="oos",
            run_dir=fold1_dir,
            status="success",
            error=None,
        ),
    ]


class TestEndToEnd:
    """Full pipeline integration test using stubbed ChildRunRefs (no engine execution)."""

    def _run_pipeline(self, tmp_path: Path) -> Path:
        """Execute the full pipeline and return out_dir."""
        plan_yaml, base_cfg = _write_minimal_plan(tmp_path)
        out_dir = tmp_path

        plan = load_validation_plan(plan_yaml)
        # Stub the runner to return pre-built ChildRunRefs (no engine execution)
        child_refs = _stub_child_refs(out_dir)

        # Load per-fold metrics
        is_metrics: dict[str, float] = {}
        oos_metrics: dict[str, float] = {}
        for ref in child_refs:
            perf = ref.run_dir / "performance.json"
            if perf.exists():
                data = json.loads(perf.read_text())
                if ref.role == "is":
                    is_metrics = {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}
                elif ref.role == "oos":
                    oos_metrics = {k: float(v) for k, v in data.items() if isinstance(v, (int, float))}

        comparison = MetricsAggregator().aggregate(is_metrics, oos_metrics, plan.metrics)
        decision = DecisionEngine(plan.metrics).evaluate(comparison, plan.decision, child_refs)

        plan_sha256 = compute_plan_sha256(plan, plan.base_config)
        base_config_sha256 = plan.base_config.read_bytes().__hash__.__class__  # just use a placeholder
        base_config_sha256 = "e2e_fake_sha256"

        audit_summary = AuditWriter().write_audit(
            plan, plan_sha256, base_config_sha256, "t1", "t2", out_dir
        )

        writer = SummaryWriter()
        summary_dict = writer.write_summary(
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
            started_at="2026-01-01T00:00:00+00:00",
            finished_at="2026-01-01T00:05:00+00:00",
            out_dir=out_dir,
        )
        writer.write_effective_plan(plan, out_dir)
        ValidationHTMLReporter().render(summary_dict, out_dir / "report.html")

        return out_dir

    def test_summary_json_exists_and_is_non_empty(self, tmp_path: Path) -> None:
        out_dir = self._run_pipeline(tmp_path)
        summary_path = out_dir / "summary.json"
        assert summary_path.exists()
        assert summary_path.stat().st_size > 0

    def test_summary_json_valid_schema(self, tmp_path: Path) -> None:
        out_dir = self._run_pipeline(tmp_path)
        with (out_dir / "summary.json").open() as f:
            data = json.load(f)
        assert data["validation_id"] == "e2e_vid"
        assert data["outcome"] in ("Pass", "Fail", "ReviewRequired", "Invalid")
        assert isinstance(data["folds"], list)
        assert isinstance(data["comparison"], dict)
        assert isinstance(data["decision"], dict)

    def test_comparison_uses_is_key(self, tmp_path: Path) -> None:
        out_dir = self._run_pipeline(tmp_path)
        with (out_dir / "summary.json").open() as f:
            data = json.load(f)
        for metric, vals in data["comparison"].items():
            assert "is" in vals, f"'is' key missing for {metric}"
            assert "is_val" not in vals

    def test_effective_plan_yaml_exists_and_is_non_empty(self, tmp_path: Path) -> None:
        out_dir = self._run_pipeline(tmp_path)
        ep_path = out_dir / "effective_plan.yaml"
        assert ep_path.exists()
        assert ep_path.stat().st_size > 0

    def test_effective_plan_yaml_valid(self, tmp_path: Path) -> None:
        out_dir = self._run_pipeline(tmp_path)
        with (out_dir / "effective_plan.yaml").open() as f:
            data = yaml.safe_load(f)
        assert isinstance(data, dict)
        assert data.get("validation_id") == "e2e_vid"

    def test_audit_environment_json_exists_and_non_empty(self, tmp_path: Path) -> None:
        out_dir = self._run_pipeline(tmp_path)
        env_path = out_dir / "audit" / "environment.json"
        assert env_path.exists()
        assert env_path.stat().st_size > 0

    def test_report_html_exists_and_non_empty(self, tmp_path: Path) -> None:
        out_dir = self._run_pipeline(tmp_path)
        report_path = out_dir / "report.html"
        assert report_path.exists()
        assert report_path.stat().st_size > 0

    def test_report_html_contains_validation_id(self, tmp_path: Path) -> None:
        out_dir = self._run_pipeline(tmp_path)
        html = (out_dir / "report.html").read_text()
        assert "e2e_vid" in html
