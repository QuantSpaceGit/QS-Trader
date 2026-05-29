"""Tests for the ``qs-trader validate`` CLI command."""

from __future__ import annotations

from pathlib import Path
from typing import Literal, cast
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from qs_trader.validation.cli import validate_command
from qs_trader.validation.decision import ValidationDecision
from qs_trader.validation.runner import ChildRunFailedError, ChildRunRef

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _make_plan_yaml(tmp_path: Path, base_cfg_path: Path | None = None) -> Path:
    if base_cfg_path is None:
        base_cfg_path = tmp_path / "base.yaml"
        base_cfg_path.write_text((FIXTURES_DIR / "base_config.yaml").read_text())
    plan_yaml = tmp_path / "test_plan.yaml"
    plan_yaml.write_text(
        f"""
validation_id: cli_test
strategy_experiment: test_exp
base_config: {base_cfg_path}
mode: static_is_oos
splits:
  in_sample:
    start_date: "2018-01-02"
    end_date: "2021-12-31"
  out_of_sample:
    start_date: "2022-01-03"
    end_date: "2024-12-31"
decision:
  rules:
    oos_sharpe_min: 0.8
    oos_max_drawdown_max: 0.25
    min_oos_trades: 30
"""
    )
    return plan_yaml


def _make_child_refs(out_dir: Path) -> list[ChildRunRef]:
    fold0_dir = out_dir / "folds" / "f0__is"
    fold0_dir.mkdir(parents=True, exist_ok=True)
    fold1_dir = out_dir / "folds" / "f1__oos"
    fold1_dir.mkdir(parents=True, exist_ok=True)
    return [
        ChildRunRef(
            fold_id="f0__is",
            run_id="val_cli_test__f0__is",
            experiment_id="test_exp",
            role="is",
            run_dir=fold0_dir,
            status="success",
            error=None,
        ),
        ChildRunRef(
            fold_id="f1__oos",
            run_id="val_cli_test__f1__oos",
            experiment_id="test_exp",
            role="oos",
            run_dir=fold1_dir,
            status="success",
            error=None,
        ),
    ]


# ---------------------------------------------------------------------------
# T10.1: dry-run
# ---------------------------------------------------------------------------


class TestValidateDryRun:
    def test_dry_run_exits_0(self, tmp_path: Path) -> None:
        plan_yaml = _make_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert result.exit_code == 0, result.output

    def test_dry_run_prints_plan_contents(self, tmp_path: Path) -> None:
        plan_yaml = _make_plan_yaml(tmp_path)
        runner = CliRunner()
        result = runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        assert "cli_test" in result.output

    def test_dry_run_does_not_write_summary_json(self, tmp_path: Path) -> None:
        plan_yaml = _make_plan_yaml(tmp_path)
        runner = CliRunner()
        runner.invoke(validate_command, [str(plan_yaml), "--dry-run"])
        out_dir = plan_yaml.parent
        assert not (out_dir / "summary.json").exists()


# ---------------------------------------------------------------------------
# T10.2: exit codes
# ---------------------------------------------------------------------------


class TestValidateExitCodes:
    def _mock_pipeline(self, outcome: str, tmp_path: Path) -> tuple[Path, list]:
        """Return plan_yaml and the patches needed to mock the pipeline."""
        plan_yaml = _make_plan_yaml(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        return plan_yaml, child_refs

    @pytest.mark.parametrize(
        "outcome,expected_code",
        [
            ("Pass", 0),
            ("Fail", 1),
            ("ReviewRequired", 2),
            ("Invalid", 3),
        ],
    )
    def test_exit_code_matches_outcome(self, outcome: str, expected_code: int, tmp_path: Path) -> None:
        plan_yaml = _make_plan_yaml(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        outcome_lit = cast(Literal["Pass", "Fail", "ReviewRequired", "Invalid"], outcome)
        mock_decision = ValidationDecision(
            outcome=outcome_lit,
            reason_codes=[] if outcome == "Pass" else ["some_reason"],
            rule_results=[],
        )

        with (
            patch("qs_trader.validation.cli.SequentialValidationRunner") as mock_runner_cls,
            patch("qs_trader.validation.cli.MetricsAggregator") as mock_agg_cls,
            patch("qs_trader.validation.cli.DecisionEngine") as mock_engine_cls,
            patch("qs_trader.validation.cli.AuditWriter") as mock_audit_cls,
            patch("qs_trader.validation.cli.SummaryWriter") as mock_summary_cls,
            patch("qs_trader.validation.cli.ValidationHTMLReporter"),
            patch("qs_trader.validation.cli.load_backtest_config") as mock_load_cfg,
            patch("qs_trader.validation.cli._sha256_file", return_value="fakehash"),
        ):
            mock_runner_cls.return_value.run.return_value = child_refs
            mock_agg_cls.return_value.aggregate.return_value = {}
            mock_engine_cls.return_value.evaluate.return_value = mock_decision
            mock_audit_cls.return_value.write_audit.return_value = {}
            mock_summary_cls.return_value.write_summary.return_value = {
                "outcome": outcome,
                "decision": {"outcome": outcome, "reason_codes": [], "rule_results": []},
                "reason_codes": [],
                "validation_id": "cli_test",
            }
            mock_load_cfg.return_value = MagicMock()

            runner = CliRunner()
            result = runner.invoke(validate_command, [str(plan_yaml)])
            assert result.exit_code == expected_code, (
                f"Expected exit_code={expected_code} for outcome={outcome}, "
                f"got {result.exit_code}. Output:\n{result.output}"
            )


# ---------------------------------------------------------------------------
# T10.1: --on-child-failure override
# ---------------------------------------------------------------------------


class TestValidateOnChildFailureOverride:
    def test_on_child_failure_override_applied(self, tmp_path: Path) -> None:
        plan_yaml = _make_plan_yaml(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        mock_decision = ValidationDecision(outcome="Pass", reason_codes=[], rule_results=[])
        captured_plan: list = []

        def _capture_runner(plan, splits, base_config, out_dir, force=False):
            captured_plan.append(plan)
            m = MagicMock()
            m.run.return_value = child_refs
            return m

        with (
            patch("qs_trader.validation.cli.SequentialValidationRunner", side_effect=_capture_runner),
            patch("qs_trader.validation.cli.MetricsAggregator") as mock_agg_cls,
            patch("qs_trader.validation.cli.DecisionEngine") as mock_engine_cls,
            patch("qs_trader.validation.cli.AuditWriter") as mock_audit_cls,
            patch("qs_trader.validation.cli.SummaryWriter") as mock_summary_cls,
            patch("qs_trader.validation.cli.ValidationHTMLReporter"),
            patch("qs_trader.validation.cli.load_backtest_config") as mock_load_cfg,
            patch("qs_trader.validation.cli._sha256_file", return_value="fakehash"),
        ):
            mock_agg_cls.return_value.aggregate.return_value = {}
            mock_engine_cls.return_value.evaluate.return_value = mock_decision
            mock_audit_cls.return_value.write_audit.return_value = {}
            mock_summary_cls.return_value.write_summary.return_value = {
                "outcome": "Pass",
                "decision": {"outcome": "Pass", "reason_codes": [], "rule_results": []},
                "reason_codes": [],
                "validation_id": "cli_test",
            }
            mock_load_cfg.return_value = MagicMock()

            runner = CliRunner()
            runner.invoke(validate_command, [str(plan_yaml), "--on-child-failure", "continue"])

        assert captured_plan, "Runner was not instantiated"
        assert captured_plan[0].execution.on_child_failure == "continue"


# ---------------------------------------------------------------------------
# T10.1: --force flag
# ---------------------------------------------------------------------------


class TestValidateForceFlag:
    def test_force_passed_to_runner(self, tmp_path: Path) -> None:
        plan_yaml = _make_plan_yaml(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        mock_decision = ValidationDecision(outcome="Pass", reason_codes=[], rule_results=[])
        captured_kwargs: list = []

        def _capture_runner(plan, splits, base_config, out_dir, force=False):
            captured_kwargs.append({"force": force})
            m = MagicMock()
            m.run.return_value = child_refs
            return m

        with (
            patch("qs_trader.validation.cli.SequentialValidationRunner", side_effect=_capture_runner),
            patch("qs_trader.validation.cli.MetricsAggregator") as mock_agg_cls,
            patch("qs_trader.validation.cli.DecisionEngine") as mock_engine_cls,
            patch("qs_trader.validation.cli.AuditWriter") as mock_audit_cls,
            patch("qs_trader.validation.cli.SummaryWriter") as mock_summary_cls,
            patch("qs_trader.validation.cli.ValidationHTMLReporter"),
            patch("qs_trader.validation.cli.load_backtest_config") as mock_load_cfg,
            patch("qs_trader.validation.cli._sha256_file", return_value="fakehash"),
        ):
            mock_agg_cls.return_value.aggregate.return_value = {}
            mock_engine_cls.return_value.evaluate.return_value = mock_decision
            mock_audit_cls.return_value.write_audit.return_value = {}
            mock_summary_cls.return_value.write_summary.return_value = {
                "outcome": "Pass",
                "decision": {"outcome": "Pass", "reason_codes": [], "rule_results": []},
                "reason_codes": [],
                "validation_id": "cli_test",
            }
            mock_load_cfg.return_value = MagicMock()

            runner = CliRunner()
            runner.invoke(validate_command, [str(plan_yaml), "--force"])

        assert captured_kwargs, "Runner was not instantiated"
        assert captured_kwargs[0]["force"] is True


# ---------------------------------------------------------------------------
# T10.1: --no-html-report gate (WARNING-4 regression guard)
# ---------------------------------------------------------------------------


class TestNoHtmlReport:
    def test_no_html_report_skips_render(self, tmp_path: Path) -> None:
        """--no-html-report must prevent ValidationHTMLReporter.render() call."""
        plan_yaml = _make_plan_yaml(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        mock_decision = ValidationDecision(outcome="Pass", reason_codes=[], rule_results=[])

        with (
            patch("qs_trader.validation.cli.SequentialValidationRunner") as mock_runner_cls,
            patch("qs_trader.validation.cli.MetricsAggregator") as mock_agg_cls,
            patch("qs_trader.validation.cli.DecisionEngine") as mock_engine_cls,
            patch("qs_trader.validation.cli.AuditWriter") as mock_audit_cls,
            patch("qs_trader.validation.cli.SummaryWriter") as mock_summary_cls,
            patch("qs_trader.validation.cli.ValidationHTMLReporter") as mock_html_cls,
            patch("qs_trader.validation.cli.load_backtest_config") as mock_load_cfg,
            patch("qs_trader.validation.cli._sha256_file", return_value="fakehash"),
        ):
            mock_runner_cls.return_value.run.return_value = child_refs
            mock_agg_cls.return_value.aggregate.return_value = {}
            mock_engine_cls.return_value.evaluate.return_value = mock_decision
            mock_audit_cls.return_value.write_audit.return_value = {}
            mock_summary_cls.return_value.write_summary.return_value = {
                "outcome": "Pass",
                "decision": {"outcome": "Pass", "reason_codes": [], "rule_results": []},
                "reason_codes": [],
                "validation_id": "cli_test",
            }
            mock_load_cfg.return_value = MagicMock()

            runner = CliRunner()
            result = runner.invoke(validate_command, [str(plan_yaml), "--no-html-report"])

        assert result.exit_code == 0, result.output
        mock_html_cls.return_value.render.assert_not_called()


# ---------------------------------------------------------------------------
# T10.1: --silent forwards replay_speed=-1.0 to child configs (WARNING-5 guard)
# ---------------------------------------------------------------------------


class TestSilentForwarding:
    def test_silent_sets_replay_speed_minus_one(self, tmp_path: Path) -> None:
        """--silent must apply replay_speed=-1.0 to the base config passed to runner."""
        plan_yaml = _make_plan_yaml(tmp_path)
        child_refs = _make_child_refs(tmp_path)
        mock_decision = ValidationDecision(outcome="Pass", reason_codes=[], rule_results=[])
        captured_base_configs: list = []

        def _capture_runner(plan, splits, base_config, out_dir, force=False):
            captured_base_configs.append(base_config)
            m = MagicMock()
            m.run.return_value = child_refs
            return m

        mock_base_cfg = MagicMock()
        # model_copy must return an object whose replay_speed we can check
        silent_cfg = MagicMock()
        silent_cfg.replay_speed = -1.0
        mock_base_cfg.model_copy.return_value = silent_cfg

        with (
            patch("qs_trader.validation.cli.SequentialValidationRunner", side_effect=_capture_runner),
            patch("qs_trader.validation.cli.MetricsAggregator") as mock_agg_cls,
            patch("qs_trader.validation.cli.DecisionEngine") as mock_engine_cls,
            patch("qs_trader.validation.cli.AuditWriter") as mock_audit_cls,
            patch("qs_trader.validation.cli.SummaryWriter") as mock_summary_cls,
            patch("qs_trader.validation.cli.ValidationHTMLReporter"),
            patch("qs_trader.validation.cli.load_backtest_config", return_value=mock_base_cfg),
            patch("qs_trader.validation.cli._sha256_file", return_value="fakehash"),
        ):
            mock_agg_cls.return_value.aggregate.return_value = {}
            mock_engine_cls.return_value.evaluate.return_value = mock_decision
            mock_audit_cls.return_value.write_audit.return_value = {}
            mock_summary_cls.return_value.write_summary.return_value = {
                "outcome": "Pass",
                "decision": {"outcome": "Pass", "reason_codes": [], "rule_results": []},
                "reason_codes": [],
                "validation_id": "cli_test",
            }

            runner = CliRunner()
            result = runner.invoke(validate_command, [str(plan_yaml), "--silent"])

        assert result.exit_code == 0, result.output
        # model_copy must have been called with replay_speed=-1.0
        mock_base_cfg.model_copy.assert_called_once_with(update={"replay_speed": -1.0, "display_events": None})
        # The object passed to runner must be the silent config
        assert captured_base_configs, "Runner was not instantiated"
        assert captured_base_configs[0].replay_speed == -1.0


# ---------------------------------------------------------------------------
# BLOCKER-1 regression: file-form out_dir must include validation_id
# ---------------------------------------------------------------------------


class TestOutDirResolution:
    def _mock_and_run(self, plan_yaml: Path, child_refs: list[ChildRunRef], tmp_path: Path) -> list:
        """Run CLI with mocked pipeline and return captured out_dir values."""
        captured_out_dirs: list = []
        mock_decision = ValidationDecision(outcome="Pass", reason_codes=[], rule_results=[])

        def _capture_runner(plan, splits, base_config, out_dir, force=False):
            captured_out_dirs.append(out_dir)
            m = MagicMock()
            m.run.return_value = child_refs
            return m

        with (
            patch("qs_trader.validation.cli.SequentialValidationRunner", side_effect=_capture_runner),
            patch("qs_trader.validation.cli.MetricsAggregator") as mock_agg_cls,
            patch("qs_trader.validation.cli.DecisionEngine") as mock_engine_cls,
            patch("qs_trader.validation.cli.AuditWriter") as mock_audit_cls,
            patch("qs_trader.validation.cli.SummaryWriter") as mock_summary_cls,
            patch("qs_trader.validation.cli.ValidationHTMLReporter"),
            patch("qs_trader.validation.cli.load_backtest_config") as mock_load_cfg,
            patch("qs_trader.validation.cli._sha256_file", return_value="fakehash"),
        ):
            mock_agg_cls.return_value.aggregate.return_value = {}
            mock_engine_cls.return_value.evaluate.return_value = mock_decision
            mock_audit_cls.return_value.write_audit.return_value = {}
            mock_summary_cls.return_value.write_summary.return_value = {
                "outcome": "Pass",
                "decision": {"outcome": "Pass", "reason_codes": [], "rule_results": []},
                "reason_codes": [],
                "validation_id": "cli_test",
            }
            mock_load_cfg.return_value = MagicMock()
            CliRunner().invoke(validate_command, [str(plan_yaml)])
        return captured_out_dirs

    def test_file_form_out_dir_includes_validation_id(self, tmp_path: Path) -> None:
        """File-form plan_path must yield out_dir = parent/<validation_id>/."""
        plan_yaml = _make_plan_yaml(tmp_path)
        child_refs = _make_child_refs(tmp_path / "cli_test")
        captured = self._mock_and_run(plan_yaml, child_refs, tmp_path)
        assert captured, "Runner was never instantiated"
        assert captured[0] == (plan_yaml.parent / "cli_test").resolve()

    def test_file_form_out_dir_is_not_parent_alone(self, tmp_path: Path) -> None:
        """Regression: out_dir must not be plan_path.parent without validation_id."""
        plan_yaml = _make_plan_yaml(tmp_path)
        child_refs = _make_child_refs(tmp_path / "cli_test")
        captured = self._mock_and_run(plan_yaml, child_refs, tmp_path)
        assert captured, "Runner was never instantiated"
        assert captured[0] != plan_yaml.parent.resolve()


# ---------------------------------------------------------------------------
# BLOCKER-2 regression: fail_fast ChildRunFailedError produces Invalid evidence
# ---------------------------------------------------------------------------


class TestFailFastInvalidEvidence:
    def test_child_run_failed_error_produces_invalid_evidence(self, tmp_path: Path) -> None:
        """ChildRunFailedError must not propagate; CLI must write evidence and exit 3."""
        plan_yaml = _make_plan_yaml(tmp_path)

        # Partial refs the runner would have collected before failure
        fold0_dir = tmp_path / "cli_test" / "folds" / "f0__is"
        fold0_dir.mkdir(parents=True, exist_ok=True)
        failed_ref = ChildRunRef(
            fold_id="f0__is",
            run_id="val_cli_test__f0__is",
            experiment_id="test_exp",
            role="is",
            run_dir=fold0_dir,
            status="failed",
            error="BacktestError: no data",
        )
        partial_refs = [failed_ref]

        def _failing_runner(plan, splits, base_config, out_dir, force=False):
            m = MagicMock()
            m.run.side_effect = ChildRunFailedError(partial_refs, "f0__is", RuntimeError("no data"))
            return m

        with (
            patch("qs_trader.validation.cli.SequentialValidationRunner", side_effect=_failing_runner),
            patch("qs_trader.validation.cli.AuditWriter") as mock_audit_cls,
            patch("qs_trader.validation.cli.SummaryWriter") as mock_summary_cls,
            patch("qs_trader.validation.cli.ValidationHTMLReporter"),
            patch("qs_trader.validation.cli.load_backtest_config") as mock_load_cfg,
            patch("qs_trader.validation.cli._sha256_file", return_value="fakehash"),
            # Let real MetricsAggregator + DecisionEngine run so we get Invalid naturally
        ):
            mock_audit_cls.return_value.write_audit.return_value = {}
            written_summaries: list = []

            def _capture_write_summary(**kwargs):
                written_summaries.append(kwargs)
                return {
                    "outcome": "Invalid",
                    "decision": {"outcome": "Invalid", "reason_codes": ["child_fold_failed"], "rule_results": []},
                    "reason_codes": ["child_fold_failed"],
                    "validation_id": "cli_test",
                }

            mock_summary_cls.return_value.write_summary.side_effect = _capture_write_summary
            mock_load_cfg.return_value = MagicMock()

            runner = CliRunner()
            result = runner.invoke(validate_command, [str(plan_yaml)])

        # Must exit 3 (Invalid), not 4 (unhandled exception)
        assert result.exit_code == 3, f"Expected 3, got {result.exit_code}.  Output:\n{result.output}"
        # write_summary must have been called (evidence written)
        assert written_summaries, "write_summary was never called; evidence pack missing"
