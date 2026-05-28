"""Audit pack writer for the QS-Trader OOS validation framework.

Writes environment, git, holdout, and plan metadata files under
``out_dir/audit/``.  NEVER writes ``os.environ`` verbatim (R6 / OWASP).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

import structlog

import qs_trader
from qs_trader.engine.experiment import ExperimentMetadata

logger = structlog.get_logger(__name__)

__all__ = ["AuditWriter"]

# Derive the project repo root from this file's location:
#   src/qs_trader/validation/audit.py → parents[3] = QS-Trader/
_REPO_ROOT: Path = Path(__file__).resolve().parents[3]


class AuditWriter:
    """Writes audit metadata files under ``out_dir/audit/``."""

    def write_audit(
        self,
        plan: Any,
        plan_sha256: str,
        base_config_sha256: str,
        started_at: str,
        finished_at: str,
        out_dir: Path,
    ) -> dict[str, Any]:
        """Write audit files and return a compact audit summary dict.

        Files written:
        - ``audit/environment.json`` — allow-list only (no ``os.environ`` dump).
        - ``audit/git.json`` — commit, branch, dirty flag.
        - ``audit/holdout.json`` — holdout declared/consumed record.
        - ``audit/plan_sha256.txt`` — SHA-256 of the effective plan + base config.
        - ``audit/base_config_sha256.txt`` — SHA-256 of the base config YAML bytes.

        Args:
            plan: The :class:`~qs_trader.validation.plan.ValidationPlan`.
            plan_sha256: SHA256 of the plan + base config.
            base_config_sha256: SHA256 of the base config YAML bytes.
            started_at: ISO timestamp when the validation started.
            finished_at: ISO timestamp when the validation finished.
            out_dir: Root output directory for the validation run.

        Returns:
            A compact summary dict for embedding in ``summary.json``.
        """
        audit_dir = out_dir / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)

        # environment.json — allow-list only (R6)
        env_data: dict[str, Any] = {
            "python_version": sys.version,
            "qs_trader_version": getattr(qs_trader, "__version__", "unknown"),
            "platform": sys.platform,
            "os_name": os.name,
        }
        _write_json(audit_dir / "environment.json", env_data)

        # git.json — use ExperimentMetadata to ensure cwd is always the
        # QS-Trader repo root, not the caller's working directory (BLOCKER-4).
        git_info = ExperimentMetadata.capture_git_info(repo_path=_REPO_ROOT)
        git_data: dict[str, Any] = {
            "commit": git_info.commit if git_info else None,
            "branch": git_info.branch if git_info else None,
            "dirty": git_info.dirty if git_info else False,
        }
        _write_json(audit_dir / "git.json", git_data)

        # holdout.json
        holdout = getattr(plan, "holdout", None)
        holdout_data: dict[str, Any] = {
            "declared": holdout is not None,
            "start_date": str(holdout.start_date) if holdout else None,
            "end_date": str(holdout.end_date) if holdout else None,
            "consumed": False,
            "consumed_at": None,
            "consumed_by_plan_id": None,
            "consumed_at_code_commit": None,
        }
        _write_json(audit_dir / "holdout.json", holdout_data)

        # plan_sha256.txt + base_config_sha256.txt (per §5.1 layout)
        (audit_dir / "plan_sha256.txt").write_text(plan_sha256 + "\n")
        (audit_dir / "base_config_sha256.txt").write_text(base_config_sha256 + "\n")

        logger.info("audit_pack_written", audit_dir=str(audit_dir))

        # Compact audit summary for embedding in summary.json
        return {
            "code_commit": git_data.get("commit"),
            "code_dirty": git_data.get("dirty", False),
            "python_version": env_data["python_version"],
            "qs_trader_version": env_data["qs_trader_version"],
            "holdout_declared": holdout_data["declared"],
            "holdout_consumed": holdout_data["consumed"],
        }


def _write_json(path: Path, data: dict[str, Any]) -> None:
    with path.open("w") as f:
        json.dump(data, f, indent=2, default=str)
