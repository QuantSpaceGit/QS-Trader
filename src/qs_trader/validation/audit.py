"""Audit pack writer for the QS-Trader OOS validation framework.

Writes environment, git, holdout, and plan metadata files under
``out_dir/audit/``.  NEVER writes ``os.environ`` verbatim (R6 / OWASP).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import structlog

import qs_trader

logger = structlog.get_logger(__name__)

__all__ = ["AuditWriter"]


def _collect_git_info() -> dict[str, Any]:
    """Collect git metadata via subprocess, returning null fields on failure."""
    commit: str | None = None
    branch: str | None = None
    dirty: bool = False

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        commit = result.stdout.strip() or None
    except subprocess.CalledProcessError:
        pass

    try:
        result = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        branch = result.stdout.strip() or None
    except subprocess.CalledProcessError:
        pass

    try:
        result = subprocess.run(
            ["git", "status", "--porcelain"],
            capture_output=True,
            text=True,
            check=True,
        )
        dirty = bool(result.stdout.strip())
    except subprocess.CalledProcessError:
        pass

    return {"commit": commit, "branch": branch, "dirty": dirty}


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
        - ``audit/plan_meta.json`` — plan sha256s and identifiers.

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

        # git.json
        git_data = _collect_git_info()
        _write_json(audit_dir / "git.json", git_data)

        # holdout.json
        holdout = getattr(plan, "holdout", None)
        holdout_data: dict[str, Any] = {
            "declared": holdout is not None,
            "start_date": str(holdout.start_date) if holdout else None,
            "end_date": str(holdout.end_date) if holdout else None,
            "consumed": False,
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
