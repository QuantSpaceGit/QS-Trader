"""Validation plan domain model, YAML loader, and plan hash computation."""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, model_validator

# ---------------------------------------------------------------------------
# Known decision-rule catalog (Phase 1)
# ---------------------------------------------------------------------------

KNOWN_FAIL_RULES: frozenset[str] = frozenset(
    {
        "oos_sharpe_min",
        "oos_max_drawdown_max",
        "is_to_oos_sharpe_decay_max",
        "min_oos_trades",
        "require_positive_oos_total_return",
    }
)

KNOWN_REVIEW_RULES: frozenset[str] = frozenset(
    {
        "is_to_oos_sharpe_decay_warn",
    }
)

# Backward-compatible alias
KNOWN_DECISION_RULES: frozenset[str] = KNOWN_FAIL_RULES

# ---------------------------------------------------------------------------
# Nested spec models
# ---------------------------------------------------------------------------


class DateRange(BaseModel):
    """Inclusive date range with a strict start < end constraint."""

    model_config = ConfigDict(frozen=True)

    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_range(self) -> "DateRange":
        """Reject date ranges where end_date is not strictly after start_date."""
        if self.end_date <= self.start_date:
            raise ValueError(f"end_date ({self.end_date}) must be strictly after start_date ({self.start_date})")
        return self


class StaticSplitSpec(BaseModel):
    """Static in-sample / out-of-sample split specification."""

    model_config = ConfigDict(frozen=True)

    in_sample: DateRange
    out_of_sample: DateRange

    @model_validator(mode="after")
    def validate_no_overlap(self) -> "StaticSplitSpec":
        """Reject IS/OOS configurations where OOS does not start after IS ends."""
        if self.out_of_sample.start_date <= self.in_sample.end_date:
            raise ValueError(
                f"out_of_sample.start_date ({self.out_of_sample.start_date}) must be strictly "
                f"after in_sample.end_date ({self.in_sample.end_date})"
            )
        return self


class HoldoutSpec(BaseModel):
    """Optional holdout period specification (record-only in Phase 1)."""

    model_config = ConfigDict(frozen=True)

    start_date: date
    end_date: date

    @model_validator(mode="after")
    def validate_range(self) -> "HoldoutSpec":
        """Reject holdout ranges where end_date is not strictly after start_date."""
        if self.end_date <= self.start_date:
            raise ValueError(f"end_date ({self.end_date}) must be strictly after start_date ({self.start_date})")
        return self


class BenchmarkRef(BaseModel):
    """Optional benchmark reference for summary reporting."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    data_source: str


class OnReviewRequiredRule(BaseModel):
    """A rule whose breach downgrades the outcome to ReviewRequired."""

    model_config = ConfigDict(frozen=True)

    rule: str
    threshold: float


class DecisionRulesSpec(BaseModel):
    """Declarative pass/fail decision rules catalog.

    All fields are Optional; None means the rule is disabled.
    ``extra='forbid'`` rejects unknown rule keys that are not in the known catalog.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    oos_sharpe_min: Optional[float] = None
    oos_max_drawdown_max: Optional[float] = None
    is_to_oos_sharpe_decay_max: Optional[float] = None
    min_oos_trades: Optional[int] = None
    require_positive_oos_total_return: Optional[bool] = None
    on_review_required: tuple[OnReviewRequiredRule, ...] = ()


class MetricsCatalog(BaseModel):
    """Metric catalog required for IS/OOS comparison."""

    model_config = ConfigDict(frozen=True)

    required: tuple[str, ...] = (
        "total_return",
        "cagr",
        "sharpe_ratio",
        "max_drawdown",
        "volatility",
        "num_trades",
    )
    recommended: tuple[str, ...] = ("sortino_ratio", "win_rate", "turnover")


class ExecutionSpec(BaseModel):
    """Execution configuration for the validation run."""

    model_config = ConfigDict(frozen=True)

    on_child_failure: Literal["fail_fast", "continue"] = "fail_fast"


class ReportingSpec(BaseModel):
    """Reporting configuration for the validation run."""

    model_config = ConfigDict(frozen=True)

    html: bool = True
    console_summary: bool = True


# ---------------------------------------------------------------------------
# Root model
# ---------------------------------------------------------------------------


class ValidationPlan(BaseModel):
    """Root model representing a complete, validated validation plan.

    Loaded from YAML via :func:`load_validation_plan`.
    Immutable after construction (``frozen=True``).
    """

    model_config = ConfigDict(frozen=True)

    validation_id: str
    strategy_experiment: str
    base_config: Path  # resolved to an absolute path by the loader
    mode: Literal["static_is_oos"]
    splits: StaticSplitSpec
    holdout: Optional[HoldoutSpec] = None
    benchmark: Optional[BenchmarkRef] = None
    metrics: MetricsCatalog = MetricsCatalog()
    decision: DecisionRulesSpec = DecisionRulesSpec()
    execution: ExecutionSpec = ExecutionSpec()
    reporting: ReportingSpec = ReportingSpec()

    @model_validator(mode="after")
    def validate_decision_rule_names(self) -> "ValidationPlan":
        """Reject unknown rule names in decision.on_review_required.

        Only review/warn rule names (``KNOWN_REVIEW_RULES``) are accepted here;
        pass/fail rule names belong under ``decision.rules``.
        """
        for rule in self.decision.on_review_required:
            if rule.rule not in KNOWN_REVIEW_RULES:
                raise ValueError(
                    f"Unknown rule '{rule.rule}' in decision.on_review_required. "
                    f"Accepted review rule names: {sorted(KNOWN_REVIEW_RULES)}"
                )
        return self


# ---------------------------------------------------------------------------
# YAML preprocessing
# ---------------------------------------------------------------------------


def _normalize_raw_plan(raw: dict[str, Any]) -> dict[str, Any]:
    """Normalize raw YAML dict to match the Pydantic model shape.

    Handles two YAML conveniences:
    - Flattening a ``decision.rules`` sub-key directly into ``decision``.
    - Converting ``on_review_required`` items from the compact
      ``{rule_name: threshold}`` mapping to ``{rule: rule_name, threshold: threshold}``.

    Args:
        raw: Raw dict produced by ``yaml.safe_load``.

    Returns:
        Normalized dict ready for ``ValidationPlan(**normalized)``.
    """
    if "decision" not in raw:
        return raw

    decision: dict[str, Any] = dict(raw["decision"])

    # Flatten decision.rules into the decision dict
    if "rules" in decision:
        rules = decision.pop("rules")
        if not isinstance(rules, dict):
            raise ValueError(f"decision.rules must be a mapping, got {type(rules).__name__}")
        decision.update(rules)

    # Normalize on_review_required from [{name: value}] to [{rule: name, threshold: value}]
    if "on_review_required" in decision:
        normalized_rules: list[dict[str, Any]] = []
        for item in decision["on_review_required"]:
            if isinstance(item, dict) and len(item) == 1:
                rule_name, threshold = next(iter(item.items()))
                normalized_rules.append({"rule": rule_name, "threshold": threshold})
            else:
                # Already in normalized {rule: ..., threshold: ...} form
                normalized_rules.append(item)
        decision["on_review_required"] = normalized_rules

    return {**raw, "decision": decision}


# ---------------------------------------------------------------------------
# YAML loader
# ---------------------------------------------------------------------------


def load_validation_plan(path: Path) -> ValidationPlan:
    """Load a ValidationPlan from a YAML file or a directory containing <name>.yaml.

    Resolves ``base_config`` relative to the plan file's parent directory.
    Returns a frozen, validated ``ValidationPlan`` with ``base_config`` as an
    absolute :class:`~pathlib.Path`.

    Args:
        path: Path to a validation plan YAML file, or a directory containing
              a YAML file named ``<directory_name>.yaml``.

    Returns:
        A frozen, validated :class:`ValidationPlan`.

    Raises:
        ValueError: If the plan file is missing, contains invalid YAML, or
                    fails model validation.
    """
    path = Path(path).resolve()

    # Directory form: look for <dir_name>.yaml inside the directory
    if path.is_dir():
        yaml_path = path / f"{path.name}.yaml"
        if not yaml_path.exists():
            raise ValueError(
                f"Validation plan directory '{path}' must contain '{path.name}.yaml'. File not found: {yaml_path}"
            )
        path = yaml_path

    if not path.exists():
        raise ValueError(f"Validation plan file not found: {path}")

    if not path.is_file():
        raise ValueError(f"Validation plan path is not a file: {path}")

    try:
        with path.open() as f:
            raw: Any = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML in validation plan '{path}': {exc}") from exc

    if not isinstance(raw, dict):
        raise ValueError(f"Validation plan must be a YAML mapping; got {type(raw).__name__}: {path}")

    # Normalize YAML structure (flatten decision.rules, convert on_review_required)
    raw = _normalize_raw_plan(raw)

    # Resolve base_config relative to the plan file's parent directory
    if "base_config" in raw:
        base_config_raw = Path(str(raw["base_config"]))
        if not base_config_raw.is_absolute():
            base_config_raw = (path.parent / base_config_raw).resolve()
        raw = {**raw, "base_config": base_config_raw}

    try:
        return ValidationPlan(**raw)
    except Exception as exc:
        raise ValueError(f"Validation plan validation failed in '{path}': {exc}") from exc


# ---------------------------------------------------------------------------
# Plan hash
# ---------------------------------------------------------------------------


def _plan_to_canonical_dict(plan: ValidationPlan) -> dict[str, Any]:
    """Convert a ValidationPlan to a canonical, hashable dict.

    Strips volatile fields (absolute filesystem paths are replaced with their
    base filename only) so that the hash is portable across machines.
    """
    d: dict[str, Any] = plan.model_dump(mode="json")
    # Replace the absolute path with its basename to avoid machine-specific variance
    d["base_config"] = Path(str(d["base_config"])).name
    return d


def _base_config_to_canonical_dict(config: Any) -> dict[str, Any]:
    """Convert a BacktestConfig to a canonical, hashable dict.

    Strips volatile per-execution metadata fields that differ across runs but
    do not affect the logical identity of the validated strategy.
    """
    d: dict[str, Any] = config.model_dump(mode="json")
    for key in ("run_id", "job_group_id", "submission_source"):
        d.pop(key, None)
    return d


def compute_plan_sha256(plan: ValidationPlan, base_config_path: Path) -> str:
    """Compute a stable SHA-256 hash over a ValidationPlan and its base BacktestConfig.

    Hash inputs (concatenated):
    1. Canonical JSON of the ValidationPlan with absolute paths normalised.
    2. Canonical JSON of the referenced BacktestConfig with per-execution
       metadata stripped (``run_id``, ``job_group_id``, ``submission_source``).

    Both JSON strings use ``sort_keys=True`` for determinism.

    Args:
        plan: The loaded and validated :class:`ValidationPlan`.
        base_config_path: Resolved absolute path to the base ``BacktestConfig``
                          YAML file.

    Returns:
        Lowercase 64-character hex SHA-256 string.

    Raises:
        ConfigLoadError: If the base config YAML cannot be loaded.
    """
    from qs_trader.engine.config import load_backtest_config  # lazy import

    base_config = load_backtest_config(base_config_path)

    plan_dict = _plan_to_canonical_dict(plan)
    base_dict = _base_config_to_canonical_dict(base_config)

    plan_json = json.dumps(plan_dict, sort_keys=True, default=str)
    base_json = json.dumps(base_dict, sort_keys=True, default=str)

    combined = plan_json + base_json
    return hashlib.sha256(combined.encode()).hexdigest()
