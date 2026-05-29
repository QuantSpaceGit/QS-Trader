"""Validation plan domain model, YAML loader, and plan hash computation."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal, Optional, Union

import yaml
from dateutil.relativedelta import relativedelta
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
# Duration-string parser (T1.1)
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^(\d+)(y|mo|d)$")


def parse_duration(s: str) -> relativedelta | timedelta:
    """Parse a duration string into a ``relativedelta`` or ``timedelta``.

    Accepted formats:
    - ``Ny``  — N years   → ``relativedelta(years=N)``
    - ``Nmo`` — N months  → ``relativedelta(months=N)``
    - ``Nd``  — N days    → ``timedelta(days=N)``

    Args:
        s: Duration string, e.g. ``"3y"``, ``"6mo"``, ``"30d"``.

    Returns:
        A :class:`~dateutil.relativedelta.relativedelta` for year/month durations
        or a :class:`~datetime.timedelta` for day durations.

    Raises:
        ValueError: If the string does not match any accepted format.
    """
    m = _DURATION_RE.match(s.strip())
    if not m:
        raise ValueError(
            f"Invalid duration string {s!r}. "
            "Expected format: Ny (years), Nmo (months), or Nd (days). "
            f"Examples: '3y', '6mo', '30d'."
        )
    n = int(m.group(1))
    unit = m.group(2)
    if unit == "y":
        return relativedelta(years=n)
    if unit == "mo":
        return relativedelta(months=n)
    # unit == "d"
    return timedelta(days=n)


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

    model_config = ConfigDict(frozen=True, extra="forbid")

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


class WalkForwardSplitsSpec(BaseModel):
    """Walk-forward split specification (anchored or rolling)."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    style: Literal["anchored", "rolling"]
    train: str
    test: str
    step: str
    embargo: str = "0d"
    total_range: DateRange
    min_fold_bars: Optional[int] = None

    @model_validator(mode="after")
    def validate_duration_strings(self) -> "WalkForwardSplitsSpec":
        """Validate all duration strings are parseable, sign-correct, and step >= test."""
        ref = date(2000, 1, 1)

        # Parse all four duration fields up front (parseability check) and require
        # train/test/step to be strictly positive; embargo may be zero but not negative.
        # A duration is zero iff (ref + duration) == ref; we use this since
        # relativedelta and timedelta do not share a common ``> 0`` comparison.
        parsed: dict[str, relativedelta | timedelta] = {}
        for field_name in ("train", "test", "step", "embargo"):
            raw = getattr(self, field_name)
            try:
                parsed[field_name] = parse_duration(raw)
            except ValueError as exc:
                raise ValueError(f"Invalid duration for field '{field_name}': {exc}") from exc

        for field_name in ("train", "test", "step"):
            if ref + parsed[field_name] <= ref:
                raise ValueError(
                    f"Field '{field_name}' must be a strictly positive duration; got {getattr(self, field_name)!r}."
                )
        if ref + parsed["embargo"] < ref:
            raise ValueError(f"Field 'embargo' must be a non-negative duration; got {self.embargo!r}.")

        if self.min_fold_bars is not None and self.min_fold_bars <= 0:
            raise ValueError(f"min_fold_bars must be strictly positive when set; got {self.min_fold_bars}.")

        # Enforce step >= test using a fixed reference date.
        # Known limitation (I2): mixing duration unit types (e.g., step="365d", test="1y") may
        # produce unexpected rejections because relativedelta and timedelta are compared via
        # calendar arithmetic from ref=date(2000, 1, 1). Use consistent units (all years,
        # all months, or all days) to avoid this edge case.
        step_end = ref + parsed["step"]
        test_end = ref + parsed["test"]
        if step_end < test_end:
            raise ValueError(
                f"step ({self.step!r}) must be >= test ({self.test!r}) duration; "
                f"computed step_end={step_end}, test_end={test_end} from reference {ref}"
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


_BENCHMARK_INSTRUMENT_RE = re.compile(r"^[A-Za-z0-9._-]+$")


class BenchmarkSpec(BaseModel):
    """Synthetic single-instrument benchmark overlay (Phase 2A.3).

    When declared on a :class:`ValidationPlan`, the validation runner executes
    one synthetic buy-and-hold child run over the plan's full validation range
    on the named instrument.  Fields per requirement doc §5.1:

    Attributes:
        instrument: Benchmark instrument symbol (filesystem- and registry-safe;
            must match ``^[A-Za-z0-9._-]+$``).
        strategy: Strategy registry name to use for the benchmark child run.
            Only ``"buy_and_hold"`` is accepted in Phase 2A.
        reinvest_dividends: When ``True`` (default) the benchmark strategy
            reinvests cash dividends.  No-op until the engine surfaces a
            dividend feed (tracked separately for Phase 2A.5).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument: str
    strategy: Literal["buy_and_hold"] = "buy_and_hold"
    reinvest_dividends: bool = True

    @model_validator(mode="after")
    def _validate_instrument(self) -> "BenchmarkSpec":
        if not self.instrument or not self.instrument.strip():
            raise ValueError("BenchmarkSpec.instrument must be a non-empty string")
        if not _BENCHMARK_INSTRUMENT_RE.match(self.instrument):
            raise ValueError(f"Invalid benchmark instrument {self.instrument!r}: must match ^[A-Za-z0-9._-]+$")
        return self


# Backward-compatible re-export name (Phase 1 stub never used by a real plan).
# The legacy ``BenchmarkRef`` shape (``symbol`` + ``data_source``) is removed in
# favour of the Phase 2A.3 ``BenchmarkSpec`` contract.
BenchmarkRef = BenchmarkSpec


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
# Cost-scenario spec (Phase 2A.2)
# ---------------------------------------------------------------------------


_SCENARIO_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class CostScenarioSpec(BaseModel):
    """A named bundle of dot-notation overrides applied to the base ``BacktestConfig``.

    ``overrides`` keys are dot-notation paths (e.g. ``"feature_config.feature_version"``)
    and are validated against the live ``BacktestConfig`` schema at plan-load time
    via :class:`ValidationPlan`'s model validator.

    Attributes:
        name: Filesystem-safe identifier matching ``^[A-Za-z0-9_-]+$``.  Used as
              the on-disk directory under ``scenarios/<name>/`` when more than
              one scenario is declared.
        overrides: Mapping from dot-notation path to the override value applied
                   on top of the base config for this scenario.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: str
    overrides: dict[str, Any] = {}

    @model_validator(mode="after")
    def validate_name(self) -> "CostScenarioSpec":
        if not _SCENARIO_NAME_RE.match(self.name):
            raise ValueError(f"Invalid cost-scenario name {self.name!r}: must match ^[A-Za-z0-9_-]+$")
        return self


# ---------------------------------------------------------------------------
# Root model
# ---------------------------------------------------------------------------


class ValidationPlan(BaseModel):
    """Root model representing a complete, validated validation plan.

    Loaded from YAML via :func:`load_validation_plan`.
    Immutable after construction (``frozen=True``).

    The ``splits`` field holds either a :class:`StaticSplitSpec` (when
    ``mode='static_is_oos'``) or a :class:`WalkForwardSplitsSpec` (when
    ``mode='walk_forward'``).  Use ``isinstance`` to narrow the type before
    accessing mode-specific attributes.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    validation_id: str
    strategy_experiment: str
    base_config: Path  # resolved to an absolute path by the loader
    description: Optional[str] = None  # human-readable plan description; ignored by the engine
    mode: Literal["static_is_oos", "walk_forward"]
    splits: Union[StaticSplitSpec, WalkForwardSplitsSpec]
    holdout: Optional[HoldoutSpec] = None
    benchmark: Optional[BenchmarkSpec] = None
    metrics: MetricsCatalog = MetricsCatalog()
    decision: DecisionRulesSpec = DecisionRulesSpec()
    execution: ExecutionSpec = ExecutionSpec()
    reporting: ReportingSpec = ReportingSpec()
    cost_scenarios: Optional[list[CostScenarioSpec]] = None

    @model_validator(mode="before")
    @classmethod
    def coerce_splits_by_mode(cls, data: Any) -> Any:
        """Coerce and validate the ``splits`` field based on the plan ``mode``.

        Rejects cross-contamination: ``static_is_oos`` plans cannot carry
        walk-forward fields, and ``walk_forward`` plans cannot carry static
        ``in_sample``/``out_of_sample`` fields.
        """
        if not isinstance(data, dict):
            return data
        mode = data.get("mode")
        splits_raw = data.get("splits")
        if not isinstance(splits_raw, dict):
            return data

        _WF_FIELDS = {"style", "train", "test", "step", "embargo", "total_range", "min_fold_bars"}
        _STATIC_FIELDS = {"in_sample", "out_of_sample"}

        if mode == "static_is_oos":
            extra = set(splits_raw.keys()) & _WF_FIELDS
            if extra:
                raise ValueError(f"Walk-forward fields {sorted(extra)} are not allowed in mode='static_is_oos' splits.")
            coerced: Union[StaticSplitSpec, WalkForwardSplitsSpec] = StaticSplitSpec(**splits_raw)
            return {**data, "splits": coerced}
        elif mode == "walk_forward":
            extra = set(splits_raw.keys()) & _STATIC_FIELDS
            if extra:
                raise ValueError(f"Static fields {sorted(extra)} are not allowed in mode='walk_forward' splits.")
            coerced = WalkForwardSplitsSpec(**splits_raw)
            return {**data, "splits": coerced}
        return data

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

    @model_validator(mode="after")
    def validate_cost_scenarios(self) -> "ValidationPlan":
        """Validate cost-scenario uniqueness and override paths against ``BacktestConfig``.

        - Scenario ``name`` values must be unique within the list.
        - Every override key must resolve to a valid dot-notation path on the
          live :class:`~qs_trader.engine.config.BacktestConfig` schema; unknown
          paths raise ``ValueError`` carrying the ``unknown_override_key:<path>``
          reason code so the CLI surfaces it consistently.
        """
        if self.cost_scenarios is None:
            return self

        # Uniqueness
        names = [s.name for s in self.cost_scenarios]
        seen: set[str] = set()
        duplicates: list[str] = []
        for n in names:
            if n in seen:
                duplicates.append(n)
            seen.add(n)
        if duplicates:
            raise ValueError(
                f"Duplicate cost-scenario name(s): {sorted(set(duplicates))}. "
                "Scenario names must be unique within a plan."
            )

        # Override path validation against the live BacktestConfig schema.
        # Lazy import to avoid module-import cycles (engine.config imports back into
        # validation in some test paths).
        from qs_trader.engine.config import BacktestConfig  # noqa: PLC0415
        from qs_trader.validation.cost_scenarios import validate_override_path  # noqa: PLC0415

        for scenario in self.cost_scenarios:
            for path in scenario.overrides:
                try:
                    validate_override_path(BacktestConfig, path)
                except ValueError as exc:
                    # Surface as a fresh ValueError so Pydantic wraps it into a
                    # ValidationError with the unknown_override_key:<path> reason.
                    raise ValueError(f"cost_scenarios[{scenario.name!r}].overrides: {exc}") from exc
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

    ``description`` is intentionally excluded: it is non-execution metadata
    (a human-readable label) and must not affect the plan hash.  Excluding it
    preserves backward compatibility — plans that predated the ``description``
    field continue to hash identically to their original values.
    """
    d: dict[str, Any] = plan.model_dump(mode="json")
    # Replace the absolute path with its basename to avoid machine-specific variance
    d["base_config"] = Path(str(d["base_config"])).name
    # Non-execution metadata: excluded to preserve hash stability across schema
    # extensions that add optional informational fields.
    d.pop("description", None)
    # Optional structural field: when not declared, drop it so plans authored
    # before Phase 2A.2 continue to hash identically (matches the description
    # exclusion pattern). When declared the field IS hashed (it changes
    # execution).
    if d.get("cost_scenarios") is None:
        d.pop("cost_scenarios", None)
    # NOTE: ``benchmark`` is intentionally NOT dropped when None. The Phase 1
    # canonical dict emitted ``"benchmark": null`` (the legacy ``BenchmarkRef``
    # field was already on the model), so removing it now would break the
    # 428e27b2 reference-plan hash pin. Plans that declare a benchmark will
    # serialize it normally and the hash will change accordingly.
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
