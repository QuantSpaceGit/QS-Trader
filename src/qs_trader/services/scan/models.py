"""Scan mode models: context and decision types.

Provides structured dataclasses for candidate scan rule evaluation.
Rules receive a ``ScanRuleContext`` and return a ``ScanDecision``.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from typing import Any, Sequence

# ---------------------------------------------------------------------------
# Allowed candidate statuses
# ---------------------------------------------------------------------------

VALID_STATUSES = frozenset({"accepted", "rejected", "ignored", "not_ready"})

# ---------------------------------------------------------------------------
# Scan rule context
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScanRuleContext:
    """Immutable context passed to a candidate scan rule for a single bar.

    Attributes:
        secid: Resolved security identifier.
        display_symbol: Display-friendly symbol (e.g. "BRK.B").
        ticker_at_date: Ticker symbol valid at the evaluation date.
        identity_source: How the identity was resolved (e.g. "secid", "ticker").
        runtime_symbol: Symbol used at runtime for data loading.
        date: Current evaluation date (ISO string).
        bar_index: Index of the current bar within the loaded arrays.
        dates: Full date array for the loaded range.
        open: Open price array (aligned with dates).
        high: High price array (aligned with dates).
        low: Low price array (aligned with dates).
        close: Close price array (aligned with dates).
        volume: Volume array (aligned with dates).
        features: Feature values for the current bar.
        feature_columns: Names of loaded feature columns.
        data_source: Data source identifier (e.g. "qs-datamaster").
        price_basis: Price basis used (e.g. "adjusted_ohlc_adj_columns").
        parameters: Rule parameter snapshot (mutable dict, not frozen).
        parameter_hash: Deterministic hash of the parameter snapshot.
    """

    secid: int | None
    display_symbol: str
    ticker_at_date: str
    identity_source: str
    runtime_symbol: str
    date: str
    bar_index: int
    dates: Sequence[Any]
    open: Sequence[float]
    high: Sequence[float]
    low: Sequence[float]
    close: Sequence[float]
    volume: Sequence[float]
    features: dict[str, Any] = field(default_factory=dict)
    feature_columns: list[str] = field(default_factory=list)
    data_source: str = ""
    price_basis: str = ""
    parameters: dict[str, Any] = field(default_factory=dict)
    parameter_hash: str = ""


# ---------------------------------------------------------------------------
# Scan decision
# ---------------------------------------------------------------------------


@dataclass
class ScanDecision:
    """Structured decision returned by a candidate scan rule.

    Attributes:
        candidate_status: One of accepted, rejected, ignored, not_ready.
        reason_code: Machine-readable reason for the decision.
        score: Optional numeric score assigned by the rule.
        gates: Named gate results (e.g. {"volume_gate": True}).
        diagnostics: Numeric or contextual values explaining the decision.
        features: Feature values used by the rule at evaluation time.
    """

    candidate_status: str
    reason_code: str
    score: float | None = None
    gates: dict[str, Any] = field(default_factory=dict)
    diagnostics: dict[str, Any] = field(default_factory=dict)
    features: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.candidate_status not in VALID_STATUSES:
            raise ValueError(
                f"Unsupported candidate_status: {self.candidate_status!r}. Must be one of {sorted(VALID_STATUSES)}."
            )


# ---------------------------------------------------------------------------
# Tuple compatibility adapter
# ---------------------------------------------------------------------------

# Legacy status mapping: old scanner used "candidate" for what is now "accepted"
_LEGACY_STATUS_MAP: dict[str, str] = {
    "candidate": "accepted",
}


def decision_from_tuple(
    result: tuple[str, str, float | None, dict[str, Any], dict[str, Any]],
) -> ScanDecision:
    """Convert a legacy tuple rule result into a ScanDecision.

    Expected tuple shape:
        (status, reason_code, score, gates, features)

    The legacy status ``"candidate"`` is mapped to ``"accepted"``.
    Diagnostics are set to an empty dict since legacy tuples do not
    provide them.

    Args:
        result: A 5-tuple from a legacy rule.

    Returns:
        A ScanDecision with adapted values.

    Raises:
        ValueError: If the tuple is not a 5-tuple or has an unmappable status.
    """
    if not isinstance(result, tuple) or len(result) != 5:
        raise ValueError(
            f"Expected a 5-tuple (status, reason_code, score, gates, features), "
            f"got {type(result).__name__} of length {len(result) if isinstance(result, tuple) else 'N/A'}."
        )

    status, reason_code, score, gates, features = result

    # Map legacy status
    adapted_status = _LEGACY_STATUS_MAP.get(status, status)
    if adapted_status not in VALID_STATUSES:
        raise ValueError(
            f"Unsupported status from tuple rule: {status!r} "
            f"(mapped to {adapted_status!r}). Must be one of {sorted(VALID_STATUSES)}."
        )

    return ScanDecision(
        candidate_status=adapted_status,
        reason_code=reason_code or "",
        score=score,
        gates=gates or {},
        diagnostics={},
        features=features or {},
    )


# ---------------------------------------------------------------------------
# Parameter hashing
# ---------------------------------------------------------------------------


def canonicalize_parameters(parameters: dict[str, Any] | None) -> str:
    """Return a canonical JSON string for a parameter dict with sorted keys.

    Args:
        parameters: Parameter dictionary (may be None or empty).

    Returns:
        Canonical JSON string with sorted keys and no extra whitespace.
    """
    if not parameters:
        return "{}"
    return json.dumps(parameters, sort_keys=True, separators=(",", ":"))


def hash_parameters(parameters: dict[str, Any] | None) -> str:
    """Compute a deterministic SHA-256 hash for a parameter snapshot.

    The hash is computed over the canonical JSON representation so that
    dictionaries with the same content but different key order produce
    the same hash.

    Args:
        parameters: Parameter dictionary (may be None or empty).

    Returns:
        Hex-encoded SHA-256 hash string.
    """
    canonical = canonicalize_parameters(parameters)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Feature column validation
# ---------------------------------------------------------------------------

_FEATURE_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_feature_column(name: str) -> str:
    """Validate a feature column name for safe SQL construction.

    Identifiers must start with a letter or underscore, matching the
    canonical ClickHouse adapter pattern ``^[A-Za-z_][A-Za-z0-9_]*$``.

    Args:
        name: The feature column name to validate.

    Returns:
        The validated name (unchanged).

    Raises:
        ValueError: If the name is empty or contains invalid characters.
    """
    if not name:
        raise ValueError("Feature column name must not be empty.")
    if not _FEATURE_COLUMN_RE.match(name):
        raise ValueError(
            f"Invalid feature column name: {name!r}. "
            "Identifiers must start with a letter or underscore and contain "
            "only letters, numbers, and underscores."
        )
    return name


def validate_feature_columns(names: list[str] | None) -> list[str]:
    """Validate a list of feature column names.

    Args:
        names: List of feature column names (may be None).

    Returns:
        Validated list of feature column names.

    Raises:
        ValueError: If any name is invalid.
    """
    if not names:
        return []
    return [validate_feature_column(n) for n in names]


# ---------------------------------------------------------------------------
# Price basis mapping
# ---------------------------------------------------------------------------

# Maps price_basis names to ClickHouse column names for the SELECT list.
# The canonical basis uses the adjusted columns from the backtest adapter.
PRICE_BASIS_COLUMNS: dict[str, dict[str, str]] = {
    "adjusted_ohlc_adj_columns": {
        "open": "openadj",
        "high": "highadj",
        "low": "lowadj",
        "close": "closeadj",
        "volume": "dailyvolumeadj",
    },
    "raw": {
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "dailyvolume",
    },
}

DEFAULT_PRICE_BASIS = "adjusted_ohlc_adj_columns"


def resolve_price_basis(price_basis: str | None) -> tuple[str, dict[str, str]]:
    """Resolve a price basis name to its column mapping.

    Args:
        price_basis: Price basis name (e.g. "adjusted_ohlc_adj_columns").
            Defaults to ``DEFAULT_PRICE_BASIS`` when None.

    Returns:
        Tuple of (resolved_basis_name, column_mapping).

    Raises:
        ValueError: If the price basis is not supported.
    """
    resolved = price_basis or DEFAULT_PRICE_BASIS
    if resolved not in PRICE_BASIS_COLUMNS:
        raise ValueError(f"Unsupported price basis: {resolved!r}. Supported: {sorted(PRICE_BASIS_COLUMNS.keys())}.")
    return resolved, PRICE_BASIS_COLUMNS[resolved]
