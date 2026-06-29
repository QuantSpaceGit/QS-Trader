"""Candidate scan service for secid-first research auditability.

Provides a dedicated execution path for scanning instruments against
candidate rules, computing forward returns, MFE, and MAE for research
and strategy development.

Exports:
    ScanRuleContext: Immutable context passed to candidate rules.
    ScanDecision: Structured decision returned by candidate rules.
    ScanRunner: Executes a candidate scan over a universe of instruments.
    ScanResult: Single scan result row.
    ScanSummary: Summary of a completed scan run.
    decision_from_tuple: Adapter for legacy tuple-return rules.
    canonicalize_parameters: Canonicalize parameter dicts for hashing.
    hash_parameters: Deterministic SHA-256 hash for parameter snapshots.
    validate_feature_column: Validate a single feature column name.
    validate_feature_columns: Validate a list of feature column names.
    resolve_price_basis: Resolve price basis to column mapping.
    DEFAULT_PRICE_BASIS: Default price basis name.
"""

from qs_trader.services.scan.calculations import (
    compute_scan_metrics,
    forward_return,
    mae,
    mfe,
)
from qs_trader.services.scan.models import (
    DEFAULT_PRICE_BASIS,
    PRICE_BASIS_COLUMNS,
    VALID_STATUSES,
    ScanDecision,
    ScanRuleContext,
    canonicalize_parameters,
    decision_from_tuple,
    hash_parameters,
    resolve_price_basis,
    validate_feature_column,
    validate_feature_columns,
)
from qs_trader.services.scan.runner import ScanResult, ScanRunner, ScanSummary

__all__ = [
    "DEFAULT_PRICE_BASIS",
    "PRICE_BASIS_COLUMNS",
    "ScanDecision",
    "ScanRuleContext",
    "ScanResult",
    "ScanRunner",
    "ScanSummary",
    "VALID_STATUSES",
    "canonicalize_parameters",
    "compute_scan_metrics",
    "decision_from_tuple",
    "forward_return",
    "hash_parameters",
    "mae",
    "mfe",
    "resolve_price_basis",
    "validate_feature_column",
    "validate_feature_columns",
]
