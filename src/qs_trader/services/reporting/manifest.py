"""ClickHouse input manifest for operational-store-backed backtest runs.

The manifest is a lightweight, immutable JSON record stored in the nullable
``input_manifest_json`` column on the operational ``runs`` table. It describes
the canonical ClickHouse data that a run **consumed**, not what it produced.

Architectural boundary
-----------------------
- **Operational store** stores what the run *produced*: ``runs``, ``equity_curve``,
  ``returns``, ``trades``, ``drawdowns``.
- **ClickHouse** stores what the run *consumed*: canonical market bars,
  precomputed features, regime context.

Only canonical ClickHouse-backed runs carry a manifest. Yahoo/CSV runs
leave the ``input_manifest_json`` column ``NULL``. This avoids duplicating
large market-data payloads inside the operational store during parameter sweeps while
keeping the full provenance of every canonical run auditable.

Schema versioning
-----------------
Every serialised manifest carries a ``schema_version`` integer.  Consumers must
inspect this field before processing so that schema drift is observable rather
than implicit.

- Version ``1``: Base manifest with symbol list (legacy, deprecated).
- Version ``2``: Adds ``resolved_instruments`` with full secid identity metadata.

When a breaking field change is needed, bump ``schema_version`` and add a
discriminated-union reader path.

Usage
-----
Create a manifest when a canonical ClickHouse-backed run is set up::

    from datetime import date

    manifest = ClickHouseInputManifestV2(
        source_name="qs-datamaster",
        database="market_data",
        bars_table="equity_daily",
        symbols=["AAPL", "MSFT"],
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        price_basis=PriceBasis.ADJUSTED,
        feature_set_version="v1",
        resolved_instruments=[...],
    )

ISO-8601 strings (``"YYYY-MM-DD"``) are also accepted and coerced to
:class:`datetime.date` by Pydantic::

    manifest = ClickHouseInputManifestV2(
        ...
        start_date="2023-01-01",
        end_date="2023-12-31",
        ...
    )

Serialise for database storage::

    json_str = manifest.to_json()           # compact JSON string
    roundtripped = read_manifest(json_str)

Read any manifest version (auto-detects schema_version)::

    manifest = read_manifest(json_str)
    # Returns ClickHouseInputManifestV1 or ClickHouseInputManifestV2
"""

from __future__ import annotations

import re
import warnings
from datetime import date
from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from qs_trader.events.price_basis import PriceBasis

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


class UnsafeManifestIdentifierError(ValueError):
    """Raised when a manifest database/table identifier is unsafe for SQL."""


def assert_safe_manifest_identifier(name: str, field: str) -> None:
    """Reject manifest identifiers containing characters unsafe for SQL identifiers."""
    if not _SAFE_IDENTIFIER_RE.match(name):
        raise UnsafeManifestIdentifierError(f"Manifest field '{field}' contains invalid characters: {name!r}")


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _validate_price_basis(v: PriceBasis | str) -> PriceBasis:
    """Validate the manifest price-basis contract."""
    return PriceBasis.coerce(v)


def _reject_legacy_adjustment_fields(data: object) -> object:
    """Fail fast on the removed manifest adjustment-mode fields."""
    if not isinstance(data, dict):
        return data

    legacy_keys = [
        key
        for key in ("adjustment_mode", "strategy_adjustment_mode", "portfolio_adjustment_mode")
        if data.get(key) is not None
    ]
    if legacy_keys:
        joined = ", ".join(legacy_keys)
        raise ValueError(
            f"Legacy manifest adjustment-mode fields are no longer supported ({joined}). "
            "Use 'price_basis' with 'raw' or 'adjusted'."
        )
    return data


# ---------------------------------------------------------------------------
# Ticker history entry (used in resolved instruments)
# ---------------------------------------------------------------------------


class TickerHistoryEntry(BaseModel):
    """Historical ticker usage for a secid within a resolved instrument."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    ticker: str
    start_date: date
    end_date: date | None = None


# ---------------------------------------------------------------------------
# Resolved instrument entry (v2 only)
# ---------------------------------------------------------------------------


class ResolvedInstrumentEntry(BaseModel):
    """Resolved instrument metadata for a single universe member.

    Captures the full identity resolution result so that downstream consumers
    can trace every bar, signal, and fill back to a stable secid.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    runtime_symbol: str
    requested_symbol: str
    secid: int | None = None
    display_symbol: str | None = None
    first_date: date | None = None
    last_date: date | None = None
    ticker_history: list[TickerHistoryEntry] = Field(default_factory=list)
    resolution: dict[str, Any] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Manifest v1 (legacy, deprecated)
# ---------------------------------------------------------------------------


class ClickHouseInputManifestV1(BaseModel):
    """Legacy manifest (schema_version 1) — symbol list without secid identity.

    This version is **deprecated**.  New runs should use
    :class:`ClickHouseInputManifestV2`.  Reading a v1 manifest emits a
    deprecation warning.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[1] = 1
    source_kind: Literal["clickhouse"] = "clickhouse"
    source_name: str
    database: str
    features_database: str | None = None
    bars_table: str
    features_table: str | None = None
    regime_table: str | None = None
    symbols: tuple[str, ...]
    start_date: date
    end_date: date
    price_basis: PriceBasis = PriceBasis.ADJUSTED
    feature_set_version: str | None = None
    regime_version: str | None = None
    feature_columns: tuple[str, ...] | None = None

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_adjustment_fields(cls, data: object) -> object:
        """Fail fast on the removed manifest adjustment-mode fields."""
        return _reject_legacy_adjustment_fields(data)

    @field_validator("price_basis", mode="before")
    @classmethod
    def validate_price_basis(cls, v: PriceBasis | str) -> PriceBasis:
        """Validate the manifest price-basis contract."""
        return _validate_price_basis(v)

    @field_validator("symbols")
    @classmethod
    def symbols_not_empty(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        """Require at least one symbol in the universe."""
        if not v:
            raise ValueError("symbols must contain at least one ticker symbol")
        return v

    @model_validator(mode="after")
    def end_date_not_before_start_date(self) -> ClickHouseInputManifestV1:
        """Require end_date to be on or after start_date."""
        if self.end_date < self.start_date:
            raise ValueError(f"end_date ({self.end_date}) must not be before start_date ({self.start_date})")
        return self

    def to_json(self) -> str:
        """Serialise the manifest to a compact JSON string."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str) -> ClickHouseInputManifestV1:
        """Deserialise a v1 manifest from a JSON string."""
        return cls.model_validate_json(raw)

    def migrate_to_v2(self) -> ClickHouseInputManifestV2:
        """Migrate this v1 manifest to v2.

        All resolved_instruments are created with ``secid=None`` (unresolved)
        since v1 does not carry identity metadata.
        """
        resolved = [
            ResolvedInstrumentEntry(
                runtime_symbol=sym,
                requested_symbol=sym,
                secid=None,
                display_symbol=None,
                first_date=None,
                last_date=None,
                ticker_history=[],
                resolution={"status": "unresolved", "reason": "migrated_from_v1"},
            )
            for sym in self.symbols
        ]

        return ClickHouseInputManifestV2(
            source_name=self.source_name,
            database=self.database,
            features_database=self.features_database,
            bars_table=self.bars_table,
            features_table=self.features_table,
            regime_table=self.regime_table,
            symbols=self.symbols,
            start_date=self.start_date,
            end_date=self.end_date,
            price_basis=self.price_basis,
            feature_set_version=self.feature_set_version,
            regime_version=self.regime_version,
            feature_columns=self.feature_columns,
            resolved_instruments=tuple(resolved),
        )


# ---------------------------------------------------------------------------
# Manifest v2 (current)
# ---------------------------------------------------------------------------


class ClickHouseInputManifestV2(BaseModel):
    """Current manifest (schema_version 2) — includes resolved_instruments.

    Extends v1 with a ``resolved_instruments`` array that captures full
    secid identity metadata for every universe member, enabling stable
    audit trails across ticker changes.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal[2] = 2
    source_kind: Literal["clickhouse"] = "clickhouse"
    source_name: str
    database: str
    features_database: str | None = None
    bars_table: str
    features_table: str | None = None
    regime_table: str | None = None
    symbols: tuple[str, ...]
    start_date: date
    end_date: date
    price_basis: PriceBasis = PriceBasis.ADJUSTED
    feature_set_version: str | None = None
    regime_version: str | None = None
    feature_columns: tuple[str, ...] | None = None
    resolved_instruments: tuple[ResolvedInstrumentEntry, ...] = Field(default_factory=tuple)

    @model_validator(mode="before")
    @classmethod
    def reject_legacy_adjustment_fields(cls, data: object) -> object:
        """Fail fast on the removed manifest adjustment-mode fields."""
        return _reject_legacy_adjustment_fields(data)

    @field_validator("price_basis", mode="before")
    @classmethod
    def validate_price_basis(cls, v: PriceBasis | str) -> PriceBasis:
        """Validate the manifest price-basis contract."""
        return _validate_price_basis(v)

    @field_validator("symbols")
    @classmethod
    def symbols_not_empty(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        """Require at least one symbol in the universe."""
        if not v:
            raise ValueError("symbols must contain at least one ticker symbol")
        return v

    @model_validator(mode="after")
    def end_date_not_before_start_date(self) -> ClickHouseInputManifestV2:
        """Require end_date to be on or after start_date."""
        if self.end_date < self.start_date:
            raise ValueError(f"end_date ({self.end_date}) must not be before start_date ({self.start_date})")
        return self

    def to_json(self) -> str:
        """Serialise the manifest to a compact JSON string."""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str) -> ClickHouseInputManifestV2:
        """Deserialise a v2 manifest from a JSON string."""
        return cls.model_validate_json(raw)


# ---------------------------------------------------------------------------
# Versioned reader (discriminated union)
# ---------------------------------------------------------------------------

# Type alias for the union of all manifest versions.
ClickHouseInputManifest = Union[ClickHouseInputManifestV1, ClickHouseInputManifestV2]


def read_manifest(raw: str) -> ClickHouseInputManifest:
    """Deserialise a manifest from JSON, auto-detecting the schema version.

    Args:
        raw: A JSON string previously produced by :meth:`to_json`.

    Returns:
        A validated manifest instance (v1 or v2).

    Warns:
        DeprecationWarning: When a v1 manifest is read.

    Raises:
        pydantic.ValidationError: If *raw* is not valid manifest JSON.
        ValueError: If the schema_version is unknown.
    """
    # Peek at schema_version without full validation
    import json

    data = json.loads(raw)
    version = data.get("schema_version", 1)

    if version == 1:
        warnings.warn(
            "ClickHouseInputManifest v1 is deprecated. "
            "Migrate to v2 using manifest.migrate_to_v2() or create new runs with v2.",
            DeprecationWarning,
            stacklevel=2,
        )
        return ClickHouseInputManifestV1.from_json(raw)

    if version == 2:
        return ClickHouseInputManifestV2.from_json(raw)

    raise ValueError(f"Unknown manifest schema_version: {version}")


# ---------------------------------------------------------------------------
# Backwards-compatible alias (points to v2 for new code)
# ---------------------------------------------------------------------------

# For code that was written against the old single-class API, provide a
# default alias that creates v2 manifests.  Existing code that explicitly
# imports ClickHouseInputManifestV1 or ClickHouseInputManifestV2 continues
# to work unchanged.
ClickHouseInputManifestLatest = ClickHouseInputManifestV2
