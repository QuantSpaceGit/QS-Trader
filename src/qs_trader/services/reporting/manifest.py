"""ClickHouse input manifest for DuckDB-backed backtest runs.

The manifest is a lightweight, immutable JSON record stored in the nullable
``input_manifest_json`` column on the DuckDB ``runs`` table. It describes
the canonical ClickHouse data that a run **consumed**, not what it produced.

Architectural boundary
-----------------------
- **DuckDB** stores what the run *produced*: ``runs``, ``equity_curve``,
  ``returns``, ``trades``, ``drawdowns``.
- **ClickHouse** stores what the run *consumed*: canonical market bars,
  precomputed features, regime context.

Only canonical ClickHouse-backed runs carry a manifest. Yahoo/CSV runs
leave the ``input_manifest_json`` column ``NULL``. This avoids duplicating
large market-data payloads inside DuckDB during parameter sweeps while
keeping the full provenance of every canonical run auditable.

Schema versioning
-----------------
Every serialised manifest carries a ``schema_version`` integer (currently
``1``).  Consumers must inspect this field before processing so that schema
drift is observable rather than implicit.  When a breaking field change is
needed, bump ``schema_version`` and add a discriminated-union reader path.

Usage
-----
Create a manifest when a canonical ClickHouse-backed run is set up::

    from datetime import date

    manifest = ClickHouseInputManifest(
        source_name="qs-datamaster",
        database="market_data",
        bars_table="equity_daily",
        symbols=["AAPL", "MSFT"],
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        strategy_adjustment_mode="split_adjusted",
        portfolio_adjustment_mode="split_adjusted",
        feature_set_version="v1",
    )

ISO-8601 strings (``"YYYY-MM-DD"``) are also accepted and coerced to
:class:`datetime.date` by Pydantic::

    manifest = ClickHouseInputManifest(
        ...
        start_date="2023-01-01",
        end_date="2023-12-31",
        ...
    )

Serialise for DuckDB storage::

    json_str = manifest.to_json()           # compact JSON string
    roundtripped = ClickHouseInputManifest.from_json(json_str)
"""

from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator, model_validator

AdjustmentMode = Literal["split_adjusted", "total_return"]


class ClickHouseInputManifest(BaseModel):
    """Lightweight description of canonical ClickHouse inputs consumed by a run.

    All instances are immutable (``frozen=True``) and reject unknown fields
    (``extra="forbid"``).  The latter ensures that typos and undocumented
    fields are surfaced at construction / deserialisation time rather than
    silently discarded, which is critical for a long-lived persisted contract.

    Attributes:
        schema_version: Monotonically increasing integer that identifies the
            manifest schema in use.  Current version is ``1``.  Consumers must
            branch on this value before processing to handle future schema
            evolution.
        source_kind: Discriminator for the input source type. Always
            ``"clickhouse"`` for this manifest variant; reserved for future
            extensibility.
        source_name: Logical name of the upstream data source
            (e.g. ``"qs-datamaster"``).
        database: ClickHouse database that hosts the OHLCV bar table
            (``bars_table``).  This may differ from ``features_database`` when
            the deployment uses separate ClickHouse databases for market data
            and precomputed features.
        features_database: ClickHouse database that hosts the feature and
            regime tables (``features_table`` / ``regime_table``), or ``None``
            when no feature service was active.  Downstream consumers must use
            this value rather than ``database`` when re-resolving feature rows.
        bars_table: Name of the OHLCV / price-bar table inside ``database``.
        features_table: Name of the precomputed-features table inside
            ``features_database``, or ``None`` when no feature service was
            active during the run.
        regime_table: Name of the regime-context table inside
            ``features_database``, or ``None`` when regime data was not
            consumed.
        symbols: Ordered tuple of ticker symbols that formed the run universe.
            Must contain at least one symbol.
        start_date: First bar date consumed by the run (inclusive).
            Accepts a :class:`datetime.date` or an ISO-8601 string
            (``"YYYY-MM-DD"``).
        end_date: Last bar date consumed by the run (inclusive).
            Must be on or after *start_date*.  Accepts a
            :class:`datetime.date` or an ISO-8601 string.
        adjustment_mode: Legacy single adjustment-mode field retained only so
            historical schema-version-1 manifests still deserialize. New
            manifests should leave this unset and use the explicit strategy /
            portfolio fields below.
        strategy_adjustment_mode: Price-adjustment convention used by the
            strategy / indicator layer (``"split_adjusted"`` or
            ``"total_return"``).
        portfolio_adjustment_mode: Price-adjustment convention used by the
            manager / execution / portfolio / reporting layer.
        feature_set_version: Feature-set schema version string (e.g. ``"v1"``),
            or ``None`` when no feature service was active.
        regime_version: Regime-context schema version string, or ``None``
            when regime data was not consumed.
        feature_columns: Explicit immutable subset of feature columns that were
            requested from the feature service, or ``None`` when all available
            columns were consumed.
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
    adjustment_mode: AdjustmentMode | None = None
    strategy_adjustment_mode: AdjustmentMode | None = None
    portfolio_adjustment_mode: AdjustmentMode | None = None
    feature_set_version: str | None = None
    regime_version: str | None = None
    feature_columns: tuple[str, ...] | None = None

    @field_validator("symbols")
    @classmethod
    def symbols_not_empty(cls, v: tuple[str, ...]) -> tuple[str, ...]:
        """Require at least one symbol in the universe.

        Args:
            v: The candidate symbols list.

        Returns:
            The validated list unchanged.

        Raises:
            ValueError: If the list is empty.
        """
        if not v:
            raise ValueError("symbols must contain at least one ticker symbol")
        return v

    @model_validator(mode="after")
    def adjustment_modes_are_paired(self) -> ClickHouseInputManifest:
        """Require paired strategy/portfolio adjustment modes when either is set.

        Raises:
            ValueError: If only one of the explicit adjustment-mode fields is set.
        """
        explicit_modes = {
            "strategy_adjustment_mode": self.strategy_adjustment_mode,
            "portfolio_adjustment_mode": self.portfolio_adjustment_mode,
        }
        populated = [name for name, value in explicit_modes.items() if value is not None]
        if populated and len(populated) != len(explicit_modes):
            raise ValueError(
                "strategy_adjustment_mode and portfolio_adjustment_mode must both be provided when either is set"
            )
        return self

    @model_validator(mode="after")
    def end_date_not_before_start_date(self) -> ClickHouseInputManifest:
        """Require end_date to be on or after start_date.

        Raises:
            ValueError: If end_date precedes start_date.
        """
        if self.end_date < self.start_date:
            raise ValueError(f"end_date ({self.end_date}) must not be before start_date ({self.start_date})")
        return self

    def to_json(self) -> str:
        """Serialise the manifest to a compact JSON string.

        Dates are serialised as ISO-8601 strings (``"YYYY-MM-DD"``).
        The resulting string is suitable for storage in a ``VARCHAR`` column.
        Field ordering is deterministic (Pydantic model field order).

        Returns:
            A UTF-8 JSON string representing this manifest.
        """
        return self.model_dump_json()

    @classmethod
    def from_json(cls, raw: str) -> ClickHouseInputManifest:
        """Deserialise a manifest from a JSON string.

        Args:
            raw: A JSON string previously produced by :meth:`to_json` or any
                compatible serialiser that honours the model schema.

        Returns:
            A fully validated :class:`ClickHouseInputManifest` instance.

        Raises:
            pydantic.ValidationError: If *raw* is not valid manifest JSON,
                violates the model schema (e.g. empty symbols list, end_date
                before start_date), or contains unknown fields.  Unknown fields
                raise so that schema drift is surfaced early rather than
                silently discarded.
        """
        return cls.model_validate_json(raw)
