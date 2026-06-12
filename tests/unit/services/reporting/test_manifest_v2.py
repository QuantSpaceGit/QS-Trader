"""Unit tests for manifest v2 (Task Group 7).

Tests manifest v2 read/write, v1 backward compatibility, deprecation warning,
and v1-to-v2 migration.
"""

import json
import warnings
from datetime import date

import pytest

from qs_trader.events.price_basis import PriceBasis
from qs_trader.services.reporting.manifest import (
    ClickHouseInputManifestV1,
    ClickHouseInputManifestV2,
    ResolvedInstrumentEntry,
    TickerHistoryEntry,
    read_manifest,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _v1_kwargs() -> dict:
    return {
        "source_name": "qs-datamaster",
        "database": "market",
        "bars_table": "equity_daily",
        "symbols": ("AAPL", "MSFT"),
        "start_date": date(2023, 1, 1),
        "end_date": date(2023, 12, 31),
        "price_basis": PriceBasis.ADJUSTED,
    }


def _resolved_instruments() -> tuple[ResolvedInstrumentEntry, ...]:
    return (
        ResolvedInstrumentEntry(
            runtime_symbol="AAPL",
            requested_symbol="AAPL",
            secid=12345,
            display_symbol="AAPL",
            first_date=date(2023, 1, 1),
            last_date=date(2023, 12, 31),
            ticker_history=[
                TickerHistoryEntry(ticker="AAPL", start_date=date(1980, 12, 12)),
            ],
            resolution={"status": "resolved", "source": "secmaster"},
        ),
        ResolvedInstrumentEntry(
            runtime_symbol="MSFT",
            requested_symbol="MSFT",
            secid=67890,
            display_symbol="MSFT",
            first_date=date(2023, 1, 1),
            last_date=date(2023, 12, 31),
            ticker_history=[
                TickerHistoryEntry(ticker="MSFT", start_date=date(1986, 3, 13)),
            ],
            resolution={"status": "resolved", "source": "secmaster"},
        ),
    )


def _v2_kwargs() -> dict:
    return {
        **_v1_kwargs(),
        "resolved_instruments": _resolved_instruments(),
    }


# ---------------------------------------------------------------------------
# Manifest v1 tests
# ---------------------------------------------------------------------------


class TestManifestV1:
    """Tests for the legacy v1 manifest."""

    def test_v1_creates_with_required_fields(self) -> None:
        manifest = ClickHouseInputManifestV1(**_v1_kwargs())
        assert manifest.schema_version == 1
        assert manifest.symbols == ("AAPL", "MSFT")

    def test_v1_serializes_to_json(self) -> None:
        manifest = ClickHouseInputManifestV1(**_v1_kwargs())
        json_str = manifest.to_json()
        data = json.loads(json_str)
        assert data["schema_version"] == 1
        assert data["symbols"] == ["AAPL", "MSFT"]

    def test_v1_roundtrip(self) -> None:
        original = ClickHouseInputManifestV1(**_v1_kwargs())
        reconstructed = ClickHouseInputManifestV1.from_json(original.to_json())
        assert reconstructed.schema_version == 1
        assert reconstructed.symbols == original.symbols
        assert reconstructed.start_date == original.start_date

    def test_v1_rejects_empty_symbols(self) -> None:
        with pytest.raises(ValueError, match="symbols must contain at least one"):
            ClickHouseInputManifestV1(
                source_name="test",
                database="market",
                bars_table="bars",
                symbols=(),
                start_date=date(2023, 1, 1),
                end_date=date(2023, 12, 31),
            )

    def test_v1_rejects_end_before_start(self) -> None:
        with pytest.raises(ValueError, match="end_date.*must not be before"):
            ClickHouseInputManifestV1(
                source_name="test",
                database="market",
                bars_table="bars",
                symbols=("AAPL",),
                start_date=date(2023, 12, 31),
                end_date=date(2023, 1, 1),
            )


# ---------------------------------------------------------------------------
# Manifest v2 tests
# ---------------------------------------------------------------------------


class TestManifestV2:
    """Tests for the current v2 manifest."""

    def test_v2_creates_with_required_fields(self) -> None:
        manifest = ClickHouseInputManifestV2(**_v2_kwargs())
        assert manifest.schema_version == 2
        assert len(manifest.resolved_instruments) == 2

    def test_v2_serializes_to_json(self) -> None:
        manifest = ClickHouseInputManifestV2(**_v2_kwargs())
        json_str = manifest.to_json()
        data = json.loads(json_str)
        assert data["schema_version"] == 2
        assert len(data["resolved_instruments"]) == 2
        assert data["resolved_instruments"][0]["secid"] == 12345

    def test_v2_roundtrip(self) -> None:
        original = ClickHouseInputManifestV2(**_v2_kwargs())
        reconstructed = ClickHouseInputManifestV2.from_json(original.to_json())
        assert reconstructed.schema_version == 2
        assert len(reconstructed.resolved_instruments) == 2
        assert reconstructed.resolved_instruments[0].secid == 12345
        assert reconstructed.resolved_instruments[0].display_symbol == "AAPL"

    def test_v2_resolved_instruments_have_ticker_history(self) -> None:
        manifest = ClickHouseInputManifestV2(**_v2_kwargs())
        entry = manifest.resolved_instruments[0]
        assert len(entry.ticker_history) == 1
        assert entry.ticker_history[0].ticker == "AAPL"

    def test_v2_empty_resolved_instruments_allowed(self) -> None:
        """v2 allows empty resolved_instruments (edge case)."""
        kwargs = _v1_kwargs()
        kwargs["resolved_instruments"] = ()
        manifest = ClickHouseInputManifestV2(**kwargs)
        assert manifest.resolved_instruments == ()

    def test_v2_rejects_empty_symbols(self) -> None:
        with pytest.raises(ValueError, match="symbols must contain at least one"):
            ClickHouseInputManifestV2(
                source_name="test",
                database="market",
                bars_table="bars",
                symbols=(),
                start_date=date(2023, 1, 1),
                end_date=date(2023, 12, 31),
                resolved_instruments=(),
            )


# ---------------------------------------------------------------------------
# Versioned reader tests
# ---------------------------------------------------------------------------


class TestReadManifest:
    """Tests for the versioned read_manifest function."""

    def test_read_v1_manifest(self) -> None:
        v1 = ClickHouseInputManifestV1(**_v1_kwargs())
        json_str = v1.to_json()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = read_manifest(json_str)

        assert isinstance(result, ClickHouseInputManifestV1)
        assert result.schema_version == 1
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "v1 is deprecated" in str(w[0].message)

    def test_read_v2_manifest(self) -> None:
        v2 = ClickHouseInputManifestV2(**_v2_kwargs())
        json_str = v2.to_json()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = read_manifest(json_str)

        assert isinstance(result, ClickHouseInputManifestV2)
        assert result.schema_version == 2
        # No deprecation warning for v2
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 0

    def test_read_unknown_version_raises(self) -> None:
        data = {**_v1_kwargs(), "schema_version": 99, "symbols": ["AAPL"]}
        # Convert dates to strings for JSON
        data["start_date"] = "2023-01-01"
        data["end_date"] = "2023-12-31"
        json_str = json.dumps(data)

        with pytest.raises(ValueError, match="Unknown manifest schema_version"):
            read_manifest(json_str)


# ---------------------------------------------------------------------------
# Migration tests
# ---------------------------------------------------------------------------


class TestManifestMigration:
    """Tests for v1-to-v2 migration."""

    def test_v1_migrate_to_v2(self) -> None:
        v1 = ClickHouseInputManifestV1(**_v1_kwargs())
        v2 = v1.migrate_to_v2()

        assert isinstance(v2, ClickHouseInputManifestV2)
        assert v2.schema_version == 2
        assert v2.symbols == v1.symbols
        assert v2.source_name == v1.source_name
        assert v2.database == v1.database
        assert v2.bars_table == v1.bars_table
        assert v2.start_date == v1.start_date
        assert v2.end_date == v1.end_date
        assert v2.price_basis == v1.price_basis

    def test_migrated_v2_has_unresolved_instruments(self) -> None:
        v1 = ClickHouseInputManifestV1(**_v1_kwargs())
        v2 = v1.migrate_to_v2()

        assert len(v2.resolved_instruments) == 2
        for entry in v2.resolved_instruments:
            assert entry.secid is None
            assert entry.display_symbol is None
            assert entry.first_date is None
            assert entry.last_date is None
            assert entry.ticker_history == []
            assert entry.resolution["status"] == "unresolved"
            assert entry.resolution["reason"] == "migrated_from_v1"

    def test_migrated_v2_roundtrips(self) -> None:
        v1 = ClickHouseInputManifestV1(**_v1_kwargs())
        v2 = v1.migrate_to_v2()
        json_str = v2.to_json()
        reconstructed = ClickHouseInputManifestV2.from_json(json_str)

        assert reconstructed.schema_version == 2
        assert len(reconstructed.resolved_instruments) == 2
        assert reconstructed.resolved_instruments[0].runtime_symbol == "AAPL"
        assert reconstructed.resolved_instruments[0].secid is None

    def test_migrated_v2_can_be_read_by_read_manifest(self) -> None:
        v1 = ClickHouseInputManifestV1(**_v1_kwargs())
        v2 = v1.migrate_to_v2()
        json_str = v2.to_json()

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            result = read_manifest(json_str)

        assert isinstance(result, ClickHouseInputManifestV2)
        assert result.schema_version == 2
        # No deprecation warning for migrated v2
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 0
