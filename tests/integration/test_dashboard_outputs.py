"""Integration tests verifying dashboard-consumable outputs.

Validates that all outputs from Units A-D (manifest v2, events, decision audit,
scan results, reporting persistence) carry the identity fields and structure
required for dashboard consumption.

These tests verify field presence, serializability, and end-to-end consistency.
"""

from __future__ import annotations

import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from qs_trader.events.events import (
    FeatureBarEvent,
    FillEvent,
    IndicatorEvent,
    OrderEvent,
    PriceBarEvent,
    RuntimeFeaturesEvent,
    SignalEvent,
    TradeEvent,
)
from qs_trader.events.price_basis import PriceBasis
from qs_trader.services.reporting.manifest import (
    ClickHouseInputManifestV2,
    ResolvedInstrumentEntry,
    TickerHistoryEntry,
    read_manifest,
)
from qs_trader.services.reporting.reporting_persistence import (
    _BAR_SNAPSHOT_COLUMNS,
    _FEATURE_SNAPSHOT_BASE_COLUMNS,
    persist_bar_snapshots,
    persist_feature_snapshots,
    write_resolved_instruments,
)
from qs_trader.services.scan.runner import ScanResult, _SCAN_RESULT_COLUMNS
from qs_trader.services.strategy.decision_audit import (
    _DECISION_COLUMNS,
    compute_candidate_id,
    persist_decisions,
    persist_decisions_csv,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_IDENTITY_FIELDS = {"secid", "display_symbol", "ticker_at_date", "identity_source"}


def _sample_resolved_instruments() -> list[ResolvedInstrumentEntry]:
    """Build sample resolved instrument entries for testing."""
    return [
        ResolvedInstrumentEntry(
            runtime_symbol="AAPL",
            requested_symbol="AAPL",
            secid=12345,
            display_symbol="Apple Inc",
            first_date=date(2010, 1, 4),
            last_date=None,
            ticker_history=[
                TickerHistoryEntry(ticker="AAPL", start_date=date(2010, 1, 4)),
            ],
            resolution={"status": "resolved", "method": "secid_direct"},
        ),
        ResolvedInstrumentEntry(
            runtime_symbol="META",
            requested_symbol="FB",
            secid=67890,
            display_symbol="Meta Platforms Inc",
            first_date=date(2012, 5, 18),
            last_date=None,
            ticker_history=[
                TickerHistoryEntry(ticker="FB", start_date=date(2012, 5, 18), end_date=date(2022, 6, 8)),
                TickerHistoryEntry(ticker="META", start_date=date(2022, 6, 9)),
            ],
            resolution={"status": "resolved", "method": "ticker_history"},
        ),
    ]


def _sample_manifest_v2() -> ClickHouseInputManifestV2:
    """Build a sample v2 manifest for testing."""
    resolved = _sample_resolved_instruments()
    return ClickHouseInputManifestV2(
        source_name="qs-datamaster-equity-1d",
        database="market_data",
        bars_table="equity_daily",
        symbols=("AAPL", "META"),
        start_date=date(2023, 1, 1),
        end_date=date(2023, 12, 31),
        price_basis=PriceBasis.ADJUSTED,
        resolved_instruments=tuple(resolved),
    )


# ---------------------------------------------------------------------------
# 11.1 — Manifest v2 dashboard-consumable fields
# ---------------------------------------------------------------------------


class TestManifestV2DashboardFields:
    """Verify manifest v2 includes all fields needed for dashboard consumption."""

    def test_manifest_v2_top_level_fields(self) -> None:
        """ClickHouseInputManifestV2 has all required top-level fields."""
        manifest = _sample_manifest_v2()
        data = json.loads(manifest.to_json())

        # Required top-level fields
        assert data["schema_version"] == 2
        assert isinstance(data["schema_version"], int), "schema_version must be int, not string"
        assert data["source_kind"] == "clickhouse"
        assert data["source_name"] == "qs-datamaster-equity-1d"
        assert data["database"] == "market_data"
        assert data["bars_table"] == "equity_daily"
        assert data["start_date"] == "2023-01-01"
        assert data["end_date"] == "2023-12-31"
        assert data["price_basis"] == "adjusted"

    def test_manifest_v2_resolved_instruments_array(self) -> None:
        """resolved_instruments is present and non-empty."""
        manifest = _sample_manifest_v2()
        data = json.loads(manifest.to_json())

        assert "resolved_instruments" in data
        assert isinstance(data["resolved_instruments"], list)
        assert len(data["resolved_instruments"]) == 2

    def test_resolved_instrument_entry_fields(self) -> None:
        """Each ResolvedInstrumentEntry has all required identity fields."""
        manifest = _sample_manifest_v2()
        data = json.loads(manifest.to_json())

        required_fields = {
            "runtime_symbol",
            "requested_symbol",
            "secid",
            "display_symbol",
            "first_date",
            "last_date",
            "ticker_history",
            "resolution",
        }

        for entry in data["resolved_instruments"]:
            missing = required_fields - set(entry.keys())
            assert not missing, f"ResolvedInstrumentEntry missing fields: {missing}"

    def test_resolved_instrument_ticker_history_structure(self) -> None:
        """TickerHistoryEntry has ticker, start_date, end_date fields."""
        manifest = _sample_manifest_v2()
        data = json.loads(manifest.to_json())

        meta_entry = data["resolved_instruments"][1]  # META with ticker history
        assert len(meta_entry["ticker_history"]) == 2

        for hist in meta_entry["ticker_history"]:
            assert "ticker" in hist
            assert "start_date" in hist
            assert "end_date" in hist  # Can be null

    def test_resolved_instrument_resolution_metadata(self) -> None:
        """Resolution metadata is present and serializable."""
        manifest = _sample_manifest_v2()
        data = json.loads(manifest.to_json())

        for entry in data["resolved_instruments"]:
            assert "resolution" in entry
            assert isinstance(entry["resolution"], dict)
            assert "status" in entry["resolution"]

    def test_manifest_roundtrip_preserves_fields(self) -> None:
        """Serialise and deserialise preserves all dashboard fields."""
        original = _sample_manifest_v2()
        json_str = original.to_json()
        roundtripped = read_manifest(json_str)

        assert isinstance(roundtripped, ClickHouseInputManifestV2)
        assert roundtripped.schema_version == 2
        assert len(roundtripped.resolved_instruments) == 2
        assert roundtripped.resolved_instruments[0].secid == 12345
        assert roundtripped.resolved_instruments[1].secid == 67890


# ---------------------------------------------------------------------------
# 11.2 — Event identity fields for dashboard joins
# ---------------------------------------------------------------------------


class TestEventIdentityFields:
    """Verify identity fields on events enable dashboard to join data by secid."""

    @pytest.mark.parametrize(
        "event_cls,kwargs",
        [
            (
                PriceBarEvent,
                {
                    "symbol": "AAPL",
                    "timestamp": "2024-01-02T00:00:00Z",
                    "interval": "1d",
                    "open": Decimal("100.00"),
                    "high": Decimal("101.00"),
                    "low": Decimal("99.50"),
                    "close": Decimal("100.75"),
                    "volume": 1000,
                    "source": "test",
                    "secid": 12345,
                    "display_symbol": "Apple Inc",
                    "ticker_at_date": "AAPL",
                    "identity_source": "secid_direct",
                },
            ),
            (
                FeatureBarEvent,
                {
                    "timestamp": "2024-01-02T00:00:00Z",
                    "symbol": "AAPL",
                    "features": {"alpha": 1.0},
                    "secid": 12345,
                    "display_symbol": "Apple Inc",
                    "ticker_at_date": "AAPL",
                    "identity_source": "secid_direct",
                },
            ),
            (
                RuntimeFeaturesEvent,
                {
                    "strategy_id": "test_strategy",
                    "symbol": "AAPL",
                    "timestamp": "2024-01-02T00:00:00Z",
                    "runtime_features": {"momentum": 0.5},
                    "secid": 12345,
                    "display_symbol": "Apple Inc",
                    "ticker_at_date": "AAPL",
                    "identity_source": "secid_direct",
                },
            ),
            (
                IndicatorEvent,
                {
                    "strategy_id": "test_strategy",
                    "symbol": "AAPL",
                    "timestamp": "2024-01-02T00:00:00Z",
                    "indicators": {"sma_10": Decimal("100.5")},
                    "secid": 12345,
                    "display_symbol": "Apple Inc",
                    "ticker_at_date": "AAPL",
                    "identity_source": "secid_direct",
                },
            ),
            (
                SignalEvent,
                {
                    "signal_id": "sig-001",
                    "timestamp": "2024-01-02T00:00:00Z",
                    "strategy_id": "test_strategy",
                    "symbol": "AAPL",
                    "intention": "OPEN_LONG",
                    "price": Decimal("100.50"),
                    "confidence": Decimal("0.85"),
                    "secid": 12345,
                    "display_symbol": "Apple Inc",
                    "ticker_at_date": "AAPL",
                    "identity_source": "secid_direct",
                },
            ),
            (
                OrderEvent,
                {
                    "intent_id": "sig-001",
                    "idempotency_key": "order-key-1",
                    "timestamp": "2024-01-02T00:00:00Z",
                    "symbol": "AAPL",
                    "side": "buy",
                    "quantity": Decimal("10"),
                    "order_type": "market",
                    "secid": 12345,
                    "display_symbol": "Apple Inc",
                    "ticker_at_date": "AAPL",
                    "identity_source": "secid_direct",
                },
            ),
            (
                FillEvent,
                {
                    "fill_id": "550e8400-e29b-41d4-a716-446655440001",
                    "source_order_id": "order-001",
                    "timestamp": "2024-01-02T00:00:00Z",
                    "symbol": "AAPL",
                    "side": "buy",
                    "filled_quantity": Decimal("10"),
                    "fill_price": Decimal("100.60"),
                    "secid": 12345,
                    "display_symbol": "Apple Inc",
                    "ticker_at_date": "AAPL",
                    "identity_source": "secid_direct",
                },
            ),
            (
                TradeEvent,
                {
                    "trade_id": "T00001",
                    "timestamp": "2024-01-02T00:00:00Z",
                    "strategy_id": "test_strategy",
                    "symbol": "AAPL",
                    "status": "open",
                    "fills": ["550e8400-e29b-41d4-a716-446655440001"],
                    "secid": 12345,
                    "display_symbol": "Apple Inc",
                    "ticker_at_date": "AAPL",
                    "identity_source": "secid_direct",
                },
            ),
        ],
    )
    def test_event_has_identity_fields(self, event_cls, kwargs: dict[str, Any]) -> None:
        """Each event type carries secid, display_symbol, ticker_at_date, identity_source."""
        event = event_cls(**kwargs)

        assert event.secid == 12345
        assert event.display_symbol == "Apple Inc"
        assert event.ticker_at_date == "AAPL"
        assert event.identity_source == "secid_direct"

    def test_event_identity_fields_are_serializable(self) -> None:
        """Identity fields survive JSON serialisation."""
        event = PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-02T00:00:00Z",
            interval="1d",
            open=Decimal("100.00"),
            high=Decimal("101.00"),
            low=Decimal("99.50"),
            close=Decimal("100.75"),
            volume=1000,
            source="test",
            secid=12345,
            display_symbol="Apple Inc",
            ticker_at_date="AAPL",
            identity_source="secid_direct",
        )

        data = event.model_dump()
        assert data["secid"] == 12345
        assert data["display_symbol"] == "Apple Inc"
        assert data["ticker_at_date"] == "AAPL"
        assert data["identity_source"] == "secid_direct"

        # JSON roundtrip
        json_str = event.model_dump_json()
        parsed = json.loads(json_str)
        assert parsed["secid"] == 12345
        assert parsed["display_symbol"] == "Apple Inc"

    def test_event_identity_fields_optional_backward_compat(self) -> None:
        """Events without identity fields still work (backward compatibility)."""
        event = PriceBarEvent(
            symbol="AAPL",
            timestamp="2024-01-02T00:00:00Z",
            interval="1d",
            open=Decimal("100.00"),
            high=Decimal("101.00"),
            low=Decimal("99.50"),
            close=Decimal("100.75"),
            volume=1000,
            source="test",
        )

        assert event.secid is None
        assert event.display_symbol is None
        assert event.ticker_at_date is None
        assert event.identity_source is None


# ---------------------------------------------------------------------------
# 11.3 — Decision audit trail dashboard fields
# ---------------------------------------------------------------------------


class TestDecisionAuditDashboardFields:
    """Verify decision audit trail enables dashboard to inspect candidate decisions."""

    def test_decision_columns_include_required_fields(self) -> None:
        """_DECISION_COLUMNS has all fields needed by dashboard."""
        required = {
            "candidate_id",
            "strategy_id",
            "secid",
            "symbol",
            "date",
            "decision_status",
            "final_action",
            "reason_code",
            "gates",
            "diagnostics",
        }
        missing = required - set(_DECISION_COLUMNS)
        assert not missing, f"Decision columns missing: {missing}"

    def test_persist_decisions_includes_all_columns(self, tmp_path: Path) -> None:
        """persist_decisions writes all required columns to parquet."""
        decisions = [
            {
                "candidate_id": compute_candidate_id(12345, "2024-01-02", "test_strategy"),
                "strategy_id": "test_strategy",
                "secid": 12345,
                "symbol": "AAPL",
                "date": "2024-01-02",
                "decision_status": "accepted",
                "final_action": "open_long",
                "reason_code": "momentum_signal",
                "gates": {"risk": "pass", "liquidity": "pass"},
                "diagnostics": {"score": 0.85, "confidence": 0.9},
                "strategy_version": "v1.0",
                "parameter_hash": "abc123",
                "confidence": 0.85,
                "decision_price": 100.50,
                "indicator_context": {"sma_10": 100.5},
                "metadata": {"note": "test"},
                "occurred_at": "2024-01-02T00:00:00Z",
            }
        ]

        result = persist_decisions(decisions, tmp_path)
        assert result is not None
        assert result.exists()

        # Read back and verify columns
        import pandas as pd

        df = pd.read_parquet(result)
        for col in _DECISION_COLUMNS:
            assert col in df.columns, f"Column {col} missing from parquet output"

    def test_persist_decisions_csv_includes_all_columns(self, tmp_path: Path) -> None:
        """persist_decisions_csv writes all required columns."""
        decisions = [
            {
                "candidate_id": compute_candidate_id(12345, "2024-01-02", "test_strategy"),
                "strategy_id": "test_strategy",
                "secid": 12345,
                "symbol": "AAPL",
                "date": "2024-01-02",
                "decision_status": "rejected",
                "final_action": "skip",
                "reason_code": "risk_gate_failed",
                "gates": {"risk": "fail"},
                "diagnostics": {"risk_score": 0.95},
                "strategy_version": "v1.0",
                "parameter_hash": "abc123",
                "confidence": 0.3,
                "decision_price": 100.50,
                "indicator_context": {},
                "metadata": {},
                "occurred_at": "2024-01-02T00:00:00Z",
            }
        ]

        result = persist_decisions_csv(decisions, tmp_path)
        assert result is not None
        assert result.exists()

        # Read CSV and verify columns
        import csv

        with open(result, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = set(reader.fieldnames or [])
            for col in _DECISION_COLUMNS:
                assert col in headers, f"Column {col} missing from CSV output"

    def test_candidate_id_deterministic(self) -> None:
        """candidate_id is deterministic for same inputs."""
        id1 = compute_candidate_id(12345, "2024-01-02", "test_strategy")
        id2 = compute_candidate_id(12345, "2024-01-02", "test_strategy")
        assert id1 == id2

        # Different secid produces different ID
        id3 = compute_candidate_id(99999, "2024-01-02", "test_strategy")
        assert id1 != id3


# ---------------------------------------------------------------------------
# 11.4 — Scan results dashboard fields
# ---------------------------------------------------------------------------


class TestScanResultsDashboardFields:
    """Verify scan results enable dashboard to filter by secid, date range, candidate_status."""

    def test_scan_result_columns_include_required_fields(self) -> None:
        """_SCAN_RESULT_COLUMNS has all fields needed by dashboard."""
        required = {
            "date",
            "secid",
            "display_symbol",
            "ticker_at_date",
            "strategy_id",
            "candidate_status",
            "reason_code",
            "score",
        }
        # forward_return is split into multiple horizon columns
        forward_return_cols = {c for c in _SCAN_RESULT_COLUMNS if c.startswith("forward_return")}
        assert len(forward_return_cols) > 0, "No forward_return columns found"

        missing = required - set(_SCAN_RESULT_COLUMNS)
        assert not missing, f"Scan result columns missing: {missing}"

    def test_scan_result_dataclass_has_required_fields(self) -> None:
        """ScanResult dataclass has all required fields."""
        result = ScanResult(
            date="2024-01-02",
            secid=12345,
            display_symbol="Apple Inc",
            ticker_at_date="AAPL",
            runtime_symbol="AAPL",
            strategy_id="test_strategy",
            candidate_status="accepted",
            reason_code="momentum_signal",
            score=0.85,
            forward_return_5d=0.02,
            forward_return_10d=0.03,
            forward_return_20d=0.05,
        )

        assert result.secid == 12345
        assert result.display_symbol == "Apple Inc"
        assert result.ticker_at_date == "AAPL"
        assert result.candidate_status == "accepted"
        assert result.reason_code == "momentum_signal"
        assert result.score == 0.85
        assert result.forward_return_5d == 0.02
        assert result.forward_return_10d == 0.03
        assert result.forward_return_20d == 0.05

    def test_scan_result_serializable_for_dashboard(self, tmp_path: Path) -> None:
        """Scan results can be persisted and read back with all identity fields."""
        results = [
            ScanResult(
                date="2024-01-02",
                secid=12345,
                display_symbol="Apple Inc",
                ticker_at_date="AAPL",
                runtime_symbol="AAPL",
                strategy_id="test_strategy",
                candidate_status="accepted",
                reason_code="momentum_signal",
                score=0.85,
                forward_return_5d=0.02,
            ),
            ScanResult(
                date="2024-01-03",
                secid=67890,
                display_symbol="Meta Platforms Inc",
                ticker_at_date="META",
                runtime_symbol="META",
                strategy_id="test_strategy",
                candidate_status="rejected",
                reason_code="risk_gate_failed",
                score=0.3,
            ),
        ]

        from qs_trader.services.scan.runner import ScanRunner

        output_path = tmp_path / "scan_results.csv"
        ScanRunner._persist_results(results, output_path)

        assert output_path.exists()

        # Read back and verify
        import csv

        with open(output_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["secid"] == "12345"
        assert rows[0]["display_symbol"] == "Apple Inc"
        assert rows[0]["ticker_at_date"] == "AAPL"
        assert rows[0]["candidate_status"] == "accepted"
        assert rows[1]["candidate_status"] == "rejected"

    def test_scan_result_secid_none_handled(self, tmp_path: Path) -> None:
        """Scan results with None secid are handled gracefully."""
        results = [
            ScanResult(
                date="2024-01-02",
                secid=None,
                display_symbol="UNKNOWN",
                ticker_at_date="UNKNOWN",
                runtime_symbol="UNKNOWN",
                strategy_id="test_strategy",
                candidate_status="unresolved",
                reason_code="no_secid",
            ),
        ]

        from qs_trader.services.scan.runner import ScanRunner

        output_path = tmp_path / "scan_results_unresolved.csv"
        ScanRunner._persist_results(results, output_path)

        assert output_path.exists()

        import csv

        with open(output_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 1
        assert rows[0]["secid"] == ""  # None serialized as empty string


# ---------------------------------------------------------------------------
# 11.5 — Audit export stable identity columns
# ---------------------------------------------------------------------------


class TestAuditExportIdentityColumns:
    """Verify audit export includes stable identity columns."""

    def test_bar_snapshot_columns_include_identity(self) -> None:
        """Bar snapshot columns include secid, display_symbol, ticker_at_date, identity_source."""
        for field in _IDENTITY_FIELDS:
            assert field in _BAR_SNAPSHOT_COLUMNS, f"Bar snapshot missing identity field: {field}"

    def test_feature_snapshot_columns_include_identity(self) -> None:
        """Feature snapshot columns include secid, display_symbol, ticker_at_date, identity_source."""
        for field in _IDENTITY_FIELDS:
            assert field in _FEATURE_SNAPSHOT_BASE_COLUMNS, (
                f"Feature snapshot missing identity field: {field}"
            )

    def test_persist_bar_snapshots_includes_identity(self, tmp_path: Path) -> None:
        """persist_bar_snapshots writes identity columns to CSV."""
        snapshots = [
            {
                "secid": 12345,
                "date": "2024-01-02",
                "runtime_symbol": "AAPL",
                "display_symbol": "Apple Inc",
                "ticker_at_date": "AAPL",
                "identity_source": "secid_direct",
                "open": 100.0,
                "high": 101.0,
                "low": 99.5,
                "close": 100.75,
                "volume": 1000,
            },
        ]

        result = persist_bar_snapshots(snapshots, tmp_path)
        assert result is not None
        assert result.exists()

        import csv

        with open(result, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = set(reader.fieldnames or [])
            row = next(reader)

        for field in _IDENTITY_FIELDS:
            assert field in headers, f"Bar snapshot CSV missing column: {field}"
            assert row[field] != "", f"Bar snapshot CSV has empty {field}"

    def test_persist_feature_snapshots_includes_identity(self, tmp_path: Path) -> None:
        """persist_feature_snapshots writes identity columns to CSV."""
        snapshots = [
            {
                "secid": 12345,
                "date": "2024-01-02",
                "runtime_symbol": "AAPL",
                "display_symbol": "Apple Inc",
                "ticker_at_date": "AAPL",
                "identity_source": "secid_direct",
                "strategy_id": "test_strategy",
                "feature_values": {"momentum": 0.5, "volatility": 0.2},
            },
        ]

        result = persist_feature_snapshots(snapshots, tmp_path)
        assert result is not None
        assert result.exists()

        import csv

        with open(result, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            headers = set(reader.fieldnames or [])
            row = next(reader)

        for field in _IDENTITY_FIELDS:
            assert field in headers, f"Feature snapshot CSV missing column: {field}"
            assert row[field] != "", f"Feature snapshot CSV has empty {field}"

    def test_write_resolved_instruments_includes_identity(self, tmp_path: Path) -> None:
        """write_resolved_instruments writes full identity metadata to JSON."""
        instruments = [
            {
                "runtime_symbol": "AAPL",
                "requested_symbol": "AAPL",
                "secid": 12345,
                "display_symbol": "Apple Inc",
                "first_date": "2010-01-04",
                "last_date": None,
                "ticker_history": [{"ticker": "AAPL", "start_date": "2010-01-04", "end_date": None}],
                "resolution": {"status": "resolved", "method": "secid_direct"},
            },
        ]

        result = write_resolved_instruments(instruments, tmp_path)
        assert result is not None
        assert result.exists()

        with open(result, encoding="utf-8") as f:
            data = json.load(f)

        assert len(data) == 1
        entry = data[0]
        assert entry["secid"] == 12345
        assert entry["display_symbol"] == "Apple Inc"
        assert "ticker_history" in entry
        assert "resolution" in entry


# ---------------------------------------------------------------------------
# 11.6 — End-to-end dashboard-consumable output verification
# ---------------------------------------------------------------------------


class TestEndToEndDashboardOutputs:
    """End-to-end: create a mock run, verify all outputs are dashboard-consumable."""

    def test_full_output_pipeline(self, tmp_path: Path) -> None:
        """Create mock run data and verify all outputs carry identity fields."""
        output_dir = tmp_path / "audit_output"
        output_dir.mkdir(parents=True, exist_ok=True)

        # 1. Create manifest v2
        manifest = _sample_manifest_v2()
        manifest_json = manifest.to_json()
        manifest_data = json.loads(manifest_json)

        # Verify manifest has all dashboard fields
        assert manifest_data["schema_version"] == 2
        assert manifest_data["source_kind"] == "clickhouse"
        assert "resolved_instruments" in manifest_data
        assert len(manifest_data["resolved_instruments"]) == 2

        # 2. Write resolved instruments
        resolved = [
            {
                "runtime_symbol": inst.runtime_symbol,
                "requested_symbol": inst.requested_symbol,
                "secid": inst.secid,
                "display_symbol": inst.display_symbol,
                "first_date": inst.first_date.isoformat() if inst.first_date else None,
                "last_date": inst.last_date.isoformat() if inst.last_date else None,
                "ticker_history": [
                    {"ticker": h.ticker, "start_date": h.start_date.isoformat(), "end_date": h.end_date.isoformat() if h.end_date else None}
                    for h in inst.ticker_history
                ],
                "resolution": inst.resolution,
            }
            for inst in manifest.resolved_instruments
        ]

        ri_path = write_resolved_instruments(resolved, output_dir)
        assert ri_path is not None

        # 3. Write bar snapshots with identity fields
        bar_snapshots = [
            {
                "secid": 12345,
                "date": "2024-01-02",
                "runtime_symbol": "AAPL",
                "display_symbol": "Apple Inc",
                "ticker_at_date": "AAPL",
                "identity_source": "secid_direct",
                "open": 100.0,
                "high": 101.0,
                "low": 99.5,
                "close": 100.75,
                "volume": 1000,
            },
            {
                "secid": 67890,
                "date": "2024-01-02",
                "runtime_symbol": "META",
                "display_symbol": "Meta Platforms Inc",
                "ticker_at_date": "META",
                "identity_source": "ticker_history",
                "open": 350.0,
                "high": 355.0,
                "low": 348.0,
                "close": 352.0,
                "volume": 2000,
            },
        ]
        bar_path = persist_bar_snapshots(bar_snapshots, output_dir)
        assert bar_path is not None

        # 4. Write feature snapshots with identity fields
        feature_snapshots = [
            {
                "secid": 12345,
                "date": "2024-01-02",
                "runtime_symbol": "AAPL",
                "display_symbol": "Apple Inc",
                "ticker_at_date": "AAPL",
                "identity_source": "secid_direct",
                "strategy_id": "momentum_strategy",
                "feature_values": {"momentum_10d": 0.05, "volatility_20d": 0.2},
            },
        ]
        feat_path = persist_feature_snapshots(feature_snapshots, output_dir)
        assert feat_path is not None

        # 5. Write decision audit with identity fields
        decisions = [
            {
                "candidate_id": compute_candidate_id(12345, "2024-01-02", "momentum_strategy"),
                "strategy_id": "momentum_strategy",
                "secid": 12345,
                "symbol": "AAPL",
                "date": "2024-01-02",
                "decision_status": "accepted",
                "final_action": "open_long",
                "reason_code": "momentum_signal",
                "gates": {"risk": "pass", "liquidity": "pass"},
                "diagnostics": {"score": 0.85},
                "strategy_version": "v1.0",
                "parameter_hash": "abc123",
                "confidence": 0.85,
                "decision_price": 100.50,
                "indicator_context": None,
                "metadata": None,
                "occurred_at": "2024-01-02T00:00:00Z",
            },
        ]
        dec_path = persist_decisions(decisions, output_dir)
        assert dec_path is not None

        # 6. Verify all output files exist
        assert ri_path.exists()
        assert bar_path.exists()
        assert feat_path.exists()
        assert dec_path.exists()

        # 7. Verify bar snapshot CSV has identity columns
        import csv

        with open(bar_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            bar_headers = set(reader.fieldnames or [])
        for field in _IDENTITY_FIELDS:
            assert field in bar_headers, f"Bar snapshot missing: {field}"

        # 8. Verify feature snapshot CSV has identity columns
        with open(feat_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            feat_headers = set(reader.fieldnames or [])
        for field in _IDENTITY_FIELDS:
            assert field in feat_headers, f"Feature snapshot missing: {field}"

        # 9. Verify decision parquet has identity columns
        import pandas as pd

        df = pd.read_parquet(dec_path)
        assert "secid" in df.columns
        assert "symbol" in df.columns
        assert "candidate_id" in df.columns

        # 10. Verify resolved instruments JSON has identity metadata
        with open(ri_path, encoding="utf-8") as f:
            ri_data = json.load(f)
        for entry in ri_data:
            assert "secid" in entry
            assert "display_symbol" in entry
            assert "ticker_history" in entry
            assert "resolution" in entry

    def test_events_with_identity_fields_serializable(self) -> None:
        """All event types with identity fields can be serialized to JSON."""
        timestamp = "2024-01-02T00:00:00Z"
        fill_uuid = "550e8400-e29b-41d4-a716-446655440001"
        identity = {
            "secid": 12345,
            "display_symbol": "Apple Inc",
            "ticker_at_date": "AAPL",
            "identity_source": "secid_direct",
        }

        events = [
            PriceBarEvent(
                symbol="AAPL", timestamp=timestamp, interval="1d",
                open=Decimal("100.00"), high=Decimal("101.00"),
                low=Decimal("99.50"), close=Decimal("100.75"),
                volume=1000, source="test", **identity,
            ),
            FeatureBarEvent(
                timestamp=timestamp, symbol="AAPL",
                features={"alpha": 1.0}, **identity,
            ),
            RuntimeFeaturesEvent(
                strategy_id="test", symbol="AAPL", timestamp=timestamp,
                runtime_features={"momentum": 0.5}, **identity,
            ),
            IndicatorEvent(
                strategy_id="test", symbol="AAPL", timestamp=timestamp,
                indicators={"sma": Decimal("100.5")}, **identity,
            ),
            SignalEvent(
                signal_id="sig-001", timestamp=timestamp,
                strategy_id="test", symbol="AAPL",
                intention="OPEN_LONG", price=Decimal("100.50"),
                confidence=Decimal("0.85"), **identity,
            ),
            OrderEvent(
                intent_id="sig-001", idempotency_key="key-1",
                timestamp=timestamp, symbol="AAPL",
                side="buy", quantity=Decimal("10"),
                order_type="market", **identity,
            ),
            FillEvent(
                fill_id=fill_uuid, source_order_id="order-001",
                timestamp=timestamp, symbol="AAPL",
                side="buy", filled_quantity=Decimal("10"),
                fill_price=Decimal("100.60"), **identity,
            ),
            TradeEvent(
                trade_id="T00001", timestamp=timestamp,
                strategy_id="test", symbol="AAPL",
                status="open", fills=[fill_uuid], **identity,
            ),
        ]

        for event in events:
            json_str = event.model_dump_json()
            parsed = json.loads(json_str)
            assert parsed["secid"] == 12345, f"{event.__class__.__name__} secid not serializable"
            assert parsed["display_symbol"] == "Apple Inc", f"{event.__class__.__name__} display_symbol not serializable"
            assert parsed["ticker_at_date"] == "AAPL", f"{event.__class__.__name__} ticker_at_date not serializable"
            assert parsed["identity_source"] == "secid_direct", f"{event.__class__.__name__} identity_source not serializable"
