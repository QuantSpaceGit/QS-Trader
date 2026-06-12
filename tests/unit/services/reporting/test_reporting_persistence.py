"""Unit tests for reporting persistence (Group 9).

Tests resolved_instruments.json, runtime bar/feature snapshots,
and ticker compatibility views.
"""

import json
import tempfile
from datetime import date
from pathlib import Path

import pytest

from qs_trader.services.reporting.reporting_persistence import (
    build_ticker_compatibility_view,
    persist_bar_snapshots,
    persist_feature_snapshots,
    write_resolved_instruments,
    write_ticker_compatibility_view,
)


# ---------------------------------------------------------------------------
# Resolved instruments tests
# ---------------------------------------------------------------------------


class TestWriteResolvedInstruments:
    """Tests for resolved_instruments.json output."""

    def test_write_resolved_instruments(self):
        instruments = [
            {
                "runtime_symbol": "AAPL",
                "requested_symbol": "AAPL",
                "secid": 12345,
                "display_symbol": "AAPL",
                "first_date": "2023-01-01",
                "last_date": "2023-12-31",
                "ticker_history": [
                    {"ticker": "AAPL", "start_date": "1980-12-12"}
                ],
                "resolution": {"status": "resolved", "source": "secmaster"},
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_resolved_instruments(instruments, Path(tmpdir))
            assert path is not None
            assert path.exists()
            assert path.name == "resolved_instruments.json"

            content = json.loads(path.read_text())
            assert len(content) == 1
            assert content[0]["secid"] == 12345
            assert content[0]["runtime_symbol"] == "AAPL"

    def test_write_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_resolved_instruments([], Path(tmpdir))
            assert path is None

    def test_write_multiple(self):
        instruments = [
            {
                "runtime_symbol": "AAPL",
                "requested_symbol": "AAPL",
                "secid": 12345,
                "display_symbol": "AAPL",
                "first_date": "2023-01-01",
                "last_date": "2023-12-31",
                "ticker_history": [],
                "resolution": {},
            },
            {
                "runtime_symbol": "MSFT",
                "requested_symbol": "MSFT",
                "secid": 67890,
                "display_symbol": "MSFT",
                "first_date": "2023-01-01",
                "last_date": "2023-12-31",
                "ticker_history": [],
                "resolution": {},
            },
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = write_resolved_instruments(instruments, Path(tmpdir))
            assert path is not None

            content = json.loads(path.read_text())
            assert len(content) == 2
            secids = {item["secid"] for item in content}
            assert secids == {12345, 67890}


# ---------------------------------------------------------------------------
# Bar snapshot tests
# ---------------------------------------------------------------------------


class TestPersistBarSnapshots:
    """Tests for runtime bar snapshot CSV persistence."""

    def test_persist_bar_snapshots(self):
        snapshots = [
            {
                "secid": 12345,
                "date": "2024-01-15",
                "runtime_symbol": "AAPL",
                "display_symbol": "AAPL",
                "ticker_at_date": "AAPL",
                "identity_source": "secmaster",
                "open": 150.0,
                "high": 151.0,
                "low": 149.0,
                "close": 150.5,
                "volume": 1000000,
                "open_adj": 155.0,
                "high_adj": 156.0,
                "low_adj": 154.0,
                "close_adj": 155.5,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = persist_bar_snapshots(snapshots, Path(tmpdir))
            assert path is not None
            assert path.exists()
            assert path.name == "runtime_bar_snapshots.csv"

            content = path.read_text()
            lines = content.strip().split("\n")
            assert len(lines) == 2  # header + 1 row
            assert "secid" in lines[0]
            assert "12345" in lines[1]
            assert "150.5" in lines[1]

    def test_persist_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = persist_bar_snapshots([], Path(tmpdir))
            assert path is None

    def test_persist_with_null_fields(self):
        snapshots = [
            {
                "secid": None,
                "date": "2024-01-15",
                "runtime_symbol": "AAPL",
                "display_symbol": None,
                "ticker_at_date": None,
                "identity_source": None,
                "open": 150.0,
                "high": 151.0,
                "low": 149.0,
                "close": 150.5,
                "volume": 1000000,
                "open_adj": None,
                "high_adj": None,
                "low_adj": None,
                "close_adj": None,
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = persist_bar_snapshots(snapshots, Path(tmpdir))
            assert path is not None
            # Should not raise — null fields handled gracefully


# ---------------------------------------------------------------------------
# Feature snapshot tests
# ---------------------------------------------------------------------------


class TestPersistFeatureSnapshots:
    """Tests for runtime feature snapshot CSV persistence."""

    def test_persist_feature_snapshots(self):
        snapshots = [
            {
                "secid": 12345,
                "date": "2024-01-15",
                "runtime_symbol": "AAPL",
                "display_symbol": "AAPL",
                "ticker_at_date": "AAPL",
                "identity_source": "secmaster",
                "strategy_id": "momentum",
                "feature_values": {
                    "momentum_score": 0.85,
                    "volatility": 0.15,
                    "regime": "bull",
                },
            }
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            path = persist_feature_snapshots(snapshots, Path(tmpdir))
            assert path is not None
            assert path.exists()
            assert path.name == "runtime_feature_snapshots.csv"

            content = path.read_text()
            lines = content.strip().split("\n")
            assert len(lines) == 2
            assert "secid" in lines[0]
            assert "feature_values" in lines[0]
            assert "momentum_score" in lines[1]

    def test_persist_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = persist_feature_snapshots([], Path(tmpdir))
            assert path is None


# ---------------------------------------------------------------------------
# Compatibility view tests
# ---------------------------------------------------------------------------


class TestTickerCompatibilityView:
    """Tests for ticker-grouped compatibility views."""

    def test_build_view(self):
        snapshots = [
            {"runtime_symbol": "AAPL", "secid": 12345, "date": "2024-01-15"},
            {"runtime_symbol": "AAPL", "secid": 12345, "date": "2024-01-16"},
            {"runtime_symbol": "MSFT", "secid": 67890, "date": "2024-01-15"},
        ]

        view = build_ticker_compatibility_view(snapshots)
        assert len(view) == 2
        assert len(view["AAPL"]) == 2
        assert len(view["MSFT"]) == 1

    def test_build_view_custom_field(self):
        snapshots = [
            {"display_symbol": "AAPL", "secid": 12345},
            {"display_symbol": "META", "secid": 3513095},
        ]

        view = build_ticker_compatibility_view(snapshots, "display_symbol")
        assert "AAPL" in view
        assert "META" in view

    def test_write_ticker_view(self):
        snapshots = [
            {"runtime_symbol": "AAPL", "secid": 12345, "date": "2024-01-15", "close": 150.0},
            {"runtime_symbol": "AAPL", "secid": 12345, "date": "2024-01-16", "close": 151.0},
            {"runtime_symbol": "MSFT", "secid": 67890, "date": "2024-01-15", "close": 380.0},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = write_ticker_compatibility_view(snapshots, Path(tmpdir))
            assert len(paths) == 2

            # Check AAPL file
            aapl_path = Path(tmpdir) / "ticker_view_AAPL.csv"
            assert aapl_path.exists()
            aapl_content = aapl_path.read_text()
            assert "150.0" in aapl_content
            assert "151.0" in aapl_content

            # Check MSFT file
            msft_path = Path(tmpdir) / "ticker_view_MSFT.csv"
            assert msft_path.exists()
            msft_content = msft_path.read_text()
            assert "380.0" in msft_content

    def test_write_ticker_view_empty(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            paths = write_ticker_compatibility_view([], Path(tmpdir))
            assert paths == []

    def test_write_ticker_view_special_chars(self):
        snapshots = [
            {"runtime_symbol": "BRK/B", "secid": 11111, "date": "2024-01-15"},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            paths = write_ticker_compatibility_view(snapshots, Path(tmpdir))
            assert len(paths) == 1
            # "/" should be replaced with "_"
            assert "BRK_B" in paths[0].name
