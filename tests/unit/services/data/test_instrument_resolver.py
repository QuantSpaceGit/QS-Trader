"""Tests for InstrumentResolver.

Unit tests for the resolve_all_secids method covering:
- History table path with date filtering
- Secmaster fallback path
- Empty results
- Logic: display_symbol is the ticker active on universe_date (not the last ticker in history)
"""

from datetime import date
from unittest.mock import MagicMock

import pytest

from qs_trader.services.data.instrument_resolver import (
    InstrumentResolver,
    MinimalInstrument,
)


class MockQueryResult:
    """Mock ClickHouse query result."""

    def __init__(self, rows):
        self.result_rows = rows


class TestResolveAllSecids:
    """Test resolve_all_secids method."""

    @pytest.fixture
    def mock_client(self):
        return MagicMock()

    @pytest.fixture
    def resolver_no_history(self, mock_client):
        """Resolver without ticker_history_table (uses secmaster fallback)."""
        return InstrumentResolver(
            clickhouse_client=mock_client,
            database="market",
        )

    @pytest.fixture
    def resolver_with_history(self, mock_client):
        """Resolver with ticker_history_table (uses normalized history path)."""
        return InstrumentResolver(
            clickhouse_client=mock_client,
            database="market",
            ticker_history_table="as_secmaster_ticker_history",
        )

    # --- History table path ---

    def test_history_table_returns_minimal_instruments(
        self, resolver_with_history, mock_client
    ):
        """History table path returns MinimalInstrument with secid and display_symbol."""
        mock_client.query.return_value = MockQueryResult(
            [
                (101, "AAPL"),
                (202, "META"),
                (303, "TSLA"),
            ]
        )

        instruments = resolver_with_history.resolve_all_secids(
            universe_date=date(2023, 6, 15)
        )

        assert len(instruments) == 3
        assert instruments[0] == MinimalInstrument(secid=101, display_symbol="AAPL")
        assert instruments[1] == MinimalInstrument(secid=202, display_symbol="META")
        assert instruments[2] == MinimalInstrument(secid=303, display_symbol="TSLA")

    def test_history_table_deduplicates_secid(
        self, resolver_with_history, mock_client
    ):
        """Multiple ticker periods for the same secid return one record with most recent ticker."""
        # Secid 202 has overlapping ticker periods: FB (2012-2022) and META (2022-)
        # ORDER BY secid, start_date DESC ensures META comes first for secid 202
        mock_client.query.return_value = MockQueryResult(
            [
                (101, "AAPL"),
                (202, "META"),   # start_date DESC places this first for secid=202
                (202, "FB"),
            ]
        )

        instruments = resolver_with_history.resolve_all_secids(
            universe_date=date(2023, 6, 15)
        )

        assert len(instruments) == 2
        assert instruments[0] == MinimalInstrument(secid=101, display_symbol="AAPL")
        assert instruments[1] == MinimalInstrument(secid=202, display_symbol="META")

    def test_history_table_passes_date_parameter(
        self, resolver_with_history, mock_client
    ):
        """The universe_date is passed as a query parameter for date filtering."""
        mock_client.query.return_value = MockQueryResult([])

        resolver_with_history.resolve_all_secids(universe_date=date(2023, 6, 15))

        call_kwargs = mock_client.query.call_args.kwargs
        assert call_kwargs["parameters"]["universe_date"] == date(2023, 6, 15)

    def test_history_table_empty_result(self, resolver_with_history, mock_client):
        """Empty history table returns an empty list."""
        mock_client.query.return_value = MockQueryResult([])

        instruments = resolver_with_history.resolve_all_secids(
            universe_date=date(2023, 6, 15)
        )

        assert instruments == []

    # --- Secmaster fallback path ---

    def test_secmaster_fallback_returns_minimal_instruments(
        self, resolver_no_history, mock_client
    ):
        """Secmaster fallback returns MinimalInstrument objects."""
        mock_client.query.return_value = MockQueryResult(
            [
                (101, "AAPL", "19801212-"),
                (202, "FB;META", "20120518-20220608;20220609-"),
            ]
        )

        instruments = resolver_no_history.resolve_all_secids(
            universe_date=date(2023, 6, 15)
        )

        assert len(instruments) == 2
        assert instruments[0] == MinimalInstrument(secid=101, display_symbol="AAPL")
        assert instruments[1] == MinimalInstrument(secid=202, display_symbol="META")

    def test_secmaster_fallback_uses_active_ticker(
        self, resolver_no_history, mock_client
    ):
        """display_symbol is the ticker active on universe_date, not the last ticker in history."""
        # FB was active 2012-2022, META 2022-.  On 2020-06-15, FB is the active ticker.
        mock_client.query.return_value = MockQueryResult(
            [
                (202, "FB;META", "20120518-20220608;20220609-"),
            ]
        )

        instruments = resolver_no_history.resolve_all_secids(
            universe_date=date(2020, 6, 15)
        )

        assert len(instruments) == 1
        assert instruments[0] == MinimalInstrument(secid=202, display_symbol="FB")

    def test_secmaster_fallback_empty_result(self, resolver_no_history, mock_client):
        """Empty secmaster returns an empty list."""
        mock_client.query.return_value = MockQueryResult([])

        instruments = resolver_no_history.resolve_all_secids(
            universe_date=date(2023, 6, 15)
        )

        assert instruments == []

    # --- Fallback: history table query fails ---

    def test_history_failure_falls_back_to_secmaster(
        self, resolver_with_history, mock_client
    ):
        """When the history table query fails, fall back to secmaster path."""
        # First call (history table) raises, second call (secmaster) succeeds
        mock_client.query.side_effect = [
            RuntimeError("Connection refused"),
            MockQueryResult(
                [(303, "TSLA", "20100629-")]
            ),
        ]

        instruments = resolver_with_history.resolve_all_secids(
            universe_date=date(2023, 6, 15)
        )

        assert len(instruments) == 1
        assert instruments[0] == MinimalInstrument(secid=303, display_symbol="TSLA")
        # Two queries were attempted
        assert mock_client.query.call_count == 2
