"""Tests for InstrumentResolver ticker history table integration.

Tests the normalized ``as_secmaster_ticker_history`` table path:
- Resolution via ticker_history_table parameter
- Time-travel queries (ticker active at a specific date)
- Fallback to secmaster array parsing when history table is empty
- anchor_first_in_range and fail_on_ambiguity policies with history table
- Table name validation
"""

from datetime import date
from unittest.mock import MagicMock

import pytest

from qs_trader.services.data.instrument_resolver import (
    InstrumentResolver,
    SecmasterAuthorityError,
)
from qs_trader.services.data.models import IdentitySource


class MockQueryResult:
    """Mock ClickHouse query result."""

    def __init__(self, rows):
        self.result_rows = rows


class TestTickerHistoryTable:
    """Test InstrumentResolver with ticker_history_table configured."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.resolver = InstrumentResolver(
            clickhouse_client=self.mock_client,
            database="market",
            cache_ttl_seconds=3600,
            ticker_history_table="as_secmaster_ticker_history",
        )

    def test_resolve_by_ticker_from_history_table(self):
        """Test resolving a ticker via the normalized history table."""
        # Mock history table query result
        self.mock_client.query.return_value = MockQueryResult(
            [
                (3513095, "FB", date(2012, 5, 18), date(2022, 6, 8), "L"),
                (3513095, "META", date(2022, 6, 9), None, "L"),
            ]
        )

        resolved = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2025, 12, 31)),
            policy="anchor_first_in_range",
        )

        assert resolved.secid == 3513095
        assert resolved.requested_symbol == "META"
        assert resolved.identity_source == IdentitySource.TICKER_POINT_IN_TIME
        assert resolved.ambiguous is False
        assert len(resolved.ticker_history) == 2

    def test_resolve_by_ticker_history_table_not_found(self):
        """Test SecmasterAuthorityError when ticker not in history table."""
        self.mock_client.query.return_value = MockQueryResult([])

        with pytest.raises(SecmasterAuthorityError) as exc_info:
            self.resolver.resolve_by_ticker(
                ticker="INVALID",
                date_range=(date(2020, 1, 1), date(2025, 12, 31)),
            )

        assert "INVALID" in str(exc_info.value)
        assert exc_info.value.ticker == "INVALID"

    def test_resolve_by_ticker_history_table_anchor_first_in_range(self):
        """Test anchor_first_in_range with history table (ticker reuse)."""
        # ABC ticker used by secid=1 (2020-2021) then secid=2 (2022-)
        self.mock_client.query.return_value = MockQueryResult(
            [
                (1, "ABC", date(2020, 1, 1), date(2021, 1, 1), "L"),
                (2, "ABC", date(2022, 1, 1), None, "L"),
            ]
        )

        resolved = self.resolver.resolve_by_ticker(
            ticker="ABC",
            date_range=(date(2020, 1, 1), date(2023, 12, 31)),
            policy="anchor_first_in_range",
        )

        assert resolved.secid == 1
        assert resolved.ambiguous is False

    def test_resolve_by_ticker_history_table_fail_on_ambiguity(self):
        """Test fail_on_ambiguity with history table."""
        self.mock_client.query.return_value = MockQueryResult(
            [
                (1, "ABC", date(2020, 1, 1), date(2021, 1, 1), "L"),
                (2, "ABC", date(2022, 1, 1), None, "L"),
            ]
        )

        with pytest.raises(SecmasterAuthorityError) as exc_info:
            self.resolver.resolve_by_ticker(
                ticker="ABC",
                date_range=(date(2020, 1, 1), date(2023, 12, 31)),
                policy="fail_on_ambiguity",
            )

        assert "maps to 2 secids" in str(exc_info.value)
        assert "Ambiguity must be resolved explicitly" in str(exc_info.value)

    def test_resolve_by_ticker_history_table_time_travel(self):
        """Test time-travel: resolve ticker active at a specific historical date."""
        # FB was the ticker in 2020, META from 2022 onward
        self.mock_client.query.return_value = MockQueryResult(
            [
                (3513095, "FB", date(2012, 5, 18), date(2022, 6, 8), "L"),
                (3513095, "META", date(2022, 6, 9), None, "L"),
            ]
        )

        # Request FB in 2020 — should resolve to secid 3513095 with ticker_at_date="FB"
        resolved = self.resolver.resolve_by_ticker(
            ticker="FB",
            date_range=(date(2020, 1, 1), date(2020, 12, 31)),
            policy="anchor_first_in_range",
        )

        assert resolved.secid == 3513095
        assert resolved.ticker_at_date == "FB"
        assert resolved.display_symbol == "META"  # Most recent ticker

    def test_resolve_by_ticker_history_table_current_ticker(self):
        """Test resolving a current ticker (end_date=NULL)."""
        self.mock_client.query.return_value = MockQueryResult(
            [
                (3513095, "META", date(2022, 6, 9), None, "L"),
            ]
        )

        resolved = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2024, 1, 1), date(2025, 12, 31)),
        )

        assert resolved.secid == 3513095
        assert resolved.ticker_at_date == "META"

    def test_fallback_to_secmaster_when_history_empty(self):
        """Test fallback to secmaster array parsing when history table returns no rows."""
        call_count = {"n": 0}

        def mock_query(query, parameters):
            call_count["n"] += 1
            if "as_secmaster_ticker_history" in query:
                # History table empty — trigger fallback
                return MockQueryResult([])
            # Secmaster fallback
            return MockQueryResult(
                [
                    (3513095, "META", "20220609-"),
                ]
            )

        self.mock_client.query.side_effect = mock_query

        resolved = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        assert resolved.secid == 3513095
        # Should have queried history table first, then secmaster
        assert call_count["n"] == 2

    def test_fallback_to_secmaster_when_history_table_error(self):
        """Test fallback when history table query raises an exception."""
        call_count = {"n": 0}

        def mock_query(query, parameters):
            call_count["n"] += 1
            if "as_secmaster_ticker_history" in query:
                raise Exception("table not found")
            return MockQueryResult(
                [
                    (3513095, "META", "20220609-"),
                ]
            )

        self.mock_client.query.side_effect = mock_query

        resolved = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        assert resolved.secid == 3513095
        assert call_count["n"] == 2

    def test_no_fallback_when_history_table_not_configured(self):
        """Test that resolver without ticker_history_table uses secmaster directly."""
        resolver_no_history = InstrumentResolver(
            clickhouse_client=self.mock_client,
            database="market",
        )

        self.mock_client.query.return_value = MockQueryResult(
            [
                (3513095, "META", "20220609-"),
            ]
        )

        resolved = resolver_no_history.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        assert resolved.secid == 3513095
        # Only one query — no history table attempt
        assert self.mock_client.query.call_count == 1

    def test_ticker_history_table_name_validation(self):
        """Test table name validation in __init__."""
        # Valid table names
        r1 = InstrumentResolver(
            self.mock_client, ticker_history_table="as_secmaster_ticker_history"
        )
        assert r1._ticker_history_table == "as_secmaster_ticker_history"

        r2 = InstrumentResolver(self.mock_client, ticker_history_table="ticker_history")
        assert r2._ticker_history_table == "ticker_history"

        # Invalid table names
        with pytest.raises(ValueError) as exc_info:
            InstrumentResolver(
                self.mock_client, ticker_history_table="ticker-history"
            )
        assert "Invalid ticker_history_table name" in str(exc_info.value)

        with pytest.raises(ValueError):
            InstrumentResolver(
                self.mock_client, ticker_history_table="123invalid"
            )

    def test_history_table_rows_exist_but_no_date_overlap(self):
        """Test error when history table has rows but none overlap the date range."""
        # Ticker existed only in 2015, but query is for 2023-2025
        self.mock_client.query.return_value = MockQueryResult(
            [
                (99999, "OLD", date(2015, 1, 1), date(2015, 12, 31), "D"),
            ]
        )

        with pytest.raises(SecmasterAuthorityError) as exc_info:
            self.resolver.resolve_by_ticker(
                ticker="OLD",
                date_range=(date(2023, 1, 1), date(2025, 12, 31)),
            )

        assert "no valid mappings" in str(exc_info.value)

    def test_history_table_caching(self):
        """Test that history table results are cached."""
        self.mock_client.query.return_value = MockQueryResult(
            [
                (3513095, "META", date(2022, 6, 9), None, "L"),
            ]
        )

        resolved1 = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        resolved2 = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        # Should only query once (cached)
        assert self.mock_client.query.call_count == 1
        assert resolved1.secid == resolved2.secid
