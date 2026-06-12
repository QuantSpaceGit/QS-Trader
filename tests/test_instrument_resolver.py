"""Tests for InstrumentResolver.

Tests secmaster-authoritative instrument resolution with:
- anchor_first_in_range policy
- fail_on_ambiguity policy
- SecmasterAuthorityError
- Ticker history parsing
- Caching
- Batch resolution
"""

from datetime import date
from unittest.mock import MagicMock

import pytest

from qs_trader.services.data.instrument_resolver import (
    InstrumentResolver,
    SecmasterAuthorityError,
    TickerHistory,
)
from qs_trader.services.data.models import IdentitySource


class MockQueryResult:
    """Mock ClickHouse query result."""

    def __init__(self, rows):
        self.result_rows = rows


class TestInstrumentResolver:
    """Test InstrumentResolver functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_client = MagicMock()
        self.resolver = InstrumentResolver(
            clickhouse_client=self.mock_client,
            database="market",
            cache_ttl_seconds=3600,
        )

    def test_resolve_by_ticker_single_match(self):
        """Test resolving a ticker with a single secmaster match."""
        # Mock secmaster query result: META with ticker history FB->META
        self.mock_client.query.return_value = MockQueryResult(
            [
                (
                    3513095,  # secid
                    "FB;META",  # tickers
                    "20120518-20220608;20220609-",  # tickersstarttoenddate
                )
            ]
        )

        resolved = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2020, 1, 1), date(2025, 12, 31)),
            policy="anchor_first_in_range",
        )

        assert resolved.secid == 3513095
        assert resolved.requested_symbol == "META"
        assert resolved.display_symbol == "META"
        assert resolved.identity_source == IdentitySource.TICKER_POINT_IN_TIME
        assert resolved.ambiguous is False
        assert len(resolved.ticker_history) == 2

    def test_resolve_by_ticker_not_found(self):
        """Test SecmasterAuthorityError when ticker not in secmaster."""
        self.mock_client.query.return_value = MockQueryResult([])

        with pytest.raises(SecmasterAuthorityError) as exc_info:
            self.resolver.resolve_by_ticker(
                ticker="INVALID",
                date_range=(date(2020, 1, 1), date(2025, 12, 31)),
            )

        assert "INVALID" in str(exc_info.value)
        assert exc_info.value.ticker == "INVALID"

    def test_resolve_by_ticker_anchor_first_in_range(self):
        """Test anchor_first_in_range policy with ticker change."""
        # Mock: ABC ticker changed from secid=1 to secid=2
        self.mock_client.query.return_value = MockQueryResult(
            [
                (
                    1,  # secid
                    "ABC",  # tickers
                    "20200101-20210101",  # tickersstarttoenddate
                ),
                (
                    2,  # secid (reused ticker)
                    "ABC",  # tickers
                    "20220101-",  # tickersstarttoenddate
                ),
            ]
        )

        # Request ABC starting from 2020 - should anchor to secid=1
        resolved = self.resolver.resolve_by_ticker(
            ticker="ABC",
            date_range=(date(2020, 1, 1), date(2023, 12, 31)),
            policy="anchor_first_in_range",
        )

        assert resolved.secid == 1
        assert resolved.ambiguous is False

    def test_resolve_by_ticker_fail_on_ambiguity(self):
        """Test fail_on_ambiguity policy raises exception when ambiguous."""
        # Mock: ABC ticker used by two different secids
        self.mock_client.query.return_value = MockQueryResult(
            [
                (
                    1,
                    "ABC",
                    "20200101-20210101",
                ),
                (
                    2,
                    "ABC",
                    "20220101-",
                ),
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
        assert exc_info.value.ticker == "ABC"

    def test_resolve_by_secid(self):
        """Test resolving by explicit secid."""
        self.mock_client.query.return_value = MockQueryResult(
            [
                (
                    3513095,
                    "FB;META",
                    "20120518-20220608;20220609-",
                )
            ]
        )

        resolved = self.resolver.resolve_by_secid(
            secid=3513095,
            date_range=(date(2020, 1, 1), date(2025, 12, 31)),
        )

        assert resolved.secid == 3513095
        assert resolved.identity_source == IdentitySource.EXPLICIT_SECID
        assert resolved.display_symbol == "META"  # Most recent ticker

    def test_resolve_by_secid_not_found(self):
        """Test SecmasterAuthorityError when secid not in secmaster."""
        self.mock_client.query.return_value = MockQueryResult([])

        with pytest.raises(SecmasterAuthorityError) as exc_info:
            self.resolver.resolve_by_secid(
                secid=99999,
                date_range=(date(2020, 1, 1), date(2025, 12, 31)),
            )

        assert "99999" in str(exc_info.value)
        assert exc_info.value.secid == 99999

    def test_ticker_history_parsing(self):
        """Test parsing of semicolon-delimited ticker history."""
        tickers_str = "FB;META"
        dates_str = "20120518-20220608;20220609-"

        history = self.resolver._parse_ticker_history(tickers_str, dates_str)

        assert len(history) == 2
        assert history[0].ticker == "FB"
        assert history[0].start_date == date(2012, 5, 18)
        assert history[0].end_date == date(2022, 6, 8)
        assert history[1].ticker == "META"
        assert history[1].start_date == date(2022, 6, 9)
        assert history[1].end_date is None  # Current ticker

    def test_ticker_history_parsing_single_ticker(self):
        """Test parsing single ticker with no end date."""
        tickers_str = "AAPL"
        dates_str = "19801212-"

        history = self.resolver._parse_ticker_history(tickers_str, dates_str)

        assert len(history) == 1
        assert history[0].ticker == "AAPL"
        assert history[0].start_date == date(1980, 12, 12)
        assert history[0].end_date is None

    def test_overlaps_date_range(self):
        """Test date range overlap detection."""
        # Ticker active 2020-2021
        th = TickerHistory(
            ticker="TEST",
            start_date=date(2020, 1, 1),
            end_date=date(2021, 12, 31),
        )

        # Overlaps
        assert self.resolver._overlaps_date_range(th, date(2020, 6, 1), date(2020, 12, 31))
        assert self.resolver._overlaps_date_range(th, date(2019, 1, 1), date(2022, 12, 31))
        assert self.resolver._overlaps_date_range(th, date(2021, 1, 1), date(2021, 6, 30))

        # No overlap
        assert not self.resolver._overlaps_date_range(th, date(2022, 1, 1), date(2022, 12, 31))
        assert not self.resolver._overlaps_date_range(th, date(2019, 1, 1), date(2019, 12, 31))

    def test_overlaps_date_range_current_ticker(self):
        """Test overlap with current ticker (end_date=None)."""
        th = TickerHistory(
            ticker="META",
            start_date=date(2022, 6, 9),
            end_date=None,  # Current
        )

        # Overlaps with any range after start
        assert self.resolver._overlaps_date_range(th, date(2023, 1, 1), date(2023, 12, 31))
        assert self.resolver._overlaps_date_range(th, date(2024, 1, 1), date(2024, 12, 31))

        # No overlap before start
        assert not self.resolver._overlaps_date_range(th, date(2020, 1, 1), date(2021, 12, 31))

    def test_caching(self):
        """Test resolution caching."""
        self.mock_client.query.return_value = MockQueryResult(
            [
                (
                    3513095,
                    "META",
                    "20220609-",
                )
            ]
        )

        # First call - should query
        resolved1 = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        # Second call - should use cache
        resolved2 = self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        # Should only query once
        assert self.mock_client.query.call_count == 1
        assert resolved1.secid == resolved2.secid

    def test_cache_clear(self):
        """Test cache clearing."""
        self.mock_client.query.return_value = MockQueryResult(
            [
                (
                    3513095,
                    "META",
                    "20220609-",
                )
            ]
        )

        # First call
        self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        # Clear cache
        self.resolver.clear_cache()

        # Second call - should query again
        self.resolver.resolve_by_ticker(
            ticker="META",
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        # Should query twice
        assert self.mock_client.query.call_count == 2

    def test_batch_resolution(self):
        """Test batch resolution of multiple tickers."""
        # Mock batch query result
        self.mock_client.query.return_value = MockQueryResult(
            [
                (3513095, "META", "20220609-"),
                (12345, "AAPL", "19801212-"),
            ]
        )

        results = self.resolver.resolve_batch(
            tickers=["META", "AAPL"],
            date_range=(date(2023, 1, 1), date(2023, 12, 31)),
        )

        assert len(results) == 2
        assert results["META"].secid == 3513095
        assert results["AAPL"].secid == 12345
        # Verify only one query was made (batch query)
        assert self.mock_client.query.call_count == 1

    def test_batch_resolution_failure(self):
        """Test batch resolution with one failing ticker."""
        def mock_query(query, parameters):
            ticker = parameters.get("ticker")
            if ticker == "META":
                return MockQueryResult([(3513095, "META", "20220609-")])
            else:
                return MockQueryResult([])

        self.mock_client.query.side_effect = mock_query

        with pytest.raises(SecmasterAuthorityError):
            self.resolver.resolve_batch(
                tickers=["META", "INVALID"],
                date_range=(date(2023, 1, 1), date(2023, 12, 31)),
            )

    def test_invalid_policy(self):
        """Test error on invalid resolution policy."""
        self.mock_client.query.return_value = MockQueryResult(
            [
                (
                    3513095,
                    "META",
                    "20220609-",
                )
            ]
        )

        with pytest.raises(ValueError) as exc_info:
            self.resolver.resolve_by_ticker(
                ticker="META",
                date_range=(date(2023, 1, 1), date(2023, 12, 31)),
                policy="invalid_policy",
            )

        assert "Invalid policy" in str(exc_info.value)

    def test_ticker_at_date_historical(self):
        """Test ticker_at_date returns correct ticker for historical date range."""
        # Mock: FB ticker changed to META in 2022
        self.mock_client.query.return_value = MockQueryResult(
            [
                (
                    3513095,
                    "FB;META",
                    "20120518-20220608;20220609-",
                )
            ]
        )

        # Request FB in 2020 - should return ticker_at_date="FB"
        resolved = self.resolver.resolve_by_ticker(
            ticker="FB",
            date_range=(date(2020, 1, 1), date(2020, 12, 31)),
            policy="anchor_first_in_range",
        )

        assert resolved.secid == 3513095
        assert resolved.ticker_at_date == "FB"  # Active in 2020
        assert resolved.display_symbol == "META"  # Most recent ticker

    def test_database_validation(self):
        """Test database name validation."""
        # Valid database names
        resolver1 = InstrumentResolver(self.mock_client, database="market")
        assert resolver1._database == "market"

        resolver2 = InstrumentResolver(self.mock_client, database="market_data")
        assert resolver2._database == "market_data"

        resolver3 = InstrumentResolver(self.mock_client, database="_private")
        assert resolver3._database == "_private"

        # Invalid database names
        with pytest.raises(ValueError) as exc_info:
            InstrumentResolver(self.mock_client, database="market; DROP TABLE foo;--")
        assert "Invalid database name" in str(exc_info.value)

        with pytest.raises(ValueError):
            InstrumentResolver(self.mock_client, database="123invalid")

        with pytest.raises(ValueError):
            InstrumentResolver(self.mock_client, database="market-data")
