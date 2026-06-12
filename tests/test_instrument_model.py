"""Tests for Instrument model with identity fields.

Tests the backward-compatible addition of secid, display_symbol,
ticker_at_date, and identity_source fields to the Instrument model.
"""

import pytest
from pydantic import ValidationError

from qs_trader.services.data.models import IdentitySource, Instrument


class TestInstrumentIdentityFields:
    """Test identity fields on Instrument model."""

    def test_instrument_basic_creation(self):
        """Test basic instrument creation without identity fields (backward compatibility)."""
        instrument = Instrument(symbol="AAPL")

        assert instrument.symbol == "AAPL"
        assert instrument.frequency is None
        assert instrument.metadata == {}
        assert instrument.secid is None
        assert instrument.display_symbol is None
        assert instrument.ticker_at_date is None
        assert instrument.identity_source is None

    def test_instrument_with_identity_fields(self):
        """Test instrument creation with all identity fields."""
        instrument = Instrument(
            symbol="META",
            secid=3513095,
            display_symbol="META",
            ticker_at_date="META",
            identity_source=IdentitySource.TICKER_POINT_IN_TIME,
        )

        assert instrument.symbol == "META"
        assert instrument.secid == 3513095
        assert instrument.display_symbol == "META"
        assert instrument.ticker_at_date == "META"
        assert instrument.identity_source == IdentitySource.TICKER_POINT_IN_TIME

    def test_instrument_with_explicit_secid(self):
        """Test instrument with explicit secid identity source."""
        instrument = Instrument(
            symbol="AAPL",
            secid=12345,
            display_symbol="AAPL",
            ticker_at_date="AAPL",
            identity_source=IdentitySource.EXPLICIT_SECID,
        )

        assert instrument.identity_source == IdentitySource.EXPLICIT_SECID

    def test_instrument_with_legacy_symbol(self):
        """Test instrument with legacy symbol identity source."""
        instrument = Instrument(
            symbol="OLD",
            identity_source=IdentitySource.LEGACY_SYMBOL,
        )

        assert instrument.identity_source == IdentitySource.LEGACY_SYMBOL
        assert instrument.secid is None

    def test_instrument_backward_compatibility(self):
        """Test that existing code without identity fields still works."""
        # Old-style creation
        instrument1 = Instrument(symbol="MSFT")
        assert instrument1.symbol == "MSFT"
        assert instrument1.secid is None

        # With frequency
        instrument2 = Instrument(symbol="BTCUSD", frequency="1m")
        assert instrument2.symbol == "BTCUSD"
        assert instrument2.frequency == "1m"
        assert instrument2.secid is None

        # With metadata
        instrument3 = Instrument(
            symbol="ES_Z24",
            metadata={"contract_month": "2024-12", "exchange": "CME"},
        )
        assert instrument3.symbol == "ES_Z24"
        assert instrument3.metadata["contract_month"] == "2024-12"
        assert instrument3.secid is None

    def test_instrument_immutability(self):
        """Test that instrument is immutable (frozen=True)."""
        instrument = Instrument(symbol="AAPL", secid=12345)

        with pytest.raises(ValidationError):
            instrument.symbol = "MSFT"

        with pytest.raises(ValidationError):
            instrument.secid = 99999

    def test_instrument_repr(self):
        """Test instrument string representation."""
        instrument1 = Instrument(symbol="AAPL")
        assert "AAPL" in repr(instrument1)

        instrument2 = Instrument(symbol="BTCUSD", frequency="1m")
        assert "BTCUSD" in repr(instrument2)
        assert "1m" in repr(instrument2)

        instrument3 = Instrument(
            symbol="ES_Z24",
            metadata={"exchange": "CME"},
        )
        assert "ES_Z24" in repr(instrument3)

    def test_identity_source_enum_values(self):
        """Test IdentitySource enum values."""
        assert IdentitySource.EXPLICIT_SECID.value == "explicit_secid"
        assert IdentitySource.TICKER_POINT_IN_TIME.value == "ticker_point_in_time"
        assert IdentitySource.LEGACY_SYMBOL.value == "legacy_symbol"

    def test_instrument_partial_identity_fields(self):
        """Test instrument with only some identity fields set."""
        # Only secid
        instrument1 = Instrument(symbol="AAPL", secid=12345)
        assert instrument1.secid == 12345
        assert instrument1.display_symbol is None

        # Only display_symbol
        instrument2 = Instrument(symbol="META", display_symbol="META")
        assert instrument2.display_symbol == "META"
        assert instrument2.secid is None

        # secid + display_symbol but no identity_source
        instrument3 = Instrument(
            symbol="GOOGL",
            secid=99999,
            display_symbol="GOOGL",
        )
        assert instrument3.secid == 99999
        assert instrument3.display_symbol == "GOOGL"
        assert instrument3.identity_source is None

    def test_instrument_with_all_fields(self):
        """Test instrument with all possible fields set."""
        instrument = Instrument(
            symbol="META",
            frequency="1d",
            metadata={"exchange": "NASDAQ"},
            secid=3513095,
            display_symbol="META",
            ticker_at_date="META",
            identity_source=IdentitySource.TICKER_POINT_IN_TIME,
        )

        assert instrument.symbol == "META"
        assert instrument.frequency == "1d"
        assert instrument.metadata["exchange"] == "NASDAQ"
        assert instrument.secid == 3513095
        assert instrument.display_symbol == "META"
        assert instrument.ticker_at_date == "META"
        assert instrument.identity_source == IdentitySource.TICKER_POINT_IN_TIME
