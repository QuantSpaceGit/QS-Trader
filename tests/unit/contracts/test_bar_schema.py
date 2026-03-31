"""
Test bar.v1 contract schema validation.
"""

import json
from pathlib import Path

import pytest
from jsonschema import ValidationError, validate


@pytest.fixture
def bar_schema():
    """Load the data/bar.v1.json schema."""
    schema_path = (
        Path(__file__).parent.parent.parent.parent
        / "src"
        / "qs_trader"
        / "contracts"
        / "schemas"
        / "data"
        / "bar.v1.json"
    )
    return json.loads(schema_path.read_text())


@pytest.fixture
def bar_example():
    """Load the data/bar.v1.example.json file."""
    example_path = (
        Path(__file__).parent.parent.parent.parent
        / "src"
        / "qs_trader"
        / "contracts"
        / "examples"
        / "data"
        / "bar.v1.example.json"
    )
    return json.loads(example_path.read_text())


def test_bar_example_validates_against_schema(bar_schema, bar_example):
    """Verify that bar.v1.example.json validates against bar.v1.json schema."""
    # Extract payload from envelope
    payload = bar_example["payload"]

    # Validate payload against schema
    validate(instance=payload, schema=bar_schema)


def test_bar_schema_requires_essential_fields(bar_schema):
    """Verify that the schema requires essential OHLCV fields."""
    required_fields = bar_schema["required"]

    assert "symbol" in required_fields
    assert "asset_class" in required_fields
    assert "interval" in required_fields
    assert "timestamp" in required_fields
    assert "open" in required_fields
    assert "high" in required_fields
    assert "low" in required_fields
    assert "close" in required_fields
    assert "volume" in required_fields


def test_bar_minimal_valid_payload(bar_schema):
    """Verify that a minimal valid payload passes validation."""
    minimal_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "interval": "1d",
        "timestamp": "2020-08-31T00:00:00Z",
        "open": "127.67",
        "high": "131.0",
        "low": "126.25",
        "close": "129.04",
        "volume": "210249674",
    }

    # Should not raise
    validate(instance=minimal_payload, schema=bar_schema)


def test_bar_rejects_invalid_interval(bar_schema):
    """Verify that invalid interval values are rejected."""
    invalid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "interval": "5y",  # Invalid - not in enum
        "timestamp": "2020-08-31T00:00:00Z",
        "open": "127.67",
        "high": "131.0",
        "low": "126.25",
        "close": "129.04",
        "volume": "210249674",
    }

    with pytest.raises(ValidationError, match="interval"):
        validate(instance=invalid_payload, schema=bar_schema)


def test_bar_rejects_missing_required_field(bar_schema):
    """Verify that missing required fields are rejected."""
    invalid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "interval": "1d",
        "timestamp": "2020-08-31T00:00:00Z",
        "open": "127.67",
        "high": "131.0",
        "low": "126.25",
        # Missing 'close' - required field
        "volume": "210249674",
    }

    with pytest.raises(ValidationError, match="'close' is a required property"):
        validate(instance=invalid_payload, schema=bar_schema)


def test_bar_rejects_additional_properties(bar_schema):
    """Verify that additional properties are rejected (strict schema)."""
    invalid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "interval": "1d",
        "timestamp": "2020-08-31T00:00:00Z",
        "open": "127.67",
        "high": "131.0",
        "low": "126.25",
        "close": "129.04",
        "volume": "210249674",
        "unknown_field": "should not be here",  # Not in schema
    }

    with pytest.raises(ValidationError, match="Additional properties are not allowed"):
        validate(instance=invalid_payload, schema=bar_schema)


def test_bar_decimal_fields_accept_strings(bar_schema):
    """Verify that price fields accept decimal strings."""
    valid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "interval": "1d",
        "timestamp": "2020-08-31T00:00:00Z",
        "open": "127.67",
        "high": "131.0",
        "low": "126.25",
        "close": "129.04",
        "volume": "210249674",
    }

    # Should not raise
    validate(instance=valid_payload, schema=bar_schema)
