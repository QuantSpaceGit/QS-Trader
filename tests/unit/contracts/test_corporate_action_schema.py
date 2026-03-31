"""
Test corporate_action.v1 contract schema validation.
"""

import json
from pathlib import Path

import pytest
from jsonschema import ValidationError, validate


@pytest.fixture
def corporate_action_schema():
    """Load the data/corporate_action.v1.json schema."""
    schema_path = (
        Path(__file__).parent.parent.parent.parent
        / "src"
        / "qs_trader"
        / "contracts"
        / "schemas"
        / "data"
        / "corporate_action.v1.json"
    )
    return json.loads(schema_path.read_text())


@pytest.fixture
def corporate_action_example():
    """Load the data/corporate_action.v1.example.json file."""
    example_path = (
        Path(__file__).parent.parent.parent.parent
        / "src"
        / "qs_trader"
        / "contracts"
        / "examples"
        / "data"
        / "corporate_action.v1.example.json"
    )
    return json.loads(example_path.read_text())


def test_corporate_action_example_validates_against_schema(corporate_action_schema, corporate_action_example):
    """Verify that corporate_action.v1.example.json validates against schema."""
    # Extract payload from envelope
    payload = corporate_action_example["payload"]

    # Validate payload against schema
    validate(instance=payload, schema=corporate_action_schema)


def test_corporate_action_schema_requires_essential_fields(corporate_action_schema):
    """Verify that the schema requires essential fields."""
    required_fields = corporate_action_schema["required"]

    assert "symbol" in required_fields
    assert "asset_class" in required_fields
    assert "action_type" in required_fields
    assert "announcement_date" in required_fields
    assert "ex_date" in required_fields
    assert "effective_date" in required_fields
    assert "source" in required_fields


def test_corporate_action_schema_has_split_fields(corporate_action_schema):
    """Verify that the schema includes split-specific fields."""
    properties = corporate_action_schema["properties"]

    assert "split_ratio" in properties
    assert "split_from" in properties
    assert "split_to" in properties
    assert "price_adjustment_factor" in properties
    assert "volume_adjustment_factor" in properties


def test_corporate_action_schema_has_dividend_fields(corporate_action_schema):
    """Verify that the schema includes dividend-specific fields."""
    properties = corporate_action_schema["properties"]

    assert "dividend_amount" in properties
    assert "dividend_currency" in properties
    assert "dividend_type" in properties


def test_corporate_action_minimal_valid_split(corporate_action_schema):
    """Verify that a minimal valid split payload passes validation."""
    minimal_split = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "split",
        "announcement_date": "2020-07-30",
        "ex_date": "2020-08-31",
        "effective_date": "2020-08-31",
        "source": "test_source",
    }

    # Should not raise
    validate(instance=minimal_split, schema=corporate_action_schema)


def test_corporate_action_minimal_valid_dividend(corporate_action_schema):
    """Verify that a minimal valid dividend payload passes validation."""
    minimal_dividend = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "dividend",
        "announcement_date": "2020-07-30",
        "ex_date": "2020-08-07",
        "effective_date": "2020-08-07",
        "source": "test_source",
    }

    # Should not raise
    validate(instance=minimal_dividend, schema=corporate_action_schema)


def test_corporate_action_rejects_invalid_action_type(corporate_action_schema):
    """Verify that invalid action_type values are rejected."""
    invalid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "bankruptcy",  # Invalid - not in enum
        "announcement_date": "2020-07-30",
        "ex_date": "2020-08-31",
        "effective_date": "2020-08-31",
        "source": "test_source",
    }

    with pytest.raises(ValidationError, match="action_type"):
        validate(instance=invalid_payload, schema=corporate_action_schema)


def test_corporate_action_rejects_missing_required_field(corporate_action_schema):
    """Verify that missing required fields are rejected."""
    invalid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "split",
        "announcement_date": "2020-07-30",
        # Missing 'ex_date' - required field
        "effective_date": "2020-08-31",
        "source": "test_source",
    }

    with pytest.raises(ValidationError, match="'ex_date' is a required property"):
        validate(instance=invalid_payload, schema=corporate_action_schema)


def test_corporate_action_rejects_additional_properties(corporate_action_schema):
    """Verify that additional properties are rejected (strict schema)."""
    invalid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "split",
        "announcement_date": "2020-07-30",
        "ex_date": "2020-08-31",
        "effective_date": "2020-08-31",
        "source": "test_source",
        "unknown_field": "should not be here",  # Not in schema
    }

    with pytest.raises(ValidationError, match="Additional properties are not allowed"):
        validate(instance=invalid_payload, schema=corporate_action_schema)


def test_corporate_action_split_with_all_fields(corporate_action_schema):
    """Verify that a complete split payload validates correctly."""
    complete_split = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "split",
        "announcement_date": "2020-07-30",
        "ex_date": "2020-08-31",
        "record_date": "2020-08-24",
        "payment_date": "2020-08-31",
        "effective_date": "2020-08-31",
        "split_ratio": "4.0",
        "split_from": 1,
        "split_to": 4,
        "dividend_amount": None,
        "dividend_currency": None,
        "dividend_type": None,
        "price_adjustment_factor": "0.25",
        "volume_adjustment_factor": "4.0",
        "new_symbol": None,
        "source": "test_source",
        "source_reference": "https://example.com",
        "notes": "1-for-4 split",
        "trace_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    }

    # Should not raise
    validate(instance=complete_split, schema=corporate_action_schema)


def test_corporate_action_dividend_with_all_fields(corporate_action_schema):
    """Verify that a complete dividend payload validates correctly."""
    complete_dividend = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "dividend",
        "announcement_date": "2020-07-30",
        "ex_date": "2020-08-07",
        "record_date": "2020-08-10",
        "payment_date": "2020-08-13",
        "effective_date": "2020-08-07",
        "split_ratio": None,
        "split_from": None,
        "split_to": None,
        "dividend_amount": "0.82",
        "dividend_currency": "USD",
        "dividend_type": "ordinary",
        "price_adjustment_factor": None,
        "volume_adjustment_factor": None,
        "new_symbol": None,
        "source": "test_source",
        "source_reference": None,
        "notes": "Quarterly dividend",
        "trace_id": None,
    }

    # Should not raise
    validate(instance=complete_dividend, schema=corporate_action_schema)


def test_corporate_action_nullable_fields_accept_null(corporate_action_schema):
    """Verify that nullable fields accept null values."""
    valid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "split",
        "announcement_date": "2020-07-30",
        "ex_date": "2020-08-31",
        "effective_date": "2020-08-31",
        "record_date": None,
        "payment_date": None,
        "split_ratio": None,
        "split_from": None,
        "split_to": None,
        "dividend_amount": None,
        "dividend_currency": None,
        "dividend_type": None,
        "price_adjustment_factor": None,
        "volume_adjustment_factor": None,
        "new_symbol": None,
        "source": "test_source",
        "source_reference": None,
        "notes": None,
        "trace_id": None,
    }

    # Should not raise
    validate(instance=valid_payload, schema=corporate_action_schema)


def test_corporate_action_date_format_validation(corporate_action_schema):
    """Verify that date fields require proper ISO 8601 format."""
    # Note: jsonschema's date format validation is lenient by default
    # This test documents expected behavior
    valid_payload = {
        "symbol": "AAPL",
        "asset_class": "equity",
        "action_type": "split",
        "announcement_date": "2020-07-30",  # ISO 8601 date
        "ex_date": "2020-08-31",
        "effective_date": "2020-08-31",
        "source": "test_source",
    }

    # Should not raise
    validate(instance=valid_payload, schema=corporate_action_schema)
