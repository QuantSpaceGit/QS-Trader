"""
Test manager/order.v1 contract schema validation.
"""

import json
from pathlib import Path

import pytest
from jsonschema import ValidationError, validate


@pytest.fixture
def order_schema():
    """Load the manager/order.v1.json schema."""
    schema_path = (
        Path(__file__).parent.parent.parent.parent
        / "src"
        / "qs_trader"
        / "contracts"
        / "schemas"
        / "manager"
        / "order.v1.json"
    )
    return json.loads(schema_path.read_text())


@pytest.fixture
def order_example():
    """Load the manager/order.v1.example.json file."""
    example_path = (
        Path(__file__).parent.parent.parent.parent
        / "src"
        / "qs_trader"
        / "contracts"
        / "examples"
        / "manager"
        / "order.v1.example.json"
    )
    return json.loads(example_path.read_text())


def test_order_example_validates_against_schema(order_schema, order_example):
    """Verify that order.v1.example.json validates against order.v1.json schema."""
    # Validate example against schema
    validate(instance=order_example, schema=order_schema)


def test_order_schema_requires_essential_fields(order_schema):
    """Verify that required fields are enforced by the schema."""
    required_fields = [
        "intent_id",
        "idempotency_key",
        "timestamp",
        "symbol",
        "side",
        "quantity",
        "order_type",
    ]
    assert set(order_schema["required"]) == set(required_fields)


def test_order_schema_rejects_missing_intent_id(order_schema):
    """Missing intent_id should fail validation."""
    invalid_order = {
        "idempotency_key": "strategy-signal-123-2024-01-01T00:00:00.000Z",
        "timestamp": "2024-01-01T09:30:00.000Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": "100",
        "order_type": "market",
    }
    with pytest.raises(ValidationError):
        validate(instance=invalid_order, schema=order_schema)


def test_order_schema_rejects_missing_idempotency_key(order_schema):
    """Missing idempotency_key should fail validation."""
    invalid_order = {
        "intent_id": "signal-123",
        "timestamp": "2024-01-01T09:30:00.000Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": "100",
        "order_type": "market",
    }
    with pytest.raises(ValidationError):
        validate(instance=invalid_order, schema=order_schema)


def test_order_schema_accepts_valid_market_order(order_schema):
    """Valid market order should pass validation."""
    valid_order = {
        "intent_id": "signal-123",
        "idempotency_key": "strategy-signal-123-2024-01-01T00:00:00.000Z",
        "timestamp": "2024-01-01T09:30:00.000Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": "100",
        "order_type": "market",
        "time_in_force": "GTC",
        "limit_price": None,
        "stop_price": None,
        "source_strategy_id": None,
        "stop_loss": None,
        "take_profit": None,
    }
    validate(instance=valid_order, schema=order_schema)


def test_order_schema_accepts_valid_limit_order(order_schema):
    """Valid limit order with limit_price should pass validation."""
    valid_order = {
        "intent_id": "signal-123",
        "idempotency_key": "strategy-signal-123-2024-01-01T00:00:00.000Z",
        "timestamp": "2024-01-01T09:30:00.000Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": "100",
        "order_type": "limit",
        "limit_price": "150.00",
        "time_in_force": "GTC",
        "stop_price": None,
        "source_strategy_id": "sma_crossover",
        "stop_loss": "140.00",
        "take_profit": "160.00",
    }
    validate(instance=valid_order, schema=order_schema)


def test_order_schema_rejects_invalid_side(order_schema):
    """Invalid side value should fail validation."""
    invalid_order = {
        "intent_id": "signal-123",
        "idempotency_key": "strategy-signal-123-2024-01-01T00:00:00.000Z",
        "timestamp": "2024-01-01T09:30:00.000Z",
        "symbol": "AAPL",
        "side": "long",  # Invalid - must be "buy" or "sell"
        "quantity": "100",
        "order_type": "market",
    }
    with pytest.raises(ValidationError):
        validate(instance=invalid_order, schema=order_schema)


def test_order_schema_rejects_invalid_order_type(order_schema):
    """Invalid order_type value should fail validation."""
    invalid_order = {
        "order_id": "order-123",
        "intent_id": "signal-123",
        "idempotency_key": "strategy-signal-123-2024-01-01T00:00:00.000Z",
        "timestamp": "2024-01-01T09:30:00.000Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": "100",
        "order_type": "iceberg",  # Invalid
    }
    with pytest.raises(ValidationError):
        validate(instance=invalid_order, schema=order_schema)


def test_order_schema_rejects_empty_strings(order_schema):
    """Empty strings for required fields should fail validation."""
    invalid_order = {
        "intent_id": "",  # Empty string
        "idempotency_key": "strategy-signal-123-2024-01-01T00:00:00.000Z",
        "timestamp": "2024-01-01T09:30:00.000Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": "100",
        "order_type": "market",
    }
    with pytest.raises(ValidationError):
        validate(instance=invalid_order, schema=order_schema)


def test_order_schema_rejects_additional_properties(order_schema):
    """Additional properties not in schema should fail validation."""
    invalid_order = {
        "intent_id": "signal-123",
        "idempotency_key": "strategy-signal-123-2024-01-01T00:00:00.000Z",
        "timestamp": "2024-01-01T09:30:00.000Z",
        "symbol": "AAPL",
        "side": "buy",
        "quantity": "100",
        "order_type": "market",
        "extra_field": "not_allowed",  # Additional property
    }
    with pytest.raises(ValidationError):
        validate(instance=invalid_order, schema=order_schema)
