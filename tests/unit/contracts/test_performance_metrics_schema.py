"""Test reporting/performance_metrics contract schema validation."""

import json
from pathlib import Path
from typing import Any, cast

import pytest
from jsonschema import ValidationError, validate

_CONTRACT_ROOT = Path(__file__).parent.parent.parent.parent / "src" / "qs_trader" / "contracts"


@pytest.fixture
def performance_metrics_v1_schema() -> dict:
    """Load the reporting/performance_metrics.v1.json schema."""
    schema_path = _CONTRACT_ROOT / "schemas" / "reporting" / "performance_metrics.v1.json"
    return cast(dict[str, Any], json.loads(schema_path.read_text()))


@pytest.fixture
def performance_metrics_v1_example() -> dict:
    """Load the reporting/performance_metrics.v1.example.json payload."""
    example_path = _CONTRACT_ROOT / "examples" / "reporting" / "performance_metrics.v1.example.json"
    return cast(dict[str, Any], json.loads(example_path.read_text()))


@pytest.fixture
def performance_metrics_v2_schema() -> dict:
    """Load the reporting/performance_metrics.v2.json schema."""
    schema_path = _CONTRACT_ROOT / "schemas" / "reporting" / "performance_metrics.v2.json"
    return cast(dict[str, Any], json.loads(schema_path.read_text()))


@pytest.fixture
def performance_metrics_v2_example() -> dict:
    """Load the reporting/performance_metrics.v2.example.json payload."""
    example_path = _CONTRACT_ROOT / "examples" / "reporting" / "performance_metrics.v2.example.json"
    return cast(dict[str, Any], json.loads(example_path.read_text()))


def test_performance_metrics_v1_example_still_validates(
    performance_metrics_v1_schema: dict,
    performance_metrics_v1_example: dict,
) -> None:
    """The legacy v1 payload must remain valid after adding v2."""
    validate(instance=performance_metrics_v1_example, schema=performance_metrics_v1_schema)


def test_performance_metrics_v2_example_validates(
    performance_metrics_v2_schema: dict,
    performance_metrics_v2_example: dict,
) -> None:
    """The v2 example must validate against the v2 schema."""
    validate(instance=performance_metrics_v2_example, schema=performance_metrics_v2_schema)


def test_performance_metrics_v2_requires_sleeve_attribution(
    performance_metrics_v2_schema: dict,
    performance_metrics_v2_example: dict,
) -> None:
    """v2 must reject payloads that omit sleeve_id or symbol."""
    without_sleeve = dict(performance_metrics_v2_example)
    without_sleeve.pop("sleeve_id")

    without_symbol = dict(performance_metrics_v2_example)
    without_symbol.pop("symbol")

    with pytest.raises(ValidationError):
        validate(instance=without_sleeve, schema=performance_metrics_v2_schema)

    with pytest.raises(ValidationError):
        validate(instance=without_symbol, schema=performance_metrics_v2_schema)


def test_performance_metrics_v2_required_fields_extend_v1(
    performance_metrics_v1_schema: dict,
    performance_metrics_v2_schema: dict,
) -> None:
    """v2 should preserve every v1 required field and add sleeve attribution."""
    v1_required = set(performance_metrics_v1_schema["required"])
    v2_required = set(performance_metrics_v2_schema["required"])

    assert v1_required.issubset(v2_required)
    assert {"sleeve_id", "symbol"}.issubset(v2_required)
