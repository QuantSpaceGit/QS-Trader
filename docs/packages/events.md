# Events Module Documentation

**Version:** 1.0 **Last Updated:** October 2025 **Status:** Production Ready

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Event Hierarchy](#event-hierarchy)
- [Validation System](#validation-system)
- [Timestamp Handling](#timestamp-handling)
- [Wire Format Contracts](#wire-format-contracts)
- [Usage Examples](#usage-examples)
- [Best Practices](#best-practices)
- [Testing](#testing)
- [Related Documentation](#related-documentation)

______________________________________________________________________

## Overview

The QS-Trader events module provides a production-ready event system with:

- **Schema-based validation**: JSON Schema validation for all events
- **Type safety**: Pydantic models with Python type hints
- **Cross-language compatibility**: Wire format works with Rust/JavaScript/TypeScript
- **UTC timestamps**: Canonical UTC with optional local timestamps for market sessions
- **Immutability**: Events are frozen after validation
- **Rich error context**: Validation errors include path, schema_path, and failed value

### Key Features

✅ Separate envelope/payload validation ✅ Integer version numbers for semantic versioning ✅ UTC timezone-aware timestamps with RFC3339 serialization ✅ Schema caching for performance (128 validators cached) ✅ Wire contract type alignment (Decimals ↔ strings, volumes ↔ int/string) ✅ Event type consistency checks ✅ Control events for system coordination ✅ Enhanced error context for debugging ✅ Local timestamp support for market session analysis

______________________________________________________________________

## Architecture

### Design Principles

1. **Envelope + Payload Separation**: All events share common envelope fields (event_id, event_type, occurred_at, etc.), domain-specific data goes in payload
1. **JSON Schema as Source of Truth**: Python models validate against JSON schemas (cross-language contract)
1. **Flat Event Model**: Payload fields are at the top level of Python classes (no nested `payload` dict)
1. **UTC Canonical**: All timestamps are UTC, with optional local timestamps for market sessions
1. **Package-Safe**: Uses `importlib.resources` for schema loading (works in wheels/Docker/Lambda)

### Event Flow

```
1. Create Event (Pydantic field validation)
   ↓
2. BaseEvent._validate_envelope() (JSON Schema)
   ↓
3. ValidatedEvent._validate_payload() (JSON Schema)
   ↓
4. Event frozen (immutable)
   ↓
5. Serialization (Decimal→string, datetime→RFC3339)
```

______________________________________________________________________

## Event Hierarchy

```
BaseEvent (envelope validation)
├── ValidatedEvent (envelope + payload validation)
│   ├── PriceBarEvent (bar.v1.json)
│   └── CorporateActionEvent (corporate_action.v1.json)
└── ControlEvent (envelope validation only)
    ├── BarCloseEvent
    ├── BacktestStartedEvent
    ├── BacktestEndedEvent
    ├── ValuationTriggerEvent
    └── RiskEvaluationTriggerEvent
```

### BaseEvent

**Purpose:** Common envelope fields for all events **Validation:** Validates against `envelope.v1.json` **Used By:** All events (ValidatedEvent and ControlEvent inherit from this)

**Envelope Fields:**

- `event_id`: UUID (auto-generated if not provided)
- `event_type`: str (must match SCHEMA_BASE for ValidatedEvent)
- `event_version`: int (schema major version)
- `occurred_at`: datetime (UTC, auto-generated if not provided)
- `correlation_id`: Optional[str] (UUID for workflow correlation)
- `causation_id`: Optional[str] (UUID for event causation)
- `source_service`: str (service that emitted the event)

### ValidatedEvent

**Purpose:** Events with domain-specific payload validation **Validation:** Envelope + payload against `{schema_base}.v{version}.json` **Used By:** Domain events (PriceBarEvent, CorporateActionEvent, etc.)

**Key Features:**

- `SCHEMA_BASE`: ClassVar specifying which schema to load (e.g., "bar", "corporate_action")
- `_validate_payload()`: Validates domain fields against JSON schema
- Ensures `event_type` matches `SCHEMA_BASE`

### ControlEvent

**Purpose:** System coordination events without payload validation **Validation:** Envelope only (no payload schema) **Used By:** Barriers, lifecycle events, coordination signals

**Examples:**

- `BarCloseEvent`: Signals end of bar period
- `BacktestStartedEvent`: Signals backtest initialization
- `BacktestEndedEvent`: Signals backtest completion
- `ValuationTriggerEvent`: Triggers portfolio valuation
- `RiskEvaluationTriggerEvent`: Triggers risk evaluation

______________________________________________________________________

## Validation System

### Schema Loading

```python
SCHEMA_PACKAGE = "qs_trader.contracts.schemas"

@lru_cache(maxsize=128)
def load_and_compile_schema(schema_name: str):
    """Load and compile JSON schema. Results are cached."""
    schema_file = resources.files(SCHEMA_PACKAGE).joinpath(schema_name)
    schema_dict = json.loads(schema_file.read_text())
    return jsonschema.Draft202012Validator(schema_dict)
```

**Benefits:**

- Package-safe (works in wheels, Docker, Lambda)
- Cached validators (fast validation, ~100-200 μs per event)
- Single source of truth (SCHEMA_PACKAGE constant)

### Validation Flow

#### 1. Pydantic Field Validation (Automatic)

Pydantic validates types and performs conversions:

- String → Decimal
- String → datetime
- Auto-generates UUID for `event_id` if not provided
- Auto-generates UTC timestamp for `occurred_at` if not provided

#### 2. Envelope Validation (All Events)

```python
@model_validator(mode="after")
def _validate_envelope(self) -> "BaseEvent":
    """Validate envelope fields against envelope.v1.json."""
    envelope_validator = load_envelope_schema()

    envelope_data = {
        "event_id": self.event_id,
        "event_type": self.event_type,
        "event_version": self.event_version,
        "occurred_at": self.occurred_at.isoformat().replace("+00:00", "Z"),
        "source_service": self.source_service,
    }

    # Only include optional fields if present
    if self.correlation_id is not None:
        envelope_data["correlation_id"] = self.correlation_id
    if self.causation_id is not None:
        envelope_data["causation_id"] = self.causation_id

    envelope_validator.validate(envelope_data)
    return self
```

#### 3. Payload Validation (ValidatedEvent Only)

```python
@model_validator(mode="after")
def _validate_payload(self) -> "ValidatedEvent":
    """Validate domain fields against {schema_base}.v{version}.json."""
    # Ensure event_type matches schema base
    if self.event_type != self.SCHEMA_BASE:
        raise ValueError(
            f"event_type '{self.event_type}' must match SCHEMA_BASE '{self.SCHEMA_BASE}'"
        )

    # Load schema
    schema_name = f"{self.SCHEMA_BASE}.v{self.event_version}.json"
    validator = load_and_compile_schema(schema_name)

    # Build payload (all fields except envelope)
    payload = self.model_dump(exclude=RESERVED_ENVELOPE_KEYS)

    # Validate
    validator.validate(payload)
    return self
```

### Error Handling

**Rich Error Context:**

```python
try:
    event = PriceBarEvent(symbol="AAPL", ...)
except ValueError as e:
    # Error includes:
    # - Class name (PriceBarEvent)
    # - Schema name (bar.v1.json)
    # - Error message
    # - JSON path to failed field
    # - Schema path to validation rule
    # - Failed value
    print(e)
```

**Example Error:**

```
PriceBarEvent payload validation failed against bar.v1.json: 'invalid' is not of type 'number'
Path: ['close']
Schema path: ['properties', 'close', 'type']
Failed value: invalid
```

______________________________________________________________________

## Timestamp Handling

### UTC Canonical Timestamp

**Field:** `timestamp` (required) **Type:** `str` (RFC3339 with Z suffix) **Purpose:** Canonical timestamp, always UTC

```python
event = PriceBarEvent(
    timestamp="2024-01-15T14:30:00Z",  # UTC: 2:30 PM
    # ...
)
```

**Serialization:**

```python
# Python datetime → RFC3339 string
occurred_at = datetime.now(timezone.utc)
serialized = occurred_at.isoformat().replace("+00:00", "Z")
# Result: "2024-01-15T14:30:00Z"
```

### Local Timestamps (Optional)

**Fields:**

- `timestamp_local`: `Optional[str]` (RFC3339 with offset)
- `timezone`: `Optional[str]` (IANA timezone identifier)

**Purpose:** Market session analysis without DST ambiguity

#### When to Use

✅ **Include for:**

- Equity markets with trading hours
- Futures markets with session times
- Any market where "local hour" has meaning

❌ **Omit for:**

- 24/7 crypto markets
- Global forex markets
- Internal system events

#### Example: Equity Market

```python
event = PriceBarEvent(
    # Canonical UTC
    timestamp="2024-03-10T14:30:00Z",  # UTC: 2:30 PM

    # Local session time
    timestamp_local="2024-03-10T09:30:00-05:00",  # EST: 9:30 AM (market open)
    timezone="America/New_York",

    # ... other fields
)
```

#### DST Handling

**Before DST (EST = UTC-5):**

```python
timestamp="2024-03-09T21:00:00Z"
timestamp_local="2024-03-09T16:00:00-05:00"  # 4 PM EST
```

**After DST (EDT = UTC-4):**

```python
timestamp="2024-03-11T20:00:00Z"
timestamp_local="2024-03-11T16:00:00-04:00"  # 4 PM EDT
```

**Both represent "4 PM market close"** - offset automatically reflects DST.

#### Query Benefits

**Without local timestamp (complex):**

```sql
SELECT
  DATE(CONVERT_TZ(timestamp, '+00:00', 'America/New_York')) AS trading_day,
  HOUR(CONVERT_TZ(timestamp, '+00:00', 'America/New_York')) AS session_hour,
  AVG(close)
FROM bars
GROUP BY 1, 2;
```

**With local timestamp (simple):**

```sql
SELECT
  DATE(timestamp_local) AS trading_day,
  HOUR(timestamp_local) AS session_hour,
  AVG(close)
FROM bars
WHERE timezone = 'America/New_York'
GROUP BY 1, 2;
```

______________________________________________________________________

## Wire Format Contracts

### Cross-Language Compatibility

Events serialize to JSON that works across Python, Rust, JavaScript, TypeScript:

| Python Type                      | Wire Type        | Schema Type        | Reason                         |
| -------------------------------- | ---------------- | ------------------ | ------------------------------ |
| `int` (event_version)            | integer          | integer            | Semantic versioning            |
| `datetime` (occurred_at)         | string           | string (date-time) | RFC3339 with Z                 |
| `Decimal` (prices)               | string           | string + pattern   | No float precision loss        |
| `int` (volume)                   | int \| string    | integer \| string  | JS safe integer limit (2^53-1) |
| `Optional[str]` (correlation_id) | string \| absent | ["string", "null"] | Nullable fields                |
| `str` (event_id)                 | string           | string + pattern   | UUID validation                |

### Decimal Serialization

**Python:**

```python
class PriceBarEvent(ValidatedEvent):
    open: Decimal  # Decimal in Python

    @field_serializer("open", "high", "low", "close", ...)
    def _serialize_decimal(self, v: Optional[Decimal]) -> Optional[str]:
        """Serialize Decimal to string for wire format."""
        return format(v, "f") if v is not None else None
```

**Wire (JSON):**

```json
{
  "open": "150.00",
  "close": "154.50"
}
```

**Consumer (JavaScript):**

```javascript
const open = Decimal(event.open);  // Use Decimal.js or similar
```

### Volume Serialization (JavaScript Safe)

**Problem:** JavaScript safe integer limit is 2^53-1 (9,007,199,254,740,991) **Solution:** Serialize large volumes as strings

```python
JS_SAFE_INTEGER_MAX = 9007199254740991  # 2^53 - 1

@field_serializer("volume")
def _serialize_volume(self, v: Optional[int]) -> Optional[int | str]:
    if v is None:
        return None
    if v > JS_SAFE_INTEGER_MAX:
        return str(v)  # String for large volumes
    return v  # Integer for normal volumes
```

**Wire (JSON):**

```json
{
  "volume": 1000000,  // Normal volume (int)
  "volume": "10000000000000000"  // Large volume (string)
}
```

**Consumer (JavaScript):**

```javascript
const volume = typeof event.volume === 'string'
  ? BigInt(event.volume)  // Large volume
  : event.volume;         // Normal volume
```

______________________________________________________________________

## Usage Examples

### Creating a Price Bar Event

```python
from datetime import datetime, timezone
from decimal import Decimal
from qs_trader.events.events import PriceBarEvent

# Equity bar with local timestamps
event = PriceBarEvent(
    # Envelope (auto-generated if omitted)
    event_id="550e8400-e29b-41d4-a716-446655440000",  # Optional
    event_type="bar",
    event_version=1,
    occurred_at=datetime.now(timezone.utc),  # Optional
    source_service="market_data_service",

    # Domain fields
    symbol="AAPL",
    asset_class="equity",
    interval="1d",

    # Timestamps
    timestamp="2024-01-15T14:30:00Z",  # UTC (required)
    timestamp_local="2024-01-15T09:30:00-05:00",  # EST (optional)
    timezone="America/New_York",  # IANA timezone (optional)

    # OHLCV
    open=Decimal("150.00"),
    high=Decimal("155.00"),
    low=Decimal("149.00"),
    close=Decimal("154.50"),
    volume=1_500_000,

    # Adjustment factors
    adjusted=False,
    cumulative_price_factor=Decimal("1.0"),
    cumulative_volume_factor=Decimal("1.0"),

    source="nyse",
)

# Event is validated and frozen
print(event.symbol)  # AAPL
print(event.close)   # Decimal('154.50')
```

### Creating a Corporate Action Event

```python
from qs_trader.events.events import CorporateActionEvent

event = CorporateActionEvent(
    event_type="corporate_action",
    event_version=1,
    source_service="corporate_actions_service",

    # Domain fields
    symbol="AAPL",
    asset_class="equity",
    action_type="split",
    announcement_date="2020-07-30",
    ex_date="2020-08-31",
    effective_date="2020-08-31",

    # Split details
    split_from=1,
    split_to=4,
    split_ratio=Decimal("4.0"),
    price_adjustment_factor=Decimal("0.25"),
    volume_adjustment_factor=Decimal("4.0"),

    source="sec_filings",
)
```

### Creating Control Events

```python
from qs_trader.events.events import (
    BarCloseEvent,
    BacktestStartedEvent,
    BacktestEndedEvent,
)

# Bar close barrier
bar_close = BarCloseEvent(
    event_type="bar_close",
    source_service="bar_aggregator",
    symbol="AAPL",
    interval="1d",
    timestamp="2024-01-15T14:30:00Z",
)

# Backtest lifecycle
backtest_start = BacktestStartedEvent(
    event_type="backtest_started",
    source_service="backtest_engine",
    config={
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "initial_capital": 100000,
    },
)

backtest_end = BacktestEndedEvent(
    event_type="backtest_ended",
    source_service="backtest_engine",
    final_capital=Decimal("125000.00"),
    total_return=Decimal("0.25"),
    sharpe_ratio=Decimal("1.85"),
)
```

### Serialization

```python
# Serialize to dict
data = event.model_dump()

# Serialize to JSON
import json
json_str = json.dumps(data)

# Serialize excluding None values
data_no_none = event.model_dump(exclude_none=True)

# Decimals become strings
assert isinstance(data["close"], str)  # "154.50"
assert isinstance(data["volume"], int)  # 1500000

# Datetime becomes RFC3339
assert data["occurred_at"].endswith("Z")  # "2024-01-15T14:30:00Z"
```

### Validation Errors

```python
from pydantic import ValidationError

try:
    event = PriceBarEvent(
        symbol="AAPL",
        # Missing required field: close
    )
except ValidationError as e:
    # Pydantic validation error (field-level)
    print(e)

try:
    event = PriceBarEvent(
        symbol="AAPL",
        close=Decimal("100.0"),
        # ... all required fields
        invalid_field="not_allowed",
    )
except ValueError as e:
    # JSON Schema validation error (schema-level)
    print(e)
```

______________________________________________________________________

## Best Practices

### 1. Always Use UTC for Canonical Timestamps

```python
# ✅ Good
timestamp="2024-01-15T14:30:00Z"  # UTC

# ❌ Bad
timestamp="2024-01-15T09:30:00-05:00"  # Local time in canonical field
```

### 2. Include Local Timestamps for Equity Markets

```python
# ✅ Good - Equity with local session time
PriceBarEvent(
    symbol="AAPL",
    asset_class="equity",
    timestamp="2024-01-15T14:30:00Z",
    timestamp_local="2024-01-15T09:30:00-05:00",
    timezone="America/New_York",
)

# ✅ Good - Crypto without local timestamps
PriceBarEvent(
    symbol="BTC-USD",
    asset_class="crypto",
    timestamp="2024-01-15T14:30:00Z",
    timestamp_local=None,
    timezone=None,
)
```

### 3. Use Decimal for Prices

```python
# ✅ Good
from decimal import Decimal
close=Decimal("154.50")

# ❌ Bad
close=154.50  # Float precision loss
```

### 4. Let Pydantic Auto-Generate IDs and Timestamps

```python
# ✅ Good - Auto-generated
event = PriceBarEvent(
    event_type="bar",
    source_service="data_service",
    # event_id and occurred_at auto-generated
)

# ✅ Also Good - Explicit (for testing)
event = PriceBarEvent(
    event_id="550e8400-e29b-41d4-a716-446655440000",
    occurred_at=datetime(2024, 1, 15, tzinfo=timezone.utc),
    event_type="bar",
    source_service="data_service",
)
```

### 5. Use Control Events for System Coordination

```python
# ✅ Good - Use BarCloseEvent for barriers
bar_close = BarCloseEvent(
    event_type="bar_close",
    source_service="bar_aggregator",
    symbol="AAPL",
    interval="1d",
    timestamp="2024-01-15T14:30:00Z",
)

# ❌ Bad - Don't create custom events for barriers
# (use built-in ControlEvent subclasses)
```

### 6. Handle Validation Errors Gracefully

```python
from pydantic import ValidationError

try:
    event = PriceBarEvent(...)
except ValidationError as e:
    # Pydantic validation (field-level)
    logger.error(f"Field validation failed: {e}")
    raise
except ValueError as e:
    # JSON Schema validation (schema-level)
    logger.error(f"Schema validation failed: {e}")
    raise
```

______________________________________________________________________

## Testing

### Test Coverage

**Total:** 72 tests (50 event + 22 contract)

#### Event Validation Tests

**File:** `tests/unit/events/test_event_validation.py` (14 tests)

- Envelope validation (4 tests)
- Payload validation (4 tests)
- Control events (2 tests)
- Decimal handling (2 tests)
- Event type mismatch (1 test)
- Error context (1 test)

**File:** `tests/unit/events/test_priority_fixes.py` (6 tests)

- Control event envelope validation
- Missing field path reporting
- Event type mismatch explicit error
- Event version schema loading
- Naive datetime UTC conversion
- Decimal roundtrip as string

**File:** `tests/unit/events/test_final_polish.py` (10 tests)

- None field handling (3 tests)
- Large volume serialization (4 tests)
- UUID pattern validation (3 tests)

**File:** `tests/unit/events/test_local_timestamps.py` (20 tests)

- Local timestamp basics (4 tests)
- Serialization (2 tests)
- Equity use cases (3 tests)
- Crypto use cases (2 tests)
- Schema validation (4 tests)
- Backwards compatibility (2 tests)
- Edge cases (3 tests)

#### Contract Schema Tests

**File:** `tests/unit/contracts/test_bar_schema.py` (9 tests) **File:** `tests/unit/contracts/test_corporate_action_schema.py` (13 tests)

### Running Tests

```bash
# All event tests
pytest tests/unit/events/ -v

# Specific test file
pytest tests/unit/events/test_event_validation.py -v

# Contract schema tests
pytest tests/unit/contracts/ -v

# All tests
pytest tests/unit/events/ tests/unit/contracts/ -v
```

### Example Files

**Working examples:**

See the test suite for comprehensive examples:

- `tests/unit/events/test_events.py` - Event creation, validation, immutability
- `tests/unit/events/test_event_bus.py` - Event bus publishing and subscription
- `tests/unit/contracts/` - JSON Schema validation examples

______________________________________________________________________

## Related Documentation

### Core Documentation

- **This File** (`docs/events.md`) - Complete events module reference
- `docs/EVENT_VALIDATION_IMPROVEMENTS.md` - Initial 9 comprehensive improvements
- `docs/PRIORITY_FIXES_SUMMARY.md` - 6 critical bug fixes
- `docs/FINAL_POLISH_SUMMARY.md` - Final production polish (5 improvements)
- `docs/LOCAL_TIMESTAMP_SUPPORT.md` - Local timestamp feature documentation
- `docs/EVENT_SYSTEM_COMPLETE_SUMMARY.md` - Overview of all improvements

### JSON Schemas

- `src/qs_trader/contracts/schemas/envelope.v1.json` - Envelope validation schema
- `src/qs_trader/contracts/schemas/bar.v1.json` - Price bar domain schema
- `src/qs_trader/contracts/schemas/corporate_action.v1.json` - Corporate action schema

### Examples

- `src/qs_trader/contracts/examples/envelope.v1.example.json` - Envelope example
- `src/qs_trader/contracts/examples/data/bar.v1.example.json` - Price bar example
- `src/qs_trader/contracts/examples/data/corporate_action.v1.example.json` - Corporate action example
- `src/qs_trader/contracts/examples/strategy/signal.v1.example.json` - Signal example
- `src/qs_trader/contracts/examples/execution/fill.v1.example.json` - Fill example

### Source Code

- `src/qs_trader/events/events.py` - Event classes and validation logic
- `tests/unit/events/` - Event validation tests
- `tests/unit/contracts/` - Contract schema tests

______________________________________________________________________

## Performance

### Validation Overhead

**Measured on typical hardware:**

- Envelope validation: ~50-100 μs (cached validator)
- Payload validation: ~100-200 μs (cached validator, 30-field event)
- First-time schema load: ~5-10 ms (compilation + file read)

**Recommendations:**

- Pre-warm validator cache at startup (load common schemas)
- Use cached validators across application lifetime
- Consider validation bypass in ultra-high-frequency paths (with contract tests)

### Memory Footprint

- Compiled validator: ~5-10 KB each
- 128 validators cached: ~640 KB - 1.3 MB total
- Negligible compared to typical Python application overhead

______________________________________________________________________

## Version History

### v1.0 (October 2025) - Production Ready

**Round 1 - Comprehensive Best Practices (9 improvements):**

1. Separate envelope/payload validation
1. Integer version numbers
1. UTC timezone-aware timestamps
1. Schema caching/pre-compilation
1. Wire contract type alignment
1. event_type ↔ schema base consistency
1. Separate base classes (BaseEvent/ValidatedEvent/ControlEvent)
1. Enhanced error context
1. Frozen after validation

**Round 2 - Priority Bug Fixes (6 fixes):**

1. Envelope validation for ControlEvent
1. event_type check bug fix
1. Package-safe schema loading (importlib.resources)
1. Targeted field_serializer
1. Centralized reserved keys
1. Method naming clarity

**Round 3 - Final Polish (5 improvements):**

1. None field handling in envelope validation
1. UUID pattern validation
1. Package name constant
1. Large volume serialization (JS-safe)
1. Cache size increase (32→128)

**Round 4 - Local Timestamps (2 additions):**

1. timestamp_local field (RFC3339 with offset)
1. timezone field (IANA timezone identifier)

______________________________________________________________________

## FAQ

### Q: Why are prices strings in JSON but Decimals in Python?

**A:** Python's `Decimal` provides exact precision for financial calculations. JSON doesn't have a Decimal type, so we serialize to strings to avoid floating-point precision loss. Consumers parse strings back to their Decimal-equivalent type.

### Q: Why separate envelope and payload validation?

**A:** Envelope fields are common to all events (event_id, occurred_at, etc.), while payload fields are domain-specific. This separation allows:

- Reusable envelope schema across all events
- Control events without payload validation
- Clear separation of concerns

### Q: When should I use ControlEvent vs ValidatedEvent?

**A:** Use `ControlEvent` for system coordination signals (barriers, lifecycle events) that don't need domain validation. Use `ValidatedEvent` for domain events (market data, trades, positions) that require schema validation.

### Q: Why is timestamp_local optional?

**A:** Not all markets have meaningful "local time" - crypto trades 24/7 globally, forex is also global. Only equity/futures markets with specific trading hours benefit from local timestamps.

### Q: How do I handle very large volumes (> 2^53-1)?

**A:** Volumes exceeding JavaScript's safe integer limit automatically serialize as strings. Consumers should check the type and handle accordingly (e.g., parse as BigInt in JavaScript).

### Q: Can I modify an event after creation?

**A:** No - events are frozen after validation (immutable). Create a new event if you need different values.

### Q: How do I add a new event type?

**A:**

1. Create JSON schema in `src/qs_trader/contracts/schemas/{name}.v1.json`
1. Create Pydantic model inheriting from `ValidatedEvent`
1. Set `SCHEMA_BASE = "{name}"`
1. Add tests in `tests/unit/events/`

### Q: How do I version schemas?

**A:** Bump `event_version` when making breaking changes. Create new schema file `{name}.v2.json`. Consumers use `event_version` to determine which schema to validate against.

______________________________________________________________________

## Support

For questions, issues, or contributions:

- Review related documentation (see [Related Documentation](#related-documentation))
- Check examples in `src/qs_trader/contracts/examples/` directory and test files in `tests/unit/events/`
- Run tests to see working code: `pytest tests/unit/events/ -v`
- Consult JSON schemas in `src/qs_trader/contracts/schemas/`

______________________________________________________________________

**Status: Production Ready** ✅ **All 72 tests passing** ✅ **Cross-language compatible** ✅ **Fully documented** ✅
