# QS-Trader Contracts

Language-neutral event contracts using JSON Schema for cross-platform compatibility (Python, Rust, TypeScript, etc.).

## Overview

Contracts define the structure of events published by QS-Trader services. Each contract consists of:

- **Schema** (`schemas/{service}/*.json`): JSON Schema defining structure, types, and validation rules
- **Example** (`examples/{service}/*.json`): Sample event showing the envelope + payload pattern
- **Version**: Semantic versioning embedded in filename (e.g., `bar.v1.json`, `bar.v2.json`)

## Directory Structure

Contracts are organized by the service that emits them:

```
contracts/
├── schemas/
│   ├── envelope.v1.json          # Common envelope pattern
│   ├── data/                     # DataService events
│   │   ├── bar.v1.json           # OHLCV price bars
│   │   └── corporate_action.v1.json  # Corporate actions (splits, dividends)
│   ├── strategy/                 # StrategyService events
│   │   └── signal.v1.json        # Trading signals
│   └── execution/                # ExecutionService events
│       └── fill.v1.json          # Order fills
└── examples/
    ├── envelope.v1.example.json
    ├── data/
    │   ├── bar.v1.example.json
    │   └── corporate_action.v1.example.json
    ├── strategy/
    │   └── signal.v1.example.json
    └── execution/
        └── fill.v1.example.json
```

## Architecture

### Envelope Pattern

All events follow the envelope + payload pattern:

```json
{
  "event_id": "uuid",
  "event_type": "ohlcv_bar",
  "event_version": 1,
  "occurred_at": "2020-08-31T20:00:00Z",
  "correlation_id": "batch-id",
  "causation_id": "triggering-event-id",
  "source_service": "data_service",
  "payload": {
    // Contract-specific data
  }
}
```

**Envelope fields** (see `schemas/envelope.v1.json`):

- `event_id`: Unique identifier for this event (UUID)
- `event_type`: Contract type (e.g., "ohlcv_bar", "corporate_action")
- `event_version`: Schema version (integer)
- `occurred_at`: When the event occurred (ISO 8601 UTC)
- `correlation_id`: Groups related events (e.g., batch imports)
- `causation_id`: ID of the event that triggered this one (null if none)
- `source_service`: Service that published the event

### Payload

The `payload` field contains contract-specific data validated against the schema.

## Available Contracts

### DataService Events

#### data/bar.v1 - OHLCV Bars

Market data bars with adjustment factors.

**Key fields**:

- `symbol`, `asset_class`, `interval`, `timestamp`
- OHLCV: `open`, `high`, `low`, `close`, `volume`
- Optional: `vwap`, `turnover`, `trade_count`
- **Adjustment factors**: `cumulative_price_factor`, `cumulative_volume_factor`
- **Per-bar adjustments**: `price_adjustment_factor`, `volume_adjustment_factor`, `adjustment_reason`

**Adjustment strategy**:

- **Unadjusted prices** + cumulative factors = single source of truth
- `cumulative_price_factor`: Apply to historical prices to adjust forward (e.g., for splits)
- `price_adjustment_factor`: Instantaneous factor for this bar (null when no adjustment)
- Consumers apply factors on-the-fly: `adjusted_price = price * cumulative_price_factor`

**Schema**: `schemas/data/bar.v1.json`\
**Example**: `examples/data/bar.v1.example.json`

#### data/corporate_action.v1 - Corporate Actions

Splits, dividends, mergers, and other corporate events.

**Key fields**:

- `symbol`, `asset_class`, `action_type`
- **Dates**: `announcement_date`, `ex_date`, `record_date`, `payment_date`, `effective_date`
- **Splits**: `split_ratio`, `split_from`, `split_to`, `price_adjustment_factor`, `volume_adjustment_factor`
- **Dividends**: `dividend_amount`, `dividend_currency`, `dividend_type`

**Use case**: Ledger/portfolio systems consume these to update positions, cost basis, and lot tracking.

**Schema**: `schemas/data/corporate_action.v1.json`\
**Example**: `examples/data/corporate_action.v1.example.json`

### StrategyService Events

#### strategy/signal.v1 - Trading Signals

Trading signals emitted by strategies indicating intent to trade.

**Key fields**:

- `timestamp`: Signal generation timestamp (UTC RFC3339)
- `strategy_id`: Unique identifier of the strategy that generated the signal
- `symbol`: Instrument identifier to trade
- `intention`: Trading action - `OPEN_LONG`, `CLOSE_LONG`, `OPEN_SHORT`, `CLOSE_SHORT`
- `price`: Price at which signal was generated (typically current market price)
- `confidence`: Signal strength [0.0, 1.0] - higher = stronger conviction
- **Optional**: `reason` (human-readable explanation), `metadata` (strategy-specific data)
- **Optional risk management**: `stop_loss`, `take_profit`

**Use case**: Strategies emit signals via Context → Manager evaluates and sizes → ExecutionService creates orders. Signals are INTENT, not orders - they declare what the strategy wants to do, while manager determines how much to trade.

**Philosophy**:

- Strategies emit **declarative signals** (what to do), not imperative orders (do this now)
- Confidence levels allow risk management to size positions appropriately
- Stop loss and take profit are **recommendations** from the strategy, not hard requirements
- Signals are immutable facts - they record what the strategy wanted at a point in time

**Schema**: `schemas/strategy/signal.v1.json`\
**Example**: `examples/strategy/signal.v1.example.json`

### ExecutionService Events

#### execution/fill.v1 - Order Fills

Order fill events indicating completed trades with execution details.

**Key fields**:

- `fill_id`: Unique identifier (UUID)
- `source_order_id`: Originating order ID
- `timestamp`: Execution timestamp (UTC RFC3339)
- `symbol`: Instrument identifier
- `side`: "buy" or "sell"
- `filled_quantity`: Shares/units filled (positive integer)
- `fill_price`: Execution price per share/unit (includes slippage)
- **Optional**: `strategy_id` (for attribution), `commission`, `slippage_bps`, `gross_value`, `net_value`

**Use case**: Portfolio/ledger services consume fills to update positions, cash balances, and calculate realized P&L.

**Schema**: `schemas/execution/fill.v1.json`\
**Example**: `examples/execution/fill.v1.example.json`

## Versioning Strategy

### Semantic Versioning

Contracts use semantic versioning embedded in filenames:

- **v1, v2, v3...**: Major versions (breaking changes)
- Schema filename: `{contract_name}.v{major}.json`
- Example filename: `{contract_name}.v{major}.example.json`

### Version Compatibility Rules

#### Breaking Changes (Require New Major Version)

Create a new schema file (e.g., `bar.v2.json`):

- Remove required field
- Change field type (e.g., string → number)
- Rename field
- Change validation rules (e.g., stricter regex pattern)
- Remove enum value that was previously valid

**Process**:

1. Copy `bar.v1.json` → `bar.v1.json`
1. Make breaking changes in `bar.v2.json`
1. Copy `bar.v1.example.json` → `bar.v2.example.json`
1. Update example to match v2 schema
1. Update `event_version: 2` in envelope
1. Both versions coexist (consumers specify which version they support)

#### Non-Breaking Changes (Update Existing Version)

Modify the existing schema file directly:

- Add optional field (not in `required` array)
- Add new enum value
- Relax validation rules (e.g., remove `minLength`)
- Add descriptive metadata (`description`, `examples`)
- Fix typos in descriptions

**Process**:

1. Update `bar.v1.json` with new optional field
1. Update `bar.v1.example.json` to show the new field (optional)
1. No version bump needed

### Version History

#### data/bar.v1.json

**v1.0** (2025-10-23)

- Initial release
- OHLCV fields with adjustment factors
- Maps to Algoseek data format
- Fields: `cumulative_price_factor`, `cumulative_volume_factor`, `price_adjustment_factor`, `volume_adjustment_factor`, `adjustment_reason`

#### data/corporate_action.v1.json

**v1.0** (2025-10-23)

- Initial release
- Supports: splits, dividends, mergers, spinoffs, symbol changes, delistings
- Key dates: announcement, ex-date, record, payment, effective

#### strategy/signal.v1.json

**v1.0** (2025-10-28)

- Initial release
- Trading signals emitted by strategies
- Required: timestamp, strategy_id, symbol, intention, price, confidence
- Optional: reason, metadata, stop_loss, take_profit
- Intention enum: OPEN_LONG, CLOSE_LONG, OPEN_SHORT, CLOSE_SHORT
- Confidence as decimal [0.0, 1.0]

#### execution/fill.v1.json

**v1.0** (2025-10-23)

- Initial release
- Order fill events from ExecutionService
- Required: fill_id, source_order_id, timestamp, symbol, side, filled_quantity, fill_price
- Optional: strategy_id, commission, slippage_bps, gross_value, net_value

## Validation

### Using pytest

Run schema validation tests:

```bash
# Run all contract validation tests
pytest tests/unit/contracts/ -v

# Run specific contract tests
pytest tests/unit/contracts/test_bar_schema.py -v
pytest tests/unit/contracts/test_corporate_action_schema.py -v
pytest tests/unit/events/test_signal_event.py -v
```

### Manual Validation

Using Python `jsonschema` library:

```python
import json
from jsonschema import validate
from pathlib import Path

# Load schema and example
schema = json.loads(Path("schemas/bar.v1.json").read_text())
example = json.loads(Path("examples/bar.v1.example.json").read_text())

# Validate
validate(instance=example["payload"], schema=schema)
print("✓ Valid")
```

### CI/CD Integration

Validation tests run automatically on:

- Pre-commit hooks
- Pull requests
- Main branch merges

## Creating New Contracts

1. **Determine the service** that will emit the event (data, strategy, execution, etc.)

1. **Define the schema**:

   ```bash
   # Create schema file in appropriate service directory
   touch schemas/{service}/my_contract.v1.json
   ```

1. **Follow the structure**:

   ```json
   {
     "$schema": "https://json-schema.org/draft/2020-12/schema",
     "$id": "contracts.schemas.{service}.my_contract.v1.json",
     "title": "My Contract v1",
     "type": "object",
     "required": ["field1", "field2"],
     "properties": {
       "field1": {"type": "string"},
       "field2": {"type": "integer"}
     },
     "additionalProperties": false
   }
   ```

1. **Create an example**:

   ```bash
   touch examples/{service}/my_contract.v1.example.json
   ```

1. **Include envelope**:

   ```json
   {
     "event_id": "uuid",
     "event_type": "my_contract",
     "event_version": 1,
     "occurred_at": "2025-10-23T12:00:00Z",
     "payload": {
       "field1": "value",
       "field2": 42
     }
   }
   ```

1. **Write validation test**:

   ```bash
   touch tests/unit/contracts/test_my_contract_schema.py
   ```

1. **Update event class** in `src/qs_trader/events/events.py`:

   ```python
   class MyContractEvent(ValidatedEvent):
       """My contract event - validates against {service}/my_contract.v{version}.json."""

       SCHEMA_BASE: ClassVar[Optional[str]] = "{service}/my_contract"
       event_type: str = "my_contract"

       # Domain fields...
   ```

1. **Document in README**:

   - Add to "Available Contracts" section under appropriate service
   - Document key fields and use cases

## Service-to-Contract Mapping

| Service              | Events                                    | Purpose                                    |
| -------------------- | ----------------------------------------- | ------------------------------------------ |
| **DataService**      | `data/bar.v1`, `data/corporate_action.v1` | Market data streaming and corporate events |
| **StrategyService**  | `strategy/signal.v1`                      | Trading signal generation                  |
| **ExecutionService** | `execution/fill.v1`                       | Order execution and fill confirmation      |

## Best Practices

### Schema Design

- **Use strict typing**: Define explicit types, patterns, formats
- **Set `additionalProperties: false`**: Prevent unexpected fields
- **Decimals as strings**: Use `"type": "string", "pattern": "^\\d+(\\.\\d+)?$"` for financial data
- **Nullable fields**: Use `"oneOf": [{"type": "string"}, {"type": "null"}]`
- **Enums for constants**: Define allowed values explicitly
- **Add descriptions**: Document every field's purpose

### Naming Conventions

- **Schema files**: `{service}/{contract_name}.v{version}.json` (lowercase, underscores)
- **Example files**: `{service}/{contract_name}.v{version}.example.json`
- **Field names**: `snake_case` (lowercase with underscores)
- **Event types**: `snake_case` matching contract name
- **Service directories**: `data/`, `strategy/`, `execution/`, etc.

### Versioning

- **Start at v1**: No v0 or beta versions
- **Breaking changes = new major version**: Don't modify existing contracts
- **Keep old versions**: Support backward compatibility
- **Document changes**: Update Version History section

### Testing

- **Validate examples**: Every example must pass its schema validation
- **Test edge cases**: Create examples for boundary conditions
- **Document failures**: If validation fails, explain why in the test

## Consumer Guidance

### Applying Adjustment Factors (Bar Contract)

**Strategy consumers** (apply factors on-the-fly):

```python
# Get adjusted price
adjusted_close = float(bar["close"]) * float(bar["cumulative_price_factor"])
adjusted_volume = float(bar["volume"]) / float(bar["cumulative_volume_factor"])
```

**Ledger consumers** (use raw prices + corporate action events):

```python
# Store unadjusted prices
position.avg_cost = float(bar["close"])

# Wait for corporate_action event to update position
if corporate_action["action_type"] == "split":
    position.shares *= float(corporate_action["split_ratio"])
    position.avg_cost /= float(corporate_action["split_ratio"])
```

### Handling Corporate Actions

**On split event**:

1. Multiply position shares by `split_ratio`
1. Divide cost basis by `split_ratio`
1. Adjust stop-loss/take-profit orders

**On dividend event**:

1. Record cash receipt: `shares * dividend_amount`
1. Update realized gains
1. Adjust cost basis (if return of capital)

### Processing Trading Signals

**RiskService workflow**:

1. Receive SignalEvent from event bus
1. Check signal confidence against threshold (e.g., only act on confidence > 0.7)
1. Query current position for symbol
1. Determine appropriate position size based on:
   - Signal confidence level
   - Portfolio risk limits
   - Current exposure
   - Stop loss distance (if provided)
1. Create order request with calculated size
1. Pass to ExecutionService

**Example**:

```python
# Strategy emits signal
context.emit_signal(
    timestamp="2024-03-15T14:35:22Z",
    symbol="AAPL",
    intention=SignalIntention.OPEN_LONG,
    price=Decimal("145.75"),
    confidence=0.85,  # High confidence
    stop_loss=Decimal("140.50"),  # 3.6% risk
    take_profit=Decimal("152.00"),  # 4.3% reward
    reason="Golden cross with volume confirmation"
)

# RiskService receives and sizes
risk_percentage = 0.02  # 2% portfolio risk per trade
position_size = calculate_size(
    portfolio_value=100000,
    risk_per_trade=risk_percentage,
    entry_price=145.75,
    stop_loss=140.50
)
# Result: ~380 shares (risking $2000 on $5.25 stop distance)
```

## Tools & Resources

- **JSON Schema Validator**: <https://www.jsonschemavalidator.net/>
- **JSON Schema Docs**: <https://json-schema.org/>
- **Python jsonschema**: <https://python-jsonschema.readthedocs.io/>
- **Rust serde_json**: <https://docs.rs/serde_json/>
- **TypeScript ajv**: <https://ajv.js.org/>

## Contributing

1. Create feature branch: `git checkout -b contracts/add-my-contract`
1. Add schema + example + test
1. Update this README
1. Run validation: `pytest tests/unit/contracts/`
1. Submit PR with clear description of the contract's purpose

## Questions?

See `docs/ARCHITECTURE.md` for system-level design decisions.
