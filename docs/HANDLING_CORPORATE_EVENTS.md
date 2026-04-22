# Corporate Actions Handling in QS-Trader

## Overview

QS-Trader implements automatic corporate action processing through an event-driven architecture. When corporate actions occur (splits, dividends, etc.), the DataService publishes `CorporateActionEvent` objects which are handled by the PortfolioService to adjust positions, cash, and maintain accurate accounting.

## Interaction with the Run-Level Price Basis

- `price_basis` controls which OHLC series a run uses for strategy evaluation, execution, portfolio valuation, and reporting.
- Dividend handling is deliberately separate from that OHLC basis choice. Cash dividends are always recorded as explicit portfolio ledger/cash-flow events.
- This means a run can choose `raw` or `adjusted` prices without reintroducing the retired per-layer `*_adjustment_mode` knobs.

## Architecture

### Event Flow

```
┌──────────────┐         ┌──────────────┐         ┌──────────────────┐
│ DataService  │────────>│  EventBus    │────────>│ PortfolioService │
│              │         │              │         │                  │
│ Publishes    │         │ Dispatches   │         │ Handles          │
│ CorporateAction│       │ Events       │         │ Corporate Actions│
│ Events       │         │              │         │                  │
└──────────────┘         └──────────────┘         └──────────────────┘
```

### Implementation Details

1. **DataService** reads corporate action data from vendor sources and publishes `CorporateActionEvent` for each action
1. **EventBus** dispatches events to all subscribers with appropriate priority
1. **PortfolioService** receives events via `on_corporate_action()` handler (priority 90) and dispatches to specific processing methods

## Supported Corporate Actions

### 1. Stock Splits (Forward Splits)

**Type:** `"split"`

**What We Support:**

- Forward stock splits (e.g., 2-for-1, 4-for-1)
- Applies to both long and short positions
- Adjusts all lots proportionally
- Preserves total position value

**How We Process:**

- Quantity multiplied by split ratio
- Entry price divided by split ratio
- Total cost preserved (quantity × price remains constant)
- Creates ledger entry for audit trail

**Example:**

```python
# Before: 100 shares @ $400 = $40,000
# 4-for-1 split
# After:  400 shares @ $100 = $40,000
```

**Event Schema Fields Used:**

- `action_type`: "split"
- `ex_date`: Date when split takes effect
- `split_ratio`: Multiplicative factor (e.g., 4.0 for 4-for-1)
- `split_from`: Denominator of ratio (e.g., 1 in 4-for-1)
- `split_to`: Numerator of ratio (e.g., 4 in 4-for-1)

**Processing Method:** `process_split(symbol, split_date, ratio)`

**Behavior:**

- Adjusts all lots for the symbol across all strategies
- Updates position quantities and average prices
- If no position exists: Silently ignored (logged as debug)

### 2. Cash Dividends

**Type:** `"dividend"`

**What We Support:**

- Regular cash dividends
- Special dividends
- Applies to both long and short positions
- Different treatment for long vs short

**How We Process:**

- **Long positions**: Cash increases (dividend income)
- **Short positions**: Cash decreases (dividend expense)
- Tracks dividends received/paid per position
- Tracks cumulative dividends received/paid globally
- Creates ledger entry for audit trail

**Example:**

```python
# Long 100 shares, $0.82 dividend
# Cash increases: +$82
# Position tracking: dividends_received += $82

# Short 50 shares, $0.82 dividend
# Cash decreases: -$41
# Position tracking: dividends_paid += $41
```

**Event Schema Fields Used:**

- `action_type`: "dividend"
- `ex_date`: Ex-dividend date
- `dividend_amount`: Amount per share (Decimal string)
- `dividend_currency`: ISO 4217 currency code (e.g., "USD")
- `dividend_type`: "ordinary" | "qualified" | "special" | "capital_return" | "stock"

**Processing Method:** `process_dividend(symbol, ex_date, amount_per_share)`

**Behavior:**

- Processes for all positions with matching symbol
- Cash flow = quantity × amount_per_share
- If no position exists: Silently ignored (logged as debug)

## Extensible Design

### Current Implementation

The `on_corporate_action()` handler uses an extensible dispatch pattern:

```python
def on_corporate_action(self, event: CorporateActionEvent) -> None:
    """Handle corporate action event - extensible dispatch."""

    if event.action_type.lower() == "split":
        self.process_split(...)

    elif event.action_type.lower() == "dividend":
        self.process_dividend(...)

    else:
        # Log unsupported types for future extension
        logger.info("corporate_action.unsupported", ...)
```

### Adding New Corporate Actions

To add support for new corporate action types:

1. **Add processing method** in `PortfolioService`:

   ```python
   def process_<action_type>(
       self,
       symbol: str,
       effective_date: datetime,
       # ... action-specific parameters
   ) -> None:
       """Process <action type> corporate action."""
       # Implementation
   ```

1. **Add dispatch case** in `on_corporate_action()`:

   ```python
   elif event.action_type.lower() == "<new_type>":
       if event.<required_field> is None:
           logger.warning("Missing required field")
           return
       self.process_<action_type>(
           symbol=event.symbol,
           effective_date=ex_date_dt,
           # ... map event fields to method parameters
       )
   ```

1. **Add integration tests** in `test_corporate_actions_integration.py`

1. **Update documentation** (this file)

## Future Extensions

The following corporate action types are supported in the event schema but not yet implemented:

### Reverse Splits

**Type:** `"reverse_split"`

**What It Would Do:**

- Reduce share quantity (e.g., 1-for-4 means 100 shares → 25 shares)
- Increase price proportionally (e.g., $10 → $40)
- Preserve total value

**Implementation Notes:**

- Could reuse `process_split()` with ratio < 1 (e.g., 0.25 for 1-for-4)
- Or create separate `process_reverse_split()` method for clarity

### Stock Dividends

**Type:** `"dividend"` with `dividend_type="stock"`

**What It Would Do:**

- Issue additional shares instead of cash
- Similar to split but only for long positions
- Short positions might need to deliver shares

**Implementation Notes:**

- Would need new `process_stock_dividend()` method
- More complex than stock split due to different tax treatment
- Need to handle fractional shares

### Special Dividends

**Type:** `"special_dividend"`

**What It Would Do:**

- One-time large cash distribution
- Same mechanics as regular dividend
- Different tax/accounting treatment

**Implementation Notes:**

- Could reuse `process_dividend()`
- Or create separate method to distinguish in ledger

### Rights Issues

**Type:** `"rights_issue"`

**What It Would Do:**

- Grant rights to purchase additional shares at discount
- Complex: need to track rights as separate instrument
- Need to model exercise decisions

**Implementation Notes:**

- Significant effort, requires:
  - Rights position tracking
  - Exercise modeling
  - Expiration handling

### Mergers

**Type:** `"merger"`

**What It Would Do:**

- Convert shares from company A to company B
- May involve cash + stock combinations
- May involve fractional shares

**Implementation Notes:**

- Complex: need to:
  - Close position in acquired company
  - Open position in acquiring company
  - Handle cash-for-fractional-shares
  - Track cost basis correctly

### Spinoffs

**Type:** `"spinoff"`

**What It Would Do:**

- Receive shares in new company proportional to holdings
- Original company shares unchanged
- Cost basis split between companies

**Implementation Notes:**

- Complex: need to:
  - Create new position in spinoff company
  - Allocate cost basis
  - Track both positions

### Symbol Changes

**Type:** `"symbol_change"`

**What It Would Do:**

- Update symbol while preserving all position details
- Maintain audit trail

**Implementation Notes:**

- Straightforward:
  - Update symbol in position
  - Update symbol in all lots
  - Create ledger entry documenting change

### Delistings

**Type:** `"delisting"`

**What It Would Do:**

- Mark position as delisted
- May require forced liquidation or transfer
- Track worthless securities for tax purposes

**Implementation Notes:**

- Need policy decisions:
  - Auto-liquidate at last price?
  - Mark as worthless?
  - Transfer to OTC market?

## Position-Agnostic Handling

The current implementation follows a defensive design:

**If corporate action occurs for a symbol not in portfolio:**

- Event is silently ignored
- Logged at debug level
- No error raised
- No position created

This design choice allows:

- Corporate action data to be broadcast to all portfolios
- Each portfolio handles only relevant actions
- No special filtering required in DataService

## Data Schema Reference

Corporate actions are validated against `src/qs_trader/contracts/schemas/data/corporate_action.v1.json`.

Key fields:

- `symbol`: Security identifier
- `asset_class`: "equity" | "equity_option" | "future" | etc.
- `action_type`: Type of corporate action (see schema for full list)
- `announcement_date`: ISO 8601 date (YYYY-MM-DD)
- `ex_date`: Ex-date when action takes effect
- `effective_date`: When adjustments applied
- `record_date`: Optional, holders as of this date entitled
- `payment_date`: Optional, when cash distributed

Action-specific fields:

- Splits: `split_ratio`, `split_from`, `split_to`
- Dividends: `dividend_amount`, `dividend_currency`, `dividend_type`
- Price adjustments: `price_adjustment_factor`, `volume_adjustment_factor`
- Symbol changes: `new_symbol`

## Ledger and Audit Trail

All corporate actions create ledger entries for complete audit trail:

**Split Entry:**

```python
LedgerEntry(
    entry_type=LedgerEntryType.SPLIT,
    symbol="AAPL",
    quantity=400,  # New quantity after split
    price=100,     # New avg price after split
    cash_flow=0,   # No cash impact
    metadata={
        "ratio": "4.0",
        "split_type": "split",
        "strategy_id": "strategy_1"
    }
)
```

**Dividend Entry:**

```python
LedgerEntry(
    entry_type=LedgerEntryType.DIVIDEND,
    symbol="AAPL",
    quantity=100,  # Position quantity
    price=0.82,    # Dividend per share
    cash_flow=82,  # Total dividend received
    metadata={
        "amount_per_share": "0.82",
        "strategy_id": "strategy_1"
    }
)
```

## Testing

### Unit Tests

- Location: `tests/unit/services/portfolio/test_corporate_actions.py`
- Coverage: Individual methods (`process_split`, `process_dividend`)
- Tests edge cases: negative amounts, zero ratio, missing positions

### Integration Tests

- Location: `tests/integration/services/portfolio/test_corporate_actions_integration.py`
- Coverage: End-to-end event flow (DataService → EventBus → Portfolio)
- Tests:
  - Dividend increases cash for long positions
  - Dividend decreases cash for short positions
  - Split adjusts quantities and prices
  - Reverse split works correctly
  - Multiple positions across strategies
  - No-position scenarios (silently ignored)

### Manual Validation

Real backtest with AAPL corporate actions:

- **2020-08-07**: $0.82 dividend on 215 shares → $176.30 cash increase ✓
- **2020-08-31**: 4-for-1 split, 190 shares → 760 shares @ 1/4 price ✓

## Configuration

No special configuration required. Corporate action handling is:

- Always enabled when DataService provides corporate action data
- Automatically handles all positions
- Works across all strategies in portfolio

## Logging

Corporate actions generate structured log events:

**When processed successfully:**

```python
logger.info("portfolio_service.split_processed",
    symbol="AAPL",
    strategy_id="strategy_1",
    ratio=4.0,
    new_quantity=400.0,
    new_avg_price=100.0)

logger.info("portfolio_service.dividend_processed",
    symbol="AAPL",
    strategy_id="strategy_1",
    amount_per_share=0.82,
    cash_flow=82.0)
```

**When skipped (no position):**

```python
logger.debug("portfolio_service.split_skipped",
    symbol="MSFT",
    reason="No position found for this symbol")
```

**When invalid data:**

```python
logger.warning("portfolio_service.corporate_action.invalid_split",
    symbol="AAPL",
    reason="Split event missing split_ratio")
```

**When unsupported type:**

```python
logger.info("portfolio_service.corporate_action.unsupported",
    symbol="AAPL",
    action_type="merger",
    message="Corporate action type not yet implemented")
```

## State Tracking

Corporate actions are tracked in portfolio state:

**Global Metrics:**

- `total_dividends_received`: Cumulative dividends received (long positions)
- `total_dividends_paid`: Cumulative dividends paid (short positions)

**Per-Position Metrics:**

- `dividends_received`: Dividends received for this position
- `dividends_paid`: Dividends paid for this position

Access via:

```python
state = portfolio.get_state()
print(f"Total dividends: {state.total_dividends_received}")

position = portfolio.get_position("AAPL")
print(f"AAPL dividends: {position.dividends_received}")
```

## Performance Considerations

- Corporate actions are processed synchronously in event handler
- Priority 90 ensures processing before strategy signals
- Position lookups are O(1) via dictionary
- Lot adjustments are O(n) where n = number of lots for symbol
- For large portfolios with many lots, consider batching optimizations

## Error Handling

The implementation follows a defensive error handling strategy:

**Invalid Ratio/Amount:**

- Splits with ratio ≤ 0: Raises `ValueError` immediately
- Dividends with amount < 0: Raises `ValueError` immediately

**Missing Required Fields:**

- Missing `split_ratio` for split: Logs warning, returns early
- Missing `dividend_amount` for dividend: Logs warning, returns early

**No Position Found:**

- Logs at debug level
- Returns silently (no exception)
- Allows broadcast of corporate actions to all portfolios

**Unsupported Action Types:**

- Logs at info level with action_type
- Returns silently
- Enables gradual implementation of new types

## Best Practices

1. **Always provide complete corporate action data** including all required fields per schema
1. **Use decimal strings** for amounts to avoid floating point precision issues
1. **Broadcast all corporate actions** - let portfolios filter based on holdings
1. **Monitor logs** for "unsupported" messages to identify needed extensions
1. **Test with real data** using backtests to validate accuracy
1. **Verify audit trail** using ledger entries after corporate actions

## Related Documentation

- [Corporate Actions Report](./CORPORATE_ACTIONS_REPORT.md) - Investigation that led to current implementation
- [Event Schema](../src/qs_trader/contracts/schemas/data/corporate_action.v1.json) - Complete schema definition
- [Portfolio Service](../src/qs_trader/services/portfolio/service.py) - Implementation code
- [Integration Tests](../tests/integration/services/portfolio/test_corporate_actions_integration.py) - Test examples
