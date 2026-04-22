# Test Fixtures for QS-Trader

This directory contains **fixed, controlled test fixtures** for integration and unit tests.

## Purpose

Test fixtures provide **predictable, known inputs** for tests to ensure:

- Tests are stable and don't break when users modify their own configurations
- Test expectations are explicit and documented
- Tests can be understood without hunting through user-modifiable files

## Guidelines

### ✅ DO Use Test Fixtures For

1. **Integration tests** that need specific behavior
1. **Tests with exact numeric expectations** (e.g., "expect 47 shares")
1. **Regression tests** that verify specific bug fixes
1. **Performance benchmarks** that need consistent baselines

### ❌ DON'T Use User Configs in Tests

**Bad Example:**

```python
# DON'T DO THIS - user can modify naive.yaml and break tests
config_dict = {"name": "naive", "config": {}}
```

**Good Example:**

```python
# DO THIS - use dedicated test fixture
config_dict = {"name": "test_naive", "config": {}}
```

## Risk Policy Fixtures

### `test_naive.yaml`

- **Purpose**: Basic risk policy for integration tests
- **Key Parameters**:
  - Sizing: 5% of allocated capital per position
  - Budget: Auto-creates default at 95% allocation
  - Leverage: 1.0 (no leverage)
  - Shorting: Disabled
- **Used By**: Manager service tests, full lifecycle tests
- **DO NOT MODIFY** - tests depend on these exact values

## Data Fixtures

### `data/sma_crossover_duplicate_window.json`

- **Purpose**: Deterministic Phase 3 incident-window reconstruction for the documented `2021-10-22` / `2021-10-25` duplicate-open scenario
- **Used By**: `tests/integration/test_duplicate_open_gate.py`
- **Provenance**: Derived from the checked-in Research duplicate-window fixture because the exact `r-001-68fe9c2c` payload is not present in-repo
- **DO NOT MODIFY** - the regression depends on these exact timestamps and closes

## Adding New Fixtures

When creating a new test fixture:

1. **Name it clearly**: Use `test_` prefix (e.g., `test_aggressive.yaml`)
1. **Document extensively**: Add comments explaining what each parameter is for
1. **Mark as immutable**: Include "DO NOT MODIFY" warnings
1. **Reference in tests**: Update test docstrings to mention the fixture

Example:

```yaml
# Test Fixture: Aggressive Risk Policy
# =====================================
# DO NOT MODIFY - Used by test_aggressive_sizing.py
#
# This fixture tests high-risk scenarios with:
# - 20% position sizing
# - 2.0x leverage allowed
# - Short selling enabled

portfolio_risk_policy:
  name: "test_aggressive"
  # ... fixed parameters
```

## Search Order

The risk policy loader searches in this order:

1. **Test fixtures**: `tests/fixtures/risk_policies/{name}.yaml` (highest priority)
1. **Built-in**: `src/qs_trader/libraries/risk/builtin/{name}.yaml`
1. **Custom**: `my_library/risk_policies/{name}.yaml` (user-modifiable)

This ensures test fixtures always take precedence in test environments.

## See Also

- User-modifiable policies: `my_library/risk_policies/`
- Built-in policies: `src/qs_trader/libraries/risk/builtin/`
- Risk policy documentation: `docs/packages/risk_library.md`
