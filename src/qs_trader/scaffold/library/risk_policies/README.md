# Custom Risk Policies

Risk policies for position sizing and portfolio risk management in QS-Trader.

## Overview

Risk policies control how QS-Trader manages capital allocation, position sizing, and risk limits. They are defined in YAML configuration files and are automatically loaded by the ManagerService.

**Important**: You do NOT call risk functions directly in your code. The risk system is used internally by ManagerService to calculate position sizes and enforce limits.

## Available Template

### template.yaml

Comprehensive risk policy template with all options documented.

**Key Sections:**

1. **Metadata**: Policy identification and versioning
1. **Evaluation**: When to check risk limits
1. **Strategy Budgets**: Allocate capital across strategies
1. **Sizing**: Position sizing algorithms
1. **Limits**: Risk constraints (position, leverage, concentration, drawdown)

## Creating Custom Risk Policies

### Basic Risk Policy

```yaml
portfolio_risk_policy:
  # Metadata
  name: "conservative"
  description: "Conservative risk policy for long-term investing"
  version: "1.0.0"
  author: "Your Name"
  policy_scope: "portfolio"

  # Evaluation timing
  evaluation:
    mode: "end_of_day"
    clock_alignment: "exchange_calendar"

  # Capital allocation (optional - defaults to 95% in single "default" budget)
  budgets:
    - strategy_id: "default"
      capital_weight: 0.90  # 90% allocated, 10% cash reserve

  # Position sizing
  sizing:
    algorithm: "fixed_equity_pct"
    fixed_equity_pct: 0.20  # 20% of allocated capital per position
    adjust_for_confidence: true  # Scale by signal confidence

  # Risk limits
  limits:
    # Maximum position size
    max_position_pct: 0.25  # No single position > 25% of allocated capital

    # Maximum portfolio leverage
    max_gross_leverage: 1.0  # Long-only (no leverage)
    max_net_leverage: 1.0

    # Concentration limits
    max_sector_exposure: 0.40  # Max 40% in any sector
    max_correlated_exposure: 0.60  # Max 60% in correlated positions

    # Drawdown protection
    max_drawdown_pct: 0.20  # Stop trading at 20% drawdown
    drawdown_action: "stop_new_positions"
```

### Multi-Strategy Portfolio

```yaml
portfolio_risk_policy:
  name: "multi_strategy"
  description: "Balanced allocation across multiple strategies"

  # Allocate capital across strategies
  budgets:
    - strategy_id: "trend_following"
      capital_weight: 0.40  # 40% to trend following

    - strategy_id: "mean_reversion"
      capital_weight: 0.30  # 30% to mean reversion

    - strategy_id: "momentum"
      capital_weight: 0.20  # 20% to momentum

    # Total: 90% allocated, 10% unallocated cash reserve

  sizing:
    algorithm: "fixed_equity_pct"
    fixed_equity_pct: 0.50  # 50% of strategy's allocated capital per position
    adjust_for_confidence: true

  limits:
    max_position_pct: 0.30
    max_gross_leverage: 1.5  # Allow 50% leverage
    max_net_leverage: 1.0
    max_drawdown_pct: 0.15
```

### Aggressive Growth Policy

```yaml
portfolio_risk_policy:
  name: "aggressive_growth"
  description: "Higher risk tolerance for maximum growth"

  budgets:
    - strategy_id: "default"
      capital_weight: 0.95  # 95% deployed

  sizing:
    algorithm: "fixed_equity_pct"
    fixed_equity_pct: 0.40  # 40% of allocated capital per position
    adjust_for_confidence: true

  limits:
    max_position_pct: 0.50  # Allow concentrated positions
    max_gross_leverage: 2.0  # Allow 2x leverage
    max_net_leverage: 1.5
    max_sector_exposure: 0.60
    max_drawdown_pct: 0.30  # Higher drawdown tolerance
    drawdown_action: "reduce_positions"  # Reduce but don't stop
```

## Configuration Sections

### 1. Metadata

Basic policy information:

```yaml
name: "my_policy"  # Must match filename
description: "Policy description"
version: "1.0.0"
date_created: "2025-01-01"
author: "Your Name"
policy_scope: "portfolio"  # Currently only "portfolio" supported
```

### 2. Evaluation

When to check risk limits:

```yaml
evaluation:
  mode: "end_of_day"  # Options: end_of_day, bar_close, intraday
  clock_alignment: "exchange_calendar"
```

**Mode Options:**

- `end_of_day`: Check limits at market close (daily strategies)
- `bar_close`: Check limits at each bar close (intraday)
- `intraday`: Check limits on every signal/order (high-frequency)

### 3. Strategy Budgets (Capital Allocation)

Allocate capital across multiple strategies:

```yaml
budgets:
  - strategy_id: "strategy_name"
    capital_weight: 0.30  # 30% of portfolio equity

  - strategy_id: "default"  # Fallback for unlisted strategies
    capital_weight: 0.60  # 60% of portfolio equity

# Total weights must be ≤ 1.0
# Remaining (1.0 - sum) stays as unallocated cash
```

**Important Notes:**

- Each strategy gets: `allocated_capital = equity × capital_weight`
- If omitted, system creates default budget at 95% weight
- Use `"default"` as strategy_id for fallback allocation
- Weights must sum to ≤ 1.0

**Examples:**

Equal allocation:

```yaml
budgets:
  - strategy_id: "sma_crossover"
    capital_weight: 0.30
  - strategy_id: "momentum"
    capital_weight: 0.30
  - strategy_id: "mean_reversion"
    capital_weight: 0.30
# 90% allocated, 10% cash
```

Core-satellite:

```yaml
budgets:
  - strategy_id: "buy_and_hold"
    capital_weight: 0.60  # 60% core
  - strategy_id: "momentum"
    capital_weight: 0.20  # 20% satellite
  - strategy_id: "mean_reversion"
    capital_weight: 0.15  # 15% satellite
# 95% allocated, 5% cash
```

### 4. Position Sizing

How to calculate position sizes from signals:

```yaml
sizing:
  algorithm: "fixed_equity_pct"  # Current: fixed_equity_pct (future: volatility_targeting, kelly_criterion)
  fixed_equity_pct: 0.20  # 20% of allocated capital per position
  adjust_for_confidence: true  # Scale by signal confidence (0.0-1.0)
```

**Two-Layer Allocation Model:**

```
Step 1: Budget Layer
  allocated_capital = equity × budget_weight
  Example: $100k × 0.95 = $95k

Step 2: Sizing Layer
  position_size = allocated_capital × fixed_equity_pct × signal_strength
  Example: $95k × 0.20 × 1.0 = $19k
```

**Key Point**: `fixed_equity_pct` applies to ALLOCATED CAPITAL, not total equity!

- `fixed_equity_pct: 1.0` with 95% budget → 95% of equity (not 100%)
- `fixed_equity_pct: 0.5` with 95% budget → 47.5% of equity (not 50%)
- `fixed_equity_pct: 0.2` with 95% budget → 19% of equity (not 20%)

**Confidence Adjustment:**

If `adjust_for_confidence: true`:

```python
position_size = allocated_capital × fixed_equity_pct × signal_confidence
```

Example:

- Signal confidence: 0.8
- Allocated capital: $95k
- Fixed equity pct: 0.20
- Position size: $95k × 0.20 × 0.8 = $15.2k

### 5. Risk Limits

Portfolio-level risk constraints:

```yaml
limits:
  # Position limits
  max_position_pct: 0.25  # Max 25% of allocated capital per position

  # Leverage limits
  max_gross_leverage: 1.5  # (long_value + |short_value|) / equity ≤ 1.5
  max_net_leverage: 1.0    # (long_value - |short_value|) / equity ≤ 1.0

  # Concentration limits
  max_sector_exposure: 0.40      # Max 40% in any sector
  max_correlated_exposure: 0.60  # Max 60% in correlated positions
  correlation_threshold: 0.70     # Positions with corr > 0.7 are "correlated"

  # Drawdown protection
  max_drawdown_pct: 0.20  # Stop at 20% drawdown from peak
  drawdown_action: "stop_new_positions"  # Options: stop_new_positions, reduce_positions, stop_all
```

**Leverage Definitions:**

- **Gross Leverage**: `(long_value + |short_value|) / equity`

  - Measures total market exposure
  - Example: $60k long + $40k short with $100k equity = 1.0 gross leverage

- **Net Leverage**: `(long_value - |short_value|) / equity`

  - Measures directional exposure
  - Example: $60k long + $40k short with $100k equity = 0.2 net leverage

**Drawdown Actions:**

- `stop_new_positions`: Block new positions, allow position exits
- `reduce_positions`: Automatically reduce position sizes
- `stop_all`: Block all trading activity

## Using Risk Policies

### 1. Create Policy File

Save your policy as `library/risk_policies/my_policy.yaml`

### 2. Reference in Portfolio Config

In `config/portfolio.yaml`:

```yaml
portfolio:
  managers:
    - manager_id: "main_manager"
      strategy_ids: ["sma_crossover", "momentum"]

      risk_policy:
        name: "my_policy"  # References library/risk_policies/my_policy.yaml

      initial_capital: 100000.0
      cash_currency: "USD"
```

### 3. Run Backtest

The ManagerService automatically:

1. Loads your risk policy
1. Applies capital allocation to strategies
1. Calculates position sizes based on signals
1. Enforces risk limits

## Common Use Cases

### Conservative Long-Only

```yaml
budgets:
  - strategy_id: "default"
    capital_weight: 0.85  # 85% deployed, 15% cash

sizing:
  fixed_equity_pct: 0.15  # ~12.75% of equity per position
  adjust_for_confidence: true

limits:
  max_position_pct: 0.20
  max_gross_leverage: 1.0  # Long-only
  max_net_leverage: 1.0
  max_drawdown_pct: 0.15
```

### Moderate Long/Short

```yaml
budgets:
  - strategy_id: "default"
    capital_weight: 0.90

sizing:
  fixed_equity_pct: 0.25  # ~22.5% per position
  adjust_for_confidence: true

limits:
  max_position_pct: 0.30
  max_gross_leverage: 1.5  # Allow modest leverage
  max_net_leverage: 0.8
  max_drawdown_pct: 0.20
```

### Aggressive Multi-Strategy

```yaml
budgets:
  - strategy_id: "momentum"
    capital_weight: 0.40
  - strategy_id: "breakout"
    capital_weight: 0.40
  - strategy_id: "default"
    capital_weight: 0.15

sizing:
  fixed_equity_pct: 0.40  # Concentrated positions
  adjust_for_confidence: true

limits:
  max_position_pct: 0.50
  max_gross_leverage: 2.0
  max_net_leverage: 1.5
  max_sector_exposure: 0.70
  max_drawdown_pct: 0.30
  drawdown_action: "reduce_positions"
```

## Best Practices

1. **Start conservative**: Begin with lower leverage and position sizes
1. **Test with paper trading**: Validate your policy before live trading
1. **Monitor drawdowns**: Set `max_drawdown_pct` appropriate for your risk tolerance
1. **Use confidence scaling**: Enable `adjust_for_confidence` for better risk management
1. **Diversify strategies**: Allocate across uncorrelated strategies when possible
1. **Keep cash reserves**: Don't allocate 100% of capital (leave buffer)
1. **Version your policies**: Increment version number when making changes
1. **Document changes**: Update description field with policy rationale

## Common Pitfalls

1. **Misunderstanding fixed_equity_pct**: Remember it applies to allocated capital, not total equity
1. **Over-allocation**: Ensure budget weights sum to ≤ 1.0
1. **Leverage confusion**: Know the difference between gross and net leverage
1. **Missing drawdown protection**: Always set `max_drawdown_pct`
1. **Ignoring correlation**: Use `max_correlated_exposure` to prevent concentrated risk
1. **No cash buffer**: Leave some capital unallocated for margin calls and opportunities

## Validation

The system validates your policy on load:

- Budget weights sum to ≤ 1.0
- All percentages in valid ranges
- Required fields present
- Numeric values in expected ranges

Validation errors will appear in logs with specific issues.

## Advanced Topics

### Dynamic Risk Adjustment (Future)

```yaml
sizing:
  algorithm: "volatility_targeting"
  target_volatility: 0.15  # Target 15% annualized volatility
  lookback_period: 60      # Days to measure volatility
```

### Kelly Criterion (Future)

```yaml
sizing:
  algorithm: "kelly_criterion"
  win_rate_estimate: 0.55
  fractional_kelly: 0.25  # Use 1/4 Kelly for safety
```

## References

- Risk service: `src/qs_trader/services/risk/`
- Risk calculators: `src/qs_trader/libraries/risk/calculators.py`
- Manager service: `src/qs_trader/services/manager/`
- Template: `src/qs_trader/scaffold/library/risk_policies/template.yaml`
- Portfolio config: `config/portfolio.yaml`
