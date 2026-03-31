# QS-Trader CLI: Backtest Command

## Overview

The `qs-trader backtest` command provides a simple, config-driven interface for running backtests from the command line. It loads backtest configuration from YAML files and executes simulations with optional CLI overrides for quick experimentation.

## Basic Usage

```bash
# Run backtest with configuration file
qs-trader backtest --file config/portfolio.yaml

# Short form
qs-trader backtest -f config/portfolio.yaml
```

## Command Options

### Required Options

- `--file, -f PATH`: Path to backtest configuration file (YAML) **[required]**

### Optional Overrides

- `--silent, -s`: Silent mode - no event display (fastest execution)
- `--replay-speed, -r FLOAT`: Override replay speed
  - `-1.0` = silent (no display)
  - `0.0` = instant display (no delay)
  - `> 0` = delay in seconds per event (e.g., `0.25` = 250ms)
- `--start-date DATE`: Override start date (format: `YYYY-MM-DD`)
- `--end-date DATE`: Override end date (format: `YYYY-MM-DD`)

## Examples

### Basic Execution

```bash
# Run with config defaults
qs-trader backtest --file config/portfolio.yaml
```

### Silent Mode (Fastest)

```bash
# No event display, maximum speed
qs-trader backtest -f config/portfolio.yaml --silent
```

### Quick Date Range Tests

```bash
# Test with shorter date range without modifying config
qs-trader backtest -f config/portfolio.yaml \
    --start-date 2020-01-01 \
    --end-date 2020-03-31
```

### Override Replay Speed

```bash
# Slow motion display (0.5 seconds per event)
qs-trader backtest -f config/portfolio.yaml -r 0.5

# Instant display (no delay)
qs-trader backtest -f config/portfolio.yaml -r 0
```

### Combined Overrides

```bash
# Silent mode with custom date range
qs-trader backtest -f config/test.yaml --silent \
    --start-date 2020-01-01 \
    --end-date 2020-01-31
```

## Output

The command displays:

1. **Configuration Summary**

   - Backtest ID
   - Date range
   - Universe (symbols)
   - Display settings

1. **Execution Progress**

   - Engine initialization
   - Data loading
   - Event display (if enabled)

1. **Results Summary**

   - Date range processed
   - Total bars processed
   - Execution duration
   - Event store location (if file-based)

### Example Output

```
─────────────────────────────── QS-Trader Backtest ───────────────────────────────

Loading configuration...
  Backtest ID: buy_and_hold
  Date Range: 2020-01-01 to 2020-09-30
  Universe: ['AAPL']
  Display: Silent mode (no events)

✓ Engine initialized

Running backtest...

✓ Backtest completed successfully!

────────────────────────────────── RESULTS ─────────────────────────────────────

Date Range:      2020-01-01 to 2020-09-30
Bars Processed:  189
Duration:        0:00:00.563517

Event Store:     memory (no files created)

────────────────────────────────────────────────────────────────────────────────
```

## Event Display

When `replay_speed >= 0`, events are displayed in real-time with Rich formatting:

```
📊 Bar #1 | AAPL | O: 296.27 H: 300.60 L: 295.19 C: 300.35 | Vol: 32,433,732
💼 Portfolio #1 | 2020-01-02T21:00:00 | Equity: $100,000.00 | Cash: $100,000.00
📊 Signal #1 | AAPL | OPEN_LONG | Conf: 1.00
→ Order #1 | AAPL | BUY 316 shares | market
✓ Fill #1 | AAPL | BUY 316 @ $297.10 | Fee: $1.58
```

Event display is controlled by:

- `display_events` in `config/portfolio.yaml` (which events to show)
- `replay_speed` in config or CLI (timing)
- `enable_event_display` in `config/system.yaml` (Rich formatting on/off)

## Override Precedence

CLI options override config file values:

```
CLI --silent         → overrides replay_speed to -1.0
CLI --replay-speed   → overrides config replay_speed
CLI --start-date     → overrides config start_date
CLI --end-date       → overrides config end_date
```

This allows quick experimentation without editing config files.

## Event Store

Event persistence is configured at the system level in `config/system.yaml`:

```yaml
output:
  event_store:
    backend: "memory"      # Options: memory, sqlite, parquet
    filename: "events.{backend}"
```

- **memory**: No files created (fastest, for development)
- **sqlite**: SQLite database (queryable, moderate size)
- **parquet**: Compressed columnar format (smallest size, analytics-friendly)

The CLI displays event store location for file-based backends:

```
Results Dir:     output/backtests/buy_and_hold/20251110_133233/
Event Store:     events.sqlite (1.23 MB)
```

## Tips & Best Practices

### Development Workflow

```bash
# Fast iteration: silent mode with short date ranges
qs-trader backtest -f config/test.yaml --silent \
    --start-date 2020-01-01 --end-date 2020-01-31
```

### Visual Debugging

```bash
# Slow motion display to watch event flow
qs-trader backtest -f config/portfolio.yaml -r 0.5
```

### Production Runs

```bash
# Silent mode for full historical backtests
qs-trader backtest -f config/production.yaml --silent
```

### Quick Config Validation

```bash
# Run 1 month to verify config loads correctly
qs-trader backtest -f config/new_strategy.yaml \
    --start-date 2020-01-01 --end-date 2020-01-31 --silent
```

## Error Handling

The command provides helpful error messages for common issues:

### Missing Config File

```bash
$ qs-trader backtest --file nonexistent.yaml
Error: Invalid value for '--file': File 'nonexistent.yaml' does not exist.
```

### Config Loading Error

```bash
$ qs-trader backtest -f config/bad.yaml
✗ Backtest failed: Invalid config format
[detailed traceback...]
```

### Data Loading Error

```bash
✗ Backtest failed: Failed to load data for symbol INVALID
[detailed traceback...]
```

## Help

Get detailed help anytime:

```bash
# Main CLI help
qs-trader --help

# Backtest command help
qs-trader backtest --help
```

## Integration with `uv`

When using `uv` for project management:

```bash
# Run with uv
uv run qs-trader backtest -f config/portfolio.yaml

# Or activate the environment first
source .venv/bin/activate
qs-trader backtest -f config/portfolio.yaml
```

## Related

- Configuration Guide: See `config/template_portfolio.yaml` for all backtest options
- System Configuration: See `config/system.yaml` for system-wide settings
- Event Display: See system config `logging.enable_event_display` setting
- Example Script: See `basic_run_example.py` for programmatic API usage
