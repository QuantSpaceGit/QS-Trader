# Candidate Scan Mode

QS-Trader's candidate scan mode provides a dedicated execution path for evaluating instruments against candidate rules without requiring a full portfolio backtest. It is the primary tool for research that needs rolling OHLCV access, stable identity, and reproducible parameter metadata.

## Overview

The scan mode:

1. Resolves tickers or secids to instruments via `InstrumentResolver`.
1. Loads OHLCV data from ClickHouse or a pass-through loader.
1. Builds a `ScanRuleContext` for each bar and passes it to the candidate rule.
1. Computes forward returns, MFE, and MAE for each bar.
1. Persists results to `candidate_scan_results.csv` and a `scan_manifest.json`.

## Scan Rule Contract

### New-Style Rule (Preferred)

Candidate rules receive a `ScanRuleContext` and return a `ScanDecision`:

```python
from qs_trader.services.scan import ScanRuleContext, ScanDecision

def candidate_rule(context: ScanRuleContext) -> ScanDecision:
    # Access current bar data
    current_close = context.close[context.bar_index]
    current_date = context.date

    # Access rolling history
    if context.bar_index >= 20:
        window = context.close[context.bar_index - 20: context.bar_index]
        rolling_mean = sum(window) / len(window)
    else:
        return ScanDecision(
            candidate_status="not_ready",
            reason_code="insufficient_history",
        )

    if current_close > rolling_mean * 1.05:
        return ScanDecision(
            candidate_status="accepted",
            reason_code="breakout_above_rolling_mean",
            score=current_close,
            gates={"price_gate": True},
            diagnostics={"close": current_close, "rolling_mean": rolling_mean},
        )

    return ScanDecision(
        candidate_status="rejected",
        reason_code="no_breakout",
        score=current_close,
        gates={"price_gate": False},
        diagnostics={"close": current_close, "rolling_mean": rolling_mean},
    )
```

### Reading `context.close` and `context.bar_index`

The `bar_index` field points to the current bar within the context's arrays:

```python
# Current bar close price
close = context.close[context.bar_index]

# Previous bar close
prev_close = context.close[context.bar_index - 1]

# 5-bar rolling average
if context.bar_index >= 5:
    window = context.close[context.bar_index - 5: context.bar_index]
    avg = sum(window) / len(window)
```

### ScanDecision Return

A `ScanDecision` has these fields:

| Field              | Type          | Description                                            |
| ------------------ | ------------- | ------------------------------------------------------ |
| `candidate_status` | str           | One of: `accepted`, `rejected`, `ignored`, `not_ready` |
| `reason_code`      | str           | Machine-readable reason for the decision               |
| `score`            | float \| None | Optional numeric score                                 |
| `gates`            | dict          | Named gate results (e.g. `{"volume_gate": True}`)      |
| `diagnostics`      | dict          | Numeric/contextual values explaining the decision      |
| `features`         | dict          | Feature values used at evaluation time                 |

### Tuple-Return Adaptation (Legacy)

Existing rules that return a 5-tuple continue to work through a compatibility adapter:

```python
def legacy_rule(context):
    # Returns: (status, reason_code, score, gates, features)
    return ("candidate", "default", 0.5, {}, context.features)
```

The adapter maps the legacy `"candidate"` status to `"accepted"`. Other statuses are passed through. Diagnostics are set to `{}` for legacy tuples.

## CLI Usage

### Basic Scan

```bash
qs-trader scan-candidates \
    --tickers AAPL --tickers MSFT \
    --start-date 2023-01-01 --end-date 2023-12-31
```

### Scan by Secid

```bash
qs-trader scan-candidates \
    --secid 12345 --secid 67890 \
    --start-date 2023-01-01 --end-date 2023-12-31
```

### Scan with ClickHouse Data Source

```bash
qs-trader scan-candidates \
    --tickers AAPL \
    --start-date 2023-01-01 --end-date 2023-12-31 \
    --data-source qs-datamaster
```

### Scan with Custom Rule and Features

```bash
qs-trader scan-candidates \
    --tickers AAPL \
    --start-date 2023-01-01 --end-date 2023-12-31 \
    --rule my_rules.momentum_rule \
    --feature-columns momentum,volatility
```

### Scan with Rule Parameters

```bash
# Via JSON string
qs-trader scan-candidates \
    --tickers AAPL \
    --start-date 2023-01-01 --end-date 2023-12-31 \
    --params-json '{"lookback": 20, "threshold": 0.5}'

# Via JSON file
qs-trader scan-candidates \
    --tickers AAPL \
    --start-date 2023-01-01 --end-date 2023-12-31 \
    --params-file params.json
```

Note: `--params-json` and `--params-file` are mutually exclusive.

### Price Basis

The scan loader uses an explicit price basis, defaulting to the canonical adjusted series used by the ClickHouse backtest adapter:

```bash
# Default: adjusted_ohlc_adj_columns (uses openadj, highadj, etc.)
qs-trader scan-candidates --tickers AAPL \
    --start-date 2023-01-01 --end-date 2023-12-31 \
    --data-source qs-datamaster

# Explicit raw prices
qs-trader scan-candidates --tickers AAPL \
    --start-date 2023-01-01 --end-date 2023-12-31 \
    --data-source qs-datamaster \
    --price-basis raw
```

## Output Files

### candidate_scan_results.csv

The default row output. Columns include:

| Column               | Description                               |
| -------------------- | ----------------------------------------- |
| `date`               | Evaluation date                           |
| `secid`              | Resolved security ID                      |
| `display_symbol`     | Display-friendly symbol                   |
| `ticker_at_date`     | Ticker valid at the date                  |
| `runtime_symbol`     | Symbol used for data loading              |
| `strategy_id`        | Strategy identifier                       |
| `candidate_status`   | accepted, rejected, ignored, or not_ready |
| `reason_code`        | Machine-readable reason                   |
| `score`              | Optional numeric score                    |
| `gates_json`         | JSON-encoded gate results                 |
| `features_json`      | JSON-encoded feature values               |
| `diagnostics_json`   | JSON-encoded diagnostic values            |
| `forward_return_5d`  | 5-day forward log return                  |
| `forward_return_10d` | 10-day forward log return                 |
| `forward_return_20d` | 20-day forward log return                 |
| `mfe_20d`            | 20-day Max Favorable Excursion            |
| `mae_20d`            | 20-day Max Adverse Excursion              |

### scan_manifest.json

Written alongside the CSV. Contains run metadata for reproducibility:

```json
{
  "schema_version": "1.0",
  "generated_at": "2024-01-15T10:00:00+00:00",
  "rule": {
    "strategy_id": "scan",
    "parameter_snapshot": {"lookback": 20},
    "parameter_hash": "a1b2c3..."
  },
  "data": {
    "data_source": "qs-datamaster",
    "price_basis": "adjusted_ohlc_adj_columns"
  },
  "date_range": {
    "start_date": "2023-01-01",
    "end_date": "2023-12-31"
  },
  "universe": {
    "requested_tickers": ["AAPL"],
    "requested_secids": [],
    "ticker_policy": "anchor_first_in_range"
  },
  "resolved_instruments": [...],
  "output_files": {
    "results_csv": "candidate_scan_results.csv",
    "manifest_json": "scan_manifest.json"
  },
  "summary": {
    "total_instruments": 1,
    "instruments_processed": 1,
    "instruments_failed": 0,
    "total_rows": 252,
    "failures": []
  }
}
```

## Deferred Work

Strategy-specific Qullamaggie breakout research is deferred until this scanner upgrade is complete. The following work will happen in QS-Research after this change is available:

- Building the Qullamaggie candidate research experiment.
- Defining and tuning Qullamaggie breakout gates.
- Running one-security analysis, IS/OOS validation, or portfolio simulation.
