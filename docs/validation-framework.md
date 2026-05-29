# QS-Trader Validation Framework

## Overview

The validation framework extends QS-Trader from a single-run backtesting tool into a validation-aware research platform. It sits **above** the existing `BacktestEngine` as an orchestration layer that runs multiple child backtests, aggregates results, and produces a structured pass/fail decision.

The framework answers institutional-grade questions that a single backtest cannot:

- Did this strategy's performance survive outside the research period?
- How large was the in-sample → out-of-sample performance decay?
- Does the strategy meet explicit, auditable pass/fail criteria?
- Is there a complete evidence pack for reproducibility review?

The full requirement narrative is in [docs/out-of-sample-validation-requirement.md](out-of-sample-validation-requirement.md).

## Package Layout

```text
src/qs_trader/validation/
├── __init__.py
├── aggregation.py      # MetricsAggregator — IS/OOS metric comparison and decay
├── audit.py            # AuditWriter — environment, git, holdout, SHA256 files
├── child_config.py     # derive_child_config — pure BacktestConfig derivation per split
├── cli.py              # qs-trader validate command implementation
├── decision.py         # DecisionEngine — declarative rule evaluation
├── plan.py             # ValidationPlan model, YAML loader, plan hash
├── reporting/
│   ├── __init__.py     # exports SummaryWriter, ValidationHTMLReporter
│   ├── html.py         # ValidationHTMLReporter — standalone HTML
│   └── summary.py      # SummaryWriter — summary.json + effective_plan.yaml
├── runner.py           # SequentialValidationRunner, ChildRunRef, ChildRunFailedError
└── splits/
    ├── __init__.py     # get_split_generator dispatch
    ├── base.py         # ValidationSplit dataclass, SplitGenerator Protocol
    ├── static.py       # StaticSplitGenerator — Phase 1 IS/OOS split
    └── walk_forward.py # WalkForwardSplitGenerator — Phase 2A anchored/rolling
```

The package is entirely additive. No files under `src/qs_trader/engine/` or `src/qs_trader/services/` were modified to land Phase 1. The CLI entry point (`src/qs_trader/cli/main.py` and `cli/commands/__init__.py`) was extended to register `validate_command`, but the existing `backtest_command` behavior is unchanged.

## Plan YAML Reference

A validation plan is a YAML file that lives alongside the experiment it validates:

```text
experiments/<experiment>/
  <experiment>.yaml                       # BacktestConfig (unchanged)
  validations/
    <validation_id>.yaml                  # ValidationPlan (this file)
    <validation_id>/                      # output directory (created on run)
```

### Required fields

| Field                 | Type                                  | Description                                          |
| --------------------- | ------------------------------------- | ---------------------------------------------------- |
| `validation_id`       | `str`                                 | Stable identifier; becomes the output directory name |
| `strategy_experiment` | `str`                                 | Parent experiment id (for provenance only)           |
| `base_config`         | `str` (relative path)                 | Path to `BacktestConfig` YAML, relative to this file |
| `mode`                | `"static_is_oos"` \| `"walk_forward"` | Validation mode (see below for walk-forward fields)  |
| `splits`              | `StaticSplitSpec`                     | IS and OOS date ranges (see below)                   |

### `splits` block

```yaml
splits:
  in_sample:
    start_date: "2018-01-02"   # YYYY-MM-DD
    end_date:   "2021-12-31"   # must be strictly before out_of_sample.start_date
  out_of_sample:
    start_date: "2022-01-03"
    end_date:   "2024-12-31"
```

Both `DateRange` objects require `end_date` strictly after `start_date`. `out_of_sample.start_date` must be strictly after `in_sample.end_date`.

### `splits` block — `mode: walk_forward`

Walk-forward plans use duration-based window parameters instead of explicit date ranges:

```yaml
splits:
  style: rolling        # rolling (fixed train length) | anchored (expanding train)
  train: 2y             # training window duration (Ny | Nmo | Nd)
  test: 1y              # test/OOS window duration
  step: 1y              # how far the window advances each fold (must be >= test)
  embargo: 0d           # gap between train end and test start (default 0d)
  total_range:
    start_date: "2010-01-01"   # earliest date for the first training window
    end_date:   "2024-12-31"   # fold generation stops when test end exceeds this
  min_fold_bars: null   # optional: minimum test-window days; shorter folds → Invalid
```

Duration strings accept `Ny` (years), `Nmo` (months), or `Nd` (days); combined units (e.g. `1y2mo`) are rejected.

**Rolling vs anchored:**

- `rolling`: the training window start advances by `step` each fold; train duration stays fixed.
- `anchored`: the training window start is fixed at `total_range.start_date`; the train window expands by `step` each fold.

**Embargo:** The calendar gap (in days) between `train_range.end_date` and the fold's test window `start_date`. Zero embargo means the test window starts the day after training ends.

**`min_fold_bars`:** When set, any fold whose test window spans fewer calendar days than this value is marked `status=invalid` with `reason=insufficient_history_for_fold:<n>`. Invalid folds appear in `--dry-run` output tagged `[INVALID: …]` and are never executed.

> **Phase 2A.1 note:** `--dry-run` is fully supported for `walk_forward` plans. Non-dry-run execution of `walk_forward` plans requires Phase 2A.2 runner support and currently exits `Invalid` (code 3) with an explanatory message.

> **Backward compatibility:** `static_is_oos` plans are unaffected by Phase 2A.1. All Phase 1 artifacts round-trip byte-identically. The static plan hash is pinned to prefix `428e27b2`; any intentional change requires updating the pin and getting reviewer sign-off.

### Optional fields

| Field         | Type                | Default   | Description                                                               |
| ------------- | ------------------- | --------- | ------------------------------------------------------------------------- |
| `holdout`     | `HoldoutSpec`       | `null`    | Optional holdout period (recorded only; not executed in Phase 1)          |
| `description` | `str`               | `null`    | Human-readable label; accepted and stored but excluded from the plan hash |
| `benchmark`   | `BenchmarkRef`      | `null`    | Optional benchmark symbol + data source for summary                       |
| `metrics`     | `MetricsCatalog`    | see below | Required and recommended metrics                                          |
| `decision`    | `DecisionRulesSpec` | all null  | Pass/fail rules (disabled when null)                                      |
| `execution`   | `ExecutionSpec`     | see below | Child run failure handling                                                |
| `reporting`   | `ReportingSpec`     | see below | HTML and console output toggles                                           |

### `holdout` block

```yaml
holdout:
  start_date: "2025-01-02"
  end_date:   "2025-12-31"
```

When declared, the holdout period is recorded in `audit/holdout.json` but not executed. Holdout enforcement (blocking re-runs) is Phase 4.

### `benchmark` block

```yaml
benchmark:
  symbol: "SPY"
  data_source: "yahoo-us-equity-1d-csv"
```

Rendered in `summary.json` if the data-source resolves at runtime. Full overlay charts are deferred to Phase 2.

### `metrics` block

```yaml
metrics:
  required:
    - total_return
    - cagr
    - sharpe_ratio
    - max_drawdown
    - volatility
    - num_trades
  recommended:
    - sortino_ratio
    - win_rate
    - turnover
```

These are the defaults. Override only when the experiment's strategy does not produce the standard metric set.

### `decision` block

```yaml
decision:
  rules:
    oos_sharpe_min: 0.8                      # OOS Sharpe must be >= threshold
    oos_max_drawdown_max: 0.25               # OOS max drawdown must be <= threshold
    is_to_oos_sharpe_decay_max: 0.5          # decay = (is - oos) / max(|is|, ε)
    min_oos_trades: 30                       # OOS trade count must be >= threshold
    require_positive_oos_total_return: true  # OOS total return must be > 0
  on_review_required:
    - is_to_oos_sharpe_decay_warn: 0.3       # decay > 0.3 → ReviewRequired (not Fail)
```

All rules are optional. A rule with a `null` value (or omitted entirely) is disabled. `DecisionRulesSpec` uses `extra="forbid"`, so unknown rule keys cause a load-time error.

### `execution` block

```yaml
execution:
  on_child_failure: fail_fast   # fail_fast (default) | continue
```

`fail_fast` aborts after the first failing child run and writes an `Invalid` evidence pack. `continue` collects partial artifacts and still evaluates available metrics.

### `reporting` block

```yaml
reporting:
  html: true            # write report.html (default: true)
  console_summary: true # print Rich summary table (default: true)
```

## Decision Rule Catalog

### Fail rules

Breach of any enabled fail rule sets the outcome to `Fail`.

| Rule key                            | Field compared   | Condition             |
| ----------------------------------- | ---------------- | --------------------- |
| `oos_sharpe_min`                    | OOS Sharpe       | `actual >= threshold` |
| `oos_max_drawdown_max`              | OOS max DD       | `actual <= threshold` |
| `is_to_oos_sharpe_decay_max`        | Sharpe decay     | `actual <= threshold` |
| `min_oos_trades`                    | OOS trade count  | `actual >= threshold` |
| `require_positive_oos_total_return` | OOS total return | `actual > 0`          |

**Sharpe decay formula:** `decay = (is_sharpe - oos_sharpe) / max(|is_sharpe|, ε)` where `ε = 1e-6`. A value of `0.5` means OOS Sharpe fell by more than 50% relative to IS.

`oos_max_drawdown_max` uses a positive-loss convention (`0.25` = 25% drawdown).

### Review-required rules

Breach downgrades the outcome from `Pass` to `ReviewRequired` (never to `Fail`). These live under `decision.on_review_required`.

| Rule key                      | Field compared | Condition             |
| ----------------------------- | -------------- | --------------------- |
| `is_to_oos_sharpe_decay_warn` | Sharpe decay   | `actual <= threshold` |

Example: set `is_to_oos_sharpe_decay_warn: 0.3` to flag decays > 30% for manual review while allowing them to pass if the harder `is_to_oos_sharpe_decay_max` threshold is met.

## Output Directory Layout

```text
experiments/<experiment>/
  validations/
    <validation_id>/
      plan.yaml                # copy of the submitted plan
      effective_plan.yaml      # post-normalization effective plan
      summary.json             # full validation summary (see §4.3 in the spec)
      report.html              # validation HTML report
      audit/
        environment.json       # python version, qs_trader version, platform
        git.json               # commit SHA, branch, dirty flag
        plan_sha256.txt        # SHA-256 of effective plan + base config bytes
        base_config_sha256.txt # SHA-256 of the base config YAML bytes
        holdout.json           # holdout declared/consumed state
      folds/
        f0__is/                # full single-run artifact directory (IS child)
          …
        f1__oos/               # full single-run artifact directory (OOS child)
          …
```

Each `folds/<fold_id>/` directory is a standard QS-Trader filesystem artifact output (the same structure produced by `qs-trader backtest` with `artifact_policy.mode=filesystem`).

## Audit Pack Contents

The `audit/` directory provides a self-contained evidence pack for reproducibility review.

| File                     | Contents                                                                                                          |
| ------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| `environment.json`       | `python_version`, `qs_trader_version`, `platform`, `os_name`. No `os.environ` dump.                               |
| `git.json`               | `commit` (SHA), `branch`, `dirty` (bool) — derived from QS-Trader repo root                                       |
| `plan_sha256.txt`        | SHA-256 of `sha256(plan_canonical_json \|\| base_config_bytes)`                                                   |
| `base_config_sha256.txt` | SHA-256 of the raw base config YAML bytes                                                                         |
| `holdout.json`           | `declared`, `start_date`, `end_date`, `consumed`, `consumed_at`, `consumed_by_plan_id`, `consumed_at_code_commit` |

`environment.json` deliberately omits raw environment variables to prevent accidental secret leakage (OWASP A02).

## Exit Codes

| Code | Outcome        | When                                                                       |
| ---- | -------------- | -------------------------------------------------------------------------- |
| `0`  | Pass           | All enabled rules pass                                                     |
| `1`  | Fail           | One or more fail rules breached                                            |
| `2`  | ReviewRequired | All fail rules pass but a review-required rule was breached                |
| `3`  | Invalid        | Configuration error, missing data, or child run failed in `fail_fast` mode |
| `4`  | Exception      | Unhandled runtime exception                                                |

> **Note:** Click's built-in argument-validation errors (missing or invalid flags) also return exit code `2`. The error message on stderr identifies which case applies.

## Troubleshooting

### `ValueError: end_date must be strictly after start_date`

Check the `splits` block. Both `in_sample` and `out_of_sample` require `end_date > start_date`. Also verify `out_of_sample.start_date > in_sample.end_date`.

### `ValueError: Validation plan directory … must contain '<name>.yaml'`

When passing a directory as `PLAN_PATH`, the loader expects a file named `<directory_name>.yaml` inside it. Rename the YAML file or pass the file path directly.

### `ValidationError: … extra inputs are not permitted` (inside `decision`)

`DecisionRulesSpec` uses `extra="forbid"`. Check that all rule keys under `decision.rules` are from the known catalog: `oos_sharpe_min`, `oos_max_drawdown_max`, `is_to_oos_sharpe_decay_max`, `min_oos_trades`, `require_positive_oos_total_return`. `on_review_required` accepts only `is_to_oos_sharpe_decay_warn`.

### `ValidationError: … extra inputs are not permitted` (at plan root)

`ValidationPlan` uses `extra="forbid"` at the root level. Unknown top-level keys (e.g. future Phase 2 fields like `cost_scenarios` on a `static_is_oos` plan) are rejected at load time.

### Exit code `3` — `walk_forward` non-dry-run not yet supported

Running `qs-trader validate <walk_forward_plan.yaml>` without `--dry-run` exits with code 3 and the message: `Error: walk_forward execution is not yet supported (Phase 2A.2+). Use --dry-run to inspect the generated splits.`

Phase 2A.2 runner support is required for live execution. Use `--dry-run` to preview the generated folds.

### Exit code `3` with `ChildRunFailedError`

A child backtest raised an exception. Check the per-fold log output for the root cause. Common causes: data source not available for the date range, missing symbol in universe, base config paths incorrect relative to the working directory. Pass `--on-child-failure continue` to collect partial results from the other fold before diagnosing.

### `FileExistsError: output directory already exists`

The `validations/<validation_id>/` directory from a previous run already exists. Pass `--force` to overwrite, or delete the directory manually.

### `outcome: Invalid` with empty `rule_results`

At least one child run failed in `fail_fast` mode (or both failed in `continue` mode). The decision engine produces `Invalid` when required metrics are unavailable. Inspect `summary.json → folds[*].status` and the fold artifact directories for details.
