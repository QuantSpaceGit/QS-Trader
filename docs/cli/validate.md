# `qs-trader validate`

Run out-of-sample validation for a strategy using a validation plan YAML.

## Synopsis

```bash
qs-trader validate PLAN_PATH [OPTIONS]
```

## Positional Argument

### `PLAN_PATH`

Path to a validation plan YAML file, **or** a directory that contains a YAML file named `<directory_name>.yaml`.

Examples:

```bash
# File form
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml

# Directory form (resolves to buy_hold_oos_2024/buy_hold_oos_2024.yaml inside)
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024/
```

## Options

### `--silent` / `-s`

Suppress per-bar event display during each child backtest run. Has the same effect as setting `replay_speed: -1` in the base config. Significantly speeds up wall-clock time when the strategy emits verbose bar-level output.

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml --silent
```

### `--log-level LEVEL`

Set the log level. Choices: `DEBUG`, `INFO`, `WARNING`, `ERROR`. Default: `INFO`.

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml --log-level DEBUG
```

### `--html-report` / `--no-html-report`

Generate (or suppress) the HTML validation report at `validations/<validation_id>/report.html`. Default: `--html-report` (enabled).

```bash
# Skip HTML generation for a quick check
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml --no-html-report
```

### `--on-child-failure MODE`

Override the `execution.on_child_failure` value from the plan YAML. Choices: `fail_fast`, `continue`.

- `fail_fast` (plan default): abort immediately when a child run fails; write an `Invalid` evidence pack.
- `continue`: proceed to remaining folds even if one fails; evaluate available metrics.

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml \
  --on-child-failure continue
```

### `--dry-run`

Resolve and print the effective plan as JSON followed by a human-readable Splits summary; do not execute any child backtest and do not write any output files. Exit code is always `0` on success.

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml --dry-run
```

**Walk-forward dry-run output** shows one line per generated split (train + OOS pairs per fold). Splits that fail the `min_fold_bars` threshold are tagged `[INVALID: insufficient_history_for_fold:<n>]`:

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_walkforward_2015_2024.yaml --dry-run
```

Example fold listing:

```
Splits:
  fold=0 role=train 2010-01-01 → 2011-12-31
  fold=0 role=oos   2012-01-01 → 2012-12-31
  fold=1 role=train 2011-01-01 → 2012-12-31
  fold=1 role=oos   2013-01-01 → 2013-12-31
  fold=2 role=train 2012-01-01 → 2013-12-31  [INVALID: insufficient_history_for_fold:2]
  fold=2 role=oos   2014-01-01 → 2014-12-31  [INVALID: insufficient_history_for_fold:2]
```

> **Phase 2A.1 note:** `--dry-run` is fully supported for `walk_forward` plans. Non-dry-run execution of a `walk_forward` plan exits with code `3` (`Invalid`) until Phase 2A.2 runner support is available.

When the plan declares `cost_scenarios`, `--dry-run` also lists the scenario expansion under the splits table:

```
Cost scenarios:
  base    folds=2
  high_friction    folds=2
```

Each line shows the scenario name and the number of folds it expands into (the same fold set is reused per scenario).

### `--force`

Allow overwriting an existing `validations/<validation_id>/` output directory. Without this flag the command exits with an error if the directory already exists.

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml --force
```

## Exit Codes

| Code | Outcome        | When                                                        |
| ---- | -------------- | ----------------------------------------------------------- |
| `0`  | Pass           | All enabled decision rules pass                             |
| `1`  | Fail           | One or more fail rules breached                             |
| `2`  | ReviewRequired | All fail rules pass but a review-required rule was breached |
| `3`  | Invalid        | Configuration error, missing data, or child run failure     |
| `4`  | Exception      | Unhandled runtime exception during execution                |

> **Note:** Click's built-in argument-validation errors (missing or invalid flags) also return exit code `2`. The error message on `stderr` identifies which case applies.

> **Cost scenarios (Phase 2A.2):** When the plan declares `cost_scenarios`, the exit code reflects the **worst** outcome across all scenarios (`Fail > ReviewRequired > Invalid > Pass`). A passing `base` scenario followed by a failing `high` scenario therefore exits `1` (Fail), not `0`, and the top-level `summary.json.reason_codes` includes `cost_scenario_failed:high`. Two carve-outs keep the reason-code stream clean: (1) a plan declaring exactly one scenario named `base` suppresses the redundant `cost_scenario_failed:base` marker — the underlying per-fold reason codes already carry the full story; and (2) under `on_child_failure: fail_fast`, scenarios that never ran (no fold ref emitted) are omitted from both the top-level reason codes and the per-scenario `cost_scenarios` block. The exit code itself is unchanged by these refinements.

## Examples

### Dry run — preview effective plan and splits

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml --dry-run
```

### Silent run — fastest execution, no bar output

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml --silent
```

### Continue on child failure — collect partial results

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml \
  --on-child-failure continue
```

### Force re-run — overwrite existing output directory

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml --force
```

### Debug mode — verbose logs, no HTML

```bash
qs-trader validate experiments/buy_hold/validations/buy_hold_oos_2024.yaml \
  --log-level DEBUG --no-html-report
```

## Output

On a successful run the following are written under `experiments/<experiment>/validations/<validation_id>/`:

```text
plan.yaml
effective_plan.yaml
summary.json
report.html
audit/
  environment.json
  git.json
  plan_sha256.txt
  base_config_sha256.txt
  holdout.json
folds/
  f0__is/      (full child backtest artifact directory)
  f1__oos/     (full child backtest artifact directory)
```

## See Also

- [docs/validation-framework.md](../validation-framework.md) — architecture, plan YAML reference, decision rule catalog
