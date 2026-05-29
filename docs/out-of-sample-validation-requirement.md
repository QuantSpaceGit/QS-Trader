# QS-Trader Requirement: Institutional Out-of-Sample Validation Framework

## 1. Objective

QS-Trader currently supports running a backtest over a selected dataset and date range. This is useful for research, but it is not sufficient for institutional-grade strategy validation because a single backtest does not confirm whether a strategy generalizes outside the period used for design, calibration, or parameter selection.

The goal is to extend QS-Trader with a formal out-of-sample validation framework that allows users to evaluate whether a strategy remains robust across unseen periods, rolling market regimes, different parameter choices, and realistic execution assumptions.

This functionality should help users move from a simple backtest result to a structured validation process suitable for professional quantitative research.

## 2. Problem Statement

At present, QS-Trader can answer:

> “How did this strategy perform over this date range?”

QS-Trader should also be able to answer:

- “Was this strategy validated properly outside the research period?”
- “Did performance survive out-of-sample?”
- “Was the result stable across different market periods?”
- “Was the strategy overfit to the selected backtest period?”
- “Is this strategy suitable for further review, paper trading, or production?”

The missing functionality is not only the ability to specify more date ranges. The system needs a validation workflow that can orchestrate multiple related backtests, compare in-sample and out-of-sample performance, detect degradation, preserve auditability, and produce a clear validation report.

## 3. Core Design Principle

The existing backtest engine should remain responsible for executing one deterministic backtest run.

The new functionality should sit above the engine as a validation orchestration layer.

The distinction should be:

| Layer                | Responsibility                                                                                                            |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| Backtest Engine      | Execute one strategy over one fixed configuration and date range                                                          |
| Validation Framework | Define validation methodology, generate multiple runs, compare results, and decide whether the strategy passes validation |

This avoids overloading the current engine and keeps the architecture clean.

## 4. User Requirement Summary

QS-Trader should support a new validation workflow where a user can define:

- The full dataset period available for research.
- The in-sample period used for development, calibration, or parameter selection.
- The out-of-sample period used for validation.
- Optional rolling or walk-forward validation periods.
- Optional final holdout period.
- Strategy parameters or experiment variants to evaluate.
- Metrics and thresholds used to determine whether the strategy passes or fails validation.
- Reporting outputs that clearly separate in-sample, out-of-sample, and full-period results.

## 5. Required Validation Modes

### 5.1 Single Backtest Mode

This is the current behavior.

The user runs a strategy over one date range and receives the normal backtest result.

This mode should remain supported.

### 5.2 Static In-Sample / Out-of-Sample Split

The user should be able to divide a historical dataset into:

| Period        | Purpose                                           |
| ------------- | ------------------------------------------------- |
| In-sample     | Strategy design, calibration, parameter selection |
| Out-of-sample | Independent validation period                     |

The report must clearly separate the two periods.

The system should not mix in-sample and out-of-sample metrics into a single performance number without also showing the separation.

### 5.3 Walk-Forward Validation

The system should support walk-forward validation.

This means the strategy is tested across multiple sequential periods, where each test period represents a fresh out-of-sample window.

The system should support at least two styles:

| Type                  | Description                                                                   |
| --------------------- | ----------------------------------------------------------------------------- |
| Anchored walk-forward | Training period expands over time; test period rolls forward                  |
| Rolling walk-forward  | Training period has fixed length; both training and test windows roll forward |

This is important because one out-of-sample period may not be enough to determine whether a strategy is robust.

### 5.4 Final Holdout Validation

The system should support a final holdout period.

This period should represent data that is not used during strategy design or repeated optimization.

The purpose is to provide a final independent check before a strategy is considered validated.

The framework should make it clear when a holdout period has been used and whether the strategy was changed after viewing holdout results.

### 5.5 Optional Advanced Validation

The initial version does not need to implement advanced financial machine learning validation, but the design should leave room for it.

Future validation methods may include:

- Purged cross-validation.
- Embargo periods between training and testing windows.
- Combinatorial purged cross-validation.
- Multiple-testing adjustment.
- Probability of backtest overfitting.
- Deflated Sharpe Ratio.

The architecture should not block these future additions.

## 6. Functional Requirements

### 6.1 Validation Plan Definition

The user should be able to define a validation plan separate from a normal backtest experiment.

The validation plan should include:

- Validation name or identifier.
- Strategy or experiment being validated.
- Full data range.
- Validation mode.
- In-sample period.
- Out-of-sample period.
- Optional holdout period.
- Optional walk-forward settings.
- Required performance metrics.
- Pass/fail thresholds.
- Reporting preferences.

The validation plan should be treated as a first-class research artifact.

### 6.2 Multiple Backtest Execution

The validation framework should be able to generate and run multiple backtests from a single validation plan.

Each child backtest should be traceable back to:

- The validation plan.
- The specific validation split.
- The strategy version.
- The date range.
- The parameters used.
- The data version.
- The execution assumptions.

Each child run should remain deterministic and reproducible.

### 6.3 In-Sample vs Out-of-Sample Separation

The system must report in-sample and out-of-sample results separately.

At minimum, the report should show:

| Metric           | In-Sample   | Out-of-Sample | Difference / Decay |
| ---------------- | ----------- | ------------- | ------------------ |
| Total return     | Required    | Required      | Required           |
| CAGR             | Required    | Required      | Required           |
| Sharpe ratio     | Required    | Required      | Required           |
| Sortino ratio    | Recommended | Recommended   | Recommended        |
| Max drawdown     | Required    | Required      | Required           |
| Volatility       | Required    | Required      | Required           |
| Win rate         | Recommended | Recommended   | Recommended        |
| Number of trades | Required    | Required      | Required           |
| Turnover         | Recommended | Recommended   | Recommended        |
| Net exposure     | Recommended | Recommended   | Recommended        |

The system should highlight cases where out-of-sample performance is materially worse than in-sample performance.

### 6.4 Walk-Forward Reporting

For walk-forward validation, the system should report each fold separately.

The report should show:

- Fold number.
- Training period.
- Testing period.
- Parameters used.
- In-sample performance.
- Out-of-sample performance.
- Out-of-sample drawdown.
- Number of trades.
- Whether the fold passed or failed.
- Aggregated walk-forward result.

The system should also show whether performance is concentrated in only one or two favorable periods.

### 6.5 Parameter Stability

If the strategy uses configurable parameters, the validation report should show whether the selected parameters are stable across validation periods.

The goal is to identify whether the strategy only works for a very narrow parameter combination.

The report should flag cases where:

- Small parameter changes cause large performance changes.
- The best in-sample parameter set performs poorly out-of-sample.
- Different walk-forward folds select highly unstable parameters.
- Performance is driven by parameter overfitting.

### 6.6 Feature and Indicator Safety

QS-Trader should support strategies that consume features, indicators, or external datasets.

The validation framework should help prevent look-ahead bias.

The system should require clear handling of:

- Feature calculation windows.
- Indicator warmup periods.
- Publication or availability lag for external datasets.
- Point-in-time correctness.
- Whether a feature was calculated using only data available at that timestamp.
- Whether any fitted transformation was trained only on in-sample data.

This is especially important for strategies using:

- Rolling indicators.
- Market regime features.
- Volatility features.
- Factor scores.
- Machine learning model predictions.
- External datasets.

### 6.7 Warmup Periods

The validation framework should support warmup periods.

A warmup period allows indicators or features to initialize before the official test period begins.

The report should clearly distinguish between:

| Period      | Purpose                                |
| ----------- | -------------------------------------- |
| Warmup      | Used to initialize indicators/features |
| Test period | Used to calculate official performance |

Warmup returns should not be included in official performance metrics unless explicitly requested.

### 6.8 Embargo / Gap Between Periods

The design should allow an optional gap between training and testing periods.

This is useful when labels, indicators, or features may overlap across time and create leakage.

Even if not implemented in the first version, the validation design should support this concept.

### 6.9 Cost and Slippage Sensitivity

The validation report should include performance under different transaction cost assumptions.

At minimum, it should support comparison across:

- Base cost assumption.
- Higher cost assumption.
- Lower cost assumption.
- Optional stress-cost scenario.

The report should flag strategies that only work under unrealistically low transaction costs.

### 6.10 Benchmark Comparison

The validation output should compare the strategy against one or more benchmarks.

Examples:

- Buy and hold benchmark.
- Equal-weight portfolio.
- Relevant ETF or index benchmark.
- Cash or risk-free baseline.
- Existing production strategy, if applicable.

The benchmark should be evaluated over the same periods as the strategy.

### 6.11 Promotion / Rejection Decision

The validation framework should produce a clear final decision.

Possible outcomes:

| Decision        | Meaning                                                           |
| --------------- | ----------------------------------------------------------------- |
| Pass            | Strategy satisfies all required validation criteria               |
| Fail            | Strategy does not satisfy required validation criteria            |
| Review Required | Strategy has mixed results or requires manual review              |
| Invalid         | Validation could not be completed due to data/configuration issue |

The decision should be based on explicit criteria, not subjective interpretation.

Example reasons for failure:

- Out-of-sample Sharpe is too low.
- Out-of-sample drawdown exceeds threshold.
- In-sample to out-of-sample decay is too high.
- Too few out-of-sample trades.
- Most walk-forward folds are negative.
- Strategy fails under higher transaction costs.
- Performance depends on one specific period.
- Feature leakage risk detected.
- Benchmark outperforms the strategy.

## 7. Expected Outputs

The validation run should produce a complete output package.

### 7.1 Validation Summary

A high-level summary should include:

- Validation plan name.
- Strategy name.
- Strategy version.
- Dataset used.
- Full data range.
- Validation method.
- Number of generated runs.
- Number of successful runs.
- Number of failed runs.
- Final validation decision.
- Main reason for pass/fail.

### 7.2 Metrics Report

The metrics report should include:

- In-sample metrics.
- Out-of-sample metrics.
- Full-period metrics.
- Walk-forward fold metrics, if applicable.
- Benchmark metrics.
- Cost sensitivity metrics.
- Drawdown metrics.
- Trade count and turnover metrics.

### 7.3 Equity Curve Outputs

The system should produce equity curve outputs for:

- In-sample period.
- Out-of-sample period.
- Full strategy period.
- Benchmark.
- Each walk-forward test period, where applicable.

The output should make it visually obvious where the strategy was developed versus where it was validated.

### 7.4 Fold-Level Report

For walk-forward validation, the system should produce a fold-level report showing:

- Fold period.
- Training window.
- Testing window.
- Parameters used.
- Fold performance.
- Fold drawdown.
- Fold benchmark comparison.
- Fold pass/fail status.

### 7.5 Audit and Reproducibility Package

Each validation run should store enough information to reproduce the results.

The package should include:

- Submitted validation plan.
- Effective validation plan after defaults are applied.
- Submitted backtest configuration.
- Effective child backtest configurations.
- Strategy version.
- Code version or commit reference, if available.
- Dataset reference.
- Feature reference, if applicable.
- Execution model assumptions.
- Transaction cost assumptions.
- Timestamp of execution.
- System/environment metadata, where practical.

This is required for institutional credibility.

## 8. Non-Functional Requirements

### 8.1 Reproducibility

Given the same data, configuration, strategy version, and environment, the validation result should be reproducible.

The system should avoid hidden state or implicit assumptions.

### 8.2 Auditability

Every validation result should be traceable.

A reviewer should be able to understand:

- What was tested.
- Over which periods.
- With which parameters.
- Using which data.
- Under which execution assumptions.
- Why the strategy passed or failed.

### 8.3 Extensibility

The validation framework should be designed so future methods can be added without redesigning the engine.

Future extensions may include:

- Financial ML validation.
- Regime-specific validation.
- Stress testing.
- Monte Carlo resampling.
- Portfolio-level validation.
- Live/paper trading comparison.

### 8.4 Backward Compatibility

Existing QS-Trader backtest functionality should continue to work.

Current users should still be able to run a simple backtest without defining a validation plan.

The validation framework should be additive, not disruptive.

### 8.5 Clear Failure Handling

If validation cannot be completed, the system should clearly report why.

Examples:

- Missing data.
- Invalid date ranges.
- Insufficient history for indicator warmup.
- Overlapping in-sample and out-of-sample periods.
- Out-of-sample period too short.
- Strategy generated no trades.
- Benchmark unavailable.
- Feature data not available point-in-time.
- Child backtest run failed.

The system should not silently produce incomplete validation results.

## 9. Suggested User Workflow

The intended user workflow should be:

1. User develops a strategy using normal QS-Trader backtests.
1. User defines a validation plan.
1. QS-Trader generates the required validation runs.
1. QS-Trader executes each run.
1. QS-Trader aggregates results.
1. QS-Trader compares in-sample and out-of-sample performance.
1. QS-Trader applies pass/fail criteria.
1. QS-Trader produces a validation report.
1. User reviews whether the strategy is suitable for further research, paper trading, or production.

## 10. Suggested Development Phases

### Phase 1: Static In-Sample / Out-of-Sample Validation

Required capabilities:

- Define in-sample and out-of-sample periods.
- Run both periods.
- Report metrics separately.
- Compare performance decay.
- Produce a basic validation summary.
- Preserve reproducibility artifacts.

This is the minimum viable institutional validation feature.

### Phase 2: Walk-Forward Validation

Required capabilities:

- Support rolling and anchored walk-forward validation.
- Generate multiple validation folds.
- Run each fold.
- Aggregate fold-level results.
- Report parameter stability.
- Report fold pass/fail status.

This makes the validation framework materially stronger.

### Phase 3: Leakage and Feature Controls

Required capabilities:

- Indicator warmup handling.
- Feature availability checks.
- Point-in-time validation support.
- Optional embargo/gap between train and test periods.
- Clear warnings for potential look-ahead bias.

This is critical for feature-driven and ML-style strategies.

### Phase 4: Institutional Review and Promotion Layer

Required capabilities:

- Explicit promotion criteria.
- Final pass/fail decision.
- Cost sensitivity analysis.
- Benchmark comparison.
- Trial registry.
- Holdout period discipline.
- Advanced overfitting diagnostics.

This phase turns QS-Trader into a more serious institutional research platform.

## 11. Acceptance Criteria

The feature should be considered complete when the following are true:

- A user can define a validation plan separate from a normal backtest.
- The system can run static in-sample and out-of-sample validation.
- The system can report IS and OOS metrics separately.
- The system can calculate and report performance degradation from IS to OOS.
- The system can produce a clear validation summary.
- The system can preserve all configuration and audit artifacts.
- The system can fail gracefully when validation inputs are invalid.
- Existing single-backtest functionality remains unaffected.
- The design allows future walk-forward and advanced validation methods.
- The output is clear enough for a quant researcher, developer, or reviewer to understand whether the strategy passed validation.

## 12. Key Design Questions for the Development Team

The development team should define:

- Where validation plans should live in the project structure.
- Whether validation outputs should reuse the current backtest report structure or have a separate report format.
- How child backtest runs should be named and stored.
- How strategy parameters should be frozen between in-sample and out-of-sample runs.
- How benchmark data should be selected and aligned.
- How feature availability and warmup periods should be represented.
- How to track strategy version, data version, and configuration version.
- Which metrics should be mandatory in the first release.
- Which validation modes should be included in the first implementation.
- How strict the first version should be about holdout contamination and repeated testing.

## 13. Final Requirement

QS-Trader should be extended from a single-run backtesting engine into a validation-aware research platform.

The required capability is not just to run a strategy over multiple date ranges. The required capability is to support a disciplined research workflow where in-sample development, out-of-sample validation, walk-forward robustness, benchmark comparison, cost sensitivity, and auditability are treated as first-class parts of the backtesting process.

The expected outcome is a validation framework that helps users determine whether a strategy is genuinely robust or simply overfit to a selected historical period.

## Phase 1 Implementation

Phase 1 (static IS/OOS validation) is **implemented and committed** in QS-Trader.

### What Phase 1 delivers

- `qs-trader validate <plan>` CLI command with exit codes 0–4 (Pass / Fail / ReviewRequired / Invalid / Exception).
- `ValidationPlan` YAML format: `validation_id`, `strategy_experiment`, `base_config`, `mode: static_is_oos`, `splits` (IS + OOS), optional `holdout`, `decision` rules, `execution`, and `reporting` blocks.
- Declarative pass/fail rule catalog: `oos_sharpe_min`, `oos_max_drawdown_max`, `is_to_oos_sharpe_decay_max`, `min_oos_trades`, `require_positive_oos_total_return`, plus `on_review_required` for downgrade-only rules.
- Self-contained evidence pack per validation run: `summary.json`, `effective_plan.yaml`, `report.html`, and `audit/` (environment, git commit, plan SHA-256, holdout state).
- Child run failure handling: configurable `fail_fast` (default) or `continue` per plan.
- Full backward compatibility: `qs-trader backtest` and all single-run artifacts unchanged.

### What Phase 1 explicitly defers

- Walk-forward validation (anchored or rolling) — Phase 2.
- Cost-sensitivity scenarios — Phase 2.
- Full benchmark overlay charts — Phase 2.
- Holdout enforcement (blocking re-runs, justification gate) — Phase 4.
- Postgres parent/children validation tables — Phase 4.
- Purged CV, CPCV, deflated Sharpe, PBO, embargo execution — Phase 5+.

### Quality gate state at Phase 1 commit

- Tests: **241 passing** (`uv run pytest tests/validation/ -q`)
- Linting: **ruff clean**

## Phase 2A.1 Implementation

Phase 2A.1 (walk-forward split generation — preview only) is **implemented and committed** in QS-Trader. Live execution of walk-forward plans is phase-gated until Phase 2A.2.

### What Phase 2A.1 delivers

- `ValidationPlan.mode` accepts `walk_forward` in addition to `static_is_oos`.
- New `WalkForwardSplitsSpec` schema: `style` (`anchored` | `rolling`), `train`, `test`, `step`, `embargo`, `total_range`, and optional `min_fold_bars`. Duration strings accept `Ny`, `Nmo`, `Nd`; combined units (e.g. `1y2mo`) are rejected; `train`/`test`/`step` must be strictly positive; `embargo` may be zero; `step` must be `>= test`.
- `WalkForwardSplitGenerator` produces alternating `train`/`oos` splits for each fold under both anchored and rolling styles, honoring `embargo` and stopping when the test window would exceed `total_range`.
- `min_fold_bars` enforcement marks short test windows with `status='invalid'` and `reason='insufficient_history_for_fold:<n>'` instead of raising.
- `qs-trader validate <plan> --dry-run` prints generated splits with `[INVALID: <reason>]` tags for invalid folds.
- `qs-trader validate <plan>` (non-dry-run) on a `walk_forward` plan exits with code `3` (`Invalid`) and an explanatory message until Phase 2A.2 runner support lands.
- `ValidationPlan` is now strict: `extra="forbid"` at the root rejects unknown top-level keys (e.g. Phase 2 fields on a `static_is_oos` plan).
- `description` is an accepted optional root field (human-readable label); it is excluded from the plan SHA-256 so existing static plan hashes are preserved, and is omitted from `effective_plan.yaml` when not set.
- `python-dateutil` is now a direct runtime dependency.

### What Phase 2A.1 explicitly defers

- Walk-forward non-dry-run execution and per-fold runner integration — Phase 2A.2.
- Cost-sensitivity scenarios — Phase 2A.2.
- Benchmark overlay (synthetic child run, equity overlay chart) — Phase 2A.3.
- Walk-forward aggregation (median, IQR, `count_pass_folds`) and new decision rules (`min_pass_folds_fraction`, `median_oos_sharpe_min`, `worst_oos_max_drawdown_max`) — Phase 2A.4.
- Walk-forward HTML reporter and equity overlay PNG — Phase 2A.5.
- Reference walk-forward validation plan in QS-Research — Phase 2A.7.

### Quality gate state at Phase 2A.1 commit

- Tests: **347 passing** (`uv run pytest tests/validation/ -q`)
- Linting: **ruff clean**; **ruff format --check** clean; **mypy** clean (15 source files)
- Type checking: **mypy clean**
- Formatting: **mdformat clean** on all documentation files

### Documentation

- Implementation spec: [QS-Infra/docs/qs-trader-oos-validation-framework.md](../../QS-Infra/docs/qs-trader-oos-validation-framework.md)
- Architecture and plan YAML reference: [docs/validation-framework.md](validation-framework.md)
- CLI reference: [docs/cli/validate.md](cli/validate.md)
