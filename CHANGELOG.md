# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning (pre-release identifiers included).

## [Unreleased]

### Removed

- **`bars_with_features` snapshot path retired (Phase 5 — DuckDB/ClickHouse boundary refactor)**
  - `DuckDBWriter.save_bars_with_features()` and `_insert_bars_with_features()` removed; the `bars_with_features` table is no longer created in fresh DuckDB schemas
  - `ReportingService` no longer accepts or stores a `feature_enabled` flag; bar/feature-bar event subscriptions and the internal `_bar_rows` buffer are gone
  - `output.database.canonical_input_policy` config option removed — all runs now use the `reference` path implicitly; ClickHouse-backed runs rely on `runs.input_manifest_json` and the QS-Datamaster `/inputs` endpoint
  - Pre-Phase-5 databases retain the `bars_with_features` table untouched; upsert-time cleanup of that table is guarded with a `_table_exists()` check so old databases remain readable without migration

### Added

- **ClickHouse Input Manifest (Phase 2)**: Canonical qs-datamaster backtest runs now record full provenance of their ClickHouse inputs alongside the DuckDB run-output summary
  - New `ClickHouseInputManifest` Pydantic model (`services/reporting/manifest.py`) stored as JSON in the nullable `input_manifest_json` column on the `runs` table (Phase 1 schema)
  - Captures source name, bar database, features database, OHLCV table, features/regime tables, symbol universe, date range, adjustment mode, feature-set version, regime version, and requested feature columns
  - Bar database (`database`) and feature/regime database (`features_database`) are stored independently, supporting deployments that use separate ClickHouse databases
  - Yahoo/CSV runs store `NULL`; only canonical `provider: qs-datamaster` runs carry a manifest
  - `BacktestEngine._build_clickhouse_manifest()` detects canonical runs via resolver metadata and fails loudly (not silently) on misconfigured canonical sources
  - `FeatureService` exposes `FEATURES_TABLE` and `REGIME_TABLE` class constants as the authoritative table-name source for the manifest
  - 28 unit tests covering provider gating, field mapping, feature-service integration, DuckDB round-trip, and separate-database scenarios

### Changed

- **Canonical Input Manifest**: ClickHouse-backed runs now record strategy and portfolio adjustment provenance separately
  - New manifests leave the legacy single-field `adjustment_mode` unset and instead persist explicit `strategy_adjustment_mode` / `portfolio_adjustment_mode` values
  - Symbol universes and feature-column selections are stored as immutable tuples in the producer contract for stronger provenance guarantees

### Fixed

- **HTML Report Generation**: `performance.json` is now written whenever the HTML report is enabled, even if standalone JSON output is disabled
  - Prevents HTML report generation from failing when the run is configured to emit HTML-only artifacts

## [0.2.0-beta.7] - 2026-03-31

### Added

- **Backtest Database Persistence**: Backtest runs can now persist run summaries and time-series outputs to DuckDB
  - Add `output.database.enabled` and `output.database.path` configuration options
  - Persist run metrics, equity curve, returns, trades, and drawdowns in a queryable database file
  - Backtest results panel now shows whether database output is enabled and the resolved database path

### Changed

- **Database Path Resolution**: Relative and environment-variable-backed database paths now resolve against the config file that defines them
- **Return Series Handling**: Reporting uses geometric compounding for cumulative returns and stores mathematically undefined log returns as `NULL`

### Fixed

- **DuckDB Output Stability**: Deduplicate equity-curve and returns points by timestamp before DuckDB insert to avoid primary-key conflicts on repeated bar timestamps
- **Package Build Artifacts**: Wheel builds no longer include duplicate scaffold, template, and data files during packaging
- **Developer Tooling**: `make` targets now isolate `uv` from inherited foreign `VIRTUAL_ENV` values for quieter, deterministic local QA and release commands

## [0.2.0-beta.6] - 2026-03-31

### Added

- **DuckDB Database Output**: Optional persistence of backtest results to a DuckDB file
  - New `DuckDBWriter` in `services/reporting/db_writer.py` writes run metrics, equity curves, returns, trades, and drawdowns
  - Enabled via `output.database.enabled: true` in `qs_trader.yaml`
  - Configurable database path via `output.database.path`
  - Upsert semantics: re-running the same run replaces previous data
  - Additive: does not replace existing file-based outputs (JSON/Parquet/HTML)
  - Designed for downstream API consumption (e.g. QS-Datamaster `/backtest` router)
  - 14 unit tests covering schema creation, persistence, upserts, and edge cases

### Changed

- **Package Rename**: Complete rename from `qtrader` to `qs_trader` across entire codebase
  - Renamed `src/qtrader/` → `src/qs_trader/` (Python package directory)
  - Updated all imports, module references, CLI entry points, and patch targets
  - Updated JSON schemas, scaffold templates, and configuration files
  - Updated pyproject.toml package name to `QS-Trader`
  - Updated all documentation, README, LICENSE, and help text
  - All 1950 tests passing

### Fixed

- **Makefile**: Fixed `make help` outputting raw ANSI escape codes (`\033[...`) instead of colors by switching `echo` → `printf '%b\n'`
- **Build Config**: Fixed `[tool.hatchling.build]` → `[tool.hatch.build]` namespace (was silently ignored)

## [0.2.0-beta.5] - 2026-01-08

### Changed

- **Reporting Output**: Refactored timeseries and chart data exports from Parquet/CSV to JSON format
  - Converted `equity_curve.parquet` → `equity_curve.json`
  - Converted `returns.parquet` → `returns.json`
  - Converted `trades.parquet` → `trades.json`
  - Converted `drawdowns.parquet` → `drawdowns.json`
  - Converted `timeline_{strategy}.csv` → `chart_data.json` (generic filename)
  - Renamed `run_manifest.json` → `manifest.json`
  - Browser-compatible JSON with ISO timestamp format for external visualization tools

### Added

- **Event-Triggered Debugging**: Enhanced interactive debugger with breakpoint system
  - New `--break-on EVENT` option for event-triggered pausing (e.g., `signal`, `signal:BUY`)
  - Two debugging modes: step-through (pause at every timestamp) and event-triggered (pause only on matching events)
  - Extensible breakpoint system with `BreakpointRule` ABC supporting signal filters
  - Signal intention aliases: BUY, SELL, SHORT, COVER for common trading actions
  - Portfolio state and strategy indicator display in interactive mode
  - EventBus subscription for real-time signal collection
  - 37 comprehensive tests for breakpoint rules
  - See [docs/cli/interactive.md](docs/cli/interactive.md) for usage guide

- **Interactive Debugging**: Step-through debugging for backtest development
  - `--interactive` / `-i` flag to pause execution at each timestamp
  - `--break-at DATE` option to start debugging from specific date
  - `--inspect LEVEL` option to control detail level (`bars`, `full`, or `strategy`)
  - Rich console UI with unified OHLCV bars and indicators table
  - Interactive commands: `Enter` (step), `c` (continue), `q` (quit), `i` (toggle inspect)

- **EventStore API**: Added `flush()` method to EventStore base class for consistent buffered write handling

### Fixed

- **Manager Service**: Prevent duplicate CLOSE signals from opening erroneous positions
  - Framework-level duplicate detection for full close signals (confidence ≥ 1.0)
  - Partial closes (confidence < 1.0) still allowed to accumulate
  - Prevents second CLOSE from opening opposite position when first hasn't filled yet

- **Portfolio Service**: Fixed RuntimeError during stock split processing by iterating over copy of lots list

## [0.2.0-beta.4] - 2025-12-09

### Fixed

- **Yahoo Data Updater**: Prevent import of incomplete intraday data when market is still open
  - Added post-fetch filtering in `fetch_yahoo_data()` to remove rows with dates after safe end date
  - Added validation in `merge_data()` as second layer of protection against incomplete data
  - yfinance sometimes returns incomplete intraday data despite `end_date` parameter being set
  - Ensures only complete trading day data is imported when market hours are active
  - Fixes issue where partial day data (e.g., Dec 09 with volume of 31,779 vs typical ~40M) was imported during market hours

### Added

- **Release Management**: Added make targets for GitHub release workflow
  - `make version`: Show current version from pyproject.toml
  - `make release-prepare`: Run QA checks and show release preparation checklist
  - `make release VERSION=x.y.z`: Create and push git tag for GitHub releases
  - Manual version bumping workflow for better control over semantic versioning

## [0.2.0-beta.2] - 2025-11-19

### Changed

- Bumped version to 0.2.0-beta.2.

### Documentation

- Updated scaffold `QS_TRADER_README.md` to reflect experiment-centric structure and enhanced CLI/data acquisition guidance.

## [0.2.0-beta.1] - 2025-11-19

### Added

- Initial prerelease tag published.

### Documentation

- Baseline project README and scaffold files.

---

Earlier versions were internal and not formally tracked.
