# DuckDB / ClickHouse Boundary Plan

## Goal

Refactor `QS-Trader` so DuckDB remains the store for **run-produced artifacts**, while ClickHouse remains the canonical source for **market and feature inputs** on canonical ClickHouse-backed runs.

The target boundary is:

- **DuckDB stores what the run produced**
  - `runs`
  - `equity_curve`
  - `returns`
  - `trades`
  - `drawdowns`
- **ClickHouse stores what the run consumed**
  - canonical market bars
  - canonical precomputed features
  - canonical regime context

This avoids duplicate storage during parameter sweeps without rolling back DuckDB persistence for the run-owned outputs that the API and dashboard plan actually need.

## Why this change

The current storage boundary is too blurry for canonical ClickHouse-backed runs:

- nothing consumes `bars_with_features` today
- the current Datamaster backtest API contract ignores it
- repeated runs with different strategy parameters duplicate the same market/features payload
- ClickHouse is already the canonical source for those inputs

The cleaner model is:

- keep DuckDB for **run-local outputs**
- replace default per-run `bars_with_features` persistence with a lightweight **ClickHouse input manifest**
- keep an explicit temporary `snapshot` escape hatch during migration

## Guiding decisions

1. Do **not** roll back DuckDB persistence overall.
1. Scope the first producer change to **canonical ClickHouse market-data runs only**.
1. Store the first manifest cut as a **nullable JSON string on the `runs` row** instead of introducing a new DuckDB table.
1. Change the default behavior to **reference**, but keep `snapshot` available temporarily as an opt-in fallback.
1. Keep historical DuckDB files untouched; no migration or backfill of old runs is part of this plan.

## Target architecture

### Producer side (`QS-Trader`)

- Continue writing run summaries and time series to DuckDB.
- For canonical ClickHouse-backed runs, persist a lightweight manifest describing:
  - source kind
  - canonical source names / table names
  - ClickHouse database
  - symbol universe
  - run date range
  - adjustment modes
  - feature / regime versions
  - optional feature column subset
- Stop defaulting to per-run `bars_with_features` duplication for those runs.

### Consumer side (`QS-Datamaster`, later follow-up)

- Keep the current `/backtest` contract for run outputs unchanged.
- Add a future-facing consumer path that uses the stored manifest to resolve canonical market/features from ClickHouse when needed.

## Phase overview

| Phase | Name                                                  | Max files | Outcome                                                |
| ----- | ----------------------------------------------------- | --------: | ------------------------------------------------------ |
| 1     | Manifest contract and DuckDB schema foundation        |         4 | Manifest contract exists and persists safely in DuckDB |
| 2     | Capture manifest for canonical ClickHouse-backed runs |         4 | ClickHouse-backed runs emit correct manifests          |
| 3     | Flip default away from `bars_with_features`           |         6 | Canonical ClickHouse runs default to `reference`       |
| 4     | Downstream manifest consumer in Datamaster            |         5 | Consumer can resolve canonical inputs from ClickHouse  |
| 5     | Remove deprecated snapshot path                       |         4 | `bars_with_features` path is retired after migration   |

## Implementation rules

- Each PR should be **mergeable on its own** and keep the repo in a testable state.
- Stay within the listed file budget for the PR unless a small adjacent test/doc fix is strictly necessary.
- Land **schema additions before behavior changes**.
- Do not mutate historical DuckDB files in place.
- If a PR changes config or default behavior, it must also update tests and user-facing docs in the same phase.
- Keep the temporary `snapshot` path available until the Datamaster consumer path is proven.

______________________________________________________________________

## Phase 1 — Manifest contract and DuckDB schema foundation

**File count:** 4

**Goal:** Introduce the manifest contract and persist it in DuckDB without changing runtime behavior yet.

### Files in scope

| File                                                               | Purpose                                       |
| ------------------------------------------------------------------ | --------------------------------------------- |
| `src/qs_trader/services/reporting/manifest.py`                     | New manifest model/helper module              |
| `src/qs_trader/services/reporting/db_writer.py`                    | Add nullable manifest field to `runs`         |
| `tests/unit/services/reports/test_db_writer.py`                    | Schema and persistence coverage               |
| `tests/unit/services/reports/test_write_outputs_db_integration.py` | Integration coverage for manifest persistence |

### PR breakdown

#### PR 1.1 — Add manifest model and writer schema hook

**Files (2):**

- `src/qs_trader/services/reporting/manifest.py`
- `src/qs_trader/services/reporting/db_writer.py`

**Checklist**

- [x] Add a lightweight manifest model/helper with JSON serialization and deserialization helpers.
- [x] Add a nullable `input_manifest_json` field to the DuckDB `runs` schema.
- [x] Extend `DuckDBWriter.save_run()` with an optional manifest argument that defaults to `None`.
- [x] Keep `bars_with_features` DDL and write logic unchanged in this PR.
- [x] Do not change engine or reporting call sites yet.

#### PR 1.2 — Add manifest persistence coverage

**Files (2):**

- `tests/unit/services/reports/test_db_writer.py`
- `tests/unit/services/reports/test_write_outputs_db_integration.py`

**Checklist**

- [x] Assert the `runs` table schema includes the new manifest field.
- [x] Assert `save_run()` writes `NULL` when no manifest is supplied.
- [x] Assert valid manifest JSON round-trips through DuckDB.
- [x] Assert rerun/upsert behavior preserves the latest manifest value.
- [x] Keep all existing run-output persistence expectations unchanged.

### Validation

- `DuckDBWriter` creates the new manifest field without breaking existing tables.
- Existing run persistence remains unchanged.
- Manifest JSON round-trips correctly through DuckDB.

### Exit criteria

- The manifest field exists and is covered by tests.
- No behavior change yet for snapshot/reference decisions.

______________________________________________________________________

## Phase 2 — Capture manifest for canonical ClickHouse-backed runs

**File count:** 4

**Goal:** Build and persist the manifest only for canonical ClickHouse-backed runs.

### Files in scope

| File                                                         | Purpose                                                 |
| ------------------------------------------------------------ | ------------------------------------------------------- |
| `src/qs_trader/engine/engine.py`                             | Pass resolved source and feature context into reporting |
| `src/qs_trader/services/reporting/service.py`                | Build manifest during setup/teardown                    |
| `tests/unit/engine/test_engine.py`                           | Engine-level gating coverage                            |
| `tests/unit/services/reports/test_reporting_feature_gate.py` | Reporting-level manifest and gating coverage            |

### PR breakdown

#### PR 2.1 — Plumb manifest context into reporting

**Files (2):**

- `src/qs_trader/engine/engine.py`
- `src/qs_trader/services/reporting/service.py`

**Checklist**

- [x] Detect canonical ClickHouse-backed runs from resolved datasource metadata.
- [x] Capture source name, database, tables, symbol universe, date range, adjustment modes, feature version, regime version, and requested feature columns.
- [x] Build the manifest during setup/teardown without changing the snapshot/reference policy yet.
- [x] Pass the manifest to `DuckDBWriter.save_run()` only for canonical ClickHouse-backed runs.
- [x] Leave Yahoo/CSV runs manifest-free.

#### PR 2.2 — Add gating and content tests

**Files (2):**

- `tests/unit/engine/test_engine.py`
- `tests/unit/services/reports/test_reporting_feature_gate.py`

**Checklist**

- [x] Prove manifest emission for canonical `qs-datamaster` / ClickHouse-backed runs.
- [x] Prove manifest omission for Yahoo/CSV runs.
- [x] Assert manifest content matches the configured source, versions, dates, and feature column subset.
- [x] Assert run persistence still succeeds when manifest is absent.
- [x] Keep config surface unchanged in this phase.

### Validation

- Manifest is written only for canonical ClickHouse-backed runs.
- Non-ClickHouse runs remain unaffected.
- Run persistence still succeeds when the manifest is absent.

### Exit criteria

- Canonical ClickHouse-backed runs persist correct manifest metadata.
- Tests prove the gating behavior.

______________________________________________________________________

## Phase 3 — Flip the default away from `bars_with_features` for canonical ClickHouse runs

**File count:** 6

**Goal:** Make `reference` the default persistence policy for canonical ClickHouse-backed inputs while preserving a temporary `snapshot` escape hatch.

### Files in scope

| File                                                         | Purpose                                             |
| ------------------------------------------------------------ | --------------------------------------------------- |
| `src/qs_trader/system/config.py`                             | Add input persistence policy parsing                |
| `src/qs_trader/scaffold/config/qs_trader.yaml`               | Document new default and temporary escape hatch     |
| `src/qs_trader/services/reporting/service.py`                | Enforce policy-driven manifest vs snapshot behavior |
| `README.md`                                                  | Update storage-boundary guidance                    |
| `tests/unit/system/test_config.py`                           | Config default and override coverage                |
| `tests/unit/services/reports/test_reporting_feature_gate.py` | Policy-driven behavior coverage                     |

### PR breakdown

#### PR 3.1 — Add canonical-input persistence policy config

**Files (2):**

- `src/qs_trader/system/config.py`
- `tests/unit/system/test_config.py`

**Checklist**

- [x] Add a persistence policy under `output.database` for canonical ClickHouse-backed inputs.
- [x] Set the default to `reference`.
- [x] Allow a temporary `snapshot` override.
- [x] Fail fast on invalid policy values.
- [x] Cover defaults and overrides in unit tests.

#### PR 3.2 — Enforce policy in reporting

**Files (2):**

- `src/qs_trader/services/reporting/service.py`
- `tests/unit/services/reports/test_reporting_feature_gate.py`

**Checklist**

- [x] When policy is `reference` and the run is canonical ClickHouse-backed, write the manifest and skip `bars_with_features`.
- [x] When policy is `snapshot`, preserve the existing snapshot behavior.
- [x] Leave non-ClickHouse behavior unchanged.
- [x] Assert run-owned outputs still persist.
- [x] Assert duplicate snapshot writes no longer happen in the default path.

#### PR 3.3 — Update user-facing docs

**Files (2):**

- `src/qs_trader/scaffold/config/qs_trader.yaml`
- `README.md`

**Checklist**

- [x] Document the new storage boundary.
- [x] Document `reference` as the default and `snapshot` as a temporary escape hatch.
- [x] Make clear that DuckDB still stores run outputs.
- [x] Make clear that ClickHouse remains the canonical source for inputs.
- [x] Add a note that historical DuckDB files are untouched.

### Validation

- Canonical ClickHouse-backed runs default to `reference`.
- `snapshot` remains available as an opt-in fallback.
- `runs`, `equity_curve`, `returns`, `trades`, and `drawdowns` persistence stays unchanged.

### Exit criteria

- The new default is live and documented.
- Snapshot duplication no longer happens by default for canonical ClickHouse-backed runs.

______________________________________________________________________

## Phase 4 — Downstream manifest consumer in Datamaster

**File count:** 5

**Goal:** Add a future-facing consumer path that reads the manifest and resolves canonical market/features from ClickHouse without changing the current run-output API contract.

### Files in scope

| File                                                      | Purpose                                        |
| --------------------------------------------------------- | ---------------------------------------------- |
| `../QS-Datamaster/src/datamaster/backtest/reader.py`      | Surface stored manifest from DuckDB            |
| `../QS-Datamaster/src/datamaster/api/routers/backtest.py` | Add manifest-backed input inspection path      |
| `../QS-Datamaster/src/datamaster/api/response_models.py`  | Add manifest/input-inspection response models  |
| `../QS-Datamaster/tests/api/test_backtest_router.py`      | Router regression and manifest-backed coverage |
| `../QS-Datamaster/docs/api_usage.md`                      | Consumer contract documentation                |

### PR breakdown

#### PR 4.1 — Surface manifest in Datamaster reader and models

**Files (2):**

- `../QS-Datamaster/src/datamaster/backtest/reader.py`
- `../QS-Datamaster/src/datamaster/api/response_models.py`

**Checklist**

- [ ] Extend the reader to fetch the manifest field from `runs`.
- [ ] Add response model(s) for manifest-backed input inspection.
- [ ] Keep existing run/equity/returns/trades/drawdowns responses unchanged.
- [ ] Treat missing manifest as valid for old runs.
- [ ] Do not add the ClickHouse query path yet.

#### PR 4.2 — Add router path and tests

**Files (2):**

- `../QS-Datamaster/src/datamaster/api/routers/backtest.py`
- `../QS-Datamaster/tests/api/test_backtest_router.py`

**Checklist**

- [ ] Add a dedicated manifest-backed input inspection route/path.
- [ ] Resolve canonical market/features from ClickHouse using manifest metadata.
- [ ] Degrade cleanly when ClickHouse is unavailable.
- [ ] Preserve the current `/backtest` run-output contract.
- [ ] Cover both old-run and manifest-backed cases in tests.

#### PR 4.3 — Document the consumer contract

**Files (1):**

- `../QS-Datamaster/docs/api_usage.md`

**Checklist**

- [ ] Document the new inspection route and response shape.
- [ ] Document failure and degradation behavior.
- [ ] Document that canonical inputs now come from ClickHouse, not DuckDB snapshots.

### Validation

- Existing backtest output endpoints behave exactly as before.
- Manifest-backed inspection can resolve canonical inputs from ClickHouse.
- Failure modes are explicit and non-destructive.

### Exit criteria

- A downstream consumer path exists.
- Producer and consumer boundaries are now aligned.

______________________________________________________________________

## Phase 5 — Remove deprecated snapshot path after consumer migration

**File count:** 4

**Goal:** Remove the obsolete `bars_with_features` buffering/writer path after the manifest-based consumer path is proven.

### Files in scope

| File                                            | Purpose                                       |
| ----------------------------------------------- | --------------------------------------------- |
| `src/qs_trader/services/reporting/db_writer.py` | Remove deprecated table DDL and helpers       |
| `src/qs_trader/services/reporting/service.py`   | Remove buffering and snapshot write path      |
| `tests/unit/services/reports/test_db_writer.py` | Update schema and writer coverage             |
| `CHANGELOG.md`                                  | Record retirement of deprecated snapshot path |

### PR breakdown

#### PR 5.1 — Remove deprecated runtime and writer path

**Files (2):**

- `src/qs_trader/services/reporting/db_writer.py`
- `src/qs_trader/services/reporting/service.py`

**Checklist**

- [ ] Delete `bars_with_features` buffering and write logic.
- [ ] Stop creating the obsolete table in fresh schemas.
- [ ] Remove deprecated helper methods.
- [ ] Leave manifest persistence and run-output writes intact.
- [ ] Do not mutate historical DuckDB files.

#### PR 5.2 — Finalize tests and release notes

**Files (2):**

- `tests/unit/services/reports/test_db_writer.py`
- `CHANGELOG.md`

**Checklist**

- [ ] Update schema assertions so fresh schemas no longer expect the deprecated table.
- [ ] Preserve manifest and run-output coverage.
- [ ] Add changelog notes for snapshot-path retirement.
- [ ] Document that old files are not migrated in place.
- [ ] Confirm the migration path is complete after the Datamaster consumer lands.

### Validation

- New schemas no longer include deprecated snapshot storage.
- Existing manifest + run-output persistence still works.
- Historical DuckDB files continue to exist untouched.

### Exit criteria

- The deprecated snapshot path is fully retired.
- Canonical ClickHouse-backed runs rely on manifest + canonical source lookup.

______________________________________________________________________

## Relevant files across the whole plan

- `/home/javier/Projects/QS-Trader/src/qs_trader/services/reporting/db_writer.py` — central storage-boundary change; manifest persistence now, deprecated snapshot removal later.
- `/home/javier/Projects/QS-Trader/src/qs_trader/services/reporting/service.py` — runtime decision point for manifest vs snapshot behavior.
- `/home/javier/Projects/QS-Trader/src/qs_trader/engine/engine.py` — resolved datasource + feature context handoff into reporting.
- `/home/javier/Projects/QS-Trader/src/qs_trader/system/config.py` — policy parsing and defaults.
- `/home/javier/Projects/QS-Trader/src/qs_trader/scaffold/config/qs_trader.yaml` — user-facing behavior documentation.
- `/home/javier/Projects/QS-Trader/README.md` — high-level storage-boundary guidance.
- `/home/javier/Projects/QS-Trader/tests/unit/services/reports/test_db_writer.py` — manifest and schema coverage.
- `/home/javier/Projects/QS-Trader/tests/unit/services/reports/test_write_outputs_db_integration.py` — integration coverage for database persistence.
- `/home/javier/Projects/QS-Trader/tests/unit/services/reports/test_reporting_feature_gate.py` — policy/gating coverage.
- `/home/javier/Projects/QS-Trader/tests/unit/engine/test_engine.py` — canonical ClickHouse gating coverage.
- `/home/javier/Projects/QS-Trader/tests/unit/system/test_config.py` — default and override coverage for the new policy.
- `/home/javier/Projects/QS-Datamaster/src/datamaster/backtest/reader.py` — later consumer follow-up.
- `/home/javier/Projects/QS-Datamaster/src/datamaster/api/routers/backtest.py` — later consumer follow-up.
- `/home/javier/Projects/QS-Datamaster/src/datamaster/api/response_models.py` — later consumer follow-up.
- `/home/javier/Projects/QS-Datamaster/tests/api/test_backtest_router.py` — later consumer follow-up.
- `/home/javier/Projects/QS-Datamaster/docs/api_usage.md` — later consumer follow-up.

## Verification checklist

1. Validate Phase 1 with targeted reporting persistence tests covering schema creation, reruns, NULL manifest behavior, and manifest JSON round-trip persistence.
1. Validate Phase 2 with engine/reporting unit tests confirming manifests are emitted only for canonical ClickHouse-backed runs and contain the expected source, version, universe, and date-range metadata.
1. Validate Phase 3 with config parsing tests plus reporting integration tests proving the default policy is `reference`, `snapshot` remains opt-in, and run-owned DuckDB outputs remain unchanged.
1. Validate Phase 4 in `QS-Datamaster` with backtest router tests showing the current API contract stays stable while manifest-backed input inspection resolves market/features from ClickHouse or degrades gracefully when ClickHouse is unavailable.
1. Validate Phase 5 with producer + consumer regression passes and a manual smoke check that repeated parameter sweeps on the same symbol/date range no longer grow DuckDB with duplicate market/features while run outputs continue to accumulate normally.

## Explicit non-goals for the initial producer milestone

- Do not roll back DuckDB persistence for run outputs.
- Do not backfill or mutate historical DuckDB files.
- Do not include Yahoo/CSV runs in the first canonical-reference cut.
- Do not change the current Datamaster backtest output contract in the first producer phase.
- Do not tie dashboard adoption to the producer milestone.

## Open questions

1. Should the first manifest include a stored `symbol -> secid` mapping?

   - Recommendation: defer unless ticker reuse is already a known reproducibility issue in target backtest windows.

1. Should the temporary `snapshot` escape hatch be time-boxed?

   - Recommendation: yes. Keep it only until Datamaster has a stable manifest-backed consumer path.

1. Should the Datamaster consumer work be in the same milestone?

   - Recommendation: no. Land the `QS-Trader` producer contract and default policy first, then pick up the consumer follow-up separately.
