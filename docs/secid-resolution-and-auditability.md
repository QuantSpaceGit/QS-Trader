# Secid-First Instrument Resolution and Auditability

This document describes the secid-first resolution system, identity fields, manifest v2, decision audit trail, and candidate scan mode added to QS-Trader.

## InstrumentResolver API

The `InstrumentResolver` resolves tickers and secids to stable instrument identities using the AlgoSeek secmaster table as the authoritative source.

### Location

`src/qs_trader/services/data/instrument_resolver.py`

### Core Classes

| Class                     | Purpose                                                                                                       |
| ------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `SecmasterAuthorityError` | Raised when a ticker or secid is not found in secmaster                                                       |
| `TickerHistory`           | Historical ticker usage for a secid (ticker, start_date, end_date)                                            |
| `CandidateMapping`        | Candidate secid mapping when ambiguity is detected                                                            |
| `ResolvedInstrument`      | Full resolved instrument identity with secid, display_symbol, ticker_at_date, identity_source, ticker_history |
| `InstrumentResolver`      | Main resolver with caching, batch resolution, and ticker history table support                                |

### Resolution Policies

- **`anchor_first_in_range`** (default): When a ticker maps to multiple secids, selects the one with the earliest first_date in the requested range.
- **`fail_on_ambiguity`**: Raises `SecmasterAuthorityError` when a ticker maps to multiple secids, forcing explicit secid selection.

### Key Methods

```python
resolver = InstrumentResolver(
    clickhouse_client=client,
    database="market",
    cache_ttl_seconds=3600,
    ticker_history_table="as_secmaster_ticker_history",  # optional
)

# Resolve by ticker
resolved = resolver.resolve_by_ticker(
    "META",
    date_range=(date(2020, 1, 1), date(2025, 12, 31)),
    policy="anchor_first_in_range",
)

# Resolve by explicit secid
resolved = resolver.resolve_by_secid(
    3513095,
    date_range=(date(2020, 1, 1), date(2025, 12, 31)),
)

# Batch resolution
results = resolver.resolve_batch(
    ["AAPL", "MSFT", "META"],
    date_range=(date(2023, 1, 1), date(2023, 12, 31)),
)

# Clear cache
resolver.clear_cache()
```

### Ticker History Table Support

When `ticker_history_table` is configured, resolution queries the normalized `as_secmaster_ticker_history` table first. Falls back to secmaster array parsing (semicolon-delimited `tickers` and `tickersstarttoenddate` fields) when the history table is not configured or returns no rows.

## ClickHouse Adapter Secid Path

The ClickHouse adapter (`src/qs_trader/services/data/adapters/builtin/clickhouse.py`) supports secid-based data loading.

### Behavior

- When `instrument.secid` is set, queries use `WHERE secid = {secid}` instead of `WHERE ticker = {symbol}`.
- `get_available_date_range()` queries by secid when available, falls back to ticker.
- `_fetch_bars()` uses secid-based WHERE clause when `instrument.secid` is set.

### Identity Fields on PriceBarEvent

The adapter populates identity fields from the instrument when available:

```python
secid = getattr(self.instrument, "secid", None)
display_symbol = getattr(self.instrument, "display_symbol", None)
ticker_at_date = getattr(self.instrument, "ticker_at_date", None)
identity_source = getattr(self.instrument, "identity_source", None)
```

## Identity Fields on Events

All event types carry optional identity fields for stable audit trails across ticker changes:

| Field             | Type            | Description                               |
| ----------------- | --------------- | ----------------------------------------- |
| `secid`           | `Optional[int]` | Stable security identifier from secmaster |
| `display_symbol`  | `Optional[str]` | Preferred display ticker                  |
| `ticker_at_date`  | `Optional[str]` | Ticker valid on the event date            |
| `identity_source` | `Optional[str]` | How identity was resolved                 |

### Identity Source Values

| Value                  | Meaning                                           |
| ---------------------- | ------------------------------------------------- |
| `explicit_secid`       | Resolved from explicit secid input                |
| `ticker_point_in_time` | Resolved from ticker with date-range anchoring    |
| `legacy_symbol`        | Fallback to original symbol (no secid resolution) |

### Event Types with Identity Fields

- `PriceBarEvent`
- `FeatureBarEvent`
- `RuntimeFeaturesEvent`
- `IndicatorEvent`
- `SignalEvent`
- `OrderEvent`
- `FillEvent`
- `TradeEvent`

All identity fields are optional for backward compatibility. The existing `symbol` field remains unchanged.

## Manifest v2 Schema

The ClickHouse input manifest describes the canonical data a backtest run consumed. Version 2 adds resolved instrument identity metadata.

### Location

`src/qs_trader/services/reporting/manifest.py`

### Schema v2 Fields

| Field                  | Type                                  | Description                               |
| ---------------------- | ------------------------------------- | ----------------------------------------- |
| `schema_version`       | `Literal[2]`                          | Schema version discriminator              |
| `source_kind`          | `Literal["clickhouse"]`               | Data source kind                          |
| `source_name`          | `str`                                 | Source identifier (e.g., "qs-datamaster") |
| `database`             | `str`                                 | ClickHouse database name                  |
| `bars_table`           | `str`                                 | OHLC bars table name                      |
| `symbols`              | `tuple[str, ...]`                     | Original symbol list                      |
| `start_date`           | `date`                                | Backtest start date                       |
| `end_date`             | `date`                                | Backtest end date                         |
| `price_basis`          | `PriceBasis`                          | Raw or adjusted price basis               |
| `resolved_instruments` | `tuple[ResolvedInstrumentEntry, ...]` | Full identity metadata per instrument     |

### ResolvedInstrumentEntry Fields

| Field              | Type                       | Description                  |
| ------------------ | -------------------------- | ---------------------------- |
| `runtime_symbol`   | `str`                      | Symbol used at runtime       |
| `requested_symbol` | `str`                      | Symbol originally requested  |
| `secid`            | `int \| None`              | Resolved security identifier |
| `display_symbol`   | `str \| None`              | Preferred display ticker     |
| `first_date`       | `date \| None`             | First available date         |
| `last_date`        | `date \| None`             | Last available date          |
| `ticker_history`   | `list[TickerHistoryEntry]` | Historical ticker changes    |
| `resolution`       | `dict[str, Any]`           | Resolution metadata          |

### Reading Manifests

```python
from qs_trader.services.reporting.manifest import read_manifest

manifest = read_manifest(json_str)
# Returns ClickHouseInputManifestV1 or ClickHouseInputManifestV2
# V1 reads emit a DeprecationWarning
```

### Migrating v1 to v2

```python
v1_manifest = ClickHouseInputManifestV1.from_json(json_str)
v2_manifest = v1_manifest.migrate_to_v2()
# Resolved instruments created with secid=None (unresolved)
```

## Decision Audit Trail

Strategy candidate decisions are persisted for post-run analysis and research auditability.

### Location

`src/qs_trader/services/strategy/decision_audit.py`

### Candidate ID

Deterministic candidate ID generated via SHA-256 of `(secid, date, strategy_id, parameter_hash)`:

```python
from qs_trader.services.strategy.decision_audit import compute_candidate_id

candidate_id = compute_candidate_id(
    secid=3513095,
    date="2023-06-15",
    strategy_id="momentum_v1",
    parameter_hash="abc123",
)
```

### Decision Columns

| Column              | Type          | Description                      |
| ------------------- | ------------- | -------------------------------- |
| `candidate_id`      | `str`         | Deterministic SHA-256 hex digest |
| `strategy_id`       | `str`         | Strategy identifier              |
| `secid`             | `int \| None` | Security identifier              |
| `symbol`            | `str`         | Symbol at decision time          |
| `date`              | `str`         | Trading date ISO string          |
| `decision_status`   | `str`         | Candidate status                 |
| `final_action`      | `str`         | Action taken                     |
| `reason_code`       | `str`         | Reason for decision              |
| `gates`             | `JSONB`       | Gate evaluation results          |
| `diagnostics`       | `JSONB`       | Diagnostic context               |
| `strategy_version`  | `str`         | Strategy version                 |
| `parameter_hash`    | `str`         | Parameter snapshot hash          |
| `confidence`        | `float`       | Decision confidence              |
| `decision_price`    | `float`       | Price at decision time           |
| `indicator_context` | `JSONB`       | Indicator values                 |
| `metadata`          | `JSONB`       | Additional metadata              |
| `occurred_at`       | `datetime`    | Event timestamp                  |

### Persistence Targets

- **Parquet**: `strategy_decisions.parquet` in the run output directory
- **CSV**: `strategy_decisions.csv` in the run output directory
- **PostgreSQL**: `strategy_decisions` table in the operational store (uses `RESEARCH_POSTGRES_*` env vars)

### Context API

```python
context.track_decision(
    candidate_id=candidate_id,
    decision_status="candidate",
    final_action="enter",
    reason_code="momentum_threshold_met",
    gates={"rsi": 65.2, "volume_ratio": 1.5},
    diagnostics={"score": 0.85},
)
```

## Candidate Scan Mode

Dedicated execution path for evaluating candidate rules across instruments with forward returns and excursion metrics.

### Location

- CLI: `src/qs_trader/cli/commands/scan.py`
- Runner: `src/qs_trader/services/scan/runner.py`

### CLI Usage

```bash
# Scan a few tickers
qs-trader scan-candidates \
    --tickers AAPL --tickers MSFT \
    --start-date 2023-01-01 --end-date 2023-12-31

# Scan with custom output directory
qs-trader scan-candidates \
    --tickers AAPL \
    --start-date 2023-01-01 --end-date 2023-12-31 \
    --output-dir /tmp/scan_results

# Scan with custom horizons
qs-trader scan-candidates \
    --tickers AAPL --tickers MSFT \
    --start-date 2023-01-01 --end-date 2023-12-31 \
    --horizons 5,10,20,60

# Scan with fail-on-ambiguity policy
qs-trader scan-candidates \
    --tickers FB \
    --start-date 2012-01-01 --end-date 2023-12-31 \
    --ticker-policy fail_on_ambiguity
```

### CLI Options

| Option            | Required | Default                 | Description                                                 |
| ----------------- | -------- | ----------------------- | ----------------------------------------------------------- |
| `--tickers`       | Yes      | —                       | Ticker symbols to scan (repeatable)                         |
| `--start-date`    | Yes      | —                       | Start date (YYYY-MM-DD)                                     |
| `--end-date`      | Yes      | —                       | End date (YYYY-MM-DD)                                       |
| `--strategy-id`   | No       | `scan`                  | Strategy identifier for attribution                         |
| `--output-dir`    | No       | `./scan_output`         | Output directory for results                                |
| `--horizons`      | No       | `5,10,20`               | Comma-separated forward return horizons in bars             |
| `--ticker-policy` | No       | `anchor_first_in_range` | Resolution policy for ambiguous tickers                     |
| `--config`        | No       | —                       | Optional config file for data source and rule configuration |

### Scan Output

Results are persisted to `candidate_scan_results.csv` with one row per `(secid, date)`:

| Column               | Description                       |
| -------------------- | --------------------------------- |
| `date`               | Trading date                      |
| `secid`              | Security identifier               |
| `display_symbol`     | Display ticker                    |
| `ticker_at_date`     | Ticker active on this date        |
| `strategy_id`        | Strategy identifier               |
| `candidate_status`   | Candidate evaluation result       |
| `reason_code`        | Reason for decision               |
| `score`              | Candidate score                   |
| `gates_json`         | Gate evaluation results (JSON)    |
| `features_json`      | Feature values (JSON)             |
| `forward_return_5d`  | 5-day forward log return          |
| `forward_return_10d` | 10-day forward log return         |
| `forward_return_20d` | 20-day forward log return         |
| `mfe_20d`            | Max favorable excursion (20 bars) |
| `mae_20d`            | Max adverse excursion (20 bars)   |

### Known Limitations

The scan CLI currently uses a pass-through candidate rule (accepts all with score=0.5) and a stub data loader. Actual wiring to `InstrumentResolver` and ClickHouse data loading is a known limitation to be addressed in a follow-up.

## PostgreSQL run_events Schema Migration

The `run_events` table is created by Alembic migration `004_run_events_audit_export` in `QS-Research/alembic/versions/`. This migration **includes** all 16 secid-first identity columns natively. New environments running `alembic upgrade head` will have these columns automatically.

For **existing** `run_events` tables (created before the secid-first identity fields were added to the migration), the 16 new columns are **not** present and must be added manually:

| Prefix    | Columns                                                                                    |
| --------- | ------------------------------------------------------------------------------------------ |
| `signal_` | `signal_secid`, `signal_display_symbol`, `signal_ticker_at_date`, `signal_identity_source` |
| `order_`  | `order_secid`, `order_display_symbol`, `order_ticker_at_date`, `order_identity_source`     |
| `fill_`   | `fill_secid`, `fill_display_symbol`, `fill_ticker_at_date`, `fill_identity_source`         |
| `trade_`  | `trade_secid`, `trade_display_symbol`, `trade_ticker_at_date`, `trade_identity_source`     |

### Options for Existing Tables

1. **Recreate the table** — drop and let the Alembic migration recreate it on the next `alembic upgrade head` run (loses historical data).
1. **ALTER TABLE** — add the missing columns manually:

```sql
ALTER TABLE run_events
    ADD COLUMN IF NOT EXISTS signal_secid BIGINT,
    ADD COLUMN IF NOT EXISTS signal_display_symbol TEXT,
    ADD COLUMN IF NOT EXISTS signal_ticker_at_date TEXT,
    ADD COLUMN IF NOT EXISTS signal_identity_source TEXT,
    ADD COLUMN IF NOT EXISTS order_secid BIGINT,
    ADD COLUMN IF NOT EXISTS order_display_symbol TEXT,
    ADD COLUMN IF NOT EXISTS order_ticker_at_date TEXT,
    ADD COLUMN IF NOT EXISTS order_identity_source TEXT,
    ADD COLUMN IF NOT EXISTS fill_secid BIGINT,
    ADD COLUMN IF NOT EXISTS fill_display_symbol TEXT,
    ADD COLUMN IF NOT EXISTS fill_ticker_at_date TEXT,
    ADD COLUMN IF NOT EXISTS fill_identity_source TEXT,
    ADD COLUMN IF NOT EXISTS trade_secid BIGINT,
    ADD COLUMN IF NOT EXISTS trade_display_symbol TEXT,
    ADD COLUMN IF NOT EXISTS trade_ticker_at_date TEXT,
    ADD COLUMN IF NOT EXISTS trade_identity_source TEXT;
```

No Alembic migration is provided — the project uses ad-hoc table creation for research tooling. Choose the approach that best fits your data-retention requirements.
