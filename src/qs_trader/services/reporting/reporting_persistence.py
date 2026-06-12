"""Reporting persistence outputs for secid-first research auditability.

Writes resolved instruments, runtime bar snapshots, and runtime feature
snapshots to the audit output directory or the PostgreSQL operational store.

Outputs:
    - resolved_instruments.json: Full identity metadata from manifest v2
    - runtime_bar_snapshots.csv: Bar data keyed by (secid, date) with identity fields
    - runtime_feature_snapshots.csv: Feature data keyed by (secid, date) with identity fields
    - Compatibility views grouped by ticker symbol
    - PostgreSQL tables: resolved_instruments, runtime_bar_snapshots, runtime_feature_snapshots
"""

from __future__ import annotations

import csv
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Resolved instruments
# ---------------------------------------------------------------------------

_BARRIER_FIELDS = {"secid", "runtime_symbol", "requested_symbol", "display_symbol"}


def write_resolved_instruments(
    resolved_instruments: list[dict[str, Any]],
    output_dir: Path,
    filename: str = "resolved_instruments.json",
) -> Path | None:
    """Write resolved instruments metadata to JSON.

    Args:
        resolved_instruments: List of resolved instrument dicts (from manifest v2).
        output_dir: Audit output directory.
        filename: Output filename.

    Returns:
        Path to the written file, or None if empty.
    """
    if not resolved_instruments:
        logger.debug("no_resolved_instruments_to_write")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename

    with open(path, "w", encoding="utf-8") as f:
        json.dump(resolved_instruments, f, indent=2, default=_json_default)

    logger.info(
        "resolved_instruments_written",
        path=str(path),
        count=len(resolved_instruments),
    )
    return path


def _json_default(obj: Any) -> Any:
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, date):
        return obj.isoformat()
    return str(obj)


# ---------------------------------------------------------------------------
# Runtime bar snapshots
# ---------------------------------------------------------------------------

_BAR_SNAPSHOT_COLUMNS = [
    "secid",
    "date",
    "runtime_symbol",
    "display_symbol",
    "ticker_at_date",
    "identity_source",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "open_adj",
    "high_adj",
    "low_adj",
    "close_adj",
]


def persist_bar_snapshots(
    snapshots: list[dict[str, Any]],
    output_dir: Path,
    filename: str = "runtime_bar_snapshots.csv",
) -> Path | None:
    """Persist runtime bar snapshots to CSV keyed by (secid, date).

    Args:
        snapshots: List of bar snapshot dicts with identity fields.
        output_dir: Audit output directory.
        filename: Output filename.

    Returns:
        Path to the written file, or None if empty.
    """
    if not snapshots:
        logger.debug("no_bar_snapshots_to_persist")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=_BAR_SNAPSHOT_COLUMNS,
            extrasaction="ignore",
        )
        writer.writeheader()
        for row in snapshots:
            csv_row = {}
            for col in _BAR_SNAPSHOT_COLUMNS:
                value = row.get(col)
                if value is None:
                    csv_row[col] = ""
                else:
                    csv_row[col] = str(value)
            writer.writerow(csv_row)

    logger.info("bar_snapshots_persisted", path=str(path), count=len(snapshots))
    return path


# ---------------------------------------------------------------------------
# Runtime feature snapshots
# ---------------------------------------------------------------------------

_FEATURE_SNAPSHOT_BASE_COLUMNS = [
    "secid",
    "date",
    "runtime_symbol",
    "display_symbol",
    "ticker_at_date",
    "identity_source",
    "strategy_id",
]


def persist_feature_snapshots(
    snapshots: list[dict[str, Any]],
    output_dir: Path,
    filename: str = "runtime_feature_snapshots.csv",
) -> Path | None:
    """Persist runtime feature snapshots to CSV keyed by (secid, date).

    Each snapshot carries identity fields plus a flat feature_values JSON
    column to accommodate arbitrary feature schemas.

    Args:
        snapshots: List of feature snapshot dicts with identity fields.
        output_dir: Audit output directory.
        filename: Output filename.

    Returns:
        Path to the written file, or None if empty.
    """
    if not snapshots:
        logger.debug("no_feature_snapshots_to_persist")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename

    columns = _FEATURE_SNAPSHOT_BASE_COLUMNS + ["feature_values"]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in snapshots:
            csv_row = {}
            for col in _FEATURE_SNAPSHOT_BASE_COLUMNS:
                value = row.get(col)
                csv_row[col] = "" if value is None else str(value)
            feature_values = row.get("feature_values", {})
            csv_row["feature_values"] = json.dumps(feature_values, default=_json_default)
            writer.writerow(csv_row)

    logger.info(
        "feature_snapshots_persisted",
        path=str(path),
        count=len(snapshots),
    )
    return path


# ---------------------------------------------------------------------------
# Compatibility views (grouped by ticker symbol)
# ---------------------------------------------------------------------------


def build_ticker_compatibility_view(
    snapshots: list[dict[str, Any]],
    ticker_field: str = "runtime_symbol",
) -> dict[str, list[dict[str, Any]]]:
    """Group snapshots by ticker symbol for backward compatibility.

    Args:
        snapshots: List of snapshot dicts with a ticker field.
        ticker_field: Field name to group by (default: runtime_symbol).

    Returns:
        Dict mapping ticker symbol to list of snapshots.
    """
    view: dict[str, list[dict[str, Any]]] = {}
    for snap in snapshots:
        ticker = snap.get(ticker_field, "unknown")
        view.setdefault(ticker, []).append(snap)
    return view


def write_ticker_compatibility_view(
    snapshots: list[dict[str, Any]],
    output_dir: Path,
    ticker_field: str = "runtime_symbol",
    filename_prefix: str = "ticker_view",
) -> list[Path]:
    """Write per-ticker CSV files for backward compatibility.

    Args:
        snapshots: List of snapshot dicts.
        output_dir: Audit output directory.
        ticker_field: Field name to group by.
        filename_prefix: Prefix for output files.

    Returns:
        List of paths to written files.
    """
    if not snapshots:
        logger.debug("no_snapshots_for_ticker_view")
        return []

    output_dir.mkdir(parents=True, exist_ok=True)
    view = build_ticker_compatibility_view(snapshots, ticker_field)
    paths = []

    for ticker, rows in view.items():
        safe_ticker = ticker.replace("/", "_").replace("\\", "_")
        path = output_dir / f"{filename_prefix}_{safe_ticker}.csv"

        all_columns = set()
        for row in rows:
            all_columns.update(row.keys())
        columns = sorted(all_columns)

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                csv_row = {}
                for col in columns:
                    value = row.get(col)
                    if isinstance(value, (dict, list)):
                        csv_row[col] = json.dumps(value, default=_json_default)
                    elif value is None:
                        csv_row[col] = ""
                    else:
                        csv_row[col] = str(value)
                writer.writerow(csv_row)

        paths.append(path)

    logger.info(
        "ticker_compatibility_view_written",
        file_count=len(paths),
        tickers=list(view.keys()),
    )
    return paths


# ---------------------------------------------------------------------------
# PostgreSQL database writers (Task 9.6)
# ---------------------------------------------------------------------------

_CREATE_RESOLVED_INSTRUMENTS_TABLE = """
CREATE TABLE IF NOT EXISTS resolved_instruments (
    runtime_symbol   TEXT NOT NULL,
    requested_symbol TEXT NOT NULL,
    secid            BIGINT,
    display_symbol   TEXT,
    first_date       TEXT,
    last_date        TEXT,
    ticker_history   JSONB,
    resolution       JSONB,
    inserted_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (secid, runtime_symbol)
)
"""

_CREATE_BAR_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS runtime_bar_snapshots (
    secid            BIGINT,
    date             TEXT NOT NULL,
    runtime_symbol   TEXT NOT NULL,
    display_symbol   TEXT,
    ticker_at_date   TEXT,
    identity_source  TEXT,
    open             DOUBLE PRECISION,
    high             DOUBLE PRECISION,
    low              DOUBLE PRECISION,
    close            DOUBLE PRECISION,
    volume           BIGINT,
    open_adj         DOUBLE PRECISION,
    high_adj         DOUBLE PRECISION,
    low_adj          DOUBLE PRECISION,
    close_adj        DOUBLE PRECISION,
    inserted_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (secid, date, runtime_symbol)
)
"""

_CREATE_FEATURE_SNAPSHOTS_TABLE = """
CREATE TABLE IF NOT EXISTS runtime_feature_snapshots (
    secid            BIGINT,
    date             TEXT NOT NULL,
    runtime_symbol   TEXT NOT NULL,
    display_symbol   TEXT,
    ticker_at_date   TEXT,
    identity_source  TEXT,
    strategy_id      TEXT NOT NULL,
    feature_values   JSONB,
    inserted_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (secid, date, strategy_id, runtime_symbol)
)
"""


def _build_postgres_url() -> str | None:
    """Build PostgreSQL connection URL from RESEARCH_POSTGRES_* env vars."""
    host = os.getenv("RESEARCH_POSTGRES_HOST")
    port = os.getenv("RESEARCH_POSTGRES_PORT", "5432")
    db = os.getenv("RESEARCH_POSTGRES_DB")
    user = os.getenv("RESEARCH_POSTGRES_USER")
    password = os.getenv("RESEARCH_POSTGRES_PASSWORD")
    sslmode = os.getenv("RESEARCH_POSTGRES_SSLMODE", "disable")

    if not all([host, db, user, password]):
        return None

    from urllib.parse import quote_plus

    return (
        f"postgresql+psycopg://{user}:{quote_plus(password)}"
        f"@{host}:{port}/{db}?sslmode={sslmode}"
    )


def _get_engine(connection_url: str | None = None):
    """Return a SQLAlchemy engine from URL or env vars."""
    try:
        from sqlalchemy import create_engine
    except ImportError:
        logger.error("sqlalchemy_not_available")
        return None

    url = connection_url or _build_postgres_url()
    if url is None:
        logger.warning(
            "postgres_url_not_available",
            hint="Set RESEARCH_POSTGRES_* env vars or pass connection_url",
        )
        return None

    return create_engine(url, pool_pre_ping=True)


def write_resolved_instruments_db(
    resolved_instruments: list[dict[str, Any]],
    connection_url: str | None = None,
) -> int:
    """Write resolved instruments metadata to PostgreSQL.

    Args:
        resolved_instruments: List of resolved instrument dicts.
        connection_url: Optional PostgreSQL URL. Falls back to env vars.

    Returns:
        Number of rows inserted.
    """
    if not resolved_instruments:
        logger.debug("no_resolved_instruments_to_write_db")
        return 0

    engine = _get_engine(connection_url)
    if engine is None:
        return 0

    rows = []
    for inst in resolved_instruments:
        row = {}
        for col in ("runtime_symbol", "requested_symbol", "first_date", "last_date"):
            row[col] = inst.get(col)
        row["secid"] = int(inst["secid"]) if inst.get("secid") is not None else None
        row["display_symbol"] = inst.get("display_symbol")
        row["ticker_history"] = json.dumps(inst.get("ticker_history"), default=_json_default) if inst.get("ticker_history") else None
        row["resolution"] = json.dumps(inst.get("resolution"), default=_json_default) if inst.get("resolution") else None
        rows.append(row)

    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(_CREATE_RESOLVED_INSTRUMENTS_TABLE))
        conn.execute(
            text(
                """
                INSERT INTO resolved_instruments (
                    runtime_symbol, requested_symbol, secid, display_symbol,
                    first_date, last_date, ticker_history, resolution
                ) VALUES (
                    :runtime_symbol, :requested_symbol, :secid, :display_symbol,
                    :first_date, :last_date,
                    CAST(:ticker_history AS JSONB), CAST(:resolution AS JSONB)
                )
                ON CONFLICT (secid, runtime_symbol) DO UPDATE SET
                    requested_symbol = EXCLUDED.requested_symbol,
                    display_symbol = EXCLUDED.display_symbol,
                    first_date = EXCLUDED.first_date,
                    last_date = EXCLUDED.last_date,
                    ticker_history = EXCLUDED.ticker_history,
                    resolution = EXCLUDED.resolution,
                    inserted_at = NOW()
                """
            ),
            rows,
        )

    logger.info("resolved_instruments_written_db", count=len(rows))
    return len(rows)


def write_bar_snapshots_db(
    snapshots: list[dict[str, Any]],
    connection_url: str | None = None,
) -> int:
    """Write runtime bar snapshots to PostgreSQL.

    Includes identity fields (secid, display_symbol, ticker_at_date,
    identity_source) for research auditability.

    Args:
        snapshots: List of bar snapshot dicts with identity fields.
        connection_url: Optional PostgreSQL URL. Falls back to env vars.

    Returns:
        Number of rows inserted.
    """
    if not snapshots:
        logger.debug("no_bar_snapshots_to_write_db")
        return 0

    engine = _get_engine(connection_url)
    if engine is None:
        return 0

    columns = [
        "secid", "date", "runtime_symbol", "display_symbol",
        "ticker_at_date", "identity_source",
        "open", "high", "low", "close", "volume",
        "open_adj", "high_adj", "low_adj", "close_adj",
    ]

    numeric_cols = {"secid": int, "volume": int}
    float_cols = {"open", "high", "low", "close", "open_adj", "high_adj", "low_adj", "close_adj"}

    rows = []
    for snap in snapshots:
        row = {}
        for col in columns:
            value = snap.get(col)
            if value is None:
                row[col] = None
            elif col in numeric_cols:
                try:
                    row[col] = numeric_cols[col](value)
                except (ValueError, TypeError):
                    row[col] = None
            elif col in float_cols:
                try:
                    row[col] = float(value)
                except (ValueError, TypeError):
                    row[col] = None
            else:
                row[col] = str(value)
        rows.append(row)

    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(_CREATE_BAR_SNAPSHOTS_TABLE))
        conn.execute(
            text(
                """
                INSERT INTO runtime_bar_snapshots (
                    secid, date, runtime_symbol, display_symbol,
                    ticker_at_date, identity_source,
                    open, high, low, close, volume,
                    open_adj, high_adj, low_adj, close_adj
                ) VALUES (
                    :secid, :date, :runtime_symbol, :display_symbol,
                    :ticker_at_date, :identity_source,
                    :open, :high, :low, :close, :volume,
                    :open_adj, :high_adj, :low_adj, :close_adj
                )
                ON CONFLICT (secid, date, runtime_symbol) DO UPDATE SET
                    display_symbol = EXCLUDED.display_symbol,
                    ticker_at_date = EXCLUDED.ticker_at_date,
                    identity_source = EXCLUDED.identity_source,
                    open = EXCLUDED.open, high = EXCLUDED.high,
                    low = EXCLUDED.low, close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    open_adj = EXCLUDED.open_adj, high_adj = EXCLUDED.high_adj,
                    low_adj = EXCLUDED.low_adj, close_adj = EXCLUDED.close_adj,
                    inserted_at = NOW()
                """
            ),
            rows,
        )

    logger.info("bar_snapshots_written_db", count=len(rows))
    return len(rows)


def write_feature_snapshots_db(
    snapshots: list[dict[str, Any]],
    connection_url: str | None = None,
) -> int:
    """Write runtime feature snapshots to PostgreSQL.

    Includes identity fields (secid, display_symbol, ticker_at_date,
    identity_source) for research auditability.

    Args:
        snapshots: List of feature snapshot dicts with identity fields.
        connection_url: Optional PostgreSQL URL. Falls back to env vars.

    Returns:
        Number of rows inserted.
    """
    if not snapshots:
        logger.debug("no_feature_snapshots_to_write_db")
        return 0

    engine = _get_engine(connection_url)
    if engine is None:
        return 0

    rows = []
    for snap in snapshots:
        row = {}
        row["secid"] = int(snap["secid"]) if snap.get("secid") is not None else None
        row["date"] = snap.get("date")
        row["runtime_symbol"] = snap.get("runtime_symbol")
        row["display_symbol"] = snap.get("display_symbol")
        row["ticker_at_date"] = snap.get("ticker_at_date")
        row["identity_source"] = snap.get("identity_source")
        row["strategy_id"] = snap.get("strategy_id")
        row["feature_values"] = json.dumps(snap.get("feature_values", {}), default=_json_default)
        rows.append(row)

    with engine.begin() as conn:
        from sqlalchemy import text

        conn.execute(text(_CREATE_FEATURE_SNAPSHOTS_TABLE))
        conn.execute(
            text(
                """
                INSERT INTO runtime_feature_snapshots (
                    secid, date, runtime_symbol, display_symbol,
                    ticker_at_date, identity_source, strategy_id, feature_values
                ) VALUES (
                    :secid, :date, :runtime_symbol, :display_symbol,
                    :ticker_at_date, :identity_source, :strategy_id,
                    CAST(:feature_values AS JSONB)
                )
                ON CONFLICT (secid, date, strategy_id, runtime_symbol) DO UPDATE SET
                    display_symbol = EXCLUDED.display_symbol,
                    ticker_at_date = EXCLUDED.ticker_at_date,
                    identity_source = EXCLUDED.identity_source,
                    feature_values = EXCLUDED.feature_values,
                    inserted_at = NOW()
                """
            ),
            rows,
        )

    logger.info("feature_snapshots_written_db", count=len(rows))
    return len(rows)
