"""Decision audit persistence for strategy candidate decisions.

Persists strategy decision events to parquet, CSV, or the PostgreSQL
operational store, enabling post-run analysis and research auditability.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()

_DECISION_COLUMNS = [
    "candidate_id",
    "strategy_id",
    "secid",
    "symbol",
    "date",
    "decision_status",
    "final_action",
    "reason_code",
    "gates",
    "diagnostics",
    "strategy_version",
    "parameter_hash",
    "confidence",
    "decision_price",
    "indicator_context",
    "metadata",
    "occurred_at",
]


def compute_candidate_id(
    secid: int | None,
    date: str,
    strategy_id: str,
    parameter_hash: str | None = None,
) -> str:
    """Deterministic candidate ID via SHA-256 of identity tuple.

    Args:
        secid: Stable security identifier (may be None for unresolved).
        date: Trading date ISO string "YYYY-MM-DD".
        strategy_id: Strategy identifier.
        parameter_hash: Optional parameter snapshot hash.

    Returns:
        Hex digest string suitable as a candidate key.
    """
    raw = json.dumps(
        {
            "secid": secid,
            "date": date,
            "strategy_id": strategy_id,
            "parameter_hash": parameter_hash,
        },
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def persist_decisions(
    decisions: list[dict[str, Any]],
    output_dir: Path,
    filename: str = "strategy_decisions.parquet",
) -> Path | None:
    """Persist a list of decision records to a parquet file.

    Args:
        decisions: List of decision dicts (from track_decision output).
        output_dir: Audit output directory.
        filename: Output parquet filename.

    Returns:
        Path to the written parquet file, or None if no decisions to write.
    """
    if not decisions:
        logger.debug("no_decisions_to_persist")
        return None

    import pandas as pd

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename

    df = pd.DataFrame(decisions, columns=_DECISION_COLUMNS)
    if "secid" in df.columns:
        df["secid"] = df["secid"].astype("Int64")
    if "confidence" in df.columns:
        df["confidence"] = pd.to_numeric(df["confidence"], errors="coerce")
    if "decision_price" in df.columns:
        df["decision_price"] = pd.to_numeric(df["decision_price"], errors="coerce")

    df.to_parquet(path, index=False, engine="pyarrow")
    logger.info("decisions_persisted", path=str(path), count=len(decisions))
    return path


def persist_decisions_csv(
    decisions: list[dict[str, Any]],
    output_dir: Path,
    filename: str = "strategy_decisions.csv",
) -> Path | None:
    """Persist a list of decision records to a CSV file.

    Args:
        decisions: List of decision dicts (from track_decision output).
        output_dir: Audit output directory.
        filename: Output CSV filename.

    Returns:
        Path to the written CSV file, or None if no decisions to write.
    """
    if not decisions:
        logger.debug("no_decisions_to_persist_csv")
        return None

    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_DECISION_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        for row in decisions:
            csv_row = {}
            for col in _DECISION_COLUMNS:
                value = row.get(col)
                if isinstance(value, (dict, list)):
                    csv_row[col] = json.dumps(value, default=str)
                elif value is None:
                    csv_row[col] = ""
                else:
                    csv_row[col] = str(value)
            writer.writerow(csv_row)

    logger.info("decisions_persisted_csv", path=str(path), count=len(decisions))
    return path


def load_decisions(path: Path) -> Any:
    """Load decision records from a parquet or CSV file.

    Args:
        path: Path to the strategy_decisions file (.parquet or .csv).

    Returns:
        DataFrame with decision records.
    """
    import pandas as pd

    if path.suffix == ".csv":
        return pd.read_csv(path)
    return pd.read_parquet(path, engine="pyarrow")


# ---------------------------------------------------------------------------
# PostgreSQL operational store persistence (Task 8.6)
# ---------------------------------------------------------------------------

_CREATE_DECISIONS_TABLE = """
CREATE TABLE IF NOT EXISTS strategy_decisions (
    candidate_id     TEXT NOT NULL,
    strategy_id      TEXT NOT NULL,
    secid            BIGINT,
    symbol           TEXT,
    date             TEXT,
    decision_status  TEXT,
    final_action     TEXT,
    reason_code      TEXT,
    gates            JSONB,
    diagnostics      JSONB,
    strategy_version TEXT,
    parameter_hash   TEXT,
    confidence       DOUBLE PRECISION,
    decision_price   DOUBLE PRECISION,
    indicator_context JSONB,
    metadata         JSONB,
    occurred_at      TIMESTAMPTZ,
    inserted_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (candidate_id, strategy_id)
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


def persist_decisions_db(
    decisions: list[dict[str, Any]],
    connection_url: str | None = None,
) -> int:
    """Persist decision records to the PostgreSQL operational store.

    Uses RESEARCH_POSTGRES_* environment variables from QS-Infra/.env
    when no explicit connection_url is provided.

    Args:
        decisions: List of decision dicts (from track_decision output).
        connection_url: Optional PostgreSQL URL. Falls back to env vars.

    Returns:
        Number of rows inserted.
    """
    if not decisions:
        logger.debug("no_decisions_to_persist_db")
        return 0

    url = connection_url or _build_postgres_url()
    if url is None:
        logger.warning(
            "postgres_url_not_available",
            hint="Set RESEARCH_POSTGRES_* env vars or pass connection_url",
        )
        return 0

    try:
        from sqlalchemy import create_engine, text
    except ImportError:
        logger.error("sqlalchemy_not_available")
        return 0

    engine = create_engine(url, pool_pre_ping=True)

    rows = []
    for decision in decisions:
        row = {}
        for col in _DECISION_COLUMNS:
            value = decision.get(col)
            if col in ("gates", "diagnostics", "indicator_context", "metadata"):
                if isinstance(value, (dict, list)):
                    row[col] = json.dumps(value, default=str)
                elif value is None:
                    row[col] = None
                else:
                    row[col] = str(value)
            elif col in ("confidence", "decision_price"):
                if value is None:
                    row[col] = None
                else:
                    try:
                        row[col] = float(value)
                    except (ValueError, TypeError):
                        row[col] = None
            elif col == "secid":
                if value is None:
                    row[col] = None
                else:
                    try:
                        row[col] = int(value)
                    except (ValueError, TypeError):
                        row[col] = None
            else:
                row[col] = value
        rows.append(row)

    with engine.begin() as conn:
        conn.execute(text(_CREATE_DECISIONS_TABLE))
        conn.execute(
            text(
                """
                INSERT INTO strategy_decisions (
                    candidate_id, strategy_id, secid, symbol, date,
                    decision_status, final_action, reason_code,
                    gates, diagnostics, strategy_version, parameter_hash,
                    confidence, decision_price, indicator_context, metadata,
                    occurred_at
                ) VALUES (
                    :candidate_id, :strategy_id, :secid, :symbol, :date,
                    :decision_status, :final_action, :reason_code,
                    CAST(:gates AS JSONB), CAST(:diagnostics AS JSONB),
                    :strategy_version, :parameter_hash,
                    :confidence, :decision_price,
                    CAST(:indicator_context AS JSONB), CAST(:metadata AS JSONB),
                    :occurred_at
                )
                ON CONFLICT (candidate_id, strategy_id) DO UPDATE SET
                    secid = EXCLUDED.secid,
                    symbol = EXCLUDED.symbol,
                    date = EXCLUDED.date,
                    decision_status = EXCLUDED.decision_status,
                    final_action = EXCLUDED.final_action,
                    reason_code = EXCLUDED.reason_code,
                    gates = EXCLUDED.gates,
                    diagnostics = EXCLUDED.diagnostics,
                    strategy_version = EXCLUDED.strategy_version,
                    parameter_hash = EXCLUDED.parameter_hash,
                    confidence = EXCLUDED.confidence,
                    decision_price = EXCLUDED.decision_price,
                    indicator_context = EXCLUDED.indicator_context,
                    metadata = EXCLUDED.metadata,
                    occurred_at = EXCLUDED.occurred_at,
                    inserted_at = NOW()
                """
            ),
            rows,
        )

    logger.info("decisions_persisted_db", count=len(rows))
    return len(rows)
