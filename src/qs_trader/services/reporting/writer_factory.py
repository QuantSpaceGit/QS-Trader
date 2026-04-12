"""Factory for creating persistence writers based on system configuration.

Post-cutover supported runtime persistence is PostgreSQL-only when database
output is enabled.
"""

from __future__ import annotations

import os
import re
from typing import TYPE_CHECKING

from sqlalchemy.engine import URL

if TYPE_CHECKING:
    from qs_trader.services.reporting.persistence import RunPersistenceWriter
    from qs_trader.system.config import DatabaseOutputConfig, SystemConfig


class WriterConfigurationError(RuntimeError):
    """Raised when writer configuration is invalid or incomplete."""


_UNRESOLVED_PLACEHOLDER_PATTERN = re.compile(r"\$\{[^}]+\}")


def create_persistence_writer(
    system_config: SystemConfig,
) -> RunPersistenceWriter:
    """Create a persistence writer based on system configuration.

    Args:
        system_config: System configuration with database backend selection

    Returns:
        Configured PostgreSQL persistence writer

    Raises:
        WriterConfigurationError: If configuration is invalid or incomplete
    """
    db_config = system_config.output.database
    backend = db_config.backend

    if backend == "postgres":
        return _create_postgres_writer(db_config)
    raise WriterConfigurationError(
        f"Unsupported database backend: {backend}. PostgreSQL is the only supported operational persistence backend."
    )


def _create_postgres_writer(db_config: DatabaseOutputConfig) -> RunPersistenceWriter:
    """Create PostgreSQL writer with connection URL from config or environment."""
    from qs_trader.services.reporting.postgres_writer import PostgreSQLWriter

    # Prefer discrete environment variables when available so credentials are
    # URL-encoded safely; fall back to an explicit postgres_url otherwise.
    connection_url = _build_postgres_url_from_env() or db_config.postgres_url

    if not connection_url:
        raise WriterConfigurationError(
            "PostgreSQL backend requires either output.database.postgres_url in config "
            "or RESEARCH_POSTGRES_* environment variables."
        )

    placeholder_match = _UNRESOLVED_PLACEHOLDER_PATTERN.search(connection_url)
    if placeholder_match is not None:
        raise WriterConfigurationError(
            f"PostgreSQL connection URL contains an unresolved environment placeholder: {placeholder_match.group(0)}"
        )

    return PostgreSQLWriter(connection_url)


def _build_postgres_url_from_env() -> str | None:
    """Build PostgreSQL connection URL from environment variables.

    Expected variables:
        RESEARCH_POSTGRES_HOST
        RESEARCH_POSTGRES_PORT
        RESEARCH_POSTGRES_DB
        RESEARCH_POSTGRES_USER
        RESEARCH_POSTGRES_PASSWORD
        RESEARCH_POSTGRES_SSLMODE (optional, defaults to 'disable')

    Returns:
        Connection URL or None if required variables are missing
    """
    host = os.getenv("RESEARCH_POSTGRES_HOST")
    port = os.getenv("RESEARCH_POSTGRES_PORT")
    db = os.getenv("RESEARCH_POSTGRES_DB")
    user = os.getenv("RESEARCH_POSTGRES_USER")
    password = os.getenv("RESEARCH_POSTGRES_PASSWORD")
    sslmode = os.getenv("RESEARCH_POSTGRES_SSLMODE", "disable")

    if not all([host, port, db, user, password]):
        return None

    assert host is not None
    assert port is not None
    assert db is not None
    assert user is not None
    assert password is not None

    return URL.create(
        drivername="postgresql+psycopg",
        username=user,
        password=password,
        host=host,
        port=int(port),
        database=db,
        query={"sslmode": sslmode},
    ).render_as_string(hide_password=False)
