"""Artifact mode validation and enforcement.

Validates artifact_mode configuration and enforces constraints for database_only
execution mode. In database_only mode:
  - Event store must be 'memory' (no persistent event files)
  - File logging should be disabled or use alternate paths
  - Run directory creation is optional (for database-backed runs)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from qs_trader.system.config import SystemConfig


class ArtifactModeError(RuntimeError):
    """Raised when artifact mode configuration is invalid."""


def validate_artifact_mode(system_config: SystemConfig) -> None:
    """Validate artifact_mode config and enforce database_only constraints.

    Raises:
        ArtifactModeError: If database_only mode has incompatible config
    """
    artifact_mode = system_config.output.artifact_policy.mode

    if artifact_mode == "database_only":
        # Database_only mode must use memory event store
        event_store_backend = system_config.output.event_store.backend
        if event_store_backend != "memory":
            raise ArtifactModeError(
                f"artifact_mode 'database_only' requires event_store.backend='memory', "
                f"got '{event_store_backend}'. "
                "Database-only runs cannot persist event store files."
            )

        # File logging should ideally be disabled, but we can allow it as long
        # as it doesn't depend on run directories
        # (This is a soft constraint for now)

    elif artifact_mode == "filesystem":
        # Filesystem mode has no special constraints
        pass

    else:
        raise ArtifactModeError(f"Unknown artifact_mode: '{artifact_mode}'. Expected 'filesystem' or 'database_only'.")


def should_create_run_directory(system_config: SystemConfig) -> bool:
    """Determine if run directory should be created based on artifact mode.

    Returns:
        True if run directory is needed, False for database_only mode
    """
    return system_config.output.artifact_policy.mode == "filesystem"
