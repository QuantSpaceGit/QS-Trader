"""Shared runtime context for canonical lifecycle event emission.

Phase 2 introduces a canonical append-only lifecycle ledger. These helpers carry
run-scoped metadata that every lifecycle event must include while keeping the
legacy event flow available during the migration window.
"""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4

CANONICAL_PRICE_BASIS = "adjusted_ohlc_adj_columns"
NO_PRICE_BASIS = "none"


@dataclass(frozen=True)
class LifecycleRunContext:
    """Run-scoped metadata required by lifecycle events.

    Attributes:
        experiment_id: Stable experiment/backtest identifier.
        run_id: Stable run identifier scoped under ``experiment_id``.
        sleeve_id: Optional stable sleeve identifier for isolated runs.
        decision_basis: Explicit basis for strategy decision prices.
        execution_price_basis: Explicit basis for execution prices.
        reporting_price_basis: Explicit basis for portfolio/reporting valuations.
    """

    experiment_id: str
    run_id: str
    sleeve_id: str | None = None
    decision_basis: str = CANONICAL_PRICE_BASIS
    execution_price_basis: str = CANONICAL_PRICE_BASIS
    reporting_price_basis: str = CANONICAL_PRICE_BASIS

    @classmethod
    def ad_hoc(
        cls,
        experiment_id: str = "adhoc",
        run_id: str | None = None,
        sleeve_id: str | None = None,
    ) -> "LifecycleRunContext":
        """Create a throwaway lifecycle context for tests and ad-hoc runs."""
        return cls(
            experiment_id=experiment_id,
            run_id=run_id or str(uuid4()),
            sleeve_id=sleeve_id,
        )
