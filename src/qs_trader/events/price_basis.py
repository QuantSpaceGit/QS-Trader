"""Price-basis domain types for strategy-facing APIs and reporting contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum


class PriceBasis(str, Enum):
    """Run-level price basis contract for bars, manifests, and future context APIs."""

    RAW = "raw"
    ADJUSTED = "adjusted"

    @classmethod
    def coerce(cls, value: str | "PriceBasis") -> "PriceBasis":
        """Coerce user/config input onto the supported price-basis enum."""
        if isinstance(value, cls):
            return value

        normalized = str(value).strip().lower()
        try:
            return cls(normalized)
        except ValueError as exc:
            raise ValueError(f"Unsupported price basis: {value!r}. Use 'raw' or 'adjusted'.") from exc

    def __str__(self) -> str:
        """Return the wire-safe enum value."""
        return self.value


@dataclass(frozen=True, slots=True)
class BarView:
    """Immutable price-bar view resolved to a concrete basis for strategy consumption."""

    symbol: str
    timestamp: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
    basis: PriceBasis


__all__ = ["BarView", "PriceBasis"]
