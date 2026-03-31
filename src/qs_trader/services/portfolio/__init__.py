"""Portfolio service for position and cash management.

This module provides portfolio accounting with lot-based position tracking,
realized/unrealized P&L calculation, corporate action processing, and complete
audit trail via ledger.

Key components:
- PortfolioService: Main service implementation
- IPortfolioService: Protocol interface
- Models: Lot, Position, LedgerEntry, PortfolioState, PortfolioConfig
- Ledger: Complete audit trail
- LotTracker: FIFO/LIFO lot accounting

Example:
    >>> from qs_trader.services.portfolio import PortfolioService, PortfolioConfig
    >>> from decimal import Decimal
    >>>
    >>> config = PortfolioConfig(initial_cash=Decimal("100000"))
    >>> portfolio = PortfolioService(config)
    >>>
    >>> # Process fill
    >>> from datetime import datetime
    >>> portfolio.apply_fill(
    ...     fill_id="fill_001",
    ...     timestamp=datetime.now(),
    ...     symbol="AAPL",
    ...     side="buy",
    ...     quantity=Decimal("100"),
    ...     price=Decimal("150.00")
    ... )
    >>>
    >>> # Query state
    >>> print(f"Cash: ${portfolio.get_cash()}")
    >>> print(f"Equity: ${portfolio.get_equity()}")
"""

from qs_trader.services.portfolio.interface import IPortfolioService
from qs_trader.services.portfolio.models import (
    Ledger,
    LedgerEntry,
    LedgerEntryType,
    Lot,
    LotSide,
    PortfolioConfig,
    PortfolioState,
    Position,
)
from qs_trader.services.portfolio.service import PortfolioService

__all__ = [
    # Service
    "IPortfolioService",
    "PortfolioService",
    # Models
    "Lot",
    "LotSide",
    "Position",
    "LedgerEntry",
    "LedgerEntryType",
    "PortfolioState",
    "PortfolioConfig",
    "Ledger",
]
