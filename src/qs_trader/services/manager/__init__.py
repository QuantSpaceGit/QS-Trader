"""
Manager Service Package.

Portfolio management service that evaluates trading signals, sizes positions,
checks limits, and emits orders using the risk library.

Phase 3 Architecture:
- Event-driven (no batching, immediate signal processing)
- Uses risk library for pure stateless calculations
- Emits OrderEvent with complete audit trail (intent_id, idempotency_key)

Public API:
- ManagerService: Main service implementation
- IManagerService: Protocol interface for dependency injection
"""

from qs_trader.services.manager.interface import IManagerService
from qs_trader.services.manager.service import ManagerService

__all__ = [
    "ManagerService",
    "IManagerService",
]
