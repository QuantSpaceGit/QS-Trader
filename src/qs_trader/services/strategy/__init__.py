"""
Strategy Service.

Orchestrates multiple external strategy instances, loads strategy files,
and routes events to each strategy.
"""

from qs_trader.services.strategy.interface import IStrategyService
from qs_trader.services.strategy.service import StrategyService

__all__ = [
    "IStrategyService",
    "StrategyService",
]
