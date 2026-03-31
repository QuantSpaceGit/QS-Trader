"""Data service for streaming price data.

This module provides the DataService implementation which coordinates
data streaming from vendor adapters and publishes events via EventBus.
"""

from qs_trader.services.data.interface import IDataService
from qs_trader.services.data.service import DataService

__all__ = [
    "IDataService",
    "DataService",
]
