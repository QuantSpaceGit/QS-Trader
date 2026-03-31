"""
OHLCV CSV Bar Model.

Simple data model for OHLCV CSV data.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class OHLCVBar:
    """
    OHLCV bar from CSV file.

    Simple CSV format with columns: Date, Open, High, Low, Close, Volume
    No adjustment factors included (split-adjusted prices only).
    """

    symbol: str
    date: datetime
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: int
