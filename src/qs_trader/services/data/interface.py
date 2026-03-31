"""Data service interface definition.

Defines Protocol interfaces for data service and adapter implementations.
These protocols enable dependency injection and make services independently
testable through mocking.
"""

from datetime import date
from typing import List, Optional, Protocol

# Re-export IDataAdapter for backward compatibility
from qs_trader.services.data.adapters.protocol import IDataAdapter
from qs_trader.services.data.models import Instrument

__all__ = ["IDataService", "IDataAdapter"]


class IDataService(Protocol):
    """
    Data service interface for streaming price data.

    Responsibilities:
    - Stream historical data for symbols via EventBus
    - Transform to canonical format (unadjusted only)
    - Publish PriceBarEvent and CorporateActionEvent
    - Provide instrument metadata

    Does NOT:
    - Execute orders
    - Manage portfolio
    - Calculate indicators
    - Make trading decisions

    Examples:
        >>> # Stream single symbol (publishes events)
        >>> service: IDataService = DataService(config, dataset="yahoo-us-equity-1d-csv", event_bus=bus)
        >>> service.stream_bars(
        ...     "AAPL",
        ...     date(2020, 1, 1),
        ...     date(2020, 12, 31)
        ... )
        >>>
        >>> # Stream multiple symbols (time-synchronized)
        >>> service.stream_universe(
        ...     ["AAPL", "MSFT", "GOOGL"],
        ...     date(2020, 1, 1),
        ...     date(2020, 12, 31)
        ... )
    """

    def get_instrument(self, symbol: str) -> Instrument:
        """
        Get instrument metadata.

        Args:
            symbol: Ticker symbol

        Returns:
            Instrument with metadata

        Raises:
            ValueError: If symbol not found
        """
        ...

    def list_available_symbols(
        self,
        data_source: Optional[str] = None,
    ) -> List[str]:
        """
        List all available symbols.

        Args:
            data_source: Filter by data source (None = all)

        Returns:
            List of available symbols
        """
        ...

    def get_corporate_actions(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        publish_events: bool = True,
    ) -> list:
        """
        Get corporate actions for symbol in date range.

        Returns events in chronological order.
        Empty list if data source doesn't provide corp actions.

        Args:
            symbol: Ticker symbol
            start_date: Start date (inclusive)
            end_date: End date (inclusive)
            publish_events: If True and EventBus configured, publishes CorporateActionEvent

        Returns:
            List of CorporateActionEvent
        """
        ...

    def stream_bars(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        *,
        is_warmup: bool = False,
    ) -> None:
        """
        Load bars and publish PriceBarEvent for each bar (event-driven mode).

        Requires EventBus to be configured during initialization.

        Args:
            symbol: Ticker symbol (e.g., 'AAPL')
            start_date: Start of date range
            end_date: End of date range (inclusive)
            is_warmup: If True, publishes bars with is_warmup=True flag

        Raises:
            ValueError: If EventBus not configured
            ValueError: If symbol not found or invalid date range
        """
        ...

    def stream_universe(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date,
        *,
        is_warmup: bool = False,
        strict: bool = False,
    ) -> None:
        """
        Load bars for multiple symbols and publish PriceBarEvent for each bar.

        Synchronizes iterators to publish all bars for a given timestamp together
        before moving to next timestamp.

        Args:
            symbols: List of ticker symbols
            start_date: Start of date range
            end_date: End of date range (inclusive)
            is_warmup: If True, all bars published with is_warmup=True
            strict: If True, raise ValueError if any symbol fails to load

        Raises:
            ValueError: If EventBus not configured
            ValueError: If any symbol not found and strict=True
        """
        ...
