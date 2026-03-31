"""QS-Trader services package.

This package contains service implementations following the lego architecture
pattern. Each service is independently testable and communicates via Protocol
interfaces using dependency injection.
"""

from qs_trader.services.data import DataService, IDataService
from qs_trader.services.data.update_service import UpdateService

__all__: list[str] = [
    "DataService",
    "IDataService",
    "UpdateService",
]
