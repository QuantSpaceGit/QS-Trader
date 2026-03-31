"""
ManagerService Protocol Interface.

Defines the contract for portfolio management services that evaluate
trading signals, size positions, check limits, and emit orders.

Following the ports & adapters pattern used by other services
(ExecutionService, DataService, etc.) for testability and flexibility.
"""

from typing import Any, Protocol

from qs_trader.events.event_bus import EventBus
from qs_trader.events.events import PortfolioStateEvent, SignalEvent
from qs_trader.libraries.risk.models import RiskConfig


class IManagerService(Protocol):
    """
    Protocol interface for ManagerService.

    Defines the contract for portfolio management services that:
    - Subscribe to SignalEvent from strategies
    - Evaluate signals using risk policies
    - Size positions using risk library tools
    - Check limits using risk library tools
    - Emit OrderEvent for approved orders

    This protocol enables:
    - Dependency injection in BacktestEngine
    - Mock implementations for testing
    - Alternative implementations (e.g., live trading manager)
    """

    _config: RiskConfig
    _event_bus: EventBus

    @classmethod
    def from_config(cls, config_dict: dict[str, Any], event_bus: EventBus) -> "IManagerService":
        """
        Factory method to create service from configuration.

        Args:
            config_dict: Configuration from BacktestConfig.risk_policy
                Expected format:
                {
                    "name": "naive",  # Policy name to load from risk library
                    "config": {}      # Optional overrides
                }
            event_bus: Event bus for service communication

        Returns:
            Configured manager service instance

        Example:
            >>> config_dict = {"name": "naive", "config": {}}
            >>> service = ManagerService.from_config(config_dict, event_bus)
        """
        ...

    def on_signal(self, event: SignalEvent) -> None:
        """
        Handle incoming trading signal.

        Immediately processes signal (no batching):
        1. Extract portfolio state from signal metadata
        2. Get sizing configuration for strategy
        3. Calculate position size using risk library
        4. Check limits using risk library
        5. Emit OrderEvent if approved (or log rejection)

        Args:
            event: Trading signal from strategy

        Side Effects:
            - Publishes OrderEvent if signal approved
            - Logs rejection if signal rejected (no rejection event emitted)

        Example:
            >>> signal = SignalEvent(
            ...     signal_id="sig-001",
            ...     timestamp="2020-01-02T16:00:00Z",
            ...     strategy_id="momentum",
            ...     symbol="AAPL",
            ...     intention="OPEN_LONG",
            ...     price=Decimal("150.0"),
            ...     confidence=Decimal("0.75"),
            ...     metadata={"equity": 100000.0}
            ... )
            >>> manager.on_signal(signal)  # Emits OrderEvent
        """
        ...

    def on_portfolio_state(self, event: PortfolioStateEvent) -> None:
        """
        Cache portfolio state for use in risk checks.

        Subscribe to PortfolioStateEvent from PortfolioService.
        This event is published after mark-to-market on each bar.

        Args:
            event: Portfolio state snapshot

        Side Effects:
            - Updates cached equity for use in sizing calculations
            - Updates cached positions for use in limit checks

        Flow:
            Bar → PortfolioService.on_bar() → mark_to_market()
                → PortfolioStateEvent → ManagerService.on_portfolio_state()
                → cache equity/positions for next signal

        Example:
            >>> state = PortfolioStateEvent(
            ...     ts="2020-01-02T16:00:00Z",
            ...     total_equity=Decimal("100000"),
            ...     cash=Decimal("50000"),
            ...     positions_value=Decimal("50000"),
            ...     num_positions=5,
            ...     gross_exposure=Decimal("50000"),
            ...     net_exposure=Decimal("50000"),
            ... )
            >>> manager.on_portfolio_state(state)
        """
        ...
