"""Custom strategy template.

Replace this docstring with your strategy description.
"""

from qs_trader.events.events import PriceBarEvent
from qs_trader.libraries.strategies.base import Strategy, StrategyConfig
from qs_trader.services.strategy.context import Context


class MyStrategyConfig(StrategyConfig):
    """Configuration for MyStrategy."""

    name: str = "my_strategy"
    display_name: str = "My Custom Strategy"
    description: str = "Replace with your strategy description"

    # Add your parameters here
    # Example:
    # lookback_period: int = 20
    # threshold: float = 0.02


class MyStrategy(Strategy[MyStrategyConfig]):
    """
    Example strategy implementation.

    Rename this class and implement your trading logic.

    The strategy receives market data through the `on_bar` method and can:
    - Access current and historical prices
    - Track custom indicators
    - Generate trading signals
    - Access portfolio state
    """

    def __init__(self, config: MyStrategyConfig):
        """
        Initialize the strategy.

        Args:
            config: Strategy configuration
        """
        super().__init__(config)
        # Access config parameters
        # Example: self.lookback = config.lookback_period

    def on_bar(self, event: PriceBarEvent, context: Context) -> None:
        """
        Called on each new bar of data.

        This is where your trading logic goes. The context provides access to:
        - Market data (prices, volumes)
        - Portfolio state (positions, cash)
        - Signal generation methods

        Args:
            context: Strategy context with market data and methods
        """
        # Example: Access current price for first symbol
        # symbol = context.symbols[0]
        # current_price = context.current_price(symbol)

        # Example: Access historical data
        # historical_data = context.data(symbol, lookback=20)

        # Example: Track an indicator
        # from qs_trader.libraries.indicators.buildin.moving_averages import SMA
        # sma = context.track_indicator(SMA(period=20), symbol)

        # Example: Generate a buy signal (direction=1)
        # context.signal(symbol=symbol, direction=1, size=100)

        # Example: Generate a sell signal (direction=-1)
        # context.signal(symbol=symbol, direction=-1, size=100)

        # Example: Close position (direction=0)
        # context.signal(symbol=symbol, direction=0)

        pass
