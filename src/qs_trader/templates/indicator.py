"""Custom indicator template."""

import pandas as pd

from qs_trader.libraries.indicators.base import BaseIndicator


class MyIndicator(BaseIndicator):
    """
    Example indicator implementation.

    Rename this class and implement your calculation logic.

    Indicators are used to calculate technical analysis values from OHLCV data.
    They can be tracked by strategies and automatically updated with each bar.
    """

    def __init__(self, period: int = 20):
        """
        Initialize the indicator.

        Args:
            period: Lookback period for calculation
        """
        self.period = period

    def calculate(self, bars: list) -> list[float | None]:
        """
        Calculate indicator values.

        This method receives OHLCV data and should return a pandas Series
        with the calculated indicator values.

        Args:
            data: OHLCV data with columns: open, high, low, close, volume
                  Index is typically DatetimeIndex

        Returns:
            Series with indicator values (same index as input data)

        Example:
            # Convert bars to prices
            prices = [bar.close for bar in bars]

            # Simple moving average
            result = []
            for i in range(len(prices)):
                if i < self.period - 1:
                    result.append(None)  # Warmup period
                else:
                    sma = sum(prices[i - self.period + 1 : i + 1]) / self.period
                    result.append(sma)
            return result
        """
        # Example: Simple moving average using pandas
        prices = pd.Series([bar.close for bar in bars])
        sma = prices.rolling(window=self.period).mean()
        # Convert to list with None for NaN values
        result: list[float | None] = [None if pd.isna(val) else float(val) for val in sma]
        return result
