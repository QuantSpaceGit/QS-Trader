"""
Custom Data Adapters.

This module provides custom data adapters for various data sources.
Adapters follow the IDataAdapter protocol and are automatically discovered
by QS-Trader's data service resolver.

Available Adapters:
    - OHLCVCSVAdapter: Simple CSV adapter for OHLCV data (Date,Open,High,Low,Close,Volume)

Usage:
    Configure adapters in config/data_sources.yaml:

    data_sources:
      - name: "my-custom-csv-1d"
        adapter: "ohlcv_csv"  # Registry name (auto-derived from class name)
        config:
          root_path: "data/sample-csv"
          path_template: "{root_path}/{symbol}.csv"
          timezone: "America/New_York"

    Then use in backtest configs (experiments/*.yaml):

    data:
      sources:
        - name: "my-custom-csv-1d"
          universe: ["AAPL", "MSFT", "GOOGL"]
"""

from qs_trader.scaffold.library.adapters.ohlcv_csv import OHLCVCSVAdapter

__all__ = ["OHLCVCSVAdapter"]
