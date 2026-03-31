"""
QS-Trader - Quantitative Trading Environment

Public API for building and running deterministic backtests.
"""

from importlib.metadata import version

try:
    __version__ = version("qs-trader")
except Exception:
    __version__ = "0.0.0.dev"  # Fallback for development


__all__ = [
    "__version__",
]
