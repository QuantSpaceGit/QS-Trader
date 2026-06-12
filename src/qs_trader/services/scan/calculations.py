"""Scan-mode calculations: forward returns, MFE, MAE.

All functions accept raw price series and return float values.
Missing data is represented as float("nan") to allow graceful
degradation in downstream consumers.
"""

from __future__ import annotations

import math
from typing import Sequence


def forward_return(
    closes: Sequence[float],
    t: int,
    horizon: int,
) -> float:
    """Compute log forward return: log(close[t+horizon] / close[t]).

    Args:
        closes: Close price series.
        t: Current bar index.
        horizon: Forward horizon in bars.

    Returns:
        Log forward return, or NaN if insufficient data.
    """
    future_idx = t + horizon
    if future_idx >= len(closes) or t >= len(closes):
        return float("nan")

    current = closes[t]
    future = closes[future_idx]

    if current <= 0 or future <= 0:
        return float("nan")

    return math.log(future / current)


def mfe(
    highs: Sequence[float],
    closes: Sequence[float],
    t: int,
    horizon: int,
) -> float:
    """Compute Max Favorable Excursion: max(high[t:t+h]) / close[t] - 1.

    Args:
        highs: High price series.
        closes: Close price series.
        t: Current bar index.
        horizon: Forward horizon in bars.

    Returns:
        MFE as a ratio, or NaN if insufficient data.
    """
    if t >= len(closes) or t >= len(highs):
        return float("nan")

    current_close = closes[t]
    if current_close <= 0:
        return float("nan")

    end_idx = min(t + horizon, len(highs))
    window_highs = highs[t:end_idx]

    if not window_highs:
        return float("nan")

    max_high = max(window_highs)
    return (max_high / current_close) - 1.0


def mae(
    lows: Sequence[float],
    closes: Sequence[float],
    t: int,
    horizon: int,
) -> float:
    """Compute Max Adverse Excursion: min(low[t:t+h]) / close[t] - 1.

    Args:
        lows: Low price series.
        closes: Close price series.
        t: Current bar index.
        horizon: Forward horizon in bars.

    Returns:
        MAE as a ratio, or NaN if insufficient data.
    """
    if t >= len(closes) or t >= len(lows):
        return float("nan")

    current_close = closes[t]
    if current_close <= 0:
        return float("nan")

    end_idx = min(t + horizon, len(lows))
    window_lows = lows[t:end_idx]

    if not window_lows:
        return float("nan")

    min_low = min(window_lows)
    return (min_low / current_close) - 1.0


def compute_scan_metrics(
    closes: Sequence[float],
    highs: Sequence[float],
    lows: Sequence[float],
    t: int,
    horizons: Sequence[int] | None = None,
) -> dict[str, float]:
    """Compute all scan metrics for a single bar index.

    Args:
        closes: Close price series.
        highs: High price series.
        lows: Low price series.
        t: Current bar index.
        horizons: Forward horizons to compute (default: [5, 10, 20]).

    Returns:
        Dict with forward_return_{h}d, mfe_{h}d, mae_{h}d keys.
    """
    if horizons is None:
        horizons = [5, 10, 20]

    metrics: dict[str, float] = {}

    for h in horizons:
        fr = forward_return(closes, t, h)
        metrics[f"forward_return_{h}d"] = fr
        metrics[f"mfe_{h}d"] = mfe(highs, closes, t, h)
        metrics[f"mae_{h}d"] = mae(lows, closes, t, h)

    return metrics
