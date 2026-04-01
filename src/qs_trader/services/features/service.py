"""Feature service for consuming precomputed features from QS-Datamaster ClickHouse store.

Provides bar-by-bar access to:
- Composite equity features (features_equity_features_daily)
- Low-level equity indicators (features_equity_indicators_daily)
- Market regime labels (features_market_regime_daily)

Design principles:
- Lazy loading: features fetched only when requested by strategy
- LRU cache: per-symbol secid and per-(symbol, date) features are cached
- Graceful degradation: returns None before warmup window; strategy decides
- Regime safety: all queries use SETTINGS join_use_nulls = 1 to prevent
  ClickHouse Enum columns returning default values for unmatched rows

Usage:
    service = FeatureService.from_config(config)
    features = service.get_features("AAPL", "2024-01-15")
    indicators = service.get_indicators("AAPL", "2024-01-15")
    regime = service.get_regime("2024-01-15")
"""

from __future__ import annotations

import math
from functools import lru_cache
from typing import Any, Optional

import structlog

logger = structlog.get_logger(__name__)


class FeatureService:
    """Fetches precomputed features from QS-Datamaster ClickHouse store.

    Args:
        host: ClickHouse host.
        port: ClickHouse HTTP port (default 8123).
        username: ClickHouse username.
        password: ClickHouse password.
        database: Target database name (default "market").
        feature_version: Feature set version to query (default "v1").
        regime_version: Regime version to query (default "v1").
        connect_timeout: Connection timeout in seconds.
        query_timeout: Query execution timeout in seconds.

    Notes:
        - Secid lookups are cached per symbol for the lifetime of the service.
        - Feature rows are cached per (symbol, date) — typically ~90 days per bar.
        - Returns None for any symbol/date with no data (e.g., before warmup window).
    """

    _FEATURE_COLUMNS = (
        "trend_strength",
        "trend_strength_long",
        "momentum_score",
        "momentum_short",
        "momentum_acceleration",
        "breakout_pressure",
        "breakout_pressure_long",
        "volatility_compression",
        "atr_expansion_score",
        "liquidity_score",
        "volume_expansion",
        "dollar_liquidity",
        "relative_strength_spy",
        "relative_strength_long",
        "breakout_quality_score",
        "trend_alignment_score",
        "regime_adj_momentum",
        "opportunity_score",
        "trend_regime",
        "vol_regime",
        "risk_regime",
        "breadth_regime",
        "composite_regime",
    )

    _INDICATOR_COLUMNS = (
        "sma_10",
        "sma_20",
        "sma_50",
        "sma_100",
        "sma_200",
        "dist_sma_10",
        "dist_sma_20",
        "dist_sma_50",
        "dist_sma_200",
        "log_ret_1d",
        "log_ret_1w",
        "log_ret_1m",
        "log_ret_3m",
        "log_ret_6m",
        "log_ret_12m",
        "atr_pct_14",
        "rv_10",
        "rv_20",
        "rv_60",
        "vol_expansion_10_60",
        "adv_20",
        "vol_ratio_20",
        "dv_ratio_20",
        "rs_spy_1m",
        "rs_spy_3m",
        "rs_spy_6m",
        "dist_high_20",
        "dist_high_52w",
    )

    def __init__(
        self,
        host: str,
        port: int = 8123,
        username: str = "default",
        password: str = "",
        database: str = "market",
        feature_version: str = "v1",
        regime_version: str = "v1",
        connect_timeout: int = 10,
        query_timeout: int = 30,
        default_columns: list[str] | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._database = database
        self._feature_version = feature_version
        self._regime_version = regime_version
        self._connect_timeout = connect_timeout
        self._query_timeout = query_timeout
        self._default_columns = default_columns

        # Caches
        self._secid_cache: dict[tuple[str, str | None], Optional[int]] = {}
        self._feature_cache: dict[tuple[str, str], Optional[dict[str, Any]]] = {}
        self._indicator_cache: dict[tuple[str, str], Optional[dict[str, Any]]] = {}
        self._regime_cache: dict[str, Optional[dict[str, str]]] = {}

        # Lazy-initialized ClickHouse client
        self._client: Any = None

        logger.debug(
            "feature_service.initialized",
            host=host,
            port=port,
            database=database,
            feature_version=feature_version,
        )

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "FeatureService":
        """Construct FeatureService from a config dict (e.g., from data_sources.yaml).

        Expected keys in ``config`` (or ``config["clickhouse"]`` subkey):
            host, port (optional), username/user (optional), password,
            database (optional), feature_version (optional), regime_version (optional).

        Missing values fall back to environment variables CLICKHOUSE_HOST,
        CLICKHOUSE_HTTP_PORT, CLICKHOUSE_USER, CLICKHOUSE_PASSWORD, CLICKHOUSE_DATABASE.
        """
        import os

        ch = config.get("clickhouse", config)  # allow flat config for tests
        return cls(
            host=ch.get("host") or os.environ.get("CLICKHOUSE_HOST", "localhost"),
            port=int(ch.get("port", 0) or os.environ.get("CLICKHOUSE_HTTP_PORT", "8123")),
            username=ch.get("username") or ch.get("user") or os.environ.get("CLICKHOUSE_USER", "default"),
            password=ch.get("password") or os.environ.get("CLICKHOUSE_PASSWORD", ""),
            database=ch.get("database") or os.environ.get("CLICKHOUSE_DATABASE", "market"),
            feature_version=config.get("feature_version", "v1"),
            regime_version=config.get("regime_version", "v1"),
            connect_timeout=int(config.get("connect_timeout", 10)),
            query_timeout=int(config.get("query_timeout", 30)),
            default_columns=config.get("columns"),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_features(
        self, symbol: str, date: str, columns: Optional[list[str]] = None
    ) -> Optional[dict[str, Any]]:
        """Fetch composite feature row for a symbol on a given date.

        Args:
            symbol: Ticker symbol (e.g. "AAPL").
            date: ISO date string "YYYY-MM-DD".
            columns: Optional subset of feature columns to return.
                     If None, falls back to ``default_columns`` from FeatureConfig
                     (feature_config.columns in the experiment YAML), then to all
                     _FEATURE_COLUMNS if that is also unset.                     Note: filtering is applied after the SQL query; all columns
                     are fetched from ClickHouse and unwanted ones are dropped
                     in-process before returning.
        Returns:
            Dict mapping column name → value, or None if not available.
            Regime columns (trend_regime, vol_regime, etc.) are strings.
            Numeric columns are float (NaN if infinite/NULL).
        """
        # Apply default_columns from FeatureConfig when caller passes no explicit filter
        effective_columns = columns if columns is not None else self._default_columns

        cache_key = (symbol, date)
        if cache_key in self._feature_cache:
            row = self._feature_cache[cache_key]
            if row is None or effective_columns is None:
                return row
            return {k: v for k, v in row.items() if k in effective_columns}

        secid = self._resolve_secid(symbol, as_of_date=date)
        if secid is None:
            self._feature_cache[cache_key] = None
            return None

        try:
            client = self._get_client()
            query = f"""
                SELECT {", ".join(self._FEATURE_COLUMNS)}
                FROM {self._database}.features_equity_features_daily FINAL
                WHERE secid = {secid}
                  AND date = toDate('{date}')
                  AND feature_version = '{self._feature_version}'
                LIMIT 1
                SETTINGS join_use_nulls = 1
            """
            result = client.query(query)
            if not result.result_rows:
                self._feature_cache[cache_key] = None
                return None

            row_data = dict(zip(self._FEATURE_COLUMNS, result.result_rows[0]))
            # Normalise: convert infinities to NaN so consumers get clean floats
            normalized: dict[str, Any] = {}
            for col, val in row_data.items():
                if isinstance(val, float) and not math.isfinite(val):
                    normalized[col] = float("nan")
                else:
                    normalized[col] = val

            self._feature_cache[cache_key] = normalized

            if effective_columns is not None:
                return {k: v for k, v in normalized.items() if k in effective_columns}
            return normalized

        except Exception as exc:
            logger.warning(
                "feature_service.get_features_failed",
                symbol=symbol,
                date=date,
                error=str(exc),
            )
            self._feature_cache[cache_key] = None
            return None

    def get_indicators(
        self, symbol: str, date: str, columns: Optional[list[str]] = None
    ) -> Optional[dict[str, float]]:
        """Fetch raw indicator row for a symbol on a given date.

        Args:
            symbol: Ticker symbol.
            date: ISO date string "YYYY-MM-DD".
            columns: Optional subset of indicator columns.
                     Defaults to all _INDICATOR_COLUMNS.

        Returns:
            Dict of indicator name → float, or None if not available.
        """
        cache_key = (symbol, date)
        if cache_key in self._indicator_cache:
            row = self._indicator_cache[cache_key]
            if row is None or columns is None:
                return row
            return {k: v for k, v in row.items() if k in columns}

        secid = self._resolve_secid(symbol, as_of_date=date)
        if secid is None:
            self._indicator_cache[cache_key] = None
            return None

        try:
            client = self._get_client()
            query = f"""
                SELECT {", ".join(self._INDICATOR_COLUMNS)}
                FROM {self._database}.features_equity_indicators_daily FINAL
                WHERE secid = {secid}
                  AND date = toDate('{date}')
                LIMIT 1
                SETTINGS join_use_nulls = 1
            """
            result = client.query(query)
            if not result.result_rows:
                self._indicator_cache[cache_key] = None
                return None

            row_data: dict[str, float] = {}
            for col, val in zip(self._INDICATOR_COLUMNS, result.result_rows[0]):
                if isinstance(val, float) and not math.isfinite(val):
                    row_data[col] = float("nan")
                else:
                    row_data[col] = float(val) if val is not None else float("nan")

            self._indicator_cache[cache_key] = row_data

            if columns is not None:
                return {k: v for k, v in row_data.items() if k in columns}
            return row_data

        except Exception as exc:
            logger.warning(
                "feature_service.get_indicators_failed",
                symbol=symbol,
                date=date,
                error=str(exc),
            )
            self._indicator_cache[cache_key] = None
            return None

    def get_regime(self, date: str) -> Optional[dict[str, str]]:
        """Fetch market regime labels for a date.

        Args:
            date: ISO date string "YYYY-MM-DD".

        Returns:
            Dict with keys: trend_regime, vol_regime, risk_regime,
            breadth_regime, composite_regime — all string values.
            Returns None if not available.

        Notes:
            Uses SETTINGS join_use_nulls = 1 to prevent Enum8 columns from
            returning default label ('bull', 'low', ...) when no row matches.
        """
        if date in self._regime_cache:
            return self._regime_cache[date]

        try:
            client = self._get_client()
            query = f"""
                SELECT
                    trend_regime,
                    vol_regime,
                    risk_regime,
                    breadth_regime,
                    composite_regime
                FROM {self._database}.features_market_regime_daily FINAL
                WHERE date = toDate('{date}')
                  AND regime_version = '{self._regime_version}'
                LIMIT 1
                SETTINGS join_use_nulls = 1
            """
            result = client.query(query)
            if not result.result_rows:
                self._regime_cache[date] = None
                return None

            cols = ("trend_regime", "vol_regime", "risk_regime", "breadth_regime", "composite_regime")
            regime = dict(zip(cols, result.result_rows[0]))
            # Convert any non-string values (e.g. Enum row may come as int) to str
            regime_str: dict[str, str] = {k: str(v) if v is not None else "" for k, v in regime.items()}
            self._regime_cache[date] = regime_str
            return regime_str

        except Exception as exc:
            logger.warning(
                "feature_service.get_regime_failed",
                date=date,
                error=str(exc),
            )
            self._regime_cache[date] = None
            return None

    def close(self) -> None:
        """Close the underlying ClickHouse client connection."""
        if self._client is not None:
            try:
                self._client.close()
            except Exception:
                pass
            self._client = None

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_client(self) -> Any:
        """Return (or create) a ClickHouse HTTP client."""
        if self._client is None:
            try:
                import clickhouse_connect  # type: ignore[import-untyped]
            except ImportError as exc:
                raise ImportError(
                    "clickhouse-connect is required for FeatureService. "
                    "Install it with: pip install clickhouse-connect>=0.7"
                ) from exc

            self._client = clickhouse_connect.get_client(
                host=self._host,
                port=self._port,
                username=self._username,
                password=self._password,
                connect_timeout=self._connect_timeout,
                query_retries=1,
                settings={"max_execution_time": self._query_timeout},
            )
            logger.debug(
                "feature_service.client_connected",
                host=self._host,
                port=self._port,
            )
        return self._client

    def _resolve_secid(self, symbol: str, as_of_date: Optional[str] = None) -> Optional[int]:
        """Resolve ticker to secid via the as_us_equity_ohlc_daily table.

        Uses the `ticker` column index for fast point lookup.
        The most-recent secid on or before ``as_of_date`` is returned, so that
        a recycled ticker symbol always maps to the correct issuer for a given
        backtest date (matches the date-range filter used by OHLC queries).
        Result is cached by ``(symbol, as_of_date)`` so that a ticker that is
        reused by a different issuer on a later date resolves to the correct
        secid for each requested date independently.

        Args:
            symbol: Ticker symbol (e.g. "AAPL").
            as_of_date: ISO date string "YYYY-MM-DD". When supplied the SQL
                includes ``AND tradedate <= toDate('{as_of_date}')`` so we
                identify the issuer that held this ticker on that date.

        Returns:
            secid integer, or None if not found.
        """
        cache_key = (symbol, as_of_date)
        if cache_key in self._secid_cache:
            return self._secid_cache[cache_key]

        date_filter = f"AND tradedate <= toDate('{as_of_date}')" if as_of_date else ""
        try:
            client = self._get_client()
            query = f"""
                SELECT secid
                FROM {self._database}.as_us_equity_ohlc_daily
                WHERE ticker = '{symbol}'
                  {date_filter}
                ORDER BY tradedate DESC
                LIMIT 1
            """
            result = client.query(query)
            if not result.result_rows:
                logger.warning(
                    "feature_service.secid_not_found",
                    symbol=symbol,
                )
                self._secid_cache[cache_key] = None
                return None

            secid = int(result.result_rows[0][0])
            self._secid_cache[cache_key] = secid
            logger.debug("feature_service.secid_resolved", symbol=symbol, secid=secid)
            return secid

        except Exception as exc:
            logger.warning(
                "feature_service.secid_resolution_failed",
                symbol=symbol,
                error=str(exc),
            )
            self._secid_cache[cache_key] = None
            return None
