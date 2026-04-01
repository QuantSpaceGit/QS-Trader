"""ClickHouse data adapter for QS-Datamaster OHLC data.

Reads per-symbol daily OHLC bars from `market.as_us_equity_ohlc_daily`
in the QS-Datamaster ClickHouse database.

Canonical PriceBarEvent mapping:
  - open/high/low/close → raw split-adjusted prices
  - open_adj/high_adj/low_adj/close_adj → total-return adjusted prices
    (openadj, highadj, lowadj, closeadj from AlgoSeek)
  - volume → dailyvolumeadj (adjusted daily volume, rounded to int)

Design:
  - Implements IDataAdapter protocol for DataSourceResolver auto-discovery
  - Uses clickhouse-connect HTTP client (port 8123)
  - Timestamps use market close 16:00 ET → UTC (same as YahooCSVDataAdapter)
  - Symbol resolution: ticker name matches `ticker` column in OHLC table
  - No corporate action support (AlgoSeek prices are pre-adjusted)
  - Cache: all bars for the full date range are fetched in one query

Example:
    >>> config = {
    ...     "host": "localhost",
    ...     "port": 8123,
    ...     "username": "default",
    ...     "password": "secret",
    ...     "database": "market",
    ... }
    >>> instrument = Instrument(symbol="AAPL")
    >>> adapter = ClickhouseDataAdapter(config, instrument, dataset_name="qs-datamaster-equity-1d")
    >>> for raw_bar in adapter.read_bars("2023-01-03", "2023-01-10"):
    ...     event = adapter.to_price_bar_event(raw_bar)
    ...     print(event.close, event.close_adj)
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Iterator, Optional

import structlog

from qs_trader.events.events import CorporateActionEvent, PriceBarEvent
from qs_trader.services.data.adapters.protocol import IDataAdapter
from qs_trader.system import LoggerFactory
from zoneinfo import ZoneInfo

logger = LoggerFactory.get_logger()


@dataclass(slots=True)
class ClickhouseBar:
    """Internal representation of a single ClickHouse daily OHLC bar."""

    symbol: str
    trade_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    open_adj: Optional[Decimal]
    high_adj: Optional[Decimal]
    low_adj: Optional[Decimal]
    close_adj: Optional[Decimal]
    volume: int


class ClickhouseDataAdapter:
    """QS-Datamaster ClickHouse OHLC adapter implementing IDataAdapter.

    Responsibilities:
      - Batch-fetch OHLC bars from ClickHouse for the full date range.
      - Stream bars as ClickhouseBar objects to DataService.
      - Convert to canonical PriceBarEvent with adjusted OHLC.
      - Provide date range and timestamp extraction.

    Configuration keys (from data_sources.yaml clickhouse section):
      host          ClickHouse hostname (required)
      port          HTTP port (default 8123)
      username/user Username (default "default")
      password      Password (required)
      database      Database name (default "market")

    Configuration keys (top-level in data source entry):
      asset_class   (default "equity")
      price_currency (default "USD")
      price_scale   (default 2)
      timezone      (default "America/New_York")
    """

    def __init__(self, config: dict[str, Any], instrument: Any, dataset_name: Optional[str] = None) -> None:
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name or "qs-datamaster-equity-1d"

        # Connection config lives under the 'clickhouse' subkey in data_sources.yaml
        ch_cfg = config.get("clickhouse", config)  # fall back to top-level for flat configs
        self._host: str = ch_cfg["host"]
        self._port: int = int(ch_cfg.get("port", 8123))
        self._username: str = ch_cfg.get("username", ch_cfg.get("user", "default"))
        self._password: str = ch_cfg.get("password", "")
        self._database: str = ch_cfg.get("database", "market")

        # Display / metadata config (top-level keys from YAML)
        self.tz_name: str = config.get("timezone", "America/New_York")
        self.asset_class: str = config.get("asset_class", "equity")
        self.price_currency: str = config.get("price_currency", "USD")
        self.price_scale: int = int(config.get("price_scale", 2))
        self.quantizer: Decimal = Decimal(10) ** -self.price_scale

        self.tz = ZoneInfo(self.tz_name)

        # Lazy-initialised ClickHouse client
        self._client: Any = None

        # In-memory bar cache: loaded once per read_bars() call
        self._bar_cache: list[ClickhouseBar] = []
        self._cache_range: Optional[tuple[str, str]] = None

        logger.debug(
            "clickhouse_adapter.initialized",
            symbol=instrument.symbol,
            host=self._host,
            port=self._port,
            database=self._database,
            dataset=self.dataset_name,
        )

    # =========================================================
    # IDataAdapter protocol methods
    # =========================================================

    def read_bars(self, start_date: str, end_date: str) -> Iterator[ClickhouseBar]:
        """Fetch bars from ClickHouse and yield as ClickhouseBar objects.

        All bars for the symbol in [start_date, end_date] are fetched in a
        single query and cached.  Subsequent calls with the same range return
        from cache.

        Args:
            start_date: Inclusive ISO date (YYYY-MM-DD).
            end_date:   Inclusive ISO date (YYYY-MM-DD).
        """
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        if start > end:
            raise ValueError("start_date must be <= end_date")

        cache_range = (start_date, end_date)
        if self._cache_range != cache_range:
            self._bar_cache = self._fetch_bars(start_date, end_date)
            self._cache_range = cache_range

        yield from self._bar_cache

    def to_price_bar_event(self, bar: ClickhouseBar) -> PriceBarEvent:
        """Convert ClickhouseBar to canonical PriceBarEvent.

        Timestamp is set to market close 16:00 local Eastern time → UTC.
        All adjusted price fields are populated (AlgoSeek provides full OHLC adj).
        """
        market_close_naive = datetime.combine(bar.trade_date, time(16, 0, 0))
        market_close_local = market_close_naive.replace(tzinfo=self.tz)
        market_close_utc = market_close_local.astimezone(timezone.utc)
        timestamp_local = market_close_local.isoformat()

        return PriceBarEvent(
            symbol=bar.symbol,
            asset_class=self.asset_class,
            interval="1d",
            timestamp=market_close_utc.isoformat(),
            timestamp_local=timestamp_local,
            timezone=self.tz_name,
            open=bar.open,
            high=bar.high,
            low=bar.low,
            close=bar.close,
            open_adj=bar.open_adj,
            high_adj=bar.high_adj,
            low_adj=bar.low_adj,
            close_adj=bar.close_adj,
            volume=bar.volume,
            price_currency=self.price_currency,
            price_scale=self.price_scale,
            source=self.dataset_name,
            source_service="data_service",
        )

    def to_corporate_action_event(
        self, bar: ClickhouseBar, prev_bar: Optional[ClickhouseBar] = None
    ) -> Optional[CorporateActionEvent]:
        """AlgoSeek prices are pre-adjusted; no explicit corp action events.

        Returns:
            Always None — corporate actions are already embedded in adjusted prices.
        """
        return None

    def get_timestamp(self, bar: ClickhouseBar) -> datetime:
        """Return trade date at midnight UTC (used for ordering)."""
        return datetime.combine(bar.trade_date, time(0, 0, 0))

    def get_available_date_range(self) -> tuple[Optional[str], Optional[str]]:
        """Query ClickHouse for the min/max tradedate for this symbol."""
        try:
            client = self._get_client()
            result = client.query(
                f"""
                SELECT
                    toString(min(tradedate)) AS min_date,
                    toString(max(tradedate)) AS max_date
                FROM {self._database}.as_us_equity_ohlc_daily
                WHERE ticker = '{self.instrument.symbol}'
                """
            )
            if result.result_rows:
                row = result.result_rows[0]
                return (row[0] or None, row[1] or None)
        except Exception as exc:
            logger.warning(
                "clickhouse_adapter.get_date_range_failed",
                symbol=self.instrument.symbol,
                error=str(exc),
            )
        return (None, None)

    # Caching stubs (not supported; memory cache used instead)
    def prime_cache(self, start_date: str, end_date: str) -> int:  # pragma: no cover
        raise NotImplementedError("ClickHouse adapter uses in-memory bar cache.")

    def write_cache(self, bars: list) -> None:  # pragma: no cover
        raise NotImplementedError("ClickHouse adapter uses in-memory bar cache.")

    # =========================================================
    # Private helpers
    # =========================================================

    def _get_client(self) -> Any:
        """Return (or create) a ClickHouse HTTP client."""
        if self._client is None:
            try:
                import clickhouse_connect  # type: ignore[import-untyped]
            except ImportError as exc:
                raise ImportError(
                    "clickhouse-connect is required for ClickhouseDataAdapter. "
                    "Install it with: pip install clickhouse-connect>=0.7"
                ) from exc

            self._client = clickhouse_connect.get_client(
                host=self._host,
                port=self._port,
                username=self._username,
                password=self._password,
                connect_timeout=10,
                query_retries=1,
            )
            logger.debug(
                "clickhouse_adapter.client_connected",
                host=self._host,
                port=self._port,
            )
        return self._client

    def _fetch_bars(self, start_date: str, end_date: str) -> list[ClickhouseBar]:
        """Fetch all bars for this symbol in [start_date, end_date] from ClickHouse.

        Returns list sorted by tradedate ascending.
        """
        symbol = self.instrument.symbol
        try:
            client = self._get_client()
            query = f"""
                SELECT
                    tradedate,
                    toFloat64(open)     AS open,
                    toFloat64(high)     AS high,
                    toFloat64(low)      AS low,
                    toFloat64(close)    AS close,
                    toFloat64(openadj)  AS openadj,
                    toFloat64(highadj)  AS highadj,
                    toFloat64(lowadj)   AS lowadj,
                    toFloat64(closeadj) AS closeadj,
                    toInt64(round(dailyvolumeadj)) AS volume
                FROM {self._database}.as_us_equity_ohlc_daily
                WHERE ticker = '{symbol}'
                  AND tradedate >= toDate('{start_date}')
                  AND tradedate <= toDate('{end_date}')
                  AND openadj > 0
                  AND closeadj > 0
                ORDER BY tradedate ASC
            """
            result = client.query(query)
        except Exception as exc:
            logger.error(
                "clickhouse_adapter.fetch_failed",
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                error=str(exc),
            )
            raise

        bars: list[ClickhouseBar] = []
        q = self.quantizer
        for row in result.result_rows:
            trade_date, raw_o, raw_h, raw_l, raw_c, raw_oa, raw_ha, raw_la, raw_ca, raw_vol = row

            def _dec(v: Any) -> Optional[Decimal]:
                if v is None:
                    return None
                try:
                    d = Decimal(str(v))
                    return d.quantize(q) if d > 0 else None
                except Exception:
                    return None

            bars.append(
                ClickhouseBar(
                    symbol=symbol,
                    trade_date=trade_date if isinstance(trade_date, date) else date.fromisoformat(str(trade_date)),
                    open=_dec(raw_o) or Decimal("0"),
                    high=_dec(raw_h) or Decimal("0"),
                    low=_dec(raw_l) or Decimal("0"),
                    close=_dec(raw_c) or Decimal("0"),
                    open_adj=_dec(raw_oa),
                    high_adj=_dec(raw_ha),
                    low_adj=_dec(raw_la),
                    close_adj=_dec(raw_ca),
                    volume=int(raw_vol) if raw_vol is not None else 0,
                )
            )

        logger.debug(
            "clickhouse_adapter.bars_fetched",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            count=len(bars),
        )
        return bars
