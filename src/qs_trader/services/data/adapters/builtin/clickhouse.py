"""ClickHouse data adapter for QS-Datamaster OHLC data.

Reads per-symbol daily OHLC bars from `market.as_us_equity_ohlc_daily`
in the QS-Datamaster ClickHouse database.

Canonical PriceBarEvent mapping:
    - open/high/low/close → base OHLC columns preserved for diagnostics and
        compatibility
    - open_adj/high_adj/low_adj/close_adj → canonical adjusted ClickHouse
        series used by the QS-Trader split_adjusted execution path and the
        Research-owned visualization contract (openadj, highadj, lowadj,
        closeadj from AlgoSeek)
    - volume → adjusted runtime volume consumed by the engine
    - volume_raw / volume_adj → dual-basis runtime volume snapshot truth

Design:
  - Implements IDataAdapter protocol for DataSourceResolver auto-discovery
  - Uses clickhouse-connect HTTP client (port 8123)
  - Timestamps use market close 16:00 ET → UTC (same as YahooCSVDataAdapter)
  - Symbol resolution: ticker name matches `ticker` column in OHLC table
    - No corporate action support (AlgoSeek adjusted series are pre-adjusted)
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

import random
import re
import time as time_module
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from typing import Any, Iterator, Optional
from zoneinfo import ZoneInfo

from qs_trader.events.events import CorporateActionEvent, PriceBarEvent
from qs_trader.system import LoggerFactory

logger = LoggerFactory.get_logger()

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _assert_safe_identifier(name: str, field: str) -> None:
    """Reject identifiers containing characters unsafe for SQL object names."""
    if not _SAFE_IDENTIFIER_RE.fullmatch(name):
        raise ValueError(f"ClickHouse adapter field '{field}' contains invalid characters: {name!r}")


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
    volume_raw: Optional[int] = None
    volume_adj: Optional[int] = None


class ClickhouseDataAdapter:
    """QS-Datamaster ClickHouse OHLC adapter implementing IDataAdapter.

    Responsibilities:
      - Batch-fetch OHLC bars from ClickHouse for the full date range.
      - Stream bars as ClickhouseBar objects to DataService.
            - Convert to canonical PriceBarEvent with raw/adjusted OHLCV snapshot truth.
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
        self._bars_table: str = config.get("bars_table", "as_us_equity_ohlc_daily")
        _assert_safe_identifier(self._database, "database")
        _assert_safe_identifier(self._bars_table, "bars_table")

        # Display / metadata config (top-level keys from YAML)
        self.tz_name: str = config.get("timezone", "America/New_York")
        self.asset_class: str = config.get("asset_class", "equity")
        self.price_currency: str = config.get("price_currency", "USD")
        self.price_scale: int = int(config.get("price_scale", 2))
        self.quantizer: Decimal = Decimal(10) ** -self.price_scale

        self.tz = ZoneInfo(self.tz_name)

        # Lazy-initialised ClickHouse client
        self._client: Any = None

        # Retry configuration for transient connection/query failures
        self._max_retries: int = int(ch_cfg.get("max_retries", 3))
        self._retry_base_delay: float = float(ch_cfg.get("retry_base_delay", 1.0))

        # In-memory bar cache: loaded once per read_bars() call
        self._bar_cache: list[ClickhouseBar] = []
        self._cache_range: Optional[tuple[str, str]] = None

        # Secid transition tracking for CorporateActionEvent emission
        # Maps trade_date -> {from_secid, to_secid, linking_factor, from_ticker, to_ticker}
        self._secid_transitions: dict[date, dict[str, Any]] = {}

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
        Identity fields (secid, display_symbol, ticker_at_date, identity_source)
        are populated from the instrument when available.
        """
        market_close_naive = datetime.combine(bar.trade_date, time(16, 0, 0))
        market_close_local = market_close_naive.replace(tzinfo=self.tz)
        market_close_utc = market_close_local.astimezone(timezone.utc)
        timestamp_local = market_close_local.isoformat()

        # Populate identity fields from instrument when available
        secid: Optional[int] = getattr(self.instrument, "secid", None)
        display_symbol: Optional[str] = getattr(self.instrument, "display_symbol", None)
        ticker_at_date: Optional[str] = getattr(self.instrument, "ticker_at_date", None)
        identity_source: Optional[str] = getattr(self.instrument, "identity_source", None)

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
            volume_raw=bar.volume_raw,
            volume_adj=bar.volume_adj,
            price_currency=self.price_currency,
            price_scale=self.price_scale,
            source=self.dataset_name,
            source_service="data_service",
            secid=secid,
            display_symbol=display_symbol,
            ticker_at_date=ticker_at_date,
            identity_source=identity_source,
        )

    def to_corporate_action_event(
        self, bar: ClickhouseBar, prev_bar: Optional[ClickhouseBar] = None
    ) -> Optional[CorporateActionEvent]:
        """Emit CorporateActionEvent for secid transitions.

        When the adapter has detected a secid transition at bar.trade_date
        (from multi-segment chaining), emits a CorporateActionEvent with the
        transition metadata.  For single-segment or legacy paths, returns None.

        Returns:
            CorporateActionEvent for secid transitions, None otherwise.
        """
        transition = self._secid_transitions.get(bar.trade_date)
        if transition is not None:
            eff_date = bar.trade_date.isoformat() if isinstance(bar.trade_date, (date, datetime)) else str(bar.trade_date)
            return CorporateActionEvent(
                symbol=bar.symbol,
                action_type="symbol_change",
                announcement_date=eff_date,
                ex_date=eff_date,
                effective_date=eff_date,
                source="qs-trader",
                price_adjustment_factor=Decimal(str(transition["linking_factor"])),
                split_ratio=Decimal(str(transition["linking_factor"])),
                new_symbol=f"secid_{transition['to_secid']}",
                notes=(
                    f"Secid transition {transition['from_secid']} -> {transition['to_secid']} "
                    f"factor={transition['linking_factor']:.6f}"
                ),
            )
        return None

    def get_timestamp(self, bar: ClickhouseBar) -> datetime:
        """Return trade date at midnight UTC (used for ordering)."""
        return datetime.combine(bar.trade_date, time(0, 0, 0))

    def get_available_date_range(self) -> tuple[Optional[str], Optional[str]]:
        """Query ClickHouse for the min/max tradedate for this symbol.

        Uses secid-based query. Returns (None, None) when secid is not available.
        """
        secid = getattr(self.instrument, "secid", None)
        if secid is None:
            return (None, None)
        try:
            client = self._get_client()
            query = f"""
                SELECT
                    toString(min(tradedate)) AS min_date,
                    toString(max(tradedate)) AS max_date
                FROM {self._database}.{self._bars_table}
                WHERE secid = {{secid:UInt64}}
                """
            result = client.query(query, parameters={"secid": secid})
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

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        """Determine if an exception is transient and worth retrying.

        Retryable: connection errors, timeouts, network issues.
        Not retryable: auth failures, SQL syntax errors, missing tables.
        """
        msg = str(exc).lower()
        # Permanent failures — never retry
        permanent_keywords = [
            "authentication",
            "auth failed",
            "access denied",
            "syntax error",
            "unknown table",
            "unknown database",
            "unknown identifier",
            "illegal column",
            "type mismatch",
        ]
        if any(kw in msg for kw in permanent_keywords):
            return False
        # Connection/network errors are retryable
        retryable_types = (ConnectionError, TimeoutError, OSError)
        if isinstance(exc, retryable_types):
            return True
        # clickhouse-connect wraps errors in various types; check message
        retryable_keywords = ["connection", "timeout", "reset", "refused", "unreachable", "network"]
        return any(kw in msg for kw in retryable_keywords)

    def _retry_with_backoff(self, operation_name: str, func: Any, *args: Any, **kwargs: Any) -> Any:
        """Execute func with exponential backoff retry for transient failures."""
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                last_exc = exc
                if attempt == self._max_retries or not self._is_retryable_error(exc):
                    raise
                delay = self._retry_base_delay * (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "clickhouse_adapter.retry",
                    operation=operation_name,
                    attempt=attempt,
                    max_retries=self._max_retries,
                    delay_seconds=round(delay, 2),
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
                time_module.sleep(delay)
        if last_exc is not None:  # pragma: no cover
            raise last_exc
        raise RuntimeError(f"{operation_name} failed without an exception")  # pragma: no cover

    def _get_client(self) -> Any:
        """Return (or create) a ClickHouse HTTP client with retry on transient failures."""
        if self._client is None:
            try:
                import clickhouse_connect  # type: ignore[import-untyped]
            except ImportError as exc:
                raise ImportError(
                    "clickhouse-connect is required for ClickhouseDataAdapter. "
                    "Install it with: pip install clickhouse-connect>=0.7"
                ) from exc

            def _connect() -> Any:
                return clickhouse_connect.get_client(
                    host=self._host,
                    port=self._port,
                    username=self._username,
                    password=self._password,
                    connect_timeout=10,
                    query_retries=1,
                )

            self._client = self._retry_with_backoff("connect", _connect)
            logger.debug(
                "clickhouse_adapter.client_connected",
                host=self._host,
                port=self._port,
            )
        return self._client

    def _fetch_bars(self, start_date: str, end_date: str) -> list[ClickhouseBar]:
        """Fetch all bars for this symbol in [start_date, end_date] from ClickHouse.

        Returns list sorted by tradedate ascending.
        When instrument.secid_segments has >1 entry, loads bars per-segment
        and chains them with linking factors for a continuous adjusted price
        series.  Single-segment and legacy paths are unchanged.
        """
        self._secid_transitions = {}
        symbol = self.instrument.symbol
        segs = getattr(self.instrument, "secid_segments", None)

        # Multi-segment chaining path
        if segs is not None and len(segs) > 1:
            return self._fetch_bars_chained(symbol, segs, start_date, end_date)

        # Single-segment path (secid required)
        secid = getattr(self.instrument, "secid", None)
        assert secid is not None, "ClickHouse adapter requires secid for single-segment fetch"
        try:
            client = self._get_client()
            query = self._build_query_by_secid()
            params: dict[str, Any] = {
                "secid": secid,
                "start_date": start_date,
                "end_date": end_date,
            }
            result = self._retry_with_backoff("fetch_bars", client.query, query, parameters=params)
        except Exception as exc:
            logger.error(
                "clickhouse_adapter.fetch_failed",
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                error=str(exc),
            )
            raise

        bars = self._rows_to_bars(result.result_rows, symbol)
        logger.debug(
            "clickhouse_adapter.bars_fetched",
            symbol=symbol,
            start_date=start_date,
            end_date=end_date,
            count=len(bars),
        )
        return bars

    def _build_query_by_secid(self) -> str:
        """Build SQL query to fetch bars by secid."""
        return f"""
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
                toInt64(round(dailyvolume)) AS volume_raw,
                toInt64(round(dailyvolumeadj)) AS volume_adj
            FROM {self._database}.{self._bars_table}
            WHERE secid = {{secid:UInt64}}
              AND tradedate >= toDate({{start_date:String}})
              AND tradedate <= toDate({{end_date:String}})
              AND openadj > 0
              AND closeadj > 0
            ORDER BY tradedate ASC
        """

    def _rows_to_bars(self, rows: list[Any], symbol: str) -> list[ClickhouseBar]:
        """Convert raw ClickHouse result rows to ClickhouseBar objects."""
        bars: list[ClickhouseBar] = []
        q = self.quantizer
        for row in rows:
            (
                trade_date,
                raw_o,
                raw_h,
                raw_l,
                raw_c,
                raw_oa,
                raw_ha,
                raw_la,
                raw_ca,
                raw_vol_raw,
                raw_vol_adj,
            ) = row

            def _dec(v: Any) -> Optional[Decimal]:
                if v is None:
                    return None
                try:
                    d = Decimal(str(v))
                    return d.quantize(q) if d > 0 else None
                except Exception:
                    return None

            volume_raw = int(raw_vol_raw) if raw_vol_raw is not None else None
            volume_adj = int(raw_vol_adj) if raw_vol_adj is not None else None
            strategy_volume = volume_adj if volume_adj is not None else (volume_raw or 0)

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
                    volume=strategy_volume,
                    volume_raw=volume_raw,
                    volume_adj=volume_adj,
                )
            )
        return bars

    def _fetch_bars_chained(
        self,
        symbol: str,
        segments: list[Any],
        start_date: str,
        end_date: str,
    ) -> list[ClickhouseBar]:
        """Fetch bars per-segment and chain them with linking factors.

        For tickers that transition between secids (e.g. GOOGL from secid
        166006 to 4579561), this method loads each segment's bars separately,
        computes a linking factor at each boundary using closeadj, and applies
        cumulative factors to all adjusted price fields so the resulting series
        is continuous.
        """
        client = self._get_client()
        start_d = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_d = datetime.strptime(end_date, "%Y-%m-%d").date()

        all_bars: list[ClickhouseBar] = []
        prev_last_closeadj: Optional[float] = None
        cumulative_factor: float = 1.0
        prev_secid: Optional[int] = None

        for seg in segments:
            seg_secid = seg.secid
            seg_start = seg.start_date
            seg_end = seg.end_date

            # Clamp segment date range to the requested [start_d, end_d]
            query_start = max(seg_start, start_d) if seg_start else start_d
            query_end = min(seg_end, end_d) if seg_end else end_d

            if query_start > query_end:
                continue  # segment outside requested range

            query = self._build_query_by_secid()
            params = {
                "secid": seg_secid,
                "start_date": query_start.isoformat(),
                "end_date": query_end.isoformat(),
            }
            try:
                result = self._retry_with_backoff("fetch_bars_chained", client.query, query, parameters=params)
            except Exception as exc:
                logger.error(
                    "clickhouse_adapter.chained_fetch_failed",
                    symbol=symbol,
                    secid=seg_secid,
                    start_date=query_start.isoformat(),
                    end_date=query_end.isoformat(),
                    error=str(exc),
                )
                raise

            segment_bars = self._rows_to_bars(result.result_rows, symbol)
            if not segment_bars:
                logger.warning(
                    "clickhouse_adapter.chained_segment_empty",
                    symbol=symbol,
                    secid=seg_secid,
                    start=query_start.isoformat(),
                    end=query_end.isoformat(),
                )
                continue

            # Compute linking factor at the boundary between this segment
            # and the previous one (for adjusted price continuity).
            first_closeadj = float(segment_bars[0].close_adj) if segment_bars[0].close_adj else None
            if prev_last_closeadj is not None and first_closeadj is not None and first_closeadj > 0:
                linking_factor = prev_last_closeadj / first_closeadj
                cumulative_factor *= linking_factor

                # Record transition for CorporateActionEvent
                transition_date = segment_bars[0].trade_date
                self._secid_transitions[transition_date] = {
                    "from_secid": prev_secid,
                    "to_secid": seg_secid,
                    "linking_factor": linking_factor,
                    "cumulative_factor": cumulative_factor,
                }

            # Apply cumulative factor to adjusted prices in this segment
            if cumulative_factor != 1.0:
                multiplied_bars = []
                for bar in segment_bars:
                    adj_fields = {
                        "open_adj": bar.open_adj,
                        "high_adj": bar.high_adj,
                        "low_adj": bar.low_adj,
                        "close_adj": bar.close_adj,
                    }
                    for field, val in adj_fields.items():
                        if val is not None:
                            scaled = float(val) * cumulative_factor
                            adj_fields[field] = Decimal(str(scaled)).quantize(self.quantizer)

                    multiplied_bars.append(
                        ClickhouseBar(
                            symbol=bar.symbol,
                            trade_date=bar.trade_date,
                            open=bar.open,
                            high=bar.high,
                            low=bar.low,
                            close=bar.close,
                            open_adj=adj_fields["open_adj"],
                            high_adj=adj_fields["high_adj"],
                            low_adj=adj_fields["low_adj"],
                            close_adj=adj_fields["close_adj"],
                            volume=bar.volume,
                            volume_raw=bar.volume_raw,
                            volume_adj=bar.volume_adj,
                        )
                    )
                all_bars.extend(multiplied_bars)
            else:
                all_bars.extend(segment_bars)

            # Track the last closeadj of this segment for the next boundary
            last_bar = segment_bars[-1]
            if cumulative_factor != 1.0:
                # Find the last bar from the multiplied version
                last_idx = len(all_bars) - 1
                last_bar = all_bars[last_idx]

            prev_last_closeadj = float(last_bar.close_adj) if last_bar.close_adj else None
            prev_secid = seg_secid

        logger.info(
            "clickhouse_adapter.bars_chained",
            symbol=symbol,
            segments=len(segments),
            total_bars=len(all_bars),
            transitions=len(self._secid_transitions),
        )
        return all_bars
