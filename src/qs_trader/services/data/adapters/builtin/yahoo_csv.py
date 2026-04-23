"""Yahoo Finance CSV daily OHLC adapter.

Reads per-symbol CSV files containing columns:
Date,Open,High,Low,Close,Adj Close,Volume

Canonical PriceBarEvent mapping:
  - open/high/low/close → standard split-adjusted prices (Yahoo already backfills
    historical prices for splits; dividend drops remain visible)
  - close_adj → mapped from "Adj Close" (total-return adjusted close)
  - open_adj/high_adj/low_adj → None (Yahoo CSV does not provide total-return adjusted values)
    - volume / volume_raw → Yahoo raw volume (no distinct adjusted series)

Design Goals:
  - Lightweight, zero third-party dependencies (uses Python csv module)
  - Streaming iterator (no full DataFrame materialization)
  Key Design Principles:
  - Independent from other adapters (no external dependencies)
  - Implements IDataAdapter protocol methods used by DataService

Limitations:
  - No corporate action extraction (Yahoo daily CSV lacks explicit split/dividend fields)
  - No caching (prime_cache/write_cache raise NotImplementedError)
  - Only supports daily frequency (interval="1d")

Example:
    >>> config = {
    ...     "root_path": "data/us-equity-yahoo-csv",
    ...     "path_template": "{root_path}/{symbol}.csv",
    ...     "timezone": "America/New_York",
    ...     "price_currency": "USD",
    ...     "price_scale": 2,
    ... }
    >>> instrument = Instrument(symbol="AAPL")
    >>> adapter = YahooCSVDataAdapter(config, instrument, dataset_name="yahoo-us-equity-1d-csv")
    >>> for raw_bar in adapter.read_bars("2020-01-02", "2020-01-10"):
    ...     event = adapter.to_price_bar_event(raw_bar)
    ...     print(event.close, event.close_adj)
"""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Iterator, Optional

from qs_trader.events.events import CorporateActionEvent, PriceBarEvent
from qs_trader.services.data.adapters.protocol import IDataAdapter
from qs_trader.system import LoggerFactory

try:  # Python 3.9+ zoneinfo
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover - fallback
    import pytz

    ZoneInfo = pytz.timezone  # type: ignore

logger = LoggerFactory.get_logger()


@dataclass(slots=True)
class YahooCSVBar:
    """Internal representation of a single Yahoo Finance daily bar."""

    symbol: str
    trade_date: date
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    adj_close: Optional[Decimal]
    volume: int


class YahooCSVDataAdapter(IDataAdapter):
    """Yahoo Finance CSV OHLC adapter implementing IDataAdapter.

    Responsibilities:
      - Stream CSV rows as YahooCSVBar objects
      - Convert to canonical PriceBarEvent
      - Provide date range and timestamp extraction

    Notes:
      - Only `close_adj` populated (from Adj Close); other *_adj fields remain None.
      - Prices are converted to Decimal and quantized to `price_scale`.
      - Timestamps use market close (16:00 America/New_York) converted to UTC.
    """

    def __init__(self, config: dict, instrument, dataset_name: Optional[str] = None):
        self.config = config
        self.instrument = instrument
        self.dataset_name = dataset_name or "yahoo-us-equity-1d-csv"
        # Config flag to toggle synthetic adjusted OHLC generation
        self.synthetic_enabled: bool = bool(config.get("synthetic_adjust_ohlc", True))

        required_keys = ["root_path", "path_template"]
        missing = [k for k in required_keys if k not in config]
        if missing:
            raise ValueError(f"Missing required config keys: {missing}")

        self.root_path = Path(config["root_path"]).expanduser()
        self.path_template = config["path_template"]
        self.csv_path = Path(self.path_template.format(root_path=str(self.root_path), symbol=instrument.symbol))

        if not self.csv_path.exists():
            raise FileNotFoundError(f"Yahoo CSV not found for symbol {instrument.symbol}: {self.csv_path}")

        # Cache static config values to avoid repeated lookups and object construction
        self.tz_name: str = config.get("timezone", "America/New_York")
        self.asset_class: str = config.get("asset_class", "equity")
        self.price_currency: str = config.get("price_currency", "USD")
        self.price_scale: int = int(config.get("price_scale", 2))
        self.quantizer: Decimal = Decimal(10) ** -self.price_scale

        # Pre-construct timezone object
        self.tz = ZoneInfo(self.tz_name)

        # Load dividend calendar for corporate action events
        self.dividends_by_symbol: dict[str, list[dict]] = {}
        dividends_calendar_path = self.root_path / "dividends_calendar.json"
        if dividends_calendar_path.exists():
            try:
                with dividends_calendar_path.open("r") as f:
                    all_dividends = json.load(f)
                    # Extract dividends for this symbol
                    if instrument.symbol in all_dividends:
                        self.dividends_by_symbol[instrument.symbol] = all_dividends[instrument.symbol]
                        logger.debug(
                            "yahoo_csv_adapter.dividends_loaded",
                            symbol=instrument.symbol,
                            dividend_count=len(self.dividends_by_symbol[instrument.symbol]),
                        )
            except Exception as e:
                logger.warning(
                    "yahoo_csv_adapter.dividends_load_failed",
                    symbol=instrument.symbol,
                    path=str(dividends_calendar_path),
                    error=str(e),
                )
        else:
            logger.debug(
                "yahoo_csv_adapter.no_dividends_calendar",
                symbol=instrument.symbol,
                path=str(dividends_calendar_path),
            )

        logger.debug(
            "yahoo_csv_adapter.initialized",
            symbol=instrument.symbol,
            path=str(self.csv_path),
            dataset=self.dataset_name,
        )

    # =========================================================
    # IDataAdapter protocol methods
    # =========================================================
    def read_bars(self, start_date: str, end_date: str) -> Iterator[YahooCSVBar]:
        """Stream Yahoo CSV rows as YahooCSVBar objects in ascending date order.

        Args:
            start_date: Inclusive ISO date (YYYY-MM-DD)
            end_date: Inclusive ISO date (YYYY-MM-DD)
        """
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()

        if start > end:
            raise ValueError("start_date must be <= end_date")

        with self.csv_path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d = date.fromisoformat(row["Date"])
                if d < start:
                    continue
                if d > end:
                    break  # Early exit - CSV is date-sorted

                # Parse numeric fields; Yahoo may include high precision floats
                try:
                    open_px = Decimal(row["Open"]) if row["Open"] else Decimal("0")
                    high_px = Decimal(row["High"]) if row["High"] else Decimal("0")
                    low_px = Decimal(row["Low"]) if row["Low"] else Decimal("0")
                    close_px = Decimal(row["Close"]) if row["Close"] else Decimal("0")
                    adj_close_raw = row.get("Adj Close")
                    adj_close_px = Decimal(str(adj_close_raw)) if adj_close_raw not in (None, "", "NA") else None
                    volume_val = int(row["Volume"]) if row["Volume"] else 0
                except Exception as e:  # pragma: no cover - defensive
                    logger.error("yahoo_csv_adapter.parse_error", symbol=self.instrument.symbol, row=row, error=str(e))
                    continue

                yield YahooCSVBar(
                    symbol=self.instrument.symbol,
                    trade_date=d,
                    open=open_px.quantize(self.quantizer),
                    high=high_px.quantize(self.quantizer),
                    low=low_px.quantize(self.quantizer),
                    close=close_px.quantize(self.quantizer),
                    adj_close=adj_close_px.quantize(self.quantizer) if adj_close_px is not None else None,
                    volume=volume_val,
                )

    def to_price_bar_event(self, bar: YahooCSVBar) -> PriceBarEvent:
        """Convert YahooCSVBar to canonical PriceBarEvent.

        Populates adjusted close from Yahoo `Adj Close`. If present, synthetic
        adjusted open/high/low are derived using ratio = adj_close / close.
        This preserves relative intraday range while embedding dividend effect.
        Timestamp set to market close 16:00 local Eastern time converted to UTC.
        """
        market_close_naive = datetime.combine(bar.trade_date, time(16, 0, 0))
        market_close_local = market_close_naive.replace(tzinfo=self.tz)
        market_close_utc = market_close_local.astimezone(timezone.utc)
        timestamp_local = market_close_local.isoformat()

        # Synthetic adjusted OHLC if enabled and adj_close available and close non-zero
        open_adj: Optional[Decimal] = None
        high_adj: Optional[Decimal] = None
        low_adj: Optional[Decimal] = None
        close_adj: Optional[Decimal] = bar.adj_close

        if self.synthetic_enabled and bar.adj_close is not None and bar.close != 0:
            try:
                ratio = (bar.adj_close / bar.close).quantize(self.quantizer)
                open_adj = (bar.open * ratio).quantize(self.quantizer)
                high_adj = (bar.high * ratio).quantize(self.quantizer)
                low_adj = (bar.low * ratio).quantize(self.quantizer)
                # close_adj already set to adj_close (quantized in read phase)
            except Exception as e:  # pragma: no cover
                logger.error("yahoo_csv_adapter.synthetic_adjustment_error", symbol=bar.symbol, error=str(e))
                # Fallback: keep only close_adj

        if not self.synthetic_enabled and bar.adj_close is not None:
            logger.debug(
                "yahoo_csv_adapter.synthetic_disabled",
                symbol=bar.symbol,
                reason="synthetic_adjust_ohlc flag false; only close_adj populated",
            )

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
            # Adjusted prices (synthetic OHLC when adj close available)
            open_adj=open_adj,
            high_adj=high_adj,
            low_adj=low_adj,
            close_adj=close_adj,
            volume=bar.volume,
            volume_raw=bar.volume,
            price_currency=self.price_currency,
            price_scale=self.price_scale,
            source=self.dataset_name,
            source_service="data_service",
        )

    def to_corporate_action_event(
        self, bar: YahooCSVBar, prev_bar: Optional[YahooCSVBar] = None
    ) -> Optional[CorporateActionEvent]:
        """Check if bar date has a dividend and emit CorporateActionEvent.

        Dividends are loaded from dividends_calendar.json. The date in the calendar
        represents the ex-dividend date (per yfinance semantics). This is the date
        when the stock starts trading without the dividend.

        Args:
            bar: Current bar
            prev_bar: Previous bar (unused for dividends)

        Returns:
            CorporateActionEvent if dividend exists for this date, None otherwise.

        Notes:
            - ex_date is set to the dividend date (ex-dividend date from yfinance)
            - effective_date is also set to ex_date (payment date not available)
            - announcement_date is set to ex_date (announcement date not available)
        """
        # Check if we have dividends for this symbol
        if bar.symbol not in self.dividends_by_symbol:
            return None

        # Search for dividend matching this bar's date
        bar_date_str = bar.trade_date.isoformat()
        for dividend in self.dividends_by_symbol[bar.symbol]:
            if dividend["date"] == bar_date_str:
                # Found dividend for this date - create corporate action event
                dividend_amount = Decimal(str(dividend["amount"])).quantize(self.quantizer)

                # Calculate effective_date as next business day after ex_date
                # Ex-date is when stock trades without dividend (price drops)
                # Effective date (T+1) is when dividend is actually applied to accounts
                # The effective date can be more than one day, but we don't have that info here

                effective_date = bar.trade_date + timedelta(days=1)
                # Skip weekends (Saturday=5, Sunday=6)
                while effective_date.weekday() >= 5:
                    effective_date += timedelta(days=1)
                effective_date_str = effective_date.isoformat()

                logger.debug(
                    "yahoo_csv_adapter.dividend_event",
                    symbol=bar.symbol,
                    ex_date=bar_date_str,
                    effective_date=effective_date_str,
                    amount=str(dividend_amount),
                )

                return CorporateActionEvent(
                    symbol=bar.symbol,
                    asset_class=self.asset_class,
                    action_type="dividend",
                    # Per yfinance: date is ex-dividend date
                    announcement_date=bar_date_str,  # Not available, use ex_date
                    ex_date=bar_date_str,  # Ex-dividend date from yfinance
                    effective_date=effective_date_str,  # Next business day after ex_date
                    source=self.dataset_name,
                    dividend_amount=dividend_amount,
                    dividend_currency=self.price_currency,
                    dividend_type="ordinary",  # Assume ordinary dividend (most common type)
                    source_service="data_service",
                )

        return None

    def get_timestamp(self, bar: YahooCSVBar) -> datetime:
        """Return trade date at midnight (naive)."""
        return datetime.combine(bar.trade_date, time(0, 0, 0))

    def get_available_date_range(self) -> tuple[Optional[str], Optional[str]]:
        """Scan CSV for min/max dates without loading all rows."""
        min_date: Optional[str] = None
        max_date: Optional[str] = None
        with self.csv_path.open("r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                d = row.get("Date")
                if not d:
                    continue
                if min_date is None:
                    min_date = d
                max_date = d
        return (min_date, max_date)

    # Optional caching interface (not supported for Yahoo CSV adapter)
    def prime_cache(self, start_date: str, end_date: str) -> int:  # pragma: no cover - not implemented
        raise NotImplementedError("Yahoo CSV adapter does not support cache priming.")

    def write_cache(self, bars: list) -> None:  # pragma: no cover - not implemented
        raise NotImplementedError("Yahoo CSV adapter does not support cache writing.")
