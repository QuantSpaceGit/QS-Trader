"""Secmaster-authoritative instrument resolution.

Resolves tickers and secids to stable instrument identities using the
AlgoSeek secmaster table as the authoritative source.

Design:
  - Two-step resolution: secmaster validates identity, then data loads by secid
  - Configurable policies: anchor_first_in_range, fail_on_ambiguity
  - Hard-fail contract: SecmasterAuthorityError when mapping not in secmaster
  - In-memory caching with configurable TTL
  - Batch resolution for multiple tickers

Usage:
    >>> resolver = InstrumentResolver(clickhouse_client, database="market")
    >>> resolved = resolver.resolve_by_ticker("META", date_range=(date(2020, 1, 1), date(2025, 12, 31)))
    >>> print(resolved.secid, resolved.display_symbol)
    3513095 META
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import structlog

from qs_trader.services.data.models import IdentitySource

logger = structlog.get_logger(__name__)

_SAFE_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Far-future sentinel date threshold.  Dates on or after this year in
# ticker-history end_date are treated as NULL ("current ticker").
# Live data may use 2299-12-31 or 2999-12-31 as the sentinel.
_SENTINEL_YEAR_THRESHOLD = 2200


def _normalize_sentinel_end_date(end_date: date | None) -> date | None:
    """Treat far-future sentinel dates as NULL (current ticker)."""
    if end_date is not None and end_date.year >= _SENTINEL_YEAR_THRESHOLD:
        return None
    return end_date


class SecmasterAuthorityError(Exception):
    """Raised when a ticker or secid is not found in secmaster.

    Secmaster is the authoritative source for instrument identity.
    If a mapping is not in secmaster, it is assumed invalid.

    Attributes:
        ticker: The ticker that was not found (if applicable)
        secid: The secid that was not found (if applicable)
        message: Explanation of the error
    """

    def __init__(
        self,
        message: str,
        ticker: Optional[str] = None,
        secid: Optional[int] = None,
    ):
        self.ticker = ticker
        self.secid = secid
        self.message = message
        super().__init__(self.message)


@dataclass
class TickerHistory:
    """Historical ticker usage for a secid.

    Attributes:
        ticker: Ticker symbol
        start_date: First date this ticker was active
        end_date: Last date this ticker was active (None if current)
    """

    ticker: str
    start_date: date
    end_date: Optional[date] = None


@dataclass
class CandidateMapping:
    """Candidate secid mapping when ambiguity is detected.

    Attributes:
        secid: Security identifier
        first_date: First available date for this secid
        last_date: Last available date for this secid
        ticker_history: Historical tickers for this secid
    """

    secid: int
    first_date: date
    last_date: date
    ticker_history: List[TickerHistory] = field(default_factory=list)


@dataclass
class ResolvedInstrument:
    """Resolved instrument with full identity metadata.

    Attributes:
        secid: Stable security identifier
        requested_symbol: Symbol the user originally requested
        display_symbol: Preferred display ticker
        ticker_at_date: Ticker valid on a specific date
        identity_source: How identity was resolved
        first_date: First available date for this secid
        last_date: Last available date for this secid
        requested_start_date: Start date of the requested date range (for audit)
        requested_end_date: End date of the requested date range (for audit)
        ticker_history: Historical tickers for this secid
        ambiguous: True if ticker mapped to multiple secids
        candidates: List of candidate mappings (only when ambiguous=True)
    """

    secid: int
    requested_symbol: str
    display_symbol: str
    ticker_at_date: str
    identity_source: IdentitySource
    first_date: date
    last_date: date
    requested_start_date: date
    requested_end_date: date
    ticker_history: List[TickerHistory] = field(default_factory=list)
    ambiguous: bool = False
    candidates: Optional[List[CandidateMapping]] = None


class InstrumentResolver:
    """Secmaster-authoritative instrument resolution.

    Resolves tickers and secids to stable instrument identities using
    the AlgoSeek secmaster table as the authoritative source.

    When a ``ticker_history_table`` is provided, resolution queries the
    normalized ``as_secmaster_ticker_history`` table instead of parsing
    semicolon-delimited arrays from ``as_secmaster``.  Falls back to
    array parsing when the history table is not configured or returns
    no rows.

    Args:
        clickhouse_client: ClickHouse client instance
        database: ClickHouse database name (default: "market")
        cache_ttl_seconds: Cache TTL in seconds (default: 3600)
        ticker_history_table: Optional normalized ticker history table name
            (e.g. "as_secmaster_ticker_history").  When set, resolution
            queries this table first and falls back to secmaster array
            parsing if the table is empty or unavailable.

    Example:
        >>> resolver = InstrumentResolver(client, database="market")
        >>> resolved = resolver.resolve_by_ticker(
        ...     "META",
        ...     date_range=(date(2020, 1, 1), date(2025, 12, 31)),
        ...     policy="anchor_first_in_range"
        ... )
        >>>
        >>> # With ticker history table for faster, cleaner resolution:
        >>> resolver = InstrumentResolver(
        ...     client,
        ...     database="market",
        ...     ticker_history_table="as_secmaster_ticker_history",
        ... )
    """

    def __init__(
        self,
        clickhouse_client: Any,
        database: str = "market",
        cache_ttl_seconds: int = 3600,
        ticker_history_table: Optional[str] = None,
    ):
        if not _SAFE_IDENTIFIER_RE.match(database):
            raise ValueError(
                f"Invalid database name '{database}'. "
                f"Database names must contain only alphanumeric characters and underscores, "
                f"and must start with a letter or underscore."
            )
        if ticker_history_table is not None and not _SAFE_IDENTIFIER_RE.match(ticker_history_table):
            raise ValueError(
                f"Invalid ticker_history_table name '{ticker_history_table}'. "
                f"Table names must contain only alphanumeric characters and underscores, "
                f"and must start with a letter or underscore."
            )
        self._client = clickhouse_client
        self._database = database
        self._cache_ttl = cache_ttl_seconds
        self._ticker_history_table = ticker_history_table
        self._cache: Dict[Tuple[str, date, date, str], ResolvedInstrument] = {}
        self._cache_timestamps: Dict[Tuple[str, date, date, str], float] = {}

    def resolve_by_ticker(
        self,
        ticker: str,
        date_range: Tuple[date, date],
        policy: str = "anchor_first_in_range",
    ) -> ResolvedInstrument:
        """Resolve a ticker to a secid using secmaster.

        When ``ticker_history_table`` is configured, queries the normalized
        history table first.  Falls back to secmaster array parsing when the
        history table is not set or returns no rows.

        Args:
            ticker: Ticker symbol to resolve
            date_range: (start_date, end_date) for the resolution
            policy: Resolution policy ("anchor_first_in_range" or "fail_on_ambiguity")

        Returns:
            ResolvedInstrument with full identity metadata.
            When policy is "fail_on_ambiguity" and ticker maps to multiple secids,
            returns a ResolvedInstrument with ambiguous=True and candidates list.

        Raises:
            SecmasterAuthorityError: If ticker not found in secmaster
            ValueError: If policy is invalid
        """
        cache_key = (ticker, date_range[0], date_range[1], policy)
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        start_date, end_date = date_range

        # Try ticker history table first if configured
        if self._ticker_history_table is not None:
            try:
                resolved = self._resolve_by_ticker_from_history(
                    ticker, start_date, end_date, policy
                )
                if resolved is not None:
                    self._add_to_cache(cache_key, resolved)
                    return resolved
            except SecmasterAuthorityError:
                raise
            except Exception as e:
                logger.warning(
                    "Ticker history table query failed, falling back to secmaster",
                    ticker=ticker,
                    table=self._ticker_history_table,
                    error=str(e),
                )

        # Fallback: query secmaster for tickers matching the input
        query = f"""
            SELECT
                secid,
                tickers,
                tickersstarttoenddate
            FROM {self._database}.as_secmaster
            WHERE has(splitByChar(';', tickers), %(ticker)s)
        """

        result = self._client.query(query, parameters={"ticker": ticker})

        if not result.result_rows:
            logger.error(
                "Ticker not found in secmaster",
                ticker=ticker,
                database=self._database,
                date_range=(str(start_date), str(end_date)),
                policy=policy,
            )
            raise SecmasterAuthorityError(
                f"Ticker '{ticker}' not found in secmaster. "
                f"Secmaster is the authoritative identity source. "
                f"If this ticker exists, the secmaster data may be stale.",
                ticker=ticker,
            )

        # Parse results and build candidate mappings
        candidates = []
        for row in result.result_rows:
            secid, tickers_str, dates_str = row
            ticker_history = self._parse_ticker_history(tickers_str, dates_str)

            # Filter ticker history to date range
            relevant_history = [
                th
                for th in ticker_history
                if th.ticker == ticker and self._overlaps_date_range(th, start_date, end_date)
            ]

            if not relevant_history:
                continue

            # Get first and last dates for this secid in the range
            first_date = min(th.start_date for th in relevant_history)
            last_date = max(
                th.end_date if th.end_date else end_date for th in relevant_history
            )

            candidates.append(
                CandidateMapping(
                    secid=secid,
                    first_date=first_date,
                    last_date=last_date,
                    ticker_history=ticker_history,
                )
            )

        if not candidates:
            logger.error(
                "Ticker found in secmaster but no valid mappings in date range",
                ticker=ticker,
                database=self._database,
                date_range=(str(start_date), str(end_date)),
                policy=policy,
            )
            raise SecmasterAuthorityError(
                f"Ticker '{ticker}' found in secmaster but no valid mappings "
                f"in date range [{start_date}, {end_date}].",
                ticker=ticker,
            )

        # Apply resolution policy
        if policy == "fail_on_ambiguity":
            if len(candidates) > 1:
                logger.warning(
                    "Ambiguity detected in ticker resolution",
                    ticker=ticker,
                    candidate_count=len(candidates),
                    database=self._database,
                    date_range=(str(start_date), str(end_date)),
                    policy=policy,
                    candidates=[
                        {"secid": c.secid, "first_date": str(c.first_date), "last_date": str(c.last_date)}
                        for c in candidates
                    ],
                )
                raise SecmasterAuthorityError(
                    f"Ticker '{ticker}' maps to {len(candidates)} secids in date range "
                    f"[{start_date}, {end_date}]. Ambiguity must be resolved explicitly. "
                    f"Candidates: {[(c.secid, str(c.first_date), str(c.last_date)) for c in candidates]}",
                    ticker=ticker,
                )
            # Single candidate, proceed normally
            resolved = self._build_resolved_instrument(
                candidates[0], ticker, IdentitySource.TICKER_POINT_IN_TIME, date_range
            )
        elif policy == "anchor_first_in_range":
            # Sort by first_date and take the earliest
            candidates.sort(key=lambda c: c.first_date)
            resolved = self._build_resolved_instrument(
                candidates[0], ticker, IdentitySource.TICKER_POINT_IN_TIME, date_range
            )
        else:
            raise ValueError(
                f"Invalid policy '{policy}'. "
                f"Must be 'anchor_first_in_range' or 'fail_on_ambiguity'."
            )

        self._add_to_cache(cache_key, resolved)
        return resolved

    def resolve_by_secid(
        self,
        secid: int,
        date_range: Tuple[date, date],
    ) -> ResolvedInstrument:
        """Resolve a secid to instrument metadata using secmaster.

        Args:
            secid: Security identifier to resolve
            date_range: (start_date, end_date) for the resolution

        Returns:
            ResolvedInstrument with full identity metadata

        Raises:
            SecmasterAuthorityError: If secid not found in secmaster
        """
        cache_key = (str(secid), date_range[0], date_range[1], "explicit_secid")
        cached = self._get_from_cache(cache_key)
        if cached is not None:
            return cached

        start_date, end_date = date_range

        query = f"""
            SELECT
                secid,
                tickers,
                tickersstarttoenddate
            FROM {self._database}.as_secmaster
            WHERE secid = %(secid)s
        """

        result = self._client.query(query, parameters={"secid": secid})

        if not result.result_rows:
            logger.error(
                "Secid not found in secmaster",
                secid=secid,
                database=self._database,
                date_range=(str(start_date), str(end_date)),
            )
            raise SecmasterAuthorityError(
                f"Secid {secid} not found in secmaster. "
                f"Secmaster is the authoritative identity source.",
                secid=secid,
            )

        row = result.result_rows[0]
        _, tickers_str, dates_str = row
        ticker_history = self._parse_ticker_history(tickers_str, dates_str)

        # Find the ticker active in the date range
        active_tickers = [
            th
            for th in ticker_history
            if self._overlaps_date_range(th, start_date, end_date)
        ]

        if not active_tickers:
            logger.error(
                "Secid found in secmaster but no tickers active in date range",
                secid=secid,
                database=self._database,
                date_range=(str(start_date), str(end_date)),
                ticker_history_count=len(ticker_history),
            )
            raise SecmasterAuthorityError(
                f"Secid {secid} found in secmaster but no tickers active "
                f"in date range [{start_date}, {end_date}].",
                secid=secid,
            )

        # Use the most recent active ticker as display
        # Sort by start_date descending, prefer current ticker (end_date=None)
        active_tickers_sorted = sorted(
            active_tickers,
            key=lambda th: (th.end_date is None, th.start_date),
            reverse=True,
        )
        display_ticker = active_tickers_sorted[0].ticker
        first_date = min(th.start_date for th in active_tickers)
        last_date = max(
            th.end_date if th.end_date else end_date for th in active_tickers
        )

        resolved = ResolvedInstrument(
            secid=secid,
            requested_symbol=str(secid),
            display_symbol=display_ticker,
            ticker_at_date=display_ticker,
            identity_source=IdentitySource.EXPLICIT_SECID,
            first_date=first_date,
            last_date=last_date,
            requested_start_date=start_date,
            requested_end_date=end_date,
            ticker_history=ticker_history,
            ambiguous=False,
            candidates=None,
        )

        self._add_to_cache(cache_key, resolved)
        return resolved

    def resolve_batch(
        self,
        tickers: List[str],
        date_range: Tuple[date, date],
        policy: str = "anchor_first_in_range",
    ) -> Dict[str, ResolvedInstrument]:
        """Resolve multiple tickers in a single batch query.

        Args:
            tickers: List of ticker symbols to resolve
            date_range: (start_date, end_date) for the resolution
            policy: Resolution policy

        Returns:
            Dict mapping ticker to ResolvedInstrument

        Raises:
            SecmasterAuthorityError: If any ticker not found in secmaster
        """
        if not tickers:
            return {}

        # Check cache first
        results = {}
        uncached_tickers = []
        for ticker in tickers:
            cache_key = (ticker, date_range[0], date_range[1], policy)
            cached = self._get_from_cache(cache_key)
            if cached is not None:
                results[ticker] = cached
            else:
                uncached_tickers.append(ticker)

        if not uncached_tickers:
            return results

        start_date, end_date = date_range

        # Single batch query for all uncached tickers
        query = f"""
            SELECT
                secid,
                tickers,
                tickersstarttoenddate
            FROM {self._database}.as_secmaster
            WHERE multiSearchAny(tickers, %(tickers)s) > 0
        """

        try:
            result = self._client.query(query, parameters={"tickers": uncached_tickers})
        except Exception as e:
            logger.error(
                "Batch secmaster query failed",
                tickers=uncached_tickers,
                database=self._database,
                error=str(e),
            )
            raise

        if not result.result_rows:
            logger.error(
                "Batch resolution: no tickers found in secmaster",
                tickers=uncached_tickers,
                database=self._database,
                date_range=(str(start_date), str(end_date)),
            )
            raise SecmasterAuthorityError(
                f"None of the tickers {uncached_tickers} found in secmaster. "
                f"Secmaster is the authoritative identity source.",
                ticker=uncached_tickers[0],
            )

        # Parse results and group by ticker
        ticker_to_candidates: Dict[str, List[CandidateMapping]] = {t: [] for t in uncached_tickers}

        for row in result.result_rows:
            secid, tickers_str, dates_str = row
            ticker_history = self._parse_ticker_history(tickers_str, dates_str)

            # Check which requested tickers this secid matches
            for ticker in uncached_tickers:
                relevant_history = [
                    th
                    for th in ticker_history
                    if th.ticker == ticker and self._overlaps_date_range(th, start_date, end_date)
                ]

                if not relevant_history:
                    continue

                first_date = min(th.start_date for th in relevant_history)
                last_date = max(
                    th.end_date if th.end_date else end_date for th in relevant_history
                )

                ticker_to_candidates[ticker].append(
                    CandidateMapping(
                        secid=secid,
                        first_date=first_date,
                        last_date=last_date,
                        ticker_history=ticker_history,
                    )
                )

        # Process each ticker
        for ticker in uncached_tickers:
            candidates = ticker_to_candidates[ticker]

            if not candidates:
                logger.error(
                    "Batch resolution: ticker not found in secmaster",
                    ticker=ticker,
                    database=self._database,
                    date_range=(str(start_date), str(end_date)),
                    policy=policy,
                )
                raise SecmasterAuthorityError(
                    f"Ticker '{ticker}' not found in secmaster. "
                    f"Secmaster is the authoritative identity source.",
                    ticker=ticker,
                )

            cache_key = (ticker, date_range[0], date_range[1], policy)

            if policy == "fail_on_ambiguity":
                if len(candidates) > 1:
                    logger.warning(
                        "Batch resolution: ambiguity detected",
                        ticker=ticker,
                        candidate_count=len(candidates),
                        database=self._database,
                        date_range=(str(start_date), str(end_date)),
                        policy=policy,
                    )
                    raise SecmasterAuthorityError(
                        f"Ticker '{ticker}' maps to {len(candidates)} secids in date range "
                        f"[{start_date}, {end_date}]. Ambiguity must be resolved explicitly.",
                        ticker=ticker,
                    )

                resolved = self._build_resolved_instrument(
                    candidates[0], ticker, IdentitySource.TICKER_POINT_IN_TIME, date_range
                )
            elif policy == "anchor_first_in_range":
                candidates.sort(key=lambda c: c.first_date)
                resolved = self._build_resolved_instrument(
                    candidates[0], ticker, IdentitySource.TICKER_POINT_IN_TIME, date_range
                )
            else:
                raise ValueError(
                    f"Invalid policy '{policy}'. "
                    f"Must be 'anchor_first_in_range' or 'fail_on_ambiguity'."
                )

            self._add_to_cache(cache_key, resolved)
            results[ticker] = resolved

        return results

    def clear_cache(self) -> None:
        """Clear the resolution cache."""
        self._cache.clear()
        self._cache_timestamps.clear()
        logger.info("Instrument resolver cache cleared")

    def _parse_ticker_history(
        self, tickers_str: str, dates_str: str
    ) -> List[TickerHistory]:
        """Parse semicolon-delimited ticker history from secmaster.

        Args:
            tickers_str: Semicolon-delimited tickers (e.g., "FB;META")
            dates_str: Semicolon-delimited date ranges (e.g., "20120518-20220608;20220609-")

        Returns:
            List of TickerHistory objects
        """
        tickers = tickers_str.split(";")
        date_ranges = dates_str.split(";")

        if len(tickers) != len(date_ranges):
            logger.warning(
                "Ticker history parse mismatch",
                tickers_count=len(tickers),
                dates_count=len(date_ranges),
            )
            return []

        history = []
        for ticker, date_range in zip(tickers, date_ranges):
            # Parse date range: "20120518-20220608" or "20220609-"
            match = re.match(r"(\d{8})[:-](\d{8})?", date_range)
            if not match:
                logger.warning(
                    "Invalid date range format",
                    ticker=ticker,
                    date_range=date_range,
                )
                continue

            start_str = match.group(1)
            end_str = match.group(2)

            start_date = self._parse_date(start_str)
            end_date = self._parse_date(end_str) if end_str else None

            # Treat far-future sentinel dates as NULL (current ticker)
            end_date = _normalize_sentinel_end_date(end_date)

            history.append(
                TickerHistory(
                    ticker=ticker,
                    start_date=start_date,
                    end_date=end_date,
                )
            )

        return history

    def _parse_date(self, date_str: str) -> date:
        """Parse YYYYMMDD date string."""
        return date(
            int(date_str[0:4]),
            int(date_str[4:6]),
            int(date_str[6:8]),
        )

    def _overlaps_date_range(
        self, ticker_history: TickerHistory, start: date, end: date
    ) -> bool:
        """Check if a ticker history entry overlaps with a date range.

        Args:
            ticker_history: Ticker history entry
            start: Range start date
            end: Range end date

        Returns:
            True if the ticker was active during any part of the range
        """
        # Ticker overlaps if: start_date <= end AND (end_date IS NULL OR end_date >= start)
        if ticker_history.start_date > end:
            return False
        if ticker_history.end_date is None:
            return True
        return ticker_history.end_date >= start

    def _build_resolved_instrument(
        self,
        candidate: CandidateMapping,
        requested_symbol: str,
        identity_source: IdentitySource,
        date_range: Tuple[date, date],
    ) -> ResolvedInstrument:
        """Build a ResolvedInstrument from a CandidateMapping.

        Args:
            candidate: Candidate mapping with secid and ticker history
            requested_symbol: Symbol the user originally requested
            identity_source: How identity was resolved
            date_range: (start_date, end_date) to find ticker_at_date and record for audit

        Returns:
            ResolvedInstrument with full identity metadata
        """
        start_date, end_date = date_range

        # Find the most recent ticker in the history for display
        display_ticker = candidate.ticker_history[-1].ticker if candidate.ticker_history else requested_symbol

        # Find the ticker active during the requested date range for ticker_at_date
        ticker_at_date = display_ticker
        if candidate.ticker_history:
            active_tickers = [
                th
                for th in candidate.ticker_history
                if self._overlaps_date_range(th, start_date, end_date)
            ]
            if active_tickers:
                # Use the first active ticker in the range
                ticker_at_date = active_tickers[0].ticker

        return ResolvedInstrument(
            secid=candidate.secid,
            requested_symbol=requested_symbol,
            display_symbol=display_ticker,
            ticker_at_date=ticker_at_date,
            identity_source=identity_source,
            first_date=candidate.first_date,
            last_date=candidate.last_date,
            requested_start_date=start_date,
            requested_end_date=end_date,
            ticker_history=candidate.ticker_history,
            ambiguous=False,
            candidates=None,
        )

    def _resolve_by_ticker_from_history(
        self,
        ticker: str,
        start_date: date,
        end_date: date,
        policy: str,
    ) -> Optional[ResolvedInstrument]:
        """Resolve a ticker using the normalized ticker_history table.

        Queries ``as_secmaster_ticker_history`` directly instead of parsing
        semicolon-delimited arrays from ``as_secmaster``.

        Returns ``None`` when the table is not configured or returns no rows,
        signalling that the caller should fall back to secmaster array parsing.

        Args:
            ticker: Ticker symbol to resolve
            start_date: Start of the requested date range
            end_date: End of the requested date range
            policy: Resolution policy

        Returns:
            ResolvedInstrument when found, or None to trigger fallback.

        Raises:
            SecmasterAuthorityError: When ticker is found but no valid mapping
                exists in the date range (same contract as the secmaster path).
        """
        table = self._ticker_history_table
        if table is None:
            return None

        query = f"""
            SELECT secid, ticker, start_date, end_date, liststatus
            FROM {self._database}.{table}
            WHERE ticker = %(ticker)s
              AND start_date <= %(end_date)s
              AND (end_date IS NULL OR end_date >= %(start_date)s)
            ORDER BY secid, start_date
        """

        result = self._client.query(
            query,
            parameters={
                "ticker": ticker,
                "start_date": start_date,
                "end_date": end_date,
            },
        )

        if not result.result_rows:
            # No rows in history table — signal fallback
            return None

        # Build candidate mappings from history table rows
        secid_to_history: Dict[int, List[TickerHistory]] = {}
        for row in result.result_rows:
            secid, row_ticker, row_start, row_end, _liststatus = row
            # Normalize far-future sentinel end_date to NULL
            row_end = _normalize_sentinel_end_date(row_end)
            th = TickerHistory(
                ticker=row_ticker,
                start_date=row_start,
                end_date=row_end,
            )
            secid_to_history.setdefault(secid, []).append(th)

        candidates = []
        for secid, history in secid_to_history.items():
            relevant = [
                th for th in history
                if th.ticker == ticker and self._overlaps_date_range(th, start_date, end_date)
            ]
            if not relevant:
                continue

            first_date = min(th.start_date for th in relevant)
            last_date = max(
                th.end_date if th.end_date else end_date for th in relevant
            )

            candidates.append(
                CandidateMapping(
                    secid=secid,
                    first_date=first_date,
                    last_date=last_date,
                    ticker_history=history,
                )
            )

        if not candidates:
            # Rows exist but none overlap the date range — this is a real error
            logger.error(
                "Ticker found in history table but no valid mappings in date range",
                ticker=ticker,
                table=table,
                date_range=(str(start_date), str(end_date)),
            )
            raise SecmasterAuthorityError(
                f"Ticker '{ticker}' found in history table but no valid mappings "
                f"in date range [{start_date}, {end_date}].",
                ticker=ticker,
            )

        # Apply resolution policy (same logic as secmaster path)
        if policy == "fail_on_ambiguity":
            if len(candidates) > 1:
                logger.warning(
                    "Ambiguity detected in ticker history resolution",
                    ticker=ticker,
                    candidate_count=len(candidates),
                    table=table,
                    date_range=(str(start_date), str(end_date)),
                    policy=policy,
                )
                raise SecmasterAuthorityError(
                    f"Ticker '{ticker}' maps to {len(candidates)} secids in date range "
                    f"[{start_date}, {end_date}]. Ambiguity must be resolved explicitly. "
                    f"Candidates: {[(c.secid, str(c.first_date), str(c.last_date)) for c in candidates]}",
                    ticker=ticker,
                )
            winning = candidates[0]
        elif policy == "anchor_first_in_range":
            candidates.sort(key=lambda c: c.first_date)
            winning = candidates[0]
        else:
            raise ValueError(
                f"Invalid policy '{policy}'. "
                f"Must be 'anchor_first_in_range' or 'fail_on_ambiguity'."
            )

        # Second query: fetch ALL ticker history rows for the resolved secid
        # so that ticker_history includes the complete record (e.g. FB→META).
        full_history = self._fetch_full_secid_history(winning.secid)
        if full_history is not None:
            winning = CandidateMapping(
                secid=winning.secid,
                first_date=winning.first_date,
                last_date=winning.last_date,
                ticker_history=full_history,
            )

        return self._build_resolved_instrument(
            winning, ticker, IdentitySource.TICKER_POINT_IN_TIME, (start_date, end_date)
        )

    def _fetch_full_secid_history(
        self, secid: int
    ) -> Optional[List[TickerHistory]]:
        """Fetch ALL ticker history rows for a secid.

        Used after initial resolution to populate the complete ticker history
        (e.g. FB→META) rather than only the rows matching the requested ticker.

        Returns ``None`` when the history table is not configured or the query
        fails, signalling that the caller should keep the existing history.
        """
        table = self._ticker_history_table
        if table is None:
            return None

        query = f"""
            SELECT ticker, start_date, end_date, liststatus
            FROM {self._database}.{table}
            WHERE secid = %(secid)s
            ORDER BY start_date
        """

        try:
            result = self._client.query(query, parameters={"secid": secid})
        except Exception as e:
            logger.warning(
                "Full secid history query failed",
                secid=secid,
                table=table,
                error=str(e),
            )
            return None

        if not result.result_rows:
            return None

        history = []
        for row in result.result_rows:
            row_ticker, row_start, row_end, _liststatus = row
            # Normalize far-future sentinel end_date to NULL
            row_end = _normalize_sentinel_end_date(row_end)
            history.append(
                TickerHistory(
                    ticker=row_ticker,
                    start_date=row_start,
                    end_date=row_end,
                )
            )

        return history

    def _get_from_cache(
        self, cache_key: Tuple[str, date, date, str]
    ) -> Optional[ResolvedInstrument]:
        """Get a cached resolution result if valid."""
        if cache_key not in self._cache:
            return None

        cached_time = self._cache_timestamps.get(cache_key, 0)
        if time.time() - cached_time > self._cache_ttl:
            # Cache expired
            del self._cache[cache_key]
            del self._cache_timestamps[cache_key]
            return None

        return self._cache[cache_key]

    def _add_to_cache(
        self, cache_key: Tuple[str, date, date, str], resolved: ResolvedInstrument
    ) -> None:
        """Add a resolution result to the cache."""
        self._cache[cache_key] = resolved
        self._cache_timestamps[cache_key] = time.time()
