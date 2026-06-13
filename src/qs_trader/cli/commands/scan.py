"""Scan candidates CLI command."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import click
from rich.console import Console

console = Console()


def _default_candidate_rule(
    secid: int,
    date_str: str,
    features: dict[str, Any],
) -> tuple[str, str, float | None, dict[str, Any], dict[str, Any]]:
    """Default candidate rule: accept all with score=0.5.

    This is a pass-through rule; users should provide their own via config.
    """
    return "candidate", "default_rule", 0.5, {}, features or {}


def _default_data_loader(identifier: int | str) -> dict[str, Any]:
    """Default data loader: returns empty data.

    In production, this would load from ClickHouse or parquet files.
    """
    return {"closes": [], "highs": [], "lows": [], "dates": []}


def _build_data_loader(
    data_source: str | None,
    start_date: str | None = None,
    end_date: str | None = None,
    feature_columns: list[str] | None = None,
) -> Callable[[int | str], dict[str, Any]]:
    """Build a data loader for the candidate scan.

    When ``data_source`` is provided, loads from ClickHouse via the
    canonical qs-datamaster dataset.  Otherwise falls back to the
    pass-through loader that returns empty data.
    """
    if data_source is None:
        return _default_data_loader

    def _clickhouse_loader(identifier: int | str) -> dict[str, Any]:
        """Load OHLCV data from ClickHouse for a secid."""
        from clickhouse_connect import get_client

        # Resolve connection from environment or defaults
        host = __import__("os").environ.get("CLICKHOUSE_HOST", "localhost")
        port = int(__import__("os").environ.get("CLICKHOUSE_PORT", "8123"))
        user = __import__("os").environ.get("CLICKHOUSE_USER", "default")
        password = __import__("os").environ.get("CLICKHOUSE_PASSWORD", "")
        database = __import__("os").environ.get("CLICKHOUSE_DATABASE", "market")

        client = get_client(host=host, port=port, user=user, password=password, database=database)
        try:
            bars_table = __import__("os").environ.get("CLICKHOUSE_BARS_TABLE", "as_us_equity_ohlc_daily")

            # Build column list: always include core OHLCV, optionally add feature columns
            base_columns = "tradedate, open, high, low, close, dailyvolume"
            if feature_columns:
                extra_cols = ", " + ", ".join(feature_columns)
                select_columns = base_columns + extra_cols
            else:
                select_columns = base_columns

            query = f"""
                SELECT {select_columns}
                FROM {database}.{bars_table}
                WHERE secid = %(secid)s
                  AND tradedate >= %(start_date)s
                  AND tradedate <= %(end_date)s
                ORDER BY tradedate
            """
            result = client.query(
                query,
                parameters={
                    "secid": int(identifier),
                    "start_date": start_date,
                    "end_date": end_date,
                },
            )

            dates = []
            opens = []
            highs = []
            lows = []
            closes = []
            features: dict[str, list] = {}
            if feature_columns:
                for col in feature_columns:
                    features[col] = []

            for row in result.result_rows:
                ts, o, h, low_val, c, _v = row[:6]
                dates.append(ts.date() if hasattr(ts, "date") else ts)
                opens.append(float(o))
                highs.append(float(h))
                lows.append(float(low_val))
                closes.append(float(c))
                if feature_columns:
                    for idx, col in enumerate(feature_columns):
                        features[col].append(row[6 + idx])

            return {
                "closes": closes,
                "highs": highs,
                "lows": lows,
                "dates": dates,
                "opens": opens,
                **features,
            }
        finally:
            client.close()

    return _clickhouse_loader


@click.command("scan-candidates")
@click.option(
    "--tickers",
    multiple=True,
    required=False,
    help="Ticker symbols to scan (repeatable).",
)
@click.option(
    "--secid",
    multiple=True,
    type=int,
    required=False,
    help="Secids to scan directly (repeatable, bypasses ticker resolution).",
)
@click.option(
    "--start-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="Start date (YYYY-MM-DD).",
)
@click.option(
    "--end-date",
    required=True,
    type=click.DateTime(formats=["%Y-%m-%d"]),
    help="End date (YYYY-MM-DD).",
)
@click.option(
    "--strategy-id",
    default="scan",
    help="Strategy identifier for attribution.",
)
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=None,
    help="Output directory for scan results.",
)
@click.option(
    "--horizons",
    default="5,10,20",
    help="Comma-separated forward return horizons in bars.",
)
@click.option(
    "--ticker-policy",
    type=click.Choice(["anchor_first_in_range", "fail_on_ambiguity"]),
    default="anchor_first_in_range",
    help="Resolution policy for ambiguous tickers.",
)
@click.option(
    "--config",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Optional config file for data source and rule configuration.",
)
@click.option(
    "--data-source",
    default=None,
    help="Data source name for ClickHouse data loading (e.g., qs-datamaster).",
)
@click.option(
    "--rule",
    default=None,
    help="Candidate rule module path (e.g., my_rules.momentum_rule).",
)
@click.option(
    "--feature-columns",
    default=None,
    help="Comma-separated feature columns to load from ClickHouse (e.g., momentum,volatility).",
)
def scan_candidates_command(
    tickers: tuple[str, ...],
    secid: tuple[int, ...],
    start_date: datetime,
    end_date: datetime,
    strategy_id: str,
    output_dir: Path | None,
    horizons: str,
    ticker_policy: str,
    config: Path | None,
    data_source: str | None,
    rule: str | None,
    feature_columns: str | None,
):
    """Scan instruments for candidate evaluation with forward returns and MFE/MAE.

    Resolves tickers via InstrumentResolver, evaluates candidate rules,
    computes forward returns / MFE / MAE, and persists results to CSV.

    \b
    Examples:
        # Scan a few tickers
        qs-trader scan-candidates --tickers AAPL --tickers MSFT \\
            --start-date 2023-01-01 --end-date 2023-12-31

        # Scan by secid directly
        qs-trader scan-candidates --secid 12345 --secid 67890 \\
            --start-date 2023-01-01 --end-date 2023-12-31

        # Scan with ClickHouse data source
        qs-trader scan-candidates --tickers AAPL \\
            --start-date 2023-01-01 --end-date 2023-12-31 \\
            --data-source qs-datamaster

        # Scan with custom rule and features
        qs-trader scan-candidates --tickers AAPL \\
            --start-date 2023-01-01 --end-date 2023-12-31 \\
            --rule my_rules.momentum_rule --feature-columns momentum,volatility
    """
    try:
        console.rule("[bold blue]QS-Trader Candidate Scan[/bold blue]")
        console.print()

        # Parse horizons
        horizon_list = [int(h.strip()) for h in horizons.split(",")]

        # Parse feature columns
        feature_col_list: list[str] | None = None
        if feature_columns:
            feature_col_list = [c.strip() for c in feature_columns.split(",") if c.strip()]

        # Resolve output directory
        if output_dir is None:
            output_dir = Path.cwd() / "scan_output"

        # Combine tickers and secids
        all_tickers = list(tickers)
        all_secids = list(secid)

        console.print(f"[cyan]Tickers:[/cyan]     {all_tickers}")
        console.print(f"[cyan]Secids:[/cyan]      {all_secids}")
        console.print(f"[cyan]Date Range:[/cyan]  {start_date.date()} to {end_date.date()}")
        console.print(f"[cyan]Strategy:[/cyan]    {strategy_id}")
        console.print(f"[cyan]Horizons:[/cyan]    {horizon_list}")
        console.print(f"[cyan]Data Source:[/cyan] {data_source or '(default)'}")
        console.print(f"[cyan]Features:[/cyan]    {feature_col_list or '(none)'}")
        console.print(f"[cyan]Output:[/cyan]      {output_dir}")
        console.print()

        # Import resolver and runner
        from qs_trader.services.scan.runner import ScanRunner

        # Build instrument resolver — needed for EITHER ticker or secid scans
        resolver = None
        ch_client = None
        if all_tickers or all_secids:
            try:
                from clickhouse_connect import get_client

                from qs_trader.services.data.instrument_resolver import InstrumentResolver

                host = __import__("os").environ.get("CLICKHOUSE_HOST", "localhost")
                port = int(__import__("os").environ.get("CLICKHOUSE_PORT", "8123"))
                user = __import__("os").environ.get("CLICKHOUSE_USER", "default")
                password = __import__("os").environ.get("CLICKHOUSE_PASSWORD", "")
                database = __import__("os").environ.get("CLICKHOUSE_DATABASE", "market")

                ch_client = get_client(host=host, port=port, user=user, password=password, database=database)
                resolver = InstrumentResolver(
                    clickhouse_client=ch_client,
                    database=database,
                    ticker_history_table="as_secmaster_ticker_history",
                )
                console.print(f"[green]✓ InstrumentResolver connected to {host}:{port}[/green]")
            except Exception as e:
                if all_secids:
                    console.print(f"[bold red]✗ InstrumentResolver required for --secid scans but failed: {e}[/bold red]")
                    console.print("[red]  Cannot proceed without secid resolution.[/red]")
                    sys.exit(1)
                console.print(f"[yellow]Warning: InstrumentResolver not available: {e}[/yellow]")
                console.print("[yellow]  Scan will proceed without secid resolution.[/yellow]")

        # Build data loader with date range and optional feature columns
        data_loader = _build_data_loader(
            data_source,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
            feature_columns=feature_col_list,
        )

        # Build candidate rule
        candidate_rule = _default_candidate_rule
        if rule is not None:
            try:
                import importlib

                module_path, func_name = rule.rsplit(".", 1)
                mod = importlib.import_module(module_path)
                candidate_rule = getattr(mod, func_name)
                console.print(f"[green]✓ Candidate rule loaded: {rule}[/green]")
            except Exception as e:
                console.print(f"[yellow]Warning: Could not load rule '{rule}': {e}[/yellow]")
                console.print("[yellow]  Using default candidate rule.[/yellow]")

        # Run scan
        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=data_loader,
            candidate_rule=candidate_rule,
            strategy_id=strategy_id,
            horizons=horizon_list,
        )

        try:
            with console.status("[cyan]Running candidate scan...[/cyan]"):
                results, summary = runner.run(
                    tickers=all_tickers,
                    secids=all_secids,
                    date_range=(start_date.date(), end_date.date()),
                    output_dir=output_dir,
                    ticker_policy=ticker_policy,
                )
        finally:
            if ch_client is not None:
                ch_client.close()

        # Display summary
        console.print()
        console.rule("[bold green]SCAN SUMMARY[/bold green]")
        console.print()
        console.print(f"[cyan]Instruments:[/cyan]    {summary.total_instruments}")
        console.print(f"[cyan]Processed:[/cyan]      {summary.instruments_processed}")
        console.print(f"[cyan]Failed:[/cyan]         {summary.instruments_failed}")
        console.print(f"[cyan]Total Rows:[/cyan]     {summary.total_rows}")
        console.print(f"[cyan]Output:[/cyan]         {output_dir / 'candidate_scan_results.csv'}")

        if summary.failures:
            console.print()
            console.print("[yellow]Failures:[/yellow]")
            for failure in summary.failures:
                console.print(f"  - {failure}")

        console.print()
        console.rule()

        sys.exit(0)

    except Exception as e:
        console.print()
        console.print(f"[bold red]✗ Scan failed:[/bold red] {e}")
        import traceback

        console.print()
        console.print("[dim]" + traceback.format_exc() + "[/dim]")
        sys.exit(1)
