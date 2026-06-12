"""Scan candidates CLI command."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from typing import Any

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


@click.command("scan-candidates")
@click.option(
    "--tickers",
    multiple=True,
    required=True,
    help="Ticker symbols to scan (repeatable).",
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
def scan_candidates_command(
    tickers: tuple[str, ...],
    start_date: datetime,
    end_date: datetime,
    strategy_id: str,
    output_dir: Path | None,
    horizons: str,
    ticker_policy: str,
    config: Path | None,
):
    """Scan instruments for candidate evaluation with forward returns and MFE/MAE.

    Resolves tickers via InstrumentResolver, evaluates candidate rules,
    computes forward returns / MFE / MAE, and persists results to CSV.

    \b
    Examples:
        # Scan a few tickers
        qs-trader scan-candidates --tickers AAPL --tickers MSFT \\
            --start-date 2023-01-01 --end-date 2023-12-31

        # Scan with custom output directory
        qs-trader scan-candidates --tickers AAPL \\
            --start-date 2023-01-01 --end-date 2023-12-31 \\
            --output-dir /tmp/scan_results

        # Scan with custom horizons
        qs-trader scan-candidates --tickers AAPL --tickers MSFT \\
            --start-date 2023-01-01 --end-date 2023-12-31 \\
            --horizons 5,10,20,60
    """
    try:
        console.rule("[bold blue]QS-Trader Candidate Scan[/bold blue]")
        console.print()

        # Parse horizons
        horizon_list = [int(h.strip()) for h in horizons.split(",")]

        # Resolve output directory
        if output_dir is None:
            output_dir = Path.cwd() / "scan_output"

        console.print(f"[cyan]Tickers:[/cyan]     {list(tickers)}")
        console.print(f"[cyan]Date Range:[/cyan]  {start_date.date()} to {end_date.date()}")
        console.print(f"[cyan]Strategy:[/cyan]    {strategy_id}")
        console.print(f"[cyan]Horizons:[/cyan]    {horizon_list}")
        console.print(f"[cyan]Output:[/cyan]      {output_dir}")
        console.print()

        # Import resolver and runner
        from qs_trader.services.scan.runner import ScanRunner

        # Create resolver (None for now — would be wired from config)
        # KNOWN LIMITATION: resolver is not wired to InstrumentResolver and
        # data_loader does not load from ClickHouse. Actual wiring to
        # InstrumentResolver and ClickHouse data loading is a known limitation
        # to be addressed in a follow-up.
        resolver = None

        # Create data loader (None for now — would be wired from config)
        # See KNOWN LIMITATION comment above.
        data_loader = _default_data_loader

        # Create candidate rule (None for now — would be wired from config)
        candidate_rule = _default_candidate_rule

        # Run scan
        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=data_loader,
            candidate_rule=candidate_rule,
            strategy_id=strategy_id,
            horizons=horizon_list,
        )

        with console.status("[cyan]Running candidate scan...[/cyan]"):
            results, summary = runner.run(
                tickers=list(tickers),
                date_range=(start_date.date(), end_date.date()),
                output_dir=output_dir,
                ticker_policy=ticker_policy,
            )

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
