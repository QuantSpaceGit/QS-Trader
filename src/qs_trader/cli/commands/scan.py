"""Scan candidates CLI command."""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import click
from rich.console import Console

console = Console()


def _default_candidate_rule(
    context: Any,
) -> tuple[str, str, float | None, dict[str, Any], dict[str, Any]]:
    """Default candidate rule: accept all with score=0.5.

    This is a pass-through rule; users should provide their own via config.
    Supports both the new context contract and legacy tuple-return.
    """
    features = getattr(context, "features", {}) if not isinstance(context, tuple) else {}
    return "candidate", "default_rule", 0.5, {}, features or {}


def _default_data_loader(identifier: int | str) -> dict[str, Any]:
    """Default data loader: returns empty data.

    In production, this would load from ClickHouse or parquet files.
    """
    return {
        "closes": [],
        "highs": [],
        "lows": [],
        "dates": [],
        "opens": [],
        "volumes": [],
    }


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, value: str) -> str:
    """Validate a ClickHouse identifier for safe SQL construction.

    Identifiers must start with a letter or underscore, matching the
    canonical adapter pattern ``^[A-Za-z_][A-Za-z0-9_]*$``.
    """
    if not value:
        raise ValueError(f"{name} must not be empty.")
    if not _IDENTIFIER_RE.match(value):
        raise ValueError(
            f"Invalid {name}: {value!r}. "
            "Identifiers must start with a letter or underscore and contain "
            "only letters, numbers, and underscores."
        )
    return value


def _parse_secid_list_option(
    _ctx: click.Context,
    _param: click.Parameter,
    value: str | None,
) -> str | Path | None:
    """Parse ``--secid-list`` input, accepting either a file path or ``*``.

    Returns:
        ``"*"``, a validated ``Path``, or ``None``.
    """
    if value is None:
        return None
    if value == "*":
        return value

    path = Path(value)
    if not path.is_file():
        raise click.BadParameter(f"File does not exist: {value}")
    return path


def _build_data_loader(
    data_source: str | None,
    start_date: str | None = None,
    end_date: str | None = None,
    feature_columns: list[str] | None = None,
    price_basis: str | None = None,
) -> Callable[[int | str], dict[str, Any]]:
    """Build a data loader for the candidate scan.

    When ``data_source`` is provided, loads from ClickHouse via the
    canonical qs-datamaster dataset.  Otherwise falls back to the
    pass-through loader that returns empty data.
    """
    if data_source is None:
        return _default_data_loader

    # Resolve price basis to column mapping
    from qs_trader.services.scan.models import resolve_price_basis

    basis_name, col_map = resolve_price_basis(price_basis)

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

            # Validate identifiers for safe SQL construction
            safe_database = _validate_identifier("database", database)
            safe_bars_table = _validate_identifier("bars_table", bars_table)

            # Build column list using price basis mapping
            # Map standard names to ClickHouse column names with aliases
            select_parts = [
                f"tradedate",
                f"{col_map['open']} AS open",
                f"{col_map['high']} AS high",
                f"{col_map['low']} AS low",
                f"{col_map['close']} AS close",
                f"{col_map['volume']} AS volume",
            ]

            # Validate and add feature columns
            if feature_columns:
                from qs_trader.services.scan.models import validate_feature_columns

                validated_features = validate_feature_columns(feature_columns)
                for col in validated_features:
                    select_parts.append(col)

            select_columns = ", ".join(select_parts)

            query = f"""
                SELECT {select_columns}
                FROM {safe_database}.{safe_bars_table}
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
            volumes = []
            features: dict[str, list] = {}
            if feature_columns:
                for col in feature_columns:
                    features[col] = []

            # Determine how many core columns we have (6: date + OHLCV)
            core_count = 6
            for row in result.result_rows:
                ts, o, h, low_val, c, v = row[:core_count]
                dates.append(ts.date() if hasattr(ts, "date") else ts)
                opens.append(float(o))
                highs.append(float(h))
                lows.append(float(low_val))
                closes.append(float(c))
                volumes.append(float(v))
                if feature_columns:
                    for idx, col in enumerate(feature_columns):
                        features[col].append(row[core_count + idx])

            return {
                "closes": closes,
                "highs": highs,
                "lows": lows,
                "dates": dates,
                "opens": opens,
                "volumes": volumes,
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
    "--secid-list",
    "secid_list_path",
    type=str,
    callback=_parse_secid_list_option,
    default=None,
    help=(
        "Path to a file containing one secid per line "
        "(blank lines and # comments ignored), or '*' to scan "
        "the full secmaster universe for --universe-as-of-date."
    ),
)
@click.option(
    "--secid-all",
    is_flag=True,
    default=False,
    help=(
        "Scan all secids active on --universe-as-of-date. "
        "Mutually exclusive with --secid and --secid-list. "
        "Can be combined with --tickers."
    ),
)
@click.option(
    "--universe-as-of-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=None,
    help="Universe date for --secid-all resolution (YYYY-MM-DD).",
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
@click.option(
    "--price-basis",
    default=None,
    help="Price basis for OHLCV columns (default: adjusted_ohlc_adj_columns).",
)
@click.option(
    "--params-json",
    default=None,
    help="Rule parameters as a JSON string (e.g., '{\"lookback\": 20}').",
)
@click.option(
    "--params-file",
    type=click.Path(exists=True, path_type=Path),
    default=None,
    help="Rule parameters as a JSON file path.",
)
def scan_candidates_command(
    tickers: tuple[str, ...],
    secid: tuple[int, ...],
    secid_list_path: str | Path | None,
    secid_all: bool,
    universe_as_of_date: datetime | None,
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
    price_basis: str | None,
    params_json: str | None,
    params_file: Path | None,
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

        # Scan secids from a file (one per line, # comments and blanks ignored)
        qs-trader scan-candidates --secid-list secids.txt \\
            --start-date 2023-01-01 --end-date 2023-12-31

        # Scan all secids active on a universe date
        qs-trader scan-candidates --secid-all --universe-as-of-date 2023-01-01 \\
            --start-date 2023-01-01 --end-date 2023-12-31

        # Equivalent wildcard alias for all secids
        qs-trader scan-candidates --secid-list '*' --universe-as-of-date 2023-01-01 \\
            --start-date 2023-01-01 --end-date 2023-12-31

        # Scan all secids plus additional tickers
        qs-trader scan-candidates --secid-all --universe-as-of-date 2023-01-01 \\
            --tickers SPY --tickers QQQ \\
            --start-date 2023-01-01 --end-date 2023-12-31

        # Scan with ClickHouse data source
        qs-trader scan-candidates --tickers AAPL \\
            --start-date 2023-01-01 --end-date 2023-12-31 \\
            --data-source qs-datamaster

        # Scan with custom rule and features
        qs-trader scan-candidates --tickers AAPL \\
            --start-date 2023-01-01 --end-date 2023-12-31 \\
            --rule my_rules.momentum_rule --feature-columns momentum,volatility

        # Scan with rule parameters
        qs-trader scan-candidates --tickers AAPL \\
            --start-date 2023-01-01 --end-date 2023-12-31 \\
            --params-json '{"lookback": 20, "threshold": 0.5}'

        # Scan with parameters from file
        qs-trader scan-candidates --tickers AAPL \\
            --start-date 2023-01-01 --end-date 2023-12-31 \\
            --params-file params.json
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

        secid_list_is_wildcard = secid_list_path == "*"

        # Parse secid list from file
        if isinstance(secid_list_path, Path):
            secid_list_from_file: list[int] = []
            for line_num, line in enumerate(secid_list_path.read_text(encoding="utf-8").splitlines(), start=1):
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    continue
                try:
                    secid_list_from_file.append(int(stripped))
                except ValueError:
                    console.print(
                        f"[bold red]✗ Invalid secid in --secid-list file at line {line_num}: {stripped!r}[/bold red]"
                    )
                    sys.exit(1)
            all_secids.extend(secid_list_from_file)
            console.print(f"[green]✓ Loaded {len(secid_list_from_file)} secids from {secid_list_path}[/green]")

        # Treat --secid-list '*' as an alias for --secid-all
        if secid_list_is_wildcard:
            secid_all = True

        # Validate full-universe mutual exclusivity with --secid and file-backed --secid-list
        if secid_all:
            if secid:
                option_name = "--secid-list '*'" if secid_list_is_wildcard else "--secid-all"
                console.print(f"[bold red]✗ {option_name} is mutually exclusive with --secid.[/bold red]")
                sys.exit(1)
            if isinstance(secid_list_path, Path):
                option_name = "--secid-list '*'" if secid_list_is_wildcard else "--secid-all"
                console.print(f"[bold red]✗ {option_name} is mutually exclusive with --secid-list.[/bold red]")
                sys.exit(1)
            if universe_as_of_date is None:
                option_name = "--secid-list '*'" if secid_list_is_wildcard else "--secid-all"
                console.print(f"[bold red]✗ {option_name} requires --universe-as-of-date.[/bold red]")
                sys.exit(1)

        # Parse rule parameters
        parameters: dict[str, Any] = {}
        if params_json is not None and params_file is not None:
            console.print("[bold red]✗ Cannot use both --params-json and --params-file.[/bold red]")
            sys.exit(1)

        if params_json is not None:
            try:
                parameters = json.loads(params_json)
                if not isinstance(parameters, dict):
                    raise ValueError("Parameters must be a JSON object.")
            except json.JSONDecodeError as e:
                console.print(f"[bold red]✗ Invalid --params-json: {e}[/bold red]")
                sys.exit(1)

        if params_file is not None:
            try:
                parameters = json.loads(params_file.read_text(encoding="utf-8"))
                if not isinstance(parameters, dict):
                    raise ValueError("Parameters file must contain a JSON object.")
            except json.JSONDecodeError as e:
                console.print(f"[bold red]✗ Invalid JSON in --params-file: {e}[/bold red]")
                sys.exit(1)

        # Resolve default price basis before passing to ScanRunner
        from qs_trader.services.scan.models import DEFAULT_PRICE_BASIS

        resolved_price_basis = price_basis if price_basis else DEFAULT_PRICE_BASIS

        console.print(f"[cyan]Tickers:[/cyan]     {all_tickers}")
        console.print(f"[cyan]Secids:[/cyan]      {all_secids}")
        console.print(f"[cyan]Date Range:[/cyan]  {start_date.date()} to {end_date.date()}")
        console.print(f"[cyan]Strategy:[/cyan]    {strategy_id}")
        console.print(f"[cyan]Horizons:[/cyan]    {horizon_list}")
        console.print(f"[cyan]Data Source:[/cyan] {data_source or '(default)'}")
        console.print(f"[cyan]Price Basis:[/cyan] {resolved_price_basis}")
        console.print(f"[cyan]Features:[/cyan]    {feature_col_list or '(none)'}")
        console.print(f"[cyan]Parameters:[/cyan]  {parameters or '(none)'}")
        console.print(f"[cyan]Output:[/cyan]      {output_dir}")
        console.print()

        # Import resolver and runner
        from qs_trader.services.scan.runner import ScanRunner

        # Build instrument resolver — needed for EITHER ticker or secid scans
        resolver = None
        ch_client = None
        if all_tickers or all_secids or secid_all:
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
                if all_secids or secid_all:
                    console.print(f"[bold red]✗ InstrumentResolver required for --secid scans but failed: {e}[/bold red]")
                    console.print("[red]  Cannot proceed without secid resolution.[/red]")
                    sys.exit(1)
                console.print(f"[yellow]Warning: InstrumentResolver not available: {e}[/yellow]")
                console.print("[yellow]  Scan will proceed without secid resolution.[/yellow]")

        # Resolve all secids when --secid-all is specified
        if secid_all and universe_as_of_date is not None:
            universe_date = universe_as_of_date.date()
            console.print(f"[cyan]Resolving all secids active on {universe_date}...[/cyan]")
            instruments = resolver.resolve_all_secids(universe_date)
            all_secids = [inst.secid for inst in instruments]
            console.print(f"[green]✓ Resolved {len(all_secids)} active instruments[/green]")

        # Build data loader with date range, optional feature columns, and price basis
        data_loader = _build_data_loader(
            data_source,
            start_date=start_date.date().isoformat(),
            end_date=end_date.date().isoformat(),
            feature_columns=feature_col_list,
            price_basis=price_basis,
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

        # Determine database and bars_table for manifest metadata
        import os

        ch_database = os.environ.get("CLICKHOUSE_DATABASE", "market")
        ch_bars_table = os.environ.get("CLICKHOUSE_BARS_TABLE", "as_us_equity_ohlc_daily")

        # Resolve source columns from price basis mapping for manifest
        from qs_trader.services.scan.models import PRICE_BASIS_COLUMNS

        source_columns = PRICE_BASIS_COLUMNS.get(resolved_price_basis, {})

        # Run scan
        runner = ScanRunner(
            instrument_resolver=resolver,
            data_loader=data_loader,
            candidate_rule=candidate_rule,
            strategy_id=strategy_id,
            horizons=horizon_list,
            data_source=data_source or "",
            price_basis=resolved_price_basis,
            parameters=parameters,
            rule_import_path=rule or "qs_trader.cli.commands.scan:_default_candidate_rule",
            database=ch_database,
            bars_table=ch_bars_table,
            source_columns=source_columns,
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
        console.print(f"[cyan]Manifest:[/cyan]       {output_dir / 'scan_manifest.json'}")

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
