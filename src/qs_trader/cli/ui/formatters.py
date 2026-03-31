"""Rich table formatters for CLI output."""

from typing import Any, Dict, Optional

from rich.table import Table


def create_bar_table(symbol: str, idx: int, total: int) -> Table:
    """
    Create a Rich table for displaying a single bar.

    Args:
        symbol: Stock symbol
        idx: Current bar index (1-based)
        total: Total number of bars

    Returns:
        Configured Rich Table
    """
    return Table(title=f"Bar {idx}/{total} - {symbol} (raw)")


def add_bar_data(table: Table, bar_data: Dict[str, Any]) -> None:
    """
    Add bar data rows to a table.

    Args:
        table: Rich Table instance
        bar_data: Dictionary with bar fields (date, open, high, low, close, volume, dividend)
    """
    table.add_column("Field", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Date", bar_data["date"])
    table.add_row("Open", f"${bar_data['open']:.2f}")
    table.add_row("High", f"${bar_data['high']:.2f}")
    table.add_row("Low", f"${bar_data['low']:.2f}")
    table.add_row("Close", f"${bar_data['close']:.2f}")
    table.add_row("Volume", f"{bar_data['volume']:,}")

    if bar_data.get("dividend"):
        table.add_row("Dividend", f"${bar_data['dividend']:.4f}", style="green bold")


def create_update_summary_table() -> Table:
    """
    Create a Rich table for update summary.

    Returns:
        Configured Rich Table with columns
    """
    table = Table(title="Update Summary")
    table.add_column("Symbol", style="cyan", no_wrap=True)
    table.add_column("Status", style="white")
    table.add_column("Bars Added", style="magenta", justify="right")
    table.add_column("Date Range", style="dim")
    table.add_column("Total Bars", style="yellow", justify="right")
    return table


def add_update_result_row(
    table: Table,
    symbol: str,
    success: bool,
    bars_added: int,
    start_date: Optional[str],
    end_date: Optional[str],
    row_count: Optional[str],
    error: Optional[str] = None,
) -> None:
    """
    Add a result row to the update summary table.

    Args:
        table: Rich Table instance
        symbol: Stock symbol
        success: Whether update was successful
        bars_added: Number of bars added
        start_date: Start date of cached data
        end_date: End date of cached data
        row_count: Total bars in cache
        error: Error message if failed
    """
    if success:
        if bars_added == 0:
            status = "[green]✓ Current[/green]"
            bars_str = "-"
        else:
            status = "[green]✓ Updated[/green]"
            bars_str = str(bars_added)
    else:
        status = "[red]✗ Error[/red]"
        bars_str = "-"

    date_range = f"{start_date} to {end_date}" if start_date and end_date else "-"
    count_str = str(row_count) if row_count else "-"

    table.add_row(symbol, status, bars_str, date_range, count_str)


def create_cache_info_table() -> Table:
    """
    Create a Rich table for cache info display.

    Returns:
        Configured Rich Table with columns
    """
    table = Table(title="Cached Symbols")
    table.add_column("Symbol", style="cyan", no_wrap=True)
    table.add_column("Start Date", style="green")
    table.add_column("End Date", style="green")
    table.add_column("Bars", justify="right", style="yellow")
    table.add_column("Last Update", style="dim")
    return table


def add_cache_info_row(
    table: Table,
    symbol: str,
    start_date: str,
    end_date: str,
    row_count: str,
    last_update: str,
) -> None:
    """
    Add a cache info row to the table.

    Args:
        table: Rich Table instance
        symbol: Stock symbol
        start_date: Start date of cached data
        end_date: End date of cached data
        row_count: Number of bars in cache
        last_update: Last update timestamp
    """
    # Format last update (show just date/time, not full ISO)
    if last_update != "N/A" and "T" in last_update:
        last_update = last_update.split("T")[0] + " " + last_update.split("T")[1][:8]

    table.add_row(symbol, start_date, end_date, str(row_count), last_update)
