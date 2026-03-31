"""Progress bar utilities for CLI."""

from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TimeElapsedColumn


def create_update_progress(console: Console) -> Progress:
    """
    Create a Rich Progress instance for dataset updates.

    Args:
        console: Rich Console instance

    Returns:
        Configured Progress instance
    """
    return Progress(
        SpinnerColumn(),
        "[progress.description]{task.description}",
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
    )
