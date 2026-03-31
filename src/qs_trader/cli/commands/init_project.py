"""Initialize a complete QS-Trader project."""

import shutil
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


@click.command("init-project")
@click.argument("path", type=click.Path(path_type=Path))
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite existing directory",
)
def init_project_command(path: Path, force: bool) -> None:
    """
    Initialize a complete QS-Trader project environment.

    Creates a production-ready backtesting system with:
    - Configuration files (qs_trader.yaml, data_sources.yaml)
    - Example backtests (buy & hold, SMA crossover)
    - Example strategies (ready to run)
    - Sample data
    - Runner script
    - Custom library scaffold

    This creates a SYSTEM, not just one backtest. You can run multiple
    different backtests within this project.

    \b
    Examples:
        # Create new project
        qs-trader init-project my-trading-system

        # Create in current directory
        qs-trader init-project .

        # Overwrite existing
        qs-trader init-project my-system --force
    """
    # Expand and resolve path
    target_path = path.expanduser().resolve()

    # Check if path exists and prompt if necessary
    if target_path.exists() and not force:
        existing_files = list(target_path.iterdir()) if target_path.is_dir() else [target_path]
        if existing_files and target_path.name != ".":
            console.print(f"[yellow]Directory {target_path} exists and is not empty.[/yellow]")
            if not click.confirm("Continue and potentially overwrite files?"):
                console.print("[yellow]Cancelled[/yellow]")
                return

    # Create base directory
    target_path.mkdir(parents=True, exist_ok=True)

    # Get scaffold directory (shipped with package)
    templates_dir = Path(__file__).parent.parent.parent / "scaffold"

    if not templates_dir.exists():
        console.print("[red]Error: Project scaffold directory not found[/red]")
        console.print(f"Expected location: {templates_dir}")
        console.print("\nThis is a package installation issue. Please reinstall qs-trader.")
        return

    created_items: list[str] = []

    # Copy entire project structure
    console.print("\n[cyan]Creating project structure...[/cyan]")

    # Copy config/
    _copy_directory(templates_dir / "config", target_path / "config", created_items)

    # Copy library/
    _copy_directory(templates_dir / "library", target_path / "library", created_items)

    # Copy data/
    _copy_directory(templates_dir / "data", target_path / "data", created_items)

    # Copy examples/
    _copy_directory(templates_dir / "examples", target_path / "examples", created_items)

    # Copy experiments/
    _copy_directory(templates_dir / "experiments", target_path / "experiments", created_items)

    # Copy QS-Trader README (named QS_TRADER_README.md to avoid collision with user's README)
    readme_src = templates_dir / "QS_TRADER_README.md"
    readme_dst = target_path / "QS_TRADER_README.md"
    if readme_src.exists():
        shutil.copy2(readme_src, readme_dst)
        created_items.append("QS_TRADER_README.md")

    # Update config to point to local library
    _update_qs_trader_config(target_path)

    # Success message with structure
    console.print(
        Panel.fit(
            f"[green]✓[/green] QS-Trader project initialized at:\n[cyan]{target_path}[/cyan]",
            title="Success",
            border_style="green",
        )
    )

    # Show structure table
    _display_project_structure(target_path)

    # Next steps
    console.print("\n[bold]🚀 Quick Start:[/bold]")
    console.print("1. [cyan]cd " + (str(target_path) if target_path.name != "." else "# (already here)") + "[/cyan]")
    console.print("2. [dim]# Review example experiments in experiments/[/dim]")
    console.print("3. [dim]# Check example strategies in library/strategies/[/dim]")
    console.print("4. [cyan]qs-trader backtest experiments/buy_hold[/cyan]")
    console.print("\n[bold]📚 What You Got:[/bold]")
    console.print("• [green]2 example experiments[/green] (buy_hold, sma_crossover)")
    console.print("• [green]2 example strategies[/green] (buy & hold, SMA crossover)")
    console.print("• [green]Sample data[/green] (AAPL, limited history)")
    console.print("• [green]Complete configs[/green] (system, data sources)")
    console.print("• [green]Custom library[/green] with examples")
    console.print("\n[bold]📊 Experiment Structure:[/bold]")
    console.print("• Each experiment has its own directory: [cyan]experiments/{name}/[/cyan]")
    console.print("• Config file matches directory: [cyan]{name}.yaml[/cyan]")
    console.print("• Runs are isolated: [cyan]runs/{timestamp}/[/cyan]")
    console.print("• Full provenance tracking with metadata")
    console.print("\n[dim]See QS_TRADER_README.md for full documentation[/dim]")


def _copy_directory(src: Path, dst: Path, items_list: list[str]) -> None:
    """Recursively copy directory and track items."""
    if not src.exists():
        return

    dst.mkdir(parents=True, exist_ok=True)

    for item in src.rglob("*"):
        if item.is_file():
            rel_path = item.relative_to(src)
            dest_file = dst / rel_path
            dest_file.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(item, dest_file)

    # Add to items list (just the top level directory)
    rel_to_parent = dst.name
    if dst.parent.name != dst.parent.parent.name:  # Not root
        rel_to_parent = f"{dst.parent.name}/{dst.name}/"
    items_list.append(rel_to_parent)


def _update_qs_trader_config(project_path: Path) -> None:
    """Update qs_trader.yaml to point to local library."""
    qs_trader_config = project_path / "config" / "qs_trader.yaml"

    if not qs_trader_config.exists():
        return

    content = qs_trader_config.read_text()

    # Update custom_libraries to point to local library
    # Match the line and replace entire line
    import re

    # Replace strategies line
    content = re.sub(
        r"  strategies: null.*$",
        '  strategies: "library/strategies"  # Example strategies included',
        content,
        flags=re.MULTILINE,
    )

    # Replace risk_policies line
    content = re.sub(
        r"  risk_policies: null.*$",
        '  risk_policies: "library/risk_policies"  # Example policies included',
        content,
        flags=re.MULTILINE,
    )

    qs_trader_config.write_text(content)


def _display_project_structure(target_path: Path) -> None:
    """Display the created project structure."""
    table = Table(title="Project Structure", show_header=True, header_style="bold cyan")
    table.add_column("Component", style="cyan", no_wrap=True)
    table.add_column("Description", style="white")

    structure = [
        ("config/", "System and data source configuration"),
        ("  ├── system.yaml", "Main system configuration"),
        ("  └── data_sources.yaml", "Data source definitions"),
        ("experiments/", "Experiment configurations and runs"),
        ("  ├── buy_hold/", "Buy and hold example experiment"),
        ("  ├── sma_crossover/", "SMA crossover example experiment"),
        ("  ├── template/", "Template for new experiments"),
        ("  └── {name}/runs/", "Run artifacts (created on execution)"),
        ("library/", "Custom implementations"),
        ("  ├── strategies/", "Strategy implementations (2 examples)"),
        ("  ├── adapters/", "Custom data adapters (1 example)"),
        ("  ├── indicators/", "Custom indicators (template)"),
        ("  └── risk_policies/", "Risk policies (template)"),
        ("data/", "Market data storage"),
        ("  ├── sample-csv/", "Sample data (AAPL, limited)"),
        ("  └── us-equity-yahoo-csv/", "Yahoo CSV data location"),
        ("QS_TRADER_README.md", "Complete project documentation"),
    ]

    for component, description in structure:
        table.add_row(component, description)

    console.print(table)
