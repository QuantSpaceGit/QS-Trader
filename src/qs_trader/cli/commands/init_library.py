"""Initialize custom library structure command."""

import shutil
from pathlib import Path

import click
from rich.console import Console
from rich.panel import Panel

console = Console()

# Template files to copy for each type
TEMPLATE_MAP = {
    "strategy": [
        ("strategy.py", "strategies/__init__.py"),
        ("strategy.py", "strategies/my_strategy.py"),
    ],
    "indicator": [
        ("indicator.py", "indicators/__init__.py"),
        ("indicator.py", "indicators/my_indicator.py"),
    ],
    "adapter": [
        ("adapter.py", "adapters/__init__.py"),
        ("adapter.py", "adapters/my_adapter.py"),
    ],
    "risk-policy": [
        ("risk_policy.yaml", "risk_policies/my_policy.yaml"),
    ],
}


@click.command("init-library")
@click.argument("path", type=click.Path(path_type=Path))
@click.option(
    "--type",
    "-t",
    "types",
    multiple=True,
    type=click.Choice(["strategy", "indicator", "adapter", "risk-policy", "all"]),
    help="Types of components to scaffold (can specify multiple, defaults to 'all')",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Overwrite existing files without prompting",
)
def init_library_command(path: Path, types: tuple[str, ...], force: bool) -> None:
    """
    Initialize a custom library structure with templates.

    Creates directory structure and copies template files for custom
    strategies, indicators, adapters, or risk policies.

    \b
    Examples:
        # Create full structure with all component types
        qs-trader init-library ~/my-qs-trader-extensions

        # Create only strategies and indicators
        qs-trader init-library ./custom --type strategy --type indicator

        # Create structure in existing directory
        qs-trader init-library ./my_lib --force
    """
    # Expand and resolve path
    target_path = path.expanduser().resolve()

    # Determine which types to scaffold
    scaffold_types = set(types) if types else {"all"}
    if "all" in scaffold_types:
        scaffold_types = {"strategy", "indicator", "adapter", "risk-policy"}

    # Check if path exists and prompt if necessary
    if target_path.exists() and not force:
        existing_files = list(target_path.iterdir())
        if existing_files:
            console.print(f"[yellow]Directory {target_path} exists and is not empty.[/yellow]")
            if not click.confirm("Continue and potentially overwrite files?"):
                console.print("[yellow]Cancelled[/yellow]")
                return

    # Create base directory
    target_path.mkdir(parents=True, exist_ok=True)

    # Get templates directory
    templates_dir = Path(__file__).parent.parent.parent / "templates"

    if not templates_dir.exists():
        console.print("[red]Error: Templates directory not found[/red]")
        console.print(f"Expected location: {templates_dir}")
        return

    created_files: list[Path] = []

    # Scaffold each type
    for component_type in scaffold_types:
        if component_type not in TEMPLATE_MAP:
            continue

        for template_file, target_file in TEMPLATE_MAP[component_type]:
            template_path = templates_dir / template_file
            target_file_path = target_path / target_file

            # Create parent directory
            target_file_path.parent.mkdir(parents=True, exist_ok=True)

            # Copy template
            if template_path.exists():
                # For __init__.py files, create empty file if it's the first one
                if target_file.endswith("__init__.py"):
                    if not target_file_path.exists() or force:
                        target_file_path.write_text('"""Custom QS-Trader components."""\n')
                        created_files.append(target_file_path)
                else:
                    shutil.copy2(template_path, target_file_path)
                    created_files.append(target_file_path)
            else:
                console.print(f"[yellow]Warning: Template not found: {template_path}[/yellow]")

    # Create README
    readme_path = target_path / "README.md"
    if not readme_path.exists() or force:
        readme_content = _generate_readme(target_path, scaffold_types)
        readme_path.write_text(readme_content)
        created_files.append(readme_path)

    # Success message
    console.print(
        Panel.fit(
            f"[green]✓[/green] Library structure created at:\n[cyan]{target_path}[/cyan]",
            title="Success",
            border_style="green",
        )
    )

    console.print("\n[bold]Created files:[/bold]")
    for file in sorted(created_files):
        rel_path = file.relative_to(target_path)
        console.print(f"  • {rel_path}")

    # Next steps
    console.print("\n[bold]Next steps:[/bold]")
    console.print("1. Edit the template files to implement your logic")
    console.print("2. Update [cyan]config/qs_trader.yaml[/cyan] to point to your library:")
    console.print("   [dim]custom_libraries:[/dim]")
    for comp_type in sorted(scaffold_types):
        if comp_type == "risk-policy":
            dir_name = "risk_policies"
        else:
            dir_name = comp_type + "s"
        console.print(f"     [dim]{dir_name}: {target_path}/{dir_name}[/dim]")
    console.print("3. Run your backtest with the custom components")
    console.print("\n[dim]Tip: See src/qs_trader/scaffold/library/ for example implementations[/dim]")


def _generate_readme(path: Path, types: set[str]) -> str:
    """Generate README content for the library."""
    structure_items = []
    if "strategy" in types:
        structure_items.append("- `strategies/` - Custom trading strategies")
    if "indicator" in types:
        structure_items.append("- `indicators/` - Custom technical indicators")
    if "adapter" in types:
        structure_items.append("- `adapters/` - Custom data adapters")
    if "risk-policy" in types:
        structure_items.append("- `risk_policies/` - Custom risk policies (YAML)")

    structure_section = "\n".join(structure_items) if structure_items else "No components scaffolded"

    config_items = []
    if "strategy" in types:
        config_items.append(f"  strategies: {path}/strategies")
    if "indicator" in types:
        config_items.append(f"  indicators: {path}/indicators")
    if "adapter" in types:
        config_items.append(f"  adapters: {path}/adapters")
    if "risk-policy" in types:
        config_items.append(f"  risk_policies: {path}/risk_policies")

    config_section = "\n".join(config_items) if config_items else "  # No custom libraries"

    return f"""# Custom QS-Trader Library

This directory contains custom components for QS-Trader.

## Structure

{structure_section}

## Usage

1. **Implement your components** by editing the template files:
   - Add your trading logic to strategies
   - Implement calculation methods for indicators
   - Add data loading logic to adapters
   - Configure risk parameters in YAML files

2. **Update configuration** in `config/qs_trader.yaml`:

```yaml
custom_libraries:
{config_section}
```

3. **Reference components** in your backtest configs:
   - Strategies: Use the class name (e.g., `MyStrategy`)
   - Indicators: Import and use in strategy code
   - Adapters: Register with data service
   - Risk policies: Reference the YAML filename (without extension)

## Documentation

- [QS-Trader Documentation](https://github.com/QuantSpaceGit/QS-Trader) - Main documentation
- See `src/qs_trader/scaffold/library/` in the QS-Trader package for example implementations
- See `src/qs_trader/templates/` for up-to-date component templates

## Tips

- **Strategies**: Implement `on_bar()` to receive market data and generate signals
- **Indicators**: Implement `calculate()` to compute values from OHLCV data
- **Adapters**: Implement `load_bars()` to load data from your source
- **Risk Policies**: Configure sizing, limits, stops, and other risk parameters

Run `qs-trader init-library --help` for more options.
"""
