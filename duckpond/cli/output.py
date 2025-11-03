"""Output formatting utilities for CLI."""

import csv
import json
from io import StringIO
from typing import Any

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

console = Console()
console_err = Console(stderr=True)


def print_table(
    data: list[dict[str, Any]],
    title: str | None = None,
    columns: list[str] | None = None,
) -> None:
    """Print data as a Rich table.

    Args:
        data: List of dictionaries to display
        title: Optional table title
        columns: Optional list of column names (defaults to all keys)
    """
    if not data:
        console.print("[yellow]No data to display[/yellow]")
        return

    if columns is None:
        columns = list(data[0].keys())

    table = Table(title=title, show_header=True, header_style="bold cyan")

    for col in columns:
        table.add_column(col, style="white", no_wrap=False)

    for row in data:
        table.add_row(*[str(row.get(col, "")) for col in columns])

    console.print(table)


def print_json(data: Any, indent: int = 2) -> None:
    """Print data as JSON.

    Args:
        data: Data to print as JSON
        indent: Number of spaces for indentation
    """
    console.print_json(json.dumps(data, indent=indent, default=str))


def print_csv(data: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    """Print data as CSV.

    Args:
        data: List of dictionaries to display
        columns: Optional list of column names
    """
    if not data:
        return

    if columns is None:
        columns = list(data[0].keys())

    output = StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(data)

    console.print(output.getvalue(), end="")


def print_dict(data: dict[str, Any], title: str | None = None) -> None:
    """Print dictionary as a formatted table.

    Args:
        data: Dictionary to display
        title: Optional table title
    """
    table = Table(title=title, show_header=False)
    table.add_column("Key", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    for key, value in data.items():
        table.add_row(key, str(value))

    console.print(table)


def print_success(message: str) -> None:
    """Print success message with checkmark.

    Args:
        message: Success message to display
    """
    console.print(f"[green]✓[/green] {message}")


def print_error(message: str) -> None:
    """Print error message with X mark.

    Args:
        message: Error message to display
    """
    console_err.print(f"[red]✗[/red] {message}")


def print_warning(message: str) -> None:
    """Print warning message with warning symbol.

    Args:
        message: Warning message to display
    """
    console.print(f"[yellow]⚠[/yellow] {message}")


def print_info(message: str) -> None:
    """Print info message with info symbol.

    Args:
        message: Info message to display
    """
    console.print(f"[blue]ℹ[/blue] {message}")


def confirm(message: str, default: bool = False) -> bool:
    """Ask user for confirmation.

    Args:
        message: Confirmation prompt
        default: Default value if user presses enter

    Returns:
        True if user confirmed, False otherwise
    """
    return Confirm.ask(message, default=default)


def prompt(message: str, default: str = "", password: bool = False) -> str:
    """Prompt user for input.

    Args:
        message: Prompt message
        default: Default value if user presses enter
        password: Hide input if True

    Returns:
        User input string
    """
    return Prompt.ask(message, default=default, password=password)


def print_panel(content: str, title: str | None = None, border_style: str = "cyan") -> None:
    """Print content in a bordered panel.

    Args:
        content: Content to display
        title: Optional panel title
        border_style: Border color style
    """
    panel = Panel(content, title=title, border_style=border_style)
    console.print(panel)
