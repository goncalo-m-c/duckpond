"""Database management CLI commands."""

import asyncio

import structlog
import typer
from rich.console import Console
from rich.table import Table

from duckpond.config import get_settings
from duckpond.db import (
    check_migration_status,
    create_engine,
    downgrade_migrations,
    get_current_revision,
    get_migration_history,
    run_migrations,
)

logger = structlog.get_logger()
console = Console()

app = typer.Typer(help="Database management commands", no_args_is_help=True)


@app.command("migrate")
def migrate_cmd(
    revision: str = typer.Option("head", "--revision", "-r", help="Target revision to migrate to"),
    sql: bool = typer.Option(False, "--sql", help="Generate SQL instead of executing"),
) -> None:
    """Run database migrations to specified revision."""

    async def _migrate():
        settings = get_settings()

        engine = create_engine(settings)
        try:
            console.print(f"[bold blue]Running migrations to: {revision}[/bold blue]")

            await run_migrations(engine, revision=revision, sql=sql)

            if not sql:
                current_rev = await get_current_revision(engine)
                console.print(
                    f"[bold green]✓[/bold green] Migration complete! Current revision: {current_rev}"
                )
            else:
                console.print("[bold green]✓[/bold green] SQL generated successfully")

        finally:
            await engine.dispose()

    try:
        asyncio.run(_migrate())
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Migration failed: {e}")
        raise typer.Exit(1)


@app.command("current")
def current_cmd() -> None:
    """Show current database migration revision."""

    async def _current():
        settings = get_settings()

        engine = create_engine(settings)
        try:
            current_rev = await get_current_revision(engine)

            if current_rev:
                console.print(f"Current revision: [bold cyan]{current_rev}[/bold cyan]")
            else:
                console.print("[yellow]No migrations applied yet[/yellow]")

        finally:
            await engine.dispose()

    try:
        asyncio.run(_current())
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Failed to get current revision: {e}")
        raise typer.Exit(1)


@app.command("downgrade")
def downgrade_cmd(
    revision: str = typer.Option("-1", "--revision", "-r", help="Target revision to downgrade to"),
    force: bool = typer.Option(False, "--force", "-f", help="Skip confirmation prompt"),
) -> None:
    """Downgrade database to specified revision."""
    if not force:
        confirm = typer.confirm(
            f"⚠️  This will downgrade the database to revision '{revision}'. Continue?",
            default=False,
        )
        if not confirm:
            console.print("[yellow]Downgrade cancelled[/yellow]")
            raise typer.Exit(0)

    async def _downgrade():
        settings = get_settings()

        engine = create_engine(settings)
        try:
            console.print(f"[bold yellow]Downgrading to: {revision}[/bold yellow]")

            await downgrade_migrations(engine, revision=revision)

            current_rev = await get_current_revision(engine)
            console.print(
                f"[bold green]✓[/bold green] Downgrade complete! Current revision: {current_rev}"
            )

        finally:
            await engine.dispose()

    try:
        asyncio.run(_downgrade())
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Downgrade failed: {e}")
        raise typer.Exit(1)


@app.command("history")
def history_cmd(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show detailed migration info"),
) -> None:
    """Show migration history."""

    async def _history():
        settings = get_settings()

        engine = create_engine(settings)
        try:
            history = await get_migration_history(engine)

            if not history:
                console.print("[yellow]No migrations found[/yellow]")
                return

            table = Table(title="Migration History", show_header=True, header_style="bold magenta")
            table.add_column("Current", style="green", width=10)
            table.add_column("Revision", style="cyan", width=15)
            table.add_column("Down Revision", style="dim", width=15)

            if verbose:
                table.add_column("Description", style="white")

            for migration in history:
                is_current = "✓" if migration["is_current"] else ""
                revision = migration["revision"]
                down_rev = migration["down_revision"] or "base"

                row = [is_current, revision, down_rev]
                if verbose:
                    description = migration["description"] or "No description"
                    row.append(description)

                table.add_row(*row)

            console.print(table)

        finally:
            await engine.dispose()

    try:
        asyncio.run(_history())
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Failed to get migration history: {e}")
        raise typer.Exit(1)


@app.command("status")
def status_cmd() -> None:
    """Check if database is up-to-date with migrations."""

    async def _status():
        settings = get_settings()

        engine = create_engine(settings)
        try:
            status = await check_migration_status(engine)

            current = status["current_revision"] or "None"
            latest = status["latest_revision"]
            up_to_date = status["is_up_to_date"]

            console.print(f"Current revision: [cyan]{current}[/cyan]")
            console.print(f"Latest revision:  [cyan]{latest}[/cyan]")

            if up_to_date:
                console.print("[bold green]✓[/bold green] Database is up-to-date")
            else:
                console.print("[bold yellow]⚠[/bold yellow] Database needs migration")
                console.print("\n[dim]Run 'duckpond db migrate' to update[/dim]")

        finally:
            await engine.dispose()

    try:
        asyncio.run(_status())
    except Exception as e:
        console.print(f"[bold red]✗[/bold red] Failed to check migration status: {e}")
        raise typer.Exit(1)
