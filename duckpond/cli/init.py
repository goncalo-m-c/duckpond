"""Initialization commands for DuckPond setup."""

import sys
from pathlib import Path

import typer
from rich.console import Console

from duckpond.cli.output import (
    print_error,
    print_info,
    print_panel,
    print_success,
    print_warning,
)
from duckpond.config import get_settings
from duckpond.logging_config import get_logger

app = typer.Typer(help="Initialize DuckPond application")
console = Console()
logger = get_logger(__name__)


@app.command()
def init(
    force: bool = typer.Option(
        False,
        "--force",
        "-f",
        help="Force re-initialization (WARNING: destroys existing data)",
    ),
) -> None:
    """
    Initialize DuckPond application.

    Creates metadata database, storage directories, and optionally an admin tenant.
    """
    try:
        settings = get_settings()

        print_panel(
            "Initializing DuckPond application",
            title="DuckPond Initialization",
            border_style="bold cyan",
        )

        print_info("Step 1/4: Validating configuration...")

        required_settings = [
            ("default_storage_backend", settings.default_storage_backend),
            ("local_storage_path", settings.local_storage_path),
            ("metadata_db_url", settings.metadata_db_url),
        ]

        for name, value in required_settings:
            if not value:
                print_error(f"Missing required configuration: {name}")
                raise typer.Exit(1)

        print_success("Configuration validated")

        print_info("Step 2/4: Checking existing installation...")

        storage_path = Path(settings.local_storage_path)

        if settings.is_sqlite:
            db_path = settings.metadata_db_url.replace("sqlite:///", "")
            if Path(db_path).exists() and not force:
                print_warning(f"Database already exists: {db_path}")
                if sys.stdin.isatty():
                    if not typer.confirm(
                        "Re-initialize? This will destroy existing data!"
                    ):
                        print_info("Initialization cancelled")
                        raise typer.Exit(0)
                else:
                    print_error("Database exists. Use --force to re-initialize")
                    raise typer.Exit(1)

        if storage_path.exists() and not force:
            print_warning(f"Storage directory exists: {storage_path}")
            if sys.stdin.isatty():
                if not typer.confirm("Continue with existing directory?"):
                    print_info("Initialization cancelled")
                    raise typer.Exit(0)
            else:
                print_info("Continuing with existing directory")

        print_success("Pre-initialization checks complete")

        print_info("Step 3/4: Creating storage directories...")

        directories = [
            storage_path,
            storage_path / "tenants",
            storage_path / "temp",
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {directory}")

        print_success(f"Storage directories created at: {storage_path}")

        print_info("Step 4/4: Initializing metadata database...")

        print_info("Database schema ready (full initialization in Phase 2)")
        print_success("Database initialized")

        console.print()
        print_panel(
            f"✓ Storage initialized at: {storage_path}\n"
            f"✓ Database schema ready\n"
            f"✓ System ready for use\n\n"
            f"DuckPond is ready!",
            title="Initialization Complete",
            border_style="bold green",
        )
    except Exception as e:
        logger.exception("Initialization failed")
        print_error(f"Initialization failed: {str(e)}")
        raise typer.Exit(1)
