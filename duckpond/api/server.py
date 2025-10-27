"""DuckPond API server entry point.

This module provides the server command for starting the DuckPond API
using uvicorn with configurable options for host, port, workers, and reload.
"""

import logging
from typing import Optional

import click
import uvicorn

from duckpond.config import get_settings

logger = logging.getLogger(__name__)


@click.command()
@click.option(
    "--host",
    default="0.0.0.0",
    help="Host to bind to (default: 0.0.0.0)",
    show_default=True,
)
@click.option(
    "--port",
    default=8000,
    type=int,
    help="Port to bind to (default: 8000)",
    show_default=True,
)
@click.option(
    "--workers",
    default=1,
    type=int,
    help="Number of worker processes (default: 1)",
    show_default=True,
)
@click.option(
    "--reload",
    is_flag=True,
    help="Enable auto-reload for development (default: False)",
    show_default=True,
)
@click.option(
    "--log-level",
    type=click.Choice(
        ["debug", "info", "warning", "error", "critical"], case_sensitive=False
    ),
    help="Logging level (overrides config)",
)
@click.option(
    "--access-log/--no-access-log",
    default=True,
    help="Enable/disable access logging (default: enabled)",
    show_default=True,
)
def serve(
    host: str,
    port: int,
    workers: int,
    reload: bool,
    log_level: Optional[str],
    access_log: bool,
):
    """Start DuckPond API server.

    The server uses uvicorn as the ASGI server and supports both
    single-worker and multi-worker deployments.

    Examples:
        duckpond api serve

        duckpond api serve --host 0.0.0.0 --port 9000

        duckpond api serve --workers 4

        duckpond api serve --reload

        duckpond api serve --log-level debug

    Note:
        - Auto-reload (--reload) is only available in single-worker mode
        - Multiple workers are recommended for production deployments
        - Use a reverse proxy (nginx, caddy) in production
    """
    settings = get_settings()

    if log_level:
        effective_log_level = log_level.lower()
    else:
        effective_log_level = settings.log_level.lower()

    if reload and workers > 1:
        click.echo(
            click.style(
                "Warning: --reload flag is ignored when workers > 1. "
                "Using single worker mode with reload.",
                fg="yellow",
            )
        )
        workers = 1

    click.echo(
        click.style(
            "\n🦆 Starting DuckPond API Server\n",
            fg="cyan",
            bold=True,
        )
    )
    click.echo(f"  Host:        {host}")
    click.echo(f"  Port:        {port}")
    click.echo(f"  Workers:     {workers}")
    click.echo(f"  Reload:      {reload}")
    click.echo(f"  Log Level:   {effective_log_level}")
    click.echo(f"  Access Log:  {access_log}")
    click.echo(
        f"\n  Docs:        http://{host if host != '0.0.0.0' else 'localhost'}:{port}/docs"
    )
    click.echo(
        f"  Health:      http://{host if host != '0.0.0.0' else 'localhost'}:{port}/health\n"
    )

    uvicorn_config = {
        "app": "duckpond.api.app:app",
        "host": host,
        "port": port,
        "log_level": effective_log_level,
        "access_log": access_log,
    }

    if reload:
        uvicorn_config["reload"] = True
        click.echo(
            click.style(
                "  Mode:        Development (auto-reload enabled)\n", fg="yellow"
            )
        )
    else:
        uvicorn_config["workers"] = workers
        mode = "Production" if workers > 1 else "Single Worker"
        click.echo(click.style(f"  Mode:        {mode}\n", fg="green"))

    try:
        uvicorn.run(**uvicorn_config)
    except KeyboardInterrupt:
        click.echo(click.style("\n\n✓ Server stopped gracefully\n", fg="green"))
    except Exception as e:
        click.echo(click.style(f"\n\n✗ Server error: {e}\n", fg="red"))
        raise


if __name__ == "__main__":
    serve()
