"""API server CLI commands for DuckPond."""

from typing import Optional

import typer
from rich.console import Console

app = typer.Typer(
    name="api",
    help="API server management commands",
    add_completion=True,
)

console = Console()


@app.command("serve")
def serve(
    host: str = typer.Option(
        "0.0.0.0",
        "--host",
        "-h",
        help="Host to bind to",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        "-p",
        help="Port to bind to",
    ),
    workers: int = typer.Option(
        1,
        "--workers",
        "-w",
        help="Number of worker processes",
    ),
    reload: bool = typer.Option(
        False,
        "--reload",
        "-r",
        help="Enable auto-reload for development",
    ),
    log_level: Optional[str] = typer.Option(
        None,
        "--log-level",
        "-l",
        help="Logging level (debug, info, warning, error, critical)",
    ),
    access_log: bool = typer.Option(
        True,
        "--access-log/--no-access-log",
        help="Enable/disable access logging",
    ),
) -> None:
    """Start DuckPond API server.

    The server uses uvicorn as the ASGI server and supports both
    single-worker and multi-worker deployments.

    Examples:
        duckpond api serve

        duckpond api serve --host 0.0.0.0 --port 9000

        duckpond api serve --workers 4

        duckpond api serve --reload

        duckpond api serve --log-level debug
    """
    import uvicorn
    from duckpond.config import get_settings

    settings = get_settings()

    if log_level:
        effective_log_level = log_level.lower()
    else:
        effective_log_level = settings.log_level.lower()

    if reload and workers > 1:
        console.print(
            "[yellow]Warning:[/yellow] --reload flag is ignored when workers > 1. "
            "Using single worker mode with reload."
        )
        workers = 1

    console.print("\n[bold cyan]🦆 Starting DuckPond API Server[/bold cyan]\n")
    console.print(f"  Host:        {host}")
    console.print(f"  Port:        {port}")
    console.print(f"  Workers:     {workers}")
    console.print(f"  Reload:      {reload}")
    console.print(f"  Log Level:   {effective_log_level}")
    console.print(f"  Access Log:  {access_log}")

    host_display = "localhost" if host == "0.0.0.0" else host
    console.print(f"\n  Docs:        http://{host_display}:{port}/docs")
    console.print(f"  Health:      http://{host_display}:{port}/health\n")

    uvicorn_config = {
        "app": "duckpond.api.app:app",
        "host": host,
        "port": port,
        "log_level": effective_log_level,
        "access_log": access_log,
    }

    if reload:
        uvicorn_config["reload"] = True
        console.print(
            "[yellow]  Mode:        Development (auto-reload enabled)[/yellow]\n"
        )
    else:
        uvicorn_config["workers"] = workers
        mode = "Production" if workers > 1 else "Single Worker"
        console.print(f"[green]  Mode:        {mode}[/green]\n")

    try:
        uvicorn.run(**uvicorn_config)
    except KeyboardInterrupt:
        console.print("\n[green]✓ Server stopped gracefully[/green]\n")
    except Exception as e:
        console.print(f"\n[red]✗ Server error: {e}[/red]\n")
        raise typer.Exit(1)


@app.command("status")
def status(
    host: str = typer.Option(
        "localhost",
        "--host",
        "-h",
        help="API server host",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        "-p",
        help="API server port",
    ),
) -> None:
    """Check API server status.

    Checks if the API server is running and responsive by
    querying the health endpoint.

    Example:
        duckpond api status
        duckpond api status --host localhost --port 9000
    """
    import httpx

    url = f"http://{host}:{port}/health"

    try:
        console.print(f"[blue]Checking API server at {url}...[/blue]")
        response = httpx.get(url, timeout=5.0)

        if response.status_code == 200:
            data = response.json()
            console.print("\n[green]✓ API server is running[/green]\n")
            console.print(f"  Status:      {data.get('status', 'unknown')}")
            console.print(f"  Version:     {data.get('version', 'unknown')}")
            console.print(f"  Timestamp:   {data.get('timestamp', 'unknown')}\n")
        else:
            console.print(
                f"\n[yellow]⚠ Server responded with status {response.status_code}[/yellow]\n"
            )
            raise typer.Exit(1)

    except httpx.ConnectError:
        console.print(f"\n[red]✗ Cannot connect to API server at {url}[/red]")
        console.print("[yellow]Is the server running?[/yellow]\n")
        raise typer.Exit(1)
    except httpx.TimeoutException:
        console.print(f"\n[red]✗ Connection timeout to {url}[/red]\n")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"\n[red]✗ Error checking server status: {e}[/red]\n")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
