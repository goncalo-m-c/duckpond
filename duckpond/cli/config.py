"""Configuration management CLI commands."""

from typing import Optional

import typer
import yaml
from rich.console import Console
from rich.syntax import Syntax
from rich.table import Table

from duckpond.cli.output import print_error, print_info, print_panel
from duckpond.config import get_settings
from duckpond.logging_config import get_logger

app = typer.Typer(help="Configuration management")
console = Console()
logger = get_logger(__name__)


@app.command("show")
def show_config(
    section: Optional[str] = typer.Option(
        None,
        "--section",
        "-s",
        help="Show specific section: server, storage, database, duckdb, limits, logging",
    ),
    format: str = typer.Option(
        "table",
        "--format",
        "-f",
        help="Output format: table, yaml, json",
    ),
) -> None:
    """
    Show current system configuration.

    Displays all DuckPond configuration settings or a specific section.
    """
    try:
        settings = get_settings()

        if format == "yaml":
            _show_config_yaml(settings, section)
        elif format == "json":
            _show_config_json(settings, section)
        else:
            _show_config_table(settings, section)

    except Exception as e:
        logger.exception("Failed to load configuration")
        print_error(f"Failed to load configuration: {str(e)}")
        raise typer.Exit(1)


def _show_config_table(settings, section: Optional[str] = None) -> None:
    """Display configuration in table format."""
    console.print()
    print_panel("DuckPond Configuration", border_style="cyan")
    console.print()

    sections = {
        "server": _get_server_config,
        "storage": _get_storage_config,
        "database": _get_database_config,
        "duckdb": _get_duckdb_config,
        "limits": _get_limits_config,
        "logging": _get_logging_config,
    }

    if section:
        if section not in sections:
            print_error(f"Unknown section: {section}")
            print_info(f"Available sections: {', '.join(sections.keys())}")
            raise typer.Exit(1)
        sections_to_show = {section: sections[section]}
    else:
        sections_to_show = sections

    for section_name, get_config_func in sections_to_show.items():
        table = get_config_func(settings)
        console.print(table)
        console.print()


def _show_config_yaml(settings, section: Optional[str] = None) -> None:
    """Display configuration in YAML format."""
    assert yaml is not None
    config_dict = _settings_to_dict(settings, include_defaults=True)

    if section:
        valid_sections = [
            "server",
            "storage",
            "database",
            "duckdb",
            "limits",
            "logging",
        ]
        if section not in valid_sections:
            print_error(f"Unknown section: {section}")
            raise typer.Exit(1)
        config_dict = {section: config_dict.get(section, {})}

    config_yaml = yaml.dump(config_dict, default_flow_style=False, sort_keys=False)
    syntax = Syntax(config_yaml, "yaml", theme="monokai", line_numbers=True)
    console.print(syntax)


def _show_config_json(settings, section: Optional[str] = None) -> None:
    """Display configuration in JSON format."""
    import json

    config_dict = _settings_to_dict(settings, include_defaults=True)

    if section:
        config_dict = {section: config_dict.get(section, {})}

    json_str = json.dumps(config_dict, indent=2)
    syntax = Syntax(json_str, "json", theme="monokai", line_numbers=True)
    console.print(syntax)


def _get_server_config(settings) -> Table:
    """Get server configuration table."""
    table = Table(title="Server Settings", show_header=True, header_style="bold cyan")
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Host", settings.duckpond_host)
    table.add_row("Port", str(settings.duckpond_port))
    table.add_row("Workers", str(settings.duckpond_workers))

    return table


def _get_storage_config(settings) -> Table:
    """Get storage configuration table."""
    table = Table(title="Storage Settings", show_header=True, header_style="bold cyan")
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Default Backend", settings.default_storage_backend)
    table.add_row("Local Storage Path", str(settings.local_storage_path))
    table.add_row("S3 Bucket", settings.s3_bucket or "Not configured")
    table.add_row("S3 Region", settings.s3_region)
    table.add_row("S3 Endpoint URL", settings.s3_endpoint_url or "Default")

    return table


def _get_database_config(settings) -> Table:
    """Get database configuration table."""
    table = Table(title="Database Settings", show_header=True, header_style="bold cyan")
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Metadata DB URL", settings.metadata_db_url)
    table.add_row("Database Type", "SQLite" if settings.is_sqlite else "PostgreSQL")
    table.add_row("Pool Size", str(settings.db_pool_size))
    table.add_row("Max Overflow", str(settings.db_max_overflow))
    table.add_row("Pool Timeout", f"{settings.db_pool_timeout}s")
    table.add_row("Pool Recycle", f"{settings.db_pool_recycle}s")

    return table


def _get_duckdb_config(settings) -> Table:
    """Get DuckDB configuration table."""
    table = Table(title="DuckDB Settings", show_header=True, header_style="bold cyan")
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Memory Limit", settings.duckdb_memory_limit)
    table.add_row("Threads", str(settings.duckdb_threads))
    table.add_row("Pool Size", str(settings.duckdb_pool_size))

    return table


def _get_limits_config(settings) -> Table:
    """Get resource limits configuration table."""
    table = Table(title="Resource Limits", show_header=True, header_style="bold cyan")
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Max File Size", f"{settings.max_file_size_mb} MB")
    table.add_row("Default Storage Quota", f"{settings.default_max_storage_gb} GB")
    table.add_row("Default Query Memory", f"{settings.default_max_query_memory_gb} GB")
    table.add_row(
        "Max Concurrent Queries", str(settings.default_max_concurrent_queries)
    )

    return table


def _get_logging_config(settings) -> Table:
    """Get logging configuration table."""
    table = Table(title="Logging Settings", show_header=True, header_style="bold cyan")
    table.add_column("Setting", style="cyan", no_wrap=True)
    table.add_column("Value", style="white")

    table.add_row("Log Level", settings.log_level)
    table.add_row("Log Format", settings.log_format)

    return table


def _settings_to_dict(settings, include_defaults: bool = False) -> dict:
    """Convert settings object to dictionary."""
    return {
        "server": {
            "host": settings.duckpond_host,
            "port": settings.duckpond_port,
            "workers": settings.duckpond_workers,
        },
        "storage": {
            "default_backend": settings.default_storage_backend,
            "local_path": str(settings.local_storage_path),
            "s3_bucket": settings.s3_bucket,
            "s3_region": settings.s3_region,
            "s3_endpoint_url": settings.s3_endpoint_url,
        },
        "database": {
            "url": settings.metadata_db_url,
            "pool_size": settings.db_pool_size,
            "max_overflow": settings.db_max_overflow,
            "pool_timeout": settings.db_pool_timeout,
            "pool_recycle": settings.db_pool_recycle,
        },
        "duckdb": {
            "memory_limit": settings.duckdb_memory_limit,
            "threads": settings.duckdb_threads,
            "pool_size": settings.duckdb_pool_size,
        },
        "limits": {
            "max_file_size_mb": settings.max_file_size_mb,
            "default_max_storage_gb": settings.default_max_storage_gb,
            "default_max_query_memory_gb": settings.default_max_query_memory_gb,
            "default_max_concurrent_queries": settings.default_max_concurrent_queries,
        },
        "logging": {
            "level": settings.log_level,
            "format": settings.log_format,
        },
    }
