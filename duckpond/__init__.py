"""DuckPond: Multi-tenant data platform with DuckDB and DuckLake."""

__version__ = "0.1.0-alpha"

from duckpond.cli import app as cli_app

__all__ = ["__version__", "cli_app"]
