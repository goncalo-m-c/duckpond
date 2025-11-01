"""CLI module for DuckPond."""

from duckpond.cli.main import app, main_cli
from duckpond.cli import api, config, dataset, db, init, stream, account

__all__ = [
    "app",
    "main_cli",
    "api",
    "config",
    "dataset",
    "db",
    "init",
    "stream",
    "account",
]
