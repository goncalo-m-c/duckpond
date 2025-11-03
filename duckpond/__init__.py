"""DuckPond: Multi-account data platform with DuckDB and DuckLake."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("duckpond")
except PackageNotFoundError:
    # Package is not installed, use fallback
    __version__ = "0.0.0.dev"


def __getattr__(name):
    """Lazy import of cli_app to avoid circular imports."""
    if name == "cli_app":
        from duckpond.cli import app as cli_app

        return cli_app
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = ["__version__", "cli_app"]
