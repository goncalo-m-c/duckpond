"""Base strategy for file conversion operations."""

from abc import ABC, abstractmethod
from pathlib import Path

import duckdb

from duckpond.conversion.config import ConversionConfig


class BaseStrategy(ABC):
    """Abstract base class for conversion strategies."""

    def __init__(self, config: ConversionConfig):
        """Initialize the conversion strategy.

        Args:
            config: Configuration for conversion operations.
        """
        self.config = config

    @abstractmethod
    def convert(
        self,
        conn: duckdb.DuckDBPyConnection,
        source_path: Path,
        dest_path: Path,
    ) -> int:
        """Execute conversion using DuckDB.

        Args:
            conn: DuckDB connection
            source_path: Source file path
            dest_path: Destination Parquet path

        Returns:
            int: Number of rows converted

        Raises:
            ConversionError: If conversion fails
        """
        pass

    def _escape_path(self, path: Path) -> str:
        """Escape single quotes in path for SQL safety.

        Args:
            path: File path to escape

        Returns:
            str: SQL-safe path string
        """
        return str(path).replace("'", "''")

    def _configure_connection(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Apply configuration to DuckDB connection.

        Args:
            conn: DuckDB connection to configure
        """
        conn.execute(f"SET threads={self.config.threads}")
        conn.execute(f"SET memory_limit='{self.config.memory_limit}'")
