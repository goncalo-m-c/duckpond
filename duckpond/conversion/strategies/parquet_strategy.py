"""Parquet copy strategy."""

from pathlib import Path
import shutil

import duckdb

from duckpond.conversion.exceptions import ConversionError
from duckpond.conversion.strategies.base import BaseStrategy


class ParquetCopyStrategy(BaseStrategy):
    """Strategy for Parquet files (copy without conversion)."""

    def convert(
        self,
        conn: duckdb.DuckDBPyConnection,
        source_path: Path,
        dest_path: Path,
    ) -> int:
        """Copy Parquet file without conversion.

        Validates Parquet file and gets row count.

        Args:
            conn: DuckDB connection
            source_path: Path to the source Parquet file
            dest_path: Path where the Parquet file should be copied

        Returns:
            Number of rows in the Parquet file

        Raises:
            ConversionError: If copy operation fails
        """
        try:
            source_str = self._escape_path(source_path)
            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{source_str}')"
            ).fetchone()
            row_count = result[0] if result else 0

            shutil.copy2(source_path, dest_path)

            return row_count

        except Exception as e:
            raise ConversionError(f"Parquet copy failed: {e}") from e
