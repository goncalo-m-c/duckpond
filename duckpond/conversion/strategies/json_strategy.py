"""JSON to Parquet conversion strategy."""

from pathlib import Path

import duckdb

from duckpond.conversion.exceptions import ConversionError
from duckpond.conversion.strategies.base import BaseStrategy


class JSONConversionStrategy(BaseStrategy):
    """Strategy for converting JSON to Parquet using DuckDB."""

    def convert(
        self,
        conn: duckdb.DuckDBPyConnection,
        source_path: Path,
        dest_path: Path,
    ) -> int:
        """Convert JSON to Parquet.

        Supports both JSON Lines (.jsonl) and regular JSON arrays.

        Args:
            conn: DuckDB connection
            source_path: Path to the source JSON file
            dest_path: Path where the Parquet file should be written

        Returns:
            Number of rows converted

        Raises:
            ConversionError: If conversion fails
        """
        self._configure_connection(conn)

        source_str = self._escape_path(source_path)
        dest_str = self._escape_path(dest_path)

        try:
            format_param = "auto"

            query = f"""
            COPY (
                SELECT * FROM read_json('{source_str}', format='{format_param}')
            ) TO '{dest_str}' (
                FORMAT PARQUET,
                COMPRESSION {self.config.compression.upper()},
                ROW_GROUP_SIZE {self.config.row_group_size}
            )
            """

            conn.execute(query)

            result = conn.execute(
                f"SELECT COUNT(*) FROM read_parquet('{dest_str}')"
            ).fetchone()
            return result[0] if result else 0

        except Exception as e:
            raise ConversionError(f"JSON conversion failed: {e}") from e
