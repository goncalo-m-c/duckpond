"""DuckDB-based file converter with async execution."""

import asyncio
import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb

from duckpond.conversion.config import ConversionConfig
from duckpond.conversion.exceptions import (
    ConversionTimeoutError,
    FileSizeExceededError,
    ValidationError,
)
from duckpond.conversion.result import ConversionResult
from duckpond.conversion.strategies.base import BaseStrategy


class DuckDBConverter:
    """Async DuckDB-based file converter."""

    def __init__(
        self,
        config: ConversionConfig,
        strategy: BaseStrategy,
    ):
        """Initialize the DuckDB converter.

        Args:
            config: Conversion configuration
            strategy: Format-specific conversion strategy
        """
        self.config = config
        self.strategy = strategy
        self.executor = ThreadPoolExecutor(max_workers=config.executor_max_workers)

    async def convert_to_parquet(
        self,
        source_path: Path,
        dest_path: Path,
    ) -> ConversionResult:
        """Convert file to Parquet asynchronously.

        Args:
            source_path: Source file path
            dest_path: Destination Parquet path

        Returns:
            ConversionResult: Conversion metrics and status

        Raises:
            FileSizeExceededError: If file exceeds size limit
            ConversionTimeoutError: If conversion times out
            ValidationError: If validation checks fail
        """
        start_time = time.time()

        try:
            await self._validate_source(source_path)

            result = await asyncio.wait_for(
                self._execute_conversion(source_path, dest_path),
                timeout=self.config.timeout_seconds,
            )

            if self.config.validate_row_count:
                await self._validate_result(source_path, dest_path, result.row_count)

            duration = time.time() - start_time
            result.duration_seconds = duration

            return result

        except asyncio.TimeoutError:
            raise ConversionTimeoutError(
                f"Conversion timed out after {self.config.timeout_seconds}s"
            )
        except (
            FileSizeExceededError,
            ValidationError,
            ConversionTimeoutError,
            FileNotFoundError,
        ):
            raise
        except Exception as e:
            duration = time.time() - start_time
            return ConversionResult(
                success=False,
                source_path=source_path,
                error_message=str(e),
                duration_seconds=duration,
            )

    async def _validate_source(self, source_path: Path) -> None:
        """Validate source file before conversion.

        Args:
            source_path: Path to source file

        Raises:
            FileNotFoundError: If source file doesn't exist
            FileSizeExceededError: If file exceeds size limit
        """
        if not source_path.exists():
            raise FileNotFoundError(f"Source file not found: {source_path}")

        file_size = source_path.stat().st_size
        if file_size > self.config.max_file_size_bytes:
            raise FileSizeExceededError(
                f"File size {file_size} exceeds limit {self.config.max_file_size_bytes}"
            )

    async def _execute_conversion(
        self,
        source_path: Path,
        dest_path: Path,
    ) -> ConversionResult:
        """Execute conversion in thread pool.

        Args:
            source_path: Path to source file
            dest_path: Path to destination file

        Returns:
            ConversionResult with metrics
        """
        loop = asyncio.get_event_loop()

        def _blocking_convert():
            """Blocking DuckDB conversion (runs in thread pool)."""
            conn = duckdb.connect()

            try:
                row_count = self.strategy.convert(conn, source_path, dest_path)

                source_size = source_path.stat().st_size
                dest_size = dest_path.stat().st_size

                schema_fp = self._compute_schema_fingerprint(conn, dest_path)

                return ConversionResult(
                    success=True,
                    source_path=source_path,
                    dest_path=dest_path,
                    source_size_bytes=source_size,
                    dest_size_bytes=dest_size,
                    row_count=row_count,
                    source_format=source_path.suffix[1:],
                    compression=self.config.compression,
                    schema_fingerprint=schema_fp,
                )
            finally:
                conn.close()

        return await loop.run_in_executor(self.executor, _blocking_convert)

    async def _validate_result(
        self,
        source_path: Path,
        dest_path: Path,
        row_count: int,
    ) -> None:
        """Validate conversion result.

        Args:
            source_path: Path to source file
            dest_path: Path to destination file
            row_count: Number of rows converted

        Raises:
            ValidationError: If validation fails
        """
        if not dest_path.exists():
            raise ValidationError(f"Destination file not created: {dest_path}")

        if row_count == 0:
            raise ValidationError("Converted file has zero rows")

    def _compute_schema_fingerprint(
        self,
        conn: duckdb.DuckDBPyConnection,
        parquet_path: Path,
    ) -> str:
        """Compute schema fingerprint for Parquet file.

        Uses column names and types to create a stable hash.

        Args:
            conn: DuckDB connection
            parquet_path: Path to Parquet file

        Returns:
            Schema fingerprint (16-character hex string)
        """
        path_str = str(parquet_path).replace("'", "''")
        result = conn.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{path_str}')"
        ).fetchall()

        schema_str = ",".join(f"{row[0]}:{row[1]}" for row in result)

        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    def shutdown(self):
        """Shutdown thread pool executor."""
        self.executor.shutdown(wait=True)
