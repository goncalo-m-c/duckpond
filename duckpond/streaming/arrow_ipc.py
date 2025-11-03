"""Arrow IPC protocol handler for streaming ingestion."""

import asyncio
from pathlib import Path
from typing import AsyncIterator, Optional

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq

from duckpond.streaming.exceptions import ProtocolError, SchemaValidationError


class ArrowIPCHandler:
    """Handler for Arrow IPC streaming format."""

    def __init__(self, expected_schema: Optional[pa.Schema] = None):
        """Initialize Arrow IPC handler.

        Args:
            expected_schema: Optional schema for validation
        """
        self.expected_schema = expected_schema
        self.received_schema: Optional[pa.Schema] = None

    async def read_stream(
        self,
        stream_path: Path,
    ) -> AsyncIterator[pa.RecordBatch]:
        """Read Arrow IPC stream file asynchronously.

        Args:
            stream_path: Path to Arrow IPC stream file

        Yields:
            pa.RecordBatch: Record batches from stream

        Raises:
            SchemaValidationError: If schema validation fails
            ProtocolError: If IPC format is invalid
        """

        def _read_batches():
            """Blocking IPC read (runs in thread pool)."""
            try:
                with pa.memory_map(str(stream_path), "r") as source:
                    reader = ipc.open_stream(source)

                    if self.expected_schema and reader.schema != self.expected_schema:
                        raise SchemaValidationError(
                            f"Schema mismatch. Expected: {self.expected_schema}, "
                            f"Got: {reader.schema}"
                        )

                    self.received_schema = reader.schema

                    batches = []
                    for batch in reader:
                        batches.append(batch)

                    return batches
            except Exception as e:
                if isinstance(e, SchemaValidationError):
                    raise
                raise ProtocolError(f"Failed to read IPC stream: {e}") from e

        batches = await asyncio.to_thread(_read_batches)

        for batch in batches:
            yield batch

    async def write_stream(
        self,
        batches: AsyncIterator[pa.RecordBatch],
        dest_path: Path,
    ) -> int:
        """Write record batches to Arrow IPC stream file.

        Args:
            batches: Async iterator of record batches
            dest_path: Output IPC stream path

        Returns:
            int: Total number of rows written

        Raises:
            ProtocolError: If batches have inconsistent schemas
        """
        total_rows = 0
        schema = None
        batch_list = []

        async for batch in batches:
            if schema is None:
                schema = batch.schema
            elif batch.schema != schema:
                raise ProtocolError(
                    f"Inconsistent schema in batch. Expected: {schema}, Got: {batch.schema}"
                )

            batch_list.append(batch)
            total_rows += batch.num_rows

        if not batch_list:
            raise ValueError("No batches to write")

        def _write():
            try:
                with pa.OSFile(str(dest_path), "wb") as sink:
                    writer = ipc.new_stream(sink, schema)
                    for batch in batch_list:
                        writer.write_batch(batch)
                    writer.close()
            except Exception as e:
                raise ProtocolError(f"Failed to write IPC stream: {e}") from e

        await asyncio.to_thread(_write)

        return total_rows

    async def convert_to_parquet(
        self,
        ipc_path: Path,
        parquet_path: Path,
        chunk_size: int = 100000,
    ) -> int:
        """Convert Arrow IPC stream to Parquet.

        Uses zero-copy when possible for maximum performance.

        Args:
            ipc_path: Source IPC stream path
            parquet_path: Destination Parquet path
            chunk_size: Rows per Parquet row group (unused, for API compat)

        Returns:
            int: Total rows written

        Raises:
            ProtocolError: If conversion fails
        """
        total_rows = 0
        schema = None

        def _convert():
            nonlocal total_rows, schema

            try:
                with pa.memory_map(str(ipc_path), "r") as source:
                    reader = ipc.open_stream(source)
                    schema = reader.schema

                    with pq.ParquetWriter(
                        str(parquet_path),
                        schema,
                        compression="snappy",
                    ) as writer:
                        for batch in reader:
                            writer.write_batch(batch)
                            total_rows += batch.num_rows
            except Exception as e:
                raise ProtocolError(f"Failed to convert IPC to Parquet: {e}") from e

        await asyncio.to_thread(_convert)

        return total_rows

    @staticmethod
    def validate_schema(schema: pa.Schema) -> bool:
        """Validate Arrow schema for compatibility.

        Args:
            schema: Arrow schema to validate

        Returns:
            bool: True if schema is valid

        Raises:
            SchemaValidationError: If schema is invalid
        """
        if len(schema) == 0:
            raise SchemaValidationError("Schema has no fields")

        field_names = [f.name for f in schema]
        if len(field_names) != len(set(field_names)):
            raise SchemaValidationError("Schema has duplicate field names")

        unsupported_types = []
        for field in schema:
            if pa.types.is_null(field.type):
                unsupported_types.append(field.name)

        if unsupported_types:
            raise SchemaValidationError(
                f"Schema contains unsupported NULL type fields: {unsupported_types}"
            )

        return True
