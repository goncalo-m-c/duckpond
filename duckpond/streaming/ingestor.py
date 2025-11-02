"""Streaming ingestion pipeline for Arrow IPC → Parquet.

This module provides end-to-end streaming ingestion with:
- Producer-consumer pattern with buffering
- Zero-copy Arrow IPC → Parquet conversion
- Backpressure handling
- Catalog integration for metadata tracking
"""

import asyncio
import hashlib
import zlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq

from duckpond.catalog.manager import DuckLakeCatalogManager
from duckpond.streaming.arrow_ipc import ArrowIPCHandler
from duckpond.streaming.buffer_manager import BufferManager
from duckpond.streaming.exceptions import StreamingError


class StreamingIngestor:
    """Streaming ingestion pipeline for Arrow IPC → Parquet.

    Implements a producer-consumer pattern where:
    - Producer reads Arrow IPC stream and buffers batches
    - Consumer writes buffered batches to Parquet files
    - Backpressure is managed through BufferManager
    - Written files are registered with the catalog

    Example:
        catalog = await create_catalog_manager(account_id, db_path)
        buffer_manager = BufferManager(
            max_buffer_size_bytes=100 * 1024 * 1024,
            max_queue_depth=100
        )

        ingestor = StreamingIngestor(catalog, buffer_manager)

        metrics = await ingestor.ingest_stream(
            account_id=UUID("..."),
            dataset_name="sales",
            ipc_stream_path=Path("data.arrow"),
            storage_root=Path("/data/warehouse/sales"),
            expected_schema=schema,
        )

        print(f"Ingested {metrics['total_rows']} rows")
    """

    def __init__(
        self,
        catalog: Optional[DuckLakeCatalogManager],
        buffer_manager: BufferManager,
    ):
        """Initialize streaming ingestor.

        Args:
            catalog: DuckLake catalog manager for metadata (optional)
            buffer_manager: Buffer manager for backpressure control
        """
        self.catalog = catalog
        self.buffer_manager = buffer_manager
        self._file_counter = 0

    async def ingest_stream(
        self,
        account_id: Union[str, UUID],
        stream_name: str,
        ipc_stream_path: Path,
        storage_root: Path,
        expected_schema: Optional[pa.Schema] = None,
        batch_flush_count: int = 10,
    ) -> dict:
        """Ingest Arrow IPC stream into stream.

        Args:
            account_id: Account UUID or string
            stream_name: Target stream name
            ipc_stream_path: Path to Arrow IPC stream file
            storage_root: Root directory for Parquet files
            expected_schema: Optional schema for validation
            batch_flush_count: Number of batches to buffer before flushing

        Returns:
            dict: Ingestion metrics including:
                - total_batches: Total batches processed
                - total_rows: Total rows ingested
                - total_bytes: Total bytes processed
                - buffer_overflows: Number of buffer overflow events
                - max_queue_depth: Maximum queue depth reached
                - files_written: Number of Parquet files written
                - duration_seconds: Total ingestion time

        Raises:
            StreamingError: If ingestion fails
        """
        if not ipc_stream_path.exists():
            raise StreamingError(f"IPC stream file not found: {ipc_stream_path}")

        storage_root.mkdir(parents=True, exist_ok=True)

        start_time = datetime.now(timezone.utc)
        files_written = 0

        handler = ArrowIPCHandler(expected_schema)

        consumer_context = {
            "account_id": account_id,
            "stream_name": stream_name,
            "storage_root": storage_root,
            "batch_flush_count": batch_flush_count,
            "files_written": 0,
        }

        try:
            producer_task = asyncio.create_task(
                self._producer(handler, ipc_stream_path)
            )

            consumer_task = asyncio.create_task(self._consumer(consumer_context))

            await asyncio.gather(producer_task, consumer_task)

            files_written = consumer_context["files_written"]

            if self.catalog and files_written > 0:
                glob_pattern = str(storage_root / "*.parquet")
                await self.catalog.register_parquet_file(
                    dataset_name=stream_name,
                    file_path=glob_pattern,
                )

        except Exception as e:
            raise StreamingError(f"Ingestion failed: {e}") from e

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        return {
            "total_batches": self.buffer_manager.metrics.total_batches,
            "total_rows": self.buffer_manager.metrics.total_rows,
            "total_bytes": self.buffer_manager.metrics.total_bytes,
            "buffer_overflows": self.buffer_manager.metrics.buffer_overflows,
            "max_queue_depth": self.buffer_manager.metrics.max_queue_depth,
            "files_written": files_written,
            "duration_seconds": duration,
        }

    async def _producer(
        self,
        handler: ArrowIPCHandler,
        ipc_stream_path: Path,
    ) -> None:
        """Producer: Read IPC stream and buffer batches.

        Reads batches from the Arrow IPC stream file and puts them
        into the buffer manager. Handles backpressure by waiting
        when the buffer is full.

        Args:
            handler: Arrow IPC handler
            ipc_stream_path: Path to IPC stream file
        """
        try:
            async for batch in handler.read_stream(ipc_stream_path):
                await self.buffer_manager.put(batch, timeout=30.0)
        finally:
            await self.buffer_manager.close()

    async def _consumer(
        self,
        context: dict,
    ) -> None:
        """Consumer: Write buffered batches to Parquet.

        Reads batches from the buffer manager, accumulates them,
        and flushes to Parquet files periodically.

        Args:
            context: Consumer context with configuration and state
        """
        batch_count = 0
        temp_batches = []

        account_id = context["account_id"]
        stream_name = context["stream_name"]
        storage_root = context["storage_root"]
        batch_flush_count = context["batch_flush_count"]

        while True:
            batch = await self.buffer_manager.get()

            if batch is None:
                break

            temp_batches.append(batch)
            batch_count += 1

            if batch_count >= batch_flush_count:
                await self._flush_batches(
                    account_id,
                    stream_name,
                    storage_root,
                    temp_batches,
                )
                context["files_written"] += 1
                temp_batches = []
                batch_count = 0

        if temp_batches:
            await self._flush_batches(
                account_id,
                stream_name,
                storage_root,
                temp_batches,
            )
            context["files_written"] += 1

    async def _flush_batches(
        self,
        account_id: Union[str, UUID],
        stream_name: str,
        storage_root: Path,
        batches: list[pa.RecordBatch],
    ) -> None:
        """Write batches to Parquet file and register with catalog.

        Args:
            account_id: Account UUID or string
            stream_name: Dataset name
            storage_root: Root directory for Parquet files
            batches: List of record batches to write

        Raises:
            StreamingError: If writing or registration fails
        """
        if not batches:
            return

        table = pa.Table.from_batches(batches)

        self._file_counter += 1
        parquet_name = f"stream_{self._file_counter:05d}.parquet"
        parquet_path = storage_root / parquet_name

        try:
            await asyncio.to_thread(
                pq.write_table,
                table,
                parquet_path,
                compression="snappy",
            )

        except Exception as e:
            raise StreamingError(
                f"Failed to flush batches to {parquet_path}: {e}"
            ) from e

    def _compute_checksum(self, file_path: Path) -> str:
        """Compute CRC32 checksum of file.

        Args:
            file_path: Path to file

        Returns:
            Hexadecimal checksum string
        """
        checksum = 0
        with open(file_path, "rb") as f:
            while chunk := f.read(8192):
                checksum = zlib.crc32(chunk, checksum)
        return f"{checksum:08x}"

    def _compute_schema_fingerprint(self, schema: pa.Schema) -> str:
        """Compute schema fingerprint for versioning.

        Args:
            schema: Arrow schema

        Returns:
            16-character hexadecimal fingerprint
        """
        schema_str = str(schema)
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]
