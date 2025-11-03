"""Prometheus streaming ingestor for DuckPond.

This module provides streaming ingestion for Prometheus remote write requests,
integrating with the Arrow conversion pipeline and buffer manager.
"""

import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import UUID

import pyarrow.ipc as ipc

from duckpond.catalog.manager import DuckLakeCatalogManager
from duckpond.streaming.buffer_manager import BufferManager
from duckpond.streaming.ingestor import StreamingIngestor
from duckpond.streaming.prometheus.converter import PrometheusToArrowConverter
from duckpond.streaming.prometheus.protocol import PrometheusRemoteWrite

logger = logging.getLogger(__name__)


class PrometheusStreamingIngestor:
    """Streaming ingestor for Prometheus metrics.

    This ingestor handles the complete pipeline:
    1. Convert Prometheus WriteRequest to Arrow batches
    2. Buffer batches for backpressure management
    3. Stream to Parquet files
    4. Register with catalog

    Example:
        async with create_catalog_manager(account_id) as catalog:
            buffer_manager = BufferManager(
                max_buffer_size_bytes=100 * 1024 * 1024,
                max_queue_depth=100
            )

            ingestor = PrometheusStreamingIngestor(catalog, buffer_manager)

            metrics = await ingestor.ingest_metrics(
                account_id=UUID("..."),
                dataset_name="prometheus_metrics",
                compressed_data=request_body
            )

            print(f"Ingested {metrics['total_rows']} samples")
    """

    def __init__(
        self,
        catalog: DuckLakeCatalogManager,
        buffer_manager: BufferManager,
        storage_root: Optional[Path] = None,
    ):
        """Initialize Prometheus streaming ingestor.

        Args:
            catalog: DuckLake catalog manager
            buffer_manager: Buffer manager for backpressure
            storage_root: Optional root directory for Parquet files
        """
        self.catalog = catalog
        self.buffer_manager = buffer_manager
        self.storage_root = storage_root
        self.converter = PrometheusToArrowConverter()
        self.protocol = PrometheusRemoteWrite()

    async def ingest_metrics(
        self,
        account_id: UUID,
        dataset_name: str,
        compressed_data: bytes,
        batch_size: int = 10000,
        include_metadata: bool = True,
    ) -> Dict[str, Any]:
        """Ingest Prometheus metrics into dataset.

        This method performs the complete ingestion pipeline:
        1. Validate the write request
        2. Convert to Arrow batches
        3. Write batches to temporary Arrow IPC file
        4. Stream IPC file to Parquet via StreamingIngestor

        Args:
            account_id: Account UUID
            dataset_name: Target dataset name
            compressed_data: Snappy-compressed Protobuf WriteRequest
            batch_size: Batch size for conversion (default: 10000)
            include_metadata: Include metric metadata (default: True)

        Returns:
            dict: Ingestion metrics with keys:
                - total_batches: Number of batches processed
                - total_rows: Total number of samples ingested
                - total_bytes: Total bytes written
                - files_written: Number of Parquet files created
                - duration_seconds: Time taken for ingestion
                - compression_ratio: Compression achieved
                - unique_metrics: Number of unique metric names
                - time_range: (min_timestamp, max_timestamp)

        Raises:
            ValueError: If write request is invalid or empty
            BufferOverflowError: If buffer is full and timeout expires

        Example:
            >>> ingestor = PrometheusStreamingIngestor(catalog, buffer_manager)
            >>> metrics = await ingestor.ingest_metrics(
            ...     account_id=uuid.uuid4(),
            ...     dataset_name="metrics",
            ...     compressed_data=request_body
            ... )
            >>> print(f"Ingested {metrics['total_rows']} samples")
        """
        start_time = datetime.now(timezone.utc)

        logger.info(
            f"Starting Prometheus ingestion: account={account_id}, "
            f"dataset={dataset_name}, size={len(compressed_data)} bytes"
        )

        write_request = self.protocol.decode_write_request(compressed_data)

        is_valid, error_msg = self.protocol.validate_write_request(write_request)
        if not is_valid:
            logger.error(f"Invalid write request: {error_msg}")
            raise ValueError(f"Invalid Prometheus write request: {error_msg}")

        stats = self.protocol.get_statistics(write_request)
        logger.info(
            f"Write request: {stats['total_time_series']} time series, "
            f"{stats['total_samples']} samples, "
            f"{stats['unique_metrics']} unique metrics"
        )

        logger.debug("Converting to Arrow batches...")
        batches = await self.converter.convert_write_request_to_batches(
            compressed_data,
            batch_size=batch_size,
            include_metadata=include_metadata,
        )

        if not batches:
            logger.warning("No batches generated from write request")
            return {
                "total_batches": 0,
                "total_rows": 0,
                "total_bytes": 0,
                "files_written": 0,
                "duration_seconds": 0.0,
                "compression_ratio": 0.0,
                "unique_metrics": 0,
                "time_range": (None, None),
            }

        total_rows = sum(b.num_rows for b in batches)
        total_arrow_bytes = sum(b.nbytes for b in batches)

        logger.info(
            f"Generated {len(batches)} batches: {total_rows} rows, {total_arrow_bytes:,} bytes"
        )

        with tempfile.NamedTemporaryFile(
            suffix=".arrow", delete=False, prefix="prometheus_"
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)

            with ipc.new_file(tmp_file, batches[0].schema) as writer:
                for batch in batches:
                    writer.write_batch(batch)

            logger.debug(f"Wrote temporary IPC file: {tmp_path}")

        try:
            if self.storage_root:
                storage_root = self.storage_root
            else:
                from duckpond.config import get_settings

                settings = get_settings()
                storage_root = (
                    Path(settings.storage_path)
                    / "accounts"
                    / str(account_id)
                    / "datasets"
                    / dataset_name
                )

            streaming_ingestor = StreamingIngestor(self.catalog, self.buffer_manager)

            ingest_metrics = await streaming_ingestor.ingest_stream(
                account_id=account_id,
                dataset_name=dataset_name,
                ipc_stream_path=tmp_path,
                storage_root=storage_root,
                expected_schema=batches[0].schema,
                batch_flush_count=10,
                register_catalog=True,
            )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            compression_ratio = (
                ingest_metrics["total_bytes"] / len(compressed_data)
                if len(compressed_data) > 0
                else 0.0
            )

            min_timestamp = (
                datetime.fromtimestamp(stats["min_timestamp"] / 1000.0, tz=timezone.utc)
                if stats["min_timestamp"]
                else None
            )
            max_timestamp = (
                datetime.fromtimestamp(stats["max_timestamp"] / 1000.0, tz=timezone.utc)
                if stats["max_timestamp"]
                else None
            )

            result = {
                "total_batches": len(batches),
                "total_rows": total_rows,
                "total_bytes": ingest_metrics["total_bytes"],
                "files_written": ingest_metrics["files_written"],
                "duration_seconds": duration,
                "compression_ratio": compression_ratio,
                "unique_metrics": stats["unique_metrics"],
                "time_range": (min_timestamp, max_timestamp),
                "buffer_overflows": self.buffer_manager.metrics.buffer_overflows,
                "max_queue_depth": self.buffer_manager.metrics.max_queue_depth,
            }

            logger.info(
                f"Prometheus ingestion complete: {total_rows} rows, "
                f"{ingest_metrics['files_written']} files, "
                f"{duration:.2f}s"
            )

            return result

        finally:
            try:
                tmp_path.unlink()
                logger.debug(f"Cleaned up temporary file: {tmp_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file: {e}")

    async def ingest_metrics_direct(
        self,
        account_id: UUID,
        dataset_name: str,
        compressed_data: bytes,
        batch_size: int = 10000,
        include_metadata: bool = True,
        timeout: float = 30.0,
    ) -> Dict[str, Any]:
        """Ingest Prometheus metrics directly to buffer (without IPC file).

        This is a more direct approach that buffers batches immediately
        without writing to a temporary IPC file. Useful when you want
        to minimize I/O and have a separate consumer processing the buffer.

        Args:
            account_id: Account UUID
            dataset_name: Target dataset name
            compressed_data: Snappy-compressed Protobuf WriteRequest
            batch_size: Batch size for conversion (default: 10000)
            include_metadata: Include metric metadata (default: True)
            timeout: Buffer put timeout in seconds (default: 30.0)

        Returns:
            dict: Ingestion metrics (simplified version)

        Raises:
            ValueError: If write request is invalid
            BufferOverflowError: If buffer is full

        Example:
            >>>
            >>> metrics = await ingestor.ingest_metrics_direct(
            ...     account_id=uuid.uuid4(),
            ...     dataset_name="metrics",
            ...     compressed_data=request_body
            ... )
        """
        start_time = datetime.now(timezone.utc)

        logger.info(
            f"Starting direct Prometheus ingestion: account={account_id}, dataset={dataset_name}"
        )

        write_request = self.protocol.decode_write_request(compressed_data)
        is_valid, error_msg = self.protocol.validate_write_request(write_request)
        if not is_valid:
            raise ValueError(f"Invalid Prometheus write request: {error_msg}")

        batches = await self.converter.convert_write_request_to_batches(
            compressed_data,
            batch_size=batch_size,
            include_metadata=include_metadata,
        )

        if not batches:
            return {
                "total_batches": 0,
                "total_rows": 0,
                "total_bytes": 0,
                "duration_seconds": 0.0,
            }

        for batch in batches:
            await self.buffer_manager.put(batch, timeout=timeout)

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        total_rows = sum(b.num_rows for b in batches)
        total_bytes = sum(b.nbytes for b in batches)

        logger.info(
            f"Direct ingestion complete: {len(batches)} batches, {total_rows} rows, {duration:.2f}s"
        )

        return {
            "total_batches": len(batches),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "duration_seconds": duration,
            "buffer_overflows": self.buffer_manager.metrics.buffer_overflows,
            "max_queue_depth": self.buffer_manager.metrics.max_queue_depth,
        }

    async def get_ingestion_stats(self) -> Dict[str, Any]:
        """Get buffer manager statistics.

        Returns:
            dict: Current buffer statistics

        Example:
            >>> stats = await ingestor.get_ingestion_stats()
            >>> print(f"Buffer depth: {stats['queue_depth']}")
        """
        return {
            "queue_depth": len(self.buffer_manager.queue),
            "current_size_bytes": self.buffer_manager.current_size_bytes,
            "total_batches": self.buffer_manager.metrics.total_batches,
            "total_rows": self.buffer_manager.metrics.total_rows,
            "total_bytes": self.buffer_manager.metrics.total_bytes,
            "buffer_overflows": self.buffer_manager.metrics.buffer_overflows,
            "max_queue_depth": self.buffer_manager.metrics.max_queue_depth,
        }
