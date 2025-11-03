"""Streaming ingestion router for DuckPond API.

This module provides endpoints for streaming data ingestion using Arrow IPC format.
Supports real-time data ingestion with backpressure management and catalog integration.
"""

import logging
import tempfile
import time
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, File, Request, UploadFile, status
from pydantic import BaseModel, Field

from duckpond.api.dependencies import CurrentAccount
from duckpond.api.exceptions import (
    BadRequestException,
    NotFoundException,
    ValidationException,
)
from duckpond.catalog.manager import create_catalog_manager
from duckpond.config import get_settings
from duckpond.streaming.buffer_manager import BufferManager
from duckpond.streaming.ingestor import StreamingIngestor
from duckpond.streaming.prometheus.ingestor import PrometheusStreamingIngestor
from duckpond.streaming.prometheus.parser import PrometheusParser

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/stream", tags=["streaming"])


class StreamIngestResponse(BaseModel):
    """Stream ingestion response."""

    status: str
    total_batches: int
    total_rows: int
    total_bytes: int
    files_written: int
    duration_seconds: float
    buffer_overflows: int
    max_queue_depth: int


class StreamConfig(BaseModel):
    """Stream ingestion configuration."""

    batch_flush_count: Optional[int] = Field(
        10, ge=1, le=1000, description="Number of batches to buffer before flushing"
    )
    max_buffer_size_mb: Optional[int] = Field(
        100, ge=1, le=1000, description="Maximum buffer size in MB"
    )
    max_queue_depth: Optional[int] = Field(
        100, ge=1, le=1000, description="Maximum queue depth for buffering"
    )


@router.post(
    "/{dataset_name}",
    response_model=StreamIngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Ingest Arrow IPC stream",
    description="""
Ingest data via Arrow IPC streaming format.

Features:
- Zero-copy Arrow IPC → Parquet conversion
- Backpressure management
- Automatic catalog registration
- Batch buffering and flushing
- Real-time ingestion metrics

The endpoint accepts an Arrow IPC stream file and ingests it into the specified dataset.
Data is buffered in memory and flushed to Parquet files periodically.

Example:
    POST /api/v1/stream/sales
    Content-Type: multipart/form-data

    file: <arrow_ipc_stream.arrow>
    batch_flush_count: 10
    max_buffer_size_mb: 100
""",
)
async def ingest_stream(
    dataset_name: str,
    account_id: CurrentAccount,
    file: UploadFile = File(..., description="Arrow IPC stream file"),
    batch_flush_count: int = 10,
    max_buffer_size_mb: int = 100,
    max_queue_depth: int = 100,
):
    """Ingest Arrow IPC stream into dataset.

    Args:
        dataset_name: Name of the target dataset
        account_id: Authenticated account ID
        file: Arrow IPC stream file
        batch_flush_count: Number of batches to buffer before flushing
        max_buffer_size_mb: Maximum buffer size in MB
        max_queue_depth: Maximum queue depth

    Returns:
        StreamIngestResponse with ingestion metrics

    Raises:
        NotFoundException: If dataset not found
        ValidationException: If stream format is invalid
        BadRequestException: If ingestion fails
    """
    start_time = time.time()

    logger.info(
        f"Starting stream ingestion for dataset {dataset_name}",
        extra={
            "account_id": account_id,
            "dataset_name": dataset_name,
            "filename": file.filename,
            "content_type": file.content_type,
        },
    )

    if file.content_type and "arrow" not in file.content_type.lower():
        logger.warning(
            f"Unexpected content type: {file.content_type}",
            extra={"account_id": account_id, "content_type": file.content_type},
        )

    temp_file_path = None

    try:
        with tempfile.NamedTemporaryFile(
            delete=False, suffix=".arrow", prefix="stream_"
        ) as temp_file:
            temp_file_path = Path(temp_file.name)

            chunk_size = 8192
            bytes_written = 0
            while chunk := await file.read(chunk_size):
                temp_file.write(chunk)
                bytes_written += len(chunk)

            logger.info(
                f"Uploaded stream file: {bytes_written} bytes",
                extra={
                    "account_id": account_id,
                    "bytes_written": bytes_written,
                    "temp_path": str(temp_file_path),
                },
            )

        settings = get_settings()
        storage_root = (
            Path(settings.local_storage_path).expanduser()
            / "accounts"
            / account_id
            / "datasets"
            / dataset_name
        )

        async with create_catalog_manager(account_id) as catalog:
            try:
                await catalog.get_dataset_metadata(dataset_name)
            except Exception as e:
                if "not found" in str(e).lower():
                    logger.info(
                        f"Dataset {dataset_name} not found, will be created during ingestion",
                        extra={"account_id": account_id, "dataset_name": dataset_name},
                    )
                else:
                    raise

            max_buffer_bytes = max_buffer_size_mb * 1024 * 1024
            buffer_manager = BufferManager(
                max_buffer_size_bytes=max_buffer_bytes,
                max_queue_depth=max_queue_depth,
            )

            ingestor = StreamingIngestor(
                catalog=catalog,
                buffer_manager=buffer_manager,
            )

            metrics = await ingestor.ingest_stream(
                account_id=account_id,
                dataset_name=dataset_name,
                ipc_stream_path=temp_file_path,
                storage_root=storage_root,
                expected_schema=None,
                batch_flush_count=batch_flush_count,
            )

            duration_seconds = time.time() - start_time

            logger.info(
                f"Stream ingestion completed: {metrics['total_rows']} rows, {metrics['files_written']} files",
                extra={
                    "account_id": account_id,
                    "dataset_name": dataset_name,
                    "metrics": metrics,
                    "duration_seconds": duration_seconds,
                },
            )

            return StreamIngestResponse(
                status="success",
                total_batches=metrics["total_batches"],
                total_rows=metrics["total_rows"],
                total_bytes=metrics["total_bytes"],
                files_written=metrics["files_written"],
                duration_seconds=duration_seconds,
                buffer_overflows=metrics["buffer_overflows"],
                max_queue_depth=metrics["max_queue_depth"],
            )

    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"Stream ingestion failed: {error_msg}",
            extra={
                "account_id": account_id,
                "dataset_name": dataset_name,
                "error": error_msg,
            },
        )

        if "not found" in error_msg.lower():
            raise NotFoundException(f"Dataset {dataset_name} not found")
        elif "schema" in error_msg.lower() or "invalid" in error_msg.lower():
            raise ValidationException(f"Stream validation failed: {error_msg}")
        else:
            raise BadRequestException(f"Stream ingestion failed: {error_msg}")

    finally:
        if temp_file_path and temp_file_path.exists():
            try:
                temp_file_path.unlink()
                logger.debug(
                    f"Cleaned up temporary file: {temp_file_path}",
                    extra={"account_id": account_id, "temp_path": str(temp_file_path)},
                )
            except Exception as e:
                logger.warning(
                    f"Failed to cleanup temporary file: {e}",
                    extra={"account_id": account_id, "temp_path": str(temp_file_path)},
                )


@router.get(
    "/formats",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    summary="Get supported streaming formats",
    description="Returns information about supported streaming formats and their specifications.",
)
async def get_streaming_formats():
    """Get supported streaming formats.

    Returns:
        Dictionary with format specifications
    """
    return {
        "formats": [
            {
                "name": "Arrow IPC",
                "format": "arrow",
                "mime_types": [
                    "application/vnd.apache.arrow.stream",
                    "application/octet-stream",
                ],
                "description": "Apache Arrow IPC streaming format",
                "features": [
                    "Zero-copy conversion",
                    "Schema preservation",
                    "High throughput",
                    "Batch processing",
                ],
                "extensions": [".arrow", ".arrows", ".ipc"],
            },
            {
                "name": "Prometheus Remote Write",
                "format": "prometheus",
                "mime_types": [
                    "application/x-protobuf",
                ],
                "description": "Prometheus remote write protocol (Snappy + Protobuf)",
                "features": [
                    "Native Prometheus integration",
                    "Snappy compression",
                    "Dynamic schema inference",
                    "Label-to-column mapping",
                ],
                "endpoint": "/api/v1/stream/prometheus/{dataset_name}",
            },
        ],
        "buffer_config": {
            "default_batch_flush_count": 10,
            "default_max_buffer_size_mb": 100,
            "default_max_queue_depth": 100,
            "max_batch_flush_count": 1000,
            "max_buffer_size_mb": 1000,
            "max_queue_depth": 1000,
        },
    }


@router.post(
    "/prometheus/{dataset_name}",
    response_model=StreamIngestResponse,
    status_code=status.HTTP_200_OK,
    summary="Prometheus remote write endpoint",
    description="""
Ingest metrics via Prometheus remote write protocol.

Accepts Snappy-compressed Protobuf data from Prometheus servers.

Prometheus configuration:
```yaml
remote_write:
  - url: http://localhost:8000/api/v1/stream/prometheus/metrics
    headers:
      X-API-Key: your_api_key_here
    remote_timeout: 30s
    queue_config:
      capacity: 10000
      max_shards: 10
      max_samples_per_send: 5000
```

Features:
- Prometheus remote write protocol support
- Snappy decompression and Protobuf decoding
- Automatic schema inference from labels
- Dynamic label-to-column mapping
- Backpressure management
- Automatic catalog registration
""",
)
async def prometheus_remote_write(
    dataset_name: str,
    account_id: CurrentAccount,
    request: Request,
    batch_size: int = 10000,
    include_metadata: bool = True,
):
    """Prometheus remote write endpoint.

    Args:
        dataset_name: Name of the target dataset
        account_id: Authenticated account ID
        request: FastAPI request object
        batch_size: Batch size for Arrow conversion (default: 10000)
        include_metadata: Include metric metadata (default: True)

    Returns:
        StreamIngestResponse with ingestion metrics

    Raises:
        ValidationException: If request is invalid
        BadRequestException: If ingestion fails
    """
    start_time = time.time()

    logger.info(
        f"Starting Prometheus ingestion for dataset {dataset_name}",
        extra={
            "account_id": account_id,
            "dataset_name": dataset_name,
            "content_type": request.headers.get("content-type"),
            "content_encoding": request.headers.get("content-encoding"),
        },
    )

    parser = PrometheusParser()
    try:
        parser.validate_headers(dict(request.headers))
    except ValueError as e:
        logger.error(
            f"Invalid Prometheus headers: {e}",
            extra={"account_id": account_id, "error": str(e)},
        )
        raise ValidationException(f"Invalid Prometheus request headers: {e}")

    compressed_data = await request.body()

    is_valid, error_msg = parser.validate_request_size(len(compressed_data))
    if not is_valid:
        logger.error(
            f"Invalid request size: {error_msg}",
            extra={"account_id": account_id, "size": len(compressed_data)},
        )
        raise ValidationException(f"Invalid request size: {error_msg}")

    logger.info(
        f"Received Prometheus write request: {len(compressed_data)} bytes",
        extra={
            "account_id": account_id,
            "dataset_name": dataset_name,
            "compressed_size": len(compressed_data),
        },
    )

    try:
        settings = get_settings()
        storage_root = (
            Path(settings.local_storage_path).expanduser()
            / "accounts"
            / account_id
            / "datasets"
            / dataset_name
        )

        async with create_catalog_manager(account_id) as catalog:
            buffer_manager = BufferManager(
                max_buffer_size_bytes=100 * 1024 * 1024,
                max_queue_depth=100,
            )

            ingestor = PrometheusStreamingIngestor(
                catalog=catalog,
                buffer_manager=buffer_manager,
                storage_root=storage_root,
            )

            metrics = await ingestor.ingest_metrics(
                account_id=account_id,
                dataset_name=dataset_name,
                compressed_data=compressed_data,
                batch_size=batch_size,
                include_metadata=include_metadata,
            )

            duration_seconds = time.time() - start_time

            logger.info(
                f"Prometheus ingestion completed: {metrics['total_rows']} samples, "
                f"{metrics['files_written']} files, {duration_seconds:.2f}s",
                extra={
                    "account_id": account_id,
                    "dataset_name": dataset_name,
                    "metrics": metrics,
                    "duration_seconds": duration_seconds,
                },
            )

            return StreamIngestResponse(
                status="success",
                total_batches=metrics["total_batches"],
                total_rows=metrics["total_rows"],
                total_bytes=metrics["total_bytes"],
                files_written=metrics["files_written"],
                duration_seconds=duration_seconds,
                buffer_overflows=metrics["buffer_overflows"],
                max_queue_depth=metrics["max_queue_depth"],
            )

    except ValueError as e:
        error_msg = str(e)
        logger.error(
            f"Prometheus ingestion validation failed: {error_msg}",
            extra={
                "account_id": account_id,
                "dataset_name": dataset_name,
                "error": error_msg,
            },
        )
        raise ValidationException(f"Invalid Prometheus data: {error_msg}")

    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"Prometheus ingestion failed: {error_msg}",
            extra={
                "account_id": account_id,
                "dataset_name": dataset_name,
                "error": error_msg,
            },
            exc_info=True,
        )
        raise BadRequestException(f"Prometheus ingestion failed: {error_msg}")


@router.get(
    "/{dataset_name}/status",
    response_model=dict,
    status_code=status.HTTP_200_OK,
    summary="Get dataset streaming status",
    description="Returns current streaming status and metrics for a dataset.",
)
async def get_streaming_status(
    dataset_name: str,
    account_id: CurrentAccount,
):
    """Get streaming status for dataset.

    Args:
        dataset_name: Name of the dataset
        account_id: Authenticated account ID

    Returns:
        Dictionary with streaming status

    Raises:
        NotFoundException: If dataset not found
    """
    logger.info(
        f"Getting streaming status for dataset {dataset_name}",
        extra={
            "account_id": account_id,
            "dataset_name": dataset_name,
        },
    )

    try:
        settings = get_settings()
        storage_root = (
            Path(settings.local_storage_path).expanduser()
            / "accounts"
            / account_id
            / "datasets"
            / dataset_name
        )

        async with create_catalog_manager(account_id) as catalog:
            try:
                metadata = await catalog.get_dataset_metadata(dataset_name)

                file_count = 0
                total_bytes = 0
                if storage_root.exists():
                    parquet_files = list(storage_root.glob("*.parquet"))
                    file_count = len(parquet_files)
                    total_bytes = sum(f.stat().st_size for f in parquet_files)

                return {
                    "dataset_name": dataset_name,
                    "status": "ready",
                    "files_count": file_count,
                    "total_bytes": total_bytes,
                    "storage_path": str(storage_root),
                    "metadata": {
                        "created_at": (
                            metadata.created_at.isoformat() if metadata.created_at else None
                        ),
                        "updated_at": (
                            metadata.updated_at.isoformat() if metadata.updated_at else None
                        ),
                    },
                }

            except Exception as e:
                if "not found" in str(e).lower():
                    raise NotFoundException(f"Dataset {dataset_name} not found")
                raise

    except NotFoundException:
        raise
    except Exception as e:
        error_msg = str(e)
        logger.error(
            f"Failed to get streaming status: {error_msg}",
            extra={
                "account_id": account_id,
                "dataset_name": dataset_name,
                "error": error_msg,
            },
        )
        raise BadRequestException(f"Failed to get streaming status: {error_msg}")
