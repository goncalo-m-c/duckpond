"""Upload router for DuckPond API.

This module provides REST endpoints for file upload with automatic
conversion to Parquet format.
"""

import tempfile
from pathlib import Path
from typing import Optional

import aiofiles
from fastapi import APIRouter, File, Form, UploadFile, status
from pydantic import BaseModel, Field

from duckpond.api.dependencies import CurrentTenant
from duckpond.api.exceptions import ValidationException
from duckpond.config import get_settings
from duckpond.conversion.config import ConversionConfig
from duckpond.conversion.factory import ConverterFactory
from duckpond.storage.local_backend import LocalBackend

router = APIRouter(prefix="/api/v1/upload", tags=["upload"])


class UploadResponse(BaseModel):
    """File upload response with conversion metrics."""

    status: str = Field(..., description="Upload status")
    dataset: str = Field(..., description="Target dataset name")
    file: str = Field(..., description="Original filename")
    size_bytes: int = Field(..., description="Original file size in bytes")
    row_count: int = Field(..., description="Number of rows ingested")
    compression_ratio: Optional[float] = Field(
        None, description="Compression ratio (original/compressed)"
    )
    throughput_mbps: Optional[float] = Field(None, description="Throughput in MB/s")
    duration_seconds: Optional[float] = Field(
        None, description="Total processing time in seconds"
    )


@router.post(
    "/{dataset_name}",
    response_model=UploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload file to dataset",
)
async def upload_file(
    dataset_name: str,
    tenant_id: CurrentTenant,
    file: UploadFile = File(..., description="File to upload"),
    threads: int = Form(4, description="Number of threads for conversion"),
    memory_limit: str = Form("2GB", description="Memory limit for conversion"),
    compression: str = Form("snappy", description="Parquet compression codec"),
):
    """Upload file to dataset with automatic Parquet conversion.

    Supports CSV, JSON, JSONL, and Parquet files. Files are automatically
    converted to Parquet format with configurable compression and validated.

    Args:
        dataset_name: Target dataset name
        tenant_id: Authenticated tenant ID
        file: Uploaded file (multipart form data)
        threads: Number of conversion threads (default: 4)
        memory_limit: Memory limit for DuckDB (default: 2GB)
        compression: Parquet compression (snappy, gzip, zstd, none)

    Returns:
        Upload response with conversion metrics

    Raises:
        ValidationException: If file format is not supported

    Example:
        POST /api/v1/upload/sales
        Content-Type: multipart/form-data

        file=@sales.csv
        threads=8
        memory_limit=4GB
        compression=zstd
    """
    if not file.filename:
        raise ValidationException("Filename is required")

    file_path = Path(file.filename)

    if not ConverterFactory.is_supported(file_path):
        raise ValidationException(
            f"Unsupported file format: {file_path.suffix}. "
            f"Supported formats: .csv, .json, .jsonl, .parquet"
        )

    with tempfile.NamedTemporaryFile(
        delete=False,
        suffix=file_path.suffix,
    ) as tmp_file:
        temp_path = Path(tmp_file.name)

        async with aiofiles.open(temp_path, "wb") as f:
            while chunk := await file.read(8192):
                await f.write(chunk)

    try:
        config = ConversionConfig(
            threads=threads,
            memory_limit=memory_limit,
            compression=compression,
        )

        settings = get_settings()
        backend = LocalBackend(base_path=Path(settings.local_storage_path))

        result = await backend.upload_file(
            local_path=temp_path,
            remote_key=f"{dataset_name}/{file.filename}",
            tenant_id=tenant_id,
            conversion_config=config,
        )

        metrics = result.get("metrics", {})

        return UploadResponse(
            status="success",
            dataset=dataset_name,
            file=file.filename,
            size_bytes=metrics.get("source_size_bytes", 0),
            row_count=metrics.get("row_count", 0),
            compression_ratio=metrics.get("compression_ratio"),
            throughput_mbps=metrics.get("throughput_mbps"),
            duration_seconds=metrics.get("duration_seconds"),
        )

    finally:
        temp_path.unlink(missing_ok=True)


@router.get(
    "/formats",
    status_code=status.HTTP_200_OK,
    summary="List supported formats",
)
async def list_supported_formats():
    """List supported file formats for upload.

    Returns:
        Dictionary of supported formats and their descriptions

    Example:
        GET /api/v1/upload/formats
        {
            "formats": [
                {"extension": ".csv", "description": "Comma-separated values"},
                {"extension": ".json", "description": "JSON lines format"},
                {"extension": ".jsonl", "description": "JSON lines format"},
                {"extension": ".parquet", "description": "Apache Parquet"}
            ]
        }
    """
    return {
        "formats": [
            {
                "extension": ".csv",
                "description": "Comma-separated values",
                "mime_types": ["text/csv", "application/csv"],
            },
            {
                "extension": ".json",
                "description": "JSON format",
                "mime_types": ["application/json"],
            },
            {
                "extension": ".jsonl",
                "description": "JSON lines format (newline-delimited JSON)",
                "mime_types": ["application/x-ndjson"],
            },
            {
                "extension": ".parquet",
                "description": "Apache Parquet columnar format",
                "mime_types": ["application/vnd.apache.parquet"],
            },
        ]
    }
