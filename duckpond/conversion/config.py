"""Configuration for file conversion operations."""

from dataclasses import dataclass
from typing import Literal


@dataclass
class ConversionConfig:
    """Configuration for DuckDB Parquet conversion."""

    threads: int = 4
    memory_limit: str = "2GB"

    compression: Literal["snappy", "zstd", "gzip", "uncompressed"] = "snappy"
    row_group_size: int = 122880

    max_file_size_bytes: int = 5 * 1024**3
    timeout_seconds: int = 300
    validate_row_count: bool = True
    validate_schema: bool = True

    executor_max_workers: int = 4

    def __post_init__(self):
        """Validate configuration."""
        if self.threads < 1:
            raise ValueError("threads must be >= 1")
        if self.executor_max_workers < 1:
            raise ValueError("executor_max_workers must be >= 1")
        if self.max_file_size_bytes < 1:
            raise ValueError("max_file_size_bytes must be >= 1")
        if self.timeout_seconds < 1:
            raise ValueError("timeout_seconds must be >= 1")
