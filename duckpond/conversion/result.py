"""Result model for file conversion operations."""

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class ConversionResult:
    """Result of a conversion operation."""

    success: bool
    source_path: Path
    dest_path: Optional[Path] = None

    source_size_bytes: int = 0
    dest_size_bytes: int = 0
    row_count: int = 0
    duration_seconds: float = 0.0

    source_format: str = ""
    dest_format: str = "parquet"
    compression: str = "snappy"
    schema_fingerprint: str = ""

    error_message: Optional[str] = None

    @property
    def compression_ratio(self) -> float:
        """Calculate compression ratio."""
        if self.source_size_bytes == 0:
            return 0.0
        return self.dest_size_bytes / self.source_size_bytes

    @property
    def throughput_mbps(self) -> float:
        """Calculate throughput in MB/s."""
        if self.duration_seconds == 0:
            return 0.0
        mb = self.source_size_bytes / 1024**2
        return mb / self.duration_seconds
