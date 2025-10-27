"""Base strategy for file conversion operations."""

from abc import ABC, abstractmethod
from pathlib import Path

from duckpond.conversion.config import ConversionConfig
from duckpond.conversion.result import ConversionResult


class BaseConversionStrategy(ABC):
    """Abstract base class for conversion strategies."""

    def __init__(self, config: ConversionConfig):
        """Initialize the conversion strategy.

        Args:
            config: Configuration for conversion operations.
        """
        self.config = config

    @abstractmethod
    async def convert(self, source_path: Path, dest_path: Path) -> ConversionResult:
        """Convert a file from source format to Parquet.

        Args:
            source_path: Path to the source file.
            dest_path: Path where the Parquet file should be written.

        Returns:
            ConversionResult containing metrics and status.

        Raises:
            ConversionError: If conversion fails.
        """
        pass
