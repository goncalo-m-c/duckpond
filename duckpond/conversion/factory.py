"""Factory for creating format-specific converters."""

from pathlib import Path
from typing import Optional

from duckpond.conversion.config import ConversionConfig
from duckpond.conversion.converter import DuckDBConverter
from duckpond.conversion.exceptions import UnsupportedFormatError
from duckpond.conversion.strategies.csv_strategy import CSVConversionStrategy
from duckpond.conversion.strategies.json_strategy import JSONConversionStrategy
from duckpond.conversion.strategies.parquet_strategy import ParquetCopyStrategy


class ConverterFactory:
    """Factory for creating format-specific converters."""

    SUPPORTED_FORMATS = {
        ".csv": CSVConversionStrategy,
        ".json": JSONConversionStrategy,
        ".jsonl": JSONConversionStrategy,
        ".parquet": ParquetCopyStrategy,
    }

    @staticmethod
    def create_converter(
        file_path: Path,
        config: Optional[ConversionConfig] = None,
    ) -> DuckDBConverter:
        """Create converter for file based on extension.

        Args:
            file_path: Source file path
            config: Optional conversion configuration

        Returns:
            DuckDBConverter: Configured converter

        Raises:
            UnsupportedFormatError: If file format not supported
        """
        if config is None:
            config = ConversionConfig()

        suffix = file_path.suffix.lower()

        strategy_class = ConverterFactory.SUPPORTED_FORMATS.get(suffix)
        if strategy_class is None:
            raise UnsupportedFormatError(
                f"Unsupported file format: {suffix}. "
                f"Supported: {list(ConverterFactory.SUPPORTED_FORMATS.keys())}"
            )

        strategy = strategy_class(config)
        return DuckDBConverter(config, strategy)

    @staticmethod
    def is_supported(file_path: Path) -> bool:
        """Check if file format is supported.

        Args:
            file_path: File path to check

        Returns:
            bool: True if format is supported
        """
        suffix = file_path.suffix.lower()
        return suffix in ConverterFactory.SUPPORTED_FORMATS
