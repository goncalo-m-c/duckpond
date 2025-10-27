"""File conversion module for DuckPond.

This module provides functionality for converting various file formats
(CSV, JSON, Parquet) to Parquet format using DuckDB's zero-copy Arrow pipeline.
"""

from duckpond.conversion.config import ConversionConfig
from duckpond.conversion.exceptions import (
    ConversionError,
    ConversionTimeoutError,
    FileSizeExceededError,
    SchemaInferenceError,
    UnsupportedFormatError,
    ValidationError,
)
from duckpond.conversion.result import ConversionResult

__all__ = [
    "ConversionConfig",
    "ConversionError",
    "ConversionResult",
    "ConversionTimeoutError",
    "FileSizeExceededError",
    "SchemaInferenceError",
    "UnsupportedFormatError",
    "ValidationError",
]
