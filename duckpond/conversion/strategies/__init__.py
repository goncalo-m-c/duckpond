"""Conversion strategies for different file formats.

This module contains strategy implementations for converting various file formats
(CSV, JSON, Parquet) to Parquet format using DuckDB.
"""

from duckpond.conversion.strategies.base import BaseStrategy
from duckpond.conversion.strategies.csv_strategy import CSVConversionStrategy
from duckpond.conversion.strategies.json_strategy import JSONConversionStrategy
from duckpond.conversion.strategies.parquet_strategy import ParquetCopyStrategy

__all__ = [
    "BaseStrategy",
    "CSVConversionStrategy",
    "JSONConversionStrategy",
    "ParquetCopyStrategy",
]
